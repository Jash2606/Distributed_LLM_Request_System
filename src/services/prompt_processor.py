"""
PromptProcessor — Facade for the API layer.

Responsibilities:
  • Idempotency check
  • Persist request + enqueue job (single atomic transaction)  [FIXED]
  • NOTIFY worker via Postgres LISTEN/NOTIFY
  • LISTEN for completion then return result within timeout

Fixes applied in this version:
  MUST #1 — Atomic request+job insert (was two separate transactions)
  MUST #4 — IntegrityError from concurrent same-prompt_id inserts → idempotent 200
  SHOULD #1 — LISTEN before NOTIFY (was NOTIFY before LISTEN → missed notifications)
  SHOULD #4 — Backpressure: reject INSERT_NEW when queue is at capacity
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import structlog
from sqlalchemy.exc import IntegrityError

from src.db.listen_notify import ListenNotify
from src.db.repositories.processing_job import ProcessingJobRepository
from src.db.repositories.prompt_request import PromptRequestRepository
from src.exceptions import QueueFullError
from src.models.enums import JobStatus, Priority, RequestStatus
from src.models.orm import ProcessingJobORM, PromptRequestORM
from src.services.cache import SemanticCacheService
from src.services.idempotency import IdempotencyAction, IdempotencyService

log = structlog.get_logger(__name__)


@dataclass
class SubmitResult:
    request: PromptRequestORM
    already_completed: bool = False


class PromptProcessor:
    def __init__(
        self,
        idempotency: IdempotencyService,
        cache: SemanticCacheService,
        request_repo: PromptRequestRepository,
        job_repo: ProcessingJobRepository,
        notifier: ListenNotify,
        max_job_attempts: int = 5,
        max_queue_depth: int = 1000,
    ):
        self._idempotency = idempotency
        self._cache = cache
        self._request_repo = request_repo
        self._job_repo = job_repo
        self._notifier = notifier
        self._max_attempts = max_job_attempts
        self._max_queue_depth = max_queue_depth

    async def submit_and_wait(
        self,
        user_id: str,
        prompt_id: str,
        text: str,
        priority: str = "normal",
        timeout_seconds: int = 10,
    ) -> PromptRequestORM:
        t_start = time.monotonic()
        text_hash = SemanticCacheService.hash_text(text)
        decision = await self._idempotency.check(prompt_id, text_hash)

        # Fast path — prompt already completed; return latest DB state (never stale).
        if decision.action == IdempotencyAction.RETURN_EXISTING:
            log.info("idempotency_return", prompt_id=prompt_id)
            latest = await self._request_repo.get_by_prompt_id(prompt_id)
            result = latest if latest is not None else decision.existing
            # This specific call did NOT invoke the LLM — override cached=True
            # so the response honestly reflects what happened for this request.
            # The DB row keeps its original cached value (historical accuracy).
            if result and result.status == "completed":
                result.cached = True
            return result

        # ── Determine what action to take ─────────────────────────────────────
        should_notify = False  # whether we need to wake workers after setup
        req: Optional[PromptRequestORM] = None

        if decision.action == IdempotencyAction.INSERT_NEW:
            # SHOULD FIX #4 — Backpressure.
            # Only check depth for INSERT_NEW — REENQUEUE and AWAIT_EXISTING
            # refer to already-queued work and should always proceed.
            depth = await self._job_repo.queued_depth()
            if depth >= self._max_queue_depth:
                raise QueueFullError(depth, self._max_queue_depth)

            priority_int = Priority.from_str(priority).value
            req_orm = PromptRequestORM(
                user_id=user_id,
                prompt_id=prompt_id,
                text=text,
                text_hash=text_hash,
                priority=priority_int,
                status=RequestStatus.QUEUED.value,
            )
            job_orm = ProcessingJobORM(
                status=JobStatus.QUEUED.value,
                priority=priority_int,
                max_attempts=self._max_attempts,
                # prompt_request_id is set inside insert_with_job after flush()
            )

            # MUST FIX #1 — Atomic request + job insert.
            # The old code called request_repo.insert() then job_repo.insert() in
            # two separate transactions.  A crash between them left a request row
            # with no job row — permanently stuck.
            # insert_with_job() uses a single session.begin() so both rows commit
            # or both roll back; there is no partial state.
            #
            # MUST FIX #4 — Catch IntegrityError from concurrent same-prompt_id.
            # Two requests that both pass the idempotency check simultaneously
            # (both see no existing row) both attempt INSERT.  The second hits
            # the UNIQUE(prompt_id) constraint → IntegrityError.
            # Without this catch: HTTP 500.
            # With this catch: re-fetch what the winning request inserted and
            # fall through to the wait loop — correct idempotent behaviour.
            try:
                req, _ = await self._request_repo.insert_with_job(req_orm, job_orm)
                should_notify = True
                log.info("request_enqueued", prompt_id=prompt_id)
            except IntegrityError:
                # Lost the insert race — the concurrent request already inserted.
                # Fetch its row and wait for its job to complete.
                log.info("idempotency_race_insert_caught", prompt_id=prompt_id)
                req = await self._request_repo.get_by_prompt_id(prompt_id)
                if req is None:
                    raise  # truly unexpected — re-raise original IntegrityError
                # should_notify stays False: the winning request already notified
                # workers (or will in the next few microseconds).

        elif decision.action == IdempotencyAction.REENQUEUE:
            req = decision.existing
            await self._request_repo.update_status(req.id, RequestStatus.QUEUED)
            await self._job_repo.reset_job(req.id)
            should_notify = True
            log.info("request_reenqueued", prompt_id=prompt_id)

        else:
            # AWAIT_EXISTING — job already in-flight from another request; just wait.
            req = decision.existing
            log.info("awaiting_existing_job", prompt_id=prompt_id)

        # Pre-check: job may have already completed (e.g. instant cache hit).
        final = await self._request_repo.get_by_prompt_id(prompt_id)
        if final and final.status in ("completed", "failed"):
            return final

        # ── SHOULD FIX #1 — LISTEN before NOTIFY ─────────────────────────────
        # Old order:
        #   1. notify("job_queued")          ← workers wake, may finish in 5 ms
        #   2. (much later) wait_for_notification()  ← LISTEN opened now — too late
        #
        # New order (prepared_listener context manager):
        #   1. open connection + add_listener()   ← channel is LIVE
        #   2. notify("job_queued") inside ctx    ← workers wake after we LISTEN
        #   3. wait() in poll loop                ← catches notification immediately
        #
        # The prepared_listener also shields the underlying asyncio.Future so it
        # survives multiple wait() calls in the 1-second poll loop below.
        channel = f"prompt_done_{req.id}"
        deadline = time.monotonic() + max(0.5, timeout_seconds - (time.monotonic() - t_start))

        async with self._notifier.prepared_listener(channel) as wait_notify:
            # Send NOTIFY *after* the listener is open — workers wake into a live channel.
            if should_notify:
                await self._notifier.notify("job_queued", prompt_id)

            while time.monotonic() < deadline:
                wait = min(1.0, deadline - time.monotonic())
                if wait <= 0:
                    break
                await wait_notify(timeout=wait)
                final = await self._request_repo.get_by_prompt_id(prompt_id)
                if final and final.status in ("completed", "failed"):
                    return final

        # Timeout — return whatever state it's in (client can poll /result/{id})
        return await self._request_repo.get_by_prompt_id(prompt_id)
