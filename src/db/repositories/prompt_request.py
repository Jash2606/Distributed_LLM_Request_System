import hashlib
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.repositories.base import BaseRepository
from src.models.enums import RequestStatus
from src.models.orm import ProcessingJobORM, PromptRequestORM


class PromptRequestRepository(BaseRepository):
    """Repository for prompt_requests table (M in MVC)."""

    async def get_by_prompt_id(self, prompt_id: str) -> Optional[PromptRequestORM]:
        async with self._session() as session:
            result = await session.execute(
                select(PromptRequestORM).where(PromptRequestORM.prompt_id == prompt_id)
            )
            return result.scalar_one_or_none()

    async def insert(self, row: PromptRequestORM) -> PromptRequestORM:
        async with self._session() as session:
            async with session.begin():
                session.add(row)
            await session.refresh(row)
            return row

    # ── MUST FIX #1 — Atomic request + job insert ─────────────────────────────
    #
    # WHY THIS IS CRITICAL:
    #   The old code called request_repo.insert() then job_repo.insert() in two
    #   separate transactions.  A process crash (OOM, SIGKILL, DB blip) between
    #   those two calls leaves a prompt_requests row with status='queued' and NO
    #   corresponding processing_jobs row.  No worker ever claims it, the reaper
    #   never touches it, and GET /result returns status='queued' forever.
    #
    # HOW THIS FIX WORKS:
    #   Both rows are added inside a single `session.begin()` block.
    #   session.flush() after the request insert makes Postgres assign request.id
    #   (via RETURNING id / sequence.nextval) — we need it to set the FK on the
    #   job before adding the job to the same transaction.
    #   If either INSERT fails (constraint, crash, etc.) the entire transaction
    #   rolls back atomically: we never have a request without a job.
    #
    # TRADEOFF:
    #   This couples the request and job inserts into a single method that lives
    #   on the request repository (it must touch ProcessingJobORM).  A cleaner
    #   solution would be a dedicated UnitOfWork service, but for this codebase
    #   the coupling is explicit and contained.
    async def insert_with_job(
        self,
        request: PromptRequestORM,
        job: ProcessingJobORM,
    ) -> tuple[PromptRequestORM, ProcessingJobORM]:
        """
        Insert a PromptRequest and its ProcessingJob in ONE atomic transaction.

        Rollback guarantee: if the process crashes anywhere inside session.begin(),
        PostgreSQL rolls back the entire transaction — we never get a request row
        without a corresponding job row.

        flush() is called after adding the request so that Postgres assigns
        request.id before we set it as the FK on the job.
        expire_on_commit=False (set in session factory) ensures both IDs are
        readable from the detached objects after the session closes.
        """
        async with self._session() as session:
            async with session.begin():
                session.add(request)
                await session.flush()          # Postgres assigns request.id
                job.prompt_request_id = request.id
                session.add(job)
                await session.flush()          # Postgres assigns job.id
            # Still inside the session context — refresh to materialise
            # server-side defaults (created_at, updated_at).
            await session.refresh(request)
            await session.refresh(job)
        return request, job

    async def update_status(
        self,
        row_id: int,
        status: RequestStatus,
        **extra_fields,
    ) -> None:
        values = {"status": status.value, "updated_at": datetime.now(timezone.utc), **extra_fields}
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(PromptRequestORM)
                    .where(PromptRequestORM.id == row_id)
                    .values(**values)
                )

    async def mark_completed(
        self,
        row_id: int,
        response: str,
        cached: bool,
        embedding: Optional[list] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        values = {
            "status": RequestStatus.COMPLETED.value,
            "response": response,
            "cached": cached,
            "error": None,   # clear any error from prior failed attempts
            "completed_at": now,
            "updated_at": now,
        }
        if embedding is not None:
            values["embedding"] = embedding
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(PromptRequestORM)
                    .where(PromptRequestORM.id == row_id)
                    .values(**values)
                )

    async def mark_failed(
        self,
        row_id: int,
        error: str,
        retry_count: int,
    ) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(PromptRequestORM)
                    .where(PromptRequestORM.id == row_id)
                    .values(
                        status=RequestStatus.FAILED.value,
                        error=error,
                        retry_count=retry_count,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
