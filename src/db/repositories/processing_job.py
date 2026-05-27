from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.repositories.base import BaseRepository
from src.models.enums import JobStatus
from src.models.orm import ProcessingJobORM, PromptRequestORM


class ProcessingJobRepository(BaseRepository):
    """Repository for processing_jobs table — the durable queue (M in MVC)."""

    async def insert(self, job: ProcessingJobORM) -> ProcessingJobORM:
        async with self._session() as session:
            async with session.begin():
                session.add(job)
            await session.refresh(job)
            return job

    async def claim_next(
        self, worker_id: str, lease_seconds: int
    ) -> Optional[tuple[ProcessingJobORM, PromptRequestORM]]:
        """
        Atomically claim one queued job using FOR UPDATE SKIP LOCKED.
        Priority ASC + scheduled_at ASC gives high-priority jobs first.
        Returns (job, request) or None if queue is empty.
        """
        now = datetime.now(timezone.utc)
        locked_until = now + timedelta(seconds=lease_seconds)

        async with self._session() as session:
            async with session.begin():
                # Select and lock
                result = await session.execute(
                    text("""
                        WITH claimed AS (
                            SELECT id
                            FROM processing_jobs
                            WHERE status = 'queued'
                              AND scheduled_at <= :now
                            ORDER BY priority ASC, scheduled_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE processing_jobs j
                        SET status        = 'processing',
                            worker_id     = :worker_id,
                            locked_until  = :locked_until,
                            last_heartbeat_at = :now,
                            started_at    = COALESCE(started_at, :now),
                            attempt_count = attempt_count + 1
                        FROM claimed
                        WHERE j.id = claimed.id
                        RETURNING j.id, j.prompt_request_id, j.status,
                                  j.worker_id, j.attempt_count, j.max_attempts,
                                  j.priority, j.scheduled_at, j.locked_until,
                                  j.last_heartbeat_at, j.started_at,
                                  j.finished_at, j.error_message, j.created_at
                    """),
                    {"now": now, "worker_id": worker_id, "locked_until": locked_until},
                )
                row = result.mappings().first()
                if row is None:
                    return None

                job = ProcessingJobORM(**dict(row))

                # Fetch associated request
                req_result = await session.execute(
                    text("SELECT * FROM prompt_requests WHERE id = :id"),
                    {"id": job.prompt_request_id},
                )
                req_row = req_result.mappings().first()
                if req_row is None:
                    return None
                req_dict = dict(req_row)
                # asyncpg returns VECTOR columns as strings from text() SQL — normalise
                if req_dict.get("embedding") is not None:
                    raw = req_dict["embedding"]
                    if isinstance(raw, str):
                        req_dict["embedding"] = [float(x) for x in raw.strip("[]{}").split(",")]
                req = PromptRequestORM(**req_dict)
                return job, req

    # ── MUST FIX #2 — Heartbeat must validate worker ownership ────────────────
    #
    # WHY THIS IS CRITICAL:
    #   Old signature: extend_lease(job_id, lease_seconds) — no ownership check.
    #   Race scenario:
    #     1. Worker A processes job, heartbeat fails silently (DB blip).
    #     2. Lease expires; reaper reclaims and re-queues the job.
    #     3. Worker B claims the job, sets worker_id = 'worker-B'.
    #     4. Worker A's heartbeat fires again — it was updating ANY row with
    #        the matching job_id, so it inadvertently extends Worker B's lease
    #        using Worker A's extended deadline, silently corrupting B's state.
    #   Worse: Worker A and B both complete and call mark_completed — double write.
    #
    # HOW THIS FIX WORKS:
    #   The UPDATE now includes `AND worker_id = :worker_id AND status = 'processing'`.
    #   If the reaper has already reclaimed the job (status changed to 'queued', or
    #   another worker has it with a different worker_id), the WHERE clause matches
    #   0 rows → RETURNING returns nothing → method returns False.
    #   HeartbeatManager checks the return value and cancels the pipeline task if False.
    #
    # TRADEOFF:
    #   One extra column in the WHERE clause.  Negligible cost.  The correctness
    #   gain (preventing double-processing) is essential.
    async def extend_lease(
        self, job_id: int, worker_id: str, lease_seconds: int
    ) -> bool:
        """
        Extend the lease ONLY if this worker still owns the job.

        Returns True  → lease extended, processing can continue.
        Returns False → ownership lost (reaper reclaimed or another worker took over).
                        The caller MUST stop processing this job immediately.
        """
        now = datetime.now(timezone.utc)
        async with self._session() as session:
            async with session.begin():
                result = await session.execute(
                    text("""
                        UPDATE processing_jobs
                        SET locked_until      = :locked_until,
                            last_heartbeat_at = :now
                        WHERE id        = :job_id
                          AND worker_id = :worker_id
                          AND status    = 'processing'
                        RETURNING id
                    """),
                    {
                        "job_id": job_id,
                        "worker_id": worker_id,
                        "locked_until": now + timedelta(seconds=lease_seconds),
                        "now": now,
                    },
                )
                return result.fetchone() is not None

    # ── MUST FIX #3 — release_lease must validate worker ownership ────────────
    #
    # WHY THIS IS CRITICAL:
    #   Old signature: release_lease(job_id, retry_after_seconds) — no ownership check.
    #   Race scenario:
    #     1. Worker A's lease expires while processing.
    #     2. Reaper re-queues; Worker B claims the job.
    #     3. Worker A finally finishes its LLM call and falls through to release_lease
    #        (e.g. rate-limit deferral path) — it resets the job to 'queued'.
    #     4. Worker B's active job just got its status stomped to 'queued' mid-flight.
    #        Another worker claims it again → triple processing of the same job.
    #
    # HOW THIS FIX WORKS:
    #   `AND worker_id = :worker_id` ensures only the current owner can re-queue.
    #   If the job was already taken by another worker, the WHERE matches 0 rows
    #   and returns False — the old worker silently does nothing.
    #
    # TRADEOFF:
    #   All callers must pass worker_id (job.worker_id from the ORM object).
    #   This is always available at the call sites (pipeline has the job ORM).
    async def release_lease(
        self,
        job_id: int,
        worker_id: str,
        retry_after_seconds: float = 0.0,
    ) -> bool:
        """
        Re-queue a job back to 'queued', but ONLY if this worker still owns it.

        Returns True  → job re-queued successfully.
        Returns False → we no longer own the job; nothing was changed.
        """
        scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=retry_after_seconds)
        async with self._session() as session:
            async with session.begin():
                result = await session.execute(
                    text("""
                        UPDATE processing_jobs
                        SET status       = 'queued',
                            worker_id    = NULL,
                            locked_until = NULL,
                            scheduled_at = :scheduled_at
                        WHERE id        = :job_id
                          AND worker_id = :worker_id
                        RETURNING id
                    """),
                    {
                        "job_id": job_id,
                        "worker_id": worker_id,
                        "scheduled_at": scheduled_at,
                    },
                )
                return result.fetchone() is not None

    async def mark_completed(self, job_id: int) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(ProcessingJobORM)
                    .where(ProcessingJobORM.id == job_id)
                    .values(
                        status=JobStatus.COMPLETED.value,
                        finished_at=datetime.now(timezone.utc),
                        locked_until=None,
                    )
                )

    async def mark_failed(self, job_id: int, error: str) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(ProcessingJobORM)
                    .where(ProcessingJobORM.id == job_id)
                    .values(
                        status=JobStatus.FAILED.value,
                        error_message=error,
                        finished_at=datetime.now(timezone.utc),
                        locked_until=None,
                    )
                )

    async def mark_dead(self, job_id: int, error: str) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(ProcessingJobORM)
                    .where(ProcessingJobORM.id == job_id)
                    .values(
                        status=JobStatus.DEAD.value,
                        error_message=error,
                        finished_at=datetime.now(timezone.utc),
                        locked_until=None,
                    )
                )

    async def reset_job(self, request_id: int) -> None:
        """Reset a job back to queued status (for client-driven re-enqueueing of a failed prompt)."""
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(ProcessingJobORM)
                    .where(ProcessingJobORM.prompt_request_id == request_id)
                    .values(
                        status=JobStatus.QUEUED.value,
                        worker_id=None,
                        attempt_count=0,
                        locked_until=None,
                        last_heartbeat_at=None,
                        started_at=None,
                        finished_at=None,
                        error_message=None,
                        scheduled_at=datetime.now(timezone.utc),
                    )
                )

    async def reap_expired_leases(self) -> list[int]:
        """
        Reclaim all jobs whose lease has expired.
        Also syncs prompt_requests.status so GET /result reflects reality.
        Returns list of reclaimed job IDs.
        """
        now = datetime.now(timezone.utc)
        async with self._session() as session:
            async with session.begin():
                # Re-queue retryable jobs and sync request status
                result = await session.execute(
                    text("""
                        WITH reclaimed AS (
                            UPDATE processing_jobs
                            SET status       = 'queued',
                                worker_id    = NULL,
                                locked_until = NULL
                            WHERE status = 'processing'
                              AND locked_until < :now
                              AND attempt_count < max_attempts
                            RETURNING id, prompt_request_id
                        )
                        UPDATE prompt_requests pr
                        SET status = 'queued', updated_at = :now
                        FROM reclaimed r
                        WHERE pr.id = r.prompt_request_id
                        RETURNING r.id
                    """),
                    {"now": now},
                )
                reclaimed_ids = [r[0] for r in result.fetchall()]

                # Mark dead — exceeded max_attempts
                await session.execute(
                    text("""
                        WITH dead AS (
                            UPDATE processing_jobs
                            SET status = 'dead', finished_at = :now
                            WHERE status = 'processing'
                              AND locked_until < :now
                              AND attempt_count >= max_attempts
                            RETURNING prompt_request_id, error_message
                        )
                        UPDATE prompt_requests pr
                        SET status = 'failed', updated_at = :now,
                            error = COALESCE(d.error_message, 'Max attempts exceeded')
                        FROM dead d
                        WHERE pr.id = d.prompt_request_id
                    """),
                    {"now": now},
                )
                return reclaimed_ids

    # ── SHOULD FIX #4 — Backpressure: queue depth check ──────────────────────
    async def queued_depth(self) -> int:
        """
        Return the total number of jobs currently in-flight (queued + processing).
        Used by PromptProcessor to reject new work when the system is overloaded.

        Counting 'processing' in addition to 'queued' prevents a thundering-herd
        scenario where all in-flight processing jobs finish simultaneously and
        briefly show depth=0, allowing a burst of new inserts before the count
        catches up.
        """
        async with self._session() as session:
            result = await session.execute(
                text(
                    "SELECT COUNT(*) FROM processing_jobs "
                    "WHERE status IN ('queued', 'processing')"
                )
            )
            return result.scalar_one()
