"""
HeartbeatManager — periodically extends the job lease while a worker processes.

MUST FIX #2 applied here:
  - extend_lease() now returns bool (was void).
  - start() accepts worker_id and pipeline_task.
  - If the lease is lost (reaper reclaimed the job), the pipeline_task is
    cancelled immediately to prevent double-processing.
"""

import asyncio

import structlog

from src.db.repositories.processing_job import ProcessingJobRepository

log = structlog.get_logger(__name__)


class HeartbeatManager:
    def __init__(
        self,
        job_repo: ProcessingJobRepository,
        lease_seconds: int,
        interval: int,
    ):
        self._job_repo = job_repo
        self._lease_seconds = lease_seconds
        self._interval = interval
        self._task: asyncio.Task | None = None
        self._worker_id: str | None = None
        self._pipeline_task: asyncio.Task | None = None

    def start(
        self,
        job_id: int,
        worker_id: str,
        pipeline_task: asyncio.Task,
    ) -> None:
        """
        Start the heartbeat for `job_id`.

        `worker_id`     — used in the ownership-validated UPDATE.
        `pipeline_task` — cancelled immediately if the lease is detected as lost,
                          preventing the original worker from completing a job
                          that the reaper already handed to another worker.

        WHY WE CANCEL THE PIPELINE TASK (not current_task()):
          If we cancelled asyncio.current_task() (the worker slot coroutine), the
          entire _worker_loop() would die and the slot would never process another
          job.  By creating the pipeline as a separate Task in WorkerPool and
          passing it here, we cancel ONLY the pipeline, not the enclosing loop —
          the slot continues and picks up the next job after cleanup.
        """
        self._worker_id = worker_id
        self._pipeline_task = pipeline_task
        self._task = asyncio.create_task(self._run(job_id), name=f"heartbeat-{job_id}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self, job_id: int) -> None:
        while True:
            await asyncio.sleep(self._interval)

            try:
                # MUST FIX #2: extend_lease now validates worker ownership.
                # Returns False if the reaper already reclaimed this job.
                still_owner = await self._job_repo.extend_lease(
                    job_id, self._worker_id, self._lease_seconds
                )
            except asyncio.CancelledError:
                # Heartbeat task itself was cancelled (worker shutdown). Stop cleanly.
                break
            except Exception as exc:
                # Transient DB error — log and keep retrying.
                # DO NOT cancel the pipeline on a transient error; we can't
                # distinguish "DB is slow" from "lease is gone" without a successful
                # query.  The reaper will sort out true expiry.
                log.warning("heartbeat_error", job_id=job_id, error=str(exc))
                continue

            if not still_owner:
                # The reaper reclaimed the job (lease expired) OR another worker
                # claimed it.  Either way, we must stop processing immediately
                # to prevent double-completion of the same job.
                log.warning(
                    "lease_lost",
                    job_id=job_id,
                    worker_id=self._worker_id,
                )
                if self._pipeline_task and not self._pipeline_task.done():
                    self._pipeline_task.cancel()
                break
