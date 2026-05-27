"""
WorkerPool — runs N asyncio coroutines concurrently, each claiming and
executing one job at a time.

Fixes applied in this version:
  MUST #2  — Pipeline runs as a sub-Task so HeartbeatManager can cancel ONLY
              the pipeline (not the worker slot) on lease expiry.
  SHOULD #5 — asyncio.TaskGroup replaces asyncio.gather(..., return_exceptions=True).
              Unhandled worker exceptions now propagate instead of being swallowed.
"""

import asyncio
import uuid

import structlog

from src.db.listen_notify import ListenNotify
from src.db.repositories.processing_job import ProcessingJobRepository
from src.observability import metrics
from src.services.prompt_pipeline import PromptPipeline
from src.worker.claimer import JobClaimer
from src.worker.heartbeat import HeartbeatManager

log = structlog.get_logger(__name__)


class WorkerPool:
    def __init__(
        self,
        pipeline: PromptPipeline,
        job_repo: ProcessingJobRepository,
        notifier: ListenNotify,
        concurrency: int,
        lease_seconds: int,
        heartbeat_interval: int,
        shutdown_event: asyncio.Event,
    ):
        self._pipeline = pipeline
        self._job_repo = job_repo
        self._notifier = notifier
        self._concurrency = concurrency
        self._lease_seconds = lease_seconds
        self._heartbeat_interval = heartbeat_interval
        self._shutdown = shutdown_event

    # ── SHOULD FIX #5 — asyncio.TaskGroup replaces gather(return_exceptions=True) ──
    #
    # WHY gather(return_exceptions=True) IS DANGEROUS:
    #   asyncio.gather(..., return_exceptions=True) swallows ALL task exceptions
    #   into the return list.  If a worker slot raises an unhandled bug, the pool
    #   silently continues with N-1 workers, the error is invisible, and throughput
    #   degrades with no alert.
    #
    # HOW TaskGroup IMPROVES THIS:
    #   If any worker task raises an unhandled exception, TaskGroup cancels all
    #   sibling tasks and raises an ExceptionGroup.  The process exits with an
    #   error code, Docker Compose restarts it, and the bug is immediately visible
    #   in logs.  This is "fail fast" — better than silent degradation.
    #
    #   Normal shutdown path (shutdown_event set):
    #     Workers finish their current job → claimer.next_job() returns None
    #     → _worker_loop() exits normally → TaskGroup waits for all → run() returns.
    #   We no longer need `await self._shutdown.wait()` because the workers
    #   themselves observe the shutdown event and exit naturally.
    #
    # TRADEOFF:
    #   A single worker bug kills all workers.  For production, the correct fix is
    #   to ensure _worker_loop() handles all expected errors internally (it does —
    #   PromptPipeline.execute() catches domain errors).  Only a programming bug
    #   (unexpected exception) should escape, and for those, restarting is correct.
    async def run(self) -> None:
        log.info("worker_pool_starting", concurrency=self._concurrency)
        async with asyncio.TaskGroup() as tg:
            for i in range(self._concurrency):
                tg.create_task(self._worker_loop(i), name=f"worker-slot-{i}")
        log.info("worker_pool_stopped")

    async def _worker_loop(self, slot: int) -> None:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}-slot{slot}"
        claimer = JobClaimer(
            self._job_repo, self._notifier, worker_id, self._lease_seconds
        )
        log.info("worker_started", worker_id=worker_id)

        while not self._shutdown.is_set():
            result = await claimer.next_job(self._shutdown)
            if result is None:
                break

            job, req = result

            # MUST FIX #2 — Create the pipeline as a *sub-Task*.
            #
            # WHY A SUB-TASK (not direct await):
            #   HeartbeatManager needs to cancel processing if the lease is lost.
            #   If we `await self._pipeline.execute(job, req)` directly, the only
            #   way to stop it is to cancel the CURRENT task (_worker_loop), which
            #   would also kill the while loop — the slot would never process another
            #   job after lease expiry.
            #
            #   By wrapping in a sub-Task, we can call pipeline_task.cancel() from
            #   HeartbeatManager.  The `await pipeline_task` below receives the
            #   CancelledError from the sub-task, we catch and log it, and the
            #   while loop continues to the next job.
            #
            # NOTE: asyncio.CancelledError from the outer _worker_loop (shutdown)
            #   propagates through `await pipeline_task` ONLY if the outer task
            #   itself is cancelled — in that case pipeline_task is also cancelled
            #   automatically by the event loop cleanup.
            pipeline_task = asyncio.create_task(
                self._pipeline.execute(job, req),
                name=f"pipeline-{job.id}",
            )
            heartbeat = HeartbeatManager(
                self._job_repo, self._lease_seconds, self._heartbeat_interval
            )
            # Pass worker_id and the pipeline_task so heartbeat can cancel it on
            # lease expiry without killing the whole worker slot.
            heartbeat.start(job.id, worker_id, pipeline_task)
            metrics.worker_active_jobs.labels(worker_id=worker_id).inc()

            pipeline_aborted = False
            try:
                await pipeline_task
            except asyncio.CancelledError:
                # HeartbeatManager cancelled the pipeline because the lease was lost.
                # The job is still in-flight (or the reaper has already re-queued it).
                # DO NOT re-raise — continue the while loop for the next job.
                pipeline_aborted = True
                log.warning(
                    "job_aborted_lease_lost",
                    job_id=job.id,
                    worker_id=worker_id,
                )
            finally:
                await heartbeat.stop()
                metrics.worker_active_jobs.labels(worker_id=worker_id).dec()

                # Only notify API waiters on actual completion (success or domain failure).
                # If the pipeline was aborted (lease lost), DO NOT notify — the job will
                # be re-processed by another worker.  Premature notification would return
                # a stale / incomplete response to the waiting API client.
                if not pipeline_aborted:
                    try:
                        await self._notifier.notify(f"prompt_done_{req.id}", req.prompt_id)
                    except Exception:
                        pass  # Non-fatal: API poll loop falls back to DB read

        log.info("worker_stopped", worker_id=worker_id)
