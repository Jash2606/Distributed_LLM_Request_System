"""
PromptPipeline — Facade over the worker-side processing stages.

Chain of Responsibility pattern:
  embed → cache_lookup → [HIT: short-circuit] → rate_limit → llm → cache_store → persist

Fixes applied in this version:
  MUST #3 — release_lease() now passes job.worker_id for ownership validation
  MUST #5 — asyncio.CancelledError is explicitly re-raised (not swallowed by
             broad except Exception) so graceful shutdown works correctly
"""

import asyncio
import time

import structlog

from src.db.listen_notify import ListenNotify
from src.db.repositories.processing_job import ProcessingJobRepository
from src.db.repositories.prompt_request import PromptRequestRepository
from src.exceptions import (
    RateLimitExceededError,
    is_retryable,
)
from src.models.enums import RequestStatus
from src.models.orm import ProcessingJobORM, PromptRequestORM
from src.observability import metrics
from src.services.cache import SemanticCacheService
from src.services.llm_mock import ILLMProvider
from src.services.ratelimit import IRateLimiter

log = structlog.get_logger(__name__)


class PromptPipeline:
    def __init__(
        self,
        cache: SemanticCacheService,
        rate_limiter: IRateLimiter,
        llm: ILLMProvider,
        request_repo: PromptRequestRepository,
        job_repo: ProcessingJobRepository,
        notifier: ListenNotify,
    ):
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._llm = llm
        self._request_repo = request_repo
        self._job_repo = job_repo
        self._notifier = notifier

    async def execute(self, job: ProcessingJobORM, req: PromptRequestORM) -> None:
        t_start = time.monotonic()
        log.info(
            "pipeline_start",
            prompt_id=req.prompt_id,
            job_id=job.id,
            attempt=job.attempt_count,
        )

        # MUST FIX #5 — asyncio.CancelledError must NOT be caught by the broad
        # `except Exception` below.
        #
        # WHY THIS IS DANGEROUS:
        #   In Python 3.8+, asyncio.CancelledError inherits from BaseException, so
        #   `except Exception` should not catch it in theory.  But there are two
        #   real problems with the old code:
        #     a) In Python < 3.8, CancelledError inherited from Exception — old habit.
        #     b) Even in 3.8+, any async library that wraps exceptions could re-raise
        #        CancelledError as a different Exception type during cleanup.
        #     c) The _handle_failure() method calls job_repo.release_lease() — this
        #        would re-queue a job that should simply be released for the reaper,
        #        turning a clean shutdown into a retry storm of re-queued jobs.
        #
        # HOW THE FIX WORKS:
        #   The explicit `except asyncio.CancelledError: raise` before the general
        #   handler ensures cancellation always propagates.  The caller (WorkerPool)
        #   catches it at the pipeline_task level and handles cleanup correctly.
        try:
            await self._run(job, req, t_start)
        except asyncio.CancelledError:
            # Cancellation from HeartbeatManager (lease lost) or shutdown signal.
            # Do NOT retry, do NOT release_lease.  The job remains in-flight;
            # the reaper will reclaim it after the lease expires.
            log.info(
                "pipeline_cancelled",
                prompt_id=req.prompt_id,
                job_id=job.id,
                reason="lease_lost_or_shutdown",
            )
            raise  # propagate — WorkerPool._worker_loop handles this
        except Exception as exc:
            await self._handle_failure(job, req, exc)

    async def _run(
        self,
        job: ProcessingJobORM,
        req: PromptRequestORM,
        t_start: float,
    ) -> None:
        # Stage 1 — cache lookup (generates embedding internally)
        t_cache = time.monotonic()
        hit, embedding = await self._cache.lookup(req.text)
        metrics.processing_latency.labels(stage="cache").observe(time.monotonic() - t_cache)

        if hit is not None:
            metrics.cache_hits_total.labels(match_type=hit.match_type).inc()
            log.info(
                "cache_hit",
                prompt_id=req.prompt_id,
                similarity=round(hit.similarity, 4),
                match_type=hit.match_type,
            )
            await self._finish(job, req, hit.entry.response, cached=True, embedding=embedding)
            metrics.processing_latency.labels(stage="total").observe(time.monotonic() - t_start)
            metrics.prompt_requests_total.labels(status="completed", cached="true").inc()
            return

        metrics.cache_misses_total.inc()

        # Stage 2 — rate-limit check (only paid on cache miss)
        if not await self._rate_limiter.try_acquire():
            wait = self._rate_limiter.wait_time_seconds()
            metrics.rate_limit_denials_total.inc()
            log.info("rate_limit_deferred", prompt_id=req.prompt_id, wait_s=wait)
            # MUST FIX #3: pass job.worker_id so the UPDATE validates ownership.
            # Without it, a delayed call from a stale worker could reset another
            # worker's active job.
            await self._job_repo.release_lease(
                job.id, job.worker_id, retry_after_seconds=wait
            )
            return

        # Stage 3 — LLM call
        t_llm = time.monotonic()
        response = await self._llm.complete(req.text)
        metrics.processing_latency.labels(stage="llm").observe(time.monotonic() - t_llm)
        metrics.llm_calls_total.labels(outcome="success").inc()

        # Stage 4 — store in cache + finish
        await self._cache.store(req.prompt_id, req.text, embedding, response)
        await self._finish(job, req, response, cached=False, embedding=embedding)
        metrics.processing_latency.labels(stage="total").observe(time.monotonic() - t_start)
        metrics.prompt_requests_total.labels(status="completed", cached="false").inc()
        log.info("pipeline_complete", prompt_id=req.prompt_id, job_id=job.id)

    async def _finish(
        self,
        job: ProcessingJobORM,
        req: PromptRequestORM,
        response: str,
        cached: bool,
        embedding: list,
    ) -> None:
        await self._request_repo.mark_completed(req.id, response, cached, embedding)
        await self._job_repo.mark_completed(job.id)
        metrics.job_attempts_histogram.observe(job.attempt_count)

    async def _handle_failure(
        self,
        job: ProcessingJobORM,
        req: PromptRequestORM,
        exc: Exception,
    ) -> None:
        metrics.llm_calls_total.labels(outcome="failure").inc()
        log.warning(
            "pipeline_error",
            prompt_id=req.prompt_id,
            job_id=job.id,
            attempt=job.attempt_count,
            error=str(exc),
            retryable=is_retryable(exc),
        )

        # Rate-limit deferral — put back in queue with wait time.
        # MUST FIX #3: worker_id ownership check in release_lease.
        if isinstance(exc, RateLimitExceededError):
            await self._job_repo.release_lease(
                job.id, job.worker_id, retry_after_seconds=exc.wait_seconds
            )
            return

        # Non-retryable or max attempts reached — mark dead permanently.
        if not is_retryable(exc) or not job.can_retry():
            await self._job_repo.mark_dead(job.id, str(exc))
            await self._request_repo.mark_failed(req.id, str(exc), req.retry_count + 1)
            metrics.prompt_requests_total.labels(status="failed", cached="false").inc()
            log.error(
                "pipeline_dead",
                prompt_id=req.prompt_id,
                job_id=job.id,
                attempts=job.attempt_count,
                error=str(exc),
            )
            return

        # Retryable — re-queue with exponential backoff so another worker picks it up.
        # MUST FIX #3: worker_id ownership check prevents stale workers from
        # re-queuing jobs that have already been claimed by a new worker.
        backoff = min(30.0, 2.0 ** (job.attempt_count - 1))  # 1s, 2s, 4s, 8s, 16s, 30s
        released = await self._job_repo.release_lease(
            job.id, job.worker_id, retry_after_seconds=backoff
        )
        if not released:
            # The lease was already reclaimed by the reaper or another worker.
            # Do nothing — the new owner is responsible for this job.
            log.info(
                "pipeline_retry_skipped_lease_gone",
                prompt_id=req.prompt_id,
                job_id=job.id,
            )
            return

        await self._request_repo.update_status(
            req.id,
            RequestStatus.QUEUED,
            retry_count=req.retry_count + 1,
            error=str(exc),
        )
        # Wake workers immediately so they don't wait for their 2-second poll.
        try:
            await self._notifier.notify("job_queued", f"retry:{req.prompt_id}")
        except Exception:
            pass  # Non-fatal: workers will pick it up on the next 2-second poll
        log.info(
            "pipeline_retry_scheduled",
            prompt_id=req.prompt_id,
            job_id=job.id,
            attempt=job.attempt_count,
            backoff_s=backoff,
        )
