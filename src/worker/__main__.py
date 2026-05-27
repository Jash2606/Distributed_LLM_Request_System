"""Worker process entry point — composition root for the worker side."""

import asyncio

import redis.asyncio as aioredis
import structlog

from src.config import get_settings
from src.db.listen_notify import ListenNotify
from src.db.repositories.processing_job import ProcessingJobRepository
from src.db.repositories.prompt_request import PromptRequestRepository
from src.db.repositories.semantic_cache import SemanticCacheRepository
from src.db.session import build_engine, build_session_factory
from src.observability.logging import configure_logging
from src.services.cache import SemanticCacheService
from src.services.embeddings import embedding_provider_factory
from src.services.llm_mock import llm_provider_factory
from src.services.prompt_pipeline import PromptPipeline
from src.services.ratelimit import RedisTokenBucketLimiter
from src.worker.pool import WorkerPool
from src.worker.reaper import Reaper
from src.worker.shutdown import ShutdownHandler


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = structlog.get_logger("worker.main")
    log.info("worker_process_starting")

    # Infrastructure
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    redis_client = await aioredis.from_url(settings.redis_url, decode_responses=False)
    notifier = ListenNotify(settings.database_url)
    await notifier.connect()

    # Repositories
    request_repo = PromptRequestRepository(session_factory)
    job_repo = ProcessingJobRepository(session_factory)
    cache_repo = SemanticCacheRepository(session_factory)

    # Services
    embedder = embedding_provider_factory(settings)
    llm = llm_provider_factory(settings)
    rate_limiter = RedisTokenBucketLimiter(
        redis_client,
        capacity=settings.rate_limit_capacity,
        refill_per_sec=settings.rate_limit_refill_per_sec,
    )
    cache_service = SemanticCacheService(cache_repo, embedder, settings.similarity_threshold)

    # Orchestration
    pipeline = PromptPipeline(cache_service, rate_limiter, llm, request_repo, job_repo, notifier)

    # Shutdown coordination
    shutdown_event = asyncio.Event()
    ShutdownHandler(shutdown_event).install()

    pool = WorkerPool(
        pipeline=pipeline,
        job_repo=job_repo,
        notifier=notifier,
        concurrency=settings.worker_concurrency,
        lease_seconds=settings.lease_seconds,
        heartbeat_interval=settings.heartbeat_interval,
        shutdown_event=shutdown_event,
    )
    reaper = Reaper(job_repo, notifier, interval_seconds=settings.reaper_interval)

    log.info("worker_process_ready", concurrency=settings.worker_concurrency)

    try:
        await asyncio.gather(pool.run(), reaper.run())
    finally:
        await notifier.close()
        await redis_client.aclose()
        await engine.dispose()
        log.info("worker_process_stopped")


if __name__ == "__main__":
    asyncio.run(main())
