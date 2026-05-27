"""
FastAPI application — composition root for the API side.
All dependencies are wired here in the lifespan; nothing is hard-coded in routers.
"""

from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI

from src.api.health_checker import HealthChecker
from src.api.routes import health, metrics, process, result
from src.config import get_settings
from src.db.listen_notify import ListenNotify
from src.db.repositories.processing_job import ProcessingJobRepository
from src.db.repositories.prompt_request import PromptRequestRepository
from src.db.repositories.semantic_cache import SemanticCacheRepository
from src.db.session import build_engine, build_session_factory
from src.observability.logging import configure_logging
from src.services.cache import SemanticCacheService
from src.services.embeddings import embedding_provider_factory
from src.services.idempotency import IdempotencyService
from src.services.prompt_processor import PromptProcessor
from src.services.ratelimit import RedisTokenBucketLimiter


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)

    # Infrastructure
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    redis_client = await aioredis.from_url(settings.redis_url, decode_responses=False)
    notifier = ListenNotify(settings.database_url)
    await notifier.connect()

    # Repositories (M in MVC)
    request_repo = PromptRequestRepository(session_factory)
    job_repo = ProcessingJobRepository(session_factory)
    cache_repo = SemanticCacheRepository(session_factory)

    # Services
    embedder = embedding_provider_factory(settings)
    rate_limiter = RedisTokenBucketLimiter(
        redis_client,
        capacity=settings.rate_limit_capacity,
        refill_per_sec=settings.rate_limit_refill_per_sec,
    )
    cache_service = SemanticCacheService(cache_repo, embedder, settings.similarity_threshold)
    idempotency = IdempotencyService(request_repo)

    # Facades
    processor = PromptProcessor(
        idempotency=idempotency,
        cache=cache_service,
        request_repo=request_repo,
        job_repo=job_repo,
        notifier=notifier,
        max_job_attempts=settings.max_job_attempts,
        max_queue_depth=settings.max_queue_depth,
    )

    # Health checker
    health_checker = HealthChecker(session_factory, redis_client)

    # Store on app.state — routers access via request.app.state
    app.state.settings = settings
    app.state.processor = processor
    app.state.request_repo = request_repo
    app.state.health_checker = health_checker

    yield

    await notifier.close()
    await redis_client.aclose()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="LLM Request Processor",
        description="Distributed LLM prompt processing with semantic caching",
        version="1.0.0",
        lifespan=lifespan,
    )
    # Register routers (C in MVC)
    app.include_router(process.router)
    app.include_router(result.router)
    app.include_router(health.router)
    app.include_router(metrics.router)
    return app


app = create_app()
