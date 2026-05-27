"""HealthChecker — probes DB, Redis, and worker liveness."""

from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker


class HealthChecker:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        redis_client: aioredis.Redis,
        worker_heartbeat_threshold_seconds: int = 30,
    ):
        self._session_factory = session_factory
        self._redis = redis_client
        self._threshold = worker_heartbeat_threshold_seconds

    async def check(self) -> dict:
        db_status = await self._check_db()
        redis_status = await self._check_redis()
        worker_status = await self._check_worker()

        all_ok = all(s in ("connected", "running", "idle", "available")
                     for s in [db_status, redis_status, worker_status])
        return {
            "all_ok": all_ok,
            "database": db_status,
            "worker": worker_status,
            "cache": redis_status,
        }

    async def _check_db(self) -> str:
        try:
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))
            return "connected"
        except Exception:
            return "unavailable"

    async def _check_redis(self) -> str:
        try:
            await self._redis.ping()
            return "available"
        except Exception:
            return "unavailable"

    async def _check_worker(self) -> str:
        """Check if any worker has sent a heartbeat recently."""
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._threshold)
            async with self._session_factory() as session:
                result = await session.execute(
                    text("""
                        SELECT COUNT(*) FROM processing_jobs
                        WHERE status = 'processing'
                          AND last_heartbeat_at > :cutoff
                    """),
                    {"cutoff": cutoff},
                )
                active = result.scalar()
                # Also check if any worker is alive by looking at recent claims
                result2 = await session.execute(
                    text("""
                        SELECT COUNT(*) FROM processing_jobs
                        WHERE last_heartbeat_at > :cutoff
                    """),
                    {"cutoff": cutoff},
                )
                recent = result2.scalar()
                return "running" if recent > 0 else "idle"
        except Exception:
            return "unknown"
