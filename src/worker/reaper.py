"""Reaper — reclaims jobs whose lease expired due to a worker crash."""

import asyncio

import structlog

from src.db.repositories.processing_job import ProcessingJobRepository
from src.db.listen_notify import ListenNotify

log = structlog.get_logger(__name__)


class Reaper:
    def __init__(
        self,
        job_repo: ProcessingJobRepository,
        notifier: ListenNotify,
        interval_seconds: int = 5,
    ):
        self._job_repo = job_repo
        self._notifier = notifier
        self._interval = interval_seconds

    async def run(self) -> None:
        log.info("reaper_started", interval_s=self._interval)
        while True:
            try:
                await asyncio.sleep(self._interval)
                reclaimed = await self._job_repo.reap_expired_leases()
                if reclaimed:
                    log.info("reaper_reclaimed", job_ids=reclaimed, count=len(reclaimed))
                    # Wake up workers to claim the reclaimed jobs
                    await self._notifier.notify("job_queued", "reaper")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("reaper_error", error=str(exc))
