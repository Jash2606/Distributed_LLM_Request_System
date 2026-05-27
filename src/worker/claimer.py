"""
JobClaimer — acquires the next available job using LISTEN/NOTIFY wakeup
with a 2-second polling fallback for missed notifications.
"""

import asyncio
from typing import Optional

import structlog

from src.db.listen_notify import ListenNotify
from src.db.repositories.processing_job import ProcessingJobRepository
from src.models.orm import ProcessingJobORM, PromptRequestORM

log = structlog.get_logger(__name__)

_POLL_INTERVAL = 2.0   # fallback polling if notification is missed
_NOTIFY_CHANNEL = "job_queued"


class JobClaimer:
    def __init__(
        self,
        job_repo: ProcessingJobRepository,
        notifier: ListenNotify,
        worker_id: str,
        lease_seconds: int,
    ):
        self._job_repo = job_repo
        self._notifier = notifier
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds

    async def next_job(
        self, shutdown_event: asyncio.Event
    ) -> Optional[tuple[ProcessingJobORM, PromptRequestORM]]:
        """
        Block until a job is available or shutdown is requested.
        Uses LISTEN for low-latency wakeup + fallback poll every 2s.
        """
        while not shutdown_event.is_set():
            result = await self._job_repo.claim_next(self._worker_id, self._lease_seconds)
            if result is not None:
                return result

            # Wait for a notification or fall back to poll timeout.
            # wait_for_notification already has its own timeout — no outer wrap needed.
            try:
                await self._notifier.wait_for_notification(_NOTIFY_CHANNEL, timeout=_POLL_INTERVAL)
            except Exception:
                pass  # Timeout or transient error — just poll again

        return None
