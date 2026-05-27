"""Graceful shutdown — SIGTERM handler that lets in-flight jobs finish."""

import asyncio
import signal

import structlog

log = structlog.get_logger(__name__)


class ShutdownHandler:
    def __init__(self, shutdown_event: asyncio.Event, grace_seconds: int = 10):
        self._event = shutdown_event
        self._grace = grace_seconds

    def install(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._on_signal)

    def _on_signal(self) -> None:
        log.info("shutdown_signal_received", grace_seconds=self._grace)
        self._event.set()
