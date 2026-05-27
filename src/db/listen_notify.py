"""
Thin asyncpg-based LISTEN/NOTIFY wrapper.

Design notes:
 - notify()               : short-lived connection per call (fire-and-forget).
 - wait_for_notification(): per-caller connection so concurrent API requests
                            don't race on a shared asyncpg connection.  asyncpg
                            does NOT allow concurrent operations on one connection,
                            so a shared connection caused "cannot perform operation:
                            another operation is in progress" under load.
 - prepared_listener()    : context manager that opens and adds the LISTEN *before*
                            the caller sends NOTIFY.  This eliminates the race where
                            the worker finishes and sends NOTIFY before the API has
                            even started listening, causing the notification to be
                            lost and forcing a 1-second poll delay.
 - Worker LISTEN (claimer): uses a single long-lived connection via connect()/
                            listen(), since only one goroutine drives it per slot.
"""

import asyncio
from contextlib import asynccontextmanager

import asyncpg


class ListenNotify:
    def __init__(self, database_url: str):
        # asyncpg uses postgres:// scheme, not postgresql+asyncpg://
        self._url = database_url.replace("postgresql+asyncpg://", "postgresql://")
        self._conn: asyncpg.Connection | None = None
        # Protect shared-connection add/remove_listener calls (used by workers only)
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self._conn = await asyncpg.connect(self._url)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def notify(self, channel: str, payload: str = "") -> None:
        """Issue NOTIFY via a fresh connection so it never blocks the shared conn."""
        conn = await asyncpg.connect(self._url)
        try:
            await conn.execute("SELECT pg_notify($1, $2)", channel, payload)
        finally:
            await conn.close()

    # ── Shared-connection listener API (used by workers) ──────────────────────

    async def listen(self, channel: str, callback) -> None:
        """Register callback on the shared persistent connection (worker use)."""
        async with self._lock:
            if self._conn is None:
                await self.connect()
            await self._conn.add_listener(channel, callback)

    async def unlisten(self, channel: str, callback) -> None:
        async with self._lock:
            if self._conn:
                await self._conn.remove_listener(channel, callback)

    # ── Per-request notification wait (used by API) ───────────────────────────

    async def wait_for_notification(self, channel: str, timeout: float) -> str | None:
        """
        Open a private connection, LISTEN, wait for one notification, then close.
        Using a private connection per waiter eliminates asyncpg's single-operation
        constraint — concurrent API requests no longer interfere with each other.
        """
        conn = await asyncpg.connect(self._url)
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()

        def _handler(_conn, _pid, _channel, payload):
            # asyncpg fires callbacks in the event-loop thread, so direct set is safe.
            # Guard done() to handle the case where wait_for already cancelled the future.
            if not future.done():
                future.set_result(payload)

        try:
            await conn.add_listener(channel, _handler)
            # No asyncio.shield — wait_for cancels the future directly on timeout,
            # which makes future.done() True so _handler skips set_result safely.
            return await asyncio.wait_for(future, timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return None
        finally:
            try:
                await conn.remove_listener(channel, _handler)
                await conn.close()
            except Exception:
                pass

    # ── SHOULD FIX #1 — Eliminate NOTIFY-before-LISTEN race ──────────────────
    #
    # WHY THIS IS NEEDED:
    #   Current race timeline (old code):
    #     1. API inserts request+job → sends NOTIFY job_queued
    #     2. Worker wakes immediately, processes job (cache hit: ~5 ms)
    #     3. Worker sends NOTIFY prompt_done_N
    #     4. API enters wait_for_notification() → opens connection → add_listener
    #        ↑ MISSED — step 3 happened before step 4 opened the connection
    #     5. 1-second poll loop eventually reads DB → returns after ~1 s delay
    #
    #   With prepared_listener:
    #     1. API inserts request+job → enters prepared_listener context
    #        → opens connection → add_listener (channel is LIVE)
    #     2. THEN API sends NOTIFY job_queued
    #     3. Worker processes job → sends NOTIFY prompt_done_N
    #        → _handler fires immediately → future.set_result(payload)
    #     4. wait() in the poll loop returns instantly
    #
    # HOW IT WORKS:
    #   asynccontextmanager opens a private asyncpg connection and registers the
    #   listener BEFORE yielding.  The yielded `wait(timeout)` callable waits on
    #   an asyncio.Future that is resolved by the asyncpg callback.
    #   asyncio.shield(future) prevents wait_for from cancelling the underlying
    #   future on a per-iteration timeout, so the future stays alive across
    #   multiple calls to wait() in the polling loop.
    #
    # REMAINING WINDOW:
    #   There is still a tiny window between asyncpg.connect() and add_listener()
    #   (~1 ms), but in practice the worker needs at minimum one DB round-trip
    #   before sending NOTIFY, which is >> the connection setup time.
    @asynccontextmanager
    async def prepared_listener(self, channel: str):
        """
        Context manager: establish LISTEN *before* the caller sends NOTIFY.

        Usage:
            async with notifier.prepared_listener("prompt_done_42") as wait:
                await notifier.notify("job_queued", ...)   # workers wake
                while deadline_not_reached:
                    await wait(timeout=1.0)                # near-instant wakeup
                    if job_done:
                        break

        The yielded `wait(timeout)` callable returns the notification payload
        if one arrives within the timeout, or None on timeout.  It is safe to
        call multiple times in a loop — asyncio.shield keeps the underlying
        Future alive across per-iteration timeouts.
        """
        conn = await asyncpg.connect(self._url)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()

        def _handler(_conn, _pid, _channel, payload: str) -> None:
            if not future.done():
                future.set_result(payload)

        try:
            await conn.add_listener(channel, _handler)
        except Exception:
            await conn.close()
            raise

        async def wait(timeout: float) -> str | None:
            # Fast path: notification already arrived (e.g. before first wait() call).
            if future.done():
                return future.result()
            try:
                # asyncio.shield prevents wait_for from cancelling the underlying
                # future on timeout.  The future stays alive for the next wait() call.
                return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                return None

        try:
            yield wait
        finally:
            try:
                await conn.remove_listener(channel, _handler)
                await conn.close()
            except Exception:
                pass
