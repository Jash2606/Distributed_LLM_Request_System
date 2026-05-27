"""
Strategy pattern — ILLMProvider + MockLLMProvider.

MockLLMProvider simulates realistic constraints:
  • 200–500 ms latency (randomised)
  • 5% random failure rate
  • Internal sliding-window guard (secondary safety net; primary cap is Redis)
"""

import asyncio
import hashlib
import random
import time
from abc import ABC, abstractmethod
from collections import deque

from src.exceptions import LLMRandomFailureError, LLMTimeoutError, RateLimitExceededError


class ILLMProvider(ABC):
    @abstractmethod
    async def complete(self, prompt: str) -> str:
        ...


class _SlidingWindowGuard:
    """Thread-safe in-process sliding-window rate limiter (secondary guard only)."""

    def __init__(self, limit: int, window_seconds: float):
        self._limit = limit
        self._window = window_seconds
        self._calls: deque[float] = deque()

    def try_acquire(self) -> bool:
        now = time.monotonic()
        cutoff = now - self._window
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()
        if len(self._calls) >= self._limit:
            return False
        self._calls.append(now)
        return True


class MockLLMProvider(ILLMProvider):
    def __init__(
        self,
        latency_min: float = 0.2,
        latency_max: float = 0.5,
        failure_rate: float = 0.05,
        rate_limit: int = 300,
    ):
        self._latency_min = latency_min
        self._latency_max = latency_max
        self._failure_rate = failure_rate
        self._guard = _SlidingWindowGuard(rate_limit, 60.0)

    async def complete(self, prompt: str) -> str:
        # Secondary guard — primary is Redis token bucket
        if not self._guard.try_acquire():
            raise RateLimitExceededError(wait_seconds=0.2)

        # Simulate realistic latency
        await asyncio.sleep(random.uniform(self._latency_min, self._latency_max))

        # 5% random failure
        if random.random() < self._failure_rate:
            raise LLMRandomFailureError("Mock LLM random failure")

        return self._generate_response(prompt)

    def _generate_response(self, prompt: str) -> str:
        digest = hashlib.md5(prompt.encode()).hexdigest()[:8]
        return f"Mock response for prompt [{digest}]: {prompt[:60]}..."


def llm_provider_factory(settings) -> ILLMProvider:
    return MockLLMProvider(
        latency_min=settings.llm_latency_min,
        latency_max=settings.llm_latency_max,
        failure_rate=settings.llm_failure_rate,
        rate_limit=settings.rate_limit_capacity,
    )
