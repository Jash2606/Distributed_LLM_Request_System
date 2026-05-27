import asyncio
import pytest
from src.services.ratelimit import InMemoryTokenBucketLimiter


@pytest.mark.asyncio
async def test_acquire_within_capacity():
    limiter = InMemoryTokenBucketLimiter(capacity=5, refill_per_sec=100)
    for _ in range(5):
        assert await limiter.try_acquire() is True


@pytest.mark.asyncio
async def test_denied_when_exhausted():
    limiter = InMemoryTokenBucketLimiter(capacity=2, refill_per_sec=0.01)
    await limiter.try_acquire()
    await limiter.try_acquire()
    assert await limiter.try_acquire() is False


@pytest.mark.asyncio
async def test_refills_over_time():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_per_sec=100)
    await limiter.try_acquire()          # drain
    assert await limiter.try_acquire() is False
    await asyncio.sleep(0.02)            # ~2 tokens refilled at 100/s
    assert await limiter.try_acquire() is True


def test_wait_time_positive():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_per_sec=5)
    assert limiter.wait_time_seconds() == pytest.approx(0.2)
