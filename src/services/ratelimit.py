"""
Strategy pattern — IRateLimiter + RedisTokenBucketLimiter.

Token bucket implemented as an atomic Lua script so all worker replicas
share one global quota without race conditions.
"""

import time
from abc import ABC, abstractmethod

import redis.asyncio as aioredis

from src.exceptions import RedisUnavailableError

_LUA_TOKEN_BUCKET = """
local key          = KEYS[1]
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])
local now          = tonumber(ARGV[3])

local bucket     = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

-- Refill proportional to elapsed time
tokens = math.min(capacity, tokens + (now - last_refill) * refill_rate)

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 120)
    return 1
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 120)
    return 0
end
"""

_BUCKET_KEY = "llm:ratelimit:global"


class IRateLimiter(ABC):
    @abstractmethod
    async def try_acquire(self) -> bool:
        ...

    @abstractmethod
    def wait_time_seconds(self) -> float:
        ...


class RedisTokenBucketLimiter(IRateLimiter):
    def __init__(
        self,
        redis_client: aioredis.Redis,
        capacity: int = 300,
        refill_per_sec: float = 5.0,
    ):
        self._redis = redis_client
        self._capacity = capacity
        self._refill_per_sec = refill_per_sec
        self._script = self._redis.register_script(_LUA_TOKEN_BUCKET)

    async def try_acquire(self) -> bool:
        try:
            result = await self._script(
                keys=[_BUCKET_KEY],
                args=[self._capacity, self._refill_per_sec, time.time()],
            )
            return bool(result)
        except Exception as exc:
            # Fail-closed: if Redis is unreachable, deny the call
            raise RedisUnavailableError(str(exc)) from exc

    def wait_time_seconds(self) -> float:
        """Approximate time until next token is available."""
        return 1.0 / self._refill_per_sec


class InMemoryTokenBucketLimiter(IRateLimiter):
    """Fake in-memory limiter for unit tests (no Redis required)."""

    def __init__(self, capacity: int = 300, refill_per_sec: float = 5.0):
        self._tokens = float(capacity)
        self._capacity = capacity
        self._refill_per_sec = refill_per_sec
        self._last_refill = time.monotonic()

    async def try_acquire(self) -> bool:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_sec)
        self._last_refill = now
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False

    def wait_time_seconds(self) -> float:
        return 1.0 / self._refill_per_sec
