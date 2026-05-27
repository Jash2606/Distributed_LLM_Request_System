import asyncio
import pytest
from src.services.llm_mock import MockLLMProvider
from src.exceptions import LLMRandomFailureError, RateLimitExceededError


@pytest.mark.asyncio
async def test_returns_string():
    llm = MockLLMProvider(latency_min=0, latency_max=0, failure_rate=0)
    result = await llm.complete("hello")
    assert isinstance(result, str) and len(result) > 0


@pytest.mark.asyncio
async def test_deterministic_given_same_prompt():
    llm = MockLLMProvider(latency_min=0, latency_max=0, failure_rate=0)
    r1 = await llm.complete("same prompt")
    r2 = await llm.complete("same prompt")
    assert r1 == r2


@pytest.mark.asyncio
async def test_failure_rate_respected():
    llm = MockLLMProvider(latency_min=0, latency_max=0, failure_rate=1.0)
    with pytest.raises(LLMRandomFailureError):
        await llm.complete("fail me")


@pytest.mark.asyncio
async def test_rate_limit_enforced():
    llm = MockLLMProvider(latency_min=0, latency_max=0, failure_rate=0, rate_limit=2)
    await llm.complete("a")
    await llm.complete("b")
    with pytest.raises(RateLimitExceededError):
        await llm.complete("c")
