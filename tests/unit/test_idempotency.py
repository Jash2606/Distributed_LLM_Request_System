import pytest
from unittest.mock import AsyncMock, MagicMock
from src.services.idempotency import IdempotencyService, IdempotencyAction
from src.exceptions import IdempotencyConflictError
from src.models.orm import PromptRequestORM
from src.services.cache import SemanticCacheService


def _make_repo(existing=None):
    repo = MagicMock()
    repo.get_by_prompt_id = AsyncMock(return_value=existing)
    return repo


def _hash(text: str) -> bytes:
    return SemanticCacheService.hash_text(text)


@pytest.mark.asyncio
async def test_new_prompt_inserts():
    svc = IdempotencyService(_make_repo(None))
    decision = await svc.check("p1", _hash("hello"))
    assert decision.action == IdempotencyAction.INSERT_NEW


@pytest.mark.asyncio
async def test_completed_returns_existing():
    row = PromptRequestORM(prompt_id="p1", text="hello", text_hash=_hash("hello"), status="completed")
    svc = IdempotencyService(_make_repo(row))
    decision = await svc.check("p1", _hash("hello"))
    assert decision.action == IdempotencyAction.RETURN_EXISTING
    assert decision.existing is row


@pytest.mark.asyncio
async def test_in_flight_awaits():
    for status in ("queued", "processing", "received"):
        row = PromptRequestORM(prompt_id="p1", text="x", text_hash=_hash("x"), status=status)
        svc = IdempotencyService(_make_repo(row))
        decision = await svc.check("p1", _hash("x"))
        assert decision.action == IdempotencyAction.AWAIT_EXISTING


@pytest.mark.asyncio
async def test_failed_reenqueues():
    row = PromptRequestORM(prompt_id="p1", text="x", text_hash=_hash("x"), status="failed")
    svc = IdempotencyService(_make_repo(row))
    decision = await svc.check("p1", _hash("x"))
    assert decision.action == IdempotencyAction.REENQUEUE


@pytest.mark.asyncio
async def test_conflict_raises():
    row = PromptRequestORM(prompt_id="p1", text="original", text_hash=_hash("original"), status="completed")
    svc = IdempotencyService(_make_repo(row))
    with pytest.raises(IdempotencyConflictError):
        await svc.check("p1", _hash("different text"))
