"""
Idempotency service — enforces the five-case decision table from the design doc.

Case 1: Same prompt_id + same text + completed  → return existing result
Case 2: Same prompt_id + same text + in-flight  → caller awaits existing job
Case 3: Same prompt_id + same text + failed     → re-enqueue
Case 4: Same prompt_id + different text         → 409 Conflict
Case 5: New prompt_id                           → proceed with insert
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.db.repositories.prompt_request import PromptRequestRepository
from src.exceptions import IdempotencyConflictError
from src.models.enums import RequestStatus
from src.models.orm import PromptRequestORM
from src.services.cache import SemanticCacheService


class IdempotencyAction(str, Enum):
    INSERT_NEW = "insert_new"
    RETURN_EXISTING = "return_existing"
    AWAIT_EXISTING = "await_existing"
    REENQUEUE = "reenqueue"


@dataclass
class IdempotencyDecision:
    action: IdempotencyAction
    existing: Optional[PromptRequestORM] = None


class IdempotencyService:
    def __init__(self, repo: PromptRequestRepository):
        self._repo = repo

    async def check(self, prompt_id: str, text_hash: bytes) -> IdempotencyDecision:
        existing = await self._repo.get_by_prompt_id(prompt_id)

        if existing is None:
            return IdempotencyDecision(action=IdempotencyAction.INSERT_NEW)

        # Same prompt_id — check text hash
        if existing.text_hash != text_hash:
            raise IdempotencyConflictError(prompt_id)

        if existing.status == RequestStatus.COMPLETED:
            return IdempotencyDecision(
                action=IdempotencyAction.RETURN_EXISTING, existing=existing
            )

        if existing.status in (RequestStatus.RECEIVED, RequestStatus.QUEUED, RequestStatus.PROCESSING):
            return IdempotencyDecision(
                action=IdempotencyAction.AWAIT_EXISTING, existing=existing
            )

        # status == "failed" — allow re-enqueue 
        return IdempotencyDecision(
            action=IdempotencyAction.REENQUEUE, existing=existing
        )
