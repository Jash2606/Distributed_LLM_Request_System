"""
V (View) in MVC — Pydantic DTOs that define the external API contract.
These are the only types the client ever sees; ORM models never leak out.
"""

import hashlib
from typing import Optional
from pydantic import BaseModel, Field, field_validator


# ── Request DTOs ──────────────────────────────────────────────────────────────

class PromptSubmitRequest(BaseModel):
    user_id: str = Field(..., min_length=1, description="Unique user identifier")
    prompt_id: Optional[str] = Field(
        default=None,
        description=(
            "Unique prompt identifier. "
            "If omitted, derived deterministically from (user_id, text) so that "
            "repeated identical requests are idempotent without a client-managed ID."
        ),
    )
    text: str = Field(..., min_length=1, description="Prompt text to process")
    priority: str = Field("normal", description="Processing priority: high | normal | low")

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: str) -> str:
        if v not in ("high", "normal", "low"):
            raise ValueError("priority must be 'high', 'normal', or 'low'")
        return v

    def resolved_prompt_id(self) -> str:
        """
        Return the client-supplied prompt_id, or derive one deterministically.

        Deterministic rule: sha256(user_id + ":" + normalize(text))
        formatted as a UUID-like hex string prefixed with "auto-".

        Same user + same text → same ID every call → idempotency works out of
        the box without the client managing IDs.  Different users submitting the
        same text still get separate IDs (user-scoped).
        """
        if self.prompt_id:
            return self.prompt_id
        normalized_text = " ".join(self.text.lower().split())
        seed = f"{self.user_id}:{normalized_text}"
        h = hashlib.sha256(seed.encode()).hexdigest()
        # Format as UUID-like: 8-4-4-4-12
        return f"auto-{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


# ── Response DTOs ─────────────────────────────────────────────────────────────

class PromptResponse(BaseModel):
    user_id: str
    prompt_id: str
    status: str
    cached: Optional[bool] = None
    response: Optional[str] = None
    error: Optional[str] = None
    retry_count: Optional[int] = None
    processing_time_ms: Optional[int] = None


class HealthComponentStatus(BaseModel):
    database: str
    worker: str
    cache: str


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    components: HealthComponentStatus
