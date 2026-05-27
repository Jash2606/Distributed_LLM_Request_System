"""
C (Controller) — POST /process handler.

Delegates all business logic to PromptProcessor (Facade).
Translates ORM result → DTO response.
"""

from fastapi import APIRouter, HTTPException, Request, status

from src.api.schemas import PromptResponse, PromptSubmitRequest
from src.exceptions import IdempotencyConflictError, QueueFullError

router = APIRouter()


def _to_response(req_orm, user_id: str) -> PromptResponse:
    return PromptResponse(
        user_id=user_id,
        prompt_id=req_orm.prompt_id,
        status=req_orm.status,
        cached=req_orm.cached,
        response=req_orm.response,
        error=req_orm.error,
        retry_count=req_orm.retry_count,
        processing_time_ms=req_orm.processing_time_ms(),
    )


@router.post("/process", response_model=PromptResponse, status_code=status.HTTP_200_OK)
async def process_prompt(body: PromptSubmitRequest, request: Request) -> PromptResponse:
    processor = request.app.state.processor
    settings = request.app.state.settings

    # Resolve prompt_id: use the client-supplied value or auto-generate a UUID4.
    prompt_id = body.resolved_prompt_id()

    try:
        result = await processor.submit_and_wait(
            user_id=body.user_id,
            prompt_id=prompt_id,
            text=body.text,
            priority=body.priority,
            timeout_seconds=settings.api_timeout_seconds,
        )
    except QueueFullError as exc:
        # SHOULD FIX #4 — Backpressure: tell the client to back off.
        # Retry-After: 5 gives the client a concrete signal — wait 5 seconds
        # before retrying.  Without this header, aggressive clients would hammer
        # the endpoint and make overload worse.
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc),
            headers={"Retry-After": "5"},
        )
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )

    return _to_response(result, body.user_id)
