"""C (Controller) — GET /result/{prompt_id} — polling fallback for async clients."""

from fastapi import APIRouter, HTTPException, Request, status

from src.api.schemas import PromptResponse

router = APIRouter()


@router.get("/result/{prompt_id}", response_model=PromptResponse)
async def get_result(prompt_id: str, request: Request) -> PromptResponse:
    repo = request.app.state.request_repo
    row = await repo.get_by_prompt_id(prompt_id)

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="prompt_id not found")

    return PromptResponse(
        user_id=row.user_id,
        prompt_id=row.prompt_id,
        status=row.status,
        cached=row.cached,
        response=row.response,
        error=row.error,
        retry_count=row.retry_count,
        processing_time_ms=row.processing_time_ms(),
    )
