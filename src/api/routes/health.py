"""C (Controller) — GET /health"""

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from src.api.schemas import HealthComponentStatus, HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    checker = request.app.state.health_checker
    report = await checker.check()
    return HealthResponse(
        status="healthy" if report["all_ok"] else "degraded",
        timestamp=datetime.now(timezone.utc).isoformat(),
        components=HealthComponentStatus(
            database=report["database"],
            worker=report["worker"],
            cache=report["cache"],
        ),
    )
