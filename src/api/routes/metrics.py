"""C (Controller) — GET /metrics (Prometheus format, stretch goal)."""

from fastapi import APIRouter
from fastapi.responses import Response

from src.observability.metrics import get_metrics_output

router = APIRouter()


@router.get("/metrics")
async def metrics_endpoint() -> Response:
    data, content_type = get_metrics_output()
    return Response(content=data, media_type=content_type)
