"""
System-level routes: liveness and readiness checks.

/health  — liveness: is the process up? Never touches the DB. Used by
           orchestrators to decide whether to restart the container.
/ready   — readiness: can the service actually serve traffic right now?
           Checks the DB connection, the RabbitMQ broker, and the Redis
           rate-limiter (Redis is reported but does NOT fail readiness —
           the API degrades gracefully instead of crashing, see Week 9).
"""
from typing import Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logging_config import get_logger
from src.shared.infrastructure.database import get_db_session
from src.shared.messaging.provider import probe_broker
from src.core.ratelimit.provider import probe_rate_limiter

router = APIRouter(tags=["System"])
logger = get_logger(__name__)


class HealthResponse(BaseModel):
    status: Literal["ok"]


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    database: Literal["up", "down"]
    broker: Literal["up", "down", "disabled"]
    redis: Literal["up", "down", "disabled"]


@router.get("/health", response_model=HealthResponse, summary="Liveness check")
async def health_check() -> HealthResponse:
    """Returns 200 as long as the process is running. Does not check dependencies."""
    return HealthResponse(status="ok")


@router.get("/ready", response_model=ReadinessResponse, summary="Readiness check")
async def readiness_check(
    response: Response,
    session: AsyncSession = Depends(get_db_session),
) -> ReadinessResponse:
    """Returns 200 only if the DB and the event broker are reachable; 503 otherwise."""
    try:
        await session.execute(text("SELECT 1"))
        database = "up"
    except Exception:
        logger.exception("readiness_check_failed")
        database = "down"

    broker = await probe_broker()
    redis_status = await probe_rate_limiter()

    ready = database == "up" and broker != "down"
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(
        status="ready" if ready else "not_ready",
        database=database,
        broker=broker,
        redis=redis_status,
    )