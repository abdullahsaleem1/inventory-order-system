"""
System-level routes: liveness and readiness checks.

/health  — liveness: is the process up? Never touches the DB. Used by
           orchestrators to decide whether to restart the container.
/ready   — readiness: can the service actually serve traffic right now?
           Checks the DB connection. Used by load balancers/orchestrators
           to decide whether to route traffic to this instance.
"""
from typing import Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logging_config import get_logger
from src.shared.infrastructure.database import get_db_session

router = APIRouter(tags=["System"])
logger = get_logger(__name__)


class HealthResponse(BaseModel):
    status: Literal["ok"]


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    database: Literal["up", "down"]


@router.get("/health", response_model=HealthResponse, summary="Liveness check")
async def health_check() -> HealthResponse:
    """Returns 200 as long as the process is running. Does not check dependencies."""
    return HealthResponse(status="ok")


@router.get("/ready", response_model=ReadinessResponse, summary="Readiness check")
async def readiness_check(
    response: Response,
    session: AsyncSession = Depends(get_db_session),
) -> ReadinessResponse:
    """Returns 200 only if the service can reach the database; 503 otherwise."""
    try:
        await session.execute(text("SELECT 1"))
        return ReadinessResponse(status="ready", database="up")
    except Exception:
        logger.exception("readiness_check_failed")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(status="not_ready", database="down")