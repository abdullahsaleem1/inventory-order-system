"""
Application entrypoint. Wires together the Inventory and Orders bounded
contexts, structured logging, and system (health/ready) routes.
This file stays thin — actual logic lives in the bounded contexts.
"""
from fastapi import FastAPI

from src.contexts.inventory.api.routes import router as inventory_router
from src.contexts.orders.api.routes import router as orders_router
from src.core.config import get_settings
from src.core.logging_config import configure_logging, get_logger
from src.core.middleware import RequestLoggingMiddleware
from src.core.system_routes import router as system_router

configure_logging()
logger = get_logger(__name__)

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description="Distributed inventory & order management system — DDD, CQRS, event-driven.",
    version="0.2.0",
)

app.add_middleware(RequestLoggingMiddleware)

app.include_router(system_router)
app.include_router(inventory_router)
app.include_router(orders_router)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("application_startup", extra={"env": settings.ENV})


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("application_shutdown")