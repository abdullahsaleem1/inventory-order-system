"""
Application entrypoint. Wires together the Inventory, Orders and Identity
bounded contexts, structured logging, standardized error handling, and
system (health/ready) routes.
This file stays thin — actual logic lives in the bounded contexts.
"""
from fastapi import FastAPI

from src.contexts.identity.api.routes import oauth2_router as identity_oauth2_router
from src.contexts.identity.api.routes import router as identity_router
from src.contexts.inventory.api.routes import router as inventory_router
from src.contexts.orders.api.routes import router as orders_router
from src.core.config import get_settings
from src.core.error_handlers import register_exception_handlers
from src.core.logging_config import configure_logging, get_logger
from src.core.middleware import RequestLoggingMiddleware
from src.core.system_routes import router as system_router

configure_logging()
logger = get_logger(__name__)

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description="Distributed inventory & order management system — DDD, CQRS, event-driven.",
    version="0.3.0",
)

app.add_middleware(RequestLoggingMiddleware)
register_exception_handlers(app)

app.include_router(system_router)
app.include_router(identity_router)
app.include_router(identity_oauth2_router)
app.include_router(inventory_router)
app.include_router(orders_router)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("application_startup", extra={"env": settings.ENV})


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("application_shutdown")