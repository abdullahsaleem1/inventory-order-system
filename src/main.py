"""
Application entrypoint. Wires together the Inventory, Orders and Identity
bounded contexts, structured logging, standardized error handling, and
system (health/ready) routes.
This file stays thin — actual logic lives in the bounded contexts.
"""
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from src.contexts.identity.api.routes import oauth2_router as identity_oauth2_router
from src.contexts.identity.api.routes import router as identity_router
from src.contexts.inventory.api.routes import router as inventory_router
from src.contexts.orders.api.routes import router as orders_router
from src.core.config import get_settings
from src.core.error_handlers import register_exception_handlers
from src.core.logging_config import configure_logging, get_logger
from src.core.middleware import RequestLoggingMiddleware
from src.core.ratelimit.middleware import RateLimitMiddleware
from src.core.ratelimit.provider import build_rate_limiter, close_rate_limiter, probe_rate_limiter
from src.core.system_routes import router as system_router
from src.shared.messaging.provider import build_event_publisher, close_event_publisher
from src.shared.readstore import build_read_store, close_read_store

configure_logging()
logger = get_logger(__name__)

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "Distributed inventory & order management system built with DDD "
        "bounded contexts, strict layered architecture, sliding-window "
        "refresh tokens, role-based access control (RBAC), a from-scratch "
        "OAuth2.0-compatible authorization server, and event-driven order "
        "creation via RabbitMQ (order.created -> durable queues -> consumer "
        "groups). v0.6 makes order creation fully asynchronous: POST /orders "
        "publishes an event instead of writing to the database synchronously."
    ),
    version="0.6.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Middleware order note (Week 9): RateLimitMiddleware runs INSIDE
# RequestLoggingMiddleware so every request — including 429s — is logged and
# carries a request_id, and rate-limit decisions happen before routing.
app.add_middleware(RateLimitMiddleware)
app.add_middleware(RequestLoggingMiddleware)
register_exception_handlers(app)

app.include_router(system_router)
app.include_router(identity_router)
app.include_router(identity_oauth2_router)
app.include_router(inventory_router)
app.include_router(orders_router)


def custom_openapi() -> dict:
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema["components"] = schema.get("components", {})
    schema["components"]["securitySchemes"] = {
        "OAuth2PasswordBearer": {
            "type": "oauth2",
            "flows": {
                "password": {
                    "tokenUrl": "/oauth/token",
                    "scopes": {},
                }
            },
            "description": (
                "OAuth2.0 Resource Owner Password Credentials grant. "
                "Use the 'Authorize' button above to obtain a Bearer token. "
                "Access tokens expire in 15 minutes; use the refresh token "
                "to obtain a new pair via POST /auth/refresh."
            ),
        }
    }
    schema["security"] = [{"OAuth2PasswordBearer": []}]
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi


@app.on_event("startup")
async def on_startup() -> None:
    build_event_publisher()
    build_read_store()
    build_rate_limiter()
    logger.info("application_startup", extra={"env": settings.ENV})


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await close_event_publisher()
    await close_read_store()
    await close_rate_limiter()
    logger.info("application_shutdown")