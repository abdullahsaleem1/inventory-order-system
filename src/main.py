"""
Application entrypoint. Wires together the Inventory and Orders bounded
contexts. Each context owns its own router — this file should stay thin.
"""
from fastapi import FastAPI

from src.contexts.inventory.api.routes import router as inventory_router
from src.contexts.orders.api.routes import router as orders_router
from src.core.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description="Distributed inventory & order management system — DDD, CQRS, event-driven.",
    version="0.1.0",
)

app.include_router(inventory_router)
app.include_router(orders_router)


@app.get("/health", tags=["System"])
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
