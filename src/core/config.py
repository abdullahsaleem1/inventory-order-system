"""
Centralized application configuration.
Loaded once and reused everywhere via `get_settings()`.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- App ---
    APP_NAME: str = "Inventory & Order Management System"
    ENV: str = "development"
    DEBUG: bool = True

    # --- Database (write model) ---
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/inventory_orders"

    # --- Read store (CQRS read side) — filled in during CQRS week ---
    READ_DATABASE_URL: str | None = None

    # --- Redis (rate limiting, caching) ---
    REDIS_URL: str = "redis://localhost:6379/0"

    # --- Auth / JWT — filled in during auth week ---
    JWT_SECRET_KEY: str = "changeme-dev-secret"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # --- Message broker — filled in during event-driven pipeline week ---
    BROKER_URL: str | None = None  # e.g. amqp://guest:guest@localhost:5672/ or Kafka bootstrap servers

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
