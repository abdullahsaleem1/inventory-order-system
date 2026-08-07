"""
Centralized application configuration.
Loaded once and reused everywhere via `get_settings()`.
"""
from functools import lru_cache
from pydantic import field_validator
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

    # --- Auth / JWT ---
    # Minimum 32 bytes required for HS256 (RFC 7518 §3.2) — enforced below.
    JWT_SECRET_KEY: str = "dev-only-insecure-please-change-me-0123456789"
    JWT_ALGORITHM: str = "HS256"
    JWT_ISSUER: str = "inventory-order-system"
    JWT_AUDIENCE: str = "inventory-order-system-api"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # --- Message broker — filled in during event-driven pipeline week ---
    BROKER_URL: str | None = None  # e.g. amqp://guest:guest@localhost:5672/ or Kafka bootstrap servers

    @field_validator("JWT_SECRET_KEY")
    @classmethod
    def validate_jwt_secret_length(cls, value: str) -> str:
        if len(value.encode("utf-8")) < 32:
            raise ValueError(
                "JWT_SECRET_KEY must be at least 32 bytes for HS256 "
                f"(got {len(value.encode('utf-8'))}); generate one with "
                "`python -c \"import secrets; print(secrets.token_urlsafe(48))\"`"
            )
        return value

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
