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

    # --- Read store (CQRS read side, Weeks 7-8) ---
    # Left empty => single-database CQRS (read side shares the primary DB).
    # Set to a dedicated read-optimized DB to serve queries from a separate store.
    READ_DATABASE_URL: str | None = None

    # --- Dedicated read store (CQRS read phase, Week 8) ---
    # ORDER QUERY HANDLERS READ EXCLUSIVELY FROM THIS STORE. The sync worker
    # (scripts/read_projector.py) consumes events from the broker and upserts
    # denormalized documents here, giving eventual consistency. Elasticsearch
    # is the production store (docker-compose); "inmemory" is a zero-dependency
    # fallback used for local runs and the integration-test suite.
    READ_STORE_TYPE: str = "inmemory"  # "elasticsearch" | "inmemory"
    READ_STORE_URL: str | None = None  # e.g. http://elasticsearch:9200
    READ_STORE_INDEX: str = "orders"   # Elasticsearch index holding order documents

    # --- Redis (rate limiting, Week 9) ---
    REDIS_URL: str = "redis://localhost:6379/0"

    # --- Rate limiting (Week 9) ---
    # Token-bucket tiers per role. `capacity` = burst allowance; `rate` = tokens
    # (requests) refilled per second. Anonymous (missing/invalid token) traffic
    # is keyed by client IP and uses the strictest tier.
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_BUCKET_TTL_SECONDS: int = 60      # idle buckets expire
    RATE_LIMIT_REDIS_TIMEOUT_SECONDS: float = 0.5  # per-command Redis timeout
    RATE_LIMIT_ANON_CAPACITY: int = 5
    RATE_LIMIT_ANON_RATE: float = 1.0
    RATE_LIMIT_CUSTOMER_CAPACITY: int = 10
    RATE_LIMIT_CUSTOMER_RATE: float = 2.0
    RATE_LIMIT_STAFF_CAPACITY: int = 20
    RATE_LIMIT_STAFF_RATE: float = 5.0
    RATE_LIMIT_MANAGER_CAPACITY: int = 50
    RATE_LIMIT_MANAGER_RATE: float = 10.0
    RATE_LIMIT_ADMIN_CAPACITY: int = 100
    RATE_LIMIT_ADMIN_RATE: float = 20.0

    # --- Auth / JWT ---
    # Minimum 32 bytes required for HS256 (RFC 7518 §3.2) — enforced below.
    JWT_SECRET_KEY: str = "dev-only-insecure-please-change-me-0123456789"
    JWT_ALGORITHM: str = "HS256"
    JWT_ISSUER: str = "inventory-order-system"
    JWT_AUDIENCE: str = "inventory-order-system-api"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # --- Message broker (RabbitMQ — event-driven pipeline, Week 5) ---
    # Leave unset to disable event publishing (POST /orders then returns 503).
    BROKER_URL: str | None = None  # e.g. amqp://guest:guest@localhost:5672/
    EVENT_EXCHANGE: str = "inventory.orders.events"  # durable topic exchange
    ORDER_CREATED_ROUTING_KEY: str = "order.created"
    ORDER_STATUS_CHANGED_ROUTING_KEY: str = "order.status.changed"
    ORDERS_PERSISTENCE_QUEUE: str = "orders.order-created.persistence"  # consumer group 1
    ORDERS_AUDIT_QUEUE: str = "orders.order-created.audit"              # consumer group 2
    ORDERS_INVENTORY_QUEUE: str = "orders.order-created.inventory"      # worker group (Week 6)
    ORDERS_READ_QUEUE: str = "orders.order-created.read"                # read-projector group (Week 8)
    DEAD_LETTER_EXCHANGE: str = "inventory.orders.dlx"  # dead-letter topic exchange
    CONSUMER_PREFETCH_COUNT: int = 10
    CONSUMER_MAX_RETRIES: int = 3
    # Exponential backoff for transient message failures (Week 6). On attempt
    # `n` (1-based) the message is parked for `base * 2^(n-1)` seconds in a
    # per-attempt retry queue before being re-delivered to the work queue.
    CONSUMER_BACKOFF_BASE_SECONDS: float = 1.0
    CONSUMER_BACKOFF_MAX_SECONDS: float = 60.0

    # --- Distributed tracing (OpenTelemetry, Week 10) ---
    # Master switch. When false the SDK is never installed, every
    # `tracer.start_as_current_span(...)` becomes a no-op context manager, and
    # the app behaves exactly as it did before tracing was added.
    OTEL_ENABLED: bool = True

    # service.name is what Jaeger groups traces by, so each process must
    # override the default. docker-compose sets this per service
    # (inventory-orders-api / order-persistence-consumer / inventory-worker /
    # read-projector); the fallback keeps local runs legible.
    OTEL_SERVICE_NAME: str = "inventory-orders-api"
    OTEL_SERVICE_VERSION: str = "0.7.0"

    # OTLP collector endpoint. Jaeger's all-in-one image accepts gRPC on 4317
    # and HTTP/protobuf on 4318. Leave BOTH empty to fall back to the console
    # exporter, which prints spans as JSON to stdout — handy with no collector
    # running, and what the test suite uses.
    OTEL_EXPORTER_OTLP_ENDPOINT: str | None = None
    OTEL_EXPORTER_OTLP_PROTOCOL: str = "grpc"  # "grpc" | "http/protobuf"
    # Seconds between span exports. Lower it in tests so assertions don't race
    # the BatchSpanProcessor's 5s default flush interval.
    OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS: int = 5

    # Force the console exporter even when an OTLP endpoint is configured.
    # Used by scripts/capture_trace.py to prove spans are being produced
    # without needing a collector.
    OTEL_CONSOLE_EXPORTER: bool = False

    # Sampling. parentbased_always_on keeps 100% of traces (including any
    # sampled by an upstream service). parentbased_traceidratio samples a
    # fraction of root traces; switch to it for load runs.
    OTEL_TRACES_SAMPLER: str = "parentbased_always_on"
    OTEL_TRACES_SAMPLER_ARG: str = "1.0"

    # Propagate trace context over the message broker. The W3C traceparent /
    # tracestate headers are written into the AMQP message headers, which is
    # what stitches the API's trace to the worker's trace in Jaeger.
    OTEL_PROPAGATE_OVER_BROKER: bool = True

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
