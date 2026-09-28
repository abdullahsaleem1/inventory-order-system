"""
Auto-instrumentation wiring (Week 10).

Each helper is independently idempotent and failure-tolerant. A missing optional
package, or an already-instrumented engine, must never stop the service from
booting — a broken trace backend is a monitoring problem, not an availability
problem.

What gets instrumented automatically vs. by hand:

| Layer                  | How                                          |
| ---------------------- | -------------------------------------------- |
| inbound HTTP           | `FastAPIInstrumentor` (server span)          |
| outbound HTTP (httpx)  | `FastAPIInstrumentor` (client spans)         |
| SQL / asyncpg          | `SQLAlchemyInstrumentor` per engine (`db.*`)  |
| log correlation        | `LoggingInstrumentor` + JSONFormatter        |
| RabbitMQ publish/consume| hand-written, in `shared/messaging`           |
| CQRS command/query     | hand-written, in `shared/cqrs`               |
| Elasticsearch read store | hand-written, in `shared/readstore`         |
"""
from __future__ import annotations

from typing import Any

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from src.core.logging_config import get_logger

logger = get_logger("telemetry.instrumentation")

# `id()` of already-instrumented SQLAlchemy engines. A set of ids is safe here
# because a live engine is referenced by module globals for the process lifetime
# and is never garbage collected and reallocated under us.
_instrumented_engines: set[int] = set()

_app_instrumented = False
_logging_instrumented = False


def instrument_app(app: Any) -> None:
    """Add the ASGI server span around the whole FastAPI app.

    `instrument_app` prepends its middleware, making it the outermost layer, so
    the server span covers the rate limiter and request logging too — which is
    what you want when a request is rejected with a 429 before it reaches a
    route.
    """
    global _app_instrumented
    if _app_instrumented:
        return
    try:
        FastAPIInstrumentor.instrument_app(
            app,
            # Health probes fire every few seconds from the container runtime and
            # would otherwise dominate the trace volume.
            excluded_urls="health,ready",
        )
        _app_instrumented = True
        logger.info("otel_instrumented", extra={"layer": "fastapi"})
    except Exception as exc:
        logger.warning("otel_instrumentation_failed", extra={"layer": "fastapi", "error": str(exc)})


def instrument_engine(engine: Any, name: str = "sqlalchemy") -> None:
    """Add `db.*` client spans for one SQLAlchemy engine.

    Takes the **sync** engine: SQLAlchemyInstrumentor patches `Engine` events,
    and the async engine's event target is `engine.sync_engine`. Safe to call
    for the same engine more than once.
    """
    target = getattr(engine, "sync_engine", engine)
    key = id(target)
    if key in _instrumented_engines:
        return
    try:
        SQLAlchemyInstrumentor().instrument(
            engine=target,
            # Annotates the emitted SQL with the active span id, so a slow query
            # in Postgres' own logs can be tied back to this trace.
            enable_commenter=True,
        )
        _instrumented_engines.add(key)
        logger.info("otel_instrumented", extra={"layer": "sqlalchemy", "engine": name})
    except Exception as exc:
        logger.warning(
            "otel_instrumentation_failed", extra={"layer": "sqlalchemy", "error": str(exc)}
        )


def instrument_logging() -> None:
    """Stamp the active trace/span ids onto every `LogRecord`.

    `JSONFormatter` also reads them straight off the ambient context, so this is
    belt-and-braces: it makes the ids available to any *other* handler attached
    later (an OTLP log exporter, say) without changing the formatter.
    """
    global _logging_instrumented
    if _logging_instrumented:
        return
    try:
        LoggingInstrumentor().instrument(set_logging_format=False)
        _logging_instrumented = True
        logger.info("otel_instrumented", extra={"layer": "logging"})
    except Exception as exc:
        logger.warning("otel_instrumentation_failed", extra={"layer": "logging", "error": str(exc)})


def instrument_read_store() -> None:
    """Elasticsearch client spans are created by hand in the read store.

    The official `opentelemetry-instrumentation-elasticsearch` package only
    supports the *synchronous* client, and this system uses `AsyncElasticsearch`.
    Hand-written `CLIENT` spans in `ElasticsearchOrderReadStore` cover the same
    ground for ~15 lines and no extra dependency.
    """
    logger.debug("otel_instrumented", extra={"layer": "elasticsearch", "mode": "manual"})
