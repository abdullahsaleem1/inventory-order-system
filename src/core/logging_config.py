"""
Structured JSON logging.

Every log line is a single JSON object, making logs directly queryable by
log aggregators (ELK, Loki, CloudWatch Insights, etc.) instead of relying on
regex-parsed plaintext. Nothing in this codebase should use print() or the
bare logging string format — always go through get_logger().

Week 10 added `trace_id` / `span_id` / `trace_sampled` to every line. Those are
read from the ambient OpenTelemetry context, so a log line can be pivoted to its
span in Jaeger and vice versa without any call site having to pass anything
extra. Lines emitted outside a span (startup, shutdown, the DLQ inspection
tools) simply have no such keys rather than nulls.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from src.core.config import get_settings

# Fields on LogRecord that are already "standard" and shouldn't be duplicated
# when we merge in `extra={...}` kwargs.
_RESERVED_RECORD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName",
}


def _trace_fields() -> dict[str, Any]:
    """Active trace/span ids, or {} when no span is recording.

    Imported lazily and defensively: logging is initialised before tracing (and
    in processes that never turn tracing on), so this must never be the reason
    a service fails to boot.
    """
    try:
        from src.core.telemetry.propagation import current_trace_identifiers

        return current_trace_identifiers().as_log_fields()
    except Exception:  # pragma: no cover - telemetry unavailable/misconfigured
        return {}


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include any structured fields passed via logger.info("msg", extra={...})
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_ATTRS and not key.startswith("_"):
                payload[key] = value

        # Trace correlation. `setdefault` so an explicit extra={"trace_id": ...}
        # in a call site still wins.
        for key, value in _trace_fields().items():
            payload.setdefault(key, value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging() -> None:
    settings = get_settings()
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if settings.DEBUG else logging.INFO)

    # Remove any pre-existing handlers (e.g. uvicorn's default plaintext ones)
    # so we don't end up with duplicate or inconsistently formatted output.
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)

    # Route uvicorn's own loggers through the same JSON handler/format
    for uvicorn_logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(uvicorn_logger_name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)