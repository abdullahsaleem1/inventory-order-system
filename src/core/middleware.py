"""
Request logging middleware.
Logs one structured JSON line per request: method, path, status code,
duration, and a request ID that's also echoed back in the response header
so it can be correlated with client-side logs / support tickets.

Week 10: the log line and the `X-Trace-Id` response header both carry the
OpenTelemetry trace id, so a support ticket containing a single trace id is
enough to pull the full distributed trace out of Jaeger. `X-Request-ID` keeps
its original meaning (caller-supplied or a fresh UUID) so the existing
`event.correlation_id` contract and its tests are unaffected.
"""
import time
import uuid
from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from src.core.logging_config import get_logger
from src.core.telemetry import current_trace_identifiers

logger = get_logger("http.access")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # Honor a caller-supplied X-Request-ID (enables cross-service
        # correlation, e.g. event.correlation_id in the message broker);
        # otherwise mint a fresh one.
        supplied_request_id = request.headers.get("X-Request-ID", "").strip()[:128]
        request_id = supplied_request_id or str(uuid.uuid4())
        request.state.request_id = request_id
        start = time.perf_counter()

        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            status_code = response.status_code if response is not None else 500
            trace_id = current_trace_identifiers().trace_id
            if response is not None:
                response.headers["X-Request-ID"] = request_id
                if trace_id:
                    response.headers["X-Trace-Id"] = trace_id

            logger.info(
                "http_request",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "query_params": str(request.url.query),
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "client_ip": request.client.host if request.client else None,
                },
            )