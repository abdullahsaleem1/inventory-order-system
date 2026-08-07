"""
Global exception handlers.

Converts every kind of failure into the single standardized error envelope
defined in src/shared/exceptions. Registered once in src/main.py:

    AppError / HTTPException / RequestValidationError / Exception -> ErrorResponse JSON

The envelope (error.code, error.message, status, request_id, path, timestamp)
is identical for 400, 401, 403, 404, 409, 422, 500 and beyond, so API clients
can parse every error the same way.
"""
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.core.logging_config import get_logger
from src.shared.exceptions import AppError

logger = get_logger(__name__)

# Map bare HTTP status codes to stable machine-readable error codes. Auth
# endpoints override these with more specific codes (e.g. invalid_grant).
_STATUS_TO_CODE = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    409: "CONFLICT",
    422: "UNPROCESSABLE_ENTITY",
    429: "TOO_MANY_REQUESTS",
    500: "INTERNAL_ERROR",
    503: "SERVICE_UNAVAILABLE",
}


def _envelope(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    details: object = None,
) -> dict:
    request_id = getattr(request.state, "request_id", None)
    return {
        "error": {"code": code, "message": message, "details": details},
        "status": status_code,
        "request_id": request_id,
        "path": request.url.path,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_envelope(
                request,
                status_code=exc.status_code,
                code=exc.code,
                message=exc.message,
                details=exc.details,
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        message = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
        return JSONResponse(
            status_code=exc.status_code,
            content=_envelope(
                request,
                status_code=exc.status_code,
                code=_STATUS_TO_CODE.get(exc.status_code, f"HTTP_{exc.status_code}"),
                message=message,
            ),
            headers=exc.headers,  # e.g. WWW-Authenticate: Bearer from OAuth2PasswordBearer
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=_envelope(
                request,
                status_code=422,
                code="VALIDATION_ERROR",
                message="Request validation failed",
                details=exc.errors(),
            ),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        # Log the real exception with its stack trace; the client only ever
        # sees the generic envelope so internals never leak.
        logger.exception(
            "unhandled_exception",
            extra={"path": request.url.path, "method": request.method, "error_type": type(exc).__name__},
        )
        return JSONResponse(
            status_code=500,
            content=_envelope(
                request,
                status_code=500,
                code="INTERNAL_ERROR",
                message="An unexpected error occurred",
            ),
        )
