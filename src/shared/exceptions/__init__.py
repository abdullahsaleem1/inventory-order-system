"""
Shared application error hierarchy.

Every error that escapes the layered stack as an HTTP error is an `AppError`
(with an HTTP status + machine-readable code) or one of the subclasses below.
Global exception handlers (see src/core/error_handlers.py) serialize ALL
errors — AppError, FastAPI's HTTPException, validation errors, and unexpected
exceptions — into the exact same JSON envelope:

    {
      "error":   {"code": "...", "message": "...", "details": ...},
      "status":  401,
      "request_id": "...",
      "path":    "...",
      "timestamp": "..."
    }

so 400/401/403/404/409/422/500 responses are structurally identical and the
client can rely on `error.code` for machine handling.
"""
from typing import Any


class AppError(Exception):
    """Base class for expected, client-visible application errors."""

    status_code: int = 500
    code: str = "INTERNAL_ERROR"

    def __init__(
        self,
        message: str | None = None,
        *,
        code: str | None = None,
        details: Any = None,
    ) -> None:
        self.message = message or self.code
        self.code = code or self.code
        self.details = details
        super().__init__(self.message)


class BadRequestError(AppError):
    status_code = 400
    code = "BAD_REQUEST"


class UnauthorizedError(AppError):
    status_code = 401
    code = "UNAUTHORIZED"


class ForbiddenError(AppError):
    status_code = 403
    code = "FORBIDDEN"


class NotFoundError(AppError):
    status_code = 404
    code = "NOT_FOUND"


class ConflictError(AppError):
    status_code = 409
    code = "CONFLICT"


class UnprocessableEntityError(AppError):
    status_code = 422
    code = "UNPROCESSABLE_ENTITY"
