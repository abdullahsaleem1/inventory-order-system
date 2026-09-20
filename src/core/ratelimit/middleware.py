"""
Rate-limit middleware (Week 9).

Runs for every request (except exempt system paths) and enforces tiered
token-bucket limits:

- Authenticated callers are keyed by their user id and limited by their role's
  tier (ADMIN gets the highest limits).
- Anonymous callers (missing/invalid token) are keyed by client IP and use the
  strictest tier.

When a bucket is empty the middleware returns 429 TOO_MANY_REQUESTS with the
standard error envelope plus ``Retry-After`` and ``X-RateLimit-*`` headers.
If Redis is down the limiter degrades to the in-process fallback and the
response is stamped ``X-RateLimit-Degraded: true`` — the API keeps serving.
"""
from datetime import datetime, timezone

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.core.config import get_settings
from src.core.logging_config import get_logger
from src.core.ratelimit.provider import get_rate_limiter

logger = get_logger("http.ratelimit")

# Paths that are never rate-limited (infrastructure / API discovery).
EXEMPT_PREFIXES = (
    "/health",
    "/ready",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/favicon.ico",
)

_RATE_LIMIT_HEADER_LIMIT = "X-RateLimit-Limit"
_RATE_LIMIT_HEADER_REMAINING = "X-RateLimit-Remaining"
_RATE_LIMIT_HEADER_RETRY_AFTER = "X-RateLimit-Retry-After"
_RATE_LIMIT_HEADER_DEGRADED = "X-RateLimit-Degraded"


def _exempt(path: str) -> bool:
    return any(path == prefix or path.startswith(prefix) for prefix in EXEMPT_PREFIXES)


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        limiter = get_rate_limiter()

        if _exempt(request.url.path) or not limiter.enabled:
            return await call_next(request)

        key, role = self._identity(request)
        result = await limiter.check(key, role)

        if not result.allowed:
            return self._rate_limited(request, result.retry_after_seconds, limiter.degraded)

        response = await call_next(request)
        self._stamp_headers(response, result.balance, limiter.degraded)
        return response

    @staticmethod
    def _identity(request: Request) -> tuple[str, str | None]:
        """Resolve the rate-limit key + role from the request. Tokens are decoded
        (signature + expiry) but the caller's session/DB state is NOT checked —
        the endpoint's own auth dependency does that. A missing/invalid token is
        treated as anonymous keyed by client IP."""
        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Bearer "):
            token = authorization[len("Bearer ") :].strip()
            claims = RateLimitMiddleware._decode_token(token)
            if claims is not None:
                user_id = str(claims.get("sub", ""))
                role = claims.get("role")
                if user_id:
                    return f"user:{user_id}", role
        client_host = request.client.host if request.client else "unknown"
        return f"ip:{client_host}", None

    @staticmethod
    def _decode_token(token: str) -> dict | None:
        settings = get_settings()
        try:
            from src.contexts.identity.infrastructure.jwt_service import JwtTokenService

            service = JwtTokenService(
                secret_key=settings.JWT_SECRET_KEY,
                algorithm=settings.JWT_ALGORITHM,
                issuer=settings.JWT_ISSUER,
                audience=settings.JWT_AUDIENCE,
                access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
            )
            return service.decode_access_token(token)
        except Exception:
            return None

    @staticmethod
    def _stamp_headers(response: Response, remaining: float, degraded: bool) -> None:
        response.headers[_RATE_LIMIT_HEADER_REMAINING] = str(max(0, int(remaining)))
        if degraded:
            response.headers[_RATE_LIMIT_HEADER_DEGRADED] = "true"

    @staticmethod
    def _rate_limited(request: Request, retry_after: float, degraded: bool) -> JSONResponse:
        if retry_after == float("inf"):
            # rate=0 buckets (e.g. tests) never refill; fall back to the bucket TTL.
            retry_after_secs = get_settings().RATE_LIMIT_BUCKET_TTL_SECONDS
        else:
            retry_after_secs = int(max(1.0, retry_after))
        request_id = getattr(request.state, "request_id", None)
        body = {
            "error": {
                "code": "TOO_MANY_REQUESTS",
                "message": f"Rate limit exceeded. Retry in {retry_after_secs} second(s).",
                "details": {"retry_after": retry_after_secs},
            },
            "status": 429,
            "request_id": request_id,
            "path": request.url.path,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        headers = {
            "Retry-After": str(retry_after_secs),
            _RATE_LIMIT_HEADER_LIMIT: "0",
            _RATE_LIMIT_HEADER_REMAINING: "0",
            _RATE_LIMIT_HEADER_RETRY_AFTER: str(retry_after_secs),
        }
        if degraded:
            headers[_RATE_LIMIT_HEADER_DEGRADED] = "true"
        return JSONResponse(status_code=429, content=body, headers=headers)