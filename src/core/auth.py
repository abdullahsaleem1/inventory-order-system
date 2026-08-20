"""
Shared authentication and authorization dependencies.

Provides two reusable FastAPI dependencies used across bounded contexts:

  - ``get_current_user``  — resolves the current user from a Bearer access
    token. Validates signature, expiry, blacklist, and active status.
  - ``require_roles(*roles)`` — factory that returns a dependency requiring
    the authenticated user to hold one of the specified roles.

Usage in routes:

    from src.core.auth import get_current_user, require_roles

    @router.get("/admin-only", dependencies=[Depends(require_roles("ADMIN"))])
    async def admin_view(user: User = Depends(get_current_user)):
        ...
"""
from collections.abc import Callable

from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.contexts.identity.infrastructure.password_hasher import BcryptPasswordHasher
from src.contexts.identity.repositories.refresh_token_repository import (
    BlacklistedTokenRepository,
    RefreshTokenRepository,
)
from src.contexts.identity.repositories.user_repository import UserRepository
from src.contexts.identity.services.auth_service import AuthService, InactiveUserError
from src.core.config import get_settings
from src.shared.exceptions import ForbiddenError, UnauthorizedError
from src.shared.infrastructure.database import get_db_session

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth/token", auto_error=False)

settings = get_settings()


def _build_auth_service(session: AsyncSession) -> AuthService:
    return AuthService(
        repository=UserRepository(session),
        password_hasher=BcryptPasswordHasher(),
        token_service=JwtTokenService(
            secret_key=settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
            issuer=settings.JWT_ISSUER,
            audience=settings.JWT_AUDIENCE,
            access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
        ),
        refresh_token_repo=RefreshTokenRepository(session),
        blocklist_repo=BlacklistedTokenRepository(session),
    )


async def get_current_user(
    token: str | None = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_db_session),
) -> User:
    if token is None:
        raise UnauthorizedError("Missing access token", code="UNAUTHORIZED")
    service = _build_auth_service(session)
    try:
        return await service.get_user_from_token(token)
    except TokenExpiredError:
        raise UnauthorizedError("Access token has expired", code="token_expired")
    except InvalidTokenError as exc:
        raise UnauthorizedError(str(exc), code="invalid_token")
    except InactiveUserError as exc:
        raise UnauthorizedError(str(exc), code="account_disabled")


def require_roles(*allowed_roles: str) -> Callable:
    """Return a dependency that enforces the caller holds one of *allowed_roles*."""

    async def _check(
        user: User = Depends(get_current_user),
    ) -> User:
        user_role = Role(user.role) if isinstance(user.role, str) else user.role
        permitted = {Role(r) if isinstance(r, str) else r for r in allowed_roles}
        if user_role not in permitted:
            raise ForbiddenError(
                f"This endpoint requires one of: {', '.join(allowed_roles)}",
                code="INSUFFICIENT_PERMISSIONS",
            )
        return user

    return _check
