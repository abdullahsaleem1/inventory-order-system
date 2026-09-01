"""
Identity bounded context — service (application) layer.
Orchestrates auth use cases: registration, login, token-based identity
resolution, sliding-window refresh token rotation, and session revocation.
"""
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.errors import (
    DuplicateEmailError,
    InactiveUserError,
    InvalidCredentialsError,
    InvalidPasswordError,
)
from src.contexts.identity.repositories.refresh_token_repository import (
    BlacklistedTokenRepository,
    RefreshTokenRepository,
    generate_refresh_token,
    hash_token,
)
from src.contexts.identity.repositories.user_repository import UserRepository


class AuthService:
    def __init__(
        self,
        repository: UserRepository,
        password_hasher,
        token_service,
        refresh_token_repo: RefreshTokenRepository | None = None,
        blocklist_repo: BlacklistedTokenRepository | None = None,
    ) -> None:
        self._repo = repository
        self._hasher = password_hasher
        self._tokens = token_service
        self._refresh_repo = refresh_token_repo
        self._blocklist_repo = blocklist_repo

    @property
    def access_token_expires_in(self) -> int:
        return self._tokens.access_token_expires_in

    @property
    def refresh_token_expires_in(self) -> int:
        return self._tokens.refresh_token_expires_in

    # ---- registration & login -----------------------------------------------

    async def register(
        self, *, email: str, full_name: str, password: str, role: Role = Role.CUSTOMER
    ) -> User:
        normalized_email = email.strip().lower()
        if await self._repo.get_by_email(normalized_email) is not None:
            raise DuplicateEmailError(f"An account with email '{normalized_email}' already exists")

        user = User(
            email=normalized_email,
            full_name=full_name.strip(),
            hashed_password="",  # set below via the hasher (keeps domain pure)
            role=role,
            is_active=True,
        )
        try:
            user.set_password(password, self._hasher)
        except ValueError as exc:
            raise InvalidPasswordError(str(exc)) from exc
        return await self._repo.add(user)

    async def login(self, *, email: str, password: str) -> User:
        user = await self._repo.get_by_email(email.strip().lower())
        if user is None or not user.verify_password(password, self._hasher):
            raise InvalidCredentialsError("Incorrect email or password")
        if not user.is_active:
            raise InactiveUserError("User account is disabled")
        return user

    # ---- token issuance -----------------------------------------------------

    def create_access_token(self, user: User) -> str:
        return self._tokens.create_access_token(user)

    async def create_token_pair(
        self, user: User, *, token_family: UUID | None = None
    ) -> dict:
        """Issue an access + refresh token pair. Stores the refresh token hash."""
        access_token = self._tokens.create_access_token(user)

        refresh_raw, refresh_hash = generate_refresh_token()
        family = token_family or uuid4()
        refresh_expires = self._tokens.refresh_token_expires_in
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=refresh_expires)

        if self._refresh_repo:
            await self._refresh_repo.add(
                user_id=user.id,
                token_hash=refresh_hash,
                token_family=family,
                expires_at=expires_at,
            )

        return {
            "access_token": access_token,
            "refresh_token": refresh_raw,
            "token_type": "bearer",
            "expires_in": self._tokens.access_token_expires_in,
            "refresh_expires_in": refresh_expires,
        }

    # ---- sliding-window refresh ----------------------------------------------

    async def refresh_tokens(self, *, refresh_token: str) -> dict:
        """Validate a refresh token, rotate it, and return a new token pair.

        Sliding window: each use revokes the old refresh token and issues a
        new one in the same token family. If a *revoked* refresh token is
        presented (replay / theft), the entire family is terminated.
        """
        if not self._refresh_repo:
            raise InvalidTokenError("Refresh tokens are not supported")

        token_hash = hash_token(refresh_token)
        stored = await self._refresh_repo.get_by_hash(token_hash)

        if stored is None:
            raise InvalidTokenError("Invalid refresh token")

        if stored.is_revoked:
            # Token reuse detected — potential theft signal.  Kill the whole family.
            await self._refresh_repo.revoke_family(stored.token_family)
            raise InvalidTokenError(
                "Refresh token has been revoked; all sessions in this family have been terminated"
            )

        if stored.is_expired:
            raise TokenExpiredError("Refresh token has expired")

        user = await self._repo.get_by_id(stored.user_id)
        if user is None:
            raise InvalidTokenError("User for this token no longer exists")
        if not user.is_active:
            raise InactiveUserError("User account is disabled")

        # Revoke the used refresh token (sliding window — each use issues a new one)
        await self._refresh_repo.revoke(stored.id)

        # Issue a new pair in the same family
        return await self.create_token_pair(user, token_family=stored.token_family)

    # ---- session revocation --------------------------------------------------

    async def logout(
        self, *, access_token: str | None = None, refresh_token: str | None = None
    ) -> None:
        """Best-effort session termination: revoke the refresh token and
        blacklist the current access token's jti so it can't be reused."""
        user = None

        # Try to identify user from the access token
        if access_token:
            try:
                user = await self.get_user_from_token(access_token)
            except Exception:
                pass

            # Blacklist the access token regardless of user-lookup outcome
            await self._try_blacklist_access_token(access_token)

        # Revoke the refresh token if we know who owns it
        if user and refresh_token and self._refresh_repo:
            token_hash = hash_token(refresh_token)
            stored = await self._refresh_repo.get_by_hash(token_hash)
            if stored and stored.user_id == user.id and not stored.is_revoked:
                await self._refresh_repo.revoke(stored.id)

    async def logout_all(self, *, user: User) -> None:
        """Revoke ALL refresh tokens for a user — ends every session."""
        await self.logout_all_for_user_id(user.id)

    async def logout_all_for_user_id(self, *, user_id: UUID) -> None:
        """Revoke ALL refresh tokens for a user by id (used by command handlers)."""
        if self._refresh_repo:
            await self._refresh_repo.revoke_all_for_user(user_id)

    # ---- access token blacklist helpers --------------------------------------

    async def _try_blacklist_access_token(self, token: str) -> None:
        """Best-effort: extract jti+exp and blacklist. Swallows all errors."""
        if not self._blocklist_repo:
            return
        claims = self._tokens.decode_access_token_unverified(token)
        if not claims:
            return
        jti = claims.get("jti")
        exp = claims.get("exp")
        if jti and exp:
            expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
            await self._blocklist_repo.add(jti=jti, expires_at=expires_at)

    # ---- identity resolution ------------------------------------------------

    async def get_user_from_token(self, token: str) -> User:
        claims = self._tokens.decode_access_token(token)

        # Check if this access token has been blacklisted (e.g. after logout)
        jti = claims.get("jti")
        if jti and self._blocklist_repo and await self._blocklist_repo.is_blacklisted(jti):
            raise InvalidTokenError("Access token has been revoked")

        user = await self._repo.get_by_id(UUID(claims["sub"]))
        if user is None:
            raise InvalidTokenError("User for this token no longer exists")
        if not user.is_active:
            raise InactiveUserError("User account is disabled")
        return user
