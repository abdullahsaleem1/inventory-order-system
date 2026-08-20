"""
Identity bounded context — repository layer.
Persistence for refresh tokens (opaque, SHA-256-hashed) and the access-token
blocklist. The raw token is only ever returned to the client — the DB stores
only the irreversible hash, so a DB breach never leaks usable tokens.
"""
import hashlib
import secrets
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.infrastructure.models import (
    BlacklistedTokenModel,
    RefreshTokenModel,
)


def generate_refresh_token() -> tuple[str, str]:
    """Create a cryptographically random opaque refresh token and its SHA-256 hash."""
    raw = secrets.token_urlsafe(48)
    token_hash = hashlib.sha256(raw.encode()).hexdigest()
    return raw, token_hash


def hash_token(token: str) -> str:
    """Deterministic SHA-256 hash of a token string."""
    return hashlib.sha256(token.encode()).hexdigest()


class RefreshTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        user_id: UUID,
        token_hash: str,
        token_family: UUID,
        expires_at: datetime,
    ) -> RefreshTokenModel:
        row = RefreshTokenModel(
            user_id=user_id,
            token_hash=token_hash,
            token_family=token_family,
            expires_at=expires_at,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_by_hash(self, token_hash: str) -> RefreshTokenModel | None:
        result = await self._session.execute(
            select(RefreshTokenModel).where(RefreshTokenModel.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    async def revoke(self, token_id: UUID) -> None:
        now = datetime.now(timezone.utc)
        await self._session.execute(
            update(RefreshTokenModel)
            .where(RefreshTokenModel.id == token_id)
            .values(revoked_at=now)
        )

    async def revoke_family(self, family_id: UUID) -> None:
        now = datetime.now(timezone.utc)
        await self._session.execute(
            update(RefreshTokenModel)
            .where(
                RefreshTokenModel.token_family == family_id,
                RefreshTokenModel.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )
        await self._session.flush()

    async def revoke_all_for_user(self, user_id: UUID) -> None:
        now = datetime.now(timezone.utc)
        await self._session.execute(
            update(RefreshTokenModel)
            .where(
                RefreshTokenModel.user_id == user_id,
                RefreshTokenModel.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )

    async def cleanup_expired(self) -> int:
        now = datetime.now(timezone.utc)
        result = await self._session.execute(
            select(RefreshTokenModel).where(RefreshTokenModel.expires_at < now)
        )
        expired = result.scalars().all()
        for token in expired:
            await self._session.delete(token)
        return len(expired)


class BlacklistedTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, *, jti: str, expires_at: datetime) -> None:
        row = BlacklistedTokenModel(jti=jti, expires_at=expires_at)
        self._session.add(row)
        await self._session.flush()

    async def is_blacklisted(self, jti: str) -> bool:
        result = await self._session.execute(
            select(BlacklistedTokenModel).where(BlacklistedTokenModel.jti == jti)
        )
        return result.scalar_one_or_none() is not None

    async def cleanup_expired(self) -> int:
        now = datetime.now(timezone.utc)
        result = await self._session.execute(
            select(BlacklistedTokenModel).where(BlacklistedTokenModel.expires_at < now)
        )
        expired = result.scalars().all()
        for token in expired:
            await self._session.delete(token)
        return len(expired)
