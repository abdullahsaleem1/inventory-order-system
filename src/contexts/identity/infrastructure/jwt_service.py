"""
Identity bounded context — infrastructure layer.
JWT signing/verification via PyJWT (HS256).

Issues short-lived signed access tokens and verifies them back. The token is
a standard JWT: base64url(header).base64url(payload).signature, with the
signature being HMAC-SHA256 over `header.payload` keyed with the configured
secret. Only the server knows the secret, so any tampering with the claims is
detected at decode time (signature mismatch). `exp`, `iat`, `iss`, `aud`,
`sub` are all validated on decode.
"""
import uuid
from datetime import datetime, timedelta, timezone

import jwt

from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import User


class JwtTokenService:
    def __init__(
        self,
        *,
        secret_key: str,
        algorithm: str,
        issuer: str,
        audience: str,
        access_token_expire_minutes: int,
        refresh_token_expire_days: int = 7,
    ) -> None:
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._issuer = issuer
        self._audience = audience
        self._access_token_expire_minutes = access_token_expire_minutes
        self._refresh_token_expire_days = refresh_token_expire_days

    @property
    def access_token_expires_in(self) -> int:
        return self._access_token_expire_minutes * 60

    @property
    def refresh_token_expires_in(self) -> int:
        return self._refresh_token_expire_days * 86400

    def create_access_token(self, user: User) -> str:
        now = datetime.now(timezone.utc)
        claims = {
            "sub": str(user.id),
            "email": user.email,
            "role": user.role.value,
            "iss": self._issuer,
            "aud": self._audience,
            "iat": now,
            "exp": now + timedelta(minutes=self._access_token_expire_minutes),
            "jti": str(uuid.uuid4()),
        }
        return jwt.encode(claims, self._secret_key, algorithm=self._algorithm)

    def decode_access_token(self, token: str) -> dict:
        try:
            return jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm],
                issuer=self._issuer,
                audience=self._audience,
                options={"require": ["exp", "iat", "sub", "iss", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise TokenExpiredError("Access token has expired") from exc
        except jwt.InvalidTokenError as exc:
            raise InvalidTokenError("Invalid or malformed access token") from exc

    def decode_access_token_unverified(self, token: str) -> dict | None:
        """Decode a token without verifying expiry — used only for logout/revocation
        to extract the jti and user id from an expired token."""
        try:
            return jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm],
                issuer=self._issuer,
                audience=self._audience,
                options={
                    "require": ["sub", "iss", "aud"],
                    "verify_exp": False,
                },
            )
        except jwt.InvalidTokenError:
            return None
