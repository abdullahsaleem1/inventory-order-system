"""
Unit tests for the JWT token service (infrastructure layer).
"""
import pytest

from src.contexts.identity.domain.errors import InvalidTokenError, TokenExpiredError
from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService

SECRET = "test-secret-key-that-is-longer-than-32-bytes-ok"


def make_service(**overrides) -> JwtTokenService:
    defaults = dict(
        secret_key=SECRET,
        algorithm="HS256",
        issuer="test-issuer",
        audience="test-audience",
        access_token_expire_minutes=15,
    )
    defaults.update(overrides)
    return JwtTokenService(**defaults)


def make_user() -> User:
    return User(
        id="11111111-1111-4111-8111-111111111111",
        email="alice@example.com",
        full_name="Alice",
        hashed_password="x",
        role=Role.ADMIN,
    )


def test_create_access_token_encodes_expected_claims():
    svc = make_service()
    token = svc.create_access_token(make_user())
    parts = token.split(".")
    assert len(parts) == 3

    claims = svc.decode_access_token(token)
    assert claims["sub"] == "11111111-1111-4111-8111-111111111111"
    assert claims["email"] == "alice@example.com"
    assert claims["role"] == "ADMIN"
    assert claims["iss"] == "test-issuer"
    assert claims["aud"] == "test-audience"
    assert claims["exp"] - claims["iat"] == 15 * 60
    assert claims["jti"]


def test_access_token_expires_in():
    assert make_service(access_token_expire_minutes=5).access_token_expires_in == 300
    assert make_service(access_token_expire_minutes=15).access_token_expires_in == 900


def test_tampered_signature_rejected():
    svc = make_service()
    token = svc.create_access_token(make_user())
    forged = token[:-1] + ("A" if token[-1] != "A" else "B")
    with pytest.raises(InvalidTokenError):
        svc.decode_access_token(forged)


def test_wrong_secret_rejected():
    svc = make_service()
    token = svc.create_access_token(make_user())
    other = make_service(secret_key="a-different-secret-that-is-also-long-enough-123")
    with pytest.raises(InvalidTokenError):
        other.decode_access_token(token)


def test_wrong_audience_rejected():
    svc = make_service()
    token = svc.create_access_token(make_user())
    with pytest.raises(InvalidTokenError):
        make_service(audience="someone-else").decode_access_token(token)


def test_expired_token_rejected():
    svc = make_service(access_token_expire_minutes=-1)
    token = svc.create_access_token(make_user())
    with pytest.raises(TokenExpiredError):
        svc.decode_access_token(token)


def test_garbage_token_rejected():
    with pytest.raises(InvalidTokenError):
        make_service().decode_access_token("not.a.jwt")
