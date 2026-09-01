"""
Integration tests for the authentication endpoints.

Exercise the full HTTP stack (routing + middleware + exception handlers +
DB via the in-memory SQLite dependency override) to verify the OAuth2.0
password-grant flow, registration, token issuance/verification, and the
standardized error envelope across 400/401/409/422/500 responses.
"""
import json
import uuid

import jwt
from httpx import AsyncClient

from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.core.config import get_settings
from src.main import app

settings = get_settings()

REGISTER_PAYLOAD = {
    "email": "alice@example.com",
    "full_name": "Alice Smith",
    "password": "S3curePass!",
}


def assert_error_envelope(body, status: int, code: str) -> None:
    """Assert a response body matches the standardized error schema."""
    assert body["status"] == status
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str) and body["error"]["message"]
    assert "details" in body["error"]
    assert isinstance(body["request_id"], str) and body["request_id"]
    assert isinstance(body["path"], str) and body["path"].startswith("/")
    assert "timestamp" in body


def decode_token(token: str) -> dict:
    return jwt.decode(
        token,
        settings.JWT_SECRET_KEY,
        algorithms=[settings.JWT_ALGORITHM],
        issuer=settings.JWT_ISSUER,
        audience=settings.JWT_AUDIENCE,
    )


# --- registration ---------------------------------------------------------


async def test_register_creates_user_and_issues_token_pair(client: AsyncClient) -> None:
    resp = await client.post("/auth/register", json=REGISTER_PAYLOAD)
    assert resp.status_code == 201

    body = resp.json()
    assert body["token_type"] == "bearer"
    assert body["expires_in"] == settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    assert body["refresh_expires_in"] == settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400
    assert isinstance(body["refresh_token"], str) and body["refresh_token"]
    assert body["user"]["email"] == "alice@example.com"
    assert body["user"]["role"] == "CUSTOMER"
    assert body["user"]["is_active"] is True

    claims = decode_token(body["access_token"])
    assert claims["sub"] == body["user"]["id"]
    assert claims["email"] == "alice@example.com"
    assert claims["role"] == "CUSTOMER"
    assert claims["exp"] - claims["iat"] == settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60


async def test_register_duplicate_email_returns_409(client: AsyncClient) -> None:
    await client.post("/auth/register", json=REGISTER_PAYLOAD)
    resp = await client.post("/auth/register", json=REGISTER_PAYLOAD)
    assert resp.status_code == 409
    assert_error_envelope(resp.json(), 409, "DUPLICATE_EMAIL")


async def test_register_invalid_payload_returns_standard_422(client: AsyncClient) -> None:
    resp = await client.post(
        "/auth/register",
        json={"email": "not-an-email", "full_name": "X", "password": "short"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert_error_envelope(body, 422, "VALIDATION_ERROR")
    assert isinstance(body["error"]["details"], list) and body["error"]["details"]


async def test_register_password_over_bcrypt_byte_limit_returns_400(client: AsyncClient) -> None:
    """40 multi-byte chars exceed bcrypt's 72-byte limit — must be a clean 400, not 500."""
    resp = await client.post(
        "/auth/register",
        json={
            "email": "unicode@example.com",
            "full_name": "Unicode User",
            "password": "é" * 40,
        },
    )
    assert resp.status_code == 400
    assert_error_envelope(resp.json(), 400, "INVALID_PASSWORD")


# --- login -----------------------------------------------------------------


async def test_login_success_issues_token_pair(client: AsyncClient) -> None:
    await client.post("/auth/register", json=REGISTER_PAYLOAD)
    resp = await client.post(
        "/auth/login", json={"email": "alice@example.com", "password": "S3curePass!"}
    )
    assert resp.status_code == 200

    body = resp.json()
    assert set(body) == {"access_token", "refresh_token", "token_type", "expires_in", "refresh_expires_in"}
    assert body["token_type"] == "bearer"
    assert body["expires_in"] == settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    assert body["refresh_expires_in"] == settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400

    claims = decode_token(body["access_token"])
    assert claims["email"] == "alice@example.com"
    assert claims["exp"] - claims["iat"] <= settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60


async def test_login_wrong_password_returns_401(client: AsyncClient) -> None:
    await client.post("/auth/register", json=REGISTER_PAYLOAD)
    resp = await client.post(
        "/auth/login", json={"email": "alice@example.com", "password": "WrongPass1"}
    )
    assert resp.status_code == 401
    assert_error_envelope(resp.json(), 401, "invalid_grant")
    assert resp.json()["error"]["message"] == "Incorrect email or password"


async def test_login_unknown_email_returns_same_401(client: AsyncClient) -> None:
    """Must not reveal whether an account exists."""
    resp = await client.post(
        "/auth/login", json={"email": "nobody@example.com", "password": "Whatever1"}
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_grant"
    assert resp.json()["error"]["message"] == "Incorrect email or password"


# --- OAuth2.0 password grant -----------------------------------------------


async def test_oauth2_password_grant_success(client: AsyncClient) -> None:
    await client.post("/auth/register", json=REGISTER_PAYLOAD)
    resp = await client.post(
        "/oauth/token",
        data={
            "grant_type": "password",
            "username": "alice@example.com",
            "password": "S3curePass!",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["token_type"] == "bearer"
    assert body["access_token"]
    assert body["refresh_token"]
    assert body["expires_in"] == settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    assert body["refresh_expires_in"] == settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400
    assert decode_token(body["access_token"])["email"] == "alice@example.com"


async def test_oauth2_unsupported_grant_type_returns_400(client: AsyncClient) -> None:
    resp = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "username": "alice@example.com",
            "password": "S3curePass!",
        },
    )
    assert resp.status_code == 400
    assert_error_envelope(resp.json(), 400, "unsupported_grant_type")


async def test_oauth2_password_grant_bad_credentials(client: AsyncClient) -> None:
    resp = await client.post(
        "/oauth/token",
        data={
            "grant_type": "password",
            "username": "alice@example.com",
            "password": "WrongPass1",
        },
    )
    assert resp.status_code == 401
    assert_error_envelope(resp.json(), 401, "invalid_grant")


# --- authenticated /auth/me ------------------------------------------------


async def test_me_with_valid_token(client: AsyncClient) -> None:
    reg = (await client.post("/auth/register", json=REGISTER_PAYLOAD)).json()
    resp = await client.get(
        "/auth/me", headers={"Authorization": f"Bearer {reg['access_token']}"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == reg["user"]["id"]
    assert body["email"] == "alice@example.com"
    assert body["role"] == "CUSTOMER"


async def test_me_with_expired_token(client: AsyncClient) -> None:
    reg = (await client.post("/auth/register", json=REGISTER_PAYLOAD)).json()
    svc = JwtTokenService(
        secret_key=settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
        issuer=settings.JWT_ISSUER,
        audience=settings.JWT_AUDIENCE,
        access_token_expire_minutes=-1,
    )
    expired = svc.create_access_token(
        User(
            id=uuid.UUID(reg["user"]["id"]),
            email=reg["user"]["email"],
            full_name=reg["user"]["full_name"],
            hashed_password="x",
            role=Role(reg["user"]["role"]),
        )
    )
    resp = await client.get("/auth/me", headers={"Authorization": f"Bearer {expired}"})
    assert resp.status_code == 401
    assert_error_envelope(resp.json(), 401, "token_expired")


async def test_me_with_tampered_token(client: AsyncClient) -> None:
    reg = (await client.post("/auth/register", json=REGISTER_PAYLOAD)).json()
    token = reg["access_token"]
    header, payload, signature = token.split(".")
    new_char = "A" if signature[0] != "A" else "B"
    forged = f"{header}.{payload}.{new_char}{signature[1:]}"
    resp = await client.get("/auth/me", headers={"Authorization": f"Bearer {forged}"})
    assert resp.status_code == 401
    assert_error_envelope(resp.json(), 401, "invalid_token")


async def test_me_without_token_returns_standard_401(client: AsyncClient) -> None:
    resp = await client.get("/auth/me")
    assert resp.status_code == 401
    assert_error_envelope(resp.json(), 401, "UNAUTHORIZED")
    assert resp.headers.get("www-authenticate") == "Bearer"


# --- standardized 500 handler ----------------------------------------------


async def test_unexpected_exception_uses_standard_500_envelope() -> None:
    from starlette.requests import Request

    handler = app.exception_handlers[Exception]
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/boom",
            "raw_path": b"/boom",
            "query_string": b"",
            "headers": [],
        }
    )
    request.state.request_id = "test-request-id"
    response = await handler(request, ValueError("kaboom"))
    body = json.loads(response.body)
    assert response.status_code == 500
    assert_error_envelope(body, 500, "INTERNAL_ERROR")
    assert body["error"]["message"] == "An unexpected error occurred"
    assert body["request_id"] == "test-request-id"
