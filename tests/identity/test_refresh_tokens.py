"""
Integration tests for sliding-window refresh tokens and token revocation.

Verifies the full refresh token lifecycle: issuance, rotation (sliding
window), replay-attack detection (token family termination), logout
revocation, and access-token blacklisting.
"""
import uuid

import jwt
from httpx import AsyncClient

from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.core.config import get_settings

settings = get_settings()

REGISTER_PAYLOAD = {
    "email": "bob@example.com",
    "full_name": "Bob Tester",
    "password": "S3curePass!",
}


async def _register_and_get_tokens(client: AsyncClient) -> dict:
    """Helper: register a user and return the full response body."""
    resp = await client.post("/auth/register", json=REGISTER_PAYLOAD)
    assert resp.status_code == 201
    return resp.json()


# --- refresh token rotation -------------------------------------------------


async def test_refresh_returns_new_token_pair(client: AsyncClient) -> None:
    body = await _register_and_get_tokens(client)
    old_refresh = body["refresh_token"]
    old_access = body["access_token"]

    resp = await client.post("/auth/refresh", json={"refresh_token": old_refresh})
    assert resp.status_code == 200
    new = resp.json()

    # New tokens are different from the old ones
    assert new["access_token"] != old_access
    assert new["refresh_token"] != old_refresh
    assert new["token_type"] == "bearer"
    assert new["expires_in"] == settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    assert new["refresh_expires_in"] == settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400

    # New access token is valid and resolves to the same user
    resp2 = await client.get(
        "/auth/me", headers={"Authorization": f"Bearer {new['access_token']}"}
    )
    assert resp2.status_code == 200
    assert resp2.json()["email"] == "bob@example.com"


async def test_refresh_invalid_token_returns_401(client: AsyncClient) -> None:
    resp = await client.post(
        "/auth/refresh", json={"refresh_token": "totally-fake-token-abc123"}
    )
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"]["code"] == "invalid_refresh_token"


async def test_refresh_reuse_detects_theft(client: AsyncClient) -> None:
    """Using a refresh token that was already rotated should revoke the
    entire token family (replay / theft detection)."""
    body = await _register_and_get_tokens(client)
    first_refresh = body["refresh_token"]

    # First rotation — should succeed
    resp1 = await client.post("/auth/refresh", json={"refresh_token": first_refresh})
    assert resp1.status_code == 200
    second_refresh = resp1.json()["refresh_token"]

    # Second rotation of the *first* token — should fail and revoke family
    resp2 = await client.post("/auth/refresh", json={"refresh_token": first_refresh})
    assert resp2.status_code == 401
    assert resp2.json()["error"]["code"] == "invalid_refresh_token"
    assert "terminated" in resp2.json()["error"]["message"].lower()

    # The second token should also be revoked (same family)
    resp3 = await client.post("/auth/refresh", json={"refresh_token": second_refresh})
    assert resp3.status_code == 401


async def test_refresh_chained_rotations(client: AsyncClient) -> None:
    """Multiple successive rotations should all work (sliding window)."""
    body = await _register_and_get_tokens(client)
    token = body["refresh_token"]

    for _ in range(5):
        resp = await client.post("/auth/refresh", json={"refresh_token": token})
        assert resp.status_code == 200
        token = resp.json()["refresh_token"]

    # Final token should resolve to the user
    final_access = (await client.post("/auth/refresh", json={"refresh_token": token})).json()[
        "access_token"
    ]
    me = await client.get("/auth/me", headers={"Authorization": f"Bearer {final_access}"})
    assert me.status_code == 200
    assert me.json()["email"] == "bob@example.com"


# --- logout and revocation --------------------------------------------------


async def test_logout_revokes_refresh_token(client: AsyncClient) -> None:
    body = await _register_and_get_tokens(client)
    refresh = body["refresh_token"]
    access = body["access_token"]

    # Logout with both tokens
    resp = await client.post(
        "/auth/logout",
        json={"refresh_token": refresh},
        headers={"Authorization": f"Bearer {access}"},
    )
    assert resp.status_code == 200
    assert resp.json()["message"] == "Logged out successfully"

    # Refresh token should now be rejected
    resp2 = await client.post("/auth/refresh", json={"refresh_token": refresh})
    assert resp2.status_code == 401

    # Access token should also be rejected (blacklisted)
    resp3 = await client.get("/auth/me", headers={"Authorization": f"Bearer {access}"})
    assert resp3.status_code == 401
    assert resp3.json()["error"]["code"] == "invalid_token"


async def test_logout_without_refresh_token(client: AsyncClient) -> None:
    """Logout should still blacklist the access token even without a refresh token."""
    body = await _register_and_get_tokens(client)
    access = body["access_token"]

    resp = await client.post(
        "/auth/logout",
        headers={"Authorization": f"Bearer {access}"},
    )
    assert resp.status_code == 200

    # Access token should be blacklisted
    resp2 = await client.get("/auth/me", headers={"Authorization": f"Bearer {access}"})
    assert resp2.status_code == 401


async def test_logout_all_revokes_all_refresh_tokens(client: AsyncClient) -> None:
    """Logout-all should terminate every session for the user."""
    body = await _register_and_get_tokens(client)

    # Create a second session by logging in again
    login_resp = await client.post(
        "/auth/login", json={"email": "bob@example.com", "password": "S3curePass!"}
    )
    assert login_resp.status_code == 200
    session2_refresh = login_resp.json()["refresh_token"]
    session2_access = login_resp.json()["access_token"]

    # Logout-all from the first session
    resp = await client.post(
        "/auth/logout-all",
        headers={"Authorization": f"Bearer {body['access_token']}"},
    )
    assert resp.status_code == 200
    assert resp.json()["message"] == "Logged out from all sessions"

    # Both refresh tokens should now be invalid
    resp1 = await client.post("/auth/refresh", json={"refresh_token": body["refresh_token"]})
    assert resp1.status_code == 401
    resp2 = await client.post("/auth/refresh", json={"refresh_token": session2_refresh})
    assert resp2.status_code == 401


async def test_login_returns_fresh_refresh_token(client: AsyncClient) -> None:
    """Login should always return a new, valid refresh token."""
    await client.post("/auth/register", json=REGISTER_PAYLOAD)
    resp = await client.post(
        "/auth/login", json={"email": "bob@example.com", "password": "S3curePass!"}
    )
    assert resp.status_code == 200
    refresh = resp.json()["refresh_token"]

    # Should be usable
    resp2 = await client.post("/auth/refresh", json={"refresh_token": refresh})
    assert resp2.status_code == 200
