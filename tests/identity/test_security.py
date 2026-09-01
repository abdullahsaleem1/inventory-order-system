"""
Integration tests for security hardening: token expiry, blacklisting,
RBAC enforcement on protected endpoints, and unauthorized access patterns.

Verifies that expired/tampered/missing tokens are correctly rejected
across inventory, orders, and auth endpoints.
"""
import uuid

import jwt
from httpx import AsyncClient

from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.core.config import get_settings

settings = get_settings()


async def _register(client: AsyncClient, email: str = "sec@example.com") -> dict:
    resp = await client.post(
        "/auth/register",
        json={"email": email, "full_name": "Security Test", "password": "S3curePass!"},
    )
    assert resp.status_code == 201
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _tamper_signature(token: str) -> str:
    header, payload, signature = token.split(".")
    new_char = "A" if signature[0] != "A" else "B"
    return f"{header}.{payload}.{new_char}{signature[1:]}"


def _create_expired_token(user_id: str, email: str, role: str) -> str:
    svc = JwtTokenService(
        secret_key=settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
        issuer=settings.JWT_ISSUER,
        audience=settings.JWT_AUDIENCE,
        access_token_expire_minutes=-1,
    )
    return svc.create_access_token(
        User(
            id=uuid.UUID(user_id),
            email=email,
            full_name="Expired",
            hashed_password="x",
            role=Role(role),
        )
    )


# --- expired access token on protected endpoints ---------------------------


async def test_expired_token_rejected_on_inventory(client: AsyncClient) -> None:
    body = await _register(client, "inv-exp@example.com")
    expired = _create_expired_token(body["user"]["id"], body["user"]["email"], body["user"]["role"])
    resp = await client.get("/inventory/products", headers=_auth(expired))
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "token_expired"


async def test_expired_token_rejected_on_orders(client: AsyncClient) -> None:
    body = await _register(client, "ord-exp@example.com")
    expired = _create_expired_token(body["user"]["id"], body["user"]["email"], body["user"]["role"])
    resp = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "lines": [{"product_id": "00000000-0000-0000-0000-000000000000", "quantity": 1, "unit_price_cents": 100}],
        },
        headers=_auth(expired),
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "token_expired"


async def test_expired_token_rejected_on_auth_me(client: AsyncClient) -> None:
    body = await _register(client, "me-exp@example.com")
    expired = _create_expired_token(body["user"]["id"], body["user"]["email"], body["user"]["role"])
    resp = await client.get("/auth/me", headers=_auth(expired))
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "token_expired"


# --- tampered access token on protected endpoints --------------------------


async def test_tampered_token_rejected_on_inventory(client: AsyncClient) -> None:
    body = await _register(client, "inv-tamper@example.com")
    token = body["access_token"]
    forged = _tamper_signature(token)
    resp = await client.get("/inventory/products", headers=_auth(forged))
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_token"


async def test_tampered_token_rejected_on_orders(client: AsyncClient) -> None:
    body = await _register(client, "ord-tamper@example.com")
    token = body["access_token"]
    forged = _tamper_signature(token)
    resp = await client.get("/orders/00000000-0000-0000-0000-000000000000", headers=_auth(forged))
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_token"


# --- missing token on protected endpoints ----------------------------------


async def test_missing_token_rejected_on_inventory_create(client: AsyncClient) -> None:
    resp = await client.post(
        "/inventory/products",
        json={"sku": "NO-TOKEN", "name": "Widget", "price_cents": 100, "quantity_on_hand": 1},
    )
    assert resp.status_code == 401


async def test_missing_token_rejected_on_inventory_reserve(client: AsyncClient) -> None:
    resp = await client.post(
        "/inventory/products/00000000-0000-0000-0000-000000000000/reserve",
        json={"quantity": 1},
    )
    assert resp.status_code == 401


async def test_missing_token_rejected_on_inventory_restock(client: AsyncClient) -> None:
    resp = await client.post(
        "/inventory/products/00000000-0000-0000-0000-000000000000/restock",
        json={"quantity": 10},
    )
    assert resp.status_code == 401


async def test_missing_token_rejected_on_order_confirm(client: AsyncClient) -> None:
    resp = await client.post("/orders/00000000-0000-0000-0000-000000000000/confirm")
    assert resp.status_code == 401


async def test_missing_token_rejected_on_order_cancel(client: AsyncClient) -> None:
    resp = await client.post("/orders/00000000-0000-0000-0000-000000000000/cancel")
    assert resp.status_code == 401


# --- RBAC: wrong role on specific endpoints --------------------------------


async def test_customer_cannot_create_product(client: AsyncClient) -> None:
    body = await _register(client, "cust-rbac@example.com")
    resp = await client.post(
        "/inventory/products",
        json={"sku": "CUST-0001", "name": "Widget", "price_cents": 100, "quantity_on_hand": 1},
        headers=_auth(body["access_token"]),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "INSUFFICIENT_PERMISSIONS"


async def test_customer_cannot_confirm_order(client: AsyncClient) -> None:
    body = await _register(client, "cust-confirm@example.com")
    resp = await client.post(
        "/orders/00000000-0000-0000-0000-000000000000/confirm",
        headers=_auth(body["access_token"]),
    )
    assert resp.status_code == 403


async def test_staff_cannot_restock(client: AsyncClient) -> None:
    body = await _register(client, "staff-restock@example.com")
    resp = await client.post(
        "/inventory/products/00000000-0000-0000-0000-000000000000/restock",
        json={"quantity": 10},
        headers=_auth(body["access_token"]),
    )
    assert resp.status_code == 403


async def test_staff_cannot_confirm_order(client: AsyncClient) -> None:
    body = await _register(client, "staff-confirm@example.com")
    resp = await client.post(
        "/orders/00000000-0000-0000-0000-000000000000/confirm",
        headers=_auth(body["access_token"]),
    )
    assert resp.status_code == 403


# --- invalid refresh token --------------------------------------------------


async def test_invalid_refresh_token_rejected(client: AsyncClient) -> None:
    """A fabricated refresh token string should be rejected."""
    resp = await client.post("/auth/refresh", json={"refresh_token": "completely-fake-token-123"})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_refresh_token"


async def test_empty_refresh_token_rejected(client: AsyncClient) -> None:
    resp = await client.post("/auth/refresh", json={"refresh_token": ""})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_refresh_token"


# --- blacklisted token persistence after logout ----------------------------


async def test_blacklisted_token_stays_rejected(client: AsyncClient) -> None:
    """After logout, the access token should stay rejected for subsequent calls."""
    body = await _register(client, "blacklist@example.com")
    access = body["access_token"]

    resp = await client.post(
        "/auth/logout",
        headers=_auth(access),
    )
    assert resp.status_code == 200

    for _ in range(3):
        resp = await client.get("/auth/me", headers=_auth(access))
        assert resp.status_code == 401
        assert resp.json()["error"]["code"] == "invalid_token"


# --- error envelope consistency --------------------------------------------


async def test_401_on_protected_endpoint_has_standard_envelope(client: AsyncClient) -> None:
    """Verify the error envelope is present on 401 responses."""
    resp = await client.get("/inventory/products")
    assert resp.status_code == 401
    body = resp.json()
    assert "error" in body
    assert "code" in body["error"]
    assert "message" in body["error"]
    assert "status" in body
    assert body["status"] == 401
    assert "request_id" in body
    assert "path" in body
    assert "timestamp" in body


async def test_403_on_protected_endpoint_has_standard_envelope(client: AsyncClient) -> None:
    """Verify the error envelope is present on 403 responses."""
    body = await _register(client, "403-envelope@example.com")
    resp = await client.post(
        "/inventory/products",
        json={"sku": "ENVELOPE", "name": "Test", "price_cents": 100, "quantity_on_hand": 1},
        headers=_auth(body["access_token"]),
    )
    assert resp.status_code == 403
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == "INSUFFICIENT_PERMISSIONS"
    assert body["status"] == 403
    assert "request_id" in body
