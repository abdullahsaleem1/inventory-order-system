"""
Integration tests for Role-Based Access Control (RBAC).

Verifies that protected inventory and orders endpoints correctly enforce
role requirements: requests from users with insufficient permissions are
rejected with a 403 FORBIDDEN response, while authorized roles pass through.
"""
from httpx import AsyncClient

ROLES = {
    "ADMIN": {"email": "admin1@rbactest.com", "full_name": "Admin One", "password": "S3curePass!", "role": "ADMIN"},
    "MANAGER": {"email": "mgr1@rbactest.com", "full_name": "Manager One", "password": "S3curePass!", "role": "MANAGER"},
    "STAFF": {"email": "staff1@rbactest.com", "full_name": "Staff One", "password": "S3curePass!", "role": "STAFF"},
    "CUSTOMER": {"email": "cust1@rbactest.com", "full_name": "Customer One", "password": "S3curePass!", "role": "CUSTOMER"},
}


async def _register_role(client: AsyncClient, role_name: str) -> str:
    """Register a user with the given role and return their access token."""
    payload = ROLES[role_name]
    resp = await client.post("/auth/register", json=payload)
    assert resp.status_code == 201
    return resp.json()["access_token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _setup(client: AsyncClient) -> dict[str, str]:
    """Register all four roles and return {role_name: access_token}."""
    return {name: await _register_role(client, name) for name in ROLES}


# --- inventory: create product ----------------------------------------------


async def test_create_product_admin_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    resp = await client.post(
        "/inventory/products",
        json={"sku": "RBAC-0001", "name": "Widget", "price_cents": 1000, "quantity_on_hand": 10},
        headers=_auth(tokens["ADMIN"]),
    )
    assert resp.status_code == 201


async def test_create_product_manager_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    resp = await client.post(
        "/inventory/products",
        json={"sku": "RBAC-0002", "name": "Gadget", "price_cents": 2000, "quantity_on_hand": 5},
        headers=_auth(tokens["MANAGER"]),
    )
    assert resp.status_code == 201


async def test_create_product_staff_forbidden(client: AsyncClient) -> None:
    tokens = await _setup(client)
    resp = await client.post(
        "/inventory/products",
        json={"sku": "RBAC-0003", "name": "Thing", "price_cents": 500, "quantity_on_hand": 1},
        headers=_auth(tokens["STAFF"]),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "INSUFFICIENT_PERMISSIONS"


async def test_create_product_customer_forbidden(client: AsyncClient) -> None:
    tokens = await _setup(client)
    resp = await client.post(
        "/inventory/products",
        json={"sku": "RBAC-0004", "name": "Stuff", "price_cents": 300, "quantity_on_hand": 1},
        headers=_auth(tokens["CUSTOMER"]),
    )
    assert resp.status_code == 403


# --- inventory: restock -----------------------------------------------------


async def test_restock_admin_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    # Create a product as admin
    create = await client.post(
        "/inventory/products",
        json={"sku": "RS-0001", "name": "Restock Item", "price_cents": 1000, "quantity_on_hand": 5},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = create.json()["id"]

    resp = await client.post(
        f"/inventory/products/{product_id}/restock",
        json={"quantity": 10},
        headers=_auth(tokens["ADMIN"]),
    )
    assert resp.status_code == 200
    assert resp.json()["quantity_on_hand"] == 15


async def test_restock_staff_forbidden(client: AsyncClient) -> None:
    tokens = await _setup(client)
    create = await client.post(
        "/inventory/products",
        json={"sku": "RS-0002", "name": "No Restock", "price_cents": 1000, "quantity_on_hand": 5},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = create.json()["id"]

    resp = await client.post(
        f"/inventory/products/{product_id}/restock",
        json={"quantity": 10},
        headers=_auth(tokens["STAFF"]),
    )
    assert resp.status_code == 403


# --- inventory: reserve stock -----------------------------------------------


async def test_reserve_staff_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    create = await client.post(
        "/inventory/products",
        json={"sku": "RV-0001", "name": "Reservable", "price_cents": 1000, "quantity_on_hand": 20},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = create.json()["id"]

    resp = await client.post(
        f"/inventory/products/{product_id}/reserve",
        json={"quantity": 5},
        headers=_auth(tokens["STAFF"]),
    )
    assert resp.status_code == 200
    assert resp.json()["quantity_on_hand"] == 15


async def test_reserve_customer_forbidden(client: AsyncClient) -> None:
    tokens = await _setup(client)
    create = await client.post(
        "/inventory/products",
        json={"sku": "RV-0002", "name": "No Reserve", "price_cents": 1000, "quantity_on_hand": 20},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = create.json()["id"]

    resp = await client.post(
        f"/inventory/products/{product_id}/reserve",
        json={"quantity": 1},
        headers=_auth(tokens["CUSTOMER"]),
    )
    assert resp.status_code == 403


# --- inventory: read (all roles) --------------------------------------------


async def test_list_products_all_roles_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    # Create at least one product
    await client.post(
        "/inventory/products",
        json={"sku": "LIST-0001", "name": "Readable", "price_cents": 1000, "quantity_on_hand": 1},
        headers=_auth(tokens["ADMIN"]),
    )
    for role_name, token in tokens.items():
        resp = await client.get("/inventory/products", headers=_auth(token))
        assert resp.status_code == 200, f"{role_name} should be able to list products"


# --- orders: confirm --------------------------------------------------------


async def test_confirm_order_manager_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    # Create a product and order
    prod = await client.post(
        "/inventory/products",
        json={"sku": "ORD-0001", "name": "Orderable", "price_cents": 1000, "quantity_on_hand": 50},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = prod.json()["id"]
    order = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "lines": [{"product_id": product_id, "quantity": 2, "unit_price_cents": 1000}],
        },
        headers=_auth(tokens["CUSTOMER"]),
    )
    order_id = order.json()["id"]

    resp = await client.post(
        f"/orders/{order_id}/confirm",
        headers=_auth(tokens["MANAGER"]),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "CONFIRMED"


async def test_confirm_order_customer_forbidden(client: AsyncClient) -> None:
    tokens = await _setup(client)
    prod = await client.post(
        "/inventory/products",
        json={"sku": "ORD-0002", "name": "No Confirm", "price_cents": 1000, "quantity_on_hand": 50},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = prod.json()["id"]
    order = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "lines": [{"product_id": product_id, "quantity": 1, "unit_price_cents": 1000}],
        },
        headers=_auth(tokens["CUSTOMER"]),
    )
    order_id = order.json()["id"]

    resp = await client.post(
        f"/orders/{order_id}/confirm",
        headers=_auth(tokens["CUSTOMER"]),
    )
    assert resp.status_code == 403


# --- orders: cancel ---------------------------------------------------------


async def test_cancel_order_staff_allowed(client: AsyncClient) -> None:
    tokens = await _setup(client)
    prod = await client.post(
        "/inventory/products",
        json={"sku": "ORD-0003", "name": "Cancellable", "price_cents": 1000, "quantity_on_hand": 50},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = prod.json()["id"]
    order = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "lines": [{"product_id": product_id, "quantity": 1, "unit_price_cents": 1000}],
        },
        headers=_auth(tokens["CUSTOMER"]),
    )
    order_id = order.json()["id"]

    resp = await client.post(
        f"/orders/{order_id}/cancel",
        headers=_auth(tokens["STAFF"]),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELLED"


async def test_cancel_order_customer_forbidden(client: AsyncClient) -> None:
    tokens = await _setup(client)
    prod = await client.post(
        "/inventory/products",
        json={"sku": "ORD-0004", "name": "No Cancel", "price_cents": 1000, "quantity_on_hand": 50},
        headers=_auth(tokens["ADMIN"]),
    )
    product_id = prod.json()["id"]
    order = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "lines": [{"product_id": product_id, "quantity": 1, "unit_price_cents": 1000}],
        },
        headers=_auth(tokens["CUSTOMER"]),
    )
    order_id = order.json()["id"]

    resp = await client.post(
        f"/orders/{order_id}/cancel",
        headers=_auth(tokens["CUSTOMER"]),
    )
    assert resp.status_code == 403


# --- unauthenticated access -------------------------------------------------


async def test_protected_endpoint_without_token_returns_401(client: AsyncClient) -> None:
    resp = await client.post(
        "/inventory/products",
        json={"sku": "NO-AUTH", "name": "No Auth", "price_cents": 100, "quantity_on_hand": 1},
    )
    assert resp.status_code == 401


async def test_protected_order_endpoint_without_token_returns_401(client: AsyncClient) -> None:
    resp = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "lines": [{"product_id": "00000000-0000-0000-0000-000000000000", "quantity": 1, "unit_price_cents": 100}],
        },
    )
    assert resp.status_code == 401
