# Distributed Inventory & Order Management System

A production-grade backend system built for the Parallax Labs backend internship.
Implements Domain-Driven Design, a strictly layered architecture, a **from-scratch
OAuth2.0/JWT authorization server**, RBAC, sliding-window refresh tokens,
**event-driven order creation over RabbitMQ** (durable topic exchange, consumer
groups, retries + dead-lettering), **async order processing in a separate
worker service** (inventory deduction with exponential-backoff retries),
**CQRS with a dedicated write side and a read-optimized projection**,
**OpenTelemetry distributed tracing that follows a request across the message
broker into a separate worker**, and structured JSON logging — containerized
with Docker Compose.

## Tech Stack
- **Language/Framework:** Python 3.12+ (verified on 3.14), FastAPI
- **Database:** PostgreSQL (async, via SQLAlchemy 2.0 + asyncpg)
- **Message broker:** RabbitMQ 3.13 (`aio-pika`) — durable queues, publisher confirms, DLQ, exponential-backoff retries
- **Migrations:** Alembic
- **Auth:** bcrypt password hashing + HS256-signed JWTs (PyJWT), OAuth2.0 password grant
- **Observability:** OpenTelemetry SDK 1.44 + Jaeger via OTLP/gRPC — W3C trace-context propagated over AMQP message headers, trace-correlated JSON logs, `X-Trace-Id` responses
- **Testing:** pytest + httpx, in-memory SQLite (aiosqlite) for DB-backed integration tests
- Additional pieces (Redis) are added as each corresponding weekly deliverable
  is implemented — see progress log below.

## Architecture

### Domain-Driven Design — Bounded Contexts
The system is split into independent bounded contexts, each with its own
domain model, database tables, and layered stack. They do **not** import each
other's domain objects directly — cross-context communication goes through
the event pipeline (added in a later week).

```
src/
├── contexts/
│   ├── inventory/          # Inventory bounded context (Product entity, stock rules)
│   │   ├── events.py       # order.created -> stock-deduction intent bridge (Week 6)
│   │   └── services/       # DeductInventoryHandler — async worker handler (Week 6)
│   ├── orders/             # Orders bounded context (Order entity, status transitions)
│   │   └── events.py       # order.created <-> Order aggregate bridge (Week 5)
│   └── identity/           # Identity bounded context — OAuth2.0 auth server
│       ├── domain/         # User entity, Role, PasswordHasher protocol, token errors
│       ├── infrastructure/ # BcryptPasswordHasher, JwtTokenService, ORM models
│       ├── repositories/   # UserRepository (persistence abstraction)
│       ├── services/       # AuthService (register / login / token resolution)
│       ├── controllers/    # AuthController (schema <-> service, error mapping)
│       └── api/            # Routes + Pydantic schemas (register, login, /oauth/token, me)
├── shared/                 # Cross-context kernel: base Entity, DB session, AppError hierarchy
│   └── messaging/          # Event envelope, RabbitMQ publisher/consumer, provider (Weeks 5-6)
├── core/                   # Config, structured logging, middleware, error handlers, health routes
└── main.py                 # FastAPI app wiring
```

### Layered Request Flow
Every request follows the same strict path — no layer is skipped:

```
Route → Controller → Service → Repository → Database
(HTTP <-> domain)  (business rules)  (domain <-> ORM)
```

- **Routes**: thin FastAPI endpoint definitions, dependency-injection wiring only.
- **Controllers**: translate request/response schemas, map domain exceptions to the shared `AppError` hierarchy.
- **Services**: orchestrate use cases; contain no business rules themselves.
- **Repositories**: only layer allowed to touch the database; maps ORM rows to domain entities.
- **Domain**: pure Python, framework-agnostic business rules.

### Command/Query Responsibility Segregation (Week 7)

Since Week 7 the request flow is split into two independent paths — the
**write side** (commands) and the **read side** (queries) — mediated by a
lightweight in-process **CQRS bus** (`src/shared/cqrs/__init__.py`):

```
Route → Controller → CqrsBus ──► CommandHandler  → WriteRepository  → write model
                            └──► QueryHandler    → ReadRepository   → read projection
```

- **Commands** (frozen dataclasses, named after the task: `CreateOrderCommand`,
  `ConfirmOrderCommand`, `RegisterUserCommand`, `ReserveStockCommand`, …) change
  state and are executed by exactly one **command handler** off the write model.
- **Queries** (frozen dataclasses: `GetOrderQuery`, `GetProductQuery`, …) answer
  reads and are executed by exactly one **query handler** off the read
  projection.
- Controllers dispatch messages to the bus and translate results/exceptions to
  HTTP — they no longer call repositories or services directly.

The order projection (`orders_read_orders`) is maintained **transactionally**
with the write model by the persistence consumer and by the confirm/cancel
command handlers, so the read side is ready-consistent in single-database mode
and eventually consistent when `READ_DATABASE_URL` is configured to a separate
store. See **CQRS Architecture & Tradeoffs** below for the full decision record.

> Prior note (kept for history): since Week 5, `POST /orders` diverged
> deliberately — Route → Controller → Service → **EventPublisher** → RabbitMQ —
> with the DB write performed by the async persistence consumer. In Week 7 that
> path was re-expressed as the `CreateOrderCommand` **command**, dispatched through
> the CQRS bus.

### Standardized Error Responses
Every error — from auth endpoints and every other endpoint — is serialized by
the global exception handlers (`src/core/error_handlers.py`) into the same
JSON envelope. **400, 401, 403, 404, 409, 422, and 500 responses are
structurally identical:**

```json
{
  "error": {
    "code": "invalid_grant",
    "message": "Incorrect email or password",
    "details": null
  },
  "status": 401,
  "request_id": "73abb953-c56c-4fd7-880e-7b7bd53586e4",
  "path": "/oauth/token",
  "timestamp": "2026-08-07T18:19:33.192792+00:00"
}
```

- `error.code` — stable, machine-readable. Auth endpoints use OAuth2.0 error
  codes (`invalid_grant`, `unsupported_grant_type`, `invalid_token`,
  `token_expired`) plus app codes (`DUPLICATE_EMAIL`, `VALIDATION_ERROR`,
  `UNAUTHORIZED`, …).
- `error.details` — optional structured detail (e.g. field-level 422 errors).
- `request_id` — echoes the `X-Request-ID` header so errors can be correlated
  with server-side structured logs.

## Authentication & Authorization (Weeks 3–4)

A from-scratch OAuth2.0-compatible authorization server in the Identity
bounded context. No Keycloak/Authlib — the token endpoint, password hashing,
JWT signing, refresh token rotation, and RBAC are all implemented in this codebase.

### Endpoints

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/auth/register` | Create a user; returns profile + token pair |
| `POST` | `/auth/login` | JSON login; returns access + refresh token pair |
| `POST` | `/auth/refresh` | Sliding-window refresh token rotation; returns new token pair |
| `POST` | `/auth/logout` | Revoke refresh token + blacklist access token |
| `POST` | `/auth/logout-all` | Terminate every session for the authenticated user |
| `POST` | `/oauth/token` | **OAuth2.0 password grant** (form-encoded); returns a token pair |
| `GET`  | `/auth/me` | Current user (requires `Authorization: Bearer <token>`) |

The `/oauth/token` endpoint implements the RFC 6749 Resource Owner Password
Credentials grant and is the `tokenUrl` wired into Swagger's **Authorize**
button. Access tokens are **short-lived by default (15 minutes)** and must be
sent as `Authorization: Bearer <token>`.

### Sliding-Window Refresh Tokens
Every login/registration returns a **refresh token** (opaque, 64-byte
`secrets.token_urlsafe`) alongside the JWT access token. Refresh tokens are
stored as **irreversible SHA-256 hashes** in the database — a DB breach never
leaks usable tokens.

Rotation follows a sliding window: each use of a refresh token revokes it
and issues a new one in the **same token family**. If a revoked refresh
token is replayed (theft signal), the **entire family is terminated** —
all sibling refresh tokens for that session are revoked immediately.
Refresh tokens expire after 7 days (configurable via `REFRESH_TOKEN_EXPIRE_DAYS`).

### Role-Based Access Control (RBAC)
Four roles exist: `ADMIN`, `MANAGER`, `STAFF`, `CUSTOMER`. The role is
embedded in every JWT access token and enforced on every protected endpoint
via the `require_roles()` FastAPI dependency (`src/core/auth.py`):

| Endpoint | Allowed Roles |
| -------- | ------------- |
| `POST /inventory/products` | ADMIN, MANAGER |
| `POST /inventory/products/{id}/restock` | ADMIN, MANAGER |
| `POST /inventory/products/{id}/reserve` | ADMIN, MANAGER, STAFF |
| `GET /inventory/products` | Any authenticated role |
| `GET /inventory/products/{id}` | Any authenticated role |
| `POST /orders` | Any authenticated role |
| `GET /orders/{id}` | Any authenticated role |
| `POST /orders/{id}/confirm` | ADMIN, MANAGER |
| `POST /orders/{id}/cancel` | ADMIN, MANAGER, STAFF |

Unauthorized access returns `403 FORBIDDEN` with code `INSUFFICIENT_PERMISSIONS`.
Missing or expired tokens return `401 UNAUTHORIZED`.

### Password hashing & salt management
Passwords are hashed with **bcrypt** at cost factor 12
(`src/contexts/identity/infrastructure/password_hasher.py`). Every hash call
generates a fresh random 16-byte salt that is embedded in the 60-character
hash string (`$2b$12$<salt><digest>`), so no separate salt column or manual
salt bookkeeping is needed — the salt travels with the hash and is re-read at
verification time. Verification is constant-time and timing-safe via
`bcrypt.checkpw`. Login returns the identical error for unknown email vs.
wrong password so responses never reveal whether an account exists.

### JWT signing
Access tokens are standard JWTs signed with **HS256** (`HMAC-SHA256` over
`base64url(header).base64url(payload)`) keyed with `JWT_SECRET_KEY`
(`src/contexts/identity/infrastructure/jwt_service.py`). Only the server
knows the secret, so any tampering with claims is detected at decode time.
Claims: `sub` (user UUID), `email`, `role`, `jti`, `iat`, `exp`, `iss`, `aud`.
On decode the signature, `exp`, `iat`, `iss` and `aud` are all validated.

`JWT_SECRET_KEY` is **strictly validated** at startup — it must be at least
32 bytes for HS256 (RFC 7518 §3.2) or the app refuses to boot. Generate one:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

### Quick start with curl

```bash
# Register a user
curl -s -X POST http://localhost:8000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"alice@example.com","full_name":"Alice Smith","password":"S3curePass!"}'

# Login (OAuth2 password grant) and capture the token
TOKEN=$(curl -s -X POST http://localhost:8000/oauth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=password&username=alice@example.com&password=S3curePass!' \
  | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Call a protected endpoint
curl -s http://localhost:8000/auth/me -H "Authorization: Bearer $TOKEN"
```

Seeded accounts (12 users across ADMIN/MANAGER/STAFF/CUSTOMER roles) all log
in with the dev password **`Password123!`** — see [seed data](#seed-data).

## Event-Driven Order Creation (Week 5)

Order creation is no longer a synchronous DB write. `POST /orders` validates
the request, builds the `Order` aggregate in memory, publishes an
**`order.created`** event to RabbitMQ, and returns **202 Accepted** with the
order representation plus the `event_id`. Persistence happens **asynchronously**
in a separate consumer process — Phase 1 of the event-driven pipeline.

### Why RabbitMQ (and not Kafka)?

| Consideration | RabbitMQ | Kafka |
| ------------- | -------- | ----- |
| Routing model | Topic exchanges route by key (`order.created`) to any number of queues — per-consumer-group fan-out is built in | Partition-based; consumers manage offsets manually |
| Per-message ack/retry/DLQ | First-class (manual ack, nack, dead-letter exchanges) | Not per-message; retries are application-level |
| Operational weight for one service | Single small container + management UI | ZooKeeper/KRaft, brokers, partition tooling |
| Fit for command events (one order = one unit of work) | Ideal work-queue semantics | Designed for high-volume stream replay |

For a *command* pipeline ("persist this order") where every message must be
individually acknowledged, retried, or dead-lettered, RabbitMQ's delivery
semantics are exactly what Phase 1 needs.

### Broker topology

Everything below is declared as **durable** — it survives broker restarts:

```
                       ┌──────────────────────────────────────────────┐
                       │      inventory.orders.events (topic)         │
POST /orders ──publish──>  routing key: order.created                  │
   202 Accepted        └───────────┬──────────────────┬─────────────┘
                                   │ bound            │ bound
                     order.created │    order.created │
                ┌──────────────────▼───┐   ┌──────────▼─────────────┐
                │ orders.order-created.│   │ orders.order-created.  │
                │ persistence          │   │ audit                  │
                │ (consumer group 1:   │   │ (consumer group 2:     │
                │  idempotent DB write)│   │  observability log)    │
                └──────────┬───────────┘   └──────────┬─────────────┘
        x-dead-letter-     │                          │
        exchange/routing   ▼                          ▼
                       ┌──────────────────────────────────────────────┐
                       │       inventory.orders.dlx (topic)           │
                       │  <queue>.dlq — poison / exhausted messages   │
                       └──────────────────────────────────────────────┘
```

- **Exchange routing** — one durable topic exchange; event type → routing key
  mapping lives in config (`ORDER_CREATED_ROUTING_KEY=order.created`). New
  consumers subscribe by binding a new queue to the same key without touching
  publishers.
- **Consumer groups** — each group is its own durable queue, so both groups
  receive *every* event (pub/sub). Running N instances of the same group gives
  competing consumers (work queue) — try `docker compose up --scale consumer=3`.
- **Dead-lettering** — work queues carry `x-dead-letter-exchange` /
  `x-dead-letter-routing-key`; rejected messages land in `<queue>.dlq`.

### Delivery guarantees & robustness

| Mechanism | Implementation |
| --------- | -------------- |
| No silent publish loss | **Publisher confirms** — `publish()` returns only after the broker accepts the message, else raises → API returns `503 EVENT_PUBLISH_FAILED` |
| Message survival across broker restarts | Persistent `delivery_mode` + durable queues/exchanges |
| Consumer crash safety | Manual acknowledgement — `ack` only after the handler succeeds |
| At-least-once duplicates | Handlers are **idempotent**: the persistence handler skips already-persisted order IDs (`event_duplicate_skipped`) |
| Transient handler failures | Retries with **exponential backoff** (Week 6) — parked in per-attempt TTL retry queues (`base * 2^(n-1)`), up to `CONSUMER_MAX_RETRIES` (3), then rejected to the DLQ; earlier Week 5 builds used backoff-free republishing |
| Poison messages (bad payload/type) | Classified as permanent → straight to DLQ, no retry burn |
| Auto-reconnect | `aio_pika.connect_robust` on both publisher and consumer |
| Backpressure | Per-consumer QoS prefetch (`CONSUMER_PREFETCH_COUNT=10`) |

### Event envelope

Single JSON document on the wire (CloudEvents-inspired), versioned via the
`x-event-version` AMQP header:

```json
{
  "event_id": "9f1c2e4a-...",
  "event_type": "order.created",
  "occurred_at": "2026-08-20T12:00:00.123456+00:00",
  "correlation_id": "<HTTP X-Request-ID that caused the event>",
  "payload": {
    "order_id": "7b2...",
    "customer_id": "00000000-0000-0000-0000-000000000001",
    "status": "PENDING",
    "total_cents": 3500,
    "lines": [{"product_id": "...", "quantity": 2, "unit_price_cents": 1500}]
  }
}
```

Publisher and consumer sides share ONE bridge module
(`src/contexts/orders/events.py`) that serializes an aggregate into this
payload and reconstructs it back — the wire contract cannot drift silently.
The middleware now honors a caller-supplied `X-Request-ID`, so the same ID
correlates HTTP logs ↔ broker messages across services.

### Structured event lifecycle logging

Every stage emits one queryable JSON line through the standard logger:

```json
{"message": "event_publish_started",   "event_id": "9f1c...", "event_type": "order.created", "correlation_id": "73ab..."}
{"message": "event_publish_succeeded", "event_id": "9f1c...", "duration_ms": 4.21}
{"message": "event_receive_started",   "queue": "orders.order-created.persistence", "attempt": 1, "redelivered": false}
{"message": "order_persisted",         "order_id": "7b2...",  "total_cents": 3500}
{"message": "event_ack",               "queue": "orders.order-created.persistence", "duration_ms": 11.8}
{"message": "event_retry_scheduled",   "attempt": 2, "max_retries": 3, "error_type": "ConnectionError"}
{"message": "event_nack",              "reason": "decode_error: ...", "retryable": false}
{"message": "event_dead_lettered",     "reason": "retries_exhausted (3): ..."}
```

Lifecycle vocabulary: **publish** (`started/succeeded/failed`), **receive**
(`event_receive_started`), **ack**, **nack** (+ `event_retry_scheduled`,
`event_dead_lettered`, `event_duplicate_skipped`, `broker_connection_established`,
`consumer_group_started`). Readiness (`GET /ready`) now also reports the broker:
`{"status": "ready", "database": "up", "broker": "up"}`.

### Failure semantics of POST /orders

| Situation | Response |
| --------- | -------- |
| Invalid payload (empty lines, non-positive quantities) | `422 VALIDATION_ERROR` — **no event published** |
| Missing/expired token, wrong role | `401/403` — **no event published** (auth runs before publishing) |
| `BROKER_URL` not configured | `503 EVENT_BROKER_UNAVAILABLE` (fails closed — never silently drops orders) |
| Broker rejects/unconfirmed after timeout | `503 EVENT_PUBLISH_FAILED` |
| Success | `202 Accepted` + body incl. `event_id`; order becomes readable once the consumer persists it (eventual consistency) |

### Observing the pipeline

```bash
docker compose up --build -d rabbitmq db api consumer

# create an order (any authenticated role)
TOKEN=$(curl -s -X POST http://localhost:8000/oauth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=password&username=admin1@example.com&password=Password123!' \
  | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
curl -s -X POST http://localhost:8000/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"customer_id":"00000000-0000-0000-0000-000000000001",
       "lines":[{"product_id":"<uuid from GET /inventory/products>","quantity":2,"unit_price_cents":1999}]}'
# -> 202 {"id":"...","status":"PENDING","event_id":"..."}

# watch the consumer persist it (structured JSON lifecycle logs)
docker compose logs -f consumer | Select-String order_persisted

# inspect queues / exchange bindings / message rates
# RabbitMQ management UI -> http://localhost:15672 (guest / guest)

# scale the persistence group horizontally (competing consumers)
docker compose up --scale consumer=3 -d
```

Run the second consumer group locally to see exchange fan-out:

```bash
python -m scripts.consume_orders --group audit          # logs every event
python -m scripts.consume_orders --drain-dlq persistence # log+ack DLQ contents
```

## Async Order Processing — Worker Service (Week 6)

Phase 2 introduces a **separate worker service** that consumes `order.created`
events and **asynchronously deducts inventory stock**. The API stays thin (it
only publishes); both the order-persistence consumer *and* the inventory
worker run in their own processes/containers.

```
                        ┌──────────────────────────────────────────────────┐
                        │        inventory.orders.events (topic)           │
 POST /orders ─publish──>                 routing key: order.created        │
    202 Accepted        └─────┬──────────────────┬────────────────┬────────┘
              order.created   │                  │                │
        ┌─────────────────────▼───────┐   ┌──────▼───────┐   ┌────▼───────────────────┐
        │ orders.order-created.       │   │ orders.order- │   │ orders.order-created.│
        │ persistence (API consumer, │   │ created.audit │   │ inventory            │
        │ idempotent DB write)       │   │ (logs events) │   │ (WORKER: deduct stock)│
        └──────────────┬─────────────┘   └──────┬────────┘   └───────────┬──────────┘
                       │   retry stairway:      │                        │
                       │   <queue>.retry.1..N (per-attempt TTL delay)    │
                       │   ──expired──> re-enter events exchange          │
        x-dead-letter  ▼                          │                       ▼
        ┌──────────────────────────────────────────────────────────────────┐
        │                    inventory.orders.dlx (topic)                  │
        │   <queue>.dlq  — poison / retry-exhausted messages per group    │
        └──────────────────────────────────────────────────────────────────┘
```

### Separate worker service

`scripts/worker.py` is a dedicated entrypoint that runs the
`orders.order-created.inventory` consumer group. In Docker Compose it is its
own `worker` container (`command: python -m scripts.worker`), started
alongside the `api`, `consumer`, `db`, and `rabbitmq` services. Its handler
(`DeductInventoryHandler` in the Inventory context) loads each product named in
the event, calls `product.reserve_stock(quantity)` against the domain model,
and persists the new quantity — all asynchronously, off the API request path.

### Asynchronous inventory deduction + idempotency

The worker's handler is **idempotent** because AMQP delivery is at-least-once:
a crash between a successful stock write and the broker `ack` would otherwise
re-deliver the event and double-deduct. It writes one row per order to the new
`inventory_reservation_log` table **in the same transaction** that mutates
stock. A redelivered event finds its `order_id` already logged and skips
(`stock_deduction_duplicate_skipped`), so exactly one deduction ever happens.
The unique index on `order_id` also makes the guard race-safe across worker
replicas (scale out with `docker compose up --scale worker=2`).

### DLQ + retry logic with exponential backoff

Transient failures are retried with **exponential backoff** rather than
immediately. Each consumer group owns a *stairway* of durable retry queues,
one per retry attempt:

| Attempt | Delay parked in `<queue>.retry.N` |
| ------- | --------------------------------- |
| 1       | `base * 2^0 = 1s` |
| 2       | `base * 2^1 = 2s` |
| 3       | `base * 2^2 = 4s` |
| …       | … capped at `CONSUMER_BACKOFF_MAX_SECONDS` (60s) |

Mechanism: on a transient failure the message is re-published into the
`N`-th retry queue with a **per-message TTL** (`expiration`) equal to that
attempt's backoff delay. When the TTL expires, RabbitMQ **dead-letters** the
message onto the main events exchange, which routes it back into the same work
queue for the next attempt. This gives true time-based backoff using only
RabbitMQ's built-in TTL + dead-letter facilities (no plugin). After
`CONSUMER_MAX_RETRIES`, the message is rejected without requeue and lands in
the group's DLQ.

Error classification (which determines retry vs. dead-letter):

| Failure | Class | Outcome |
| ------- | ----- | ------- |
| Malformed envelope (undecodable JSON) | permanent | straight to DLQ |
| Malformed payload / unknown product / wrong event type | `PermanentMessageError` | straight to DLQ, no retry burn |
| Insufficient stock (may be restocked later) | transient | exponential-backoff retry, then DLQ |

### Observing the worker pipeline

```bash
docker compose up --build -d rabbitmq db api consumer worker

# create an order (any authenticated role)
TOKEN=$(curl -s -X POST http://localhost:8000/oauth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=password&username=admin1@example.com&password=Password123!' \
  | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# pick a real product id so stock actually exists
PID=$(curl -s http://localhost:8000/inventory/products -H "Authorization: Bearer $TOKEN" \
  | python -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")

curl -s -X POST http://localhost:8000/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"customer_id\":\"00000000-0000-0000-0000-000000000001\",
       \"lines\":[{\"product_id\":\"$PID\",\"quantity\":2,\"unit_price_cents\":1999}]}"
# -> 202 {"id":"...","status":"PENDING","event_id":"..."}

# watch the worker deduct stock (structured JSON lifecycle logs)
docker compose logs -f worker | Select-String stock_deducted

# inspect the retry stairway, DLQ and message rates
# RabbitMQ management UI -> http://localhost:15672 (guest / guest)

# scale inventory workers horizontally for more concurrent deduction
docker compose up --scale worker=2 -d

# local variant: drain the worker's dead-letter queue
python -m scripts.worker --drain-dlq
```

## CQRS Architecture & Tradeoffs (Week 7)

### What was built
The codebase was fully refactored so that **every** use case is now expressed
as either a **Command** (write) or a **Query** (read), dispatched through a
lightweight in-process mediator — the **CqrsBus** (`src/shared/cqrs/__init__.py`).

- **Command / Query message types** — frozen dataclasses per context:
  `src/contexts/{orders,inventory,identity}/{commands,queries}.py`
  (`CreateOrderCommand`, `ConfirmOrderCommand`, `CancelOrderCommand`,
  `CreateProductCommand`, `ReserveStockCommand`, `RestockCommand`,
  `LoginCommand`, `RegisterUserCommand`, `RefreshTokensCommand`; and
  `GetOrderQuery`, `ListOrdersByCustomerQuery`, `GetProductQuery`,
  `GetCurrentUserQuery`).
- **Command handlers** (write side) — one class per command, operating on a
  *write repository*:
  `src/contexts/{orders,inventory,identity}/command_handlers.py`.
- **Query handlers** (read side) — one class per query, reading from a *read
  repository*: `src/contexts/{orders,inventory,identity}/query_handlers.py`.
- **Split repositories** — each context now has a write repository (the only
  path that mutates the normalized tables) and a read repository (the only path
  API queries use). E.g. `OrderWriteRepository` / `OrderReadRepository`,
  `ProductWriteRepository` / `ProductReadRepository`.
- **A read-optimized projection** — `orders_read_orders`
  (`src/contexts/orders/infrastructure/read_models.py`): one denormalized row
  per order with line items stored inline as JSON, a materialized `total_cents`
  and `line_count`, and a `customer_id` index to serve "all orders for customer
  X" without a join or aggregate. Created in migration `0004`.
- **A write-optimized write table** — migration `0004` also **drops the
  `customer_id` index from the write table** `orders_orders`: that index served
  only reads (which now go to the projection) and slowed every `INSERT` with
  by-maintenance writes.
- **Projection maintenance** — `PersistOrderCreatedHandler` writes the aggregate
  AND upserts the read projection in the **same transaction**; the
  `ConfirmOrderCommandHandler` / `CancelOrderCommandHandler` keep the
  projection's `status` in sync in the same transaction as the write-model
  update.
- **Read-database plumbing** — `src/shared/infrastructure/read_database.py`
  exposes `get_read_db_session()`. When `READ_DATABASE_URL` is empty (default)
  the read side shares the primary DB (single-database CQRS); when set, read
  queries resolve against a dedicated read-optimized store.

### Structural layout (per context)
```
orders/
├── commands.py          # frozen write-side message types
├── queries.py           # frozen read-side message types
├── command_handlers.py  # one handler per command (write side)
├── query_handlers.py    # one handler per query   (read side)
├── repositories/
│   ├── order_write_repository.py   # normalized write tables only
│   └── order_read_repository.py    # orders_read_orders projection only
└── infrastructure/read_models.py   # denormalized read model
```

### The CQRS bus
`CqrsBus.register_command(type, handler)` / `register_query(type, handler)` bind
a concrete message type to a handler instance (or bare callable).
`dispatch_command(...)` / `dispatch_query(...)` resolve by message type and
raise `CqrsMessageError` if no handler is registered. Handlers are wired in the
routes' dependency-injection chain, so the bus is assembled fresh per request
with the correct write/read sessions.

```
# src/contexts/orders/api/routes.py
bus = (CqrsBus()
    .register_command(CreateOrderCommand, CreateOrderCommandHandler(publisher))
    .register_command(ConfirmOrderCommand, ConfirmOrderCommandHandler(write_repo, read_repo))
    .register_query(GetOrderQuery, GetOrderQueryHandler(read_repo, write_repo))
    ...)
return OrderController(bus)
```

### Configuring a dedicated read store
Set `READ_DATABASE_URL` to a separate database (or logical schema). The only
table the read side needs is `orders_read_orders` (and, for the inventory read
side, the product read table). Each read-optimized store can then be sized,
indexed, and even replicated independently of the write database.

### Tradeoffs & decision record
| Tradeoff | Where we landed | Why |
| -------- | --------------- | --- |
| **Command vs Query separation** | Explicit types + handlers + repositories | Clearer intent, independent testing, each side can evolve its schema/storage freely. |
| **Which store reads hit** | Read repositories read ONLY the projections; write tables have only write-friendly indexes | Keeps hot read queries off the write path; removes per-write index overhead. |
| **Eventual vs strong consistency** | Create path is *transactionally* consistent (same txn writes model + projection); confirm/cancel keep them in sync in-txn. With a separate read store, reads are eventually consistent. | Most CQRS systems accept eventual consistency for reads; we keep the create path strong for simplicity. |
| **Scope** | Single-database CQRS by default; separate read store opt-in via `READ_DATABASE_URL` | Works out of the box, matches the test setup, and scales up when a real read replica is warranted. |
| **The CQRS bus is in-process, not a message bus** | No serialization or distributed dispatch | This is a monolith; distributed command/query dispatch over a broker would add latency and complexity without benefit here. The event pipeline (RabbitMQ) still handles the async cross-context decoupling. |
| **Inventory read side** | Shares the product table with the write side for now | Inventory reads are single-row gets; a projection would add no latency win until inventory is read-heavy or needs a denormalized shape. The repository split is in place so a projection can replace it later. |
| **Complexity** | A mediator, message types, and two repositories per context | Acceptable for the correctness/readability gains; a lighter "just split the service" approach would blur the write/read boundary we wanted to enforce. |

### Why this is the right shape for this system
The system is write-heavy on ingestion (`POST /orders` is event-driven and
returns 202 immediately) and read-heavy on queries (per-customer order list,
product listings). A write-optimized write table + a denormalized, indexed read
projection lets each side be tuned independently, and the in-process bus keeps
the bounded contexts decoupled without adding a second broker. This directly
fulfils the brief's "highly optimized for fast insertions" goal by removing
unnecessary read indexes from the write database.

## CQRS Read Phase — Dedicated Read Store (Week 8)

Week 7 shipped the read projection *inside* the same database. Week 8 removes
reading from the database entirely **for orders**: query handlers now answer
**exclusively** from a dedicated read store that lives **outside** the write
database, is written **only** by a background sync worker, and is therefore
*eventually* consistent with the write model.

### What was built
- **Dedicated read store** behind a single `OrderReadStore` interface
  (`src/shared/readstore/`):
  - `InMemoryOrderReadStore` — the zero-dependency default (dev + tests).
  - `ElasticsearchOrderReadStore` — the deployment-backed store; compose runs
    an `elasticsearch` service and points the API / sync worker at it via
    `READ_STORE_TYPE=elasticsearch`, `READ_STORE_URL=http://elasticsearch:9200`.
  - `build_read_store()` / `get_read_store()` factory wired through FastAPI DI
    (built at startup, closed at shutdown).
- **Sync worker (read projector)** — `scripts/read_projector.py` runs a third
  consumer group (`read`, queue `orders.order-created.read`) bound to
  `order.created` and `order.status.changed`. Its handler
  (`ProjectOrderToReadStoreHandler` in
  `src/contexts/orders/services/read_model_projector.py`) is the **only**
  writer of the read store. It is idempotent (at-least-once safe) and classifies
  unhandled event types / malformed payloads as `PermanentMessageError` → DLQ.
- **New event type `order.status.changed`** — confirm/cancel command handlers
  publish it *before* the DB commit (a publish failure fails the command and
  rolls the write back), so the write model and the read store transition
  independently and the read side can lag-and-converge.
- **Query handlers read only the read store** — the Week-7 fallback to the
  cached projection table was **removed**: `GetOrderQueryHandler` and
  `ListOrdersQueryHandler` go through `OrderReadStoreRepository` →
  `OrderReadStore` and nowhere else. The Week-7 `orders_read_orders` projection
  is still maintained transactionally for the write path and week-7 tests, but
  queries no longer touch it.

### Document shape
Each projection is a plain JSON document (no joins, no DB), shared verbatim by
the in-memory and Elasticsearch stores and the repository mapping:

```json
{ "order_id": "…", "customer_id": "…", "status": "PENDING",
  "total_cents": 3000, "line_count": 2,
  "items": [{ "product_id": "…", "quantity": 2, "unit_price_cents": 1500, "subtotal_cents": 3000 }],
  "created_at": "…", "updated_at": "…" }
```

### Eventual consistency, on purpose
- Writes **commit** when `POST /orders` / confirm / cancel returns.
- The read store catches up when the sync worker consumes the event.
- A query between those two instants returns **404** for `GET /orders/{id}` and
  simply omits the order from a list — the query path **never falls back** to
  the write model, so the read store is the single source of truth for reads.
- Status transitions flow as events: a `GET` served before the read store
  applies them shows the stale status, then converges (verified by
  `tests/orders/test_read_store_consistency.py`, including the
  out-of-order/no-prior-document safety case).

### Read latency (measured)
`python -m scripts.benchmark_reads --orders 5000 --iterations 1000` seeds
5,000 orders, then times each read strategy this project has evolved through.
The Week-8 row uses the in-memory backend (no Docker available on the
benchmark machine); the same script takes `--es-url` to measure the
Elasticsearch store compose brings up.

| Read strategy | mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
| ------------- | --------- | -------- | -------- | -------- |
| GET /orders/{id} (write-engine join - pre-CQRS) | 4.4889 | 4.1726 | 6.4527 | 7.8258 |
| GET /orders/{id} (Week-7 projection, one table) | 1.6793 | 1.5555 | 2.4256 | 2.9711 |
| GET /orders/{id} (Week-8 dedicated read store) | 0.0275 | 0.0227 | 0.0573 | 0.0812 |
| List by customer (write-engine full scan - pre-CQRS) | 115.6851 | 104.0222 | 176.2422 | 223.0320 |
| List by customer (Week-7 projection index) | 1.8400 | 1.6220 | 3.0569 | 4.1004 |
| List by customer (Week-8 dedicated read store) | 0.8244 | 0.6149 | 1.7695 | 2.7097 |

Analysis:
- **GET by id ≈ 163× faster** than the normalized write-engine join and ≈ 61×
  faster than the Week-7 single-table projection.
- **List by customer ≈ 140× faster** than the pre-CQRS full scan (the write
  table deliberately has no `customer_id` index) and ≈ 2.2× faster than the
  Week-7 indexed projection.
- The measured Week-8 number is the query-handler fetch cost only — 5,000
  documents served from a dict-backed store. The Elasticsearch store adds
  network + Lucene cost but keeps reads entirely off the write DB and is
  horizontally replicable. The cost of *keeping the store current* is the
  event→projection transition (≈µs in-memory; bounded by the broker in prod),
  which is exactly the eventual-consistency window the tests measure.

### Week 8 decision record
| Decision | Week 7 | Week 8 |
| -------- | ------ | ------ |
| Where queries read | `orders_read_orders` table | `OrderReadStore` (ES in compose, in-memory in dev/test) |
| Written by | write handlers, in the same transaction | sync worker (consumer group `read`) |
| Read consistency | strong | eventual (404 until projected) |
| Optimized for | zero extra infrastructure | reads that replicate/scale independently of the write DB |
| Store stays current via | transaction commit | `order.created` + `order.status.changed` events |

### Running it
```bash
# Docker Compose (recommended): adds elasticsearch + readprojector services
docker compose up --build

# Pure local: the in-memory read store needs nothing started; run the sync
# worker (consumer group "read") to keep the store current:
python -m scripts.read_projector

# Benchmark the read strategies (numbers above):
python -m scripts.benchmark_reads --orders 5000 --iterations 1000
# Same workload against the compose Elasticsearch store:
python -m scripts.benchmark_reads --orders 5000 --iterations 1000 --es-url http://localhost:9200
```

## Rate Limiting — Token Bucket (Week 9)

A from-scratch **token-bucket** rate limiter guards every request. Buckets are
administered by **Redis** (via a single atomic Lua script) so the burst cap is
strict even under concurrency; when Redis is down the API degrades to
per-process in-memory buckets instead of failing.

| Identity class | Bucket capacity (burst) | Refill rate (req/s) |
| -------------- | ----------------------- | ------------------- |
| anonymous (per IP) | 5 | 1 |
| CUSTOMER (per user) | 10 | 2 |
| STAFF (per user) | 20 | 5 |
| MANAGER (per user) | 50 | 10 |
| ADMIN (per user) | 100 | 20 |

- Authenticated users are keyed `user:{sub}`; anonymous callers `ip:{host}`.
- Limits are per user/role, not per IP for authenticated traffic.
- Exceeding a bucket returns **429 TOO_MANY_REQUESTS** with the standard error
  envelope and `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and
  `X-RateLimit-Retry-After` headers.
- Redis down? Requests keep flowing, rate-limited per instance, with
  `X-RateLimit-Degraded: true`; `/ready` shows `redis: degraded`. A circuit breaker
  (Week 11) also stops a dead Redis from adding a timeout to every request.
- System paths (`/health`, `/ready`, `/docs`, `/redoc`, `/openapi.json`) are
  never rate-limited.

```bash
# Local dev without Redis — disable it (or let it auto-degrade):
RATE_LIMIT_ENABLED=false uvicorn src.main:app --reload

# Concurrency load test against the running compose stack (Redis-backed):
docker compose up --build -d
python -m scripts.loadtest_ratelimit --url http://localhost:8000 --concurrency 40
# To see degradation: docker compose stop redis
```

## Getting Started

### Local (without Docker)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # set DATABASE_URL + a real JWT_SECRET_KEY

# Apply migrations
alembic upgrade head

# Seed realistic test data (60 products, 12 users across 4 roles)
python -m scripts.seed_data

# Terminal 1 — API (set BROKER_URL=amqp://guest:guest@localhost:5672/ if a local broker is running)
uvicorn src.main:app --reload

# Terminal 2 — order persistence consumer (required for POST /orders to reach the DB)
python -m scripts.consume_orders --group persistence

# Optional: second consumer group to observe exchange fan-out
python -m scripts.consume_orders --group audit

# Terminal 3 — async inventory worker (Week 6: deducts stock from order.created)
python -m scripts.worker

# Terminal 4 — async READ PROJECTOR (Week 8: keeps the dedicated read store
# current; without it, orders are 404 until a projector catches up). Uses the
# in-memory read store here; READ_STORE_TYPE=elasticsearch points it at ES.
python -m scripts.read_projector
```

### With Docker Compose
```bash
cp .env.example .env
docker compose up --build
# services: api + consumer + worker + readprojector + db + elasticsearch
#                     + rabbitmq + pgadmin + redis + jaeger
# API runs with READ_STORE_TYPE=elasticsearch; the readprojector consumer
# group fills the Elasticsearch index from order.created / order.status.changed
# Redis hosts the token-bucket rate-limiter state (Week 9).
# Jaeger receives OTLP spans from all four application services (Week 10).

# in a separate terminal, once the api/db containers are up:
docker compose exec api alembic upgrade head
docker compose exec api python -m scripts.seed_data
```

- API docs (Swagger UI): `http://localhost:8000/docs` — click **Authorize**
  and enter credentials to call `/auth/me` from the docs.
- Health check: `http://localhost:8000/health` (liveness) / `http://localhost:8000/ready`
  (readiness — checks DB **and** RabbitMQ).
- **Jaeger UI** (distributed tracing, Week 10): `http://localhost:16686` —
  pick a service (`inventory-orders-api`, `order-event-consumer`,
  `inventory-worker`, `read-projector`) and **Find Traces**. A single
  `POST /orders` shows the API's request, its publish to RabbitMQ, and the
  worker's stock deduction as one trace.
- **RabbitMQ management UI**: `http://localhost:15672` (`guest` / `guest`) —
  inspect the `inventory.orders.events` exchange, durable queues, bindings,
  DLQs, and live message rates.
- **pgAdmin** (Postgres management UI): `http://localhost:5050` — log in with
  the credentials in `.env` (defaults: `admin@example.com` / `admin`), then
  register a new server with host=`db`, port=`5432`, user=`postgres`,
  password=`postgres`.

### Seed Data
```bash
python -m scripts.seed_data               # idempotent — safe to re-run
python -m scripts.seed_data --reset       # wipe seeded tables first
```
Seeds 60 realistic products and 12 users (2 ADMIN, 3 MANAGER, 3 STAFF,
4 CUSTOMER). Every seeded user's password is **`Password123!`** (bcrypt-hashed).

### Database Migrations
Migrations live in `migrations/versions/`, managed by Alembic and targeting
the app's own `DATABASE_URL` from `.env`. Migration `0004` (Week 7) adds the
CQRS read projection `orders_read_orders` and drops the now-unneeded
`customer_id` index from the write table `orders_orders`.
```bash
alembic upgrade head                              # apply all migrations
alembic revision --autogenerate -m "add X table"  # generate a new migration
alembic downgrade -1                               # roll back one migration
```

### Postman / Insomnia
Import [`postman_collection.json`](./postman_collection.json) — includes auth
(register/login/OAuth2 token/refresh/logout/logout-all/me), health/readiness,
and every Inventory and Orders endpoint. Run the **Login** request once and
the returned token is stored in the `{{accessToken}}` collection variable for
protected calls. Refresh, logout, and logout-all endpoints are pre-configured.

### Running Tests
```bash
pip install -r requirements.txt
pytest -v
```

**324 integration + unit tests** (all passing), organized as:

| Test File | Tests | Coverage |
| --------- | ----- | -------- |
| `tests/orders/test_read_store_consistency.py` | 6 | **Week 8 eventual consistency:** 404 until the read projector applies `order.created`; convergence when the sync worker processes the event; confirm/cancel status lag then convergence via `order.status.changed`; cancel-before-projection (out-of-order) safety; idempotent projection under at-least-once replay; projection matches the write model |
| `tests/orders/test_read_projector.py` | 7 | **Week 8 sync-worker handler:** full projection from `order.created`; status-only update from `order.status.changed`; duplicate delivery is idempotent; unsupported event types / malformed payloads → `PermanentMessageError` (DLQ); store↔record round-trip; status update on a missing document is a safe no-op |
| `tests/orders/test_order_read_model.py` | 5 | **Week 7 CQRS read side:** GET resolves via query handler off `orders_read_orders`, confirm/cancel commands keep the projection status in sync, by-customer listing reads the customer_id-indexed projection only, isolation per customer |
| `tests/messaging/test_cqrs_bus.py` | 5 | **Week 7 CQRS bus:** command/query handler invocation (instance + callable), unregistered dispatch raises `CqrsMessageError`, registration is per concrete type |
| `tests/messaging/test_inventory_worker.py` | 15 | **Week 6 async worker:** stock deduction + idempotency log, duplicate redelivery never double-deducts, insufficient stock is retryable (transient), malformed/unknown-product/wrong-type → `PermanentMessageError` (DLQ), retry-after-restock succeeds, exponential-backoff schedule |
| `tests/messaging/test_order_events_api.py` | 7 | **Week 5 event publishing:** exactly one `order.created` per accepted order, payload fidelity, correlation via `X-Request-ID`, no events on 422/401, fail-closed 503 without broker, serializable envelope with UTC timestamp |
| `tests/messaging/test_order_persistence_handler.py` | 10 | Consumer-group handler: persistence correctness + read-projection maintenance, idempotency under duplicate delivery (projection too), poison payloads → `PermanentMessageError` (DLQ), aggregate round-trip through the event bridge |
| `tests/messaging/test_event_envelope.py` | 9 | Envelope serialization round-trip, unique UUIDv4 ids, strict decode rejection of malformed messages |
| `tests/messaging/test_inmemory_publisher.py` | 4 | Publisher test double: ordered recording, inline subscribers, outage-like failure propagation |
| `tests/identity/test_auth_endpoints.py` | 15 | Register, login, OAuth2 token, /auth/me, expired/tampered tokens, error envelope |
| `tests/identity/test_refresh_tokens.py` | 8 | Sliding-window rotation, reuse/theft detection, chained rotations, logout revocation |
| `tests/identity/test_rbac.py` | 18 | RBAC enforcement: create product, restock, reserve, list, confirm, cancel — per role |
| `tests/identity/test_security.py` | 19 | Expired/tampered/missing tokens across inventory/orders/auth, RBAC wrong-role, blacklist persistence, error envelope consistency |
| `tests/inventory/test_product_domain.py` | 8 | Product domain unit tests (stock rules, SKU validation) |
| `tests/identity/test_password_hasher.py` | 3 | bcrypt hashing, salt, verify |
| `tests/identity/test_jwt_service.py` | 2 | JWT signing, expiry, tamper/signature/audience rejection |
| `tests/ratelimit/test_token_bucket.py` | 11 | **Week 9 token-bucket unit tests:** pure `attempt()` math (burst cap, capped balance, refill over time, deny + retry-after, rate=0 → ∞, negative-clock clamp, remaining = floor) plus the in-memory backend (exact-capacity burst, strict multi-task burst never overspends, isolated keys, refill unblocks) |
| `tests/ratelimit/test_redis_backend.py` | 9 | **Week 9 Redis backend:** Lua-script registration, KEYS/ARGV marshalling, result parsing, Redis/Response errors → `RateLimiterUnavailableError`, ping-up/ping-down, key pass-through matching the pure math |
| `tests/ratelimit/test_middleware.py` | 12 | **Week 9 middleware integration over the real HTTP stack:** anonymous IP burst caps at capacity, 429 envelope + `Retry-After`/`X-RateLimit-*` headers, role tiers, per-user keying (not per-IP), exempt `/health`/`/ready`, `/ready` redis status, degraded Redis→in-memory fallback stamping `X-RateLimit-Degraded`, recovery clears degraded, disabled-limiter passthrough |
| `tests/ratelimit/test_circuit_breaker.py` | 18 | **Week 11 circuit breaker:** opens at the failure threshold, skips Redis while open, admits exactly one half-open probe, success closes, failure reopens with capped exponential backoff, `/ready` probe closes it early, disabled limiter never opens |
| `tests/test_resilience_degradation.py` | 11 | **Week 11 degradation:** `/ready` reports `redis: degraded` (not a 500) with Redis down, `/health` never touches dependencies, read-store failure → `503 READ_STORE_UNAVAILABLE`, genuine absence → `404` |
| `tests/messaging/test_consumer_resilience.py` | 15 | **Week 11 retry/DLQ contract:** per-routing-key retry stairways and queue names, undeclared routing key rejected instead of guessed, ack/retry/dead-letter paths, exponential capped backoff |
| `tests/messaging/test_idempotency_race.py` | 6 | **Week 11 idempotency under redelivery:** concurrent duplicate `order.created` persists and deducts stock exactly once; a genuine integrity error is still raised, not swallowed |

Integration tests run against an **in-memory SQLite** DB injected via
`get_db_session` (write side) and `get_read_db_session` (read side) dependency
overrides — no PostgreSQL required. The broker is substituted by an
`InMemoryEventPublisher` dependency override that forwards events inline to the
real handlers, each filtered by its queue's routing-key bindings, so the full
publish → consume → persist → read-projection → **read-store projection** path
is exercised deterministically with **no RabbitMQ container needed**. Order
query handlers read a fresh `InMemoryOrderReadStore` (injected via the
`get_read_store` override), and tests that specialize in eventual consistency
drive the read projector themselves to prove lag-and-converge. The RabbitMQ
client (`aio-pika`) itself is only touched in Docker Compose / local-broker
runs.

Beyond the suite, `scripts/chaos_scenarios.py` and `scripts/chaos_drill.py` run the
load-driven fault injections described in
[Chaos Engineering & Resilience (Week 11)](#chaos-engineering--resilience-week-11) and
write their measurements to `artifacts/`.

## Weekly Progress Log

### Week 1 — Project Setup & DDD Skeleton
- Strictly layered architecture (Routes → Controllers → Services → Repositories)
- Two bounded contexts: **Inventory** (Product entity with stock rules) and
  **Orders** (Order entity with status transitions)
- Async SQLAlchemy 2.0 setup with shared `Base`/session, separate tables per context
- Full CRUD + domain-rule endpoints for both contexts, verified via OpenAPI schema
- Domain-layer unit tests; Dockerfile + docker-compose skeleton (API + Postgres)

### Week 2 — Layered Architecture & Seed Data
**Admin feedback addressed:** added **pgAdmin** to `docker-compose.yml`
(`http://localhost:5050`).

- **Structured JSON logging** (`src/core/logging_config.py`) — every log line
  is a single JSON object; `RequestLoggingMiddleware` logs each request
  (method, path, status, duration, request ID echoed via `X-Request-ID`).
- **Database migrations** via Alembic (`0001_initial_schema.py`).
- **Seed data script** — 60 realistic products + 12 role-based users; idempotent.
- **Health/readiness endpoints** (`/health`, `/ready`).
- **Postman collection** covering all current endpoints.

### Week 3 — OAuth2.0 Authorization Server (Phase 1)
**Admin feedback addressed:** pgAdmin was already added in Week 2 and remains
in `docker-compose.yml`.

- **From-scratch OAuth2.0-compatible authorization server** in the Identity
  bounded context: `POST /oauth/token` implements the RFC 6749 password grant;
  `POST /auth/login` is the JSON convenience equivalent; `POST /auth/register`
  creates accounts and issues a token; `GET /auth/me` verifies tokens
  end-to-end. Swagger's Authorize button is wired to the token endpoint.
- **Secure password hashing & salt management** — bcrypt (cost 12) with
  per-password random salts embedded in the hash (`BcryptPasswordHasher`),
  behind a `PasswordHasher` protocol so the domain stays library-free.
  Malformed/legacy hashes fail verification gracefully; unknown email and
  wrong password return the identical 401.
- **JWT signing** — HS256-signed short-lived access tokens
  (`JwtTokenService`) with `sub`/`email`/`role`/`jti`/`iat`/`exp`/`iss`/`aud`
  claims; signature, expiry, issuer and audience all validated on decode.
- **Strict environment validation** — `JWT_SECRET_KEY` must be ≥ 32 bytes
  (RFC 7518 §3.2); the app refuses to boot with a short key.
- **Standardized error responses** — new shared `AppError` hierarchy plus
  global handlers (`src/core/error_handlers.py`) that serialize *every* error
  (400/401/403/404/409/422/500 and unexpected exceptions) into one JSON
  envelope: `error.code`, `error.message`, `error.details`, `status`,
  `request_id`, `path`, `timestamp`. OAuth2 error codes
  (`invalid_grant`, `unsupported_grant_type`, `invalid_token`, `token_expired`)
  are preserved in `error.code`.
- **Automated integration tests** — 30 tests total: end-to-end HTTP tests for
  register/login/OAuth2 token/me (including expired & tampered tokens) plus
  the standardized error envelope across 400/401/409/422/500, and unit tests
  for the hasher and JWT service. Integration tests run against in-memory
  SQLite via a `get_db_session` dependency override — no external DB required.
- **Seed script upgraded** to real bcrypt hashing; every seeded user logs in
  with `Password123!`.
- **Postman collection** extended with the auth endpoints and a token-storing
  test script.
- **Dependencies added:** `bcrypt`, `PyJWT`, `email-validator`,
  `python-multipart`, `aiosqlite` (test). All pins bumped to versions that
  ship cp314 wheels so the stack installs on Python 3.14.
- **Verified:** full suite passes (`pytest` — 30 passed); OpenAPI registers
  all four auth endpoints plus the `OAuth2PasswordBearer` security scheme.

<!-- Next week's entry goes here -->

### Week 4 — Security Hardening, Swagger Enhancement & Test Suite
- **Sliding-window refresh tokens** with replay/theft detection: each use
  rotates the token; a revoked token reuse terminates the entire family.
  Refresh tokens stored as SHA-256 hashes (never plaintext).
- **Access token blacklisting**: logout adds the token's `jti` to a
  blacklist checked on every authenticated request.
- **Logout & logout-all**: single-session and global session termination.
- **RBAC enforcement on all protected endpoints** via `require_roles()`
  dependency — missing/invalid tokens → 401; wrong role → 403.
- **Portable ORM models**: replaced `PG_UUID` with SQLAlchemy's portable
  `Uuid` type so integration tests run on in-memory SQLite without
  PostgreSQL-specific adapters.
- **Swagger/OpenAPI enhancements**: every endpoint documents error responses
  (`401`, `403`, `404`, `409`) with the `ErrorResponse` model; custom OpenAPI
  schema adds `OAuth2PasswordBearer` security scheme and global security.
- **Postman collection** updated with refresh, logout, logout-all endpoints
  and Authorization headers.
- **73 integration tests** (up from 30): 19 new security tests covering
  expired/tampered/missing tokens, RBAC role enforcement across all
  endpoints, blacklist persistence, error envelope consistency; plus 18
  RBAC tests and 8 refresh token tests.
- **Bug fixes**: added `is_expired`/`is_revoked` properties to
  `RefreshTokenModel` for ORM-level token state; fixed `revoke_family`
  to flush SQL before raising (ensures theft detection persists);
  protected previously-public GET endpoints with `get_current_user`.
- **Full test suite**: `pytest -v` — 73 passed, 0 failed.

### Week 5 — Event-Driven Architecture (Phase 1)
- **RabbitMQ integrated via Docker Compose** (`rabbitmq:3.13-management-alpine`
  with healthcheck + management UI on `:15672`); API and consumer services
  depend on broker readiness. Chosen over Kafka for first-class per-message
  ack/retry/DLQ semantics and topic-exchange routing — see the
  [design rationale](#why-rabbitmq-and-not-kafka).
- **Durable topology**: topic exchange `inventory.orders.events` with routing
  key `order.created`; two consumer groups (durable queues) —
  `orders.order-created.persistence` (idempotent DB write) and
  `orders.order-created.audit` (observability) — both bound to the same key,
  demonstrating pub/sub fan-out + competing consumers; dead-letter exchange
  `inventory.orders.dlx` feeding per-group `<queue>.dlq`.
- **POST /orders refactored to publish, not persist**: validates + builds the
  aggregate, publishes a persistent, confirm-tracked `order.created` event,
  returns **202 Accepted** with the order body and `event_id`. Persistence is
  now asynchronous via `scripts/consume_orders.py --group persistence`
  (separate container in Compose). Broker misconfiguration fails closed with
  `503 EVENT_BROKER_UNAVAILABLE`; unconfirmed publishes return
  `503 EVENT_PUBLISH_FAILED`.
- **Robust delivery semantics**: publisher confirms, persistent messages,
  durable queues/exchanges, manual acks, idempotent duplicate handling,
  bounded retries via `x-retry-count` republishing (max 3), permanent-error
  classification straight to DLQ, `connect_robust` auto-reconnect, QoS prefetch.
- **Structured lifecycle logging** for every event transition — publish
  started/succeeded/failed, receive, retry-scheduled, ack, nack,
  dead-lettered, duplicate-skipped — as queryable JSON lines; `/ready` now
  reports broker health alongside the database.
- **Shared messaging kernel** (`src/shared/messaging/`): versioned event
  envelope with strict decode validation, `EventPublisher` protocol +
  RabbitMQ/in-memory implementations, configurable provider wiring; orders
  context owns a single bridge module keeping the wire contract symmetric.
- **Cross-request correlation**: middleware honors caller-supplied
  `X-Request-ID`, propagated as `correlation_id` so HTTP logs ↔ broker
  messages ↔ persisted orders share one traceable ID.
- **29 new tests (73 → 102)**: event publishing logic end-to-end through the
  HTTP stack (exactly-one-event, payload fidelity, no events on failures,
  fail-closed 503), envelope round-trip/decode-rejection matrix, persistence
  handler correctness + idempotency + poison-payload classification.
- **Full test suite**: `pytest -v` — 102 passed, 0 failed.

### Week 6 — Async Processing (Phase 2)
- **Separate worker service** (`scripts/worker.py`, run as its own `worker`
  container in Docker Compose alongside `api`) that consumes `order.created`
  events on the new `orders.order-created.inventory` consumer group and
  deducts stock asynchronously — entirely off the API request path.
- **Async inventory deduction in the worker**: `DeductInventoryHandler`
  (`src/contexts/inventory/services/order_event_handler.py`) loads each product
  named in the event, applies the domain rule `product.reserve_stock(quantity)`,
  and persists the new quantity. A new cross-context bridge
  (`src/contexts/inventory/events.py`) validates the payload and reconstructs a
  `StockDeductionIntent`, keeping the wire contract symmetric.
- **Idempotency under at-least-once delivery**: handler writes one row per order
  into the new `inventory_reservation_log` table (unique index on `order_id`) in
  the *same transaction* that mutates stock, so a redelivered event can never
  double-deduct — race-safe across worker replicas. New Alembic migration `0003`.
- **DLQ + retry logic with exponential backoff**: refactored
  `RabbitMQEventConsumer` to park failed messages in a *stairway* of per-attempt
  durable retry queues (`<queue>.retry.1..N`) whose per-message TTL equals the
  backoff delay (`base * 2^(n-1)`, default 1s→2s→4s→… capped at
  `CONSUMER_BACKOFF_MAX_SECONDS`). Expired messages dead-letter back onto the
  events exchange and re-enter the work queue. After `CONSUMER_MAX_RETRIES` the
  message is rejected to the DLQ. Malformed envelopes/payloads and unknown
  products are classified permanent → straight to DLQ with no retry burn;
  insufficient stock is transient → retried, then DLQ'd.
- **Tests for graceful malformed-message handling** — 15 new tests
  (`tests/messaging/test_inventory_worker.py`): stock deduction + idempotency
  log, duplicate redelivery never double-deducts, malformed/unknown-product/
  wrong-type payloads raise `PermanentMessageError` (→ DLQ), insufficient stock
  is retryable and atomic (no partial deduction), retry-after-restock succeeds,
  and the exponential-backoff schedule is bounded.
- **Docker Compose runs API and Worker together**: `worker` service starts with
  `api`, `consumer`, `db`, and `rabbitmq`; scale inventory workers with
  `docker compose up --scale worker=2`.
- **Full test suite**: `pytest -v` — 117 passed, 0 failed.

### Week 7 — CQRS (Phase 3: Write Side)
- **Command/Query segregation across every context** — added a lightweight
  in-process **CQRS bus** (`src/shared/cqrs/`) that dispatches frozen
  `Command`/`Query` dataclasses to exactly one registered handler. All three
  bounded contexts were refactored: `orders`, `inventory`, and `identity` each
  gained `commands.py`, `queries.py`, `command_handlers.py`, and
  `query_handlers.py`.
- **Split repositories** — each context now has a separate **write repository**
  (the only path that mutates the normalized tables) and a **read repository**
  (the only path API queries use): `OrderWriteRepository`/`OrderReadRepository`,
  `ProductWriteRepository`/`ProductReadRepository`. The former single
  `OrderRepository`/`ProductRepository`/`ProductService`/`OrderService` were
  removed.
- **Read-optimized projection** — `orders_read_orders`
  (`src/contexts/orders/infrastructure/read_models.py`): one denormalized row
  per order with line items stored inline as JSON, materialized `total_cents`
  and `line_count`, and a `customer_id` index serving per-customer listing
  without a join. New Alembic migration `0004`.
- **Write-optimized write DB** — migration `0004` also **drops the
  `customer_id` index from `orders_orders`**: reads now hit the projection, so
  the write table keeps no read-only index overhead on every INSERT.
- **Read-database plumbing** — `get_read_db_session()` in
  `src/shared/infrastructure/read_database.py`. Default is single-database CQRS
  (as in tests); set `READ_DATABASE_URL` to point the read side at a dedicated
  read-optimized store.
- **Consistent projection** — `PersistOrderCreatedHandler` writes the model and
  upserts the projection in the same transaction; confirm/cancel command
  handlers keep the projection's `status` in sync in-transaction.
- **Docs** — new **CQRS Architecture & Tradeoffs** section above records the
  decision, the write/read split per context, how to configure a separate read
  store, and the tradeoffs (eventual vs strong consistency, in-process bus vs a
  message bus, inventory sharing the product table).
- **Tests refactored to the CQRS structure** — existing tests now use the
  write/read repositories; added `tests/orders/test_order_read_model.py` (5) and
  `tests/messaging/test_cqrs_bus.py` (5); the persistence-handler test asserts
  the read projection is maintained transactionally.
- **Stabilized a pre-existing flaky JWT tamper test** — the tampered-token tests
  mutated only the *last* base64 character of a JWT signature (occasionally a
  no-op, so they intermittently failed). They now flip the *first* signature
  character via a shared tamper helper in `test_security.py`,
  `test_auth_endpoints.py`, and `test_jwt_service.py`, making signature
  tampering deterministic.
- **Full test suite**: `pytest -v` — 127 passed, 0 failed.

### Week 8 — CQRS (Phase 3: Read Side)
- **Dedicated read store outside the write DB** — `OrderReadStore` interface
  with `InMemoryOrderReadStore` (zero-dependency dev/test default) and
  `ElasticsearchOrderReadStore` (compose-backed, `READ_STORE_TYPE=elasticsearch`);
  factory wired through FastAPI DI and closed at shutdown.
- **Sync worker (read projector)** — new consumer group (`read`, queue
  `orders.order-created.read`) bound to `order.created` and
  `order.status.changed`; `ProjectOrderToReadStoreHandler` is the **only**
  writer of the dedicated read store, is idempotent, and classifies
  unsupported/malformed events as `PermanentMessageError` → DLQ.
- **New event type `order.status.changed`** — confirm/cancel command handlers
  publish it before the DB commit so the write model and the read store can
  lag-and-converge; a publish failure rolls back the command.
- **Query handlers read ONLY the read store** — `GetOrderQueryHandler` and
  `ListOrdersQueryHandler` now go through `OrderReadStoreRepository`; the
  Week-7 fallback to the cached projection table was removed.
- **Eventual-consistency tests** (6 in `test_read_store_consistency.py`):
  404-before-projection, convergence-on-project, confirm/cancel status-lag then
  convergence via the new event, cancel-before-projection safety (out-of-order
  delivery), idempotent double-delivery, and `update_status` on a missing
  document without crashing.
- **Read-projector handler unit tests** (7 in `test_read_projector.py`):
  full projection, status-only update, idempotency, unsupported-event /
  malformed-payload → DLQ, store↔record round-trip, and missing-document
  safety.
- **Read-latency benchmark** — `scripts/benchmark_reads.py` measures all three
  read strategies side-by-side: 5,000 orders, 1,000 iterations per strategy.
  Week-8 in-memory GET-by-id ≈ 163× faster than the pre-CQRS normalized join;
  list-by-customer ≈ 140× faster than the write-side full scan.
- **Docker Compose additions** — `elasticsearch` service (single-node,
  `xpack.security.enabled=false`, `ES_JAVA_OPTS="-Xms512m -Xmx512m"`),
  `readprojector` service, `esdata` volume, API env with `READ_STORE_*`
  settings; local mode uses the in-memory store with no infrastructure.
- **Full test suite**: `pytest -v` — 140 passed, 0 failed.

### Week 9 — Redis-Backed Token-Bucket Rate Limiting

**Admin feedback addressed:** rate limiting is implemented **from scratch** —
no pre-built middleware/library (no `slowapi`/`limits`). The token-bucket
algorithm, the middleware, and the Redis backend are all hand-written using
raw Redis primitives.

- **Token-bucket algorithm from scratch** (`src/core/ratelimit/token_bucket.py`):
  a pure, framework-agnostic `attempt()` step (`balance = min(capacity, tokens
  + elapsed*rate)`, spend-if-available, `retry_after = ceil((cost-balance)/rate)`)
  shared by every backend so the math is unit-tested once. A fresh bucket holds
  `capacity` tokens; tokens accrue at `rate`/sec up to `capacity`; each request
  costs one token; `Math.ceil`/`math.ceil` floors `retry_after` to whole seconds
  for the `Retry-After` header.
- **Redis is the strict, atomic backend** (`src/core/ratelimit/backends.py`):
  the entire read-refill-spend-write for one key runs inside a **single Lua
  script** (`_TOKEN_BUCKET_LUA`) that Redis executes atomically — two
  concurrent requests can never both observe the same last token, so bursts
  are capped *exactly* even under concurrency. Bucket state lives in a Redis
  hash (`tokens` + `ts`) refreshed with a sliding TTL.
- **Tiered limits by role** (`policy.py`): ADMIN > MANAGER > STAFF > CUSTOMER >
  anonymous. Authenticated callers are keyed by `user:{sub}` and limited by
  their JWT role's tier; anonymous/missing-token traffic is keyed by
  `ip:{client_host}` and gets the strictest tier. Tiers are fully configurable
  via env vars (`RATE_LIMIT_*_CAPACITY`/`RATE_LIMIT_*_RATE`), defaults:
  ANON 5/1, CUSTOMER 10/2, STAFF 20/5, MANAGER 50/10, ADMIN 100/20.
- **Middleware** (`middleware.py`): runs for every request ahead of routing
  (inside `RequestLoggingMiddleware`, so 429s are logged with a request_id).
  Emits the standard error envelope (`error.code = TOO_MANY_REQUESTS`) with
  `Retry-After` and `X-RateLimit-Limit`/`X-RateLimit-Remaining`/
  `X-RateLimit-Retry-After` headers, plus `X-RateLimit-Degraded: true` when
  operating on the in-memory fallback. Identity is resolved without a DB hit by
  decoding the Bearer JWT (signature + expiry only); `/health`, `/ready`,
  `/docs`, `/redoc`, `/openapi.json`, `/favicon.ico` are never rate-limited.
- **Graceful degradation** (`provider.py`): Redis is *optional at runtime*. If
  it is unreachable when first used or goes down mid-flight, `RateLimiter`
  catches `RateLimiterUnavailableError` and serves the request from a per-process
  `InMemoryTokenBucket` fallback — the API keeps rate limiting (per instance)
  instead of crashing. Recovery clears the degraded flag automatically. `/ready`
  now reports `redis: up | degraded | disabled` alongside DB + broker.
- **Redis via Docker Compose** — new `redis:7-alpine` service with healthcheck
  and `redis_data` volume; the `api` service depends on it and `REDIS_URL`
  points at `redis://redis:6379/0`. In pure-local mode, `RATE_LIMIT_ENABLED=false`
  (or just let it degrade) needs nothing started.
- **Tests (`tests/ratelimit/`, 32)** — pure `attempt()` math (burst cap,
  refill, deny + retry-after, `rate=0` → ∞, negative-clock clamp), the in-memory
  backend (isolated keys, strict multi-task burst never overspends capacity),
  the Redis wrapper driven by a fake async client that mirrors the Lua math
  (marshalling KEYS/ARGV, result parsing, error → `RateLimiterUnavailableError`),
  and full middleware integration over the HTTP stack: anonymous IP caps,
  role tiers, per-user bucket isolation, exempt paths, the 429 envelope +
  headers, degraded fallback, and disabled-limiter passthrough. Rate limiting is
  disabled in every pre-existing fixture so the prior 140 tests keep their exact
  semantics.
- **Concurrency load test** — `scripts/loadtest_ratelimit.py` fires
  concurrent bursts against the running stack and asserts the four guarantees:
  exact burst cap under concurrency (Lua atomicity), tier separation under an
  anonymous flood, refill instead of ban, and `X-RateLimit-Degraded` when Redis
  is stopped.
- **Dependencies added:** `redis==6.4.0` (supports Python 3.14).
- **Full test suite**: `pytest -v` — 172 passed, 0 failed.

## OpenTelemetry Distributed Tracing (Week 10)

**What problem this solves.** Weeks 5–9 built a genuinely distributed system:
an HTTP request, a message broker, and three consumer groups, each in its own
process. When `POST /orders` is slow, the access log tells you *that* it was
slow, and the worker logs tell you the worker was slow, but nothing connects
the two. A request fans out to three queues, two of which can retry with
backoff; without a shared trace id there is no way to answer "which of these
four things ate the 2 seconds?". This week adds that thread.

**The headline result:** one `POST /orders` request appears in Jaeger as a
single trace spanning `inventory-orders-api` and `inventory-worker`, across a
process boundary, with the consumer's work nested underneath the publish that
caused it.

![Captured trace: one trace across the API and the worker](artifacts/trace-waterfall.svg)

*(Regenerated from real captured data — see [Trace artifact](#trace-artifact).)*

### The captured trace

```text
POST /orders  [SERVER]    0.53ms  service=inventory-orders-api
    +-- jwt.verify  [INTERNAL]    0.02ms  service=inventory-orders-api
    +-- command CreateOrderCommand  [INTERNAL]    0.37ms  service=inventory-orders-api
        +-- INSERT orders  [CLIENT]    0.04ms  service=inventory-orders-api
        +-- inventory.orders.events publish  [PRODUCER]    0.15ms  service=inventory-orders-api
            +-- orders.order-created.inventory process  [CONSUMER]    0.23ms  service=inventory-worker
                +-- SELECT products  [CLIENT]    0.01ms  service=inventory-worker
                +-- INSERT inventory_deduplication_log  [CLIENT]    0.01ms  service=inventory-worker
```

The nesting is the whole point. `orders.order-created.inventory process` runs
in a **different container**, minutes or milliseconds later, and it is still a
*child* of the `publish` span that put the message on the queue — because the
publish wrote a W3C `traceparent` into the AMQP headers and the consumer read it
back out.

### What is instrumented

| Layer | Mechanism | Span kind |
| --- | --- | --- |
| Inbound HTTP | `FastAPIInstrumentor` | `SERVER` |
| Outbound HTTP | `FastAPIInstrumentor` (httpx) | `CLIENT` |
| SQL / Postgres | `SQLAlchemyInstrumentor`, per engine | `CLIENT` |
| CQRS commands & queries | hand-written in `shared/cqrs` | `INTERNAL` |
| RabbitMQ publish | hand-written in `messaging/publisher.py` | `PRODUCER` |
| RabbitMQ consume | hand-written in `messaging/consumer.py` | `CONSUMER` |
| Retry republish | hand-written in `messaging/consumer.py` | `PRODUCER` |
| Elasticsearch read store | hand-written in `readstore/elasticsearch_store.py` | `CLIENT` |
| Log correlation | `JSONFormatter` + `LoggingInstrumentor` | — |

`health` and `ready` are excluded: container health probes fire every few seconds
and would otherwise dominate the trace volume.

### The broker hop, and why it is hand-written

No OpenTelemetry package instruments AMQP, and this system's central hop is
AMQP. The implementation is four small functions in
[`src/core/telemetry/propagation.py`](src/core/telemetry/propagation.py):

1. `inject_trace_headers()` — serialises the active span into a W3C
   `traceparent` header and writes it into the message headers **inside** the
   `PRODUCER` span, so the header points at the publish and not at its caller.
2. `extract_trace_context()` — reads it back and returns the parent `Context`.
3. A custom `Getter`/`Setter` pair (`_AmqpHeaderGetter` / `_AmqpHeaderSetter`)
   that normalises AMQP field tables: keys arrive as `str` *or* `bytes`, casing
   is not guaranteed, and RabbitMQ represents some field-table types as a list.
   The OTel `Getter` contract also requires `list[str] | None`, not a bare
   string — the single most common way to write this by hand and silently lose
   every trace.
4. `current_trace_identifiers()` — a hex snapshot of the active context, used
   for log correlation and the `X-Trace-Id` header.

**Headers, not the message body.** The `DomainEvent` envelope is a deliberately
small, language-neutral contract. Baking OpenTelemetry fields into it would
couple every producer and consumer to this tracing library permanently. AMQP
headers already exist, already carry transport metadata, and leave the payload
untouched.

**Failure is silent by design.** A message published before this change, or by a
service with tracing off, has no `traceparent`. Extraction then returns an empty
context and the consumer starts a fresh root trace. After a rolling deploy,
queues legitimately contain both kinds of message, so "no parent is normal" is a
first-class case — not an error path. A corrupt header behaves the same way
rather than raising.

### Retries

A retry is a *new* AMQP delivery that re-enters the queue after a TTL, so it is a
new span, not a continuation of the failed one. The consumer's republish is
opened **inside** the failing attempt's span, so the retry chain stays inside
the original trace instead of scattering across the DLQ views:

![Captured retry trace](artifacts/trace-retry-waterfall.svg)

```text
POST /orders  [SERVER]    0.48ms  service=inventory-orders-api
    ...
    +-- inventory.orders.events publish  [PRODUCER]    0.14ms  service=inventory-orders-api
        +-- orders.order-created.inventory process  [CONSUMER]    0.37ms  service=inventory-worker
            +-- SELECT products  [CLIENT]    0.02ms  service=inventory-worker
            +-- INSERT inventory_deduplication_log  [CLIENT]    0.01ms  service=inventory-worker
        +-- orders.order-created.inventory process  [CONSUMER]    2.56ms  service=inventory-worker
            +-- orders.order-created.inventory.retry.1 publish  [PRODUCER]    0.51ms  service=inventory-worker
```

The second delivery is a **sibling** of the first, not a child — that is
honest, because nothing in the broker caused it. Its republish is a child of the
attempt that failed, which is what keeps the chain in one place. Failing spans
carry `error_type`, `will_retry` / `retries_exhausted`, and `dead_lettered`, so
the DLQ story is readable from the trace alone.

### Configuration

All of it is env-driven, in `src/core/config.py`:

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_ENABLED` | `true` | Master switch. `false` installs nothing; every span call site becomes a no-op. |
| `OTEL_SERVICE_NAME` | `inventory-orders-api` | Fallback only — each entrypoint passes its own. An explicit env value wins, so Compose can relabel a service. |
| `OTEL_SERVICE_VERSION` | `0.7.0` | Reported as `service.version`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `None` | e.g. `http://jaeger:4317`. Unset means no exporter. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | `grpc` or `http/protobuf`. |
| `OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS` | `5` | Export timeout. |
| `OTEL_CONSOLE_EXPORTER` | `false` | Print spans to stdout — useful with no collector. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | See below. |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | Ratio for the `*traceidratio` samplers. |
| `OTEL_PROPAGATE_OVER_BROKER` | `true` | Write/read `traceparent` in AMQP headers. |

Three design points worth calling out:

- **Tracing is opt-out, not opt-in.** `OTEL_ENABLED=false` short-circuits
  `init_tracing()`, and because the OTel API hands out `NoOpTracer` until a
  provider is installed, every `start_as_current_span` call site degrades to a
  no-op context manager. One branch to test, no per-call-site conditionals.
- **No endpoint is not "no tracing".** With neither an endpoint nor the console
  exporter, a real `TracerProvider` with a `Resource` is still installed, so ids
  exist, the broker still propagates, and logs still carry `trace_id` — spans
  are simply dropped at export time. This is what lets the test suite import
  `src.main` without a collector running.
- **Sampling is parent-based by default.** A worker continuing a trace the API
  already sampled must never drop it for being "unsampled" locally, which is
  exactly what a bare `traceidratio` sampler would do.

### One process, one provider

`init_tracing()` is called once per process, as early as possible, by all four
entrypoints (`src/main.py`, `scripts/consume_orders.py`, `scripts/worker.py`,
`scripts/read_projector.py`), and `shutdown_tracing()` runs on the way out — from
the app's shutdown hook and from `atexit` — so the `BatchSpanProcessor` flushes
instead of losing the last few seconds of a trace. Losing the tail of a trace
because the process exited is the classic way to "not see" a span that really
happened.

The `SQLAlchemyInstrumentor` is attached to each engine at import time, which is
why `init_tracing()` must run before the engine module is imported.

### Log and response correlation

Every JSON log line emitted inside a span carries `trace_id`, `span_id` and
`trace_sampled`, read from the ambient context — so no call site has to pass
anything extra:

```json
{"timestamp": "2026-09-28T06:48:54.183Z", "level": "INFO", "logger": "http.access",
 "message": "http_request", "request_id": "6750ee4b-...", "method": "POST",
 "path": "/orders", "status_code": 202, "duration_ms": 53.73,
 "trace_id": "6b0f91caa8ccf74860bd578125b9c9cd",
 "span_id": "dbd775c1fff06422", "trace_sampled": true}
```

Lines emitted outside a span (startup, shutdown, DLQ inspection) simply omit
those keys rather than emitting nulls. Every response also carries an
`X-Trace-Id` header, which is what lets you go from an API response — or a
support ticket quoting one — straight to the trace in Jaeger. It is present on
error responses too, since a failing request is exactly the one you need to
look up.

### Running it

Docker Compose brings Jaeger up with the rest of the stack:

```bash
docker compose up -d
# Jaeger UI:          http://localhost:16686
# OTLP/gRPC receiver: jaeger:4317   (exposed on the host as 4317)
# OTLP/HTTP receiver: jaeger:4318   (exposed on the host as 4318)
```

All four services ship with `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317` and
their own `OTEL_SERVICE_NAME`, declared once via a YAML anchor so the four
blocks cannot drift apart. Jaeger's `COLLECTOR_OTLP_ENABLED=true` is required —
the all-in-one image has OTLP off by default.

```bash
# Jaeger UI -> Services -> pick "inventory-orders-api" -> Find Traces
# or search by tag:  messaging.destination.name=orders.order-created.inventory
# or by trace id copied from an X-Trace-Id response header
```

With no collector available, `OTEL_CONSOLE_EXPORTER=true` prints spans to stdout
instead. Because the console exporter uses a `SimpleSpanProcessor` rather than a
batch one, lines appear in emission order.

**Loading a trace without a running stack.** `scripts/capture_trace.py` drives
the real publisher and consumer classes (substituting only the aio-pika
connection) and writes the result as OTLP JSON. Upload it to a running Jaeger
via **Upload JSON**, or replay it against any OTLP collector:

```bash
python scripts/capture_trace.py                  # -> artifacts/trace.json + trace-tree.txt
python scripts/capture_trace.py --with-retry     # also exercise the retry path
python scripts/render_trace.py                   # -> artifacts/trace-waterfall.svg
```

`capture_trace.py` exits non-zero if the broker hop fails to propagate, so a
broken trace cannot quietly become a committed artifact.

<a id="trace-artifact"></a>
### Trace artifact

The two SVG waterfalls above are **not screenshots of the Jaeger UI** — they are
rendered by `scripts/render_trace.py` from the OTLP JSON in `artifacts/`, which
in turn came from the real instrumented code paths. They are committed so the
documented example is reproducible and diffable without a Docker host. To see the
same trace in Jaeger itself, run the stack and upload `artifacts/trace.json`
(**Upload JSON** in the Jaeger UI), or generate a fresh one with
`scripts/capture_trace.py` while the stack is up and read it in the UI.

### Tests (`tests/telemetry/`, 102)

| File | Covers |
| --- | --- |
| `test_setup.py` | Resource attributes, all sampler names, ratio parsing/clamping, exporter resolution, provider lifecycle and idempotency. |
| `test_propagation.py` | Injection, extraction (case-insensitive, `bytes` keys, list values), the producer→consumer round trip, malformed and missing headers, and 15 parametrised unparseable-header cases. |
| `test_end_to_end.py` | The real `RabbitMQEventPublisher` → `RabbitMQEventConsumer` path: one trace across the hop, span kinds, attribute survival, retries, retry exhaustion, permanent errors, undecodable envelopes, and untraced/legacy messages. |
| `test_api_tracing.py` | HTTP through the ASGI app: `SERVER` span, CQRS nesting, DB spans, `X-Trace-Id`, inbound `traceparent` continuation, and log correlation via `JSONFormatter`. |
| `test_capture_script.py` | The committed artifact: single trace, both services, correct parent/child wiring, well-formed ids, and the renderer's output. |

The `spans` fixture attaches an `InMemorySpanExporter` to the already-installed
provider rather than installing a second one — the OpenTelemetry API permits
exactly one provider per process, and the FastAPI/SQLAlchemy instrumentations are
already bound to it.

- **Full test suite**: `pytest -q` — 274 passed, 0 failed at the end of Week 10
  (172 pre-existing, all unchanged, + 102 new). Week 11 brings the suite to 324.
- **Dependencies added:** `opentelemetry-api`/`-sdk`/`-exporter-otlp-proto-grpc`/
  `-exporter-otlp-proto-http` `==1.44.0`; `opentelemetry-instrumentation-fastapi`/
  `-sqlalchemy`/`-logging` `==0.65b0`. The instrumentation packages trail the SDK
  by one minor release, so they are pinned to the newest version compatible with
  SDK 1.44.0 rather than to the newest that exists.
- **No new transitive dependencies on the request path**: tracing is initialised
  once at startup, remote exporters are batched so a slow collector cannot add
  latency to a request, and every instrumentation helper swallows its own
  failures — a broken trace backend is a monitoring problem, not an availability
  problem.

### Week 10 — OpenTelemetry Distributed Tracing

Full write-up above: [OpenTelemetry Distributed Tracing (Week 10)](#opentelemetry-distributed-tracing-week-10).
In brief — OpenTelemetry SDK 1.44 instruments all four deployables
(`inventory-orders-api`, `order-event-consumer`, `inventory-worker`,
`read-projector`); Jaeger receives them over OTLP/gRPC from a new Compose
service; the API → RabbitMQ → worker hop is stitched with W3C `traceparent`
written into AMQP message headers, since no OTel package instruments AMQP;
retries stay in the original trace; JSON logs and every response carry
`trace_id`; and `scripts/capture_trace.py` regenerates a committed trace
artifact. 102 new tests, 274 total.

## Chaos Engineering & Resilience (Week 11)

Load-driven fault injection against the real FastAPI app, real rate limiter, real
consumer handlers and a real write database, with the hypothesis written down
*before* each fault. Full write-up with measured numbers:
**[`RESILIENCE_REPORT.md`](RESILIENCE_REPORT.md)**.

```bash
# In-process: no Docker needed, exits non-zero if any scenario fails.
python -m scripts.chaos_scenarios                  # full matrix
python -m scripts.chaos_scenarios --scenario redis # one scenario
python -m scripts.chaos_scenarios --window 5 --concurrency 32

# Real containers: docker compose stop <service> / docker kill --signal=SIGKILL <worker>
python -m scripts.chaos_drill --list
python -m scripts.chaos_drill
```

### Scenarios & measured outcome

Recorded run (`artifacts/chaos-results.json`, 1 s windows, concurrency 8) — 4/4 passed:

| Scenario | Fault injected | Result |
| --- | --- | --- |
| Redis outage | `RateLimiter` backend raises | 0 errors, 79/82 responses `X-RateLimit-Degraded: true`, `/ready` → **200** with `redis: degraded`, and **0 Redis calls for 40 requests** once the circuit opened |
| Broker outage | publisher raises `EventPublishError` | creates fail closed **503 `EVENT_PUBLISH_FAILED`** (34/34, no phantom `202`), reads + `/health` unaffected, creation resumes on heal |
| Worker outage | consumer subscribers fail | **202s continue** (22 accepted, 0 errors); unacked work survives the outage and is processed on recovery |
| Elasticsearch outage | read store raises | reads → **503 `READ_STORE_UNAVAILABLE`**, including for a non-existent order id — never a misleading `404`; creates + `/health` unaffected |

Retry exhaustion → DLQ is verified at the contract level in
`tests/messaging/test_consumer_resilience.py`: transient below budget retries,
permanent skips retries, exhausted budget dead-letters once, undecodable bodies go
straight to the DLQ, backoff is `base * 2^(attempt-1)` capped at 60 s.

### Defects found by the exercise, and fixed

| Defect | Impact | Fix |
| --- | --- | --- |
| `/ready` returned **500** when Redis was down (`ReadinessResponse.redis` omitted `"degraded"`) | orchestrators would evict healthy, serving pods | `src/core/system_routes.py` |
| No circuit breaker on the rate limiter | a dead Redis added its 0.5 s timeout to *every* request, indefinitely | `RateLimiter` circuit + half-open probe in `src/core/ratelimit/provider.py` |
| Retry queues shared across routing keys (`routing_keys[0]`) | `order.status.changed` retried on the `order.created` queue; unknown keys guessed | per-`(routing_key, attempt)` stairways in `src/shared/messaging/consumer.py` |
| Check-then-insert in the persistence + inventory handlers | double stock deduction on redelivery; a duplicate raised `IntegrityError` → retried → **DLQ'd despite succeeding** | `IntegrityError` → rollback + re-read + treat as success |
| Read-store errors caught as "not found" | every Elasticsearch failure surfaced as `404 ORDER_NOT_FOUND` | `ReadStoreUnavailableError` → `503 READ_STORE_UNAVAILABLE` |

New settings (see `.env.example`):

```bash
RATE_LIMIT_REDIS_CIRCUIT_FAILURES=2                 # consecutive failures before opening
RATE_LIMIT_REDIS_CIRCUIT_COOLDOWN_SECONDS=5.0        # first cooldown; doubles per failed probe
RATE_LIMIT_REDIS_CIRCUIT_MAX_COOLDOWN_SECONDS=60.0   # backoff cap
```

> **Deployment note:** the retry-queue rename (now
> `{queue}.retry.{routing_key}.{attempt}`) orphans queues declared by earlier
> versions. Drain or delete them after deploying.

### Tests (`tests/test_resilience_degradation.py`, `tests/ratelimit/test_circuit_breaker.py`, `tests/messaging/test_consumer_resilience.py`, `tests/messaging/test_idempotency_race.py`)

| File | Covers |
| --- | --- |
| `test_resilience_degradation.py` | Readiness reports `degraded` (not 500) with Redis down; `/health` never touches dependencies; read-store failure → `503 READ_STORE_UNAVAILABLE`, genuine absence → `404`. |
| `test_circuit_breaker.py` | Opens at the threshold, skips Redis while open, admits exactly one half-open probe, success closes, failure reopens with capped exponential backoff, and `/ready` closes it early. |
| `test_consumer_resilience.py` | Per-routing-key retry stairways and queue names, undeclared key rejected, ack/retry/dead-letter paths, exponential capped backoff. |
| `test_idempotency_race.py` | Concurrent redelivery of the same `order.created` persists and deducts stock exactly once; a genuine integrity error is still raised. |

- **Full suite**: `pytest -q` — **324 passed, 6 warnings** (274 pre-existing + 50 new).
- **No new runtime dependencies**: the harness uses `httpx` and `asyncio` only, and the
  breaker is plain state inside `RateLimiter`.

### Week 11 — Chaos Engineering Resilience Tests

In brief — a load-driven chaos harness (`scripts/chaos_scenarios.py`, plus
`scripts/chaos_drill.py` for real container kills) exercised Redis, RabbitMQ, worker
and Elasticsearch outages under concurrent read/create/liveness load; all four scenarios
matched their pre-written hypotheses, and the run exposed five real defects — a
readiness `500` during Redis degradation, the missing rate-limiter circuit breaker,
cross-routing-key retry queues, duplicate-event side effects in the persistence and
inventory handlers, and read-store outages masquerading as `404`s. All five are fixed and
regression-tested. 50 new tests, 324 total.

## Roadmap (from project brief)
- [x] DDD bounded contexts + layered architecture
- [x] Structured JSON logging
- [x] DB migrations + seed data
- [x] Health/readiness endpoints
- [x] OAuth2.0 password grant + JWT short-lived access tokens + standardized errors (Week 3)
- [x] Sliding-window refresh tokens + access token blacklisting (Week 4)
- [x] RBAC permission checks with role enforcement on all endpoints (Week 4)
- [x] Security hardening + Swagger enhancement + 73 integration tests (Week 4)
- [x] RabbitMQ via Docker Compose: durable exchange, queues, consumer groups, DLQ (Week 5)
- [x] Event-driven order creation: POST /orders publishes `order.created` (202), async persistence (Week 5)
- [x] Full event lifecycle structured logging (publish/receive/ack/nack/retry/DLQ) + 102 tests (Week 5)
- [x] Separate async worker: inventory stock deduction with exponential-backoff retries + DLQ + 117 tests (Week 6)
- [x] CQRS: write side (commands) / read side (queries) split via in-process CqrsBus, write-optimized write DB + read-optimized projection `orders_read_orders` + 127 tests (Week 7)
- [x] CQRS read phase: dedicated read store (Elasticsearch in compose, in-memory in dev/test), async read-projector consumer group as the only read-store writer, query handlers reading exclusively from the read store, eventual-consistency + projector tests, read-latency benchmark + 140 tests (Week 8)
- [x] Redis-backed token bucket rate limiter (from scratch, no library): atomic Lua backend, per-role tiers, graceful degradation + in-memory fallback, 429 envelope + headers, Redis via Docker Compose, concurrency load test + 172 tests (Week 9)
- [x] OpenTelemetry distributed tracing: W3C propagation over AMQP headers, Jaeger via OTLP/gRPC, trace-correlated JSON logs, `X-Trace-Id`, committed trace artifact + 274 tests (Week 10)
- [x] Chaos engineering: load-driven fault injection for Redis / RabbitMQ / worker / read-store outages, 5 resilience defects found and fixed (readiness 500, missing circuit breaker, cross-routing-key retries, duplicate-event side effects, read-store 404s), formal `RESILIENCE_REPORT.md` + 324 tests (Week 11)
