# Distributed Inventory & Order Management System

A production-grade backend system built for the Parallax Labs backend internship.
Implements Domain-Driven Design, a strictly layered architecture, a **from-scratch
OAuth2.0/JWT authorization server**, RBAC, sliding-window refresh tokens,
**event-driven order creation over RabbitMQ** (durable topic exchange, consumer
groups, retries + dead-lettering), CQRS groundwork, and structured JSON logging —
containerized with Docker Compose.

## Tech Stack
- **Language/Framework:** Python 3.12+ (verified on 3.14), FastAPI
- **Database:** PostgreSQL (async, via SQLAlchemy 2.0 + asyncpg)
- **Message broker:** RabbitMQ 3.13 (`aio-pika`) — durable queues, publisher confirms, DLQ
- **Migrations:** Alembic
- **Auth:** bcrypt password hashing + HS256-signed JWTs (PyJWT), OAuth2.0 password grant
- **Testing:** pytest + httpx, in-memory SQLite (aiosqlite) for DB-backed integration tests
- Additional pieces (Redis, OpenTelemetry/Jaeger) are added as each corresponding
  weekly deliverable is implemented — see progress log below.

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
│   └── messaging/          # Event envelope, RabbitMQ publisher/consumer, provider (Week 5)
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

> Since Week 5, `POST /orders` diverges deliberately: Route → Controller →
> Service → **EventPublisher** → RabbitMQ, with the database write performed by
> the async persistence consumer. Every other endpoint still follows the full
> synchronous path.

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
| Transient handler failures | Bounded retries — message is republished with `x-retry-count` header, up to `CONSUMER_MAX_RETRIES` (3), then rejected to the DLQ |
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
```

### With Docker Compose
```bash
cp .env.example .env
docker compose up --build        # api + consumer + db + rabbitmq + pgadmin

# in a separate terminal, once the api/db containers are up:
docker compose exec api alembic upgrade head
docker compose exec api python -m scripts.seed_data
```

- API docs (Swagger UI): `http://localhost:8000/docs` — click **Authorize**
  and enter credentials to call `/auth/me` from the docs.
- Health check: `http://localhost:8000/health` (liveness) / `http://localhost:8000/ready`
  (readiness — checks DB **and** RabbitMQ).
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
the app's own `DATABASE_URL` from `.env`.
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

**102 integration tests** (all passing), organized as:

| Test File | Tests | Coverage |
| --------- | ----- | -------- |
| `tests/messaging/test_order_events_api.py` | 7 | **Week 5 event publishing:** exactly one `order.created` per accepted order, payload fidelity, correlation via `X-Request-ID`, no events on 422/401, fail-closed 503 without broker, serializable envelope with UTC timestamp |
| `tests/messaging/test_order_persistence_handler.py` | 9 | Consumer-group handler: persistence correctness, idempotency under duplicate delivery, poison payloads → `PermanentMessageError` (DLQ), aggregate round-trip through the event bridge |
| `tests/messaging/test_event_envelope.py` | 9 | Envelope serialization round-trip, unique UUIDv4 ids, strict decode rejection of malformed messages |
| `tests/messaging/test_inmemory_publisher.py` | 4 | Publisher test double: ordered recording, inline subscribers, outage-like failure propagation |
| `tests/identity/test_auth_endpoints.py` | 15 | Register, login, OAuth2 token, /auth/me, expired/tampered tokens, error envelope |
| `tests/identity/test_refresh_tokens.py` | 8 | Sliding-window rotation, reuse/theft detection, chained rotations, logout revocation |
| `tests/identity/test_rbac.py` | 18 | RBAC enforcement: create product, restock, reserve, list, confirm, cancel — per role |
| `tests/identity/test_security.py` | 19 | Expired/tampered/missing tokens across inventory/orders/auth, RBAC wrong-role, blacklist persistence, error envelope consistency |
| `tests/inventory/test_product_domain.py` | 8 | Product domain unit tests (stock rules, SKU validation) |
| `tests/identity/test_password_hasher.py` | 3 | bcrypt hashing, salt, verify |
| `tests/identity/test_jwt_service.py` | 2 | JWT signing, expiry, tamper/signature/audience rejection |

Integration tests run against an **in-memory SQLite** DB injected via a
`get_db_session` dependency override — no PostgreSQL required. The broker is
substituted by an `InMemoryEventPublisher` dependency override that also
forwards events inline to the real persistence handler, so the full
publish → consume → persist path is exercised deterministically with **no
RabbitMQ container needed**. The RabbitMQ client (`aio-pika`) itself is only
touched in Docker Compose / local-broker runs.

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
- [ ] OrderConfirmed events → Inventory stock reservation consumer (Phase 2)
- [ ] CQRS: write-optimized DB + read-optimized store
- [ ] Redis-backed token bucket rate limiter (from scratch)
- [ ] OpenTelemetry distributed tracing (Jaeger/Zipkin)
- [ ] Chaos engineering resilience tests
