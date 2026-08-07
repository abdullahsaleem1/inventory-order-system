# Distributed Inventory & Order Management System

A production-grade backend system built for the Parallax Labs backend internship.
Implements Domain-Driven Design, a strictly layered architecture, a **from-scratch
OAuth2.0/JWT authorization server**, event-driven order processing, CQRS, a
Redis rate limiter, distributed tracing, and chaos engineering tests —
containerized with Docker Compose.

## Tech Stack
- **Language/Framework:** Python 3.12+ (verified on 3.14), FastAPI
- **Database:** PostgreSQL (async, via SQLAlchemy 2.0 + asyncpg)
- **Migrations:** Alembic
- **Auth:** bcrypt password hashing + HS256-signed JWTs (PyJWT), OAuth2.0 password grant
- **Testing:** pytest + httpx, in-memory SQLite (aiosqlite) for DB-backed integration tests
- Additional pieces (Redis, RabbitMQ/Kafka, OpenTelemetry/Jaeger) are added
  as each corresponding weekly deliverable is implemented — see progress log below.

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
│   └── identity/           # Identity bounded context — OAuth2.0 auth server
│       ├── domain/         # User entity, Role, PasswordHasher protocol, token errors
│       ├── infrastructure/ # BcryptPasswordHasher, JwtTokenService, ORM models
│       ├── repositories/   # UserRepository (persistence abstraction)
│       ├── services/       # AuthService (register / login / token resolution)
│       ├── controllers/    # AuthController (schema <-> service, error mapping)
│       └── api/            # Routes + Pydantic schemas (register, login, /oauth/token, me)
├── shared/                 # Cross-context kernel: base Entity, DB session, AppError hierarchy
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

## Authentication (Week 3)

A from-scratch OAuth2.0-compatible authorization server in the Identity
bounded context. No Keycloak/Authlib — the token endpoint, password hashing,
and JWT signing are all implemented in this codebase.

### Endpoints

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/auth/register` | Create a user; returns profile + short-lived access token |
| `POST` | `/auth/login` | JSON login; returns a short-lived Bearer access token |
| `POST` | `/oauth/token` | **OAuth2.0 password grant** (form-encoded); returns a token |
| `GET`  | `/auth/me` | Current user (requires `Authorization: Bearer <token>`) |

The `/oauth/token` endpoint implements the RFC 6749 Resource Owner Password
Credentials grant and is the `tokenUrl` wired into Swagger's **Authorize**
button. Access tokens are **short-lived by default (15 minutes)** and must be
sent as `Authorization: Bearer <token>`.

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

uvicorn src.main:app --reload
```

### With Docker Compose
```bash
cp .env.example .env
docker compose up --build

# in a separate terminal, once the api/db containers are up:
docker compose exec api alembic upgrade head
docker compose exec api python -m scripts.seed_data
```

- API docs (Swagger UI): `http://localhost:8000/docs` — click **Authorize**
  and enter credentials to call `/auth/me` from the docs.
- Health check: `http://localhost:8000/health` (liveness) / `http://localhost:8000/ready` (readiness, checks DB)
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
(register/login/OAuth2 token/me), health/readiness, and every Inventory and
Orders endpoint. Run the **Login** request once and the returned token is
stored in the `{{accessToken}}` collection variable for protected calls.

### Running Tests
```bash
pip install -r requirements.txt
pytest
```

Tests:
- `tests/identity/test_auth_endpoints.py` — **integration tests** for the
  auth endpoints. They run the real FastAPI app end-to-end (routing,
  middleware, exception handlers, database) against an in-memory SQLite DB
  injected via a dependency override, so no PostgreSQL is needed.
- `tests/identity/test_password_hasher.py` — bcrypt hashing/salt/verify unit tests.
- `tests/identity/test_jwt_service.py` — JWT signing, expiry, tamper/signature/audience rejection.
- `tests/inventory/test_product_domain.py` — existing domain unit tests (still passing).

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

## Roadmap (from project brief)
- [x] DDD bounded contexts + layered architecture
- [x] Structured JSON logging
- [x] DB migrations + seed data
- [x] Health/readiness endpoints
- [x] OAuth2.0 password grant + JWT short-lived access tokens + standardized errors (Week 3)
- [ ] Sliding-window refresh tokens
- [ ] RBAC permission checks (roles already issued in tokens)
- [ ] Event-driven order pipeline (RabbitMQ/Kafka) with DLQ + retry logic
- [ ] CQRS: write-optimized DB + read-optimized store
- [ ] Redis-backed token bucket rate limiter (from scratch)
- [ ] OpenTelemetry distributed tracing (Jaeger/Zipkin)
- [ ] Chaos engineering resilience tests
