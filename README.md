# Distributed Inventory & Order Management System

A production-grade backend system built for the Parallax Labs backend internship.
Implements Domain-Driven Design, a strictly layered architecture, custom OAuth2/JWT
auth, event-driven order processing, CQRS, a from-scratch Redis rate limiter,
distributed tracing, and chaos engineering tests — containerized with Docker Compose.

## Tech Stack

- **Language/Framework:** Python 3.12, FastAPI
- **Database:** PostgreSQL (async, via SQLAlchemy 2.0 + asyncpg)
- **Migrations:** Alembic
- Additional pieces (Redis, RabbitMQ/Kafka, OpenTelemetry/Jaeger) are added
  as each corresponding weekly deliverable is implemented — see progress log below.

## Architecture

### Domain-Driven Design — Bounded Contexts

The system is split into two independent bounded contexts, each with its own
domain model, database tables, and layered stack. They do **not** import each
other's domain objects directly — cross-context communication will go through
the event pipeline (added in a later week).

src/
├── contexts/
│ ├── inventory/ # Inventory bounded context
│ │ ├── domain/ # Pure business logic (Product entity, rules)
│ │ ├── repositories/ # Persistence abstraction (domain <-> ORM mapping)
│ │ ├── services/ # Use-case orchestration
│ │ ├── controllers/ # HTTP schema <-> service translation, error mapping
│ │ ├── api/ # Routes (thin) + Pydantic schemas
│ │ └── infrastructure/ # SQLAlchemy ORM models
│ ├── orders/ # Orders bounded context (same structure)
│ └── identity/ # Minimal for now — User entity/table for seed data.
│ # Full auth (hashing, JWT, RBAC) lands in a later week.
├── shared/ # Cross-context kernel: base Entity, DB session
├── core/ # App-wide config, structured logging, request middleware, health routes
└── main.py # FastAPI app wiring

migrations/ # Alembic migrations (async, targets app's DATABASE_URL)
scripts/seed_data.py # Idempotent seed script — realistic products + role-based users
postman_collection.json # Postman/Insomnia-importable API collection

### Layered Request Flow

Every request follows the same strict path — no layer is skipped:

Route → Controller → Service → Repository → Database
(HTTP <-> domain) (business rules) (domain <-> ORM)

- **Routes**: thin FastAPI endpoint definitions, dependency-injection wiring only.
- **Controllers**: translate request/response schemas, map domain exceptions to HTTP errors.
- **Services**: orchestrate use cases; contain no business rules themselves.
- **Repositories**: only layer allowed to touch the database; maps ORM rows to domain entities.
- **Domain**: pure Python, framework-agnostic business rules (e.g. `Product.reserve_stock`).

## Getting Started

### Local (without Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # edit DATABASE_URL to point at a local Postgres

# Apply migrations
alembic upgrade head

# Seed realistic test data (60 products, 12 users across 4 roles)
python -m scripts.seed_data
# Re-run any time — it's idempotent. To wipe and reseed: python -m scripts.seed_data --reset

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

- API docs (Swagger UI): `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health` (liveness) / `http://localhost:8000/ready` (readiness, checks DB)
- **pgAdmin** (Postgres management UI): `http://localhost:5050` — log in with the credentials in `.env` (defaults: `admin@example.com` / `admin`), then register a new server with host=`db`, port=`5432`, user=`postgres`, password=`postgres`.

### Postman / Insomnia

Import [`postman_collection.json`](./postman_collection.json) — includes the health/readiness endpoints plus every Inventory and Orders endpoint. Grows incrementally each week.

### Running Tests

```bash
pip install -r requirements.txt
pytest
```

### Database Migrations

Migrations live in `migrations/versions/`, managed by Alembic and targeting the app's own `DATABASE_URL` from `.env`.

```bash
alembic upgrade head                              # apply all migrations
alembic revision --autogenerate -m "add X table"  # generate a new migration after changing ORM models
alembic downgrade -1                               # roll back one migration
```

## Weekly Progress Log

### Week 1 — Project Setup & DDD Skeleton

- Set up FastAPI project with strict layered architecture (Routes → Controllers → Services → Repositories)
- Implemented two bounded contexts: **Inventory** (Product entity with stock rules) and **Orders** (Order entity with status transitions)
- Async SQLAlchemy 2.0 setup with shared `Base`/session, separate tables per context
- Full CRUD + domain-rule endpoints for both contexts, wired end-to-end and verified via OpenAPI schema
- Domain-layer unit tests (framework-independent) for Inventory rules
- Dockerfile + docker-compose skeleton (API + Postgres)
- **Dependencies added:** fastapi, uvicorn, sqlalchemy[asyncio], asyncpg, alembic, pydantic-settings, pytest
- **How to run:** see "Getting Started" above

### Week 2 — Layered Architecture & Seed Data

**Admin feedback from Week 1 addressed:** added **pgAdmin** to `docker-compose.yml` as a Postgres management UI (`http://localhost:5050`).

- **Structured JSON logging**: replaced all ad-hoc logging with a custom `JSONFormatter` (`src/core/logging_config.py`) — every log line is a single JSON object (timestamp, level, logger, message, plus structured `extra` fields), suitable for log aggregators. A `RequestLoggingMiddleware` logs every HTTP request (method, path, status, duration, request ID) and echoes the request ID back via `X-Request-ID` for correlation. No `print()`/console-log statements anywhere in the app.
- **Database migrations**: added Alembic, configured for async SQLAlchemy against the app's own `DATABASE_URL`. Initial migration (`0001_initial_schema.py`) creates all four tables (`inventory_products`, `orders_orders`, `orders_order_lines`, `identity_users`) — verified against Alembic's migration graph.
- **Seed data script** (`scripts/seed_data.py`): generates 60 realistic products (via Faker) across 5 categories with randomized SKUs/prices/stock, and 12 users spread across 4 roles (2 ADMIN, 3 MANAGER, 3 STAFF, 4 CUSTOMER). Idempotent — safe to re-run; `--reset` flag wipes seeded tables first.
  - Added a minimal **Identity** bounded context (`domain/user.py`, `infrastructure/models.py`, `repositories/user_repository.py`) purely so there's a real `User` entity/table to seed role data into. Password hashing and auth endpoints are intentionally deferred to the OAuth2/JWT week — this context will grow there, not get replaced.
- **Health check endpoints**: `/health` (liveness — never touches the DB) and `/ready` (readiness — runs `SELECT 1` against Postgres, returns 503 if unreachable), both with typed Pydantic response models and full OpenAPI docs.
- **Postman collection** (`postman_collection.json`): started this week, covers `/health`, `/ready`, and all existing Inventory/Orders endpoints. Will grow incrementally alongside the API.
- **Dependencies added:** `faker` (seed data)
- **Verified:** all 4 domain unit tests still pass; full route set (10 endpoints incl. `/ready`) registers correctly in the OpenAPI schema; Alembic migration graph resolves cleanly (`alembic history`); JSON log formatter emits valid JSON; `docker-compose.yml` and `postman_collection.json` validated as well-formed YAML/JSON.
- **How to run:** see "Getting Started" above (now includes migration + seed steps)

<!-- Next week's entry goes here -->

## Roadmap (from project brief)

- [x] DDD bounded contexts + layered architecture
- [x] Structured JSON logging
- [x] DB migrations + seed data
- [x] Health/readiness endpoints
- [ ] Custom OAuth2.0 / JWT + sliding-window refresh tokens + RBAC
- [ ] Event-driven order pipeline (RabbitMQ/Kafka) with DLQ + retry logic
- [ ] CQRS: write-optimized DB + read-optimized store
- [ ] Redis-backed token bucket rate limiter (from scratch)
- [ ] OpenTelemetry distributed tracing (Jaeger/Zipkin)
- [ ] Chaos engineering resilience tests
- [ ] Full Docker Compose + OpenAPI/Swagger docs
