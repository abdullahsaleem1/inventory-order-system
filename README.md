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

```
src/
├── contexts/
│   ├── inventory/          # Inventory bounded context
│   │   ├── domain/         # Pure business logic (Product entity, rules)
│   │   ├── repositories/   # Persistence abstraction (domain <-> ORM mapping)
│   │   ├── services/       # Use-case orchestration
│   │   ├── controllers/    # HTTP schema <-> service translation, error mapping
│   │   ├── api/            # Routes (thin) + Pydantic schemas
│   │   └── infrastructure/ # SQLAlchemy ORM models
│   └── orders/              # Orders bounded context (same structure)
├── shared/                  # Cross-context kernel: base Entity, DB session
├── core/                    # App-wide config
└── main.py                  # FastAPI app wiring
```

### Layered Request Flow
Every request follows the same strict path — no layer is skipped:

```
Route → Controller → Service → Repository → Database
         (HTTP <-> domain)   (business rules)  (domain <-> ORM)
```

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
uvicorn src.main:app --reload
```

### With Docker Compose
```bash
cp .env.example .env
docker compose up --build
```

API docs (Swagger UI) available at `http://localhost:8000/docs` once running.

### Running Tests
```bash
pip install -r requirements.txt
pytest
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

<!-- Next week's entry goes here -->

## Roadmap (from project brief)
- [x] DDD bounded contexts + layered architecture
- [ ] Custom OAuth2.0 / JWT + sliding-window refresh tokens + RBAC
- [ ] Event-driven order pipeline (RabbitMQ/Kafka) with DLQ + retry logic
- [ ] CQRS: write-optimized DB + read-optimized store
- [ ] Redis-backed token bucket rate limiter (from scratch)
- [ ] OpenTelemetry distributed tracing (Jaeger/Zipkin)
- [ ] Chaos engineering resilience tests
- [ ] Full Docker Compose + OpenAPI/Swagger docs
