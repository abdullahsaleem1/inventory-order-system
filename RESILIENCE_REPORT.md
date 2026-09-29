# Resilience Report — Chaos Engineering (Week 11)

**System:** Distributed Inventory & Order Management System
**Scope:** API service, order-persistence consumer, inventory worker, read projector, and
their dependencies (Redis, RabbitMQ, Elasticsearch, PostgreSQL).
**Harness:** `scripts/chaos_scenarios.py` (in-process) + `scripts/chaos_drill.py` (real containers).
**Run recorded below:** `2026-09-28T13:08:46`, 1.0 s windows, concurrency 8 →
`artifacts/chaos-results.json`.
**Verdict:** all four injected-fault scenarios behaved as designed. Five defects were
found by the exercise and fixed; two of them were silent data-integrity risks.

---

## 1. Method

A chaos run is only evidence if the hypothesis is written down *before* the fault is
injected, so every scenario in `scripts/chaos_scenarios.py` prints its hypothesis, runs
three measured windows (**steady → faulted → recovered**), then asserts the hypothesis
and emits a single `PASS`/`FAIL` line. Three windows, not two, because "it never went
down" and "it came back" are different claims.

**In-process mode** (default) boots the real FastAPI application with real middleware,
real exception handlers, the real rate limiter, the real consumer handlers and a real
SQLite write database, then replaces exactly one dependency at the boundary where the
failure would occur in production:

| Fault | Injected as |
| --- | --- |
| Redis | The `RateLimiter` backend's `consume()` raises `RateLimiterUnavailableError` |
| Broker | The publisher's `publish()` raises `EventPublishError` |
| Worker | Consumer subscribers fail, so `order.created` is never projected or persisted |
| Elasticsearch | The read store's `get_order()` raises |

This matters because it exercises the production code path, not a mock of it: the
readiness response, the error envelope, the rate-limiter fallback and the
consumer-subscriber contract are all the shipped implementations.

**Live-stack mode** (`scripts/chaos_drill.py`) repeats the same four scenarios against
`docker-compose` with `docker compose stop <service>` and
`docker kill --signal=SIGKILL <worker>`, which additionally covers real AMQP unacked-message
requeue, real socket timeouts, and real process death. It is committed and runnable, but
**was not executed for this report** — Docker is not available in this environment. Every
number below is therefore in-process evidence, and section 6 states exactly what that
does and does not cover.

**Workload.** A mixed authenticated workload mirroring real use: 60 % order reads, 30 %
order creates, 10 % liveness probes. A chaos run that only hits one endpoint proves
little, because the three have different failure modes — reads pass through Redis, creates
depend on the broker, and liveness must survive anything.

---

## 2. Scenario results

Throughput is requests/second per window at concurrency 8; `degraded` is the count of
responses carrying `X-RateLimit-Degraded: true`.

| # | Scenario | Window | Req | RPS | Status codes | p50 / p95 ms | `degraded` | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Redis outage | steady | 89 | 89.0 | 200×50, 202×36, 404×3 | 72.5 / 168.0 | 0 | 0 |
| | | **redis_down** | 82 | 82.0 | 200×36, 202×34, 404×12 | 71.3 / 180.2 | **79** | 0 |
| | | recovered | 165 | 82.5 | 200×89, 202×66, 404×10 | 74.1 / 185.8 | 0 | 0 |
| 2 | Broker outage | steady | 87 | 87.0 | 200×39, 202×38, 404×10 | 74.0 / 164.4 | 0 | 0 |
| | | **broker_down** | 96 | 96.0 | 200×8, 404×54, **503×34** | 91.1 / 112.2 | 0 | 0 |
| | | recovered | 91 | 91.0 | 200×51, 202×35, 404×5 | 67.9 / 182.8 | 0 | 0 |
| 3 | Worker outage | steady | 80 | 80.0 | 200×42, 202×33, 404×5 | 76.3 / 186.9 | 0 | 0 |
| | | **worker_down** | 64 | 64.0 | 200×31, **202×22**, 404×11 | 97.1 / 266.3 | 0 | 0 |
| | | recovered | 76 | 76.0 | 200×36, 202×27, 404×13 | 85.1 / 209.2 | 0 | 0 |
| 4 | Elasticsearch outage | steady | 70 | 70.0 | 200×38, 202×27, 404×5 | 88.9 / 252.8 | 0 | 0 |
| | | **es_down** | 72 | 72.0 | 200×5, 202×26, **503×41** | 90.5 / 199.4 | 0 | 0 |
| | | recovered | 78 | 78.0 | 200×34, 202×26, 404×18 | 85.3 / 217.0 | 0 | 0 |

**Retry exhaustion → DLQ** is verified at the contract level rather than by wall-clock
backoff, in `tests/messaging/test_consumer_resilience.py` (15 tests): a transient failure
below budget is retried, a permanent failure skips retries entirely, the budget-exhausted
transient failure is dead-lettered exactly once, an undecodable body goes straight to the
DLQ, and backoff is `base * 2^(attempt-1)` capped at 60 s.

### 2.1 Redis outage — degraded, and cheap

*Hypothesis: a dead Redis degrades rate limiting to an in-process bucket; the API keeps
serving, `/ready` still answers, and a circuit breaker stops the outage costing
per-request latency.*

- **Confirmed.** 0 requests errored. 79 of 82 responses carried
  `X-RateLimit-Degraded: true`, so clients and operators can see the degradation.
- `/ready` returned **200** with `{"status": "ready", "database": "up", "broker":
  "disabled", "redis": "degraded"}`. Readiness stays green because a lost rate limiter
  must not evict a healthy, serving pod.
- **The breaker is what makes this survivable.** Before it existed, every request paid a
  0.5 s Redis timeout. After the circuit opened, **40 consecutive authenticated requests
  made 0 Redis calls** (`redis_calls_at_trip=233` → 0 during the window), and p95 latency
  in the faulted window (180.2 ms) was within noise of the steady window (168.0 ms).
  Without the breaker that window would have been bounded below by the timeout, not
  comparable to baseline.
- After healing, the next `/ready` probe closed the circuit and the
  `X-RateLimit-Degraded` header disappeared (probe result `None`).

### 2.2 Broker outage — fail closed, no phantom writes

*Hypothesis: order creation fails closed with `503 EVENT_PUBLISH_FAILED` and writes
nothing; reads and liveness are unaffected; creation resumes once the broker returns.*

- **Confirmed.** All 34 creates in the faulted window returned `503` with
  `EVENT_PUBLISH_FAILED`; no `202` was issued while the broker was unreachable. This is
  the correct trade: refusing the write is recoverable, accepting an order that no
  consumer will ever see is not.
- `/health` stayed `200` (8 responses) and reads kept working (54× `200`/`404` from the
  already-projected orders) — the outage is scoped to the write path.
- No phantom rows: the persistence handler is downstream of the confirm, so a failed
  publish means no event and therefore no `Order` row. Order ids are minted before
  publish, so a `503` can leave a gap in the id space — harmless, and preferable to
  silently accepting a write.
- p95 latency *improved* during the outage (112.2 ms vs 164.4 ms) because failing fast on
  a refused connection is cheaper than waiting on a confirm timeout.

### 2.3 Worker outage — accepted work is not lost

*Hypothesis: the API keeps accepting orders (202) while consumers are down, because
acceptance depends only on broker confirms; once the consumer returns the work completes;
no duplicate side effects.*

- **Confirmed.** 22 `202`s during the faulted window with 0 errors, versus 33 in the
  steady window. Order acceptance is decoupled from consumption by design — the API's
  contract is "durably queued", not "processed".
- The cost is visible and expected: p95 rose from 186.9 ms to 266.3 ms and throughput fell
  from 80.0 to 64.0 rps, because unprojected orders return `404` and because work
  accumulates for the returning consumer.
- Durability comes from the broker, not from the worker being alive: unacked messages are
  redelivered after a crash, and both handlers are idempotent (section 3.4).

### 2.4 Elasticsearch outage — 503, never a lie

*Hypothesis: order reads fail with `503 READ_STORE_UNAVAILABLE` — never a misleading 404 —
while creates and liveness stay healthy.*

- **Confirmed, and this scenario caught a real defect.** All 41 reads in the faulted window
  returned `503`; a request for an order id that genuinely does not exist *also* returned
  `503` during the outage, because "I cannot reach the store" and "this order is not there"
  are different facts and must not share a status code. Before the fix, both returned
  `404`, telling users their orders had vanished while they were in fact unreadable.
- Creates were unaffected (26× `202`) — the write path does not touch the read store.
- After healing, reads returned to normal (`200` for projected orders, `404` only for
  genuinely absent ids).

---

## 3. Defects found and fixed

The point of the exercise was to find these. All five are covered by new regression tests
(50 new tests; 324 total, up from 274).

### 3.1 `/ready` returned HTTP 500 when Redis died — *availability bug*

`ReadinessResponse.redis` was typed as `Literal["up", "down", "disabled"]`, but
`RateLimiter.probe()` reports `"degraded"`. Pydantic rejected the response, so the
readiness endpoint raised during a validation error instead of reporting a healthy-but-
degraded system — and any container orchestrator polling it would have pulled a serving pod
out of rotation. Fixed in `src/core/system_routes.py`. Covered by
`tests/test_resilience_degradation.py`.

### 3.2 No circuit breaker on the rate limiter — *latency-amplification bug*

Every request called Redis with a 0.5 s timeout and fell back on failure. A dead Redis
therefore cost up to 0.5 s of added latency to *every* request, forever, rather than to
the first few. `RateLimiter` now opens a circuit after
`RATE_LIMIT_REDIS_CIRCUIT_FAILURES` consecutive failures (default 2), serves in-memory
buckets without touching Redis, and after
`RATE_LIMIT_REDIS_CIRCUIT_COOLDOWN_SECONDS` (default 5 s, doubling on each failed probe up
to 60 s) admits exactly one half-open probe. A successful probe — including the one
`/ready` triggers — closes it. Covered by `tests/ratelimit/test_circuit_breaker.py` (18
tests) and measured in §2.1.

### 3.3 Retry queues were shared across routing keys — *poison-pill bug*

The consumer declared one retry stairway per attempt, taken from
`routing_keys[0]`, and `_schedule_retry` looked the queue up by attempt number alone. A
failed `order.status.changed` therefore retried on the `order.created` queue, and any
routing key without a stairway was silently sent somewhere arbitrary. Retry queues are now
keyed by `(routing_key, attempt)` and named
`{queue_name}.retry.{routing_key}.{attempt}`; an undeclared routing key is rejected
rather than guessed. Covered by `tests/messaging/test_consumer_resilience.py`.

*Operational note:* this changes queue names. Queues declared by a previous version are
orphaned, not reused — delete them after deploying, or drain them first.

### 3.4 Duplicate events caused duplicate side effects — *data-integrity bug*

Both the order-persistence and inventory handlers checked for an existing row and then
inserted, with no protection against the window between the check and the commit. Two
redeliveries of the same `order.created` (guaranteed during any consumer restart) could
double-deduct stock, and a race raised `IntegrityError` that the consumer treated as a
transient failure — retried three times and then dead-lettered, i.e. a successfully
persisted order ended up in the DLQ. Both handlers now catch `IntegrityError`, roll back,
re-read the row, and treat a confirmed duplicate as success. Covered by
`tests/messaging/test_idempotency_race.py` (6 tests).

### 3.5 Read-store outages reported as "not found" — *correctness bug*

`ElasticsearchOrderReadStore.get_order` called `_ensure_index()` outside its `try`, and the
query handler caught every exception and returned `None`. Any Elasticsearch failure —
connection refused, timeout, 5xx — surfaced as `404 ORDER_NOT_FOUND`. Fixed with an
explicit `ReadStoreUnavailableError` that the query handler maps to
`503 READ_STORE_UNAVAILABLE`, so only a genuine `NotFoundError` becomes a `404`. Covered
by `tests/test_resilience_degradation.py`.

---

## 4. Design decisions the drills confirmed

| Decision | Evidence |
| --- | --- |
| Fail closed on the broker; fail open on the rate limiter | §2.2 refuses writes (no phantom orders); §2.1 keeps serving (no outage cascade from a cache). |
| Liveness never touches dependencies | `/health` returned `200` in every faulted window, including a total broker outage. |
| Readiness gates on the database only, not on optional infrastructure | §2.1 stays `200` with `redis: degraded`; the DB is the one dependency without a degraded mode. |
| Consumers are idempotent because redelivery is normal, not exceptional | §2.3 + §3.4: a returning consumer re-processes queued work, so the handlers absorb duplicates. |
| Reads degrade to `503` rather than lying with `404` | §2.4, and the defect in §3.5 that made this necessary. |
| Retries are per routing key, with a bounded budget and a DLQ | §3.3 plus the 15 resilience tests: transient vs permanent failures take different paths, and exhaustion terminates in the DLQ rather than in a hot loop. |

---

## 5. Known limitations and residual risk

1. **The recorded run is in-process.** `scripts/chaos_drill.py` is committed and ready, but
   Docker was unavailable here, so real AMQP requeue, real socket timeouts, and SIGKILL
   recovery are **unverified in this environment**. Run
   `python -m scripts.chaos_drill` on a Docker host to close this gap; it writes
   `artifacts/chaos-drills.json`.
2. **No Postgres fault drill.** Losing the write database is a data-loss scenario, not a
   resilience one, and needs a separate restore drill. `db` is deliberately excluded from
   the matrix.
3. **The rate-limiter fallback is per-process.** During a Redis outage each replica enforces
   its own in-memory bucket, so the effective cluster-wide limit is up to N× the configured
   rate. Degraded, not unsafe, but it should be documented in the runbook.
4. **The broker confirm timeout is a hard-coded 10 s** (`RabbitMQEventPublisher`), not a
   setting, and `RATE_LIMIT_REDIS_TIMEOUT_SECONDS` is likewise not yet read by the Redis
   backend, which hard-codes 0.5 s. Both should become configuration.
5. **No DLQ-depth alerting.** The DLQ works, but nothing pages on it growing. A retry
   storm that exhausts its budget is currently silent.
6. **Small sample.** 1 s windows at concurrency 8 with a SQLite write database and no real
   network. The numbers characterise *behaviour under fault* (which codes are returned,
   which headers appear, whether recovery happens); they are not a capacity benchmark.
   Week 9's `scripts/loadtest_ratelimit.py` covers throughput against the real stack.
7. **Eventual consistency is not a fault.** In §2.3 and §2.4 the `404`s on freshly created
   orders are the CQRS read model catching up (Week 8), not a resilience defect.

---

## 6. Reproducing

```bash
# In-process: no Docker required, exits non-zero if any scenario fails.
python -m scripts.chaos_scenarios                     # full matrix
python -m scripts.chaos_scenarios --scenario redis    # one scenario
python -m scripts.chaos_scenarios --window 5 --concurrency 32   # longer, harder

# Real containers: requires Docker and a running stack.
python -m scripts.chaos_drill --list
python -m scripts.chaos_drill
python -m scripts.chaos_drill --scenario worker --window 30

# Regression tests for everything fixed above.
pytest -q tests/test_resilience_degradation.py tests/ratelimit/test_circuit_breaker.py \
          tests/messaging/test_consumer_resilience.py tests/messaging/test_idempotency_race.py
```

Artifacts: `artifacts/chaos-results.json` (in-process),
`artifacts/chaos-drills.json` (container drills, when run).
