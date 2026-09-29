"""
Load-test + chaos harness (Week 11).

Drives sustained concurrent traffic against the running API while a fault is
injected, and records what the system actually did. Two modes:

* **In-process mode** (default) — boots the real FastAPI app with real
  middleware, real exception handlers and a real SQLite write DB, then injects
  faults at the dependency boundary (Redis socket, broker publisher, consumer
  handler). No Docker required, so the behaviour under fault is reproducible in
  CI and on any machine.
* **Live-stack mode** (`--url`) — same workload, but aimed at the
  docker-compose API, and paired with `scripts/chaos_drill.py` for real
  container kills.

Each scenario states a hypothesis up front, asserts it, and prints a PASS/FAIL
line, so the output doubles as the evidence table in the resilience report.
"""
from __future__ import annotations

import asyncio
import dataclasses
import random
import statistics
import sys
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

RESULTS: list[dict[str, Any]] = []


# ---------------------------------------------------------------------------
# Result recording
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ScenarioResult:
    name: str
    hypothesis: str
    passed: bool
    detail: str
    metrics: dict[str, Any] = dataclasses.field(default_factory=dict)


def record(result: ScenarioResult) -> ScenarioResult:
    RESULTS.append(dataclasses.asdict(result))
    mark = "PASS" if result.passed else "FAIL"
    print(f"  [{mark}] {result.name}: {result.detail}")
    return result


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * len(ordered))) - 1))
    return ordered[idx]


# ---------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------


class Workload:
    """A mixed read/write workload mirroring how the API is actually used.

    A chaos run that only hits one endpoint proves very little, so this mixes
    the three behaviours whose failure modes differ: an authenticated read
    (passes through Redis), an order create (depends on the broker), and a
    liveness probe (must stay up no matter what).
    """

    def __init__(self, client, token: str, *, read_ratio: float = 0.6) -> None:
        self._client = client
        self._headers = {"Authorization": f"Bearer {token}"}
        self._read_ratio = read_ratio
        self._customer_id = str(uuid.uuid4())
        self._product_id = str(uuid.uuid4())
        self._order_ids: list[str] = []

    @property
    def order_ids(self) -> list[str]:
        return list(self._order_ids)

    async def _one_request(self) -> tuple[str, int, float]:
        roll = random.random()
        started = time.perf_counter()
        if roll < 0.1:
            resp = await self._client.get("/health")
            kind = "health"
        elif roll < self._read_ratio:
            order_id = random.choice(self._order_ids) if self._order_ids else str(uuid.uuid4())
            resp = await self._client.get(f"/orders/{order_id}", headers=self._headers)
            kind = "read"
        else:
            resp = await self._client.post(
                "/orders",
                headers=self._headers,
                json={
                    "customer_id": self._customer_id,
                    "lines": [
                        {
                            "product_id": self._product_id,
                            "quantity": 1,
                            "unit_price_cents": 1000,
                        }
                    ],
                },
            )
            kind = "create"
            if resp.status_code == 202:
                self._order_ids.append(resp.json()["id"])
        degraded = resp.headers.get("x-ratelimit-degraded") == "true"
        return kind, resp.status_code, time.perf_counter() - started, degraded

    async def run(self, *, duration: float, concurrency: int) -> "WindowStats":
        """Hammer for `duration` seconds, returning per-window statistics."""
        stop = time.monotonic() + duration
        latencies: list[float] = []
        codes: Counter[int] = Counter()
        errors: list[str] = []
        kinds: Counter[str] = Counter()
        degraded_headers = 0

        async def worker() -> None:
            nonlocal degraded_headers
            while time.monotonic() < stop:
                try:
                    kind, code, elapsed, degraded = await self._one_request()
                except Exception as exc:  # noqa: BLE001 - harness must not die
                    errors.append(f"{type(exc).__name__}: {exc}")
                    continue
                latencies.append(elapsed)
                codes[code] += 1
                kinds[kind] += 1
                if degraded:
                    degraded_headers += 1

        await asyncio.gather(*(worker() for _ in range(concurrency)))
        return WindowStats(
            requests=len(latencies),
            codes=dict(codes),
            kinds=dict(kinds),
            latencies_ms=[ms * 1000 for ms in latencies],
            duration_s=duration,
            errors=errors,
            degraded_headers=degraded_headers,
        )


@dataclasses.dataclass
class WindowStats:
    requests: int
    codes: dict[int, int]
    kinds: dict[str, int]
    latencies_ms: list[float]
    duration_s: float
    errors: list[str]
    degraded_headers: int = 0

    def merged(self, other: "WindowStats") -> "WindowStats":
        merged_codes = Counter(self.codes)
        merged_codes.update(Counter(other.codes))
        merged_kinds = Counter(self.kinds)
        merged_kinds.update(Counter(other.kinds))
        return WindowStats(
            requests=self.requests + other.requests,
            codes=dict(merged_codes),
            kinds=dict(merged_kinds),
            latencies_ms=self.latencies_ms + other.latencies_ms,
            duration_s=self.duration_s + other.duration_s,
            errors=self.errors + other.errors,
            degraded_headers=self.degraded_headers + other.degraded_headers,
        )

    @property
    def rps(self) -> float:
        return self.requests / self.duration_s if self.duration_s > 0 else 0.0

    def summary(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "duration_s": round(self.duration_s, 3),
            "rps": round(self.rps, 1),
            "codes": {str(k): v for k, v in sorted(self.codes.items())},
            "kinds": dict(sorted(self.kinds.items())),
            "errors": len(self.errors),
            "sample_errors": self.errors[:3],
            "degraded_headers": self.degraded_headers,
            "p50_ms": round(percentile(self.latencies_ms, 50), 2),
            "p95_ms": round(percentile(self.latencies_ms, 95), 2),
            "p99_ms": round(percentile(self.latencies_ms, 99), 2),
            "mean_ms": round(statistics.fmean(self.latencies_ms), 2) if self.latencies_ms else 0.0,
            "max_ms": round(max(self.latencies_ms), 2) if self.latencies_ms else 0.0,
        }


def verdict(result: ScenarioResult, *, ok: bool, detail: str) -> ScenarioResult:
    result.passed = ok
    result.detail = detail
    return record(result)
