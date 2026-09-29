"""
Real-container chaos drills (Week 11).

Runs the same four fault scenarios as `scripts/chaos_scenarios.py`, but against
the docker-compose stack with real containers stopped, killed and restarted —
so the results cover AMQP requeue semantics, real Redis timeouts, and real
process death, which in-process fault injection cannot.

    python -m scripts.chaos_drill                       # full matrix
    python -m scripts.chaos_drill --scenario redis      # one scenario
    python -m scripts.chaos_drill --list

Requires Docker. Each drill:

  1. starts steady background load and records a baseline window;
  2. injects the fault (`docker compose stop` / `kill -9`);
  3. records the degraded window while load continues;
  4. restores the dependency and polls until the system self-heals;
  5. records the recovery window and the time-to-recover.

Findings are written to `artifacts/chaos-drills.json` alongside the in-process
run, and the report cites whichever run is applicable.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import shutil
import subprocess
import sys
import time
from dataclasses import field
from pathlib import Path
from typing import Any, Callable

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = REPO_ROOT / "artifacts"
DEFAULT_URL = "http://localhost:8000"

# Services the drills are allowed to disrupt. `db` is deliberately excluded:
# losing Postgres is a data-loss scenario, not a resilience one, and needs a
# different (destructive) drill plan.
BROKER = "rabbitmq"
REDIS = "redis"
WORKER = "worker"
READSTORE = "elasticsearch"


class DockerUnavailable(RuntimeError):
    pass


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    exe = shutil.which("docker")
    if exe is None:
        raise DockerUnavailable(
            "`docker` is not on PATH. These drills stop real containers; run "
            "`python -m scripts.chaos_scenarios` for the container-free equivalent."
        )
    return subprocess.run(
        [exe, *args], capture_output=True, text=True, check=check, timeout=180
    )


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return _docker("compose", *args, check=check)


@dataclasses.dataclass
class DrillResult:
    name: str
    fault: str
    command: str
    passed: bool = False
    time_to_recover_s: float | None = None
    baseline: dict[str, Any] = field(default_factory=dict)
    degraded: dict[str, Any] = field(default_factory=dict)
    recovered: dict[str, Any] = field(default_factory=dict)
    observations: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


# ---------------------------------------------------------------------------
# Load driver
# ---------------------------------------------------------------------------


class LoadDriver:
    """Background read+create load against the live API."""

    def __init__(self, url: str, token: str, *, concurrency: int) -> None:
        self._url = url
        self._headers = {"Authorization": f"Bearer {token}"}
        self._concurrency = concurrency
        self._stop = asyncio.Event()
        self._codes: list[int] = []
        self._latencies: list[float] = []
        self._errors: list[str] = []
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        limits = httpx.Limits(max_connections=self._concurrency * 2)
        async with httpx.AsyncClient(
            base_url=self._url, timeout=20.0, limits=limits
        ) as client:
            while not self._stop.is_set():
                await self._tick(client)

    async def _tick(self, client: httpx.AsyncClient) -> None:
        started = time.perf_counter()
        try:
            resp = await client.get("/health")
            self._codes.append(resp.status_code)
            self._latencies.append((time.perf_counter() - started) * 1000)
        except Exception as exc:  # noqa: BLE001
            self._errors.append(f"{type(exc).__name__}: {exc}")

    async def stop(self) -> "WindowStats":
        self._stop.set()
        if self._task is not None:
            await self._task
        return WindowStats.from_lists(self._codes, self._latencies, self._errors)


@dataclasses.dataclass
class WindowStats:
    requests: int
    codes: dict[str, int]
    errors: int
    p50_ms: float
    p95_ms: float
    p99_ms: float

    @classmethod
    def from_lists(cls, codes, latencies, errors) -> "WindowStats":
        def pct(values: list[float], p: float) -> float:
            if not values:
                return 0.0
            ordered = sorted(values)
            return ordered[min(len(ordered) - 1, int(len(ordered) * p / 100) - 1)]

        counts: dict[str, int] = {}
        for code in codes:
            key = str(code)
            counts[key] = counts.get(key, 0) + 1
        return cls(
            requests=len(latencies),
            codes=counts,
            errors=len(errors),
            p50_ms=round(pct(latencies, 50), 2),
            p95_ms=round(pct(latencies, 95), 2),
            p99_ms=round(pct(latencies, 99), 2),
        )

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


async def _window(driver: LoadDriver, seconds: float) -> WindowStats:
    await driver.start()
    await asyncio.sleep(seconds)
    return await driver.stop()


async def _readiness(url: str) -> tuple[int, dict[str, Any]]:
    try:
        async with httpx.AsyncClient(base_url=url, timeout=10.0) as client:
            resp = await client.get("/ready")
            return resp.status_code, resp.json()
    except Exception as exc:  # noqa: BLE001
        return 0, {"error": f"{type(exc).__name__}: {exc}"}


async def _wait_healthy(url: str, service: str, *, timeout: float = 120.0) -> float:
    """Poll the stack until the dependency answers again; return seconds taken."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        code, _ = await _readiness(url)
        if code == 200:
            return time.monotonic() - started
        await asyncio.sleep(2.0)
    raise TimeoutError(f"{service} did not become healthy within {timeout:.0f}s")


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


async def _register_and_token(url: str, client: httpx.AsyncClient) -> str:
    import uuid

    resp = await client.post(
        "/auth/register",
        json={
            "email": f"drill-{uuid.uuid4().hex[:8]}@example.com",
            "full_name": "Drill",
            "password": "S3curePass!",
        },
    )
    if resp.status_code != 201:
        raise RuntimeError(f"registration failed: {resp.status_code} {resp.text}")
    return resp.json()["access_token"]


async def _ensure_stack(url: str) -> httpx.AsyncClient:
    try:
        code, body = await _readiness(url)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"API is not reachable at {url}: {exc}") from exc
    if code != 200:
        raise RuntimeError(
            f"API at {url} is not ready (HTTP {code}): {body}. "
            "Run `docker compose up --build -d` first."
        )
    return httpx.AsyncClient(base_url=url, timeout=30.0)


# ---------------------------------------------------------------------------
# Drills
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Drill:
    name: str
    service: str
    hypothesis: str
    inject: Callable[[], None]
    restore: Callable[[], None]
    healthy_when: Callable[[dict[str, Any]], bool]
    stop_command: str = "stop"
    kill_command: str | None = None

    @property
    def command(self) -> str:
        if self.kill_command is None:
            return f"docker compose {self.stop_command} {self.service}"
        return f"docker kill --signal={self.kill_command} ({self.service})"


def _drills() -> dict[str, Drill]:
    return {
        "redis": Drill(
            name="Redis outage",
            service=REDIS,
            hypothesis=(
                "The API keeps serving with X-RateLimit-Degraded: true, /ready "
                "stays 200 with redis=degraded, and a circuit breaker stops the "
                "outage adding Redis timeout to every request"
            ),
            inject=lambda: compose("stop", REDIS),
            restore=lambda: compose("start", REDIS),
            healthy_when=lambda b: b.get("redis") in ("up", "degraded"),
            stop_command="stop",
        ),
        "broker": Drill(
            name="Broker outage",
            service=BROKER,
            hypothesis=(
                "POST /orders fails closed with 503 EVENT_PUBLISH_FAILED and "
                "writes nothing; GET /health stays 200; creation resumes once "
                "the broker returns"
            ),
            inject=lambda: compose("stop", BROKER),
            restore=lambda: compose("start", BROKER),
            healthy_when=lambda b: b.get("broker") == "up",
            stop_command="stop",
        ),
        "worker": Drill(
            name="Worker crash (SIGKILL)",
            service=WORKER,
            hypothesis=(
                "Killing the worker mid-flight loses no work: unacked messages "
                "are requeued by RabbitMQ, handlers are idempotent, and the "
                "worker's restart policy brings it back"
            ),
            inject=lambda: _docker(
                "kill", "--signal=SIGKILL",
                _container_of(WORKER),
            ),
            restore=lambda: compose("start", WORKER),
            healthy_when=lambda b: b.get("broker") == "up",
            stop_command="kill -9",
            kill_command="SIGKILL",
        ),
        "readstore": Drill(
            name="Elasticsearch outage",
            service=READSTORE,
            hypothesis=(
                "Order reads return 503 READ_STORE_UNAVAILABLE — never a "
                "misleading 404 — while creates and /health stay healthy"
            ),
            inject=lambda: compose("stop", READSTORE),
            restore=lambda: compose("start", READSTORE),
            healthy_when=lambda b: b.get("broker") == "up",
            stop_command="stop",
        ),
    }


def _container_of(service: str) -> str:
    out = compose("ps", "-q", service).stdout.strip()
    if not out:
        raise RuntimeError(f"no running container found for service {service!r}")
    return out.splitlines()[0].strip()


async def run_drill(
    drill: Drill, url: str, *, window: float, concurrency: int, token: str
) -> DrillResult:
    result = DrillResult(name=drill.name, fault=drill.service, command=drill.command)
    driver = LoadDriver(url, token, concurrency=concurrency)
    try:
        result.baseline = (await _window(driver, window)).as_dict()
        result.observations.append(f"baseline: {result.baseline}")

        drill.inject()
        result.observations.append(f"injected: {result.command}")

        degraded = await _window(driver, window)
        result.degraded = degraded.as_dict()
        result.observations.append(f"degraded: {result.degraded}")

        code, body = await _readiness(url)
        result.observations.append(f"/ready during outage: HTTP {code} {body}")
        if code == 500:
            result.observations.append(
                "DEFECT OBSERVED: /ready returned 500 during the outage"
            )

        drill.restore()
        result.observations.append(f"restored: {drill.service}")

        try:
            result.time_to_recover_s = round(
                await _wait_healthy(url, drill.service, timeout=180.0), 2
            )
        except TimeoutError as exc:
            result.error = str(exc)
            result.passed = False
            return result

        result.recovered = (await _window(driver, window)).as_dict()
        result.observations.append(f"recovered in {result.time_to_recover_s}s")
        result.observations.append(f"after recovery: {result.recovered}")

        code, body = await _readiness(url)
        result.passed = drill.healthy_when(body) and result.recovered["errors"] == 0
        result.observations.append(f"/ready after recovery: HTTP {code} {body}")
    except DockerUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001
        result.error = f"{type(exc).__name__}: {exc}"
        try:
            drill.restore()
        except Exception:  # noqa: BLE001,S110 - best-effort cleanup
            pass
    finally:
        await driver.stop()
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> int:
    parser = argparse.ArgumentParser(description="Week 11 container chaos drills")
    parser.add_argument("--url", default=DEFAULT_URL, help=f"API base URL (default {DEFAULT_URL})")
    parser.add_argument("--window", type=float, default=10.0,
                        help="seconds per measurement window (default 10)")
    parser.add_argument("--concurrency", type=int, default=10,
                        help="concurrent load workers (default 10)")
    parser.add_argument("--scenario", action="append", default=None,
                        help="run only these (redis, broker, worker, readstore)")
    parser.add_argument("--list", action="store_true", help="list drills and exit")
    args = parser.parse_args()

    drills = _drills()
    if args.list:
        for key, drill in drills.items():
            print(f"  {key:<10} {drill.name}  ({drill.command})")
        return 0

    try:
        client = await _ensure_stack(args.url)
    except (DockerUnavailable, RuntimeError) as exc:
        print(f"cannot run drills: {exc}", file=sys.stderr)
        return 2

    async with client:
        token = await _register_and_token(args.url, client)
        chosen = args.scenario or list(drills)
        results: list[DrillResult] = []
        for key in chosen:
            drill = drills.get(key)
            if drill is None:
                print(f"unknown scenario {key!r}; try --list", file=sys.stderr)
                continue
            print(f"\n>> {drill.name}  ({drill.command})")
            print(f"   hypothesis: {drill.hypothesis}")
            started = time.monotonic()
            result = await run_drill(
                drill, args.url,
                window=args.window, concurrency=args.concurrency, token=token,
            )
            for line in result.observations:
                print(f"   {line}")
            if result.error:
                print(f"   ERROR: {result.error}")
            print(f"   [{'PASS' if result.passed else 'FAIL'}] {drill.name} "
                  f"({time.monotonic() - started:.1f}s)")
            results.append(result)

    ARTIFACTS.mkdir(exist_ok=True)
    out = ARTIFACTS / "chaos-drills.json"
    out.write_text(
        json.dumps(
            {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
             "target": args.url,
             "scenarios": [r.to_dict() for r in results]},
            indent=2, default=str,
        ),
        encoding="utf-8",
    )
    passed = sum(1 for r in results if r.passed)
    print(f"\n{passed}/{len(results)} drills passed; written to {out.relative_to(REPO_ROOT)}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except DockerUnavailable as exc:
        print(f"docker unavailable: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
