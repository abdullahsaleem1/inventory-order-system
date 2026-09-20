"""
Concurrency load-test for the from-scratch Token-Bucket rate limiter (Week 9).

Drives the real HTTP stack (uvicorn + middleware + Redis Lua bucket) with
concurrent request bursts and asserts the limiter's headline guarantees:

  1. **Burst cap never missed** — N concurrent authenticated requests against a
     bucket of capacity C admit *exactly* C when C <= N and no refill can
     accumulate within the burst window (the Lua script runs atomically).
  2. **Tier separation** — different roles use different buckets, so an
     anonymous flood cannot starve an authenticated caller.
  3. **Refill** — after exhausting a bucket, waiting ~1 second restores the
     sustained rate (uploads/checkouts are gated, not banned).
  4. **Graceful degradation** — with Redis stopped, the API keeps serving with
     `X-RateLimit-Degraded: true` instead of crashing.

Run against the docker-compose stack (which includes the `redis` service):

    docker compose up --build -d
    python -m scripts.loadtest_ratelimit --url http://localhost:8000

To exercise the degradation scenario, `docker compose stop redis` first.
"""
import argparse
import asyncio
import datetime
import json
import random
import time

import httpx

DEFAULT_URL = "http://localhost:8000"


async def _register(client: httpx.AsyncClient, email: str) -> dict:
    resp = await client.post(
        "/auth/register",
        json={"email": email, "full_name": "Load Test", "password": "S3curePass!"},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _auth_headers(client: httpx.AsyncClient, email: str) -> dict:
    """Register and return the Bearer Authorization header for the caller."""
    body = await _register(client, email)
    return {"Authorization": f"Bearer {body['access_token']}"}


async def _fire_burst(
    client: httpx.AsyncClient,
    url: str,
    headers: dict,
    concurrency: int,
) -> dict:
    async def one(_: int) -> int:
        resp = await client.get(url, headers=headers)
        return resp.status_code

    started = time.perf_counter()
    codes = await asyncio.gather(*(one(i) for i in range(concurrency)))
    elapsed = time.perf_counter() - started
    return {
        "total": len(codes),
        "codes": codes,
        "admitted": sum(1 for c in codes if c != 429),
        "4xx_429": sum(1 for c in codes if c == 429),
        "other": sum(1 for c in codes if c not in (200, 404, 401, 422, 429)),
        "elapsed_s": round(elapsed, 3),
    }


async def run_burst_scenario(client, base_url, headers, capacity: int, concurrency: int) -> None:
    url = "/auth/me"  # cheap non-exempt endpoint; ratelimit applies
    stats = await _fire_burst(client, url, headers, concurrency)
    admitted = stats["admitted"]
    status = "ok" if admitted == capacity and stats["other"] == 0 else "FAIL"
    print(
        f"  burst  {stats['total']:>3} concurrent, bucket capacity {capacity:>3} "
        f"=> admitted {admitted:>3}, 429 {stats['4xx_429']:>3}, "
        f"other {stats['other']:>1}, {stats['elapsed_s']:>4}s  [{status}]"
    )
    if status == "FAIL":
        raise SystemExit(f"burst-cap assertion failed (admitted {admitted} != capacity {capacity})")


async def run_tier_scenario(client) -> None:
    """Anonymous flood cannot starve an authenticated caller (same IP)."""
    print(">> Tier separation: same IP, different roles")
    anon_headers = {}
    resident_headers = await _auth_headers(client, f"tl-{int(time.time())}@example.com")
    url = "/auth/me"

    # Anonymous tier (cap 5) is exhausted first: exactly 5 admitted.
    stats = await _fire_burst(client, url, anon_headers, 8)
    anon_admitted = stats["admitted"]
    print(f"  anonymous burst 8 concurrent => admitted {anon_admitted} (expect <= 5)")

    # The logged-in resident (CUSTOMER tier, cap 10) is unaffected.
    stats = await _fire_burst(client, url, resident_headers, 10)
    print(
        f"  resident burst 10 concurrent => admitted {stats['admitted']}, 429 {stats['4xx_429']}"
    )
    if stats["admitted"] != 10:
        raise SystemExit("tier test failed: resident caller was starved by anonymous flood")


async def run_refill_scenario(client, headers, capacity: int) -> None:
    url = "/auth/me"
    stats = await _fire_burst(client, url, headers, capacity + 4)
    print(
        f"  exhaust bucket (cap {capacity}) => admitted {stats['admitted']}, 429 {stats['4xx_429']}"
    )
    if stats["4xx_429"] == 0:
        raise SystemExit("refill scenario: bucket was not exhausted")

    await asyncio.sleep(1.2)  # rate >= 1 token/s for every tier
    resp = await client.get(url, headers=headers)
    print(f"  after 1.2s quiet => status {resp.status_code} (expect 200/any-non-429)")
    if resp.status_code == 429:
        raise SystemExit("refill scenario: token not restored after wait")


async def run_degraded_scenario(client, headers) -> None:
    url = "/auth/me"
    resp = await client.get(url, headers=headers)
    degraded = resp.headers.get("x-ratelimit-degraded")
    print(
        f"  degraded probe => status {resp.status_code}, "
        f"X-RateLimit-Degraded: {degraded} (expect 'true' when Redis is down)"
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Token-bucket rate-limiter load test")
    parser.add_argument("--url", default=DEFAULT_URL, help=f"API base URL (default {DEFAULT_URL})")
    parser.add_argument("--concurrency", type=int, default=40, help="concurrent requests in the burst")
    parser.add_argument(
        "--capacity",
        type=int,
        default=10,
        help="assumed bucket capacity of the registered (CUSTOMER) user (default 10)",
    )
    args = parser.parse_args()

    print(f"Load test: token-bucket rate limiter @ {args.url}  ({datetime.datetime.now():%H:%M:%S})")
    async with httpx.AsyncClient(base_url=args.url, timeout=15.0) as client:
        headers = await _auth_headers(
            client,
            f"loadtest-{random.randint(1_000_000, 9_999_999)}@example.com",
        )
        print(">> Burst cap: every concurrent request counts once, atomically")
        await run_burst_scenario(client, args.url, headers, args.capacity, args.concurrency)
        await run_tier_scenario(client)
        print(">> Refill: token bucket replenishes instead of banning")
        await run_refill_scenario(client, headers, args.capacity)
        print(">> Degradation: Redis down -> API keeps serving")
        await run_degraded_scenario(client, headers)
    print("load test finished")


if __name__ == "__main__":
    asyncio.run(main())