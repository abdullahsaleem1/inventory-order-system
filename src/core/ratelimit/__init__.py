"""
Rate limiting (Week 9).

A Redis-backed **Token Bucket** rate limiter implemented from scratch (no
pre-built middleware library). Bucket state lives in Redis (hash per key,
updated atomically by a Lua script we wrote); when Redis is unreachable the
limiter falls back to a per-process in-memory bucket so the API stays up in a
degraded state instead of crashing.

Packages:
- ``policy``  — role-based tiers (ADMIN gets the highest limits).
- ``token_bucket`` — the from-scratch token-bucket algorithm; Redis + in-memory
  backends behind one ``TokenBucket`` protocol.
- ``provider`` — process-wide singleton wiring (build/close/get), Redis probe.
- ``middleware`` — FastAPI middleware that resolves the caller (JWT or IP) and
  enforces the tiered limits, returning 429 with the standard error envelope.
"""