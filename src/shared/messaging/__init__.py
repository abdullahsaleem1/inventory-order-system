"""
Shared messaging kernel (Week 5 — Event-Driven Architecture, Phase 1).

Cross-context infrastructure for publishing and consuming domain events
over RabbitMQ:

- events.py     — versioned event envelope + JSON (de)serialization
- publisher.py  — EventPublisher protocol, RabbitMQ + in-memory implementations
- consumer.py   — durable queue consumer with retries, DLQ, lifecycle logging
- provider.py   — settings-driven wiring + FastAPI dependency

Every event lifecycle transition (publish, receive, retry, ack, nack,
dead-letter) is emitted as a structured JSON log line.
"""
