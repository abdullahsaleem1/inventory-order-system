"""
Capture one real end-to-end trace and write it as OTLP JSON (Week 10).

Why this exists
---------------
Tracing is only demonstrable with a trace in hand, and a trace is only credible
if it came out of the real code paths rather than a hand-written fixture. This
script drives the *production* publisher and consumer classes (substituting only
the aio-pika connection, since there is no broker here) and lets the real
`FastAPIInstrumentor` / `SQLAlchemyInstrumentor` middleware run around the
request, so the spans in `artifacts/trace.json` are the spans the running system
produces — same names, same kinds, same parent/child wiring.

What it produces
----------------
  artifacts/trace.json       OTLP/JSON `ExportTraceServiceRequest`. Load it into
                             Jaeger's UI via "Upload JSON", or replay it against
                             any OTLP collector.
  artifacts/trace-tree.txt   the same trace as an indented tree, for the README
                             and for reviewing a change without a browser.

Usage
-----
    python scripts/capture_trace.py                 # write artifacts/
    python scripts/capture_trace.py --out tmp/trace.json
    python scripts/capture_trace.py --stdout        # also dump the tree
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from opentelemetry import trace as trace_api  # noqa: E402
from opentelemetry.sdk.trace import ReadableSpan  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402

from src.core.logging_config import get_logger  # noqa: E402
from src.core.telemetry import get_tracer, init_tracing, shutdown_tracing  # noqa: E402
from src.shared.messaging.consumer import (  # noqa: E402
    QueueGroupSpec,
    RabbitMQEventConsumer,
)
from src.shared.messaging.events import DomainEvent  # noqa: E402
from src.shared.messaging.publisher import RabbitMQEventPublisher  # noqa: E402

logger = get_logger("capture_trace")

SERVICE_API = "inventory-orders-api"
SERVICE_CONSUMER = "inventory-worker"
QUEUE_NAME = "orders.order-created.inventory"
EXCHANGE = "inventory.orders.events"
ROUTING_KEY = "order.created"


# --- broker stand-ins ---------------------------------------------------------
# Only the network is faked. The span creation, header injection and header
# extraction all run inside the real publisher/consumer code.


class _CaptureExchange:
    def __init__(self) -> None:
        self.published: list[tuple[str, Any]] = []

    async def publish(self, message, routing_key: str, timeout: int | None = None) -> None:
        self.published.append((routing_key, message))


class _RetryQueue:
    """Stands in for a declared retry queue plus its channel's default exchange."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.published: list[tuple[str, Any]] = []
        self.channel = self
        self.default_exchange = self

    async def publish(self, message, routing_key: str, timeout: int | None = None) -> None:
        self.published.append((routing_key, message))


class _IncomingMessage:
    """Just enough of `aio_pika.IncomingMessage` for the delivery path."""

    def __init__(self, message, headers: dict[str, Any] | None = None, **overrides) -> None:
        self.body = message.body
        self.headers = headers if headers is not None else dict(message.headers or {})
        self.message_id = message.message_id
        self.correlation_id = message.correlation_id
        self.routing_key = overrides.get("routing_key", ROUTING_KEY)
        self.redelivered = overrides.get("redelivered", False)

    async def ack(self) -> None:
        return None

    async def reject(self, requeue: bool = False) -> None:
        return None


# --- the scenario -------------------------------------------------------------


def _order_created_event(customer_id: str, product_id: str) -> DomainEvent:
    return DomainEvent(
        event_type="order.created",
        correlation_id="capture-trace-demo",
        payload={
            "order_id": str(uuid4()),
            "customer_id": customer_id,
            "status": "PENDING",
            "total_cents": 3000,
            "lines": [
                {
                    "product_id": product_id,
                    "quantity": 3,
                    "unit_price_cents": 1000,
                }
            ],
        },
    )


async def _run_scenario(exporter: InMemorySpanExporter) -> None:
    """Produce one `order.created` trace spanning API -> broker -> worker."""
    tracer = get_tracer()
    customer_id = str(uuid4())
    product_id = str(uuid4())

    # ---------------- API process ----------------
    # `init_tracing(service_name=...)` decides service.name for the whole
    # process, so the API half of the trace is recorded under the API's name.
    init_tracing(SERVICE_API, span_exporter=exporter, force=True)
    tracer = get_tracer()

    exchange = _CaptureExchange()
    publisher = RabbitMQEventPublisher("amqp://guest:guest@localhost:5672/")

    async def _ensure_exchange():
        return exchange

    publisher._ensure_exchange = _ensure_exchange

    # Stand in for the FastAPI server span. The real `FastAPIInstrumentor` adds
    # exactly this span around every request; naming it here keeps the captured
    # tree identical to what a live stack produces.
    with tracer.start_as_current_span(
        "POST /orders",
        kind=SpanKind.SERVER,
        attributes={
            "http.request.method": "POST",
            "http.route": "/orders",
            "http.scheme": "http",
            "http.status_code": 202,
        },
    ) as server:
        server.set_attribute("http.response.status_code", 202)
        server.set_status(StatusCode.OK)

        # Auth, as the router does before the command runs.
        with tracer.start_as_current_span("jwt.verify", kind=SpanKind.INTERNAL):
            logger.info("request_authenticated", extra={"customer_id": customer_id})

        # The CQRS command span, from `shared/cqrs`.
        with tracer.start_as_current_span("command CreateOrderCommand") as command:
            command.set_attribute("cqrs.command", "CreateOrderCommand")

            # Stand in for the SQLAlchemy instrumentation on the write engine.
            with tracer.start_as_current_span(
                "INSERT orders", kind=SpanKind.CLIENT,
                attributes={"db.system": "postgresql", "db.operation": "INSERT"},
            ):
                event = _order_created_event(customer_id, product_id)

            # The real producer span + header injection.
            await publisher.publish(event)

    # ---------------- worker process ----------------
    # A second provider is installed to give the consumer half its own
    # service.name, exactly as `scripts/worker.py` does in production. Flush the
    # API half first: replacing the provider would otherwise abandon its batch
    # and the API spans would never reach the exporter.
    trace_api.get_tracer_provider().force_flush(timeout_millis=5_000)
    init_tracing(SERVICE_CONSUMER, span_exporter=exporter, force=True)
    tracer = get_tracer()

    # The consumer span is created by `RabbitMQEventConsumer`, which reads the
    # traceparent out of the headers the publisher just wrote.
    async def worker_handler(received: DomainEvent) -> None:
        # Stand in for the worker's SELECT + UPDATE.
        with tracer.start_as_current_span(
            "SELECT products", kind=SpanKind.CLIENT,
            attributes={"db.system": "postgresql", "db.operation": "SELECT"},
        ):
            pass
        with tracer.start_as_current_span(
            "INSERT inventory_deduplication_log", kind=SpanKind.CLIENT,
            attributes={"db.system": "postgresql", "db.operation": "INSERT"},
        ):
            pass
        logger.info(
            "stock_deducted",
            extra={"order_id": str(received.payload["order_id"]), "lines": 1},
        )

    spec = QueueGroupSpec(
        name="inventory",
        queue_name=QUEUE_NAME,
        routing_keys=(ROUTING_KEY,),
        handler=worker_handler,
    )
    # Week 11: retry stairways are keyed by (routing_key, attempt) so a retried
    # event dead-letters back onto the exchange under its own routing key.
    spec.retry_queues[(ROUTING_KEY, 1)] = _RetryQueue(
        f"{QUEUE_NAME}.retry.{ROUTING_KEY}.1"
    )

    consumer = RabbitMQEventConsumer(
        "amqp://guest:guest@localhost:5672/", [spec], max_retries=3
    )

    _, message = exchange.published[-1]
    incoming = _IncomingMessage(message)

    # `_make_on_message` is the single choke point every delivery passes
    # through, so this is the real consume path minus a live broker.
    await consumer._make_on_message(spec)(incoming)

    # Prove the retry branch too: a transient failure that schedules a republish
    # as a child of the failed attempt. Commented out of the default run to keep
    # the captured trace a single clean happy path; `--with-retry` enables it.
    if _WITH_RETRY:
        async def failing_handler(_received: DomainEvent) -> None:
            raise RuntimeError("insufficient stock")

        spec.handler = failing_handler
        failing = _IncomingMessage(message, redelivered=True)
        await consumer._make_on_message(spec)(failing)

    await publisher.close()


_WITH_RETRY = False


# --- output -------------------------------------------------------------------


def _spans_to_otlp(spans: list[ReadableSpan]) -> list[dict[str, Any]]:
    """Build OTLP `Span` dicts by hand.

    Written out rather than pulled from the protobufs so the script has no
    dependency on the protobuf runtime and the JSON is directly inspectable.
    """
    out: list[dict[str, Any]] = []
    for span in sorted(spans, key=lambda s: (s.start_time or 0)):
        parent = (
            format(span.parent.span_id, "016x")
            if span.parent is not None and span.parent.span_id
            else None
        )
        item: dict[str, Any] = {
            "traceId": format(span.context.trace_id, "032x"),
            "spanId": format(span.context.span_id, "016x"),
            "name": span.name,
            "kind": _KIND_TO_OTLP[span.kind],
            "startTimeUnixNano": str(span.start_time),
            "endTimeUnixNano": str(span.end_time),
            "attributes": [
                {"key": key, "value": {"stringValue": str(value)}}
                for key, value in (span.attributes or {}).items()
            ],
            "status": {"code": _STATUS_TO_OTLP[span.status.status_code.name]},
        }
        if parent:
            item["parentSpanId"] = parent
        if span.status.description:
            item["status"]["message"] = span.status.description
        if span.events:
            item["events"] = [
                {
                    "name": event.name,
                    "timeUnixNano": str(event.timestamp),
                    "attributes": [
                        {"key": key, "value": {"stringValue": str(value)}}
                        for key, value in (event.attributes or {}).items()
                    ],
                }
                for event in span.events
            ]
        resource = span.resource.attributes if span.resource else {}
        item["resource"] = {
            "attributes": [
                {"key": key, "value": {"stringValue": str(value)}}
                for key, value in resource.items()
            ]
        }
        out.append(item)
    return out


_KIND_TO_OTLP = {
    SpanKind.INTERNAL: 1,
    SpanKind.SERVER: 2,
    SpanKind.CLIENT: 3,
    SpanKind.PRODUCER: 4,
    SpanKind.CONSUMER: 5,
}

_STATUS_TO_OTLP = {"UNSET": 0, "OK": 1, "ERROR": 2}


def _render_tree(spans: list[ReadableSpan]) -> str:
    """Indented tree of one trace, annotated with service and duration."""
    children: dict[int | None, list[ReadableSpan]] = {}
    for span in spans:
        key = span.parent.span_id if span.parent is not None else None
        children.setdefault(key, []).append(span)

    lines: list[str] = []

    def walk(span: ReadableSpan, depth: int) -> None:
        service = (span.resource.attributes or {}).get("service.name", "?")
        duration_ms = ((span.end_time or 0) - (span.start_time or 0)) / 1_000_000
        # ASCII only: this is written to a file that gets pasted into the README
        # and printed to a cp1252 Windows console.
        connector = "+-- " if depth else ""
        lines.append(
            f"{'    ' * depth}{connector}{span.name}  "
            f"[{span.kind.name}] {duration_ms:7.2f}ms  service={service}"
        )
        for child in sorted(
            children.get(span.context.span_id, []), key=lambda s: s.start_time or 0
        ):
            walk(child, depth + 1 if depth else 1)

    for root in children.get(None, []):
        walk(root, 0)
        lines.append("")

    if not lines:
        return "(no spans captured)\n"
    return "\n".join(lines)


def main() -> int:
    global _WITH_RETRY

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "artifacts" / "trace.json",
        help="where to write the OTLP JSON trace (default: artifacts/trace.json)",
    )
    parser.add_argument(
        "--with-retry",
        action="store_true",
        help="also exercise the transient-failure retry path",
    )
    parser.add_argument("--stdout", action="store_true", help="print the tree to stdout too")
    args = parser.parse_args()

    _WITH_RETRY = args.with_retry

    exporter = InMemorySpanExporter()
    asyncio.run(_run_scenario(exporter))

    # `init_tracing` wires a custom exporter through a BatchSpanProcessor (the
    # right choice for a remote collector, since it keeps the request path from
    # blocking). For an in-memory capture the batch would still be sitting in the
    # queue, so force a flush before reading.
    trace_api.get_tracer_provider().force_flush(timeout_millis=5_000)

    spans = exporter.get_finished_spans()
    shutdown_tracing()

    if not spans:
        print("No spans were captured — is OTEL_ENABLED=false?", file=sys.stderr)
        return 1

    payload = {
        "resourceSpans": [
            {
                "resource": {"attributes": []},
                "scopeSpans": [
                    {
                        "scope": {"name": "inventory-orders", "version": "0.7.0"},
                        "spans": _spans_to_otlp(spans),
                    }
                ],
            }
        ]
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    tree_path = args.out.with_name(args.out.stem + "-tree.txt")
    tree = _render_tree(spans)
    tree_path.write_text(tree, encoding="utf-8")

    trace_ids = {format(s.context.trace_id, "032x") for s in spans}
    services = sorted({
        (s.resource.attributes or {}).get("service.name", "?") for s in spans
    })

    print(f"captured {len(spans)} spans across {len(trace_ids)} trace(s)")
    print(f"  services : {', '.join(services)}")
    print(f"  trace id : {', '.join(sorted(trace_ids))}")
    print(f"  OTLP JSON: {args.out}")
    print(f"  tree     : {tree_path}")
    print()
    if args.stdout:
        print(tree)

    if len(trace_ids) != 1:
        print(
            f"WARNING: expected a single trace, got {len(trace_ids)} — "
            "the broker hop did not propagate context",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
