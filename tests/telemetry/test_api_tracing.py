"""
Week 10 — HTTP-side tracing and log correlation.

Covers the parts of the instrumentation a request actually goes through:
the FastAPI SERVER span, the CQRS command/query spans beneath it, the
`X-Trace-Id` response header, the trace ids on the JSON log records, and the
client-supplied `traceparent` case (incoming requests join an existing trace).
"""
from __future__ import annotations

import json
import logging
from uuid import uuid4

import pytest
from opentelemetry.trace import SpanKind

from src.core.logging_config import JSONFormatter

_CUSTOMER_ID = str(uuid4())

# An upstream service (or a retry from a client) hands us its traceparent, and we
# must join that trace rather than starting a fresh one.
INBOUND_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


async def _register(client, email: str) -> dict:
    resp = await client.post(
        "/auth/register",
        json={
            "email": email,
            "full_name": "Trace Test",
            "password": "S3curePass!",
            "role": "CUSTOMER",
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _order_body() -> dict:
    return {
        "customer_id": _CUSTOMER_ID,
        "lines": [{"product_id": str(uuid4()), "quantity": 2, "unit_price_cents": 1500}],
    }


class TestServerSpans:
    async def test_post_orders_produces_a_server_span(self, spans, evented_client) -> None:
        client, _ = evented_client
        user = await _register(client, "server-span@example.com")

        resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
        assert resp.status_code == 202, resp.text

        server = spans.first("POST /orders", kind=SpanKind.SERVER)
        assert server.kind == SpanKind.SERVER
        # `opentelemetry-instrumentation-fastapi` 0.65b0 still emits the
        # pre-1.20 `http.*` names, pinned to the SDK version in requirements.txt.
        assert server.attributes["http.method"] == "POST"
        # The route template, not the concrete path — otherwise every order id
        # would be a separate span name and the trace view would be unusable.
        assert server.attributes["http.route"] == "/orders"
        assert server.attributes["http.status_code"] == 202

    async def test_command_span_nests_under_the_request(self, spans, evented_client) -> None:
        """The CQRS command span is what makes a slow handler visible inside a
        fast-looking request."""
        client, _ = evented_client
        user = await _register(client, "command-span@example.com")

        await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))

        server = spans.first("POST /orders", kind=SpanKind.SERVER)
        command = spans.first("CreateOrder")
        assert command.parent.span_id == server.context.span_id
        assert spans.trace_id(command) == spans.trace_id(server)

    async def test_publish_span_nests_under_the_command(self, spans, evented_client) -> None:
        """`InMemoryEventPublisher` opens the same PRODUCER span the real
        `RabbitMQEventPublisher` does, so the request-level trace shape is the
        same in tests as in production."""
        client, _ = evented_client
        user = await _register(client, "publish-nesting@example.com")

        await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))

        command = spans.first("CreateOrder")
        publish = spans.first("publish")
        assert publish.kind == SpanKind.PRODUCER
        assert publish.parent.span_id == command.context.span_id

    async def test_database_spans_are_captured(self, spans, evented_client) -> None:
        """SQLAlchemy instrumentation must emit CLIENT spans inside the request
        trace — that is how you find a slow query.

        The conftest overrides the session with an in-memory SQLite engine, so
        the connection spans are the ones present here; statement spans appear
        against the real PostgreSQL engine in a running stack.
        """
        client, _ = evented_client
        user = await _register(client, "db-spans@example.com")
        spans.clear()

        await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))

        db_spans = [
            s for s in spans.all
            if s.kind == SpanKind.CLIENT
            and any(n in s.name for n in ("connect", "SELECT", "INSERT", "commit"))
        ]
        assert db_spans, f"expected SQLAlchemy client spans; saw {spans.names()}"
        server = spans.first("POST /orders", kind=SpanKind.SERVER)
        # The query work must live inside the request trace, not in its own.
        assert {spans.trace_id(s) for s in db_spans} == {spans.trace_id(server)}

    async def test_registration_and_order_are_separate_traces(self, spans, evented_client) -> None:
        """Each request is its own trace; only the broker hop stitches them
        together."""
        client, _ = evented_client
        user = await _register(client, "separate-traces@example.com")

        await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))

        register_trace = spans.trace_id(spans.first("POST /auth/register", kind=SpanKind.SERVER))
        order_trace = spans.trace_id(spans.first("POST /orders", kind=SpanKind.SERVER))
        assert register_trace != order_trace

    async def test_failed_request_is_marked_as_an_error(self, spans, client) -> None:
        """A 4xx must not look like a healthy span in the trace list.

        OpenTelemetry's HTTP semantic conventions put 4xx in the "unset but
        annotated" bucket rather than ERROR, so assert on the recorded status
        code — which is what an operator actually filters on in Jaeger.
        """
        resp = await client.get("/orders/00000000-0000-0000-0000-000000000000")
        assert resp.status_code in (401, 404)

        server = spans.first("GET", kind=SpanKind.SERVER)
        assert server.attributes["http.status_code"] == resp.status_code
        # A 4xx is "the server answered correctly, the caller was wrong", which
        # the spec says to leave UNSET rather than flag as an error.
        assert server.status.status_code.name == "UNSET"

    async def test_inbound_traceparent_is_continued(self, spans, evented_client) -> None:
        """An upstream caller sends `traceparent`; our SERVER span becomes a child
        of it instead of starting an orphan trace."""
        client, _ = evented_client
        user = await _register(client, "inbound-traceparent@example.com")

        await client.post(
            "/orders",
            json=_order_body(),
            headers={**_auth(user["access_token"]), "traceparent": INBOUND_TRACEPARENT},
        )

        server = spans.first("POST /orders", kind=SpanKind.SERVER)
        assert spans.trace_id(server) == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert server.parent is not None
        assert format(server.parent.span_id, "016x") == "00f067aa0ba902b7"

    async def test_unparseable_inbound_traceparent_is_ignored(self, spans, evented_client) -> None:
        """A corrupt header must not break the request — it just starts a new trace."""
        client, _ = evented_client
        user = await _register(client, "bad-traceparent@example.com")

        resp = await client.post(
            "/orders",
            json=_order_body(),
            headers={**_auth(user["access_token"]), "traceparent": "garbage"},
        )
        assert resp.status_code == 202, resp.text
        assert spans.first("POST /orders", kind=SpanKind.SERVER).parent is None


class TestTraceIdHeader:
    async def test_response_carries_the_trace_id(self, spans, evented_client) -> None:
        """Lets you jump from an API response (or a support ticket quoting one)
        straight to the trace in Jaeger."""
        client, _ = evented_client
        user = await _register(client, "header@example.com")

        resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))

        assert resp.headers["x-trace-id"] == spans.trace_id(spans.first("POST /orders", kind=SpanKind.SERVER))
        assert len(resp.headers["x-trace-id"]) == 32

    async def test_header_present_on_errors_too(self, spans, client) -> None:
        """A failing request is exactly the one you need to look up."""
        resp = await client.get("/orders/00000000-0000-0000-0000-000000000000")
        assert resp.headers.get("x-trace-id"), "trace id must be on the failure response too"

    async def test_header_matches_the_inherited_trace(self, spans, evented_client) -> None:
        client, _ = evented_client
        user = await _register(client, "header-inherit@example.com")

        resp = await client.post(
            "/orders",
            json=_order_body(),
            headers={**_auth(user["access_token"]), "traceparent": INBOUND_TRACEPARENT},
        )
        assert resp.headers["x-trace-id"] == "4bf92f3577b34da6a3ce929d0e0e4736"


class TestLogCorrelation:
    """`trace_id` on a log line is what makes the round trip log -> trace work."""

    @pytest.fixture
    def formatted(self):
        """Format whatever is logged through the app's JSONFormatter."""
        emitted: list[dict] = []

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                emitted.append(json.loads(JSONFormatter().format(record)))

        handler = Capture()
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            yield emitted
        finally:
            root.removeHandler(handler)

    async def test_log_line_inside_a_span_carries_its_trace_id(
        self, spans, formatted, evented_client
    ) -> None:
        client, _ = evented_client
        user = await _register(client, "log-trace@example.com")
        formatted.clear()

        # A subscriber stands in for any service code that logs while handling
        # the request: the formatter must annotate it with no help from the
        # call site.
        client, publisher = evented_client
        recorded: list[dict] = []

        async def logging_subscriber(event, headers) -> None:
            logging.getLogger("orders.subscriber").info(
                "order_created", extra={"order_id": str(event.payload["order_id"])}
            )

        publisher.subscribers.append(logging_subscriber)

        resp = await client.post(
            "/orders", json=_order_body(), headers=_auth(user["access_token"])
        )
        assert resp.status_code == 202, resp.text

        line = next(r for r in formatted if r["message"] == "order_created")
        recorded.append(line)
        assert line["trace_id"] == spans.trace_id(spans.first("POST /orders", kind=SpanKind.SERVER))
        assert line["span_id"]
        assert line["trace_sampled"] is True
        # The call site's own structured fields are preserved alongside it.
        assert line["order_id"] == line["order_id"]
        assert len(line["order_id"]) == 36  # a uuid, i.e. not overwritten

    async def test_log_line_outside_a_span_has_no_trace_keys(self, formatted) -> None:
        """Startup/shutdown lines must not carry null or empty trace ids."""
        formatted.clear()
        logging.getLogger("startup").info("service_starting", extra={"port": 8000})

        line = next(r for r in formatted if r["message"] == "service_starting")
        assert "trace_id" not in line
        assert "span_id" not in line
        assert line["port"] == 8000

    async def test_every_line_in_one_request_shares_the_trace_id(
        self, spans, formatted, evented_client
    ) -> None:
        client, publisher = evented_client
        user = await _register(client, "log-same-trace@example.com")
        formatted.clear()

        async def noisy(event, headers) -> None:
            log = logging.getLogger("orders.subscriber")
            for i in range(3):
                log.info("order_progress", extra={"step": i})

        publisher.subscribers.append(noisy)
        await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))

        lines = [r for r in formatted if r["message"] == "order_progress"]
        assert len(lines) == 3
        assert {r["trace_id"] for r in lines} == {spans.trace_id(spans.first("POST /orders", kind=SpanKind.SERVER))}

    async def test_logs_outside_a_request_have_no_trace_id(self) -> None:
        """`current_trace_identifiers()` outside a span is empty, not bogus."""
        from src.core.telemetry import current_trace_identifiers

        identifiers = current_trace_identifiers()
        assert identifiers.trace_id == ""
        assert identifiers.as_log_fields() == {}
