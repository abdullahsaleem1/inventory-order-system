"""
Week 10 — trace-context propagation over the message broker.

The message broker is the one hop OpenTelemetry does not instrument for us, so
these tests cover the hand-written glue that carries a trace from the API
process into the worker process: W3C `traceparent` written into AMQP message
headers, read back out, and used as the parent context of the consumer's span.
"""
import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from src.core.telemetry import (
    current_trace_identifiers,
    extract_trace_context,
    get_tracer,
    inject_trace_headers,
)

TRACEPARENT = "traceparent"
TRACESTATE = "tracestate"

# A syntactically valid W3C traceparent: 32 hex trace id, 16 hex span id, flags.
SAMPLE_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def _parent_of(headers: dict) -> str | None:
    """Span id a new span would be parented onto, from the context in `headers`.

    Reads the resulting span's *parent*, not its own id: a span created from a
    remote context is a fresh child with a freshly generated span id, and the
    remote id is what proves extraction worked.
    """
    context = extract_trace_context(headers)
    with get_tracer().start_as_current_span("probe", context=context) as span:
        if span.parent is None:
            return None
        return format(span.parent.span_id, "016x")


#: the span id inside SAMPLE_TRACEPARENT
SAMPLE_SPAN_ID = "00f067aa0ba902b7"


class TestInjection:
    def test_injects_traceparent_inside_an_active_span(self, spans) -> None:
        with get_tracer().start_as_current_span("orders.create") as span:
            headers = inject_trace_headers({})

        expected_parent = format(span.get_span_context().span_id, "016x")
        version, trace_id, parent_id, flags = headers[TRACEPARENT].split("-")

        assert version == "00"
        assert trace_id == spans.trace_id(span)
        assert parent_id == expected_parent
        # W3C trace-flags: bit 0 = sampled. Bit 1 (random) may also be set, so
        # assert the sampled bit rather than the whole byte.
        assert int(flags, 16) & 0x01 == 0x01

    def test_injection_is_additive(self, spans) -> None:
        """The publisher already writes x-event-version; injection must not
        clobber it, and must not drop it."""
        with get_tracer().start_as_current_span("orders.create"):
            headers = inject_trace_headers({"x-event-version": 1})

        assert headers["x-event-version"] == 1
        assert TRACEPARENT in headers

    def test_injection_outside_a_span_is_a_no_op(self) -> None:
        assert inject_trace_headers({"x-event-version": 1}) == {"x-event-version": 1}

    def test_injection_returns_the_same_dict_it_was_given(self, spans) -> None:
        carrier = {"x-event-version": 1}
        with get_tracer().start_as_current_span("orders.create"):
            assert inject_trace_headers(carrier) is carrier


class TestExtraction:
    @pytest.mark.parametrize("key", [TRACEPARENT, "TraceParent", "TRACEPARENT"])
    def test_header_lookup_is_case_insensitive(self, key) -> None:
        """AMQP field tables are not case-normalised, so neither is the lookup."""
        assert _parent_of({key: SAMPLE_TRACEPARENT}) == "00f067aa0ba902b7"

    def test_bytes_keys_and_values_are_decoded(self) -> None:
        headers = {b"traceparent": SAMPLE_TRACEPARENT.encode("utf-8")}
        assert _parent_of(headers) == "00f067aa0ba902b7"

    def test_field_table_list_values_are_accepted(self) -> None:
        """RabbitMQ decodes some field-table types as a list."""
        headers = {TRACEPARENT: [SAMPLE_TRACEPARENT]}
        assert _parent_of(headers) == "00f067aa0ba902b7"

    def test_tracestate_is_preserved(self) -> None:
        headers = {TRACEPARENT: SAMPLE_TRACEPARENT, TRACESTATE: "vendor=value"}
        context = extract_trace_context(headers)
        span_context = trace.get_current_span(context).get_span_context()
        assert span_context.trace_state.to_header() == "vendor=value"

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="no-headers"),
            pytest.param(None, id="none"),
            pytest.param({"x-retry-count": 1}, id="unrelated-headers"),
            pytest.param({TRACEPARENT: "garbage"}, id="malformed-traceparent"),
            pytest.param({TRACEPARENT: ""}, id="empty-traceparent"),
            pytest.param(
                {TRACEPARENT: "00-" + "0" * 32 + "-00f067aa0ba902b7-01"},
                id="all-zero-trace-id",
            ),
            pytest.param(
                {TRACEPARENT: "00-4bf92f3577b34da6a3ce929d0e0e4736-" + "0" * 16 + "-01"},
                id="all-zero-span-id",
            ),
        ],
    )
    def test_missing_or_invalid_context_yields_a_new_root_trace(self, headers) -> None:
        """Messages predating instrumentation, or a corrupt header, must not
        crash the consumer — they simply start a fresh root trace."""
        context = extract_trace_context(headers)
        assert not dict(context), "an unusable header must not produce a context"

        with get_tracer().start_as_current_span("orphan", context=context) as span:
            assert span.get_span_context().is_valid
            assert span.parent is None


class TestRoundTrip:
    def test_producer_and_consumer_share_one_trace(self, spans) -> None:
        """The headline behaviour: publish on one side, consume on the other,
        and both spans belong to the same trace with the right ancestry.

        This is what makes a single Jaeger trace span the API, the queue and the
        worker even though the work happens in a different process.
        """
        # --- producer side (API process) ---
        with get_tracer().start_as_current_span("POST /orders", kind=SpanKind.SERVER) as server:
            with get_tracer().start_as_current_span(
                "inventory.orders.events publish", kind=SpanKind.PRODUCER
            ) as producer:
                headers = inject_trace_headers({"x-event-version": 1})

        # --- consumer side (worker process) ---
        with get_tracer().start_as_current_span(
            "orders.order-created.inventory process",
            context=extract_trace_context(headers),
            kind=SpanKind.CONSUMER,
        ) as consumer:
            with get_tracer().start_as_current_span("SELECT products"):
                pass

        assert spans.trace_id(server) == spans.trace_id(consumer)
        # consumer's parent is the producer span that created the message
        assert consumer.parent.span_id == producer.context.span_id
        assert producer.parent.span_id == server.context.span_id
        # the DB work is a child of the consumer
        db_span = spans.first("SELECT products")
        assert db_span.parent.span_id == consumer.context.span_id

    def test_nested_spans_appear_after_their_parents(self, spans) -> None:
        """Span completion order is children-first; Jaeger rebuilds the tree from
        parent ids, so this is only a sanity check that nesting really happened."""
        with get_tracer().start_as_current_span("outer"):
            with get_tracer().start_as_current_span("inner"):
                pass

        assert spans.names() == ["inner", "outer"]


class TestCurrentIdentifiers:
    def test_empty_outside_a_span(self) -> None:
        identifiers = current_trace_identifiers()
        assert identifiers.trace_id == ""
        assert identifiers.trace_id == "" or identifiers.traceparent == ""
        assert identifiers.as_log_fields() == {}

    def test_populated_inside_a_span(self, spans) -> None:
        with get_tracer().start_as_current_span("probe") as span:
            identifiers = current_trace_identifiers()
            assert identifiers.trace_id == spans.trace_id(span)
            assert identifiers.span_id == format(span.get_span_context().span_id, "016x")
            assert identifiers.sampled is True
            assert identifiers.as_log_fields()["trace_id"] == spans.trace_id(span)

    def test_traceparent_is_well_formed(self, spans) -> None:
        with get_tracer().start_as_current_span("probe") as span:
            traceparent = current_trace_identifiers().traceparent

        version, trace_id, span_id, flags = traceparent.split("-")
        assert version == "00"
        assert trace_id == spans.trace_id(span)
        assert span_id == format(span.get_span_context().span_id, "016x")
        assert len(trace_id) == 32 and len(span_id) == 16
        assert int(flags, 16) & 0x01 == 0x01
