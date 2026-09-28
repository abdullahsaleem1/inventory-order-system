"""
W3C trace-context propagation across process boundaries (Week 10).

HTTP is handled for free — `FastAPIInstrumentor` reads `traceparent` off the
inbound request and writes it onto the outbound response. The message broker is
not instrumented by any OTel package, so the hop that actually matters for this
system (API -> RabbitMQ -> worker) is done by hand, in four small functions.

The mechanism: the active span context is serialised into a W3C `traceparent`
header (and `tracestate` if present) and written into the AMQP **message
headers**. On the consuming side those headers are read back and used as the
parent context, so the consumer's span is a child of the producer's span and
Jaeger draws one continuous trace across both processes.

Why headers and not the message body: the `DomainEvent` envelope is a
deliberately small, language-neutral contract (see `shared/messaging/events.py`).
Baking OpenTelemetry fields into it would couple every producer and consumer to
this tracing library forever. AMQP headers are already there, already
`str -> Any`, and can carry transport metadata without polluting the payload.

Failure mode is deliberately silent. A message that predates this change, or
comes from a service with tracing off, simply has no `traceparent`; `extract`
then returns an empty context and the consumer starts a fresh root trace rather
than crashing. Queues routinely contain both kinds of message after a rolling
deploy, so "no parent is normal" has to be a first-class case.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter, Setter

from src.core.config import get_settings

# AMQP field-table keys are strings; these are the W3C header names, lowercased,
# which is what TraceContextTextMapPropagator looks for by default.
TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"


class _AmqpHeaderSetter(Setter):
    """Write propagator output into a plain dict of AMQP message headers."""

    def set(self, carrier: dict[str, Any], key: str, value: str) -> None:
        carrier[key.lower()] = value


class _AmqpHeaderGetter(Getter):
    """Read propagator input out of an incoming message's headers.

    `Getter.get` returns a **list** of values, not a single string: HTTP allows
    a header to repeat and the propagators are written against that contract
    (`header[0]`). AMQP field tables are single-valued in practice, so the list
    always has zero or one element.

    AMQP decodes field-table keys as `str` or `bytes` depending on broker and
    client version, so keys are normalised (lowercased) and values coerced to
    `str`. list/tuple values — RabbitMQ's representation for some field-table
    types, and how it round-trips a comma-separated `tracestate` — are passed
    through as multiple values rather than being flattened.
    """

    def get(self, carrier: dict[str, Any], key: str) -> list[str] | None:
        wanted = key.lower()
        for actual_key, value in (carrier or {}).items():
            normalised = (
                actual_key.decode("utf-8", "ignore")
                if isinstance(actual_key, bytes)
                else str(actual_key)
            ).lower()
            if normalised != wanted:
                continue
            if value is None:
                return None
            if isinstance(value, bytes):
                return [value.decode("utf-8", "ignore")]
            if isinstance(value, (list, tuple)):
                return [str(item) for item in value]
            return [str(value)]
        return None

    def keys(self, carrier: dict[str, Any]) -> Iterable[str]:
        return [
            key.decode("utf-8", "ignore") if isinstance(key, bytes) else str(key)
            for key in (carrier or {})
        ]


_SETTER = _AmqpHeaderSetter()
_GETTER = _AmqpHeaderGetter()

_INVALID_TRACE_ID = 0
_INVALID_SPAN_ID = 0


# --- current context ----------------------------------------------------------


@dataclass(frozen=True)
class TraceIdentifiers:
    """The ids needed to correlate a log line, a span, or a human ticket."""

    trace_id: str
    span_id: str
    trace_flags: str
    tracestate: str
    sampled: bool

    @property
    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"

    def as_log_fields(self) -> dict[str, Any]:
        """Fields for `logger.info(..., extra=...)`; empty when there is no span."""
        if not self.trace_id:
            return {}
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "trace_sampled": self.sampled,
        }


EMPTY_IDENTIFIERS = TraceIdentifiers(
    trace_id="", span_id="", trace_flags="00", tracestate="", sampled=False
)


def current_trace_identifiers() -> TraceIdentifiers:
    """Snapshot the active span context as hex ids.

    Returns `EMPTY_IDENTIFIERS` when no span is recording, which is the normal
    state for background startup/shutdown work and for a completely
    un-instrumented code path.
    """
    context = trace.get_current_span().get_span_context()
    if not context.is_valid:
        return EMPTY_IDENTIFIERS
    return TraceIdentifiers(
        trace_id=format(context.trace_id, "032x"),
        span_id=format(context.span_id, "016x"),
        trace_flags=f"{int(context.trace_flags):02x}",
        tracestate=_format_tracestate(getattr(context, "trace_state", None)),
        sampled=bool(context.trace_flags & trace.TraceFlags.SAMPLED),
    )


def _format_tracestate(state: Any) -> str:
    """Render a TraceState object as its `key=value,key=value` header form."""
    if not state:
        return ""
    if isinstance(state, str):
        return state
    try:
        # TraceState is iterable of (key, value) members.
        return ",".join(f"{key}={value}" for key, value in state)
    except TypeError:  # pragma: no cover - unexpected TraceState shape
        return ""


def current_traceparent() -> str | None:
    """Serialised W3C `traceparent` for the active span, or None if there isn't one."""
    ids = current_trace_identifiers()
    return ids.traceparent or None


# --- injection / extraction ---------------------------------------------------


def inject_trace_headers(headers: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return `headers` with the active trace context written into it.

    Additive: existing keys survive (the publisher already writes
    `x-event-version`, and retries add `x-retry-count`). When tracing is
    disabled, broker propagation is turned off, or no span is recording, the
    headers come back unchanged.
    """
    target = headers if headers is not None else {}
    if not get_settings().OTEL_PROPAGATE_OVER_BROKER:
        return target
    if not current_trace_identifiers().trace_id:
        return target
    from opentelemetry.propagate import inject

    inject(carrier=target, setter=_SETTER)
    return target


def extract_trace_context(headers: dict[str, Any] | None) -> Context:
    """Rebuild the producer's context from incoming AMQP headers.

    Returns an empty `Context` when the message carries no trace context, in
    which case the consumer's span becomes a new root trace. Callers pass the
    result as `context=` to `tracer.start_as_current_span(...)`.
    """
    if not headers or not get_settings().OTEL_PROPAGATE_OVER_BROKER:
        return Context()
    from opentelemetry.propagate import extract

    return extract(carrier=headers, getter=_GETTER)
