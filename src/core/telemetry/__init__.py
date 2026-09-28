"""
Distributed tracing (Week 10 — OpenTelemetry).

Public surface:

    init_tracing()      install this process's TracerProvider
    get_tracer()        tracer for hand-written spans
    inject_trace_headers() / extract_trace_context()   bridge the message broker
    instrument_app() / instrument_engine() / instrument_logging()   auto-instrumentation

Import from here rather than from `opentelemetry` directly, so the rest of the
codebase never depends on OpenTelemetry's module layout.
"""
from src.core.telemetry.instrumentation import (
    instrument_app,
    instrument_engine,
    instrument_logging,
    instrument_read_store,
)
from src.core.telemetry.propagation import (
    TraceIdentifiers,
    current_trace_identifiers,
    current_traceparent,
    extract_trace_context,
    inject_trace_headers,
)
from src.core.telemetry.setup import (
    INTERNAL,
    PRODUCER,
    SERVER,
    CLIENT,
    CONSUMER,
    TRACER_NAME,
    StatusCode,
    build_resource,
    build_sampler,
    get_tracer,
    init_tracing,
    is_tracing_active,
    set_span_attributes,
    shutdown_tracing,
)

__all__ = [
    "CLIENT",
    "CONSUMER",
    "INTERNAL",
    "PRODUCER",
    "SERVER",
    "TRACER_NAME",
    "StatusCode",
    "TraceIdentifiers",
    "build_resource",
    "build_sampler",
    "current_trace_identifiers",
    "current_traceparent",
    "extract_trace_context",
    "get_tracer",
    "init_tracing",
    "inject_trace_headers",
    "instrument_app",
    "instrument_engine",
    "instrument_logging",
    "instrument_read_store",
    "is_tracing_active",
    "set_span_attributes",
    "shutdown_tracing",
]
