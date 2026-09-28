"""
OpenTelemetry TracerProvider bootstrap (Week 10 — Distributed Tracing).

One process == one service, and this module is the single place that decides
what that service is called and where its spans go. Every entrypoint
(`src.main`, `scripts.consume_orders`, `scripts.worker`, `scripts.read_projector`)
calls `init_tracing()` exactly once, as early as possible, and `shutdown_tracing()`
on the way out so the BatchSpanProcessor flushes instead of dropping the last
spans in the process.

Design decisions worth knowing:

* **Tracing is opt-out, not opt-in.** `OTEL_ENABLED=false` short-circuits
  `init_tracing()` and every `tracer.start_as_current_span(...)` in the codebase
  then degrades to a no-op context manager, because the OTel API hands out
  `NoOpTracer` until a provider is installed. So there is exactly one branch to
  test and no per-call-site conditionals.
* **No endpoint != no exporter, not no tracing.** With neither
  `OTEL_EXPORTER_OTLP_ENDPOINT` nor `OTEL_CONSOLE_EXPORTER` set we still install
  a real TracerProvider with a Resource, so trace/span ids exist, W3C
  `traceparent` still propagates across the broker, and every JSON log line
  still carries `trace_id`. Spans are simply dropped at export time — that
  keeps the test suite (which imports `src.main`) from shelling out to a
  collector that isn't there.
* **Sampling is parent-based.** A worker that continues a trace the API
  started must never be dropped for being "unsampled" locally, so the default
  is `parentbased_always_on`.
* **Batch for remote, simple for console.** Remote exporters get a
  `BatchSpanProcessor` so the request path never blocks on the collector.
  The console exporter gets a `SimpleSpanProcessor` so lines appear in
  emission order, which is what makes the no-collector output readable.
"""
from __future__ import annotations

import atexit
import os
import socket
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)
from opentelemetry.trace import SpanKind, Tracer

from src.core.config import get_settings
from src.core.logging_config import get_logger

logger = get_logger("telemetry.setup")

# Tracer name used for hand-written spans. Library instrumentations use their
# own ("opentelemetry.instrumentation.fastapi", ...), so this one is easy to
# spot in the Jaeger UI as "ours".
TRACER_NAME = "inventory-orders"

_provider: TracerProvider | None = None
_atexit_registered = False


# --- resource -----------------------------------------------------------------


def build_resource(service_name: str, service_version: str) -> Resource:
    """Describe *this* process to the collector.

    `service.name` is the axis Jaeger groups by, so the four deployables
    (api / order-persistence-consumer / inventory-worker / read-projector) must
    each pass their own. `service.instance.id` distinguishes replicas so
    scaling a worker out shows up as separate nodes in one service.
    """
    settings = get_settings()
    return Resource.create(
        {
            # Resource.create() gives OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME
            # precedence over these defaults, which is what we want.
            "service.name": service_name,
            "service.version": service_version,
            "service.instance.id": f"{service_name}-{socket.gethostname()}-{os.getpid()}",
            "service.namespace": settings.APP_NAME,
            "deployment.environment.name": settings.ENV,
            "process.pid": os.getpid(),
            # Non-standard but useful when reading a trace cold: which deployable
            # and which bounded context produced this span.
            "app.service": service_name,
            "app.component": "bounded-contexts:identity,inventory,orders",
        }
    )


# --- sampler ------------------------------------------------------------------


def build_sampler(name: str, arg: str) -> Sampler:
    """Map the OTEL_TRACES_SAMPLER env var onto a concrete Sampler."""
    match name.strip().lower():
        case "always_on":
            return ALWAYS_ON
        case "always_off":
            return ALWAYS_OFF
        case "traceidratio":
            return _root_for_ratio(arg)
        case "parentbased_always_off":
            return ParentBased(ALWAYS_OFF)
        case "parentbased_traceidratio":
            return ParentBased(_root_for_ratio(arg))
        case "parentbased_always_on" | "":
            return ParentBased(ALWAYS_ON)
        case _:
            logger.warning(
                "otel_sampler_unknown",
                extra={
                    "requested": name,
                    "fallback": "parentbased_always_on",
                    "hint": "valid: always_on, always_off, traceidratio, parentbased_*",
                },
            )
            return ParentBased(ALWAYS_ON)


def _parse_float(raw: str, default: float) -> float:
    """Read a sampling ratio, clamping to [0.0, 1.0]."""
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return min(max(value, 0.0), 1.0)


def _is_float(raw: str) -> bool:
    try:
        float(raw)
    except (TypeError, ValueError):
        return False
    return True


def _root_for_ratio(raw: str) -> Sampler:
    """Root sampler for a `*traceidratio` name, degrading to always-on.

    A ratio we cannot read must never silently become a *lower* rate, so fall
    back to sampling everything and say so in the logs. `ratio(1.0)` and
    `always_on` behave identically, but `always_on` is what we actually meant,
    and it is what an operator will see in the Jaeger sampler field.
    """
    if not _is_float(raw):
        if raw is not None and str(raw).strip():
            logger.warning(
                "otel_sampler_arg_invalid",
                extra={
                    "requested": raw,
                    "fallback": "always_on",
                    "hint": "expected a number between 0.0 and 1.0",
                },
            )
        return ALWAYS_ON
    return TraceIdRatioBased(_parse_float(raw, default=1.0))


# --- exporter -----------------------------------------------------------------


def build_exporter() -> tuple[SpanExporter | None, str]:
    """Resolve (exporter, protocol) from settings.

    Returns `(None, "none")` when there is nowhere to send spans, which is a
    supported mode, not an error.
    """
    settings = get_settings()
    protocol = (settings.OTEL_EXPORTER_OTLP_PROTOCOL or "grpc").strip().lower()

    if settings.OTEL_CONSOLE_EXPORTER:
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        return ConsoleSpanExporter(), "console"

    endpoint = (settings.OTEL_EXPORTER_OTLP_ENDPOINT or "").strip()
    if not endpoint:
        return None, "none"

    timeout = settings.OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS
    match protocol:
        case "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            # gRPC wants host:port; the exporter tolerates (and strips) the scheme
            # when it is present.
            return OTLPSpanExporter(endpoint=endpoint, timeout=timeout), "grpc"
        case "http/protobuf" | "http" | "https":
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )

            # The HTTP exporter needs the FULL path. Appending it when missing is
            # the single most common OTLP misconfiguration, so be forgiving.
            if not endpoint.rstrip("/").endswith("/v1/traces"):
                endpoint = f"{endpoint.rstrip('/')}/v1/traces"
            return OTLPSpanExporter(endpoint=endpoint, timeout=timeout), "http/protobuf"
        case _:
            logger.warning(
                "otel_protocol_unknown",
                extra={
                    "requested": protocol,
                    "fallback": "grpc",
                    "hint": "valid: grpc, http/protobuf",
                },
            )
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            return OTLPSpanExporter(endpoint=endpoint, timeout=timeout), "grpc"


# --- lifecycle ----------------------------------------------------------------


def init_tracing(
    service_name: str | None = None,
    *,
    span_exporter: SpanExporter | None = None,
    force: bool = False,
) -> Tracer | None:
    """Install the global TracerProvider for this process.

    Idempotent: the second and later calls are no-ops and return a tracer for the
    already-installed provider, so importing two modules that both bootstrap
    tracing is harmless.

    `span_exporter` bypasses exporter resolution entirely — that is how the test
    suite captures spans in an `InMemorySpanExporter` without a collector.
    `force=True` replaces an existing provider (tests, and re-running the
    capture script in one process).
    """
    global _provider

    settings = get_settings()

    if not settings.OTEL_ENABLED:
        logger.info("otel_disabled", extra={"reason": "OTEL_ENABLED=false"})
        return None

    if _provider is not None and not force:
        return trace.get_tracer(TRACER_NAME)

    # W3C trace-context is the whole point (it is what we stash in AMQP headers),
    # so set the propagator explicitly rather than relying on API defaults.
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )

    name = _resolve_service_name(service_name)
    provider = TracerProvider(
        resource=build_resource(name, settings.OTEL_SERVICE_VERSION),
        sampler=build_sampler(settings.OTEL_TRACES_SAMPLER, settings.OTEL_TRACES_SAMPLER_ARG),
    )

    exporter, protocol = (span_exporter, "custom") if span_exporter is not None else build_exporter()
    if exporter is not None:
        if protocol == "console":
            # Immediate, ordered output beats batched when a human is reading.
            provider.add_span_processor(SimpleSpanProcessor(exporter))
        else:
            provider.add_span_processor(BatchSpanProcessor(exporter))

    _install_provider(provider)
    _provider = provider

    # Losing the last few seconds of a trace because the process exited is the
    # classic way to "not see" a span that really happened. Register once, so a
    # forced re-init does not stack up handlers.
    global _atexit_registered

    if not _atexit_registered:
        atexit.register(shutdown_tracing)
        _atexit_registered = True

    logger.info(
        "otel_initialized",
        extra={
            "service_name": name,
            "exporter": protocol,
            "endpoint": settings.OTEL_EXPORTER_OTLP_ENDPOINT or None,
            "sampler": settings.OTEL_TRACES_SAMPLER,
            "instance_id": provider.resource.attributes.get("service.instance.id"),
        },
    )
    return trace.get_tracer(TRACER_NAME)


def _resolve_service_name(default: str | None) -> str:
    """Pick this process's `service.name`.

    An explicitly-set `OTEL_SERVICE_NAME` always wins, so docker-compose can
    relabel a service without a code change. Otherwise the caller-supplied
    default applies — that is how the three worker entrypoints name themselves
    without depending on env plumbing — and finally the settings default.
    """
    return (
        os.environ.get("OTEL_SERVICE_NAME", "").strip()
        or (default or "").strip()
        or get_settings().OTEL_SERVICE_NAME
    )


def _install_provider(provider: TracerProvider) -> None:
    """Set the global provider, replacing any previous one.

    `trace.set_tracer_provider` is deliberately one-shot — it guards the
    assignment with a `sync.Once` and only logs a warning on the second call,
    which makes re-initialisation impossible. Tests (and re-running the capture
    script inside one process) need to swap the provider, so clear the cache and
    the once-guard first. This mirrors what OpenTelemetry's own test suite does;
    it is internal API and pinned to the versions in requirements.txt.
    """
    from opentelemetry import trace as trace_api

    trace_api._TRACER_PROVIDER = None
    once = getattr(trace_api, "_TRACER_PROVIDER_SET_ONCE", None)
    if once is not None:
        once._done = False
    trace_api.set_tracer_provider(provider)


def shutdown_tracing(timeout_millis: int = 5_000) -> None:
    """Flush pending spans. Safe to call more than once."""
    global _provider
    provider = _provider
    if provider is None:
        return
    try:
        provider.force_flush(timeout_millis=timeout_millis)
        provider.shutdown()
    except Exception as exc:  # never let telemetry teardown break shutdown
        logger.warning("otel_shutdown_failed", extra={"error": str(exc)})
    finally:
        _provider = None


def is_tracing_active() -> bool:
    """True when a real (non-no-op) TracerProvider is installed."""
    return _provider is not None


def get_tracer(name: str = TRACER_NAME) -> Tracer:
    """Tracer for hand-written spans.

    Safe to call before `init_tracing()`: the API returns a `ProxyTracer` that
    becomes a real tracer once a provider is installed, and stays `NoOpTracer`
    when tracing is off.
    """
    return trace.get_tracer(name)


# --- span helpers -------------------------------------------------------------

# Re-exported so call sites read as `with span(..., kind=SpanKind.PRODUCER):`
# and never have to import opentelemetry directly.
INTERNAL = SpanKind.INTERNAL
SERVER = SpanKind.SERVER
CLIENT = SpanKind.CLIENT
PRODUCER = SpanKind.PRODUCER
CONSUMER = SpanKind.CONSUMER

StatusCode = trace.StatusCode


def set_span_attributes(span: trace.Span, **attributes: Any) -> None:
    """`set_attribute` a dict, skipping None/empty so spans stay uncluttered."""
    for key, value in attributes.items():
        if value is None or value == "":
            continue
        if isinstance(value, (str, bool, int, float)):
            span.set_attribute(key, value)
        elif isinstance(value, (list, tuple)):
            span.set_attribute(key, [str(item) for item in value])
        else:
            span.set_attribute(key, str(value))
