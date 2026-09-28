"""
Fixtures for the Week 10 tracing tests.

The TracerProvider is process-global and is installed once, when `src.main` is
imported by the parent conftest. These tests therefore *attach* an
`InMemorySpanExporter` to the live provider for the duration of one test and
detach it afterwards, rather than trying to install a second provider (the
OpenTelemetry API allows exactly one, and the SQLAlchemy / FastAPI
instrumentations are already bound to it).
"""
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


@pytest.fixture
def spans():
    """Captures every span finished inside the test into an in-memory list.

    Yields a small helper object rather than the raw exporter so assertions read
    as `spans.named("POST /orders")` instead of list comprehensions.
    """
    exporter = InMemorySpanExporter()
    multi = trace.get_tracer_provider()._active_span_processor
    original = multi._span_processors
    multi._span_processors = original + (SimpleSpanProcessor(exporter),)
    try:
        yield _SpanRecorder(exporter)
    finally:
        multi._span_processors = original


class _SpanRecorder:
    """Read-only view over the finished spans of one test."""

    def __init__(self, exporter: InMemorySpanExporter) -> None:
        self._exporter = exporter

    @property
    def all(self):
        return list(self._exporter.get_finished_spans())

    def named(self, *needles: str, kind=None):
        """Spans whose name contains every needle, in completion order.

        `kind` matters: the FastAPI instrumentation emits `POST /orders` (SERVER)
        *and* `POST /orders http receive` (INTERNAL) sub-spans, and a naive
        name match would grab the sub-span first.
        """
        return [
            span
            for span in self._exporter.get_finished_spans()
            if all(needle in span.name for needle in needles)
            and (kind is None or span.kind == kind)
        ]

    def first(self, *needles: str, kind=None):
        matches = self.named(*needles, kind=kind)
        assert matches, f"no span matching {needles}; saw {self.names()}"
        return matches[0]

    def any_of(self, *needles: str, kind=None):
        """First span matching *any* needle — for 'SELECT or INSERT' assertions."""
        matches = [span for span in self.all if any(n in span.name for n in needles)]
        if kind is not None:
            matches = [span for span in matches if span.kind == kind]
        assert matches, f"no span matching any of {needles}; saw {self.names()}"
        return matches[0]

    def names(self) -> list[str]:
        return [span.name for span in self._exporter.get_finished_spans()]

    def clear(self) -> None:
        self._exporter.clear()

    def children_of(self, parent, kind=None):
        """Direct children of `parent` — the shape Jaeger draws as nested rows."""
        return [
            s
            for s in self.all
            if s.parent is not None
            and s.parent.span_id == parent.context.span_id
            and (kind is None or s.kind == kind)
        ]

    @staticmethod
    def trace_id(span) -> str:
        return format(span.context.trace_id, "032x")

    @staticmethod
    def parent_trace_id(span) -> str | None:
        if span.parent is None:
            return None
        return format(span.parent.trace_id, "032x")
