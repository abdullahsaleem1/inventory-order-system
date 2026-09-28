"""
Week 10 — the `scripts/capture_trace.py` artifact generator.

The README quotes the captured trace, so the script that produces it is part of
the deliverable. These tests keep it honest: it must keep emitting a single
trace across both services, with the broker hop stitched together, or the
documented example is fiction.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "capture_trace.py"
RENDERER = REPO_ROOT / "scripts" / "render_trace.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import render_trace  # noqa: E402


@pytest.fixture(scope="module")
def captured(tmp_path_factory):
    """Run the script once for the whole module and reuse its output."""
    out = tmp_path_factory.mktemp("trace") / "trace.json"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--out", str(out)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=180,
    )
    assert result.returncode == 0, f"capture failed:\n{result.stdout}\n{result.stderr}"
    return json.loads(out.read_text(encoding="utf-8")), out, result


def _spans(document) -> list[dict]:
    return document["resourceSpans"][0]["scopeSpans"][0]["spans"]


def _service_of(span: dict) -> str:
    return {
        a["key"]: a["value"]["stringValue"] for a in span["resource"]["attributes"]
    }.get("service.name")


def _attr(span: dict, key: str) -> str | None:
    return next(
        (a["value"]["stringValue"] for a in span["attributes"] if a["key"] == key), None
    )


def _by_name(spans: list[dict], name: str) -> dict:
    matches = [s for s in spans if s["name"] == name]
    assert matches, f"no span named {name!r}; saw {[s['name'] for s in spans]}"
    return matches[0]


class TestCapturedTrace:
    def test_script_exits_cleanly_and_reports_its_outputs(self, captured) -> None:
        _, out, result = captured
        assert "captured" in result.stdout
        assert str(out) in result.stdout

    def test_emits_exactly_one_trace(self, captured) -> None:
        document, _, _ = captured
        assert len({s["traceId"] for s in _spans(document)}) == 1, (
            "the broker hop failed to propagate context, so the trace fragmented"
        )

    def test_spans_both_deployables(self, captured) -> None:
        document, _, _ = captured
        assert {_service_of(s) for s in _spans(document)} == {
            "inventory-orders-api",
            "inventory-worker",
        }

    def test_consumer_span_is_a_child_of_the_producer_span(self, captured) -> None:
        """The single most important structural property, asserted on the
        artifact the README shows."""
        document, _, _ = captured
        spans = _spans(document)
        producer = _by_name(spans, "inventory.orders.events publish")
        consumer = _by_name(spans, "orders.order-created.inventory process")

        assert consumer["parentSpanId"] == producer["spanId"]
        assert consumer["traceId"] == producer["traceId"]
        # OTLP kind 4 = PRODUCER, 5 = CONSUMER, 2 = SERVER, 3 = CLIENT.
        assert producer["kind"] == 4
        assert consumer["kind"] == 5

    def test_server_span_is_the_root(self, captured) -> None:
        document, _, _ = captured
        server = _by_name(_spans(document), "POST /orders")
        assert "parentSpanId" not in server
        assert server["kind"] == 2

    def test_database_spans_sit_on_the_right_side_of_the_hop(self, captured) -> None:
        document, _, _ = captured
        spans = _spans(document)
        # The API's write query belongs under the command; the worker's under the
        # consumer. That split is what makes "which side is slow?" answerable.
        assert _service_of(_by_name(spans, "INSERT orders")) == "inventory-orders-api"
        assert _service_of(_by_name(spans, "SELECT products")) == "inventory-worker"

    def test_spans_carry_searchable_business_attributes(self, captured) -> None:
        document, _, _ = captured
        spans = _spans(document)
        consumer = _by_name(spans, "orders.order-created.inventory process")
        assert consumer["attributes"], "consumer span has no attributes to search on"
        assert _attr(consumer, "messaging.system") == "rabbitmq"
        assert _attr(consumer, "messaging.destination.name") == (
            "orders.order-created.inventory"
        )
        assert _attr(consumer, "messaging.message.id")

    def test_ids_are_well_formed(self, captured) -> None:
        """Jaeger will reject the upload if the hex ids are the wrong width."""
        document, _, _ = captured
        for span in _spans(document):
            assert len(span["traceId"]) == 32
            assert len(span["spanId"]) == 16
            int(span["traceId"], 16)
            int(span["spanId"], 16)
            assert span["startTimeUnixNano"] <= span["endTimeUnixNano"]

    def test_tree_file_is_written_next_to_the_json(self, captured) -> None:
        _, out, _ = captured
        tree = out.with_name(out.stem + "-tree.txt")
        assert tree.exists()
        text = tree.read_text(encoding="utf-8")
        assert "POST /orders" in text
        assert "service=inventory-worker" in text
        # ASCII only, so it pastes cleanly into the README and prints on Windows.
        text.encode("ascii")


class TestRetryScenario:
    @pytest.fixture(scope="class")
    def retry_capture(self, tmp_path_factory):
        out = tmp_path_factory.mktemp("retry") / "trace.json"
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--out", str(out), "--with-retry"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=180,
        )
        assert result.returncode == 0, f"capture failed:\n{result.stdout}\n{result.stderr}"
        return json.loads(out.read_text(encoding="utf-8"))

    def test_retry_stays_in_the_same_trace(self, retry_capture) -> None:
        document = retry_capture
        assert len({s["traceId"] for s in _spans(document)}) == 1

    def test_retry_publish_is_a_child_of_the_failed_attempt(self, retry_capture) -> None:
        """A retry is a fresh AMQP delivery, so it is a sibling span, not a child
        of the failure — but its republish must hang off the attempt that failed,
        or the retry chain scatters into separate traces."""
        spans = _spans(retry_capture)
        attempts = [s for s in spans if s["name"] == "orders.order-created.inventory process"]
        assert len(attempts) == 2, "expected the original delivery plus the retry"

        republish = _by_name(spans, "orders.order-created.inventory.retry.1 publish")
        assert republish["kind"] == 4
        # It hangs off the failing attempt, not the first (successful) one.
        assert republish["parentSpanId"] in {a["spanId"] for a in attempts}
        assert _attr(republish, "messaging.retry.attempt") == "1"

    def test_failed_attempt_is_marked(self, retry_capture) -> None:
        spans = _spans(retry_capture)
        failing = next(
            s
            for s in spans
            if s["name"] == "orders.order-created.inventory process"
            and _attr(s, "will_retry") == "True"
        )
        assert failing["status"]["code"] == 2  # ERROR
        assert _attr(failing, "error_type") == "RuntimeError"


class TestRenderer:
    """The renderer reads the artifact and only draws it — it must never be able
    to invent or reorder spans."""

    @pytest.fixture(scope="class")
    def svg(self, tmp_path_factory):
        out = tmp_path_factory.mktemp("svg") / "t.json"
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--out", str(out)],
            capture_output=True, text=True, cwd=REPO_ROOT, timeout=180,
        )
        assert result.returncode == 0, result.stderr
        render = subprocess.run(
            [sys.executable, str(RENDERER), "--input", str(out),
             "--output", str(out.with_suffix(".svg"))],
            capture_output=True, text=True, cwd=REPO_ROOT, timeout=180,
        )
        assert render.returncode == 0, render.stderr
        return out.with_suffix(".svg").read_text(encoding="utf-8"), out

    def test_is_well_formed_svg(self, svg) -> None:
        markup, _ = svg
        assert markup.startswith("<svg")
        assert markup.rstrip().endswith("</svg>")
        assert 'xmlns="http://www.w3.org/2000/svg"' in markup

    def test_draws_one_row_per_span(self, svg) -> None:
        from xml.etree import ElementTree

        markup, trace_path = svg
        document = json.loads(trace_path.read_text(encoding="utf-8"))
        root = ElementTree.fromstring(markup)
        bars = [
            e for e in root.iter()
            if e.tag.endswith("rect") and e.get("rx") == "3" and e.get("fill-opacity")
        ]
        assert len(bars) == len(_spans(document))

    def test_text_stays_inside_the_canvas(self, svg) -> None:
        """A caption running off the right edge is the classic SVG bug, and it is
        invisible until someone opens the file."""
        from xml.etree import ElementTree

        markup, _ = svg
        root = ElementTree.fromstring(markup)
        width = float(root.get("width"))
        overflow = [
            e.text
            for e in root.iter()
            if e.tag.endswith("text")
            and e.get("text-anchor") != "end"
            and float(e.get("x", 0)) + len(e.text or "") * 6.2 > width
        ]
        assert not overflow, f"text overflows the {width}px canvas: {overflow}"

    def test_error_spans_are_outlined(self, tmp_path) -> None:
        """Status colour is the fastest way to spot the failure in a wide trace."""
        from xml.etree import ElementTree

        out = tmp_path / "t.json"
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--out", str(out), "--with-retry"],
            capture_output=True, text=True, cwd=REPO_ROOT, timeout=180,
        )
        assert result.returncode == 0, result.stderr

        document = json.loads(out.read_text(encoding="utf-8"))
        markup = render_trace.render(document)
        root = ElementTree.fromstring(markup)
        red_outlines = [
            e for e in root.iter()
            if e.tag.endswith("rect") and e.get("stroke") == "#dc2626"
        ]
        # The --with-retry trace marks the failed attempt and the republish it
        # scheduled, so there must be exactly as many outlines as ERROR spans.
        errors = [s for s in _spans(document) if s.get("status", {}).get("code") == 2]
        assert len(errors) == 2, [s["name"] for s in errors]
        assert len(red_outlines) == len(errors)

    def test_renders_a_happy_path_trace_without_error_outlines(self, svg) -> None:
        from xml.etree import ElementTree

        markup, _ = svg
        root = ElementTree.fromstring(markup)
        assert not [
            e for e in root.iter()
            if e.tag.endswith("rect") and e.get("stroke") == "#dc2626"
        ]

