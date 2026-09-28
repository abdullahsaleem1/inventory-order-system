"""
Render a captured trace as an SVG waterfall (Week 10).

Reads the OTLP JSON written by `scripts/capture_trace.py` and draws the
span tree as a timeline: one row per span, indented by depth, bars positioned by
start/end time, coloured by span kind and annotated with the owning service.

This exists because the Jaeger UI is the tool you actually use day to day, but
it needs a running Docker stack. The renderer needs nothing but the trace file,
so the repository carries a picture of a *real* trace that anyone can regenerate
or diff. It is deliberately a separate step from capture: the data and the
picture are produced independently, so a rendering bug cannot invent a trace.

    python scripts/capture_trace.py
    python scripts/render_trace.py                     # -> artifacts/trace-waterfall.svg
    python scripts/render_trace.py --input t.json --output t.svg
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Jaeger's own kind palette, so the diagram and the UI read the same way.
KIND_STYLE = {
    1: ("#5c6bc0", "internal"),  # INTERNAL
    2: ("#26a69a", "server"),    # SERVER
    3: ("#42a5f5", "client"),    # CLIENT
    4: ("#ab47bc", "producer"),  # PRODUCER
    5: ("#ffa726", "consumer"),  # CONSUMER
}

ROW_HEIGHT = 30
BAR_HEIGHT = 18
LABEL_WIDTH = 430
CHART_PADDING = 20
# Room for the "0.23 ms · consumer" caption that follows the rightmost bar.
TAIL_WIDTH = 130
HEADER_HEIGHT = 86
FOOTER_HEIGHT = 30
MIN_CHART_WIDTH = 560

SERVICE_COLOR = {
    "inventory-orders-api": "#1a237e",
    "order-event-consumer": "#4a148c",
    "inventory-worker": "#b71c1c",
    "read-projector": "#1b5e20",
}


def _spans(document: dict[str, Any]) -> list[dict[str, Any]]:
    return document["resourceSpans"][0]["scopeSpans"][0]["spans"]


def _service_of(span: dict[str, Any]) -> str:
    return {
        a["key"]: a["value"].get("stringValue") for a in span["resource"]["attributes"]
    }.get("service.name", "?")


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _order_spans(spans: list[dict[str, Any]]) -> list[tuple[dict[str, Any], int]]:
    """Depth-first ordering, matching how Jaeger lays out a trace."""
    children: dict[str | None, list[dict[str, Any]]] = {}
    for span in spans:
        children.setdefault(span.get("parentSpanId"), []).append(span)

    ordered: list[tuple[dict[str, Any], int]] = []

    def walk(span: dict[str, Any], depth: int) -> None:
        ordered.append((span, depth))
        for child in sorted(
            children.get(span["spanId"], []), key=lambda s: int(s["startTimeUnixNano"])
        ):
            walk(child, depth + 1)

    for root in sorted(children.get(None, []), key=lambda s: int(s["startTimeUnixNano"])):
        walk(root, 0)
    return ordered


def render(document: dict[str, Any], *, title: str = "POST /orders") -> str:
    spans = _spans(document)
    if not spans:
        raise SystemExit("trace contains no spans")

    trace_id = spans[0]["traceId"]
    ordered = _order_spans(spans)

    t_start = min(int(s["startTimeUnixNano"]) for s in spans)
    t_end = max(int(s["endTimeUnixNano"]) for s in spans)
    # A floor on the window, so sub-millisecond spans are still visible.
    span_of_time = max(t_end - t_start, 1_000_000)
    total_ms = span_of_time / 1_000_000

    height = HEADER_HEIGHT + len(ordered) * ROW_HEIGHT + FOOTER_HEIGHT
    chart_width = max(MIN_CHART_WIDTH, int(total_ms * 12))
    width = LABEL_WIDTH + chart_width + CHART_PADDING * 2 + TAIL_WIDTH

    def x_of(timestamp: int) -> float:
        return LABEL_WIDTH + CHART_PADDING + (timestamp - t_start) / span_of_time * chart_width

    def width_of(span: dict[str, Any]) -> float:
        raw = (int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])) / span_of_time
        return max(raw * chart_width, 3.0)

    out: list[str] = []
    out.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="ui-monospace,SFMono-Regular,Menlo,monospace">'
    )
    out.append(
        '<rect width="100%" height="100%" fill="#ffffff"/>'
    )

    # Header
    out.append(f'<text x="{CHART_PADDING}" y="30" font-size="17" font-weight="600" fill="#111827">'
               f'{_escape(title)}</text>')
    services = sorted({_service_of(s) for s in spans})
    out.append(
        f'<text x="{CHART_PADDING}" y="52" font-size="12" fill="#6b7280">'
        f'{len(spans)} spans &#183; {len(services)} services &#183; '
        f'trace {trace_id[:16]}&#8230; &#183; {total_ms:.2f} ms</text>'
    )

    # Time gridlines, in the chart area only.
    grid_start = LABEL_WIDTH + CHART_PADDING
    for i in range(5):
        fraction = i / 4
        gx = grid_start + fraction * chart_width
        label_ms = fraction * total_ms
        out.append(
            f'<line x1="{gx:.1f}" y1="{HEADER_HEIGHT - 14}" x2="{gx:.1f}" y2="{height - FOOTER_HEIGHT}" '
            f'stroke="#e5e7eb" stroke-width="1"/>'
        )
        out.append(
            f'<text x="{gx:.1f}" y="{HEADER_HEIGHT - 20}" font-size="10" fill="#9ca3af" '
            f'text-anchor="middle">{label_ms:.2f} ms</text>'
        )

    # Rows
    for index, (span, depth) in enumerate(ordered):
        y = HEADER_HEIGHT + index * ROW_HEIGHT
        if index % 2 == 0:
            out.append(
                f'<rect x="0" y="{y}" width="{width}" height="{ROW_HEIGHT}" fill="#f9fafb"/>'
            )

        service = _service_of(span)
        colour, kind_name = KIND_STYLE.get(span["kind"], ("#6b7280", "unknown"))
        duration_ms = (
            int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])
        ) / 1_000_000

        label_x = CHART_PADDING + 14 + depth * 16
        text = f"{span['name']}"
        out.append(
            f'<text x="{label_x}" y="{y + 20}" font-size="12" fill="#111827">'
            f'{_escape(text)}</text>'
        )

        # Service tag, right-aligned in the label gutter.
        out.append(
            f'<text x="{LABEL_WIDTH - 8}" y="{y + 20}" font-size="10" '
            f'fill="{SERVICE_COLOR.get(service, "#6b7280")}" text-anchor="end">'
            f'{_escape(service)}</text>'
        )

        bx = x_of(int(span["startTimeUnixNano"]))
        bw = width_of(span)
        out.append(
            f'<rect x="{bx:.1f}" y="{y + 6}" width="{bw:.1f}" height="{BAR_HEIGHT}" '
            f'rx="3" fill="{colour}" fill-opacity="0.85"/>'
        )
        # Duration label just after the bar.
        out.append(
            f'<text x="{bx + bw + 6:.1f}" y="{y + 20}" font-size="10" fill="#6b7280">'
            f'{duration_ms:.2f} ms &#183; {kind_name}</text>'
        )

        # An error bar is outlined in red so it is findable at a glance.
        if span.get("status", {}).get("code") == 2:
            out.append(
                f'<rect x="{bx - 1:.1f}" y="{y + 5}" width="{bw + 2:.1f}" '
                f'height="{BAR_HEIGHT + 2}" rx="4" fill="none" stroke="#dc2626" stroke-width="1.5"/>'
            )

    # Footer: the point of the diagram.
    out.append(
        f'<text x="{CHART_PADDING}" y="{height - 10}" font-size="10" fill="#9ca3af">'
        f'One trace across {len(services)} services: the CONSUMER span in '
        f'inventory-worker is a child of the PRODUCER span in the API, '
        f'propagated through AMQP headers.</text>'
    )
    out.append("</svg>")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=REPO_ROOT / "artifacts" / "trace.json",
        help="OTLP JSON from scripts/capture_trace.py",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "artifacts" / "trace-waterfall.svg",
    )
    parser.add_argument("--title", default="POST /orders")
    args = parser.parse_args()

    if not args.input.exists():
        print(
            f"{args.input} not found - run: python scripts/capture_trace.py",
            file=sys.stderr,
        )
        return 1

    document = json.loads(args.input.read_text(encoding="utf-8"))
    svg = render(document, title=args.title)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(svg, encoding="utf-8")

    spans = _spans(document)
    print(f"rendered {len(spans)} spans -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
