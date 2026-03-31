from __future__ import annotations

import math
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


BROKER_LABELS = {
    "kafka": "Apache Kafka",
    "rabbitmq": "RabbitMQ",
    "artemis": "ActiveMQ Artemis",
    "nats": "NATS JetStream",
}

SETUP_LABELS = {
    "baseline": "Standard",
    "optimized": "Performance",
}

DEPLOYMENT_LABELS = {
    "normal": "Single node",
    "ha": "3-node cluster",
}

REPORT_SECTION_ORDER = (
    "summary",
    "comparison",
    "details",
    "configuration",
    "artifacts",
    "resources",
)

ACCENT = colors.HexColor("#43b4ff")
GLOW = colors.HexColor("#4dffc5")
INK = colors.HexColor("#132536")
MUTED = colors.HexColor("#5a748c")
SURFACE = colors.HexColor("#eef5fb")
GRID = colors.HexColor("#d6e4f0")

_TABLE_HEADER_PARAGRAPH_STYLE: ParagraphStyle | None = None
_TABLE_BODY_PARAGRAPH_STYLE: ParagraphStyle | None = None


def broker_label(broker_id: str) -> str:
    return BROKER_LABELS.get(str(broker_id or "").lower(), str(broker_id or "-"))


def setup_label(run: dict[str, Any]) -> str:
    return SETUP_LABELS.get(str(run.get("configMode") or "").lower(), str(run.get("configMode") or "-"))


def deployment_label(deployment_mode: str) -> str:
    return DEPLOYMENT_LABELS.get(str(deployment_mode or "").lower(), str(deployment_mode or "-"))


def load_profile_label(run: dict[str, Any]) -> str:
    summary = ((run.get("metrics") or {}).get("summary") or {}).get("loadProfile") or {}
    options = run.get("transportOptions") or {}
    base_rate = int(run.get("messageRate") or summary.get("baseMessageRate") or 0)
    peak_rate = int(summary.get("peakMessageRate") or options.get("peakMessageRate") or base_rate or 0)
    kind = str(summary.get("rateProfileKind") or options.get("rateProfileKind") or "constant").strip().lower()
    if kind == "constant":
        return f"Steady {base_rate:,} msg/s"
    return f"{kind.replace('-', ' ').title()} {base_rate:,} -> {peak_rate:,} msg/s"


def has_measured_results(run: dict[str, Any]) -> bool:
    summary = ((run.get("metrics") or {}).get("summary") or {})
    latency = summary.get("endToEndLatencyMs") or {}
    return float(latency.get("count") or 0.0) > 0.0


def throughput_message_rate(run: dict[str, Any]) -> float:
    throughput = (((run.get("metrics") or {}).get("summary") or {}).get("throughput") or {})
    return float(throughput.get("achieved") or 0.0)


def throughput_payload_bytes_per_second(run: dict[str, Any]) -> float:
    throughput = (((run.get("metrics") or {}).get("summary") or {}).get("throughput") or {})
    return float(throughput.get("achievedBytesPerSec") or 0.0)


def format_payload_size(value: int | float) -> str:
    payload_bytes = max(0.0, float(value or 0.0))
    if payload_bytes >= 1024.0 * 1024.0:
        return f"{payload_bytes / (1024.0 * 1024.0):.2f} MiB"
    if payload_bytes >= 1024.0:
        return f"{payload_bytes / 1024.0:.1f} KiB"
    return f"{int(payload_bytes)} B"


def format_payload_rate(value: float) -> str:
    payload_bytes = max(0.0, float(value or 0.0))
    if payload_bytes >= 1024.0 * 1024.0:
        return f"{payload_bytes / (1024.0 * 1024.0):.2f} MiB/s"
    if payload_bytes >= 1024.0:
        return f"{payload_bytes / 1024.0:.2f} KiB/s"
    return f"{payload_bytes:.0f} B/s"


def format_duration(seconds: int | float) -> str:
    total = max(0, int(seconds or 0))
    if total >= 3600:
        hours = total // 3600
        minutes = (total % 3600) // 60
        remaining = total % 60
        if not minutes and not remaining:
            return f"{hours}h"
        if not remaining:
            return f"{hours}h {minutes}m"
        if not minutes:
            return f"{hours}h {remaining}s"
        return f"{hours}h {minutes}m {remaining}s"
    if total < 60:
        return f"{total}s"
    minutes = total // 60
    remaining = total % 60
    return f"{minutes}m {remaining}s" if remaining else f"{minutes}m"


def configured_run_duration_seconds(run: dict[str, Any]) -> int:
    return (
        int(run.get("warmupSeconds") or 0)
        + int(run.get("measurementSeconds") or 0)
        + int(run.get("cooldownSeconds") or 0)
    )


def effective_measured_duration_seconds(run: dict[str, Any]) -> int:
    measurement = ((run.get("metrics") or {}).get("measurement") or {})
    return int(measurement.get("effectiveWindowSeconds") or run.get("measurementSeconds") or 0)


def run_duration_label(run: dict[str, Any]) -> str:
    parts: list[str] = []
    configured = configured_run_duration_seconds(run)
    if configured > 0:
        parts.append(f"{format_duration(configured)} total")
    if has_measured_results(run):
        effective = effective_measured_duration_seconds(run)
        if effective > 0:
            parts.append(f"{format_duration(effective)} effective")
    return ", ".join(parts) if parts else "-"


def resource_config_label(run: dict[str, Any]) -> str:
    config = run.get("resourceConfig") or {}
    cpu_request = str(config.get("cpuRequest") or "").strip()
    cpu_limit = str(config.get("cpuLimit") or "").strip()
    memory = str(config.get("memoryLimit") or config.get("memoryRequest") or "-")
    replicas = int(config.get("replicas") or 0)
    storage = str(config.get("storageSize") or "-")
    parts: list[str] = []
    if cpu_request or cpu_limit:
        cpu = cpu_limit or cpu_request
        if cpu_request and cpu_limit and cpu_request != cpu_limit:
            cpu = f"{cpu_request}/{cpu_limit}"
        parts.append(f"{cpu} CPU")
    if memory != "-":
        parts.append(memory)
    if replicas:
        parts.append(f"x{replicas}")
    if storage != "-":
        parts.append(storage)
    return ", ".join(parts) if parts else "-"


def _artifact_summary_label(run: dict[str, Any]) -> str:
    raw_store = (((run.get("metrics") or {}).get("measurement") or {}).get("rawRecordStore") or {})
    producer_count = int(raw_store.get("producerRecordCount") or 0)
    consumer_count = int(raw_store.get("consumerRecordCount") or 0)
    resource_count = int(raw_store.get("resourceSampleCount") or 0)
    parts: list[str] = []
    if producer_count:
        parts.append(f"{producer_count:,} producer rows")
    if consumer_count:
        parts.append(f"{consumer_count:,} consumer rows")
    if resource_count:
        parts.append(f"{resource_count:,} resource samples")
    return ", ".join(parts) if parts else "No persisted run artifacts"


def normalize_report_sections(sections: list[str] | None) -> list[str]:
    if not sections:
        return list(REPORT_SECTION_ORDER)
    normalized: list[str] = []
    seen: set[str] = set()
    for section in sections:
        candidate = str(section or "").strip().lower()
        if candidate in REPORT_SECTION_ORDER and candidate not in seen:
            normalized.append(candidate)
            seen.add(candidate)
    return normalized or list(REPORT_SECTION_ORDER)


def slugify_file_name(title: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", title.strip().lower()).strip("-")
    return cleaned or "benchmark-report"


def _sparse_category_labels(labels: list[str], *, max_visible: int = 10) -> list[str]:
    if len(labels) <= max_visible:
        return labels
    step = max(1, math.ceil((len(labels) - 1) / max(1, max_visible - 1)))
    visible_indexes = {0, len(labels) - 1}
    visible_indexes.update(index for index in range(0, len(labels), step))
    return [label if index in visible_indexes else "" for index, label in enumerate(labels)]


def _bar_chart(title: str, labels: list[str], values: list[float], fill: colors.Color) -> Drawing:
    drawing = Drawing(720, 220)
    drawing.add(String(16, 198, title, fontName="Helvetica-Bold", fontSize=13, fillColor=INK))
    chart = VerticalBarChart()
    chart.x = 56
    chart.y = 44
    chart.height = 120
    chart.width = 620
    chart.data = [values or [0.0]]
    chart.categoryAxis.categoryNames = labels or ["-"]
    chart.categoryAxis.labels.boxAnchor = "n"
    chart.categoryAxis.labels.fontName = "Helvetica"
    chart.categoryAxis.labels.fontSize = 8
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.strokeColor = GRID
    chart.categoryAxis.strokeColor = GRID
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = GRID
    chart.bars[0].fillColor = fill
    chart.bars[0].strokeColor = fill
    chart.barSpacing = 8
    chart.groupSpacing = 14
    if values:
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max(1.0, max(values) * 1.2)
    drawing.add(chart)
    return drawing


def _line_chart(title: str, labels: list[str], series: list[tuple[str, list[float], colors.Color]]) -> Drawing:
    drawing = Drawing(720, 220)
    drawing.add(String(16, 198, title, fontName="Helvetica-Bold", fontSize=13, fillColor=INK))
    chart = HorizontalLineChart()
    chart.x = 56
    chart.y = 46
    chart.height = 118
    chart.width = 620
    chart.data = [values or [0.0] for _, values, _ in series] or [[0.0]]
    chart.categoryAxis.categoryNames = _sparse_category_labels(labels or ["0"])
    chart.categoryAxis.labels.fontSize = 8
    chart.categoryAxis.labels.boxAnchor = "n"
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.strokeColor = GRID
    chart.categoryAxis.strokeColor = GRID
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = GRID
    chart.joinedLines = 1
    chart.lines.strokeWidth = 2
    max_value = max([max(values or [0.0]) for _, values, _ in series], default=1.0)
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(1.0, max_value * 1.2)
    for index, (_, _, color) in enumerate(series):
        chart.lines[index].strokeColor = color
    drawing.add(chart)
    legend_y = 180
    legend_x = 56
    for label, _, color in series:
        drawing.add(String(legend_x + 16, legend_y, label, fontName="Helvetica", fontSize=9, fillColor=MUTED))
        drawing.add(Rect(legend_x, legend_y - 2, 10, 10, fillColor=color, strokeColor=color))
        legend_x += 132
    return drawing


def _table_style() -> TableStyle:
    return TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), SURFACE),
            ("TEXTCOLOR", (0, 0), (-1, 0), INK),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("GRID", (0, 0), (-1, -1), 0.5, GRID),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, SURFACE]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
    )


def _overview_table_style() -> TableStyle:
    style = _table_style()
    return style


def _table_header_paragraph_style() -> ParagraphStyle:
    global _TABLE_HEADER_PARAGRAPH_STYLE
    if _TABLE_HEADER_PARAGRAPH_STYLE is None:
        _TABLE_HEADER_PARAGRAPH_STYLE = ParagraphStyle(
            "BusTableHeader",
            fontName="Helvetica-Bold",
            fontSize=7.2,
            leading=8.2,
            textColor=INK,
            wordWrap="CJK",
        )
    return _TABLE_HEADER_PARAGRAPH_STYLE


def _table_body_paragraph_style() -> ParagraphStyle:
    global _TABLE_BODY_PARAGRAPH_STYLE
    if _TABLE_BODY_PARAGRAPH_STYLE is None:
        _TABLE_BODY_PARAGRAPH_STYLE = ParagraphStyle(
            "BusTableBody",
            fontName="Helvetica",
            fontSize=7.2,
            leading=8.4,
            textColor=INK,
            wordWrap="CJK",
        )
    return _TABLE_BODY_PARAGRAPH_STYLE


def _table_paragraph(value: Any, *, header: bool = False) -> Paragraph:
    text = "-" if value is None else str(value)
    markup = escape(text).replace("\n", "<br/>")
    return Paragraph(
        markup,
        _table_header_paragraph_style() if header else _table_body_paragraph_style(),
    )


def _wrap_table_rows(rows: list[list[str]], *, header_rows: int = 1) -> list[list[Any]]:
    wrapped: list[list[Any]] = []
    for row_index, row in enumerate(rows):
        wrapped.append(
            [
                _table_paragraph(cell, header=row_index < header_rows)
                for cell in row
            ]
        )
    return wrapped


def _metric_rows(runs: list[dict[str, Any]]) -> list[list[str]]:
    rows: list[list[str]] = [
        [
            "Run",
            "Broker",
            "Setup",
            "Deploy",
            "P/C",
            "Payload",
            "Flow",
            "p50 (ms)",
            "p95 (ms)",
            "p99 (ms)",
            "Delivered msg/s",
            "Payload rate",
            "Success %",
            "Resources",
            "Duration",
        ]
    ]
    for run in runs:
        summary = ((run.get("metrics") or {}).get("summary") or {})
        latency = summary.get("endToEndLatencyMs") or {}
        measured = has_measured_results(run)
        rows.append(
            [
                str(run.get("name") or "-"),
                broker_label(str(run.get("brokerId") or "")),
                setup_label(run),
                deployment_label(str(run.get("deploymentMode") or "")),
                f"{int(run.get('producers') or 0)}/{int(run.get('consumers') or 0)}",
                format_payload_size(int(run.get("messageSizeBytes") or 0)),
                load_profile_label(run),
                f"{float(latency.get('p50') or 0.0):.3f}" if measured else "-",
                f"{float(latency.get('p95') or 0.0):.3f}" if measured else "-",
                f"{float(latency.get('p99') or 0.0):.3f}" if measured else "-",
                f"{throughput_message_rate(run):,.0f}" if measured else "-",
                format_payload_rate(throughput_payload_bytes_per_second(run)) if measured else "-",
                f"{float(summary.get('deliverySuccessRate') or 0.0):.2f}" if measured else "-",
                resource_config_label(run),
                run_duration_label(run),
            ]
        )
    return rows


def _overview_table(runs: list[dict[str, Any]]) -> Table:
    table = Table(
        _wrap_table_rows(_metric_rows(runs), header_rows=1),
        repeatRows=1,
        colWidths=[72, 54, 42, 46, 32, 40, 78, 32, 32, 32, 46, 50, 34, 60, 70],
    )
    table.setStyle(_overview_table_style())
    return table


def _primary_run_charts(run: dict[str, Any]) -> list[Drawing]:
    metrics = run.get("metrics") or {}
    points = metrics.get("timeseries") or []
    labels = [f"{int(point.get('second', 0) or 0)}s" for point in points]
    latency_p50_values = [float(point.get("latencyP50Ms") or point.get("latencyMs") or 0.0) for point in points]
    latency_p95_values = [float(point.get("latencyP95Ms") or point.get("latencyMs") or 0.0) for point in points]
    latency_p99_values = [float(point.get("latencyP99Ms") or point.get("latencyMs") or 0.0) for point in points]
    producer_values = [float(point.get("producerRate") or 0.0) for point in points]
    consumer_values = [float(point.get("consumerRate") or 0.0) for point in points]
    throughput_values = [float(point.get("throughput") or 0.0) for point in points]
    histogram = metrics.get("histogramMs") or []
    histogram_labels: list[str] = []
    histogram_values: list[float] = []
    for bucket in histogram:
        lower = float(bucket.get("lowerMs") or 0.0)
        upper = bucket.get("upperMs")
        histogram_labels.append(f">{lower:g}" if upper is None else f"{lower:g}-{float(upper):g}")
        histogram_values.append(float(bucket.get("count") or 0.0))

    return [
        _line_chart(
            f"{run['name']} latency by measured second",
            labels,
            [
                ("p50 latency", latency_p50_values, ACCENT),
                ("p95 latency", latency_p95_values, GLOW),
                ("p99 latency", latency_p99_values, colors.HexColor("#ffbd59")),
            ],
        ),
        _line_chart(
            f"{run['name']} producer, consumer, and delivered rate",
            labels,
            [
                ("producer rate", producer_values, ACCENT),
                ("consumer rate", consumer_values, GLOW),
                ("delivered rate", throughput_values, colors.HexColor("#ffbd59")),
            ],
        ),
        _bar_chart(
            f"{run['name']} latency distribution",
            histogram_labels[:16],
            histogram_values[:16],
            fill=colors.HexColor("#a78bfa"),
        ),
    ]


def _resource_points(run: dict[str, Any]) -> list[dict[str, Any]]:
    summary = ((run.get("metrics") or {}).get("summary") or {})
    resource_usage = summary.get("resourceUsage") or {}
    points = resource_usage.get("points") or []
    return [point for point in points if isinstance(point, dict)]


def _resource_charts(run: dict[str, Any]) -> list[Drawing]:
    points = _resource_points(run)
    if not points:
        return []
    labels = [f"{int(point.get('second', 0) or 0)}s" for point in points]
    return [
        _line_chart(
            f"{run['name']} broker CPU and memory",
            labels,
            [
                ("broker CPU cores", [float(point.get("brokerCpuCores") or 0.0) for point in points], ACCENT),
                ("broker memory MB", [float(point.get("brokerMemoryMB") or 0.0) for point in points], GLOW),
            ],
        ),
        _line_chart(
            f"{run['name']} producer and consumer CPU",
            labels,
            [
                ("producer CPU cores", [float(point.get("producerCpuCores") or 0.0) for point in points], colors.HexColor("#ffbd59")),
                ("consumer CPU cores", [float(point.get("consumerCpuCores") or 0.0) for point in points], colors.HexColor("#a78bfa")),
            ],
        ),
        _line_chart(
            f"{run['name']} broker network",
            labels,
            [
                ("broker RX MB/s", [float(point.get("brokerNetworkRxMBps") or 0.0) for point in points], colors.HexColor("#36f8ff")),
                ("broker TX MB/s", [float(point.get("brokerNetworkTxMBps") or 0.0) for point in points], colors.HexColor("#ff6d9e")),
            ],
        ),
    ]


def _comparison_resource_charts(runs: list[dict[str, Any]]) -> list[Drawing]:
    labels = [run["name"] for run in runs]
    broker_cpu_peaks = [
        float(((((run.get("metrics") or {}).get("summary") or {}).get("resourceUsage") or {}).get("peaks") or {}).get("brokerCpuCores") or 0.0)
        for run in runs
    ]
    broker_memory_peaks = [
        float(((((run.get("metrics") or {}).get("summary") or {}).get("resourceUsage") or {}).get("peaks") or {}).get("brokerMemoryMB") or 0.0)
        for run in runs
    ]
    drawings: list[Drawing] = []
    if any(value > 0.0 for value in broker_cpu_peaks):
        drawings.append(_bar_chart("Peak broker CPU by run", labels, broker_cpu_peaks, fill=colors.HexColor("#36f8ff")))
    if any(value > 0.0 for value in broker_memory_peaks):
        drawings.append(_bar_chart("Peak broker memory by run", labels, broker_memory_peaks, fill=colors.HexColor("#ff6d9e")))
    return drawings


def _flatten_pairs(prefix: str, value: Any, sink: list[tuple[str, str]]) -> None:
    if isinstance(value, dict):
        for key in sorted(value.keys()):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_pairs(child_prefix, value[key], sink)
        return
    if isinstance(value, list):
        rendered = ", ".join(str(item) for item in value)
    elif value is None:
        rendered = "-"
    else:
        rendered = str(value)
    sink.append((prefix, rendered))


def _table_from_rows(
    rows: list[list[str]],
    *,
    repeat_rows: int = 1,
    col_widths: list[int] | None = None,
) -> Table:
    table = Table(_wrap_table_rows(rows, header_rows=repeat_rows), repeatRows=repeat_rows, colWidths=col_widths)
    table.setStyle(_table_style())
    return table


def _run_configuration_table(run: dict[str, Any]) -> Table:
    rows: list[list[str]] = [["Field", "Value"]]
    rows.extend(
        [
            ["Broker", broker_label(str(run.get("brokerId") or ""))],
            ["Protocol", str(run.get("protocol") or "-")],
            ["Setup", setup_label(run)],
            ["Deployment", deployment_label(str(run.get("deploymentMode") or ""))],
            ["Producers", str(int(run.get("producers") or 0))],
            ["Consumers", str(int(run.get("consumers") or 0))],
            ["Message rate target", f"{int(run.get('messageRate') or 0):,} msg/s"],
            ["Payload size", format_payload_size(int(run.get("messageSizeBytes") or 0))],
            ["Configured duration", f"{format_duration(configured_run_duration_seconds(run))} total"],
            [
                "Window",
                (
                    f"warmup {int(run.get('warmupSeconds') or 0)}s, "
                    f"measure {int(run.get('measurementSeconds') or 0)}s, "
                    f"cooldown {int(run.get('cooldownSeconds') or 0)}s"
                ),
            ],
            [
                "Effective measured window",
                (
                    f"{format_duration(effective_measured_duration_seconds(run))} effective"
                    if has_measured_results(run)
                    else "-"
                ),
            ],
            ["Load profile", load_profile_label(run)],
            ["Delivered payload rate", format_payload_rate(throughput_payload_bytes_per_second(run)) if has_measured_results(run) else "-"],
        ]
    )
    resource_pairs: list[tuple[str, str]] = []
    _flatten_pairs("resources", run.get("resourceConfig") or {}, resource_pairs)
    rows.extend([[key, value] for key, value in resource_pairs])
    tuning_pairs: list[tuple[str, str]] = []
    _flatten_pairs("tuning", run.get("brokerTuning") or {}, tuning_pairs)
    rows.extend([[key, value] for key, value in tuning_pairs])
    return _table_from_rows(rows)


def _calculation_rule_rows() -> list[list[str]]:
    return [
        ["Criterion", "How it is calculated"],
        [
            "End-to-end latency",
            "For the same message, consumer receive time minus producer send time. Percentiles come from merged HDR histogram snapshots when available; otherwise from the persisted raw samples or bucketed fallbacks.",
        ],
        [
            "Producer ack latency",
            "Producer publish acknowledgement latency recorded by the producer agent. The report applies the same percentile logic as end-to-end latency.",
        ],
        [
            "Stabilization trim",
            "Measured seconds before stabilization are ignored. Stabilization starts at the first active second where both attempted and delivered counts reach at least 70% of their non-zero medians for 3 consecutive active seconds, or 2 consecutive active seconds when fewer than 6 active seconds exist.",
        ],
        [
            "Effective window",
            "max(1, lastActiveSecond - stabilizationTrimSeconds + 1). If no active seconds exist, the configured measurement window is used.",
        ],
        [
            "Attempted msg/s",
            "round(total attempted messages after stabilization trim / effectiveWindowSeconds).",
        ],
        [
            "Delivered msg/s",
            "round(total delivered messages after stabilization trim / effectiveWindowSeconds).",
        ],
        [
            "Payload rate",
            "Delivered msg/s multiplied by the configured payload size in bytes, then rendered as B/s, KiB/s, or MiB/s.",
        ],
        [
            "Success and failure",
            "Success % = min(100, deliveredTotal / attemptedTotal x 100). Failure % = max(0, 100 - success %).",
        ],
        [
            "Duplicates, out-of-order, parse errors",
            "Direct sums of the consumer-reported counters across all consumer jobs after aggregation.",
        ],
        [
            "Per-second charts",
            "The latency chart shows p50, p95, and p99 by measured second. The generic latencyMs point used elsewhere is the per-second p95. Throughput points are per-second delivered message counts.",
        ],
        [
            "Latency distribution",
            "Display buckets are grouped in milliseconds at 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, and 5000, with one final overflow bucket above the last bound.",
        ],
        [
            "Resource peaks",
            "Peak CPU, memory, network, and storage values are the maxima of the persisted per-second resource samples after stabilization trim.",
        ],
    ]


def _summary_table_definition_rows() -> list[list[str]]:
    return [
        ["Column", "Meaning"],
        ["Run", "User-defined run name stored with the result set."],
        ["Broker", "Broker implementation used for the run."],
        ["Setup", "Starting tuning preset applied before any explicit broker override values."],
        ["Topology", "Deployment shape used for the run, including whether the broker ran in single-node or clustered HA mode."],
        ["Protocol", "Client protocol used by the benchmark agents for that broker."],
        ["P/C", "Producer and consumer job counts recorded as producers/consumers."],
        ["Payload size", "Configured payload size for each benchmark message body."],
        ["Message flow", "Configured target load profile and delivery shape."],
        ["p50 / p95 / p99", "End-to-end latency percentiles in milliseconds after stabilization trim."],
        ["Delivered (msg/s)", "Delivered messages per second across all consumers after stabilization trim."],
        ["Payload rate", "Delivered msg/s multiplied by payload size, rendered as B/s, KiB/s, or MiB/s."],
        ["Success %", "Delivered measured messages divided by attempted measured messages after stabilization trim, capped at 100%."],
        ["Resources", "Broker CPU, memory, storage, and replica settings persisted with the run."],
        ["Duration", "Configured total runtime plus the effective measured window retained after stabilization trim."],
    ]


def _artifact_definition_rows() -> list[list[str]]:
    return [
        ["Artifact", "Meaning"],
        [
            "producer-raw-records",
            "Compressed JSONL reservoir sample of producer records. By default the agent retains up to 20,000 rows while preserving an unbiased sample of larger runs. Rows include send and ack timing plus routing metadata.",
        ],
        [
            "consumer-raw-records",
            "Compressed JSONL reservoir sample of consumer records. By default the agent retains up to 20,000 rows with receive time, end-to-end latency, delivery attempt metadata, and ordering context.",
        ],
        [
            "resource-samples",
            "JSON file of per-second broker, producer, and consumer CPU, memory, network, and storage samples used for charts and peak calculations.",
        ],
        [
            "report PDF",
            "Rendered from the persisted run metrics and selected report sections. If the PDF file is missing after restart, the platform can rebuild it from the stored report metadata and runs.",
        ],
    ]


def _methodology_run_rows(runs: list[dict[str, Any]]) -> list[list[str]]:
    rows: list[list[str]] = [
        ["Run", "Configured total", "Effective window", "Trim", "Target load", "P/C", "Payload", "Artifacts"],
    ]
    for run in runs:
        measurement = ((run.get("metrics") or {}).get("measurement") or {})
        rows.append(
            [
                str(run.get("name") or "-"),
                format_duration(configured_run_duration_seconds(run)),
                format_duration(int(measurement.get("effectiveWindowSeconds") or 0)),
                format_duration(int(measurement.get("stabilizationTrimSeconds") or 0)),
                load_profile_label(run),
                f"{int(run.get('producers') or 0)}/{int(run.get('consumers') or 0)}",
                format_payload_size(int(run.get("messageSizeBytes") or 0)),
                _artifact_summary_label(run),
            ]
        )
    return rows


def _artifact_store_table(run: dict[str, Any]) -> Table:
    measurement = ((run.get("metrics") or {}).get("measurement") or {})
    raw_store = measurement.get("rawRecordStore") or {}
    artifacts = raw_store.get("artifacts") or []
    rows: list[list[str]] = [["Artifact", "Format", "Records", "Size", "Created"]]
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        rows.append(
            [
                str(artifact.get("artifactType") or "-"),
                str(artifact.get("fileFormat") or "-"),
                str(int(artifact.get("recordCount") or 0)),
                f"{int(artifact.get('sizeBytes') or 0):,} B",
                str(artifact.get("createdAt") or "-"),
            ]
        )
    if len(rows) == 1:
        rows.append(["No persisted artifacts", "-", "0", "0 B", "-"])
    return _table_from_rows(rows)


def _append_section_break(story: list[Any], has_sections: bool) -> bool:
    if has_sections:
        story.append(PageBreak())
    return True


def build_report_pdf_bytes(
    *,
    title: str,
    runs: list[dict[str, Any]],
    generated_at: datetime,
    sections: list[str] | None = None,
) -> bytes:
    selected_sections = normalize_report_sections(sections)
    include_resources = "resources" in selected_sections
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=landscape(letter),
        leftMargin=0.45 * inch,
        rightMargin=0.45 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
        title=title,
        author="Concerto Bus Benchmark",
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "BusTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=22,
        leading=26,
        textColor=INK,
        spaceAfter=8,
    )
    section_style = ParagraphStyle(
        "BusSection",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=18,
        textColor=INK,
        spaceAfter=8,
        spaceBefore=8,
    )
    subsection_style = ParagraphStyle(
        "BusSubsection",
        parent=styles["Heading3"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=14,
        textColor=INK,
        spaceAfter=6,
        spaceBefore=6,
    )
    body_style = ParagraphStyle(
        "BusBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=INK,
    )
    muted_style = ParagraphStyle(
        "BusMuted",
        parent=body_style,
        textColor=MUTED,
    )

    comparison_labels = [run["name"] for run in runs]
    p99_values = [
        float(((((run.get("metrics") or {}).get("summary") or {}).get("endToEndLatencyMs") or {}).get("p99") or 0.0))
        for run in runs
    ]
    throughput_values = [throughput_message_rate(run) for run in runs]

    story: list[Any] = [
        Paragraph(title, title_style),
        Paragraph(
            f"Generated {generated_at.strftime('%Y-%m-%d %H:%M:%S %Z')}, {len(runs)} run{'s' if len(runs) != 1 else ''}",
            muted_style,
        ),
        Spacer(1, 10),
    ]
    has_sections = False

    if "summary" in selected_sections:
        has_sections = _append_section_break(story, has_sections)
        story.extend(
            [
                Paragraph("Run Summary", section_style),
                _overview_table(runs),
            ]
        )

    if "comparison" in selected_sections and len(runs) > 1:
        has_sections = _append_section_break(story, has_sections)
        story.extend(
            [
                Paragraph("Comparison", section_style),
                KeepTogether([_bar_chart("p99 latency by run", comparison_labels, p99_values, fill=ACCENT), Spacer(1, 8)]),
                KeepTogether([_bar_chart("Delivered throughput by run", comparison_labels, throughput_values, fill=GLOW), Spacer(1, 8)]),
            ]
        )
        if include_resources:
            for drawing in _comparison_resource_charts(runs):
                story.append(KeepTogether([drawing, Spacer(1, 8)]))

    if "details" in selected_sections:
        for run in runs:
            has_sections = _append_section_break(story, has_sections)
            story.extend(
                [
                    Paragraph(str(run.get("name") or "-"), section_style),
                    Paragraph(
                        (
                            f"{broker_label(str(run.get('brokerId') or ''))}, "
                            f"{setup_label(run)}, "
                            f"{deployment_label(str(run.get('deploymentMode') or ''))}, "
                            f"{load_profile_label(run)}, "
                            f"{resource_config_label(run)}"
                        ),
                        muted_style,
                    ),
                    Spacer(1, 8),
                ]
            )
            for drawing in _primary_run_charts(run):
                story.append(KeepTogether([drawing, Spacer(1, 8)]))
            if include_resources:
                for drawing in _resource_charts(run):
                    story.append(KeepTogether([drawing, Spacer(1, 8)]))

    if "configuration" in selected_sections:
        has_sections = _append_section_break(story, has_sections)
        story.append(Paragraph("Configuration", section_style))
        for run in runs:
            story.append(Paragraph(str(run.get("name") or "-"), body_style))
            story.append(_run_configuration_table(run))
            story.append(Spacer(1, 8))

    if "artifacts" in selected_sections:
        has_sections = _append_section_break(story, has_sections)
        story.append(Paragraph("Artifacts", section_style))
        for run in runs:
            story.append(Paragraph(str(run.get("name") or "-"), body_style))
            story.append(_artifact_store_table(run))
            story.append(Spacer(1, 8))

    has_sections = _append_section_break(story, has_sections)
    story.extend(
        [
            Paragraph("Methodology Annex", section_style),
            Paragraph(
                "This appendix documents the exact benchmark criteria used by Concerto Bus Benchmark for the runs in this PDF. The formulas below match the current aggregation and persistence logic used by the platform.",
                body_style,
            ),
            Spacer(1, 8),
            Paragraph("Per-run measurement window", subsection_style),
            _table_from_rows(
                _methodology_run_rows(runs),
                col_widths=[100, 68, 60, 42, 112, 36, 48, 224],
            ),
            Spacer(1, 8),
            Paragraph("Summary table fields", subsection_style),
            _table_from_rows(
                _summary_table_definition_rows(),
                col_widths=[132, 582],
            ),
            Spacer(1, 8),
            Paragraph("Calculation rules", subsection_style),
            _table_from_rows(
                _calculation_rule_rows(),
                col_widths=[132, 582],
            ),
            Spacer(1, 8),
            Paragraph("Artifacts and persisted evidence", subsection_style),
            _table_from_rows(
                _artifact_definition_rows(),
                col_widths=[140, 574],
            ),
        ]
    )

    document.build(story)
    return buffer.getvalue()


def write_report_pdf(
    *,
    output_path: Path,
    title: str,
    runs: list[dict[str, Any]],
    generated_at: datetime,
    sections: list[str] | None = None,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_bytes = build_report_pdf_bytes(
        title=title,
        runs=runs,
        generated_at=generated_at,
        sections=sections,
    )
    output_path.write_bytes(pdf_bytes)
    return len(pdf_bytes)
