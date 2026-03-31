from __future__ import annotations

import base64
import gzip
import io
import json
import math
from statistics import mean, median
from typing import Any, Iterable, Iterator

try:
    from hdrh.histogram import HdrHistogram
except ImportError:  # pragma: no cover - exercised by runtime environment
    HdrHistogram = None

LATENCY_BUCKETS_MS = [
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    200.0,
    500.0,
    1000.0,
    2000.0,
    5000.0,
]
LATENCY_HIGHEST_TRACKABLE_US = 60_000_000


def _create_latency_histogram() -> Any:
    if HdrHistogram is None:
        return None
    return HdrHistogram(1, LATENCY_HIGHEST_TRACKABLE_US, 3)


def _merge_histogram_snapshots(snapshots: list[Any]) -> Any:
    histogram = _create_latency_histogram()
    if histogram is None:
        return None
    for snapshot in snapshots:
        if not isinstance(snapshot, list):
            continue
        for item in snapshot:
            if not isinstance(item, dict):
                continue
            value_us = int(item.get("valueUs", 0) or 0)
            count = int(item.get("count", 0) or 0)
            if value_us <= 0 or count <= 0:
                continue
            histogram.record_value(value_us, count)
    return histogram


def _summary_from_latency_histogram(histogram: Any) -> dict[str, float]:
    if histogram is None or int(histogram.get_total_count() or 0) <= 0:
        return summarize_values([])
    return {
        "count": float(histogram.get_total_count()),
        "min": float(histogram.get_min_value()) / 1000.0,
        "p50": float(histogram.get_value_at_percentile(50.0)) / 1000.0,
        "p95": float(histogram.get_value_at_percentile(95.0)) / 1000.0,
        "p99": float(histogram.get_value_at_percentile(99.0)) / 1000.0,
        "p999": float(histogram.get_value_at_percentile(99.9)) / 1000.0,
        "max": float(histogram.get_max_value()) / 1000.0,
        "mean": float(histogram.get_mean_value()) / 1000.0,
    }


def _display_histogram_from_latency_histogram(histogram: Any) -> list[dict[str, Any]]:
    if histogram is None or int(histogram.get_total_count() or 0) <= 0:
        return []
    counts = [0 for _ in range(len(LATENCY_BUCKETS_MS) + 1)]
    for item in histogram.get_recorded_iterator():
        value_ms = float(item.value_iterated_to) / 1000.0
        count = int(item.count_added_in_this_iter_step)
        placed = False
        for index, upper in enumerate(LATENCY_BUCKETS_MS):
            if value_ms <= upper:
                counts[index] += count
                placed = True
                break
        if not placed:
            counts[-1] += count

    lower = 0.0
    buckets: list[dict[str, Any]] = []
    for index, count in enumerate(counts):
        if count <= 0:
            lower = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else lower
            continue
        upper = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else None
        buckets.append({"lowerMs": lower, "upperMs": upper, "count": count})
        lower = upper if upper is not None else lower
    return buckets


def iter_json_lines(text: str) -> Iterator[dict[str, Any]]:
    if not text:
        return
    for line in io.StringIO(text):
        candidate = line.strip()
        if not candidate or not candidate.startswith("{"):
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            yield parsed


def parse_json_lines(text: str) -> list[dict[str, Any]]:
    return list(iter_json_lines(text))
    return records


def result_records(records: Iterable[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [
        record
        for record in records
        if record.get("role") == role and isinstance(record.get("result"), dict)
    ]


def iter_expanded_measurement_chunks(
    records: Iterable[dict[str, Any]],
    role: str,
    *,
    kind: str,
) -> Iterator[dict[str, Any]]:
    for record in records:
        if record.get("kind") != kind or record.get("role") != role:
            continue
        payload = str(record.get("payload") or "").strip()
        if not payload:
            continue
        try:
            raw_bytes = gzip.decompress(base64.b64decode(payload))
            decoded = json.loads(raw_bytes.decode("utf-8"))
        except Exception:
            continue
        schema = decoded.get("schema")
        rows = decoded.get("rows")
        if not isinstance(schema, list) or not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, list):
                continue
            item = {str(key): row[index] for index, key in enumerate(schema) if index < len(row)}
            item["role"] = role
            yield item


def expand_measurement_chunks(
    records: list[dict[str, Any]],
    role: str,
    *,
    kind: str,
) -> list[dict[str, Any]]:
    return list(iter_expanded_measurement_chunks(records, role, kind=kind))


def expand_measurement_record_chunks(records: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return expand_measurement_chunks(records, role, kind="measurement-record-chunk")


def expand_measurement_timeseries_chunks(records: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return expand_measurement_chunks(records, role, kind="measurement-timeseries-chunk")


def summarize_values(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "p999": 0.0,
            "max": 0.0,
            "mean": 0.0,
        }
    ordered = sorted(float(value) for value in values)

    def percentile(ratio: float) -> float:
        index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * ratio)))
        return float(ordered[index])

    return {
        "count": float(len(ordered)),
        "min": float(ordered[0]),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "p999": percentile(0.999),
        "max": float(ordered[-1]),
        "mean": float(mean(ordered)),
    }


def histogram_from_values(values: list[float]) -> list[dict[str, Any]]:
    if not values:
        return []
    counts = [0 for _ in range(len(LATENCY_BUCKETS_MS) + 1)]
    for value in values:
        placed = False
        for index, upper in enumerate(LATENCY_BUCKETS_MS):
            if value <= upper:
                counts[index] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1

    lower = 0.0
    buckets: list[dict[str, Any]] = []
    for index, count in enumerate(counts):
        if count <= 0:
            lower = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else lower
            continue
        upper = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else None
        buckets.append({"lowerMs": lower, "upperMs": upper, "count": count})
        lower = upper if upper is not None else lower
    return buckets


def merge_histograms(histograms: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged: dict[tuple[float, float | None], int] = {}
    for histogram in histograms:
        for bucket in histogram or []:
            key = (float(bucket["lowerMs"]), bucket.get("upperMs"))
            merged[key] = merged.get(key, 0) + int(bucket.get("count", 0) or 0)
    items = [
        {"lowerMs": lower, "upperMs": upper, "count": count}
        for (lower, upper), count in merged.items()
        if count > 0
    ]
    return sorted(
        items,
        key=lambda item: (
            item["lowerMs"],
            math.inf if item["upperMs"] is None else item["upperMs"],
        ),
    )


def summary_from_histogram(histogram: list[dict[str, Any]]) -> dict[str, float]:
    total = sum(int(bucket.get("count", 0) or 0) for bucket in histogram)
    if total <= 0:
        return summarize_values([])

    def percentile(ratio: float) -> float:
        threshold = total * ratio
        seen = 0
        for bucket in histogram:
            seen += int(bucket.get("count", 0) or 0)
            if seen >= threshold:
                upper = bucket.get("upperMs")
                return float(bucket["lowerMs"] if upper is None else upper)
        tail = histogram[-1]
        return float(tail.get("upperMs") or tail["lowerMs"])

    weighted_sum = 0.0
    for bucket in histogram:
        lower = float(bucket["lowerMs"])
        upper = bucket.get("upperMs")
        midpoint = lower if upper is None else (lower + float(upper)) / 2.0
        weighted_sum += midpoint * int(bucket.get("count", 0) or 0)

    first = histogram[0]
    last = histogram[-1]
    return {
        "count": float(total),
        "min": float(first["lowerMs"]),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "p999": percentile(0.999),
        "max": float(last.get("upperMs") or last["lowerMs"]),
        "mean": weighted_sum / total,
    }


def _median_non_zero(values: list[int]) -> float:
    non_zero = [float(value) for value in values if int(value or 0) > 0]
    if not non_zero:
        return 0.0
    return float(median(non_zero))


def _active_bucket_seconds(bucket_map: dict[int, dict[str, Any]]) -> list[int]:
    return [
        second
        for second in sorted(bucket_map)
        if int(bucket_map[second].get("_attempted", 0) or 0) > 0
        or int(bucket_map[second].get("_delivered", 0) or 0) > 0
    ]


def _stabilization_second(bucket_map: dict[int, dict[str, Any]]) -> int:
    active_seconds = _active_bucket_seconds(bucket_map)
    if not active_seconds:
        return 0

    attempted_values = [int(bucket_map[second].get("_attempted", 0) or 0) for second in active_seconds]
    delivered_values = [int(bucket_map[second].get("_delivered", 0) or 0) for second in active_seconds]
    attempted_reference = _median_non_zero(attempted_values)
    delivered_reference = _median_non_zero(delivered_values)
    attempted_threshold = max(1.0, attempted_reference * 0.70) if attempted_reference else 0.0
    delivered_threshold = max(1.0, delivered_reference * 0.70) if delivered_reference else 0.0

    readiness: list[bool] = []
    for second in active_seconds:
        attempted = int(bucket_map[second].get("_attempted", 0) or 0)
        delivered = int(bucket_map[second].get("_delivered", 0) or 0)
        attempted_ready = attempted >= attempted_threshold if attempted_threshold else attempted > 0 or delivered > 0
        delivered_ready = delivered >= delivered_threshold if delivered_threshold else delivered > 0 or attempted > 0
        readiness.append(attempted_ready and delivered_ready)

    if len(active_seconds) >= 6:
        consecutive_required = 3
    elif len(active_seconds) >= 3:
        consecutive_required = 2
    else:
        consecutive_required = 1
    for index, second in enumerate(active_seconds):
        window = readiness[index:index + consecutive_required]
        if len(window) == consecutive_required and all(window):
            return second

    for index, ready in enumerate(readiness):
        if ready:
            return active_seconds[index]
    return active_seconds[0]


def aggregate_agent_results(
    *,
    broker_id: str,
    config_mode: str,
    deployment_mode: str,
    message_rate: int,
    message_size_bytes: int,
    producers: int,
    consumers: int,
    measurement_seconds: int,
    transport_options: dict[str, Any] | None,
    producer_records: Iterable[dict[str, Any]],
    consumer_records: Iterable[dict[str, Any]],
    timing_confidence: str,
    raw_producer_records: list[dict[str, Any]] | None = None,
    raw_consumer_records: list[dict[str, Any]] | None = None,
    producer_timeseries_records: Iterable[dict[str, Any]] | None = None,
    consumer_timeseries_records: Iterable[dict[str, Any]] | None = None,
    resource_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    producer_results = [record.get("result", {}) for record in result_records(producer_records, "producer")]
    consumer_results = [record.get("result", {}) for record in result_records(consumer_records, "consumer")]
    raw_producer = list(raw_producer_records or [])
    raw_consumer = list(raw_consumer_records or [])
    producer_timeseries = producer_timeseries_records or ()
    consumer_timeseries = consumer_timeseries_records or ()
    has_producer_timeseries = producer_timeseries_records is not None
    has_consumer_timeseries = consumer_timeseries_records is not None
    use_raw_producer_records = bool(raw_producer) and not any(
        isinstance(result.get("ackLatencyHdrUs"), list) and result.get("ackLatencyHdrUs")
        for result in producer_results
    )
    use_raw_consumer_records = bool(raw_consumer) and not any(
        isinstance(result.get("latencyHdrUs"), list) and result.get("latencyHdrUs")
        for result in consumer_results
    )

    ack_histograms: list[list[dict[str, Any]]] = []
    histograms: list[list[dict[str, Any]]] = []
    latency_samples: list[float] = []
    ack_latency_samples: list[float] = []
    ack_histogram_snapshots: list[Any] = []
    latency_histogram_snapshots: list[Any] = []
    attempted_total = 0
    delivered_total = 0
    parse_errors = 0
    duplicates = 0
    out_of_order = 0
    bucket_map: dict[int, dict[str, Any]] = {}

    def ensure_bucket(second: int) -> dict[str, Any]:
        return bucket_map.setdefault(
            second,
            {
                "second": second,
                "producerRate": 0,
                "consumerRate": 0,
                "throughput": 0,
                "failureCount": 0,
                "successRate": 100.0,
                "latencyMs": 0.0,
                "latencyP50Ms": 0.0,
                "latencyP95Ms": 0.0,
                "latencyP99Ms": 0.0,
                "_latencySamples": [],
                "_latencyHdrSnapshots": [],
                "_latencyHistograms": [],
                "_ackSamples": [],
                "_ackHdrSnapshots": [],
                "_ackHistograms": [],
                "_delivered": 0,
                "_attempted": 0,
            },
        )

    for result in producer_results:
        attempted_total += int(result.get("attempted", result.get("sent", 0)) or 0)
        ack_snapshot = result.get("ackLatencyHdrUs")
        if isinstance(ack_snapshot, list) and ack_snapshot:
            ack_histogram_snapshots.append(ack_snapshot)
        else:
            ack_histograms.append(result.get("ackLatencyHistogramMs", []) or [])
        if has_producer_timeseries:
            continue
        for bucket in result.get("timeseries", []) or []:
            second = int(bucket.get("second", 0) or 0)
            entry = ensure_bucket(second)
            attempted_messages = int(bucket.get("attemptedMessages", 0) or 0)
            entry["producerRate"] += attempted_messages
            entry["_attempted"] += attempted_messages
            ack_bucket_snapshot = bucket.get("ackLatencyHdrUs")
            if isinstance(ack_bucket_snapshot, list) and ack_bucket_snapshot:
                entry["_ackHdrSnapshots"].append(ack_bucket_snapshot)
            else:
                ack_bucket_histogram = bucket.get("ackLatencyHistogramMs")
                if isinstance(ack_bucket_histogram, list) and ack_bucket_histogram:
                    entry["_ackHistograms"].append(ack_bucket_histogram)

    for bucket in producer_timeseries:
        second = int(bucket.get("second", 0) or 0)
        entry = ensure_bucket(second)
        attempted_messages = int(bucket.get("attemptedMessages", 0) or 0)
        entry["producerRate"] += attempted_messages
        entry["_attempted"] += attempted_messages
        ack_bucket_snapshot = bucket.get("ackLatencyHdrUs")
        if isinstance(ack_bucket_snapshot, list) and ack_bucket_snapshot:
            entry["_ackHdrSnapshots"].append(ack_bucket_snapshot)
        else:
            ack_bucket_histogram = bucket.get("ackLatencyHistogramMs")
            if isinstance(ack_bucket_histogram, list) and ack_bucket_histogram:
                entry["_ackHistograms"].append(ack_bucket_histogram)

    for result in consumer_results:
        parse_errors += int(result.get("parseErrors", 0) or 0)
        duplicates += int(result.get("duplicates", 0) or 0)
        out_of_order += int(result.get("outOfOrder", 0) or 0)
        delivered_total += int(result.get("receivedMeasured", result.get("received", 0)) or 0)
        latency_snapshot = result.get("latencyHdrUs")
        if isinstance(latency_snapshot, list) and latency_snapshot:
            latency_histogram_snapshots.append(latency_snapshot)
        else:
            histograms.append(result.get("latencyHistogramMs", []) or [])
        if has_consumer_timeseries or (use_raw_consumer_records and not latency_snapshot):
            continue
        for bucket in result.get("timeseries", []) or []:
            second = int(bucket.get("second", 0) or 0)
            entry = ensure_bucket(second)
            delivered = int(bucket.get("deliveredMessages", 0) or 0)
            entry["consumerRate"] += delivered
            entry["throughput"] += delivered
            entry["_delivered"] += delivered
            bucket_histogram = bucket.get("latencyHdrUs")
            if isinstance(bucket_histogram, list) and bucket_histogram:
                entry["_latencyHdrSnapshots"].append(bucket_histogram)
            else:
                compact_histogram = bucket.get("latencyHistogramMs")
                if isinstance(compact_histogram, list) and compact_histogram:
                    entry["_latencyHistograms"].append(compact_histogram)
                else:
                    bucket_latency_samples = [
                        float(value) for value in bucket.get("latencySamplesMs", []) or []
                    ]
                    latency_samples.extend(bucket_latency_samples)
                    entry["_latencySamples"].extend(bucket_latency_samples)

    for bucket in consumer_timeseries:
        second = int(bucket.get("second", 0) or 0)
        entry = ensure_bucket(second)
        delivered = int(bucket.get("deliveredMessages", 0) or 0)
        entry["consumerRate"] += delivered
        entry["throughput"] += delivered
        entry["_delivered"] += delivered
        bucket_histogram = bucket.get("latencyHdrUs")
        if isinstance(bucket_histogram, list) and bucket_histogram:
            entry["_latencyHdrSnapshots"].append(bucket_histogram)
        else:
            compact_histogram = bucket.get("latencyHistogramMs")
            if isinstance(compact_histogram, list) and compact_histogram:
                entry["_latencyHistograms"].append(compact_histogram)

    if use_raw_producer_records and raw_producer:
        ack_latency_samples = [float(item.get("ackLatencyMs") or 0.0) for item in raw_producer]
        for item in raw_producer:
            second = int(item.get("phaseSecond", 0) or 0)
            entry = ensure_bucket(second)
            entry.setdefault("_ackSamples", []).append(float(item.get("ackLatencyMs") or 0.0))

    # Deduplicate raw consumer records by event ID to prevent inflated counts
    if use_raw_consumer_records and raw_consumer:
        seen_ids = set()
        deduped = []
        for item in raw_consumer:
            event_id = item.get("eventId") or item.get("id") or id(item)
            if event_id not in seen_ids:
                seen_ids.add(event_id)
                deduped.append(item)
        raw_consumer = deduped

    if use_raw_consumer_records and raw_consumer:
        delivered_total = len(raw_consumer)
        for item in raw_consumer:
            second = int(item.get("phaseSecond", 0) or 0)
            entry = ensure_bucket(second)
            entry["consumerRate"] += 1
            entry["throughput"] += 1
            entry["_delivered"] += 1
            entry["_latencySamples"].append(float(item.get("latencyMs") or 0.0))

    stabilization_second = _stabilization_second(bucket_map)
    if use_raw_producer_records and raw_producer:
        raw_producer = [
            item
            for item in raw_producer
            if int(item.get("phaseSecond", 0) or 0) >= stabilization_second
        ]
    if use_raw_consumer_records and raw_consumer:
        raw_consumer = [
            item
            for item in raw_consumer
            if int(item.get("phaseSecond", 0) or 0) >= stabilization_second
        ]

    filtered_seconds = [
        second
        for second in sorted(bucket_map)
        if second >= stabilization_second
    ]
    filtered_bucket_map = {second: bucket_map[second] for second in filtered_seconds}
    filtered_active_seconds = _active_bucket_seconds(filtered_bucket_map)
    filtered_window_seconds = (
        max(1, filtered_active_seconds[-1] - stabilization_second + 1)
        if filtered_active_seconds
        else max(1, int(measurement_seconds))
    )

    filtered_latency_histogram_snapshots = [
        snapshot
        for second in filtered_seconds
        for snapshot in filtered_bucket_map[second].get("_latencyHdrSnapshots", [])
    ]
    filtered_latency_histograms = [
        histogram
        for second in filtered_seconds
        for histogram in filtered_bucket_map[second].get("_latencyHistograms", [])
    ]
    filtered_ack_histogram_snapshots = [
        snapshot
        for second in filtered_seconds
        for snapshot in filtered_bucket_map[second].get("_ackHdrSnapshots", [])
    ]
    filtered_ack_histograms = [
        histogram
        for second in filtered_seconds
        for histogram in filtered_bucket_map[second].get("_ackHistograms", [])
    ]

    if use_raw_consumer_records and raw_consumer:
        latency_samples = [float(item.get("latencyMs") or 0.0) for item in raw_consumer]
    else:
        latency_samples = [
            float(value)
            for second in filtered_seconds
            for value in filtered_bucket_map[second].get("_latencySamples", [])
        ]
    if use_raw_producer_records and raw_producer:
        ack_latency_samples = [float(item.get("ackLatencyMs") or 0.0) for item in raw_producer]
    else:
        ack_latency_samples = [
            float(value)
            for second in filtered_seconds
            for value in filtered_bucket_map[second].get("_ackSamples", [])
        ]

    bucket_attempted_total = sum(
        int(filtered_bucket_map[second].get("_attempted", 0) or 0)
        for second in filtered_seconds
    )
    bucket_delivered_total = sum(
        int(filtered_bucket_map[second].get("_delivered", 0) or 0)
        for second in filtered_seconds
    )
    attempted_total = bucket_attempted_total or attempted_total
    latency_histogram = None
    if filtered_latency_histogram_snapshots:
        latency_histogram = _merge_histogram_snapshots(filtered_latency_histogram_snapshots)
    elif stabilization_second <= 0 and latency_histogram_snapshots:
        latency_histogram = _merge_histogram_snapshots(latency_histogram_snapshots)
    ack_histogram = None
    if filtered_ack_histogram_snapshots:
        ack_histogram = _merge_histogram_snapshots(filtered_ack_histogram_snapshots)
    elif stabilization_second <= 0 and ack_histogram_snapshots:
        ack_histogram = _merge_histogram_snapshots(ack_histogram_snapshots)

    if latency_histogram is not None and int(latency_histogram.get_total_count() or 0) > 0:
        delivered_total = int(latency_histogram.get_total_count() or 0)
    else:
        delivered_total = (
            len(raw_consumer)
            if use_raw_consumer_records and raw_consumer
            else (bucket_delivered_total or delivered_total)
        )

    if latency_histogram is not None and int(latency_histogram.get_total_count() or 0) > 0:
        merged_latency_histogram = _display_histogram_from_latency_histogram(latency_histogram)
        latency_summary = _summary_from_latency_histogram(latency_histogram)
    else:
        merged_latency_histogram = (
            histogram_from_values(latency_samples)
            if latency_samples
            else merge_histograms(filtered_latency_histograms or histograms)
        )
        latency_summary = summarize_values(latency_samples) if latency_samples else summary_from_histogram(merged_latency_histogram)

    if ack_histogram is not None and int(ack_histogram.get_total_count() or 0) > 0:
        merged_ack_histogram = _display_histogram_from_latency_histogram(ack_histogram)
        ack_summary = _summary_from_latency_histogram(ack_histogram)
    else:
        merged_ack_histogram = (
            histogram_from_values(ack_latency_samples)
            if ack_latency_samples
            else merge_histograms(filtered_ack_histograms or ack_histograms)
        )
        ack_summary = summary_from_histogram(merged_ack_histogram)

    attempted_rate = int(round(attempted_total / filtered_window_seconds)) if attempted_total else 0
    delivered_rate = int(round(delivered_total / filtered_window_seconds)) if delivered_total else 0
    success_rate = round((delivered_total / attempted_total) * 100.0, 4) if attempted_total else 0.0
    success_rate = min(100.0, success_rate)  # Cap at 100% to prevent impossible values
    failure_rate = round(max(0.0, 100.0 - success_rate), 4)

    timeseries: list[dict[str, Any]] = []
    for second in filtered_seconds:
        entry = filtered_bucket_map[second]
        second_histogram_snapshots = entry.pop("_latencyHdrSnapshots", [])
        second_histograms = entry.pop("_latencyHistograms", [])
        second_latency_samples = entry.pop("_latencySamples")
        if second_histogram_snapshots:
            latency = _summary_from_latency_histogram(
                _merge_histogram_snapshots(second_histogram_snapshots)
            )
        else:
            if second_histograms:
                latency = summary_from_histogram(merge_histograms(second_histograms))
            else:
                latency = summarize_values(second_latency_samples)
        entry.pop("_ackHistograms", [])
        entry.pop("_ackHdrSnapshots", [])
        entry.pop("_ackSamples", [])
        attempted = int(entry.pop("_attempted"))
        delivered = int(entry.pop("_delivered"))
        failures = max(0, attempted - delivered)
        point = {
            "second": second,
            "latencyMs": round(latency["p95"], 3),
            "latencyP50Ms": round(latency["p50"], 3),
            "latencyP95Ms": round(latency["p95"], 3),
            "latencyP99Ms": round(latency["p99"], 3),
            "throughput": entry["throughput"],
            "producerRate": entry["producerRate"],
            "consumerRate": entry["consumerRate"],
            "successRate": round(min(100.0, (delivered / attempted) * 100.0), 4) if attempted else 100.0,
            "failureCount": failures,
        }
        timeseries.append(point)

    normalized_resource_samples = sorted(
        [
            dict(sample)
            for sample in (resource_samples or [])
            if isinstance(sample, dict) and int(sample.get("second", 0) or 0) >= stabilization_second
        ],
        key=lambda item: int(item.get("second", 0) or 0),
    )
    if normalized_resource_samples and timeseries:
        timeseries_by_second = {int(point["second"]): point for point in timeseries}
        for sample in normalized_resource_samples:
            second = int(sample.get("second", 0) or 0)
            point = timeseries_by_second.get(second)
            if point is None:
                continue
            point.update(
                {
                    "cpuCores": round(float(sample.get("brokerCpuCores", sample.get("cpuCores", 0.0)) or 0.0), 4),
                    "memoryMB": round(float(sample.get("brokerMemoryMB", sample.get("memoryMB", 0.0)) or 0.0), 2),
                    "networkRxMBps": round(float(sample.get("brokerNetworkRxMBps", 0.0) or 0.0), 4),
                    "networkTxMBps": round(float(sample.get("brokerNetworkTxMBps", 0.0) or 0.0), 4),
                    "storageUsedMB": round(float(sample.get("brokerStorageUsedMB", 0.0) or 0.0), 2),
                    "producerCpuCores": round(float(sample.get("producerCpuCores", 0.0) or 0.0), 4),
                    "producerMemoryMB": round(float(sample.get("producerMemoryMB", 0.0) or 0.0), 2),
                    "consumerCpuCores": round(float(sample.get("consumerCpuCores", 0.0) or 0.0), 4),
                    "consumerMemoryMB": round(float(sample.get("consumerMemoryMB", 0.0) or 0.0), 2),
                }
            )
        timeseries = [timeseries_by_second[key] for key in sorted(timeseries_by_second)]

    resource_usage_summary = None
    if normalized_resource_samples:
        def peak(metric_name: str) -> float:
            return max(float(sample.get(metric_name, 0.0) or 0.0) for sample in normalized_resource_samples)

        latest = normalized_resource_samples[-1]
        resource_usage_summary = {
            "points": normalized_resource_samples,
            "latest": {
                "brokerCpuCores": round(float(latest.get("brokerCpuCores", 0.0) or 0.0), 4),
                "brokerMemoryMB": round(float(latest.get("brokerMemoryMB", 0.0) or 0.0), 2),
                "brokerNetworkRxMBps": round(float(latest.get("brokerNetworkRxMBps", 0.0) or 0.0), 4),
                "brokerNetworkTxMBps": round(float(latest.get("brokerNetworkTxMBps", 0.0) or 0.0), 4),
                "brokerStorageUsedMB": round(float(latest.get("brokerStorageUsedMB", 0.0) or 0.0), 2),
            },
            "peaks": {
                "brokerCpuCores": round(peak("brokerCpuCores"), 4),
                "brokerMemoryMB": round(peak("brokerMemoryMB"), 2),
                "brokerNetworkRxMBps": round(peak("brokerNetworkRxMBps"), 4),
                "brokerNetworkTxMBps": round(peak("brokerNetworkTxMBps"), 4),
                "brokerStorageUsedMB": round(peak("brokerStorageUsedMB"), 2),
                "producerCpuCores": round(peak("producerCpuCores"), 4),
                "producerMemoryMB": round(peak("producerMemoryMB"), 2),
                "consumerCpuCores": round(peak("consumerCpuCores"), 4),
                "consumerMemoryMB": round(peak("consumerMemoryMB"), 2),
            },
        }

    options = transport_options or {}
    rate_profile_kind = str(options.get("rateProfileKind", "constant") or "constant")
    derived_peak = int(message_rate) if rate_profile_kind == "constant" else max(int(message_rate), int(message_rate) * 2)
    peak_message_rate = max(
        int(message_rate),
        int(options.get("peakMessageRate", derived_peak) or derived_peak),
    )
    return {
        "source": "benchmark-agent",
        "measurement": {
            "primaryMetric": "producer_send_to_consumer_receive",
            "cloudEventsMode": "structured-json-v1.0",
            "windowSelection": "producer-tagged-measure-phase",
            "timingConfidence": timing_confidence,
            "effectiveWindowSeconds": filtered_window_seconds,
            "stabilizationTrimSeconds": max(0, stabilization_second),
            "clockNotes": [
                "Latency uses CloudEvent producer send time and consumer receive time for the same message.",
                "Measurement windows are selected by producer-stamped phase metadata carried in the CloudEvent.",
                "On a single-node cluster the producer and consumer usually share the same host clock; multi-node clock skew will widen error bounds.",
            ],
        },
        "summary": {
            "endToEndLatencyMs": latency_summary,
            "producerAckLatencyMs": ack_summary,
            "throughput": {
                "target": int(message_rate),
                "attempted": attempted_rate,
                "achieved": delivered_rate,
                "achievedBytesPerSec": delivered_rate * int(message_size_bytes),
                "effectiveWindowSeconds": filtered_window_seconds,
            },
            "loadProfile": {
                "baseMessageRate": int(message_rate),
                "peakMessageRate": peak_message_rate,
                "rateProfileKind": rate_profile_kind,
            },
            "producerRate": attempted_rate,
            "consumerRate": delivered_rate,
            "deliverySuccessRate": success_rate,
            "deliveryFailureRate": failure_rate,
            "duplicates": duplicates,
            "outOfOrder": out_of_order,
            "parseErrors": parse_errors,
            "messageSizeBytes": int(message_size_bytes),
            "scaling": {
                "producers": int(producers),
                "consumers": int(consumers),
                "factor": int(producers) * int(consumers),
            },
            "resourceUsage": resource_usage_summary,
            "configMode": config_mode,
            "deploymentMode": deployment_mode,
        },
        "histogramMs": merged_latency_histogram,
        "timeseries": timeseries,
    }
