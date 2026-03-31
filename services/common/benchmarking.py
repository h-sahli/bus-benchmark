from __future__ import annotations

import base64
import json
import math
import time
import uuid
from datetime import datetime, timezone
from statistics import mean, pstdev
from typing import Any


DEFAULT_HISTOGRAM_EDGES_MS = [
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    bounded = min(1.0, max(0.0, fraction))
    index = int(round((len(sorted_values) - 1) * bounded))
    return float(sorted_values[index])


def summarize_latencies(latencies_ms: list[float]) -> dict[str, float]:
    if not latencies_ms:
        return {
            "count": 0.0,
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "stddev": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "p999": 0.0,
        }
    sorted_values = sorted(float(value) for value in latencies_ms)
    return {
        "count": float(len(sorted_values)),
        "min": float(sorted_values[0]),
        "max": float(sorted_values[-1]),
        "mean": float(mean(sorted_values)),
        "stddev": float(pstdev(sorted_values)) if len(sorted_values) > 1 else 0.0,
        "p50": percentile(sorted_values, 0.50),
        "p95": percentile(sorted_values, 0.95),
        "p99": percentile(sorted_values, 0.99),
        "p999": percentile(sorted_values, 0.999),
    }


def latency_histogram_ms(
    latencies_ms: list[float], edges_ms: list[float] | None = None
) -> list[dict[str, float]]:
    edges = edges_ms or DEFAULT_HISTOGRAM_EDGES_MS
    if not latencies_ms:
        return []
    counts = [0 for _ in range(len(edges) + 1)]
    for value in latencies_ms:
        index = len(edges)
        for edge_index, edge in enumerate(edges):
            if value <= edge:
                index = edge_index
                break
        counts[index] += 1

    lower = 0.0
    buckets: list[dict[str, float]] = []
    for edge, count in zip(edges, counts[:-1]):
        buckets.append(
            {
                "lowerBoundMs": lower,
                "upperBoundMs": float(edge),
                "count": float(count),
            }
        )
        lower = float(edge)
    buckets.append(
        {
            "lowerBoundMs": lower,
            "upperBoundMs": math.inf,
            "count": float(counts[-1]),
        }
    )
    return buckets


def build_cloudevent(
    *,
    run_id: str,
    scenario_id: str,
    broker_id: str,
    producer_id: str,
    receiver_id: str,
    sequence: int,
    payload: bytes,
    destination: str,
    config_mode: str,
    topology_mode: str,
    durability_mode: str,
    phase: str = "measurement",
    producer_elapsed_ms: int = 0,
) -> tuple[dict[str, Any], int]:
    producer_send_ns = time.time_ns()
    event_id = str(uuid.uuid4())
    event = {
        "specversion": "1.0",
        "id": event_id,
        "source": f"/bus/benchmark/{producer_id}",
        "type": "io.ninefinger9.benchmark.message.v1",
        "time": utc_now_iso(),
        "datacontenttype": "application/octet-stream",
        "subject": destination,
        "benchmarkrunid": run_id,
        "scenarioid": scenario_id,
        "brokerid": broker_id,
        "produceragentid": producer_id,
        "receiverid": receiver_id,
        "payloadsizebytes": len(payload),
        "producerseq": sequence,
        "testmode": "latency",
        "topologymode": topology_mode,
        "durabilitymode": durability_mode,
        "configmode": config_mode,
        "destinationkind": "queue-or-topic",
        "destinationname": destination,
        "benchmarkphase": phase,
        "producerelapsedms": int(max(0, producer_elapsed_ms)),
        "producersendstartns": producer_send_ns,
        "producerinvokepublishns": 0,
        "measurementmodel": "application-producer-send-to-consumer-receive",
        "cloudeventmode": "structured-json",
        "contractversion": "1.1.0",
        "data_base64": base64.b64encode(payload).decode("ascii"),
    }
    return event, producer_send_ns


def parse_cloudevent(raw_payload: bytes) -> dict[str, Any]:
    parsed = json.loads(raw_payload.decode("utf-8"))
    required = [
        "specversion",
        "id",
        "source",
        "type",
        "time",
        "benchmarkrunid",
        "scenarioid",
        "brokerid",
        "produceragentid",
        "receiverid",
        "payloadsizebytes",
        "producerseq",
        "producersendstartns",
        "benchmarkphase",
    ]
    missing = [item for item in required if item not in parsed]
    if missing:
        raise ValueError(f"CloudEvent missing required fields: {','.join(missing)}")
    return parsed
