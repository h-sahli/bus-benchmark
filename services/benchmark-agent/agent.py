from __future__ import annotations

import argparse
import asyncio
import atexit
import base64
from collections import deque
import gzip
import json
import math
import os
from pathlib import Path
import random
import signal
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Any

try:
    from confluent_kafka import Consumer as KafkaConsumer
    from confluent_kafka import KafkaError, Producer as KafkaProducer
except ImportError:  # pragma: no cover - exercised by runtime environment
    KafkaConsumer = None
    KafkaProducer = None
    KafkaError = None

try:
    from pika import URLParameters, BlockingConnection
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties
except ImportError:  # pragma: no cover - exercised by runtime environment
    URLParameters = None
    BlockingConnection = None
    BlockingChannel = Any
    Basic = None
    BasicProperties = None

try:
    from proton import Message
    from proton._exceptions import ConnectionException as ProtonConnectionException
    from proton._exceptions import Timeout as ProtonTimeout
    from proton.utils import BlockingConnection as ProtonBlockingConnection
    from proton.utils import LinkDetached
except ImportError:  # pragma: no cover - exercised by runtime environment
    Message = None
    ProtonConnectionException = RuntimeError
    ProtonTimeout = TimeoutError
    ProtonBlockingConnection = None
    LinkDetached = RuntimeError

try:
    import nats
    from nats.errors import TimeoutError as NatsTimeoutError
    from nats.js import api as nats_js_api
except ImportError:  # pragma: no cover - exercised by runtime environment
    nats = None
    NatsTimeoutError = TimeoutError
    nats_js_api = None

try:
    from hdrh.histogram import HdrHistogram
except ImportError:  # pragma: no cover - exercised by runtime environment
    HdrHistogram = None


STOP_REQUESTED = False
STRUCTURED_OUTPUT_HANDLE: Any | None = None
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
RAW_RECORD_CHUNK_SIZE = 2048
RAW_RECORD_SAMPLE_LIMIT = max(0, int(os.environ.get("RAW_RECORD_SAMPLE_LIMIT", "20000")))
RECENT_EVENT_WINDOW = max(1024, int(os.environ.get("RECENT_EVENT_WINDOW", "65536")))
LATENCY_HIGHEST_TRACKABLE_US = max(
    1_000_000,
    int(os.environ.get("LATENCY_HIGHEST_TRACKABLE_US", "60000000")),
)


def configure_structured_output(path: str | None) -> None:
    global STRUCTURED_OUTPUT_HANDLE
    candidate = str(path or "").strip()
    if not candidate:
        return
    target = Path(candidate)
    target.parent.mkdir(parents=True, exist_ok=True)
    STRUCTURED_OUTPUT_HANDLE = target.open("a", encoding="utf-8")


def close_structured_output() -> None:
    global STRUCTURED_OUTPUT_HANDLE
    if STRUCTURED_OUTPUT_HANDLE is None:
        return
    try:
        STRUCTURED_OUTPUT_HANDLE.close()
    finally:
        STRUCTURED_OUTPUT_HANDLE = None


atexit.register(close_structured_output)


def emit_structured_record(record: dict[str, Any]) -> None:
    serialized = json.dumps(record, separators=(",", ":"), sort_keys=False)
    print(serialized, flush=True)
    if STRUCTURED_OUTPUT_HANDLE is not None:
        STRUCTURED_OUTPUT_HANDLE.write(serialized)
        STRUCTURED_OUTPUT_HANDLE.write("\n")
        STRUCTURED_OUTPUT_HANDLE.flush()


class RecentIdWindow:
    def __init__(self, max_items: int = RECENT_EVENT_WINDOW) -> None:
        self.max_items = max(1, int(max_items))
        self._items: set[str] = set()
        self._order: deque[str] = deque()

    def seen_before(self, value: str) -> bool:
        candidate = str(value or "").strip()
        if not candidate:
            return False
        if candidate in self._items:
            return True
        self._items.add(candidate)
        self._order.append(candidate)
        while len(self._order) > self.max_items:
            expired = self._order.popleft()
            self._items.discard(expired)
        return False


def create_latency_histogram() -> Any:
    ensure_dependency("hdrhistogram", HdrHistogram)
    return HdrHistogram(1, LATENCY_HIGHEST_TRACKABLE_US, 3)


def latency_ms_to_histogram_value(latency_ms: float) -> int:
    return max(1, int(round(float(latency_ms or 0.0) * 1000.0)))


def histogram_value_to_latency_ms(value_us: float) -> float:
    return float(value_us) / 1000.0


def serialize_latency_histogram(histogram: Any) -> list[dict[str, int]]:
    if histogram is None or int(histogram.get_total_count() or 0) <= 0:
        return []
    snapshot: list[dict[str, int]] = []
    for item in histogram.get_recorded_iterator():
        snapshot.append(
            {
                "valueUs": int(item.value_iterated_to),
                "count": int(item.count_added_in_this_iter_step),
            }
        )
    return snapshot


def histogram_from_latency_histogram(histogram: Any) -> list[dict[str, Any]]:
    if histogram is None or int(histogram.get_total_count() or 0) <= 0:
        return []
    counts = [0 for _ in range(len(LATENCY_BUCKETS_MS) + 1)]
    for item in histogram.get_recorded_iterator():
        value_ms = histogram_value_to_latency_ms(item.value_iterated_to)
        count = int(item.count_added_in_this_iter_step)
        placed = False
        for index, upper in enumerate(LATENCY_BUCKETS_MS):
            if value_ms <= upper:
                counts[index] += count
                placed = True
                break
        if not placed:
            counts[-1] += count

    buckets: list[dict[str, Any]] = []
    lower = 0.0
    for index, count in enumerate(counts):
        if count <= 0:
            lower = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else lower
            continue
        upper = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else None
        buckets.append({"lowerMs": lower, "upperMs": upper, "count": count})
        lower = upper if upper is not None else lower
    return buckets


def summarize_latency_histogram(histogram: Any) -> dict[str, float]:
    if histogram is None or int(histogram.get_total_count() or 0) <= 0:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }
    return {
        "count": float(histogram.get_total_count()),
        "min": histogram_value_to_latency_ms(histogram.get_min_value()),
        "p50": histogram_value_to_latency_ms(histogram.get_value_at_percentile(50.0)),
        "max": histogram_value_to_latency_ms(histogram.get_max_value()),
        "mean": histogram_value_to_latency_ms(histogram.get_mean_value()),
        "p95": histogram_value_to_latency_ms(histogram.get_value_at_percentile(95.0)),
        "p99": histogram_value_to_latency_ms(histogram.get_value_at_percentile(99.0)),
    }


def parse_scheduled_start_ns(value: str | None) -> int | None:
    if not value:
        return None
    candidate = str(value).strip()
    if not candidate:
        return None
    try:
        numeric = int(candidate)
    except ValueError:
        parsed = datetime.fromisoformat(candidate)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp() * 1_000_000_000)
    if numeric >= 1_000_000_000_000_000_000:
        return numeric
    if numeric >= 1_000_000_000_000_000:
        return numeric * 1_000
    if numeric >= 1_000_000_000_000:
        return numeric * 1_000_000
    if numeric >= 1_000_000_000:
        return numeric * 1_000_000_000
    return numeric


def phase_second_from_elapsed(elapsed_ms: int, warmup_seconds: int) -> int:
    return max(0, int(max(0, elapsed_ms - (warmup_seconds * 1000)) / 1000))


def wait_for_scheduled_start(
    scheduled_start_ns: int | None,
    *,
    role: str,
    agent_id: str,
) -> int:
    if scheduled_start_ns is None:
        return time.time_ns()
    emit_structured_record(
        {
            "kind": "agent-lifecycle",
            "role": role,
            "agentId": agent_id,
            "state": "armed",
            "scheduledStartNs": scheduled_start_ns,
        }
    )
    while not STOP_REQUESTED:
        now_ns = time.time_ns()
        remaining_ns = scheduled_start_ns - now_ns
        if remaining_ns <= 0:
            break
        time.sleep(min(0.25, remaining_ns / 1_000_000_000.0))
    return scheduled_start_ns


def consumer_idle_exit_not_before_ns(
    scheduled_start_ns: int | None,
    *,
    warmup_seconds: int,
    measurement_seconds: int,
    cooldown_seconds: int,
) -> int:
    base_ns = scheduled_start_ns or time.time_ns()
    total_window_ns = max(0, warmup_seconds + measurement_seconds + cooldown_seconds) * 1_000_000_000
    return base_ns + total_window_ns


def consumer_stop_after_ns(
    idle_exit_not_before_ns: int | None,
    *,
    idle_exit_seconds: int,
) -> int | None:
    if idle_exit_not_before_ns is None:
        return None
    return idle_exit_not_before_ns + (max(0, int(idle_exit_seconds)) * 1_000_000_000)


def should_force_exit_consumer(*, stop_after_ns: int | None) -> bool:
    return stop_after_ns is not None and time.time_ns() >= stop_after_ns


def should_exit_consumer_idle(
    *,
    idle_exit_not_before_ns: int | None,
    idle_exit_seconds: int,
    last_activity: float,
) -> bool:
    if idle_exit_not_before_ns is not None and time.time_ns() < idle_exit_not_before_ns:
        return False
    return time.monotonic() - last_activity >= idle_exit_seconds


def should_exit_consumer(
    *,
    stop_after_ns: int | None,
    idle_exit_not_before_ns: int | None,
    idle_exit_seconds: int,
    last_activity: float,
) -> bool:
    if should_force_exit_consumer(stop_after_ns=stop_after_ns):
        return True
    return should_exit_consumer_idle(
        idle_exit_not_before_ns=idle_exit_not_before_ns,
        idle_exit_seconds=idle_exit_seconds,
        last_activity=last_activity,
    )


class RawRecordChunkBuffer:
    def __init__(
        self,
        *,
        role: str,
        schema: list[str],
        kind: str = "measurement-record-chunk",
        chunk_size: int = RAW_RECORD_CHUNK_SIZE,
        sample_limit: int | None = RAW_RECORD_SAMPLE_LIMIT,
    ) -> None:
        self.role = role
        self.kind = str(kind or "measurement-record-chunk").strip() or "measurement-record-chunk"
        self.schema = list(schema)
        self.chunk_size = max(1, int(chunk_size))
        self.sample_limit = None if sample_limit is None else max(0, int(sample_limit))
        self._rows: list[list[Any]] = []
        self._observed_count = 0

    def add(self, row: list[Any]) -> None:
        self._observed_count += 1
        if self.sample_limit is None:
            self._rows.append(row)
            return
        if self.sample_limit <= 0:
            return
        if len(self._rows) < self.sample_limit:
            self._rows.append(row)
            return
        replacement_index = random.randrange(self._observed_count)
        if replacement_index < self.sample_limit:
            self._rows[replacement_index] = row

    def emit(self, *, run_id: str, broker: str, agent_id: str) -> int:
        if not self._rows:
            return 0
        emitted = 0
        for index in range(0, len(self._rows), self.chunk_size):
            chunk_rows = self._rows[index:index + self.chunk_size]
            payload = gzip.compress(
                json.dumps(
                    {"schema": self.schema, "rows": chunk_rows},
                    separators=(",", ":"),
                    sort_keys=False,
                ).encode("utf-8")
            )
            encoded = base64.b64encode(payload).decode("ascii")
            emit_structured_record(
                {
                    "kind": self.kind,
                    "role": self.role,
                    "runId": run_id,
                    "broker": broker,
                    "agentId": agent_id,
                    "recordCount": len(chunk_rows),
                    "chunkIndex": index // self.chunk_size,
                    "encoding": "gzip+base64+json",
                    "payload": encoded,
                    "sampled": self.sample_limit is not None and self._observed_count > len(self._rows),
                    "observedCount": self._observed_count,
                }
            )
            emitted += len(chunk_rows)
        return emitted


def ensure_dependency(name: str, dependency: Any) -> None:
    if dependency is None:
        raise RuntimeError(
            f"Missing optional dependency for {name}. Install services/benchmark-agent/requirements.txt"
        )


def handle_signal(signum: int, frame: Any) -> None:
    del frame
    print(f"[agent] signal={signum} received, stopping gracefully", flush=True)
    global STOP_REQUESTED
    STOP_REQUESTED = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_payload(size: int, seed: int) -> bytes:
    rng = random.Random(seed)
    return bytes(rng.getrandbits(8) for _ in range(size))


def classify_phase(
    elapsed_ms: int,
    *,
    warmup_seconds: int,
    measurement_seconds: int,
    cooldown_seconds: int,
) -> str:
    warmup_ms = max(0, warmup_seconds) * 1000
    measurement_ms = max(0, measurement_seconds) * 1000
    cooldown_ms = max(0, cooldown_seconds) * 1000
    if elapsed_ms < warmup_ms:
        return "warmup"
    if elapsed_ms < warmup_ms + measurement_ms:
        return "measure"
    if elapsed_ms < warmup_ms + measurement_ms + cooldown_ms:
        return "cooldown"
    return "complete"


def effective_rate(message_rate: int, peak_message_rate: int, rate_profile: str, progress: float) -> float:
    base = max(1.0, float(message_rate))
    peak = max(base, float(peak_message_rate))
    mode = str(rate_profile or "constant").strip().lower()
    progress = max(0.0, min(1.0, float(progress)))
    if mode == "ramp-up":
        return base + ((peak - base) * progress)
    if mode == "ramp-down":
        return peak - ((peak - base) * progress)
    if mode == "step-up":
        return peak if progress >= 0.5 else base
    if mode == "burst":
        return peak if 0.55 <= progress <= 0.75 else base
    return base


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
    measurement_phase: str,
    phase_elapsed_ms: int,
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
        "producersendstartns": producer_send_ns,
        "producerinvokepublishns": 0,
        "measurementphase": measurement_phase,
        "phaseelapsedms": phase_elapsed_ms,
        "contractversion": "1.0.0",
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
        "measurementphase",
        "phaseelapsedms",
    ]
    missing = [item for item in required if item not in parsed]
    if missing:
        raise ValueError(f"CloudEvent missing required fields: {','.join(missing)}")
    return parsed


@dataclass
class ProducerResult:
    sent: int
    publish_errors: int
    latencies_ms: list[float]


@dataclass
class ConsumerResult:
    received: int
    parse_errors: int
    duplicates: int
    out_of_order: int
    latencies_ms: list[float]


def log_consumer_error(kind: str, exc: Exception, count: int) -> None:
    if count <= 3:
        print(f"[consumer][{kind}] {type(exc).__name__}: {exc}", flush=True)


def raw_message_bytes(body: Any) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)
    if isinstance(body, memoryview):
        return body.tobytes()
    return str(body).encode("utf-8")


def jetstream_stream_name(destination: str) -> str:
    normalized = "".join(
        character if character.isalnum() else "-"
        for character in str(destination or "benchmark-events")
    ).strip("-")
    return (normalized or "benchmark-events").upper()


def normalize_jetstream_replicas(value: int | None) -> int:
    try:
        candidate = int(value or 1)
    except (TypeError, ValueError):
        candidate = 1
    return max(1, min(5, candidate))


def normalize_jetstream_stream_manager(value: str | None) -> str:
    candidate = str(value or "client").strip().lower()
    return candidate if candidate in {"client", "nack"} else "client"


def jetstream_stream_config(stream_name: str, destination: str, replicas: int) -> Any:
    ensure_dependency("nats", nats_js_api)
    return nats_js_api.StreamConfig(
        name=stream_name,
        subjects=[destination],
        storage=nats_js_api.StorageType.FILE,
        num_replicas=normalize_jetstream_replicas(replicas),
    )


def jetstream_consumer_config(replicas: int) -> Any:
    ensure_dependency("nats", nats_js_api)
    return nats_js_api.ConsumerConfig(
        ack_policy=nats_js_api.AckPolicy.EXPLICIT,
        deliver_policy=nats_js_api.DeliverPolicy.NEW,
        num_replicas=normalize_jetstream_replicas(replicas),
    )


def is_retryable_connect_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    markers = (
        "connection refused",
        "timed out",
        "temporarily unavailable",
        "connection reset",
        "stream lost",
        "streamlosterror",
        "connection workflow failed",
        "amqpconnectionerror",
        "connection closed unexpectedly",
        "failed to all addresses",
        "refused to all addresses",
        "connection abort",
        "nobrokersavailable",
        "no brokers available",
        "no servers available",
    )
    return any(marker in text for marker in markers)


def with_retryable_startup(
    factory: Any,
    *,
    role: str,
    broker_id: str,
    timeout_seconds: int,
) -> Any:
    deadline = time.monotonic() + max(5, int(timeout_seconds))
    attempts = 0
    while True:
        try:
            return factory()
        except Exception as exc:
            attempts += 1
            if STOP_REQUESTED or not is_retryable_connect_error(exc) or time.monotonic() >= deadline:
                raise
            if attempts <= 3:
                print(
                    f"[agent][{role}][{broker_id}] startup retry after transient failure: {exc}",
                    flush=True,
                )
            time.sleep(1.0)


class KafkaAdapter:
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str,
        *,
        acks: str,
        linger_ms: int,
        batch_size_bytes: int,
        compression_type: str,
        socket_nagle_disable: bool,
        max_in_flight_requests: int,
        request_timeout_ms: int,
        buffer_memory_bytes: int,
        max_request_size_bytes: int,
        delivery_timeout_ms: int,
    ) -> None:
        ensure_dependency("kafka", KafkaProducer)
        self.completed_deliveries: list[dict[str, Any]] = []
        effective_request_timeout = max(1000, request_timeout_ms)
        effective_delivery_timeout = max(
            max(1000, int(delivery_timeout_ms)),
            effective_request_timeout + max(0, int(linger_ms)),
        )
        self.producer = KafkaProducer(
            {
                "bootstrap.servers": ",".join(
                    item.strip() for item in bootstrap_servers.split(",") if item.strip()
                ),
                "client.id": client_id,
                "acks": str(acks),
                "linger.ms": max(0, linger_ms),
                "batch.size": max(1, batch_size_bytes),
                "compression.type": str(compression_type),
                "socket.nagle.disable": bool(socket_nagle_disable),
                "max.in.flight.requests.per.connection": max(1, max_in_flight_requests),
                "request.timeout.ms": effective_request_timeout,
                "delivery.timeout.ms": effective_delivery_timeout,
                "queue.buffering.max.kbytes": max(1024, int(buffer_memory_bytes / 1024)),
                "message.max.bytes": max(1024, max_request_size_bytes),
            }
        )

    def publish(
        self,
        destination: str,
        payload: bytes,
        key: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        delivery_context = dict(context or {})

        def on_delivery(err: Any, msg: Any) -> None:
            self.completed_deliveries.append(
                {
                    "eventId": key,
                    "ackNs": time.time_ns(),
                    "error": str(err) if err is not None else "",
                    "partition": str(msg.partition()) if msg is not None else "",
                    "offset": str(msg.offset()) if msg is not None else "",
                    "routing_key": "",
                    "redelivered": "false",
                    "delivery_attempt": "1",
                    "context": delivery_context,
                }
            )

        while not STOP_REQUESTED:
            try:
                self.producer.produce(
                    destination,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                self.producer.poll(0.005)
        self.producer.poll(0)
        return {
            "partition": "",
            "offset": "",
            "routing_key": "",
            "redelivered": "false",
            "delivery_attempt": "1",
        }

    def drain_deliveries(self) -> list[dict[str, Any]]:
        self.producer.poll(0)
        deliveries = list(self.completed_deliveries)
        self.completed_deliveries.clear()
        return deliveries

    def flush(self) -> None:
        self.producer.flush(30.0)

    def close(self) -> None:
        self.producer.flush(10.0)

    @staticmethod
    def consume(
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        handler: Any,
        message_limit: int,
        idle_exit_seconds: int,
        idle_exit_not_before_ns: int | None,
        fetch_min_bytes: int,
        fetch_max_wait_ms: int,
        max_poll_records: int,
        max_partition_fetch_bytes: int,
        fetch_max_bytes: int,
        session_timeout_ms: int,
        heartbeat_interval_ms: int,
        enable_auto_commit: bool,
        max_poll_interval_ms: int,
        socket_nagle_disable: bool,
    ) -> ConsumerResult:
        ensure_dependency("kafka", KafkaConsumer)
        stop_after_ns = consumer_stop_after_ns(
            idle_exit_not_before_ns,
            idle_exit_seconds=idle_exit_seconds,
        )
        consume_timeout_seconds = min(
            0.25,
            max(0.02, (max(1, int(fetch_max_wait_ms)) / 1000.0) * 2.0),
        )
        consumer = KafkaConsumer(
            {
                "bootstrap.servers": ",".join(
                    item.strip() for item in bootstrap_servers.split(",") if item.strip()
                ),
                "group.id": group_id,
                "enable.auto.commit": bool(enable_auto_commit),
                "auto.offset.reset": "latest",
                "fetch.min.bytes": max(1, fetch_min_bytes),
                "fetch.wait.max.ms": max(1, fetch_max_wait_ms),
                "max.partition.fetch.bytes": max(1024, max_partition_fetch_bytes),
                "fetch.max.bytes": max(1024, fetch_max_bytes),
                "session.timeout.ms": max(1000, session_timeout_ms),
                "heartbeat.interval.ms": max(500, heartbeat_interval_ms),
                "max.poll.interval.ms": max(1000, max_poll_interval_ms),
                "socket.nagle.disable": bool(socket_nagle_disable),
            }
        )
        consumer.subscribe([topic])

        received = 0
        parse_errors = 0
        duplicates = 0
        out_of_order = 0
        recent_ids = RecentIdWindow()
        last_seq_by_producer: dict[str, int] = {}
        last_activity = time.monotonic()
        parse_error_logs = 0

        try:
            while not STOP_REQUESTED and (message_limit <= 0 or received < message_limit):
                if should_force_exit_consumer(stop_after_ns=stop_after_ns):
                    break
                records = consumer.consume(
                    num_messages=max(1, max_poll_records),
                    timeout=consume_timeout_seconds,
                )
                if not records:
                    if should_exit_consumer(
                        stop_after_ns=stop_after_ns,
                        idle_exit_not_before_ns=idle_exit_not_before_ns,
                        idle_exit_seconds=idle_exit_seconds,
                        last_activity=last_activity,
                    ):
                        break
                    continue
                for record in records:
                    if should_force_exit_consumer(stop_after_ns=stop_after_ns):
                        break
                    if record is None:
                        continue
                    if record.error():
                        if KafkaError is not None and record.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise RuntimeError(str(record.error()))
                    try:
                        metrics = handler(
                            record.value(),
                            {
                                "partition": str(record.partition()),
                                "offset": str(record.offset()),
                                "routing_key": "",
                                "redelivered": "false",
                                "delivery_attempt": "1",
                            },
                        )
                        event_id = metrics["event_id"]
                        producer_id = str(metrics.get("producer_id") or "")
                        seq = metrics["sequence"]
                        if recent_ids.seen_before(event_id):
                            duplicates += 1
                        last_seq = last_seq_by_producer.get(producer_id, -1)
                        if last_seq >= 0 and seq < last_seq:
                            out_of_order += 1
                        last_seq_by_producer[producer_id] = max(last_seq, seq)
                        received += 1
                        last_activity = time.monotonic()
                    except Exception as exc:
                        parse_errors += 1
                        parse_error_logs += 1
                        log_consumer_error("parse-error", exc, parse_error_logs)
        finally:
            consumer.close()

        return ConsumerResult(
            received=received,
            parse_errors=parse_errors,
            duplicates=duplicates,
            out_of_order=out_of_order,
            latencies_ms=[],
        )


class RabbitMqAdapter:
    def __init__(
        self,
        broker_url: str,
        *,
        publisher_confirms: bool,
        heartbeat_sec: int,
        frame_max: int,
        channel_max: int,
        connection_timeout_ms: int,
    ) -> None:
        ensure_dependency("rabbitmq", BlockingConnection)
        ensure_dependency("rabbitmq", URLParameters)
        parameters = URLParameters(broker_url)
        parameters.heartbeat = max(1, heartbeat_sec)
        parameters.frame_max = max(4096, frame_max)
        parameters.channel_max = max(1, channel_max)
        parameters.socket_timeout = max(1.0, connection_timeout_ms / 1000.0)
        self.connection = BlockingConnection(parameters)
        self.channel: BlockingChannel = self.connection.channel()
        self._declared_queues: set[str] = set()
        if publisher_confirms:
            self.channel.confirm_delivery()

    def _ensure_queue_declared(self, destination: str) -> None:
        if destination in self._declared_queues:
            return
        self.channel.queue_declare(queue=destination, durable=True)
        self._declared_queues.add(destination)

    def publish(self, destination: str, payload: bytes, key: str, mandatory: bool) -> dict[str, str]:
        ensure_dependency("rabbitmq", BasicProperties)
        self._ensure_queue_declared(destination)
        self.channel.basic_publish(
            exchange="",
            routing_key=destination,
            body=payload,
            properties=BasicProperties(
                delivery_mode=2,
                message_id=key,
                content_type="application/cloudevents+json",
            ),
            mandatory=mandatory,
        )
        return {
            "partition": "",
            "offset": "",
            "routing_key": destination,
            "redelivered": "false",
            "delivery_attempt": "1",
        }

    def close(self) -> None:
        self.connection.close()

    @staticmethod
    def consume(
        *,
        broker_url: str,
        queue_name: str,
        prefetch: int,
        prefetch_global: bool,
        auto_ack: bool,
        handler: Any,
        message_limit: int,
        idle_exit_seconds: int,
        idle_exit_not_before_ns: int | None,
    ) -> ConsumerResult:
        ensure_dependency("rabbitmq", BlockingConnection)
        ensure_dependency("rabbitmq", URLParameters)
        connection = BlockingConnection(URLParameters(broker_url))
        channel: BlockingChannel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_qos(prefetch_count=prefetch, global_qos=prefetch_global)

        received = 0
        parse_errors = 0
        duplicates = 0
        out_of_order = 0
        recent_ids = RecentIdWindow()
        last_seq_by_producer: dict[str, int] = {}
        last_activity = time.monotonic()
        parse_error_logs = 0
        stop_after_ns = consumer_stop_after_ns(
            idle_exit_not_before_ns,
            idle_exit_seconds=idle_exit_seconds,
        )

        try:
            consumer_stream = channel.consume(
                queue=queue_name,
                auto_ack=auto_ack,
                inactivity_timeout=1,
            )
            for method_frame, properties, body in consumer_stream:
                if (
                    STOP_REQUESTED
                    or should_force_exit_consumer(stop_after_ns=stop_after_ns)
                    or (message_limit > 0 and received >= message_limit)
                ):
                    break
                if method_frame is None:
                    if should_exit_consumer(
                        stop_after_ns=stop_after_ns,
                        idle_exit_not_before_ns=idle_exit_not_before_ns,
                        idle_exit_seconds=idle_exit_seconds,
                        last_activity=last_activity,
                    ):
                        break
                    continue
                try:
                    headers = getattr(properties, "headers", None) if properties is not None else None
                    if not isinstance(headers, dict):
                        headers = {}
                    metrics = handler(
                        body,
                        {
                            "partition": "",
                            "offset": "",
                            "routing_key": method_frame.routing_key,
                            "redelivered": str(bool(method_frame.redelivered)).lower(),
                            "delivery_attempt": str(headers.get("x-delivery-count", 1)),
                        },
                    )
                    event_id = metrics["event_id"]
                    producer_id = str(metrics.get("producer_id") or "")
                    seq = metrics["sequence"]
                    if recent_ids.seen_before(event_id):
                        duplicates += 1
                    last_seq = last_seq_by_producer.get(producer_id, -1)
                    if last_seq >= 0 and seq < last_seq:
                        out_of_order += 1
                    last_seq_by_producer[producer_id] = max(last_seq, seq)
                    received += 1
                    last_activity = time.monotonic()
                    if not auto_ack:
                        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                except Exception as exc:
                    parse_errors += 1
                    parse_error_logs += 1
                    log_consumer_error("parse-error", exc, parse_error_logs)
                    if not auto_ack:
                        channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
        finally:
            try:
                channel.cancel()
            except Exception:
                pass
            channel.close()
            connection.close()

            return ConsumerResult(
                received=received,
                parse_errors=parse_errors,
                duplicates=duplicates,
                out_of_order=out_of_order,
                latencies_ms=[],
            )


class ArtemisAdapter:
    def __init__(
        self,
        broker_url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        connect_timeout_seconds: int = 60,
    ) -> None:
        ensure_dependency("artemis", ProtonBlockingConnection)
        connection_url = broker_url
        if username:
            parsed = urllib.parse.urlsplit(broker_url)
            hostname = parsed.hostname or ""
            if not hostname:
                raise RuntimeError("Broker URL for Artemis is missing a hostname")
            netloc = f"{urllib.parse.quote(username)}"
            if password:
                netloc += f":{urllib.parse.quote(password)}"
            netloc += f"@{hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            connection_url = urllib.parse.urlunsplit(
                (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
            )
        self.connection_url = connection_url
        self.connect_timeout_seconds = max(1, int(connect_timeout_seconds))
        self.connection: Any | None = None
        self._senders: dict[str, Any] = {}
        self._connect()

    def _connect(self) -> None:
        deadline = time.monotonic() + self.connect_timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline and not STOP_REQUESTED:
            try:
                self.connection = ProtonBlockingConnection(self.connection_url, timeout=30)
                self._senders = {}
                break
            except (ProtonConnectionException, OSError) as exc:
                last_error = exc
                if not is_retryable_connect_error(exc):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                print(
                    f"[artemis][connect-retry] waiting for broker endpoint: {exc}",
                    flush=True,
                )
                time.sleep(min(2.0, max(0.25, remaining)))
        else:
            self.connection = None
        if self.connection is None:
            raise RuntimeError(
                f"Unable to connect to Artemis broker within {self.connect_timeout_seconds}s"
            ) from last_error

    def reconnect(self) -> None:
        try:
            if self.connection is not None:
                self.connection.close()
        except Exception:
            pass
        self.connection = None
        self._senders = {}
        self._connect()

    def publish(self, destination: str, payload: bytes, key: str) -> dict[str, str]:
        ensure_dependency("artemis", Message)
        message = Message(
            id=key,
            address=destination,
            content_type="application/cloudevents+json",
            body=payload,
            durable=True,
        )
        last_error: Exception | None = None
        for _ in range(2):
            try:
                sender = self._senders.get(destination)
                if sender is None:
                    sender = self.connection.create_sender(destination)
                    self._senders[destination] = sender
                sender.send(message)
                return {
                    "partition": "",
                    "offset": "",
                    "routing_key": destination,
                    "redelivered": "false",
                    "delivery_attempt": "1",
                }
            except (LinkDetached, ProtonConnectionException, OSError) as exc:
                last_error = exc
                self.reconnect()
        raise RuntimeError(f"Unable to publish to Artemis destination {destination}") from last_error

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()

    @staticmethod
    def consume(
        *,
        broker_url: str,
        address: str,
        username: str | None,
        password: str | None,
        handler: Any,
        message_limit: int,
        idle_exit_seconds: int,
        idle_exit_not_before_ns: int | None,
    ) -> ConsumerResult:
        ensure_dependency("artemis", ProtonBlockingConnection)
        adapter = ArtemisAdapter(
            broker_url,
            username=username,
            password=password,
            connect_timeout_seconds=max(5, idle_exit_seconds),
        )
        receiver: Any | None = None

        def open_receiver() -> Any:
            return adapter.connection.create_receiver(address)

        received = 0
        parse_errors = 0
        duplicates = 0
        out_of_order = 0
        recent_ids = RecentIdWindow()
        last_seq_by_producer: dict[str, int] = {}
        last_activity = time.monotonic()
        parse_error_logs = 0
        stop_after_ns = consumer_stop_after_ns(
            idle_exit_not_before_ns,
            idle_exit_seconds=idle_exit_seconds,
        )

        try:
            while not STOP_REQUESTED and (message_limit <= 0 or received < message_limit):
                if should_force_exit_consumer(stop_after_ns=stop_after_ns):
                    break
                if receiver is None:
                    try:
                        receiver = open_receiver()
                    except (LinkDetached, ProtonConnectionException, OSError):
                        adapter.reconnect()
                        time.sleep(0.5)
                        continue
                try:
                    message = receiver.receive(timeout=1)
                except (TimeoutError, ProtonTimeout):
                    if should_exit_consumer(
                        stop_after_ns=stop_after_ns,
                        idle_exit_not_before_ns=idle_exit_not_before_ns,
                        idle_exit_seconds=idle_exit_seconds,
                        last_activity=last_activity,
                    ):
                        break
                    continue
                except (LinkDetached, ProtonConnectionException, OSError):
                    receiver = None
                    adapter.reconnect()
                    time.sleep(0.5)
                    continue
                if message is None:
                    continue
                raw_payload = raw_message_bytes(message.body)
                try:
                    redelivered = str(bool(getattr(message, "redelivered", False))).lower()
                    delivery_attempt = getattr(message, "delivery_count", None)
                    metrics = handler(
                        raw_payload,
                        {
                            "partition": "",
                            "offset": "",
                            "routing_key": address,
                            "redelivered": redelivered,
                            "delivery_attempt": str(delivery_attempt or 1),
                        },
                    )
                    event_id = metrics["event_id"]
                    producer_id = str(metrics.get("producer_id") or "")
                    seq = metrics["sequence"]
                    if recent_ids.seen_before(event_id):
                        duplicates += 1
                    last_seq = last_seq_by_producer.get(producer_id, -1)
                    if last_seq >= 0 and seq < last_seq:
                        out_of_order += 1
                    last_seq_by_producer[producer_id] = max(last_seq, seq)
                    received += 1
                    last_activity = time.monotonic()
                    receiver.accept()
                except Exception as exc:
                    parse_errors += 1
                    parse_error_logs += 1
                    log_consumer_error("parse-error", exc, parse_error_logs)
                    try:
                        receiver.reject()
                    except Exception:
                        receiver.release()
        finally:
            adapter.close()

        return ConsumerResult(
            received=received,
            parse_errors=parse_errors,
            duplicates=duplicates,
            out_of_order=out_of_order,
            latencies_ms=[],
        )


class NatsJetStreamAdapter:
    def __init__(
        self,
        broker_url: str,
        *,
        stream_name: str,
        jetstream_replicas: int = 1,
        stream_managed_by: str = "client",
        connect_timeout_seconds: int = 60,
    ) -> None:
        ensure_dependency("nats", nats)
        self.broker_url = broker_url
        self.stream_name = stream_name
        self.jetstream_replicas = normalize_jetstream_replicas(jetstream_replicas)
        self.stream_managed_by = normalize_jetstream_stream_manager(stream_managed_by)
        self.connect_timeout_seconds = max(1, int(connect_timeout_seconds))
        self.loop = asyncio.new_event_loop()
        self.nc: Any | None = None
        self.js: Any | None = None
        self.completed_deliveries: list[dict[str, Any]] = []
        self.pending_deliveries: list[tuple[str, str, asyncio.Task[Any], dict[str, Any]]] = []
        self._ensured_destinations: set[str] = set()
        self.max_pending_publishes = max(8, int(os.environ.get("JETSTREAM_MAX_PENDING_PUBLISHES", "1024")))
        self._connect()

    def _run_async(self, awaitable: Any) -> Any:
        try:
            asyncio.set_event_loop(self.loop)
            return self.loop.run_until_complete(awaitable)
        finally:
            asyncio.set_event_loop(None)

    async def _ensure_stream_async(self, destination: str) -> None:
        config = jetstream_stream_config(
            self.stream_name,
            destination,
            self.jetstream_replicas,
        )
        try:
            await self.js.add_stream(config=config)
        except Exception as exc:
            message = str(exc or "").strip().lower()
            if "stream name already in use" in message or "stream already exists" in message:
                await self.js.update_stream(config=config)
                return
            raise

    def _ensure_stream(self, destination: str) -> None:
        if self.stream_managed_by != "client" or destination in self._ensured_destinations:
            return
        self._run_async(self._ensure_stream_async(destination))
        self._ensured_destinations.add(destination)

    def _collect_completed_deliveries(self, *, wait_for_one: bool) -> None:
        if not self.pending_deliveries:
            return

        async def await_tasks(tasks: list[asyncio.Task[Any]], timeout: float) -> set[asyncio.Task[Any]]:
            done, _pending = await asyncio.wait(
                tasks,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED if wait_for_one else asyncio.ALL_COMPLETED,
            )
            return set(done)

        tasks = [task for _event_id, _destination, task, _context in self.pending_deliveries]
        completed = self._run_async(await_tasks(tasks, 0.25 if wait_for_one else 0.0))
        if not completed:
            return

        remaining: list[tuple[str, str, asyncio.Task[Any], dict[str, Any]]] = []
        for event_id, destination, task, context in self.pending_deliveries:
            if task not in completed:
                remaining.append((event_id, destination, task, context))
                continue
            error = ""
            offset = ""
            try:
                ack = task.result()
                offset = str(getattr(ack, "seq", "") or "")
            except Exception as exc:
                error = str(exc)
            self.completed_deliveries.append(
                {
                    "eventId": event_id,
                    "ackNs": time.time_ns(),
                    "error": error,
                    "partition": "",
                    "offset": offset,
                    "routing_key": destination,
                    "redelivered": "false",
                    "delivery_attempt": "1",
                    "context": dict(context or {}),
                }
            )
        self.pending_deliveries = remaining

    def _connect(self) -> None:
        self.nc = self._run_async(
            nats.connect(
                servers=[self.broker_url],
                connect_timeout=self.connect_timeout_seconds,
                max_reconnect_attempts=-1,
                reconnect_time_wait=1,
            )
        )
        self.js = self.nc.jetstream()

    def publish(
        self,
        destination: str,
        payload: bytes,
        key: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        self._ensure_stream(destination)
        if context is None:
            ack = self._run_async(self.js.publish(destination, payload))
            return {
                "partition": "",
                "offset": str(getattr(ack, "seq", "") or ""),
                "routing_key": destination,
                "redelivered": "false",
                "delivery_attempt": "1",
            }

        task = self.loop.create_task(self.js.publish(destination, payload))
        self.pending_deliveries.append((key, destination, task, dict(context or {})))
        self._collect_completed_deliveries(wait_for_one=False)
        while len(self.pending_deliveries) >= self.max_pending_publishes:
            self._collect_completed_deliveries(wait_for_one=True)
        return {
            "partition": "",
            "offset": "",
            "routing_key": destination,
            "redelivered": "false",
            "delivery_attempt": "1",
        }

    def drain_deliveries(self) -> list[dict[str, Any]]:
        self._collect_completed_deliveries(wait_for_one=False)
        deliveries = list(self.completed_deliveries)
        self.completed_deliveries.clear()
        return deliveries

    def flush(self) -> None:
        while self.pending_deliveries:
            self._collect_completed_deliveries(wait_for_one=True)

    def close(self) -> None:
        self.flush()
        if self.nc is not None:
            try:
                self._run_async(self.nc.close())
            finally:
                self.nc = None
        if not self.loop.is_closed():
            self.loop.close()

    @staticmethod
    def consume(
        *,
        broker_url: str,
        subject: str,
        consumer_group: str,
        handler: Any,
        message_limit: int,
        idle_exit_seconds: int,
        idle_exit_not_before_ns: int | None,
        connect_timeout_seconds: int,
        jetstream_replicas: int,
        stream_managed_by: str = "client",
    ) -> ConsumerResult:
        ensure_dependency("nats", nats)
        loop = asyncio.new_event_loop()
        normalized_replicas = normalize_jetstream_replicas(jetstream_replicas)
        normalized_stream_manager = normalize_jetstream_stream_manager(stream_managed_by)

        async def consume_async() -> ConsumerResult:
            nc = await nats.connect(
                servers=[broker_url],
                connect_timeout=max(1, connect_timeout_seconds),
                max_reconnect_attempts=-1,
                reconnect_time_wait=1,
            )
            js = nc.jetstream()
            stream_name = jetstream_stream_name(subject)
            try:
                if normalized_stream_manager == "client":
                    stream_config = jetstream_stream_config(
                        stream_name,
                        subject,
                        normalized_replicas,
                    )
                    try:
                        await js.add_stream(config=stream_config)
                    except Exception as exc:
                        message = str(exc or "").strip().lower()
                        if "stream name already in use" not in message and "stream already exists" not in message:
                            raise
                        await js.update_stream(config=stream_config)
                subscribe_kwargs = {
                    "durable": consumer_group or None,
                    "config": jetstream_consumer_config(normalized_replicas),
                    "manual_ack": True,
                }
                subscribe_kwargs = {
                    key: value for key, value in subscribe_kwargs.items() if value is not None
                }
                if consumer_group:
                    subscription = await js.subscribe(subject, queue=consumer_group, **subscribe_kwargs)
                else:
                    subscription = await js.subscribe(subject, **subscribe_kwargs)

                received = 0
                parse_errors = 0
                duplicates = 0
                out_of_order = 0
                recent_ids = RecentIdWindow()
                last_seq_by_producer: dict[str, int] = {}
                last_activity = time.monotonic()
                parse_error_logs = 0
                stop_after_ns = consumer_stop_after_ns(
                    idle_exit_not_before_ns,
                    idle_exit_seconds=idle_exit_seconds,
                )

                while not STOP_REQUESTED and (message_limit <= 0 or received < message_limit):
                    if should_force_exit_consumer(stop_after_ns=stop_after_ns):
                        break
                    try:
                        message = await subscription.next_msg(timeout=1)
                    except (TimeoutError, NatsTimeoutError):
                        if should_exit_consumer(
                            stop_after_ns=stop_after_ns,
                            idle_exit_not_before_ns=idle_exit_not_before_ns,
                            idle_exit_seconds=idle_exit_seconds,
                            last_activity=last_activity,
                        ):
                            break
                        continue

                    metadata = getattr(message, "metadata", None)
                    delivery_attempt = int(getattr(metadata, "num_delivered", 1) or 1)
                    sequence_meta = getattr(metadata, "sequence", None)
                    stream_offset = str(getattr(sequence_meta, "stream", "") or "")
                    raw_payload = raw_message_bytes(message.data)
                    try:
                        metrics = handler(
                            raw_payload,
                            {
                                "partition": "",
                                "offset": stream_offset,
                                "routing_key": subject,
                                "redelivered": str(delivery_attempt > 1).lower(),
                                "delivery_attempt": str(delivery_attempt),
                            },
                        )
                        event_id = metrics["event_id"]
                        producer_id = str(metrics.get("producer_id") or "")
                        sequence = metrics["sequence"]
                        if recent_ids.seen_before(event_id):
                            duplicates += 1
                        last_seq = last_seq_by_producer.get(producer_id, -1)
                        if last_seq >= 0 and sequence < last_seq:
                            out_of_order += 1
                        last_seq_by_producer[producer_id] = max(last_seq, sequence)
                        received += 1
                        last_activity = time.monotonic()
                    except Exception as exc:
                        parse_errors += 1
                        parse_error_logs += 1
                        log_consumer_error("parse-error", exc, parse_error_logs)
                    finally:
                        await message.ack()

                return ConsumerResult(
                    received=received,
                    parse_errors=parse_errors,
                    duplicates=duplicates,
                    out_of_order=out_of_order,
                    latencies_ms=[],
                )
            finally:
                await nc.close()

        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(consume_async())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def summarize_latencies(latencies_ms: list[float]) -> dict[str, float]:
    if not latencies_ms:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }
    sorted_values = sorted(latencies_ms)

    def percentile(p: float) -> float:
        idx = int((len(sorted_values) - 1) * p)
        return float(sorted_values[idx])

    return {
        "count": float(len(sorted_values)),
        "min": float(sorted_values[0]),
        "p50": percentile(0.50),
        "max": float(sorted_values[-1]),
        "mean": float(mean(sorted_values)),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
    }


def latency_histogram(latencies_ms: list[float]) -> list[dict[str, Any]]:
    if not latencies_ms:
        return []
    counts = [0 for _ in range(len(LATENCY_BUCKETS_MS) + 1)]
    for value in latencies_ms:
        placed = False
        for index, upper in enumerate(LATENCY_BUCKETS_MS):
            if value <= upper:
                counts[index] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1

    buckets: list[dict[str, Any]] = []
    lower = 0.0
    for index, count in enumerate(counts):
        if count <= 0:
            lower = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else lower
            continue
        upper = LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else None
        buckets.append({"lowerMs": lower, "upperMs": upper, "count": count})
        lower = upper if upper is not None else lower
    return buckets


def build_producer_adapter(args: argparse.Namespace, producer_id: str) -> Any:
    producer_factories: dict[str, Any] = {
        "kafka": lambda: KafkaAdapter(
            args.bootstrap_servers,
            producer_id,
            acks=args.acks,
            linger_ms=args.linger_ms,
            batch_size_bytes=args.batch_size_bytes,
            compression_type=args.compression_type,
            socket_nagle_disable=args.producer_socket_nagle_disable,
            max_in_flight_requests=args.max_in_flight_requests,
            request_timeout_ms=args.request_timeout_ms,
            buffer_memory_bytes=args.buffer_memory_bytes,
            max_request_size_bytes=args.max_request_size_bytes,
            delivery_timeout_ms=args.delivery_timeout_ms,
        ),
        "rabbitmq": lambda: RabbitMqAdapter(
            args.broker_url,
            publisher_confirms=args.publisher_confirms,
            heartbeat_sec=args.heartbeat_sec,
            frame_max=args.frame_max,
            channel_max=args.channel_max,
            connection_timeout_ms=args.connection_timeout_ms,
        ),
        "artemis": lambda: ArtemisAdapter(
            args.broker_url,
            username=args.broker_username or None,
            password=args.broker_password or None,
            connect_timeout_seconds=args.connect_timeout_seconds,
        ),
        "nats": lambda: NatsJetStreamAdapter(
            args.broker_url,
            stream_name=jetstream_stream_name(args.destination),
            jetstream_replicas=args.jetstream_replicas,
            stream_managed_by=args.jetstream_stream_managed_by,
            connect_timeout_seconds=args.connect_timeout_seconds,
        ),
    }
    return producer_factories[args.broker]()


def publish_message(
    *,
    adapter: Any,
    broker_id: str,
    destination: str,
    payload: bytes,
    event_id: str,
    mandatory: bool,
    context: dict[str, Any] | None = None,
) -> dict[str, str]:
    if broker_id == "rabbitmq":
        return adapter.publish(destination, payload, key=event_id, mandatory=mandatory)
    if broker_id == "kafka":
        return adapter.publish(destination, payload, key=event_id, context=context)
    if broker_id == "nats":
        return adapter.publish(destination, payload, key=event_id, context=context)
    return adapter.publish(destination, payload, key=event_id)


def consume_messages(
    args: argparse.Namespace,
    handler: Any,
) -> ConsumerResult:
    scheduled_start_ns = parse_scheduled_start_ns(args.scheduled_start_at)
    idle_exit_not_before_ns = consumer_idle_exit_not_before_ns(
        scheduled_start_ns,
        warmup_seconds=args.warmup_seconds,
        measurement_seconds=args.measurement_seconds,
        cooldown_seconds=args.cooldown_seconds,
    )
    consumer_runners: dict[str, Any] = {
        "kafka": lambda: KafkaAdapter.consume(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.destination,
            group_id=args.consumer_group or "bench-consumers",
            handler=handler,
            message_limit=args.message_limit,
            idle_exit_seconds=args.idle_exit_seconds,
            idle_exit_not_before_ns=idle_exit_not_before_ns,
            fetch_min_bytes=args.fetch_min_bytes,
            fetch_max_wait_ms=args.fetch_max_wait_ms,
            max_poll_records=args.max_poll_records,
            max_partition_fetch_bytes=args.max_partition_fetch_bytes,
            fetch_max_bytes=args.fetch_max_bytes,
            session_timeout_ms=args.session_timeout_ms,
            heartbeat_interval_ms=args.heartbeat_interval_ms,
            enable_auto_commit=args.enable_auto_commit,
            max_poll_interval_ms=args.max_poll_interval_ms,
            socket_nagle_disable=args.consumer_socket_nagle_disable,
        ),
        "rabbitmq": lambda: RabbitMqAdapter.consume(
            broker_url=args.broker_url,
            queue_name=args.destination,
            prefetch=args.prefetch,
            prefetch_global=args.prefetch_global,
            auto_ack=args.auto_ack,
            handler=handler,
            message_limit=args.message_limit,
            idle_exit_seconds=args.idle_exit_seconds,
            idle_exit_not_before_ns=idle_exit_not_before_ns,
        ),
        "artemis": lambda: ArtemisAdapter.consume(
            broker_url=args.broker_url,
            address=args.destination,
            username=args.broker_username or None,
            password=args.broker_password or None,
            handler=handler,
            message_limit=args.message_limit,
            idle_exit_seconds=args.idle_exit_seconds,
            idle_exit_not_before_ns=idle_exit_not_before_ns,
        ),
        "nats": lambda: NatsJetStreamAdapter.consume(
            broker_url=args.broker_url,
            subject=args.destination,
            consumer_group=args.consumer_group or "bench-consumers",
            handler=handler,
            message_limit=args.message_limit,
            idle_exit_seconds=args.idle_exit_seconds,
            idle_exit_not_before_ns=idle_exit_not_before_ns,
            connect_timeout_seconds=args.connect_timeout_seconds,
            jetstream_replicas=args.jetstream_replicas,
            stream_managed_by=args.jetstream_stream_managed_by,
        ),
    }
    return consumer_runners[args.broker]()


def run_producer(args: argparse.Namespace) -> int:
    producer_id = args.producer_id or f"producer-{uuid.uuid4().hex[:8]}"
    publish_errors = 0
    measured_publish_errors = 0
    sent = 0
    attempted = 0
    measured_attempted = 0
    measured_sent = 0
    sequence = 0
    scheduled_start_ns = parse_scheduled_start_ns(args.scheduled_start_at)
    next_emit = 0.0
    run_started_ns = scheduled_start_ns or time.time_ns()
    ack_histogram = create_latency_histogram()
    ack_timeseries: dict[int, Any] = {}
    timeseries: dict[int, dict[str, int]] = {}
    raw_records = RawRecordChunkBuffer(
        role="producer",
        schema=[
            "eventId",
            "producerId",
            "producerSeq",
            "producerSendNs",
            "producerInvokePublishNs",
            "producerAckNs",
            "ackLatencyMs",
            "phaseElapsedMs",
            "phaseSecond",
            "routingKey",
            "partition",
            "offset",
        ],
    )
    timeseries_records = RawRecordChunkBuffer(
        role="producer",
        kind="measurement-timeseries-chunk",
        schema=[
            "second",
            "attemptedMessages",
            "sentMessages",
            "ackLatencyHistogramMs",
        ],
        chunk_size=240,
        sample_limit=None,
    )

    adapter = with_retryable_startup(
        lambda: build_producer_adapter(args, producer_id),
        role="producer",
        broker_id=args.broker,
        timeout_seconds=args.connect_timeout_seconds,
    )
    pending_delivery_contexts: dict[str, dict[str, Any]] = {}
    uses_delivery_queue = hasattr(adapter, "drain_deliveries")

    def ensure_measure_bucket(second: int) -> dict[str, int]:
        return timeseries.setdefault(
            second,
            {"second": second, "attemptedMessages": 0, "sentMessages": 0},
        )

    def ensure_ack_bucket(second: int) -> Any:
        return ack_timeseries.setdefault(second, create_latency_histogram())

    def drain_async_deliveries() -> None:
        nonlocal sent, publish_errors, measured_publish_errors, measured_sent
        if not uses_delivery_queue:
            return
        for delivery in adapter.drain_deliveries():
            event_id = str(delivery.get("eventId") or "").strip()
            context = pending_delivery_contexts.pop(event_id, None)
            if context is None:
                continue
            error = str(delivery.get("error") or "").strip()
            phase = str(context["phase"])
            second = int(context["phaseSecond"])
            if error:
                publish_errors += 1
                print(f"[producer][error] sequence={context['sequence']} reason={error}", flush=True)
                if phase == "measure":
                    measured_publish_errors += 1
                continue
            ack_ns = int(delivery.get("ackNs") or time.time_ns())
            ack_latency_ms = (ack_ns - int(context["producerSendNs"])) / 1_000_000.0
            sent += 1
            if phase == "measure":
                histogram_value = latency_ms_to_histogram_value(ack_latency_ms)
                ack_histogram.record_value(histogram_value)
                ensure_ack_bucket(second).record_value(histogram_value)
                measured_sent += 1
                ensure_measure_bucket(second)["sentMessages"] += 1
                raw_records.add(
                    [
                        event_id,
                        producer_id,
                        int(context["sequence"]),
                        int(context["producerSendNs"]),
                        int(context["producerInvokePublishNs"]),
                        int(ack_ns),
                        round(ack_latency_ms, 6),
                        int(context["phaseElapsedMs"]),
                        int(second),
                        str(delivery.get("routing_key", "")),
                        str(delivery.get("partition", "")),
                        str(delivery.get("offset", "")),
                    ]
                )

    try:
        run_started_ns = wait_for_scheduled_start(
            scheduled_start_ns,
            role="producer",
            agent_id=producer_id,
        )
        next_emit = time.perf_counter()
        while not STOP_REQUESTED and sequence < args.message_count:
            drain_async_deliveries()
            elapsed_ms = int((time.time_ns() - run_started_ns) / 1_000_000)
            phase = classify_phase(
                elapsed_ms,
                warmup_seconds=args.warmup_seconds,
                measurement_seconds=args.measurement_seconds,
                cooldown_seconds=args.cooldown_seconds,
            )
            if phase == "complete":
                break
            payload = make_payload(args.message_size_bytes, sequence)
            event, producer_send_start_ns = build_cloudevent(
                run_id=args.run_id,
                scenario_id=args.scenario_id,
                broker_id=args.broker,
                producer_id=producer_id,
                receiver_id=args.receiver_id,
                sequence=sequence,
                payload=payload,
                destination=args.destination,
                config_mode=args.config_mode,
                topology_mode=args.topology_mode,
                durability_mode=args.durability_mode,
                measurement_phase=phase,
                phase_elapsed_ms=elapsed_ms,
            )
            raw = json.dumps(event, separators=(",", ":"), sort_keys=False).encode("utf-8")
            invoke_ns = time.time_ns()
            event["producerinvokepublishns"] = invoke_ns
            raw = json.dumps(event, separators=(",", ":"), sort_keys=False).encode("utf-8")
            attempted += 1
            if phase == "measure":
                measured_attempted += 1
                second = phase_second_from_elapsed(elapsed_ms, args.warmup_seconds)
                ensure_measure_bucket(second)["attemptedMessages"] += 1
            try:
                publish_metadata: dict[str, str]
                event_id = str(event["id"])
                phase_second = phase_second_from_elapsed(elapsed_ms, args.warmup_seconds)
                publish_context = {
                    "sequence": int(event["producerseq"]),
                    "producerSendNs": int(producer_send_start_ns),
                    "producerInvokePublishNs": int(invoke_ns),
                    "phase": phase,
                    "phaseElapsedMs": int(elapsed_ms),
                    "phaseSecond": int(phase_second),
                }
                if uses_delivery_queue:
                    pending_delivery_contexts[event_id] = publish_context
                publish_metadata = publish_message(
                    adapter=adapter,
                    broker_id=args.broker,
                    destination=args.destination,
                    payload=raw,
                    event_id=event_id,
                    mandatory=args.mandatory,
                    context=publish_context,
                )
                if not uses_delivery_queue:
                    ack_ns = time.time_ns()
                    ack_latency_ms = (ack_ns - producer_send_start_ns) / 1_000_000.0
                    if phase == "measure":
                        histogram_value = latency_ms_to_histogram_value(ack_latency_ms)
                        ack_histogram.record_value(histogram_value)
                        ensure_ack_bucket(phase_second).record_value(histogram_value)
                        measured_sent += 1
                        ensure_measure_bucket(phase_second)["sentMessages"] += 1
                        raw_records.add(
                            [
                                event_id,
                                producer_id,
                                int(event["producerseq"]),
                                int(producer_send_start_ns),
                                int(invoke_ns),
                                int(ack_ns),
                                round(ack_latency_ms, 6),
                                int(elapsed_ms),
                                int(phase_second),
                                str(publish_metadata.get("routing_key", "")),
                                str(publish_metadata.get("partition", "")),
                                str(publish_metadata.get("offset", "")),
                            ]
                        )
                    sent += 1
            except Exception as exc:
                if uses_delivery_queue:
                    pending_delivery_contexts.pop(event_id, None)
                publish_errors += 1
                print(f"[producer][error] sequence={sequence} reason={exc}", flush=True)
                if phase == "measure":
                    measured_publish_errors += 1

            sequence += 1
            if uses_delivery_queue:
                drain_async_deliveries()
            progress = min(
                1.0,
                elapsed_ms / max(1.0, (args.warmup_seconds + args.measurement_seconds + args.cooldown_seconds) * 1000.0),
            )
            current_rate = max(
                1.0,
                effective_rate(
                    args.message_rate,
                    args.peak_message_rate,
                    args.rate_profile,
                    progress,
                ),
            )
            next_emit += 1.0 / current_rate
            sleep_for = next_emit - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

        if uses_delivery_queue:
            adapter.flush()
            drain_async_deliveries()
    finally:
        adapter.close()

    result = ProducerResult(sent=sent, publish_errors=publish_errors, latencies_ms=[])
    raw_record_count = raw_records.emit(
        run_id=args.run_id,
        broker=args.broker,
        agent_id=producer_id,
    )
    for second in sorted(timeseries):
        point: dict[str, Any] = dict(timeseries[second])
        histogram = ack_timeseries.get(second)
        timeseries_records.add(
            [
                int(point.get("second", second)),
                int(point.get("attemptedMessages", 0) or 0),
                int(point.get("sentMessages", 0) or 0),
                histogram_from_latency_histogram(histogram),
            ]
        )
    timeseries_record_count = timeseries_records.emit(
        run_id=args.run_id,
        broker=args.broker,
        agent_id=producer_id,
    )
    emit_structured_record(
        {
            "role": "producer",
            "runId": args.run_id,
            "broker": args.broker,
            "result": {
                "attempted": measured_attempted,
                "attemptedTotal": attempted,
                "sent": measured_sent,
                "sentTotal": result.sent,
                "publishErrors": measured_publish_errors,
                "publishErrorsTotal": result.publish_errors,
                "ackLatencyMs": summarize_latency_histogram(ack_histogram),
                "ackLatencyHistogramMs": histogram_from_latency_histogram(ack_histogram),
                "ackLatencyHdrUs": serialize_latency_histogram(ack_histogram),
                "timingWindow": {
                    "warmupSeconds": args.warmup_seconds,
                    "measurementSeconds": args.measurement_seconds,
                    "cooldownSeconds": args.cooldown_seconds,
                },
                "measurementContract": {
                    "primaryMetric": "producer_ack",
                    "phaseSelection": "measure",
                    "cloudEventsMode": "structured-json-v1.0",
                },
                "rawRecordCount": raw_record_count,
                "timeseriesRecordCount": timeseries_record_count,
                "scheduledStartNs": scheduled_start_ns,
            },
        }
    )
    return 0 if publish_errors == 0 else 2


def run_consumer(args: argparse.Namespace) -> int:
    consumer_id = args.consumer_id or f"consumer-{uuid.uuid4().hex[:8]}"
    measured_latency_histogram = create_latency_histogram()
    latency_timeseries: dict[int, Any] = {}
    raw_records = RawRecordChunkBuffer(
        role="consumer",
        schema=[
            "eventId",
            "consumerId",
            "producerId",
            "producerSeq",
            "producerSendNs",
            "consumerReceiveNs",
            "latencyMs",
            "phaseElapsedMs",
            "phaseSecond",
            "redelivered",
            "deliveryAttempt",
            "routingKey",
        ],
    )
    timeseries_records = RawRecordChunkBuffer(
        role="consumer",
        kind="measurement-timeseries-chunk",
        schema=[
            "second",
            "deliveredMessages",
            "latencyHistogramMs",
        ],
        chunk_size=240,
        sample_limit=None,
    )

    def handler(raw_payload: bytes, transport: dict[str, str]) -> dict[str, Any]:
        parsed = parse_cloudevent(raw_payload)
        receive_ns = time.time_ns()
        start_ns = int(parsed["producersendstartns"])
        latency_ms = (receive_ns - start_ns) / 1_000_000.0
        phase = str(parsed["measurementphase"])
        phase_elapsed_ms = int(parsed["phaseelapsedms"])
        measured = phase == "measure"
        if measured:
            histogram_value = latency_ms_to_histogram_value(latency_ms)
            measured_latency_histogram.record_value(histogram_value)
            second = phase_second_from_elapsed(phase_elapsed_ms, args.warmup_seconds)
            latency_timeseries.setdefault(second, create_latency_histogram()).record_value(histogram_value)
            raw_records.add(
                [
                    str(parsed["id"]),
                    consumer_id,
                    str(parsed["produceragentid"]),
                    int(parsed["producerseq"]),
                    int(start_ns),
                    int(receive_ns),
                    round(latency_ms, 6),
                    int(phase_elapsed_ms),
                    int(second),
                    str(transport.get("redelivered", "false")),
                    str(transport.get("delivery_attempt", "1")),
                    str(transport.get("routing_key", "")),
                ]
            )
        return {
            "event_id": str(parsed["id"]),
            "producer_id": str(parsed["produceragentid"]),
            "sequence": int(parsed["producerseq"]),
            "latency_ms": latency_ms,
            "phase": phase,
            "phase_elapsed_ms": phase_elapsed_ms,
            "measured": measured,
            "transport": transport,
        }

    result = with_retryable_startup(
        lambda: consume_messages(args, handler),
        role="consumer",
        broker_id=args.broker,
        timeout_seconds=args.connect_timeout_seconds,
    )

    summary = summarize_latency_histogram(measured_latency_histogram)
    for second in sorted(latency_timeseries):
        bucket_histogram = latency_timeseries[second]
        timeseries_records.add(
            [
                int(second),
                int(bucket_histogram.get_total_count() or 0),
                histogram_from_latency_histogram(bucket_histogram),
            ]
        )
    raw_record_count = raw_records.emit(
        run_id=args.run_id,
        broker=args.broker,
        agent_id=consumer_id,
    )
    timeseries_record_count = timeseries_records.emit(
        run_id=args.run_id,
        broker=args.broker,
        agent_id=consumer_id,
    )
    emit_structured_record(
        {
            "role": "consumer",
            "runId": args.run_id,
            "broker": args.broker,
            "consumerId": consumer_id,
            "result": {
                "received": result.received,
                "receivedMeasured": int(measured_latency_histogram.get_total_count() or 0),
                "parseErrors": result.parse_errors,
                "duplicates": result.duplicates,
                "outOfOrder": result.out_of_order,
                "endToEndLatencyMs": summary,
                "latencyHistogramMs": histogram_from_latency_histogram(measured_latency_histogram),
                "latencyHdrUs": serialize_latency_histogram(measured_latency_histogram),
                "measurementContract": {
                    "primaryMetric": "producer_send_to_consumer_receive",
                    "phaseSelection": "measure",
                    "cloudEventsMode": "structured-json-v1.0",
                },
                "rawRecordCount": raw_record_count,
                "timeseriesRecordCount": timeseries_record_count,
                "timingWindow": {
                    "warmupSeconds": args.warmup_seconds,
                    "measurementSeconds": args.measurement_seconds,
                    "cooldownSeconds": args.cooldown_seconds,
                },
            },
        }
    )
    return 0 if result.parse_errors == 0 else 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark producer/consumer agent")
    parser.add_argument("--role", choices=["producer", "consumer"], required=True)
    parser.add_argument("--broker", choices=["kafka", "rabbitmq", "artemis", "nats"], required=True)
    parser.add_argument("--run-id", default=os.environ.get("RUN_ID", str(uuid.uuid4())))
    parser.add_argument("--scenario-id", default=os.environ.get("SCENARIO_ID", "default-scenario"))
    parser.add_argument("--destination", default=os.environ.get("DESTINATION", "benchmark.events"))
    parser.add_argument("--config-mode", default=os.environ.get("CONFIG_MODE", "baseline"))
    parser.add_argument("--topology-mode", default=os.environ.get("TOPOLOGY_MODE", "spsc"))
    parser.add_argument("--durability-mode", default=os.environ.get("DURABILITY_MODE", "persistent"))
    parser.add_argument("--bootstrap-servers", default=os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--broker-url", default=os.environ.get("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/%2F"))
    parser.add_argument("--broker-username", default=os.environ.get("BROKER_USERNAME", ""))
    parser.add_argument("--broker-password", default=os.environ.get("BROKER_PASSWORD", ""))
    parser.add_argument("--producer-id", default=os.environ.get("PRODUCER_ID", "producer-1"))
    parser.add_argument("--receiver-id", default=os.environ.get("RECEIVER_ID", "consumer-1"))
    parser.add_argument("--consumer-id", default=os.environ.get("CONSUMER_ID", "consumer-1"))
    parser.add_argument("--consumer-group", default=os.environ.get("CONSUMER_GROUP", "bench-consumers"))
    parser.add_argument("--message-size-bytes", type=int, default=int(os.environ.get("MESSAGE_SIZE_BYTES", "1024")))
    parser.add_argument("--message-rate", type=int, default=int(os.environ.get("MESSAGE_RATE", "5000")))
    parser.add_argument("--peak-message-rate", type=int, default=int(os.environ.get("PEAK_MESSAGE_RATE", "5000")))
    parser.add_argument("--rate-profile", choices=["constant", "ramp-up", "ramp-down", "step-up", "burst"], default=os.environ.get("RATE_PROFILE", "constant"))
    parser.add_argument("--message-count", type=int, default=int(os.environ.get("MESSAGE_COUNT", "10000")))
    parser.add_argument("--message-limit", type=int, default=int(os.environ.get("MESSAGE_LIMIT", "10000")))
    parser.add_argument("--scheduled-start-at", default=os.environ.get("SCHEDULED_START_AT", ""))
    parser.add_argument("--warmup-seconds", type=int, default=int(os.environ.get("WARMUP_SECONDS", "0")))
    parser.add_argument("--measurement-seconds", type=int, default=int(os.environ.get("MEASUREMENT_SECONDS", "60")))
    parser.add_argument("--cooldown-seconds", type=int, default=int(os.environ.get("COOLDOWN_SECONDS", "0")))
    parser.add_argument("--idle-exit-seconds", type=int, default=int(os.environ.get("IDLE_EXIT_SECONDS", "15")))
    parser.add_argument("--linger-ms", type=int, default=int(os.environ.get("LINGER_MS", "0")))
    parser.add_argument("--batch-size-bytes", type=int, default=int(os.environ.get("BATCH_SIZE_BYTES", "16384")))
    parser.add_argument("--compression-type", default=os.environ.get("COMPRESSION_TYPE", "none"))
    parser.add_argument("--acks", default=os.environ.get("ACKS", "all"))
    parser.add_argument("--max-in-flight-requests", type=int, default=int(os.environ.get("MAX_IN_FLIGHT_REQUESTS", "5")))
    parser.add_argument("--request-timeout-ms", type=int, default=int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")))
    parser.add_argument("--buffer-memory-bytes", type=int, default=int(os.environ.get("BUFFER_MEMORY_BYTES", "33554432")))
    parser.add_argument("--max-request-size-bytes", type=int, default=int(os.environ.get("MAX_REQUEST_SIZE_BYTES", "1048576")))
    parser.add_argument("--delivery-timeout-ms", type=int, default=int(os.environ.get("DELIVERY_TIMEOUT_MS", "120000")))
    parser.add_argument(
        "--producer-socket-nagle-disable",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("PRODUCER_SOCKET_NAGLE_DISABLE", "false")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument("--fetch-min-bytes", type=int, default=int(os.environ.get("FETCH_MIN_BYTES", "1")))
    parser.add_argument("--fetch-max-wait-ms", type=int, default=int(os.environ.get("FETCH_MAX_WAIT_MS", "500")))
    parser.add_argument("--max-poll-records", type=int, default=int(os.environ.get("MAX_POLL_RECORDS", "500")))
    parser.add_argument("--max-partition-fetch-bytes", type=int, default=int(os.environ.get("MAX_PARTITION_FETCH_BYTES", "1048576")))
    parser.add_argument("--fetch-max-bytes", type=int, default=int(os.environ.get("FETCH_MAX_BYTES", "52428800")))
    parser.add_argument("--session-timeout-ms", type=int, default=int(os.environ.get("SESSION_TIMEOUT_MS", "45000")))
    parser.add_argument("--heartbeat-interval-ms", type=int, default=int(os.environ.get("HEARTBEAT_INTERVAL_MS", "3000")))
    parser.add_argument("--max-poll-interval-ms", type=int, default=int(os.environ.get("MAX_POLL_INTERVAL_MS", "300000")))
    parser.add_argument(
        "--enable-auto-commit",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("ENABLE_AUTO_COMMIT", "false")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument(
        "--consumer-socket-nagle-disable",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("CONSUMER_SOCKET_NAGLE_DISABLE", "false")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument("--jetstream-replicas", type=int, default=int(os.environ.get("JETSTREAM_REPLICAS", "1")))
    parser.add_argument(
        "--jetstream-stream-managed-by",
        choices=["client", "nack"],
        default=normalize_jetstream_stream_manager(
            os.environ.get("JETSTREAM_STREAM_MANAGED_BY", "client")
        ),
    )
    parser.add_argument("--prefetch", type=int, default=int(os.environ.get("PREFETCH", "500")))
    parser.add_argument(
        "--prefetch-global",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("PREFETCH_GLOBAL", "false")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument(
        "--auto-ack",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("AUTO_ACK", "false")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument(
        "--publisher-confirms",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("PUBLISHER_CONFIRMS", "true")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument(
        "--mandatory",
        type=lambda item: str(item).lower() in {"1", "true", "yes", "y"},
        default=str(os.environ.get("MANDATORY", "false")).lower() in {"1", "true", "yes", "y"},
    )
    parser.add_argument("--heartbeat-sec", type=int, default=int(os.environ.get("HEARTBEAT_SEC", "30")))
    parser.add_argument("--frame-max", type=int, default=int(os.environ.get("FRAME_MAX", "131072")))
    parser.add_argument("--channel-max", type=int, default=int(os.environ.get("CHANNEL_MAX", "2047")))
    parser.add_argument("--connection-timeout-ms", type=int, default=int(os.environ.get("CONNECTION_TIMEOUT_MS", "30000")))
    parser.add_argument("--connect-timeout-seconds", type=int, default=int(os.environ.get("CONNECT_TIMEOUT_SECONDS", "60")))
    parser.add_argument("--structured-output-path", default=os.environ.get("STRUCTURED_OUTPUT_PATH", ""))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    configure_structured_output(args.structured_output_path)
    if args.role == "producer":
        return run_producer(args)
    return run_consumer(args)


if __name__ == "__main__":
    sys.exit(main())
