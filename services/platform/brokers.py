from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable

from fastapi import HTTPException

from services.platform.versions import (
    artemis_operator_version,
    broker_version,
    nack_controller_version,
    nats_chart_version,
    rabbitmq_operator_version,
    strimzi_operator_version,
)


BROKER_CATALOG: dict[str, dict[str, Any]] = {
    "kafka": {
        "id": "kafka",
        "label": "Apache Kafka",
        "defaultProtocol": "kafka",
        "operatorRequired": True,
        "controlPlaneRequired": True,
        "controlPlaneMode": "cluster-operator",
        "protocolOptions": ["kafka"],
        "protocolDetails": {
            "kafka": "Native Kafka wire protocol.",
        },
        "deploymentMode": "latest-stable-auto",
        "latestStableVersion": broker_version("kafka"),
        "operatorVersion": f"strimzi {strimzi_operator_version()}",
        "modeNotes": {
            "baseline": "Starts close to the normal Kafka defaults for a neutral reference point.",
            "optimized": "Starts from a split-role Kafka layout with lower-latency broker and client defaults.",
        },
        "deploymentNotes": {
            "normal": "Separated KRaft controller and broker pools: 1 controller pod plus 1 broker pod.",
            "ha": "Separated KRaft controller and broker pools: 3 controller pods plus 3 broker pods.",
        },
        "latencyNotes": [
            "Kafka controllers are isolated from data brokers so metadata work does not compete with the hot path.",
            "Latency-tuned Kafka starts with higher broker CPU, zero producer linger, 1 ms consumer fetch wait, and partitions that scale with producer/consumer concurrency.",
        ],
    },
    "rabbitmq": {
        "id": "rabbitmq",
        "label": "RabbitMQ",
        "defaultProtocol": "amqp-0-9-1",
        "operatorRequired": True,
        "controlPlaneRequired": True,
        "controlPlaneMode": "cluster-operator",
        "protocolOptions": ["amqp-0-9-1"],
        "protocolDetails": {
            "amqp-0-9-1": "The common RabbitMQ client protocol.",
        },
        "deploymentMode": "latest-stable-auto",
        "latestStableVersion": broker_version("rabbitmq"),
        "operatorVersion": f"cluster-operator {rabbitmq_operator_version()}",
        "modeNotes": {
            "baseline": "Starts close to the normal RabbitMQ defaults for a neutral reference point.",
            "optimized": "Starts from queue and client values tuned for lower end-to-end latency.",
        },
        "deploymentNotes": {
            "normal": "Single cluster node.",
            "ha": "Three-node RabbitMQ cluster.",
        },
        "latencyNotes": [
            "RabbitMQ HA mode uses quorum queues, so its durable three-node path is intentionally stricter than a single-node classic queue.",
            "The control plane uses the official upstream RabbitMQ Cluster Operator manifest instead of a third-party wrapper chart.",
        ],
    },
    "nats": {
        "id": "nats",
        "label": "NATS JetStream",
        "defaultProtocol": "nats",
        "operatorRequired": True,
        "controlPlaneRequired": True,
        "controlPlaneMode": "cluster-operator",
        "protocolOptions": ["nats"],
        "protocolDetails": {
            "nats": "NATS native protocol with JetStream persistence.",
        },
        "deploymentMode": "latest-stable-auto",
        "latestStableVersion": broker_version("nats"),
        "operatorVersion": f"nack {nack_controller_version()} + nats/nats {nats_chart_version()}",
        "modeNotes": {
            "baseline": "Starts close to NATS JetStream defaults for a neutral reference point.",
            "optimized": "Starts from values tuned for lower end-to-end latency.",
        },
        "deploymentNotes": {
            "normal": "Single NATS server with JetStream enabled.",
            "ha": "Three-node NATS cluster with JetStream enabled.",
        },
        "latencyNotes": [
            "NATS uses the official nats/nats runtime chart with a cluster-scoped NACK controller watching the benchmark namespaces.",
            "JetStream stays enabled for persistence while the NACK controller reconciles the benchmark stream definition and HA mode replicates stream state across the active NATS replicas, up to 5.",
        ],
    },
    "artemis": {
        "id": "artemis",
        "label": "ActiveMQ Artemis",
        "defaultProtocol": "amqp-1-0",
        "operatorRequired": True,
        "controlPlaneRequired": True,
        "controlPlaneMode": "cluster-operator",
        "protocolOptions": ["amqp-1-0"],
        "protocolDetails": {
            "amqp-1-0": "Standards-based AMQP 1.0 transport.",
        },
        "deploymentMode": "latest-stable-auto",
        "latestStableVersion": broker_version("artemis"),
        "operatorVersion": f"operator {artemis_operator_version()}",
        "modeNotes": {
            "baseline": "Starts close to the normal Artemis defaults for a neutral reference point.",
            "optimized": "Starts from journal and address values tuned for lower end-to-end latency.",
        },
        "deploymentNotes": {
            "normal": "Single Artemis broker pod.",
            "ha": "Three clustered Artemis broker pods. This is clustered multi-broker mode, not a shared-store live/backup failover pair.",
        },
    },
}


def broker_catalog_entries() -> list[dict[str, Any]]:
    return list(BROKER_CATALOG.values())


def broker_definition(broker_id: str) -> dict[str, Any]:
    candidate = str(broker_id or "").strip().lower()
    broker = BROKER_CATALOG.get(candidate)
    if broker is None:
        raise HTTPException(status_code=422, detail="Unsupported broker")
    return broker


def normalize_protocol_for_broker(broker_id: str, protocol: str | None) -> str:
    broker = broker_definition(broker_id)
    if not protocol:
        return str(broker["defaultProtocol"])
    candidate = str(protocol).strip()
    if candidate not in broker["protocolOptions"]:
        raise HTTPException(status_code=422, detail="Unsupported protocol for broker")
    return candidate


def target_replicas_for_mode(deployment_mode: str) -> int:
    return 3 if str(deployment_mode or "").strip().lower() == "ha" else 1


def _boolean_cli_flag(flag_name: str) -> Callable[[Any], str]:
    return lambda value: f"--{flag_name}={str(bool(value)).lower()}"


RABBITMQ_FRAME_MAX_MIN = 4096
RABBITMQ_FRAME_MAX_MAX = 131072


CLIENT_ARG_SPECS: dict[str, dict[str, list[tuple[str, Callable[[Any], str]]]]] = {
    "kafka": {
        "producer": [
            ("lingerMs", lambda value: f"--linger-ms={value}"),
            ("batchSizeBytes", lambda value: f"--batch-size-bytes={value}"),
            ("compressionType", lambda value: f"--compression-type={value}"),
            ("socketNagleDisable", _boolean_cli_flag("producer-socket-nagle-disable")),
            ("acks", lambda value: f"--acks={value}"),
            ("maxInFlightRequests", lambda value: f"--max-in-flight-requests={value}"),
            ("requestTimeoutMs", lambda value: f"--request-timeout-ms={value}"),
            ("bufferMemoryBytes", lambda value: f"--buffer-memory-bytes={value}"),
            ("maxRequestSizeBytes", lambda value: f"--max-request-size-bytes={value}"),
            ("deliveryTimeoutMs", lambda value: f"--delivery-timeout-ms={value}"),
        ],
        "consumer": [
            ("fetchMinBytes", lambda value: f"--fetch-min-bytes={value}"),
            ("fetchMaxWaitMs", lambda value: f"--fetch-max-wait-ms={value}"),
            ("maxPollRecords", lambda value: f"--max-poll-records={value}"),
            ("maxPartitionFetchBytes", lambda value: f"--max-partition-fetch-bytes={value}"),
            ("fetchMaxBytes", lambda value: f"--fetch-max-bytes={value}"),
            ("sessionTimeoutMs", lambda value: f"--session-timeout-ms={value}"),
            ("heartbeatIntervalMs", lambda value: f"--heartbeat-interval-ms={value}"),
            ("enableAutoCommit", _boolean_cli_flag("enable-auto-commit")),
            ("maxPollIntervalMs", lambda value: f"--max-poll-interval-ms={value}"),
            ("socketNagleDisable", _boolean_cli_flag("consumer-socket-nagle-disable")),
        ],
    },
    "rabbitmq": {
        "producer": [
            ("publisherConfirms", _boolean_cli_flag("publisher-confirms")),
            ("mandatory", _boolean_cli_flag("mandatory")),
            ("heartbeatSec", lambda value: f"--heartbeat-sec={value}"),
            ("frameMax", lambda value: f"--frame-max={value}"),
            ("channelMax", lambda value: f"--channel-max={value}"),
            ("connectionTimeoutMs", lambda value: f"--connection-timeout-ms={value}"),
        ],
        "consumer": [
            ("prefetch", lambda value: f"--prefetch={value}"),
            ("prefetchGlobal", _boolean_cli_flag("prefetch-global")),
            ("autoAck", _boolean_cli_flag("auto-ack")),
        ],
    },
    "artemis": {
        "producer": [],
        "consumer": [],
    },
    "nats": {
        "producer": [],
        "consumer": [],
    },
}


def sanitize_broker_tuning(
    broker_id: str,
    broker_tuning: dict[str, Any] | None,
) -> dict[str, Any]:
    tuning = deepcopy(broker_tuning or {})
    broker_key = str(broker_id or "").strip().lower()
    if broker_key != "rabbitmq":
        return tuning

    producer_values = tuning.get("producer", {}) or {}
    if "frameMax" in producer_values and producer_values["frameMax"] not in {None, ""}:
        try:
            frame_max = int(producer_values["frameMax"])
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="RabbitMQ frame max must be an integer") from exc
        if frame_max < RABBITMQ_FRAME_MAX_MIN or frame_max > RABBITMQ_FRAME_MAX_MAX:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"RabbitMQ frame max must stay between "
                    f"{RABBITMQ_FRAME_MAX_MIN} and {RABBITMQ_FRAME_MAX_MAX} bytes"
                ),
            )
        producer_values["frameMax"] = frame_max
        tuning["producer"] = producer_values
    return tuning


def build_supported_client_args(
    broker_id: str,
    broker_tuning: dict[str, Any] | None,
) -> tuple[list[str], list[str]]:
    tuning = sanitize_broker_tuning(broker_id, broker_tuning)
    producer_values = tuning.get("producer", {}) or {}
    consumer_values = tuning.get("consumer", {}) or {}
    specs = CLIENT_ARG_SPECS.get(str(broker_id).strip().lower(), {})
    producer_args: list[str] = []
    consumer_args: list[str] = []

    for key, formatter in specs.get("producer", []):
        value = producer_values.get(key)
        if value not in {None, ""}:
            producer_args.append(formatter(value))
    for key, formatter in specs.get("consumer", []):
        value = consumer_values.get(key)
        if value not in {None, ""}:
            consumer_args.append(formatter(value))

    return producer_args, consumer_args
