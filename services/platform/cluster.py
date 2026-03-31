from __future__ import annotations

import base64
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

import yaml

from services.platform.brokers import build_supported_client_args
from services.platform.versions import (
    artemis_chart_ref,
    artemis_chart_version,
    cnpg_chart_ref,
    cnpg_chart_version,
    minio_operator_chart_ref,
    minio_operator_chart_version,
    minio_tenant_chart_ref,
    minio_tenant_chart_version,
    nack_chart_ref,
    nack_chart_version,
    nats_chart_ref,
    nats_chart_version,
    rabbitmq_operator_manifest_url,
    strimzi_chart_ref,
    strimzi_chart_version,
)


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def run_namespace_for(run_id: str) -> str:
    return f"bench-run-{run_id.split('-')[0].lower()}"


def run_short_id(run_id: str) -> str:
    return run_id.split("-")[0].lower()


def benchmark_job_names_for(run_id: str, *, producers: int, consumers: int) -> tuple[list[str], list[str]]:
    short_id = run_short_id(run_id)
    consumer_jobs = [f"{short_id}-consumer-{index + 1}" for index in range(max(1, consumers))]
    producer_jobs = [f"{short_id}-producer-{index + 1}" for index in range(max(1, producers))]
    return consumer_jobs, producer_jobs


def finalizer_job_name_for(run_id: str) -> str:
    return f"run-finalizer-{run_short_id(run_id)}"


def manifest_path_for(
    repo_root: Path, broker_id: str, config_mode: str, deployment_mode: str
) -> Path:
    suffix = "-ha" if deployment_mode == "ha" else ""
    return repo_root / "deploy" / "k8s" / "brokers" / f"{broker_id}-{config_mode}{suffix}.yaml"


LEGACY_BROKER_NAMESPACES = (
    "bench-kafka",
    "bench-rabbitmq",
    "bench-artemis",
    "bench-nats",
)

CRD_NAMES = (
    "kafkas.kafka.strimzi.io",
    "kafkanodepools.kafka.strimzi.io",
    "rabbitmqclusters.rabbitmq.com",
    "activemqartemises.broker.amq.io",
    "activemqartemisaddresses.broker.amq.io",
    "clusters.postgresql.cnpg.io",
    "streams.jetstream.nats.io",
    "consumers.jetstream.nats.io",
    "accounts.jetstream.nats.io",
    "tenants.minio.min.io",
)

BROKER_CRD_NAMES = (
    "kafkas.kafka.strimzi.io",
    "kafkanodepools.kafka.strimzi.io",
    "rabbitmqclusters.rabbitmq.com",
    "activemqartemises.broker.amq.io",
    "activemqartemisaddresses.broker.amq.io",
    "streams.jetstream.nats.io",
    "consumers.jetstream.nats.io",
    "accounts.jetstream.nats.io",
)

PLATFORM_DATA_CRD_NAMES = (
    "clusters.postgresql.cnpg.io",
    "tenants.minio.min.io",
)

OPERATOR_TARGETS = {
    "kafka": {"label": "Kafka", "namespace": "bench-kafka-operator", "requiresOperator": True},
    "rabbitmq": {"label": "RabbitMQ", "namespace": "rabbitmq-system", "requiresOperator": True},
    "artemis": {"label": "Artemis", "namespace": "bench-artemis-operator", "requiresOperator": True},
    "nats": {"label": "NATS JetStream", "namespace": "bench-nats-operator", "requiresOperator": True},
}
RABBITMQ_LEGACY_OPERATOR_NAMESPACE = "bench-rabbitmq-operator"

PLATFORM_OPERATOR_TARGETS = {
    "cnpg": {"label": "CloudNativePG", "namespace": "bench-cnpg-operator"},
    "minio": {"label": "MinIO Operator", "namespace": "bench-minio-operator"},
}

PLATFORM_SERVICE_TARGETS = {
    "postgres": {"label": "PostgreSQL", "namespace": "bench-platform-data"},
    "objectStore": {"label": "S3 Store", "namespace": "bench-platform-data"},
}

SHARED_BROKER_RESOURCE_TYPES = (
    "jobs.batch",
    "pods",
    "services",
    "configmaps",
    "secrets",
    "persistentvolumeclaims",
    "deployments.apps",
    "statefulsets.apps",
    "kafkas.kafka.strimzi.io",
    "kafkanodepools.kafka.strimzi.io",
    "kafkatopics.kafka.strimzi.io",
    "rabbitmqclusters.rabbitmq.com",
    "activemqartemises.broker.amq.io",
    "activemqartemisaddresses.broker.amq.io",
    "streams.jetstream.nats.io",
    "consumers.jetstream.nats.io",
)

NATS_STREAM_RESOURCE_NAME = "benchmark-events"
NATS_STREAM_NAME = "BENCHMARK-EVENTS"
NATS_STREAM_SUBJECT = "benchmark.events"
NATS_STREAM_CRD = "streams.jetstream.nats.io"
CLUSTER_CAPACITY_HEADROOM = 0.92
_BINARY_SIZE_UNITS = {
    "Ki": 1024.0,
    "Mi": 1024.0**2,
    "Gi": 1024.0**3,
    "Ti": 1024.0**4,
    "Pi": 1024.0**5,
    "Ei": 1024.0**6,
}
_DECIMAL_SIZE_UNITS = {
    "K": 1000.0,
    "M": 1000.0**2,
    "G": 1000.0**3,
    "T": 1000.0**4,
    "P": 1000.0**5,
    "E": 1000.0**6,
}


def _target_state_templates(
    targets: dict[str, dict[str, Any]],
    *,
    message: str,
) -> dict[str, dict[str, Any]]:
    return {
        key: {
            "label": meta["label"],
            "namespace": meta["namespace"],
            "ready": False,
            "message": message,
        }
        for key, meta in targets.items()
    }


def _merge_labels(metadata: dict[str, Any], run_id: str, deployment_mode: str) -> None:
    labels = metadata.setdefault("labels", {})
    labels["benchmark.ninefinger9.io/run-id"] = run_id
    labels["benchmark.ninefinger9.io/owned-by"] = "bus-platform"
    labels["benchmark.ninefinger9.io/runtime-scope"] = "run"
    labels["benchmark.ninefinger9.io/deployment-mode"] = deployment_mode


def _coerce_broker_tuning(broker_tuning: dict[str, Any] | None) -> dict[str, Any]:
    if not broker_tuning:
        return {"broker": {}, "producer": {}, "consumer": {}}
    return {
        "broker": broker_tuning.get("broker", {}) or {},
        "producer": broker_tuning.get("producer", {}) or {},
        "consumer": broker_tuning.get("consumer", {}) or {},
        "setupPreset": broker_tuning.get("setupPreset"),
        "profileId": broker_tuning.get("profileId"),
        "profileMode": broker_tuning.get("profileMode"),
        "tuningPreset": broker_tuning.get("tuningPreset"),
    }


def _positive_int(value: Any, fallback: int) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(1, candidate)


def _derived_peak_message_rate(message_rate: int, rate_profile_kind: str) -> int:
    base_rate = max(1, int(message_rate))
    if str(rate_profile_kind or "constant").strip().lower() == "constant":
        return base_rate
    return max(base_rate, base_rate * 2)


def _nats_jetstream_replicas(value: Any, fallback: int = 1) -> int:
    return max(1, min(5, _positive_int(value, fallback)))


def _measurement_window_ended(
    *,
    scheduled_start_ns: int | None,
    warmup_seconds: int,
    measurement_seconds: int,
) -> bool:
    if not scheduled_start_ns:
        return False
    measure_end_ns = int(scheduled_start_ns) + int(max(0, warmup_seconds) + max(1, measurement_seconds)) * 1_000_000_000
    return time.time_ns() >= measure_end_ns


def _parse_cpu_cores(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if text.endswith("m"):
        try:
            return float(text[:-1]) / 1000.0
        except ValueError:
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_bytes(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([a-zA-Z]+)?", text)
    if not match:
        return 0.0
    amount = float(match.group(1))
    suffix = str(match.group(2) or "")
    if not suffix:
        return amount
    if suffix in _BINARY_SIZE_UNITS:
        return amount * _BINARY_SIZE_UNITS[suffix]
    if suffix in _DECIMAL_SIZE_UNITS:
        return amount * _DECIMAL_SIZE_UNITS[suffix]
    return 0.0


def _format_cpu_cores(value: float) -> str:
    if value <= 0:
        return "0"
    if value < 1:
        return f"{int(round(value * 1000.0))}m"
    rounded = round(value, 2)
    if float(rounded).is_integer():
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def _format_bytes(value: float) -> str:
    if value <= 0:
        return "0Mi"
    for suffix, factor in (("Gi", 1024.0**3), ("Mi", 1024.0**2), ("Ki", 1024.0)):
        if value >= factor:
            rendered = value / factor
            if abs(rendered - round(rendered)) < 0.05:
                return f"{round(rendered):.0f}{suffix}"
            return f"{rendered:.1f}{suffix}"
    return f"{int(round(value))}B"


def _resource_spec(
    *,
    cpu_request: str = "",
    memory_request: str = "",
    cpu_limit: str = "",
    memory_limit: str = "",
) -> dict[str, Any]:
    resources: dict[str, Any] = {}
    if cpu_request or memory_request:
        resources["requests"] = {}
        if cpu_request:
            resources["requests"]["cpu"] = cpu_request
        if memory_request:
            resources["requests"]["memory"] = memory_request
    if cpu_limit or memory_limit:
        resources["limits"] = {}
        if cpu_limit:
            resources["limits"]["cpu"] = cpu_limit
        if memory_limit:
            resources["limits"]["memory"] = memory_limit
    return resources


def _requested_resources_for_document(document: dict[str, Any]) -> tuple[float, float]:
    kind = str(document.get("kind") or "").strip()
    spec = document.get("spec", {}) or {}
    if kind == "KafkaNodePool":
        replicas = _positive_int(spec.get("replicas"), 1)
        requests = (spec.get("resources", {}) or {}).get("requests", {}) or {}
        return (
            replicas * _parse_cpu_cores(requests.get("cpu")),
            replicas * _parse_bytes(requests.get("memory")),
        )
    if kind == "RabbitmqCluster":
        replicas = _positive_int(spec.get("replicas"), 1)
        requests = (spec.get("resources", {}) or {}).get("requests", {}) or {}
        return (
            replicas * _parse_cpu_cores(requests.get("cpu")),
            replicas * _parse_bytes(requests.get("memory")),
        )
    if kind == "ActiveMQArtemis":
        plan = spec.get("deploymentPlan", {}) or {}
        replicas = _positive_int(plan.get("size"), 1)
        requests = (plan.get("resources", {}) or {}).get("requests", {}) or {}
        return (
            replicas * _parse_cpu_cores(requests.get("cpu")),
            replicas * _parse_bytes(requests.get("memory")),
        )
    if kind not in {"Deployment", "StatefulSet", "Job"}:
        return (0.0, 0.0)
    replicas = _positive_int(spec.get("replicas"), 1) if kind in {"Deployment", "StatefulSet"} else 1
    pod_spec = ((spec.get("template", {}) or {}).get("spec", {}) or {})
    containers = pod_spec.get("containers", []) or []
    pod_cpu = 0.0
    pod_memory = 0.0
    for container in containers:
        requests = ((container.get("resources", {}) or {}).get("requests", {}) or {})
        pod_cpu += _parse_cpu_cores(requests.get("cpu"))
        pod_memory += _parse_bytes(requests.get("memory"))
    return (replicas * pod_cpu, replicas * pod_memory)


def _sum_requested_resources(documents: list[dict[str, Any]]) -> dict[str, float]:
    cpu_cores = 0.0
    memory_bytes = 0.0
    for document in documents:
        cpu, memory = _requested_resources_for_document(document)
        cpu_cores += cpu
        memory_bytes += memory
    return {"cpuCores": cpu_cores, "memoryBytes": memory_bytes}


def _kafka_requested_resources_by_role(documents: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    grouped = {
        "controller": {"cpuCores": 0.0, "memoryBytes": 0.0},
        "broker": {"cpuCores": 0.0, "memoryBytes": 0.0},
    }
    for document in documents:
        if str(document.get("kind") or "").strip() != "KafkaNodePool":
            continue
        roles = _kafka_node_pool_roles(document)
        cpu, memory = _requested_resources_for_document(document)
        if "controller" in roles:
            grouped["controller"]["cpuCores"] += cpu
            grouped["controller"]["memoryBytes"] += memory
        if "broker" in roles:
            grouped["broker"]["cpuCores"] += cpu
            grouped["broker"]["memoryBytes"] += memory
    return grouped


def _upsert_artemis_property(properties: list[str], key: str, value: Any) -> None:
    rendered = f"{key}={value}"
    prefix = f"{key}="
    for index, item in enumerate(properties):
        if item.startswith(prefix):
            properties[index] = rendered
            return
    properties.append(rendered)


def _apply_kafka_broker_tuning(document: dict[str, Any], broker_values: dict[str, Any]) -> None:
    spec = document.setdefault("spec", {})
    kafka_spec = spec.setdefault("kafka", {})
    kafka_config = kafka_spec.setdefault("config", {})

    mapping = {
        "defaultPartitions": "num.partitions",
        "defaultReplicationFactor": "default.replication.factor",
        "minInSyncReplicas": "min.insync.replicas",
        "messageMaxBytes": "message.max.bytes",
        "queuedMaxRequests": "queued.max.requests",
        "replicaFetchMaxBytes": "replica.fetch.max.bytes",
        "numNetworkThreads": "num.network.threads",
        "numIoThreads": "num.io.threads",
        "numReplicaFetchers": "num.replica.fetchers",
        "socketSendBufferBytes": "socket.send.buffer.bytes",
        "socketReceiveBufferBytes": "socket.receive.buffer.bytes",
        "logFlushIntervalMessages": "log.flush.interval.messages",
        "logFlushIntervalMs": "log.flush.interval.ms",
        "logSegmentBytes": "log.segment.bytes",
    }
    for source_key, target_key in mapping.items():
        value = broker_values.get(source_key)
        if value not in {None, ""}:
            kafka_config[target_key] = str(value)


def _apply_rabbitmq_broker_tuning(document: dict[str, Any], broker_values: dict[str, Any]) -> None:
    spec = document.setdefault("spec", {})
    rabbitmq = spec.setdefault("rabbitmq", {})
    current_text = rabbitmq.get("additionalConfig", "") or ""
    current_lines = [line for line in current_text.splitlines() if line.strip()]
    current_map: dict[str, str] = {}
    for line in current_lines:
        if "=" in line:
            key, value = line.split("=", 1)
            current_map[key.strip()] = value.strip()

    mapping = {
        "defaultQueueType": "default_queue_type",
        "vmMemoryHighWatermark": "vm_memory_high_watermark.relative",
        "diskFreeLimitRelative": "disk_free_limit.relative",
        "consumerTimeoutMs": "consumer_timeout",
        "clusterPartitionHandling": "cluster_partition_handling",
        "collectStatisticsIntervalMs": "collect_statistics_interval",
        "handshakeTimeoutMs": "handshake_timeout",
        "tcpBacklog": "tcp_listen_options.backlog",
    }
    for source_key, target_key in mapping.items():
        value = broker_values.get(source_key)
        if value not in {None, ""}:
            current_map[target_key] = str(value)

    if current_map:
        rabbitmq["additionalConfig"] = "\n".join(f"{key} = {value}" for key, value in current_map.items())


def _apply_artemis_broker_tuning(document: dict[str, Any], broker_values: dict[str, Any]) -> None:
    spec = document.setdefault("spec", {})
    properties = list(spec.get("brokerProperties", []) or [])

    mapping = {
        "journalType": ("journal-type", str),
        "globalMaxSizeMb": ("global-max-size", lambda value: f"{value}m"),
        "journalBufferTimeoutNs": ("journal-buffer-timeout", str),
        "journalBufferSizeBytes": ("journal-buffer-size", str),
        "journalFileSizeBytes": ("journal-file-size", str),
        "maxDeliveryAttempts": ("address-setting.#.max-delivery-attempts", str),
        "redeliveryDelayMs": ("address-setting.#.redelivery-delay", str),
        "addressFullPolicy": ("address-setting.#.address-full-policy", str),
        "pageSizeBytes": ("address-setting.#.page-size-bytes", str),
        "threadPoolMaxSize": ("thread-pool-max-size", str),
    }
    for source_key, (target_key, formatter) in mapping.items():
        value = broker_values.get(source_key)
        if value not in {None, ""}:
            _upsert_artemis_property(properties, target_key, formatter(value))

    if properties:
        spec["brokerProperties"] = properties


def _apply_nats_broker_tuning(document: dict[str, Any], broker_values: dict[str, Any]) -> None:
    spec = document.setdefault("spec", {})
    template = (((spec.setdefault("template", {})).setdefault("spec", {})).setdefault("containers", []))
    if not template:
        return
    container = template[0]
    args = [str(item) for item in (container.get("args", []) or [])]

    def upsert_flag(flag_name: str, value: Any) -> None:
        prefix = f"--{flag_name}="
        rendered = f"{prefix}{value}"
        for index, item in enumerate(args):
            if item.startswith(prefix):
                args[index] = rendered
                break
        else:
            args.append(rendered)

    if broker_values.get("maxPayload") not in {None, ""}:
        upsert_flag("max_payload", broker_values["maxPayload"])
    if broker_values.get("maxMemoryStore") not in {None, ""}:
        memory_store = int(broker_values["maxMemoryStore"])
        upsert_flag("max_mem_store", f"{memory_store}MB")

    if args:
        container["args"] = args


def _rewrite_nats_labels(document: dict[str, Any], broker_name: str) -> None:
    metadata = document.setdefault("metadata", {})
    labels = metadata.setdefault("labels", {})
    labels["app"] = broker_name


def _apply_nats_runtime_identity(
    document: dict[str, Any],
    *,
    broker_name: str,
    replicas: int,
) -> None:
    kind = str(document.get("kind") or "")
    metadata = document.setdefault("metadata", {})
    metadata["name"] = broker_name
    _rewrite_nats_labels(document, broker_name)

    spec = document.setdefault("spec", {})
    if kind == "Service":
        selector = spec.setdefault("selector", {})
        selector["app"] = broker_name
        return

    if kind == "Deployment":
        selector = spec.setdefault("selector", {}).setdefault("matchLabels", {})
        selector["app"] = broker_name
        template_labels = spec.setdefault("template", {}).setdefault("metadata", {}).setdefault("labels", {})
        template_labels["app"] = broker_name
        return

    if kind != "StatefulSet":
        return

    spec["serviceName"] = broker_name
    selector = spec.setdefault("selector", {}).setdefault("matchLabels", {})
    selector["app"] = broker_name
    template_labels = spec.setdefault("template", {}).setdefault("metadata", {}).setdefault("labels", {})
    template_labels["app"] = broker_name

    containers = (((spec.setdefault("template", {})).setdefault("spec", {})).setdefault("containers", []))
    if not containers:
        return
    args = [str(item) for item in (containers[0].get("args", []) or [])]
    route_prefix = "--routes="
    rendered_routes = ",".join(
        f"nats://{broker_name}-{index}.{broker_name}:6222"
        for index in range(max(1, replicas))
    )
    updated = False
    for index, item in enumerate(args):
        if item.startswith(route_prefix):
            args[index] = f"{route_prefix}{rendered_routes}"
            updated = True
            break
    if not updated and replicas > 1:
        args.append(f"{route_prefix}{rendered_routes}")
    containers[0]["args"] = args


def _run_config_document(
    *,
    run_id: str,
    broker_id: str,
    config_mode: str,
    deployment_mode: str,
    namespace: str,
    tuning: dict[str, Any],
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{broker_id}-{run_id.split('-')[0].lower()}-run-config",
            "namespace": namespace,
            "labels": {
                "benchmark.ninefinger9.io/run-id": run_id,
                "benchmark.ninefinger9.io/owned-by": "bus-platform",
                "benchmark.ninefinger9.io/runtime-scope": "run",
            },
        },
        "data": {
            "run-id": run_id,
            "broker-id": broker_id,
            "config-mode": config_mode,
            "deployment-mode": deployment_mode,
            "broker-tuning.json": json.dumps(tuning, indent=2, sort_keys=True),
        },
    }


def _build_nats_helm_values(
    *,
    run_id: str,
    broker_name: str,
    deployment_mode: str,
    broker_tuning: dict[str, Any] | None,
    resource_config: dict[str, Any] | None,
) -> dict[str, Any]:
    tuning = _coerce_broker_tuning(broker_tuning)
    broker_values = tuning["broker"]
    run_labels = {
        "benchmark.ninefinger9.io/run-id": run_id,
        "benchmark.ninefinger9.io/owned-by": "bus-platform",
        "benchmark.ninefinger9.io/runtime-scope": "run",
        "benchmark.ninefinger9.io/deployment-mode": deployment_mode,
        "benchmark.ninefinger9.io/broker-id": "nats",
    }
    replicas = _positive_int(
        (resource_config or {}).get("replicas"),
        3 if deployment_mode == "ha" else 1,
    )
    storage_size = str((resource_config or {}).get("storageSize") or "20Gi").strip() or "20Gi"
    storage_class_name = str((resource_config or {}).get("storageClassName") or "").strip()
    values: dict[str, Any] = {
        "fullnameOverride": broker_name,
        "global": {
            "labels": run_labels,
        },
        "config": {
            "cluster": {
                "enabled": replicas > 1,
                "replicas": replicas,
            },
            "jetstream": {
                "enabled": True,
                "fileStore": {
                    "enabled": True,
                    "pvc": {
                        "enabled": True,
                        "size": storage_size,
                    },
                },
            },
        },
        "natsBox": {
            "enabled": False,
        },
        "promExporter": {
            "enabled": False,
        },
    }

    if storage_class_name:
        values["config"]["jetstream"]["fileStore"]["pvc"]["storageClassName"] = storage_class_name

    max_payload = broker_values.get("maxPayload")
    if max_payload not in {None, ""}:
        values["config"]["merge"] = {
            "max_payload": int(max_payload),
        }

    max_memory_store = broker_values.get("maxMemoryStore")
    if max_memory_store not in {None, ""}:
        values["config"]["jetstream"]["memoryStore"] = {
            "enabled": True,
            "maxSize": f"{int(max_memory_store)}Mi",
        }

    resources: dict[str, Any] = {}
    cpu_request = str((resource_config or {}).get("cpuRequest") or "").strip()
    cpu_limit = str((resource_config or {}).get("cpuLimit") or "").strip()
    memory_request = str((resource_config or {}).get("memoryRequest") or "").strip()
    memory_limit = str((resource_config or {}).get("memoryLimit") or "").strip()
    if cpu_request or memory_request:
        resources["requests"] = {}
        if cpu_request:
            resources["requests"]["cpu"] = cpu_request
        if memory_request:
            resources["requests"]["memory"] = memory_request
    if cpu_limit or memory_limit:
        resources["limits"] = {}
        if cpu_limit:
            resources["limits"]["cpu"] = cpu_limit
        if memory_limit:
            resources["limits"]["memory"] = memory_limit
    if resources:
        values["container"] = {
            "resources": resources,
        }

    return values


def _build_nack_operator_values() -> dict[str, Any]:
    return {
        "namespaced": False,
        "readOnly": False,
        "jetstream": {
            "enabled": True,
            "additionalArgs": ["--control-loop"],
        },
        "resources": {
            "requests": {
                "cpu": "150m",
                "memory": "192Mi",
            },
            "limits": {
                "cpu": "500m",
                "memory": "384Mi",
            },
        },
    }


def _basic_auth_secret_document(
    *,
    namespace: str,
    name: str,
    username: str,
    password: str,
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": "kubernetes.io/basic-auth",
        "stringData": {
            "username": username,
            "password": password,
        },
    }


def _opaque_secret_document(
    *,
    namespace: str,
    name: str,
    string_data: dict[str, str],
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": "Opaque",
        "stringData": dict(string_data),
    }


def _cnpg_cluster_document(
    *,
    namespace: str,
    cluster_name: str,
    database: str,
    username: str,
    app_secret_name: str,
    superuser_secret_name: str,
    instances: int,
    storage_size: str,
    storage_class_name: str,
) -> dict[str, Any]:
    storage: dict[str, Any] = {
        "size": storage_size,
    }
    if storage_class_name:
        storage["storageClass"] = storage_class_name
    return {
        "apiVersion": "postgresql.cnpg.io/v1",
        "kind": "Cluster",
        "metadata": {
            "name": cluster_name,
            "namespace": namespace,
            "labels": {
                "benchmark.ninefinger9.io/owned-by": "bus-platform",
                "benchmark.ninefinger9.io/runtime-scope": "platform",
            },
        },
        "spec": {
            "instances": max(1, int(instances)),
            "enableSuperuserAccess": True,
            "superuserSecret": {
                "name": superuser_secret_name,
            },
            "bootstrap": {
                "initdb": {
                    "database": database,
                    "owner": username,
                    "secret": {
                        "name": app_secret_name,
                    },
                }
            },
            "storage": storage,
        },
    }


def _build_minio_tenant_values(
    *,
    tenant_name: str,
    root_secret_name: str,
    app_user_secret_name: str,
    bucket_name: str,
    storage_size: str,
    storage_class_name: str,
) -> dict[str, Any]:
    pool: dict[str, Any] = {
        "servers": 1,
        "name": "pool-0",
        "volumesPerServer": 1,
        "size": storage_size,
        "resources": {
            "requests": {
                "cpu": "250m",
                "memory": "512Mi",
            },
            "limits": {
                "cpu": "1",
                "memory": "2Gi",
            },
        },
    }
    if storage_class_name:
        pool["storageClassName"] = storage_class_name
    return {
        "tenant": {
            "name": tenant_name,
            "configSecret": {
                "name": root_secret_name,
            },
            "pools": [pool],
            "certificate": {
                "requestAutoCert": False,
            },
            "features": {
                "bucketDNS": False,
            },
            "buckets": [
                {
                    "name": bucket_name,
                    "objectLock": False,
                }
            ],
            "users": [
                {
                    "name": app_user_secret_name,
                }
            ],
        }
    }


def _nats_stream_document(
    *,
    run_id: str,
    namespace: str,
    broker_name: str,
    deployment_mode: str,
    replicas: int,
) -> dict[str, Any]:
    return {
        "apiVersion": "jetstream.nats.io/v1beta2",
        "kind": "Stream",
        "metadata": {
            "name": NATS_STREAM_RESOURCE_NAME,
            "namespace": namespace,
            "labels": {
                "benchmark.ninefinger9.io/run-id": run_id,
                "benchmark.ninefinger9.io/owned-by": "bus-platform",
                "benchmark.ninefinger9.io/runtime-scope": "run",
                "benchmark.ninefinger9.io/deployment-mode": deployment_mode,
                "benchmark.ninefinger9.io/broker-id": "nats",
            },
        },
        "spec": {
            "name": NATS_STREAM_NAME,
            "subjects": [NATS_STREAM_SUBJECT],
            "storage": "file",
            "replicas": max(1, int(replicas)),
            "servers": [f"nats://{broker_name}.{namespace}.svc.cluster.local:4222"],
        },
    }


def _nats_stream_is_ready(payload: dict[str, Any]) -> bool:
    status = payload.get("status", {}) or {}
    state = str(status.get("state") or status.get("phase") or "").strip().lower()
    if state == "ready":
        return True
    for condition in status.get("conditions", []) or []:
        if (
            str(condition.get("type") or "").strip().lower() == "ready"
            and str(condition.get("status") or "").strip().lower() == "true"
        ):
            return True
    return False


def _kafka_node_pool_roles(document: dict[str, Any]) -> set[str]:
    spec = document.get("spec", {}) or {}
    return {
        str(role).strip().lower()
        for role in (spec.get("roles", []) or [])
        if str(role).strip()
    }


def _kafka_node_pool_name(document: dict[str, Any], broker_name: str) -> str:
    roles = _kafka_node_pool_roles(document)
    if roles == {"controller"}:
        return f"{broker_name}-controllers"
    if roles == {"broker"}:
        return f"{broker_name}-brokers"
    return f"{broker_name}-pool"


def _kafka_node_pool_is_broker(document: dict[str, Any]) -> bool:
    roles = _kafka_node_pool_roles(document)
    return "broker" in roles and "controller" not in roles


def _apply_resource_config(document: dict[str, Any], broker_id: str, resource_config: dict[str, Any]) -> None:
    """Overlay CPU/memory/storage/replica overrides onto a manifest document."""
    if not resource_config:
        return

    cpu_req = resource_config.get("cpuRequest")
    cpu_lim = resource_config.get("cpuLimit")
    mem_req = resource_config.get("memoryRequest")
    mem_lim = resource_config.get("memoryLimit")
    storage = resource_config.get("storageSize")
    storage_class_name = resource_config.get("storageClassName")
    replicas = resource_config.get("replicas")

    kind = document.get("kind", "")

    if kind == "KafkaNodePool":
        if not _kafka_node_pool_is_broker(document):
            return
        spec = document.setdefault("spec", {})
        if replicas is not None:
            spec["replicas"] = int(replicas)
        res = spec.setdefault("resources", {})
        if cpu_req or mem_req:
            reqs = res.setdefault("requests", {})
            if cpu_req:
                reqs["cpu"] = str(cpu_req)
            if mem_req:
                reqs["memory"] = str(mem_req)
        if cpu_lim or mem_lim:
            lims = res.setdefault("limits", {})
            if cpu_lim:
                lims["cpu"] = str(cpu_lim)
            if mem_lim:
                lims["memory"] = str(mem_lim)
        if storage:
            storage_spec = spec.setdefault("storage", {})
            storage_spec["size"] = str(storage)
            if storage_class_name:
                storage_spec["class"] = str(storage_class_name)
        elif storage_class_name:
            spec.setdefault("storage", {})["class"] = str(storage_class_name)

    elif kind == "RabbitmqCluster":
        spec = document.setdefault("spec", {})
        if replicas is not None:
            spec["replicas"] = int(replicas)
        res = spec.setdefault("resources", {})
        if cpu_req or mem_req:
            reqs = res.setdefault("requests", {})
            if cpu_req:
                reqs["cpu"] = str(cpu_req)
            if mem_req:
                reqs["memory"] = str(mem_req)
        if cpu_lim or mem_lim:
            lims = res.setdefault("limits", {})
            if cpu_lim:
                lims["cpu"] = str(cpu_lim)
            if mem_lim:
                lims["memory"] = str(mem_lim)
        if storage:
            persistence = spec.setdefault("persistence", {})
            persistence["storage"] = str(storage)
            if storage_class_name:
                persistence["storageClassName"] = str(storage_class_name)
        elif storage_class_name:
            spec.setdefault("persistence", {})["storageClassName"] = str(storage_class_name)

    elif kind == "ActiveMQArtemis":
        spec = document.setdefault("spec", {})
        plan = spec.setdefault("deploymentPlan", {})
        if replicas is not None:
            plan["size"] = int(replicas)
        res = plan.setdefault("resources", {})
        if cpu_req or mem_req:
            reqs = res.setdefault("requests", {})
            if cpu_req:
                reqs["cpu"] = str(cpu_req)
            if mem_req:
                reqs["memory"] = str(mem_req)
        if cpu_lim or mem_lim:
            lims = res.setdefault("limits", {})
            if cpu_lim:
                lims["cpu"] = str(cpu_lim)
            if mem_lim:
                lims["memory"] = str(mem_lim)
        if storage:
            storage_spec = plan.setdefault("storage", {})
            storage_spec["size"] = str(storage)
            if storage_class_name:
                storage_spec["storageClassName"] = str(storage_class_name)
        elif storage_class_name:
            plan.setdefault("storage", {})["storageClassName"] = str(storage_class_name)

    elif kind in {"Deployment", "StatefulSet"} and broker_id == "nats":
        spec = document.setdefault("spec", {})
        if replicas is not None:
            spec["replicas"] = int(replicas)
        template_spec = (((spec.setdefault("template", {})).setdefault("spec", {})).setdefault("containers", []))
        if template_spec:
            container = template_spec[0]
            res = container.setdefault("resources", {})
            if cpu_req or mem_req:
                reqs = res.setdefault("requests", {})
                if cpu_req:
                    reqs["cpu"] = str(cpu_req)
                if mem_req:
                    reqs["memory"] = str(mem_req)
            if cpu_lim or mem_lim:
                lims = res.setdefault("limits", {})
                if cpu_lim:
                    lims["cpu"] = str(cpu_lim)
                if mem_lim:
                    lims["memory"] = str(mem_lim)
        if storage and kind == "StatefulSet":
            volume_claims = spec.setdefault("volumeClaimTemplates", [])
            if volume_claims:
                claim_spec = volume_claims[0].setdefault("spec", {})
                if storage_class_name:
                    claim_spec["storageClassName"] = str(storage_class_name)
                resources = claim_spec.setdefault("resources", {}).setdefault("requests", {})
                resources["storage"] = str(storage)
        elif storage_class_name and kind == "StatefulSet":
            volume_claims = spec.setdefault("volumeClaimTemplates", [])
            if volume_claims:
                claim_spec = volume_claims[0].setdefault("spec", {})
                claim_spec["storageClassName"] = str(storage_class_name)


def patch_broker_manifest_documents(
    *,
    repo_root: Path,
    run_id: str,
    broker_id: str,
    config_mode: str,
    deployment_mode: str,
    broker_tuning: dict[str, Any] | None,
    producers: int = 1,
    consumers: int = 1,
    resource_config: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str, str]:
    manifest_path = manifest_path_for(repo_root, broker_id, config_mode, deployment_mode)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Broker manifest not found: {manifest_path}")

    namespace = run_namespace_for(run_id)
    broker_name = f"{broker_id}-{run_id.split('-')[0].lower()}"
    tuning = _coerce_broker_tuning(broker_tuning)
    nats_replicas = _positive_int(
        (resource_config or {}).get("replicas"),
        3 if deployment_mode == "ha" else 1,
    )

    with manifest_path.open("r", encoding="utf-8") as handle:
        raw_documents = [
            document for document in yaml.safe_load_all(handle) if isinstance(document, dict)
        ]

    patched_documents: list[dict[str, Any]] = []
    for document in raw_documents:
        kind = document.get("kind")
        metadata = document.setdefault("metadata", {})
        metadata["namespace"] = namespace
        _merge_labels(metadata, run_id, deployment_mode)

        if kind == "KafkaNodePool":
            metadata["name"] = _kafka_node_pool_name(document, broker_name)
            metadata.setdefault("labels", {})["strimzi.io/cluster"] = broker_name
        elif kind == "Kafka":
            metadata["name"] = broker_name
            _apply_kafka_broker_tuning(document, tuning["broker"])
        elif kind == "RabbitmqCluster":
            metadata["name"] = broker_name
            _apply_rabbitmq_broker_tuning(document, tuning["broker"])
        elif kind == "ActiveMQArtemis":
            metadata["name"] = broker_name
            _apply_artemis_broker_tuning(document, tuning["broker"])
        elif kind == "ActiveMQArtemisAddress":
            metadata["name"] = f"{broker_name}-benchmark-events"
            spec = document.setdefault("spec", {})
            spec["applyToCrNames"] = [broker_name]
        elif broker_id == "nats" and kind in {"Service", "Deployment", "StatefulSet"}:
            _apply_nats_runtime_identity(
                document,
                broker_name=broker_name,
                replicas=nats_replicas,
            )
            if kind in {"Deployment", "StatefulSet"}:
                _apply_nats_broker_tuning(document, tuning["broker"])

        if resource_config:
            _apply_resource_config(document, broker_id, resource_config)

        patched_documents.append(document)

    patched_documents.append(
        _run_config_document(
            run_id=run_id,
            broker_id=broker_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            namespace=namespace,
            tuning=tuning,
        )
    )

    if broker_id == "kafka":
        broker_values = tuning["broker"]
        configured_partitions = _positive_int(broker_values.get("defaultPartitions"), 3)
        # Ensure partitions >= max(producers, consumers) for optimal parallelism
        configured_partitions = max(
            configured_partitions,
            _positive_int(producers, 1),
            _positive_int(consumers, 1),
        )
        broker_count = _positive_int(
            (resource_config or {}).get("replicas"),
            3 if deployment_mode == "ha" else 1,
        )
        configured_replicas = _positive_int(
            broker_values.get("defaultReplicationFactor"),
            broker_count,
        )
        topic_replicas = max(1, min(broker_count, configured_replicas))
        configured_min_isr = _positive_int(
            broker_values.get("minInSyncReplicas"),
            2 if topic_replicas >= 3 else 1,
        )
        topic_min_isr = max(1, min(topic_replicas, configured_min_isr))
        patched_documents.append(
            {
                "apiVersion": "kafka.strimzi.io/v1",
                "kind": "KafkaTopic",
                "metadata": {
                    "name": "benchmark-events",
                    "namespace": namespace,
                    "labels": {
                        "strimzi.io/cluster": broker_name,
                        "benchmark.ninefinger9.io/run-id": run_id,
                        "benchmark.ninefinger9.io/owned-by": "bus-platform",
                        "benchmark.ninefinger9.io/runtime-scope": "run",
                    },
                },
                "spec": {
                    "topicName": "benchmark.events",
                    "partitions": configured_partitions,
                    "replicas": topic_replicas,
                    "config": {
                        "cleanup.policy": "delete",
                        "min.insync.replicas": topic_min_isr,
                    },
                },
            }
        )

    return patched_documents, namespace, broker_name


class ClusterAutomation:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.enabled = env_flag("BUS_ENABLE_CLUSTER_ACTIONS", False)
        self.operator_bootstrap_enabled = env_flag("BUS_ENABLE_OPERATOR_BOOTSTRAP", True)
        self.agent_image = os.environ.get(
            "BUS_BENCHMARK_AGENT_IMAGE", "ninefinger9/bus-benchmark-agent:latest"
        ).strip()
        self.platform_image = os.environ.get(
            "BUS_PLATFORM_IMAGE", "ninefinger9/bus-platform:latest"
        ).strip()
        self.platform_namespace = os.environ.get(
            "BUS_PLATFORM_NAMESPACE", "bench-platform"
        ).strip() or "bench-platform"
        self.platform_data_namespace = os.environ.get(
            "BUS_PLATFORM_DATA_NAMESPACE", "bench-platform-data"
        ).strip() or "bench-platform-data"
        self.platform_data_pvc = os.environ.get(
            "BUS_PLATFORM_DATA_PVC", "bus-platform-data"
        ).strip() or "bus-platform-data"
        self.shared_artifact_dir = Path(
            os.environ.get("BUS_ARTIFACT_DIR", "/data/artifacts")
        )
        self.agent_pull_secret = os.environ.get("BUS_AGENT_IMAGE_PULL_SECRET", "").strip()
        self.agent_pull_policy = os.environ.get(
            "BUS_AGENT_IMAGE_PULL_POLICY", "IfNotPresent"
        ).strip() or "IfNotPresent"
        self.agent_start_barrier_seconds = max(
            8,
            int(os.environ.get("BUS_AGENT_START_BARRIER_SECONDS", "20")),
        )
        self.resource_poll_seconds = max(
            1.0,
            float(os.environ.get("BUS_RESOURCE_POLL_SECONDS", "2")),
        )
        self.job_timeout_seconds = int(
            os.environ.get("BUS_BENCHMARK_JOB_TIMEOUT_SECONDS", "1800")
        )
        self.platform_resource_requests = _resource_spec(
            cpu_request=str(os.environ.get("BUS_PLATFORM_CPU_REQUEST") or "750m").strip(),
            memory_request=str(os.environ.get("BUS_PLATFORM_MEMORY_REQUEST") or "2Gi").strip(),
            cpu_limit=str(os.environ.get("BUS_PLATFORM_CPU_LIMIT") or "750m").strip(),
            memory_limit=str(os.environ.get("BUS_PLATFORM_MEMORY_LIMIT") or "2Gi").strip(),
        )
        self.producer_job_resources = _resource_spec(
            cpu_request=str(os.environ.get("BUS_PRODUCER_JOB_CPU_REQUEST") or "250m").strip(),
            memory_request=str(os.environ.get("BUS_PRODUCER_JOB_MEMORY_REQUEST") or "256Mi").strip(),
            cpu_limit=str(os.environ.get("BUS_PRODUCER_JOB_CPU_LIMIT") or "1").strip(),
            memory_limit=str(os.environ.get("BUS_PRODUCER_JOB_MEMORY_LIMIT") or "1Gi").strip(),
        )
        self.consumer_job_resources = _resource_spec(
            cpu_request=str(os.environ.get("BUS_CONSUMER_JOB_CPU_REQUEST") or "100m").strip(),
            memory_request=str(os.environ.get("BUS_CONSUMER_JOB_MEMORY_REQUEST") or "256Mi").strip(),
            cpu_limit=str(os.environ.get("BUS_CONSUMER_JOB_CPU_LIMIT") or "500m").strip(),
            memory_limit=str(os.environ.get("BUS_CONSUMER_JOB_MEMORY_LIMIT") or "1280Mi").strip(),
        )
        self.finalizer_job_resources = _resource_spec(
            cpu_request=str(os.environ.get("BUS_FINALIZER_JOB_CPU_REQUEST") or "500m").strip(),
            memory_request=str(os.environ.get("BUS_FINALIZER_JOB_MEMORY_REQUEST") or "1Gi").strip(),
            cpu_limit=str(os.environ.get("BUS_FINALIZER_JOB_CPU_LIMIT") or "2").strip(),
            memory_limit=str(os.environ.get("BUS_FINALIZER_JOB_MEMORY_LIMIT") or "6Gi").strip(),
        )
        self.strimzi_chart_ref = os.environ.get(
            "BUS_STRIMZI_OPERATOR_CHART",
            strimzi_chart_ref(),
        ).strip()
        self.strimzi_version = os.environ.get(
            "BUS_STRIMZI_OPERATOR_VERSION", strimzi_chart_version()
        ).strip()
        self.rabbitmq_operator_manifest_url = os.environ.get(
            "BUS_RABBITMQ_OPERATOR_MANIFEST_URL",
            rabbitmq_operator_manifest_url(),
        ).strip()
        self.artemis_chart_ref = os.environ.get(
            "BUS_ARTEMIS_OPERATOR_CHART",
            artemis_chart_ref(),
        ).strip()
        self.artemis_chart_version = os.environ.get(
            "BUS_ARTEMIS_OPERATOR_CHART_VERSION",
            artemis_chart_version(),
        ).strip()
        self.nats_chart_ref = os.environ.get(
            "BUS_NATS_RUNTIME_CHART",
            nats_chart_ref(),
        ).strip()
        self.nats_chart_version = os.environ.get(
            "BUS_NATS_RUNTIME_CHART_VERSION",
            nats_chart_version(),
        ).strip()
        self.nats_runtime_chart_timeout = (
            os.environ.get("BUS_NATS_RUNTIME_CHART_TIMEOUT", "20m").strip() or "20m"
        )
        self.nack_chart_ref = os.environ.get(
            "BUS_NATS_CONTROLLER_CHART",
            nack_chart_ref(),
        ).strip()
        self.nack_chart_version = os.environ.get(
            "BUS_NATS_CONTROLLER_CHART_VERSION",
            nack_chart_version(),
        ).strip()
        self.cnpg_operator_namespace = os.environ.get(
            "BUS_CNPG_OPERATOR_NAMESPACE",
            PLATFORM_OPERATOR_TARGETS["cnpg"]["namespace"],
        ).strip() or PLATFORM_OPERATOR_TARGETS["cnpg"]["namespace"]
        self.cnpg_chart_ref = os.environ.get(
            "BUS_CNPG_OPERATOR_CHART",
            cnpg_chart_ref(),
        ).strip()
        self.cnpg_chart_version = os.environ.get(
            "BUS_CNPG_OPERATOR_CHART_VERSION",
            cnpg_chart_version(),
        ).strip()
        self.postgres_cluster_name = os.environ.get(
            "BUS_POSTGRES_CLUSTER_NAME",
            "bus-platform-db",
        ).strip() or "bus-platform-db"
        self.postgres_database = os.environ.get(
            "BUS_POSTGRES_DATABASE",
            "bus_platform",
        ).strip() or "bus_platform"
        self.postgres_username = os.environ.get(
            "BUS_POSTGRES_USERNAME",
            "bus_platform",
        ).strip() or "bus_platform"
        self.postgres_password = os.environ.get(
            "BUS_POSTGRES_PASSWORD",
            "bus-platform-password",
        ).strip() or "bus-platform-password"
        self.postgres_superuser_password = os.environ.get(
            "BUS_POSTGRES_SUPERUSER_PASSWORD",
            "postgres-platform-password",
        ).strip() or "postgres-platform-password"
        self.postgres_instances = max(
            1,
            int(os.environ.get("BUS_POSTGRES_INSTANCES", "1")),
        )
        self.postgres_storage_size = os.environ.get(
            "BUS_POSTGRES_STORAGE_SIZE",
            "8Gi",
        ).strip() or "8Gi"
        self.postgres_storage_class_name = os.environ.get(
            "BUS_POSTGRES_STORAGE_CLASS_NAME",
            "",
        ).strip()
        self.postgres_app_secret_name = f"{self.postgres_cluster_name}-app"
        self.postgres_superuser_secret_name = f"{self.postgres_cluster_name}-superuser"
        self.minio_operator_namespace = os.environ.get(
            "BUS_MINIO_OPERATOR_NAMESPACE",
            PLATFORM_OPERATOR_TARGETS["minio"]["namespace"],
        ).strip() or PLATFORM_OPERATOR_TARGETS["minio"]["namespace"]
        self.minio_operator_chart_ref = os.environ.get(
            "BUS_MINIO_OPERATOR_CHART",
            minio_operator_chart_ref(),
        ).strip()
        self.minio_operator_chart_version = os.environ.get(
            "BUS_MINIO_OPERATOR_CHART_VERSION",
            minio_operator_chart_version(),
        ).strip()
        self.minio_tenant_chart_ref = os.environ.get(
            "BUS_MINIO_TENANT_CHART",
            minio_tenant_chart_ref(),
        ).strip()
        self.minio_tenant_chart_version = os.environ.get(
            "BUS_MINIO_TENANT_CHART_VERSION",
            minio_tenant_chart_version(),
        ).strip()
        self.minio_tenant_name = os.environ.get(
            "BUS_MINIO_TENANT_NAME",
            "bus-platform-store",
        ).strip() or "bus-platform-store"
        self.minio_bucket = os.environ.get(
            "BUS_MINIO_BUCKET",
            "bus-platform-artifacts",
        ).strip() or "bus-platform-artifacts"
        self.minio_root_user = os.environ.get(
            "BUS_MINIO_ROOT_USER",
            "busplatform",
        ).strip() or "busplatform"
        self.minio_root_password = os.environ.get(
            "BUS_MINIO_ROOT_PASSWORD",
            "busplatform-minio-password",
        ).strip() or "busplatform-minio-password"
        self.minio_app_access_key = os.environ.get(
            "BUS_MINIO_APP_ACCESS_KEY",
            "bus-platform-app",
        ).strip() or "bus-platform-app"
        self.minio_app_secret_key = os.environ.get(
            "BUS_MINIO_APP_SECRET_KEY",
            "bus-platform-app-secret",
        ).strip() or "bus-platform-app-secret"
        self.minio_storage_size = os.environ.get(
            "BUS_MINIO_STORAGE_SIZE",
            "20Gi",
        ).strip() or "20Gi"
        self.minio_storage_class_name = os.environ.get(
            "BUS_MINIO_STORAGE_CLASS_NAME",
            "",
        ).strip()
        self.minio_root_secret_name = f"{self.minio_tenant_name}-root"
        self.minio_app_user_secret_name = f"{self.minio_tenant_name}-app-user"
        self._lock = threading.Lock()
        self._bootstrap_status: dict[str, Any] = {
            "enabled": self.operator_bootstrap_enabled and self.enabled,
            "attempted": False,
            "ready": False,
            "message": "disabled",
            "operators": _target_state_templates(OPERATOR_TARGETS, message="not checked"),
            "platformOperators": _target_state_templates(
                PLATFORM_OPERATOR_TARGETS,
                message="not checked",
            ),
            "platformServices": _target_state_templates(
                {
                    key: {
                        **meta,
                        "namespace": self.platform_data_namespace,
                    }
                    for key, meta in PLATFORM_SERVICE_TARGETS.items()
                },
                message="not checked",
            ),
        }
        self._last_probe_at = 0.0

    def status(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            if now - self._last_probe_at >= 5.0:
                self._bootstrap_status.update(self._probe_bootstrap_state())
                self._last_probe_at = now
            return dict(self._bootstrap_status)

    def tools_available(self) -> bool:
        return shutil.which("kubectl") is not None

    def bootstrap_tools_available(self) -> bool:
        return self.tools_available() and shutil.which("helm") is not None

    def _run(
        self,
        arguments: list[str],
        *,
        check: bool = True,
        stdin_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            arguments,
            cwd=self.repo_root,
            check=check,
            input=stdin_text,
            text=True,
            capture_output=True,
        )

    def _apply_cluster_bootstrap_overlay(self) -> None:
        overlay_path = self.repo_root / "deploy" / "kustomize" / "cluster" / "portable"
        self._run(["kubectl", "apply", "-k", str(overlay_path)])

    def _helm_operator_values_path(self, file_name: str) -> Path:
        return self.repo_root / "deploy" / "k8s" / "operators" / "helm-values" / file_name

    def _helm_release_status(self, namespace: str, release_name: str) -> str:
        result = self._run(["helm", "-n", namespace, "status", release_name], check=False)
        if result.returncode != 0:
            return ""
        match = re.search(r"^STATUS:\s+(.+)$", result.stdout or "", re.MULTILINE)
        if not match:
            return ""
        return str(match.group(1)).strip().lower()

    def _crd_exists(self, crd_name: str) -> bool:
        result = self._run(
            ["kubectl", "get", "crd", crd_name, "-o", "name"],
            check=False,
        )
        return result.returncode == 0 and bool((result.stdout or "").strip())

    def _wait_for_namespace_absent(self, namespace: str, timeout_seconds: int = 240) -> None:
        deadline = time.time() + timeout_seconds
        forced = False
        while time.time() < deadline:
            result = self._run(
                ["kubectl", "get", "namespace", namespace, "-o", "name"],
                check=False,
            )
            if result.returncode != 0:
                return
            if not forced and time.time() >= deadline - 30:
                self._force_delete_namespace(namespace)
                forced = True
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for namespace deletion: {namespace}")

    def _reset_namespace_for_helm_release(self, namespace: str, release_name: str) -> None:
        release_status = self._helm_release_status(namespace, release_name)
        if release_status == "deployed":
            return
        if release_status:
            self._run(
                ["helm", "-n", namespace, "uninstall", release_name, "--wait=false"],
                check=False,
            )
        namespace_result = self._run(
            ["kubectl", "get", "namespace", namespace, "-o", "name"],
            check=False,
        )
        if namespace_result.returncode == 0:
            self._run(
                ["kubectl", "delete", "namespace", namespace, "--ignore-not-found=true", "--wait=false"],
                check=False,
            )
            self._wait_for_namespace_absent(namespace)
        self._apply_cluster_bootstrap_overlay()

    def _wait_for_deployments(self, namespace: str, timeout: str = "10m") -> None:
        result = self._run(["kubectl", "-n", namespace, "get", "deploy", "-o", "json"])
        payload = json.loads(result.stdout or "{}")
        items = payload.get("items", [])
        if not items:
            raise RuntimeError(f"No deployments found in {namespace}")
        for item in items:
            name = item.get("metadata", {}).get("name")
            if not name:
                continue
            self._run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "rollout",
                    "status",
                    f"deployment/{name}",
                    f"--timeout={timeout}",
                ]
            )

    def _wait_for_crds(self, crd_names: tuple[str, ...] = CRD_NAMES) -> None:
        for crd_name in crd_names:
            self._run(
                [
                    "kubectl",
                    "wait",
                    "--for=condition=Established",
                    f"crd/{crd_name}",
                    "--timeout=180s",
                ]
            )

    def _wait_for_ready_pods(
        self, namespace: str, minimum_ready_pods: int, timeout_seconds: int = 900
    ) -> int:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            result = self._run(
                ["kubectl", "-n", namespace, "get", "pods", "-o", "json"],
                check=False,
            )
            if result.returncode == 0:
                payload = json.loads(result.stdout or "{}")
                ready_count = 0
                for item in payload.get("items", []):
                    conditions = item.get("status", {}).get("conditions", []) or []
                    if any(
                        condition.get("type") == "Ready"
                        and condition.get("status") == "True"
                        for condition in conditions
                    ):
                        ready_count += 1
                if ready_count >= minimum_ready_pods:
                    return ready_count
            time.sleep(3)
        raise RuntimeError(
            f"Timed out waiting for {minimum_ready_pods} ready pod(s) in {namespace}"
        )

    def _run_json(self, arguments: list[str], *, check: bool = False) -> dict[str, Any]:
        result = self._run(arguments, check=check)
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _schedulable_cluster_allocatable(self) -> dict[str, float]:
        payload = self._run_json(["kubectl", "get", "nodes", "-o", "json"], check=False)
        node_count = 0
        cpu_cores = 0.0
        memory_bytes = 0.0
        for item in payload.get("items", []) or []:
            spec = item.get("spec", {}) or {}
            if spec.get("unschedulable"):
                continue
            allocatable = (item.get("status", {}) or {}).get("allocatable", {}) or {}
            cpu_cores += _parse_cpu_cores(allocatable.get("cpu"))
            memory_bytes += _parse_bytes(allocatable.get("memory"))
            node_count += 1
        return {
            "nodeCount": node_count,
            "cpuCores": cpu_cores,
            "memoryBytes": memory_bytes,
        }

    def _rendered_runtime_documents(
        self,
        *,
        run_id: str,
        broker_id: str,
        config_mode: str,
        deployment_mode: str,
        broker_tuning: dict[str, Any] | None,
        producers: int,
        consumers: int,
        resource_config: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        documents, _, _ = patch_broker_manifest_documents(
            repo_root=self.repo_root,
            run_id=run_id,
            broker_id=broker_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            broker_tuning=broker_tuning,
            producers=producers,
            consumers=consumers,
            resource_config=resource_config,
        )
        return documents

    def _benchmark_worker_requested_resources(
        self,
        *,
        producers: int,
        consumers: int,
    ) -> dict[str, float]:
        producer_document = {
            "kind": "Job",
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"resources": self.producer_job_resources}],
                    }
                }
            },
        }
        consumer_document = {
            "kind": "Job",
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"resources": self.consumer_job_resources}],
                    }
                }
            },
        }
        producer_request = _sum_requested_resources([producer_document])
        consumer_request = _sum_requested_resources([consumer_document])
        return {
            "cpuCores": (producer_request["cpuCores"] * max(1, producers))
            + (consumer_request["cpuCores"] * max(1, consumers)),
            "memoryBytes": (producer_request["memoryBytes"] * max(1, producers))
            + (consumer_request["memoryBytes"] * max(1, consumers)),
        }

    def _platform_requested_resources(self) -> dict[str, float]:
        document = {
            "kind": "Deployment",
            "spec": {
                "replicas": 1,
                "template": {
                    "spec": {
                        "containers": [{"resources": self.platform_resource_requests}],
                    }
                },
            },
        }
        return _sum_requested_resources([document])

    def _capacity_shortfall_detail(
        self,
        *,
        run_id: str,
        broker_id: str,
        config_mode: str,
        deployment_mode: str,
        broker_tuning: dict[str, Any] | None,
        producers: int,
        consumers: int,
        resource_config: dict[str, Any] | None,
    ) -> str | None:
        capacity = self._schedulable_cluster_allocatable()
        if capacity["nodeCount"] <= 0:
            return None
        documents = self._rendered_runtime_documents(
            run_id=run_id,
            broker_id=broker_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            broker_tuning=broker_tuning,
            producers=producers,
            consumers=consumers,
            resource_config=resource_config,
        )
        requested = _sum_requested_resources(documents)
        benchmark_workers = self._benchmark_worker_requested_resources(
            producers=producers,
            consumers=consumers,
        )
        platform_request = self._platform_requested_resources()
        requested["cpuCores"] += benchmark_workers["cpuCores"] + platform_request["cpuCores"]
        requested["memoryBytes"] += benchmark_workers["memoryBytes"] + platform_request["memoryBytes"]
        allowed_cpu = capacity["cpuCores"] * CLUSTER_CAPACITY_HEADROOM
        allowed_memory = capacity["memoryBytes"] * CLUSTER_CAPACITY_HEADROOM
        shortfalls: list[str] = []
        if requested["cpuCores"] > allowed_cpu:
            shortfalls.append(
                f"CPU request {_format_cpu_cores(requested['cpuCores'])} exceeds schedulable allocatable {_format_cpu_cores(capacity['cpuCores'])}"
            )
        if requested["memoryBytes"] > allowed_memory:
            shortfalls.append(
                f"memory request {_format_bytes(requested['memoryBytes'])} exceeds schedulable allocatable {_format_bytes(capacity['memoryBytes'])}"
            )
        if not shortfalls:
            return None
        detail_suffix = ""
        if broker_id == "kafka":
            grouped = _kafka_requested_resources_by_role(documents)
            broker_memory = grouped["broker"]["memoryBytes"]
            controller_memory = grouped["controller"]["memoryBytes"]
            broker_cpu = grouped["broker"]["cpuCores"]
            controller_cpu = grouped["controller"]["cpuCores"]
            detail_suffix = (
                f" Broker pool from the UI/request is {_format_cpu_cores(broker_cpu)} CPU and {_format_bytes(broker_memory)} memory; "
                f"fixed controller pool adds {_format_cpu_cores(controller_cpu)} CPU and {_format_bytes(controller_memory)} memory; "
                f"benchmark workers add {_format_cpu_cores(benchmark_workers['cpuCores'])} CPU and {_format_bytes(benchmark_workers['memoryBytes'])} memory; "
                f"platform runtime reserves {_format_cpu_cores(platform_request['cpuCores'])} CPU and {_format_bytes(platform_request['memoryBytes'])} memory."
            )
        else:
            detail_suffix = (
                f" Benchmark workers add {_format_cpu_cores(benchmark_workers['cpuCores'])} CPU and {_format_bytes(benchmark_workers['memoryBytes'])} memory; "
                f"platform runtime reserves {_format_cpu_cores(platform_request['cpuCores'])} CPU and {_format_bytes(platform_request['memoryBytes'])} memory."
            )
        return (
            f"Cluster capacity check failed for {broker_id} {deployment_mode} run: "
            + "; ".join(shortfalls)
            + "."
            + detail_suffix
            + " Reduce broker resources or choose a smaller setup."
        )

    def _wait_for_secret_exists(
        self,
        namespace: str,
        secret_name: str,
        timeout_seconds: int = 300,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._secret_exists(namespace, secret_name):
                return
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for secret {secret_name} in {namespace}")

    def _service_has_ready_endpoints(self, namespace: str, service_name: str) -> bool:
        payload = self._run_json(
            ["kubectl", "-n", namespace, "get", "endpoints", service_name, "-o", "json"],
            check=False,
        )
        subsets = payload.get("subsets", []) or []
        for subset in subsets:
            addresses = subset.get("addresses", []) or []
            ports = subset.get("ports", []) or []
            if addresses and ports:
                return True
        return False

    def _wait_for_service_endpoints(
        self,
        namespace: str,
        service_name: str,
        timeout_seconds: int = 300,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._service_has_ready_endpoints(namespace, service_name):
                return
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for ready endpoints on service {service_name} in {namespace}"
        )

    def _wait_for_resource_exists(
        self,
        *,
        namespace: str,
        resource_type: str,
        resource_name: str,
        timeout_seconds: int = 300,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            result = self._run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    f"{resource_type}/{resource_name}",
                    "-o",
                    "name",
                ],
                check=False,
            )
            if result.returncode == 0 and str(result.stdout or "").strip():
                return
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for {resource_type}/{resource_name} in {namespace}"
        )

    def _resource_condition_is_true(
        self,
        *,
        namespace: str,
        resource_type: str,
        resource_name: str,
        condition_type: str,
    ) -> bool:
        payload = self._run_json(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                f"{resource_type}/{resource_name}",
                "-o",
                "json",
            ],
            check=False,
        )
        conditions = ((payload.get("status") or {}).get("conditions") or [])
        for condition in conditions:
            if (
                str(condition.get("type") or "").strip().lower() == condition_type.lower()
                and str(condition.get("status") or "").strip().lower() == "true"
            ):
                return True
        return False

    def _wait_for_resource_condition(
        self,
        *,
        namespace: str,
        resource_type: str,
        resource_name: str,
        condition_type: str,
        timeout_seconds: int = 300,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._resource_condition_is_true(
                namespace=namespace,
                resource_type=resource_type,
                resource_name=resource_name,
                condition_type=condition_type,
            ):
                return
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for {resource_type}/{resource_name} condition {condition_type} in {namespace}"
        )

    def _wait_for_kafka_runtime(
        self,
        namespace: str,
        broker_name: str,
    ) -> None:
        self._wait_for_service_endpoints(namespace, f"{broker_name}-kafka-bootstrap")
        self._wait_for_resource_exists(
            namespace=namespace,
            resource_type="kafkatopic",
            resource_name="benchmark-events",
        )
        self._wait_for_resource_condition(
            namespace=namespace,
            resource_type="kafkatopic",
            resource_name="benchmark-events",
            condition_type="Ready",
        )

    def _discover_rabbitmq_amqp_services(
        self,
        namespace: str,
        broker_name: str,
    ) -> list[str]:
        payload = self._run_json(
            ["kubectl", "-n", namespace, "get", "svc", "-o", "json"],
            check=False,
        )
        service_names: list[str] = []
        prefix = f"{broker_name}"
        for item in payload.get("items", []) or []:
            metadata = item.get("metadata", {}) or {}
            name = str(metadata.get("name") or "").strip()
            if not name or not name.startswith(prefix):
                continue
            ports = ((item.get("spec", {}) or {}).get("ports", []) or [])
            if not any(int(port.get("port", 0) or 0) == 5672 for port in ports):
                continue
            service_names.append(name)
        return sorted(set(service_names))

    def _wait_for_rabbitmq_runtime(
        self,
        namespace: str,
        broker_name: str,
        timeout_seconds: int = 300,
    ) -> str:
        self._wait_for_secret_exists(namespace, f"{broker_name}-default-user")
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            service_names = self._discover_rabbitmq_amqp_services(namespace, broker_name)
            for service_name in service_names:
                if self._service_has_ready_endpoints(namespace, service_name):
                    return service_name
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for RabbitMQ AMQP service endpoints for {broker_name} in {namespace}"
        )

    def _discover_artemis_acceptor_services(
        self,
        namespace: str,
        broker_name: str,
    ) -> list[str]:
        payload = self._run_json(
            ["kubectl", "-n", namespace, "get", "svc", "-o", "json"],
            check=False,
        )
        service_names: list[str] = []
        pattern = re.compile(rf"^{re.escape(broker_name)}-amqp-(\d+)-svc$")
        for item in payload.get("items", []) or []:
            metadata = item.get("metadata", {}) or {}
            name = str(metadata.get("name") or "").strip()
            if not pattern.match(name):
                continue
            ports = ((item.get("spec", {}) or {}).get("ports", []) or [])
            if not any(int(port.get("port", 0) or 0) == 5672 for port in ports):
                continue
            service_names.append(name)
        return sorted(service_names)

    def _wait_for_artemis_runtime(
        self,
        namespace: str,
        broker_name: str,
        expected_broker_pods: int,
    ) -> list[str]:
        self._wait_for_secret_exists(namespace, f"{broker_name}-credentials-secret")
        self._wait_for_resource_exists(
            namespace=namespace,
            resource_type="activemqartemisaddress",
            resource_name=f"{broker_name}-benchmark-events",
        )
        deadline = time.time() + 300
        while time.time() < deadline:
            service_names = self._discover_artemis_acceptor_services(namespace, broker_name)
            if len(service_names) >= expected_broker_pods:
                all_ready = True
                for service_name in service_names[:expected_broker_pods]:
                    if not self._service_has_ready_endpoints(namespace, service_name):
                        all_ready = False
                        break
                if all_ready:
                    return service_names[:expected_broker_pods]
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for Artemis AMQP acceptor services for {broker_name} in {namespace}"
        )

    def _wait_for_nats_runtime(
        self,
        namespace: str,
        broker_name: str,
        timeout_seconds: int = 300,
    ) -> str:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._service_has_ready_endpoints(namespace, broker_name):
                return broker_name
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for NATS service endpoints for {broker_name} in {namespace}"
        )

    def _namespace_pod_inventory(self, namespace: str) -> dict[str, dict[str, Any]]:
        payload = self._run_json(
            ["kubectl", "-n", namespace, "get", "pods", "-o", "json"],
            check=False,
        )
        inventory: dict[str, dict[str, Any]] = {}
        for item in payload.get("items", []) or []:
            metadata = item.get("metadata", {}) or {}
            spec = item.get("spec", {}) or {}
            name = str(metadata.get("name") or "").strip()
            if not name:
                continue
            labels = metadata.get("labels", {}) or {}
            role = str(labels.get("benchmark.ninefinger9.io/role") or "").strip().lower()
            if role not in {"producer", "consumer"}:
                role = "broker"
            inventory[name] = {
                "name": name,
                "role": role,
                "nodeName": str(spec.get("nodeName") or "").strip(),
            }
        return inventory

    def _node_summary_pods(self, node_name: str) -> dict[str, dict[str, Any]]:
        payload = self._run_json(
            ["kubectl", "get", "--raw", f"/api/v1/nodes/{node_name}/proxy/stats/summary"],
            check=False,
        )
        pods: dict[str, dict[str, Any]] = {}
        for item in payload.get("pods", []) or []:
            pod_ref = item.get("podRef", {}) or {}
            namespace = str(pod_ref.get("namespace") or "").strip()
            name = str(pod_ref.get("name") or "").strip()
            if namespace and name:
                pods[f"{namespace}/{name}"] = item
        return pods

    def _sample_namespace_resources(
        self,
        *,
        namespace: str,
        start_ns: int,
        stop_event: threading.Event,
        sink: list[dict[str, Any]],
    ) -> None:
        previous_totals: dict[str, tuple[float, float, float]] = {}
        while not stop_event.is_set():
            inventory = self._namespace_pod_inventory(namespace)
            if inventory:
                node_summaries = {
                    node_name: self._node_summary_pods(node_name)
                    for node_name in sorted(
                        {item["nodeName"] for item in inventory.values() if item.get("nodeName")}
                    )
                }
                grouped: dict[str, dict[str, float]] = {
                    "broker": {
                        "cpuCores": 0.0,
                        "memoryMB": 0.0,
                        "networkRxBytes": 0.0,
                        "networkTxBytes": 0.0,
                        "storageUsedMB": 0.0,
                    },
                    "producer": {
                        "cpuCores": 0.0,
                        "memoryMB": 0.0,
                        "networkRxBytes": 0.0,
                        "networkTxBytes": 0.0,
                        "storageUsedMB": 0.0,
                    },
                    "consumer": {
                        "cpuCores": 0.0,
                        "memoryMB": 0.0,
                        "networkRxBytes": 0.0,
                        "networkTxBytes": 0.0,
                        "storageUsedMB": 0.0,
                    },
                }
                for pod_name, item in inventory.items():
                    node_name = item.get("nodeName")
                    if not node_name:
                        continue
                    pod_summary = node_summaries.get(node_name, {}).get(f"{namespace}/{pod_name}")
                    if not isinstance(pod_summary, dict):
                        continue
                    role = item["role"]
                    for container in pod_summary.get("containers", []) or []:
                        cpu = container.get("cpu", {}) or {}
                        memory = container.get("memory", {}) or {}
                        grouped[role]["cpuCores"] += float(cpu.get("usageNanoCores", 0) or 0) / 1_000_000_000.0
                        grouped[role]["memoryMB"] += float(
                            memory.get("workingSetBytes", memory.get("usageBytes", 0)) or 0
                        ) / (1024.0 * 1024.0)
                    network = pod_summary.get("network", {}) or {}
                    grouped[role]["networkRxBytes"] += float(network.get("rxBytes", 0) or 0)
                    grouped[role]["networkTxBytes"] += float(network.get("txBytes", 0) or 0)
                    storage_used_bytes = 0.0
                    for volume in pod_summary.get("volume", []) or []:
                        fs = volume.get("fs", {}) or {}
                        storage_used_bytes += float(fs.get("usedBytes", 0) or 0)
                    grouped[role]["storageUsedMB"] += storage_used_bytes / (1024.0 * 1024.0)

                now_ns = time.time_ns()
                sample = {
                    "second": max(0, int((now_ns - start_ns) / 1_000_000_000)),
                }
                for role in ("broker", "producer", "consumer"):
                    totals = grouped[role]
                    previous = previous_totals.get(role)
                    rx_rate = 0.0
                    tx_rate = 0.0
                    if previous is not None and self.resource_poll_seconds > 0:
                        rx_rate = max(0.0, (totals["networkRxBytes"] - previous[0]) / (1024.0 * 1024.0 * self.resource_poll_seconds))
                        tx_rate = max(0.0, (totals["networkTxBytes"] - previous[1]) / (1024.0 * 1024.0 * self.resource_poll_seconds))
                    previous_totals[role] = (
                        totals["networkRxBytes"],
                        totals["networkTxBytes"],
                        totals["storageUsedMB"],
                    )
                    sample[f"{role}CpuCores"] = round(totals["cpuCores"], 4)
                    sample[f"{role}MemoryMB"] = round(totals["memoryMB"], 2)
                    sample[f"{role}NetworkRxMBps"] = round(rx_rate, 4)
                    sample[f"{role}NetworkTxMBps"] = round(tx_rate, 4)
                    sample[f"{role}StorageUsedMB"] = round(totals["storageUsedMB"], 2)
                sample["cpuCores"] = sample.get("brokerCpuCores", 0.0)
                sample["memoryMB"] = sample.get("brokerMemoryMB", 0.0)
                sink.append(sample)
            stop_event.wait(self.resource_poll_seconds)

    def _discover_minio_api_services(
        self,
        namespace: str,
        tenant_name: str,
    ) -> list[str]:
        payload = self._run_json(
            ["kubectl", "-n", namespace, "get", "svc", "-o", "json"],
            check=False,
        )
        service_names: list[str] = []
        for item in payload.get("items", []) or []:
            metadata = item.get("metadata", {}) or {}
            name = str(metadata.get("name") or "").strip()
            if not name or not name.startswith(tenant_name):
                continue
            ports = ((item.get("spec", {}) or {}).get("ports", []) or [])
            if not any(int(port.get("port", 0) or 0) == 9000 for port in ports):
                continue
            service_names.append(name)
        return sorted(set(service_names))

    def _probe_deployment_targets(
        self,
        targets: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        states: dict[str, Any] = {}
        overall_ready = True
        if not self.enabled:
            for key, meta in targets.items():
                ready = False
                states[key] = {
                    "label": meta["label"],
                    "namespace": meta["namespace"],
                    "ready": ready,
                    "message": "disabled",
                }
                overall_ready = overall_ready and ready
            return {"states": states, "ready": False}
        if not self.tools_available():
            for key, meta in targets.items():
                ready = False
                states[key] = {
                    "label": meta["label"],
                    "namespace": meta["namespace"],
                    "ready": ready,
                    "message": "kubectl unavailable",
                }
                overall_ready = overall_ready and ready
            return {"states": states, "ready": False}

        for key, meta in targets.items():
            result = self._run(
                ["kubectl", "-n", meta["namespace"], "get", "deploy", "-o", "json"],
                check=False,
            )
            if result.returncode != 0:
                states[key] = {
                    "label": meta["label"],
                    "namespace": meta["namespace"],
                    "ready": False,
                    "message": "not installed",
                }
                overall_ready = False
                continue

            payload = json.loads(result.stdout or "{}")
            items = payload.get("items", [])
            if not items:
                states[key] = {
                    "label": meta["label"],
                    "namespace": meta["namespace"],
                    "ready": False,
                    "message": "no deployments",
                }
                overall_ready = False
                continue

            ready_count = 0
            for item in items:
                spec = item.get("spec", {}) or {}
                status = item.get("status", {}) or {}
                desired = int(spec.get("replicas", 1) or 1)
                available = int(status.get("availableReplicas", 0) or 0)
                if available >= desired:
                    ready_count += 1

            ready = ready_count == len(items)
            states[key] = {
                "label": meta["label"],
                "namespace": meta["namespace"],
                "ready": ready,
                "message": "ready" if ready else f"{ready_count}/{len(items)} deployments ready",
            }
            overall_ready = overall_ready and ready

        return {"states": states, "ready": overall_ready}

    def _probe_operator_states(self) -> dict[str, Any]:
        probe = self._probe_deployment_targets(OPERATOR_TARGETS)
        return {
            "operators": probe["states"],
            "ready": probe["ready"],
            "message": "operators ready" if probe["ready"] else "operator deployment not ready",
        }

    def _probe_platform_operator_states(self) -> dict[str, Any]:
        targets = {
            "cnpg": {
                **PLATFORM_OPERATOR_TARGETS["cnpg"],
                "namespace": self.cnpg_operator_namespace,
            },
            "minio": {
                **PLATFORM_OPERATOR_TARGETS["minio"],
                "namespace": self.minio_operator_namespace,
            },
        }
        probe = self._probe_deployment_targets(targets)
        return {
            "platformOperators": probe["states"],
            "ready": probe["ready"],
            "message": "platform operators ready" if probe["ready"] else "platform operator deployment not ready",
        }

    def _probe_platform_service_states(self) -> dict[str, Any]:
        states: dict[str, Any] = {
            key: {
                "label": meta["label"],
                "namespace": self.platform_data_namespace,
                "ready": False,
                "message": "not installed",
            }
            for key, meta in PLATFORM_SERVICE_TARGETS.items()
        }
        overall_ready = True
        if not self.enabled:
            for state in states.values():
                state["message"] = "disabled"
            return {
                "platformServices": states,
                "ready": False,
                "message": "platform services disabled",
            }
        if not self.tools_available():
            for state in states.values():
                state["message"] = "kubectl unavailable"
            return {
                "platformServices": states,
                "ready": False,
                "message": "kubectl unavailable",
            }

        postgres_payload = self._run_json(
            [
                "kubectl",
                "-n",
                self.platform_data_namespace,
                "get",
                f"clusters.postgresql.cnpg.io/{self.postgres_cluster_name}",
                "-o",
                "json",
            ],
            check=False,
        )
        postgres_state = states["postgres"]
        if postgres_payload:
            service_name = f"{self.postgres_cluster_name}-rw"
            ready = self._resource_condition_is_true(
                namespace=self.platform_data_namespace,
                resource_type="clusters.postgresql.cnpg.io",
                resource_name=self.postgres_cluster_name,
                condition_type="Ready",
            ) and self._service_has_ready_endpoints(self.platform_data_namespace, service_name)
            postgres_state["ready"] = ready
            postgres_state["service"] = service_name
            postgres_state["database"] = self.postgres_database
            postgres_state["username"] = self.postgres_username
            postgres_state["message"] = (
                f"ready via {service_name}"
                if ready
                else f"waiting for {service_name} endpoints"
            )
        else:
            overall_ready = False

        object_store_state = states["objectStore"]
        tenant_payload = self._run_json(
            [
                "kubectl",
                "-n",
                self.platform_data_namespace,
                "get",
                f"tenants.minio.min.io/{self.minio_tenant_name}",
                "-o",
                "json",
            ],
            check=False,
        )
        if tenant_payload:
            service_names = self._discover_minio_api_services(
                self.platform_data_namespace,
                self.minio_tenant_name,
            )
            ready_service = next(
                (
                    service_name
                    for service_name in service_names
                    if self._service_has_ready_endpoints(self.platform_data_namespace, service_name)
                ),
                "",
            )
            ready = bool(ready_service)
            object_store_state["ready"] = ready
            object_store_state["bucket"] = self.minio_bucket
            if ready_service:
                object_store_state["service"] = ready_service
                object_store_state["message"] = f"ready via {ready_service}"
            elif service_names:
                object_store_state["service"] = service_names[0]
                object_store_state["message"] = f"waiting for {service_names[0]} endpoints"
            else:
                object_store_state["message"] = "tenant installed, waiting for S3 service"
        else:
            overall_ready = False

        overall_ready = overall_ready and all(state["ready"] for state in states.values())
        return {
            "platformServices": states,
            "ready": overall_ready,
            "message": "platform services ready" if overall_ready else "platform services not ready",
        }

    def _probe_bootstrap_state(self) -> dict[str, Any]:
        operator_probe = self._probe_operator_states()
        platform_operator_probe = self._probe_platform_operator_states()
        service_probe = self._probe_platform_service_states()
        ready = bool(operator_probe["ready"] and platform_operator_probe["ready"] and service_probe["ready"])
        return {
            "operators": operator_probe["operators"],
            "platformOperators": platform_operator_probe["platformOperators"],
            "platformServices": service_probe["platformServices"],
            "ready": ready,
            "message": "control planes ready" if ready else "bootstrap dependencies not ready",
        }

    def _ensure_rabbitmq_operator(self) -> None:
        namespace = OPERATOR_TARGETS["rabbitmq"]["namespace"]
        self._run(
            [
                "kubectl",
                "delete",
                "namespace",
                RABBITMQ_LEGACY_OPERATOR_NAMESPACE,
                "--ignore-not-found=true",
                "--wait=false",
            ],
            check=False,
        )
        self._run(["kubectl", "apply", "-f", self.rabbitmq_operator_manifest_url])
        self._run(
            [
                "kubectl",
                "label",
                "namespace",
                namespace,
                "benchmark.ninefinger9.io/owned-by=bus-platform",
                "benchmark.ninefinger9.io/runtime-scope=operator",
                "--overwrite",
            ],
            check=False,
        )
        self._wait_for_deployments(namespace)

    def _ensure_artemis_operator(self) -> None:
        namespace = "bench-artemis-operator"
        release_name = "arkmq-broker-operator"
        values_path = self._helm_operator_values_path("artemis.yaml")
        self._reset_namespace_for_helm_release(namespace, release_name)
        arguments = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            self.artemis_chart_ref,
            "--version",
            self.artemis_chart_version,
            "--namespace",
            namespace,
            "--create-namespace",
            "--take-ownership",
            "--wait",
            "--timeout",
            "10m",
            "-f",
            str(values_path),
        ]
        if self._crd_exists("activemqartemises.broker.amq.io"):
            arguments.extend(["--set", "crds.apply=false"])
        self._run(arguments)
        self._wait_for_deployments(namespace)

    def _ensure_nats_operator(self) -> None:
        namespace = "bench-nats-operator"
        release_name = "nats-jetstream-controller"
        self._reset_namespace_for_helm_release(namespace, release_name)
        self._ensure_nats_chart_repo()
        with tempfile.TemporaryDirectory(prefix="bus-nack-operator-values-") as temp_dir:
            values_path = Path(temp_dir) / "nack-values.yaml"
            values_path.write_text(
                yaml.safe_dump(_build_nack_operator_values(), sort_keys=False),
                encoding="utf-8",
            )
            arguments = [
                "helm",
                "upgrade",
                "--install",
                release_name,
                self.nack_chart_ref,
                "--version",
                self.nack_chart_version,
                "--namespace",
                namespace,
                "--create-namespace",
                "--take-ownership",
                "--wait",
                "--timeout",
                "10m",
                "-f",
                str(values_path),
            ]
            if self._crd_exists(NATS_STREAM_CRD):
                arguments.append("--skip-crds")
            self._run(arguments)
        self._wait_for_deployments(namespace)

    def _ensure_strimzi_operator(self) -> None:
        namespace = "bench-kafka-operator"
        values_path = self._helm_operator_values_path("strimzi.yaml")
        self._run(
            ["helm", "repo", "add", "strimzi", "https://strimzi.io/charts/"],
            check=False,
        )
        self._run(["helm", "repo", "update"])
        self._run(
            [
                "helm",
                "upgrade",
                "--install",
                "strimzi-kafka-operator",
                self.strimzi_chart_ref,
                "--version",
                self.strimzi_version,
                "--namespace",
                namespace,
                "--create-namespace",
                "--take-ownership",
                "--wait",
                "--timeout",
                "10m",
                "-f",
                str(values_path),
            ]
        )
        self._wait_for_deployments(namespace)

    def _ensure_nats_chart_repo(self) -> None:
        self._run(
            ["helm", "repo", "add", "nats", "https://nats-io.github.io/k8s/helm/charts/"],
            check=False,
        )
        self._run(["helm", "repo", "update"])

    def _ensure_minio_chart_repo(self) -> None:
        self._run(
            ["helm", "repo", "add", "minio-operator", "https://operator.min.io/"],
            check=False,
        )
        self._run(["helm", "repo", "update"])

    def _ensure_cnpg_operator(self) -> None:
        namespace = self.cnpg_operator_namespace
        release_name = "cloudnative-pg"
        values_path = self._helm_operator_values_path("cnpg.yaml")
        self._reset_namespace_for_helm_release(namespace, release_name)
        arguments = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            self.cnpg_chart_ref,
            "--version",
            self.cnpg_chart_version,
            "--namespace",
            namespace,
            "--create-namespace",
            "--take-ownership",
            "--wait",
            "--timeout",
            "10m",
            "-f",
            str(values_path),
        ]
        if self._crd_exists("clusters.postgresql.cnpg.io"):
            arguments.append("--skip-crds")
        self._run(arguments)
        self._wait_for_deployments(namespace)

    def _ensure_minio_operator(self) -> None:
        namespace = self.minio_operator_namespace
        release_name = "minio-operator"
        values_path = self._helm_operator_values_path("minio-operator.yaml")
        self._ensure_minio_chart_repo()
        self._reset_namespace_for_helm_release(namespace, release_name)
        arguments = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            self.minio_operator_chart_ref,
            "--version",
            self.minio_operator_chart_version,
            "--namespace",
            namespace,
            "--create-namespace",
            "--take-ownership",
            "--wait",
            "--timeout",
            "10m",
            "-f",
            str(values_path),
        ]
        if self._crd_exists("tenants.minio.min.io"):
            arguments.append("--skip-crds")
        self._run(arguments)
        self._wait_for_deployments(namespace)

    def _ensure_platform_data_namespace(self) -> None:
        self._run(["kubectl", "create", "namespace", self.platform_data_namespace], check=False)
        self._run(
            [
                "kubectl",
                "label",
                "namespace",
                self.platform_data_namespace,
                "benchmark.ninefinger9.io/owned-by=bus-platform",
                "benchmark.ninefinger9.io/runtime-scope=platform",
                "--overwrite",
            ],
            check=False,
        )

    def _ensure_platform_database(self) -> None:
        self._ensure_platform_data_namespace()
        documents = [
            _basic_auth_secret_document(
                namespace=self.platform_data_namespace,
                name=self.postgres_app_secret_name,
                username=self.postgres_username,
                password=self.postgres_password,
            ),
            _basic_auth_secret_document(
                namespace=self.platform_data_namespace,
                name=self.postgres_superuser_secret_name,
                username="postgres",
                password=self.postgres_superuser_password,
            ),
            _cnpg_cluster_document(
                namespace=self.platform_data_namespace,
                cluster_name=self.postgres_cluster_name,
                database=self.postgres_database,
                username=self.postgres_username,
                app_secret_name=self.postgres_app_secret_name,
                superuser_secret_name=self.postgres_superuser_secret_name,
                instances=self.postgres_instances,
                storage_size=self.postgres_storage_size,
                storage_class_name=self.postgres_storage_class_name,
            ),
        ]
        self._run(
            ["kubectl", "apply", "-f", "-"],
            stdin_text=yaml.safe_dump_all(documents, sort_keys=False),
        )
        self._wait_for_resource_exists(
            namespace=self.platform_data_namespace,
            resource_type="clusters.postgresql.cnpg.io",
            resource_name=self.postgres_cluster_name,
            timeout_seconds=600,
        )
        self._wait_for_resource_condition(
            namespace=self.platform_data_namespace,
            resource_type="clusters.postgresql.cnpg.io",
            resource_name=self.postgres_cluster_name,
            condition_type="Ready",
            timeout_seconds=900,
        )
        self._wait_for_service_endpoints(
            self.platform_data_namespace,
            f"{self.postgres_cluster_name}-rw",
            timeout_seconds=300,
        )

    def _ensure_platform_object_store(self) -> None:
        self._ensure_platform_data_namespace()
        documents = [
            _opaque_secret_document(
                namespace=self.platform_data_namespace,
                name=self.minio_app_user_secret_name,
                string_data={
                    "CONSOLE_ACCESS_KEY": self.minio_app_access_key,
                    "CONSOLE_SECRET_KEY": self.minio_app_secret_key,
                },
            )
        ]
        self._run(
            ["kubectl", "apply", "-f", "-"],
            stdin_text=yaml.safe_dump_all(documents, sort_keys=False),
        )
        self._ensure_minio_chart_repo()
        with tempfile.TemporaryDirectory(prefix="bus-minio-tenant-values-") as temp_dir:
            values_path = Path(temp_dir) / "minio-tenant-values.yaml"
            values_path.write_text(
                yaml.safe_dump(
                    _build_minio_tenant_values(
                        tenant_name=self.minio_tenant_name,
                        root_secret_name=self.minio_root_secret_name,
                        app_user_secret_name=self.minio_app_user_secret_name,
                        bucket_name=self.minio_bucket,
                        storage_size=self.minio_storage_size,
                        storage_class_name=self.minio_storage_class_name,
                    ),
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            self._run(
                [
                    "helm",
                    "upgrade",
                    "--install",
                    self.minio_tenant_name,
                    self.minio_tenant_chart_ref,
                    "--version",
                    self.minio_tenant_chart_version,
                    "--namespace",
                    self.platform_data_namespace,
                    "--create-namespace",
                    "--wait",
                    "--timeout",
                    "15m",
                    "-f",
                    str(values_path),
                    "--set",
                    f"tenant.configSecret.accessKey={self.minio_root_user}",
                    "--set",
                    f"tenant.configSecret.secretKey={self.minio_root_password}",
                ]
            )
        self._wait_for_resource_exists(
            namespace=self.platform_data_namespace,
            resource_type="tenants.minio.min.io",
            resource_name=self.minio_tenant_name,
            timeout_seconds=600,
        )
        deadline = time.time() + 900
        while time.time() < deadline:
            service_names = self._discover_minio_api_services(
                self.platform_data_namespace,
                self.minio_tenant_name,
            )
            if any(
                self._service_has_ready_endpoints(self.platform_data_namespace, service_name)
                for service_name in service_names
            ):
                return
            time.sleep(3)
        raise RuntimeError(
            f"Timed out waiting for MinIO tenant {self.minio_tenant_name} S3 service endpoints"
        )

    def _deploy_nats_runtime(
        self,
        *,
        namespace: str,
        broker_name: str,
        run_id: str,
        config_mode: str,
        deployment_mode: str,
        broker_tuning: dict[str, Any] | None,
        resource_config: dict[str, Any] | None,
    ) -> None:
        values = _build_nats_helm_values(
            run_id=run_id,
            broker_name=broker_name,
            deployment_mode=deployment_mode,
            broker_tuning=broker_tuning,
            resource_config=resource_config,
        )
        self._ensure_nats_chart_repo()
        with tempfile.TemporaryDirectory(prefix="bus-nats-values-") as temp_dir:
            values_path = Path(temp_dir) / "nats-values.yaml"
            values_path.write_text(
                yaml.safe_dump(values, sort_keys=False),
                encoding="utf-8",
            )
            self._run(
                [
                    "helm",
                    "upgrade",
                    "--install",
                    broker_name,
                    self.nats_chart_ref,
                    "--version",
                    self.nats_chart_version,
                    "--namespace",
                    namespace,
                    "--create-namespace",
                    "--wait",
                    "--timeout",
                    self.nats_runtime_chart_timeout,
                    "-f",
                    str(values_path),
                ]
            )
        config_document = _run_config_document(
            run_id=run_id,
            broker_id="nats",
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            namespace=namespace,
            tuning=_coerce_broker_tuning(broker_tuning),
        )
        self._run(
            ["kubectl", "apply", "-f", "-"],
            stdin_text=yaml.safe_dump(config_document, sort_keys=False),
        )

    def _apply_nats_stream_definition(
        self,
        *,
        run_id: str,
        namespace: str,
        broker_name: str,
        deployment_mode: str,
        replicas: int,
    ) -> None:
        document = _nats_stream_document(
            run_id=run_id,
            namespace=namespace,
            broker_name=broker_name,
            deployment_mode=deployment_mode,
            replicas=replicas,
        )
        self._run(
            ["kubectl", "apply", "-f", "-"],
            stdin_text=yaml.safe_dump(document, sort_keys=False),
        )

    def _wait_for_nats_stream_definition(
        self,
        *,
        namespace: str,
        timeout_seconds: int = 300,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            payload = self._run_json(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    "stream",
                    NATS_STREAM_RESOURCE_NAME,
                    "-o",
                    "json",
                ],
                check=False,
            )
            if payload and _nats_stream_is_ready(payload):
                return
            time.sleep(2)
        raise RuntimeError(
            f"Timed out waiting for NATS stream definition {NATS_STREAM_RESOURCE_NAME} in {namespace}"
        )

    def _set_bootstrap_status_from_probe(
        self,
        probe: dict[str, Any],
        *,
        enabled: bool,
        attempted: bool,
        ready: bool | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        resolved_ready = bool(probe.get("ready")) if ready is None else bool(ready)
        resolved_message = str(
            message
            if message is not None
            else probe.get("message")
            or ("control planes ready" if resolved_ready else "bootstrap dependencies not ready")
        )
        self._bootstrap_status = {
            "enabled": enabled,
            "attempted": attempted,
            "ready": resolved_ready,
            "message": resolved_message,
            "operators": probe.get("operators", {}),
            "platformOperators": probe.get("platformOperators", {}),
            "platformServices": probe.get("platformServices", {}),
        }
        self._last_probe_at = time.monotonic()
        return dict(self._bootstrap_status)

    def _bootstrap_guard_status(self, *, attempted: bool, message: str, enabled: bool) -> dict[str, Any]:
        probe = self._probe_bootstrap_state()
        ready = bool(probe.get("ready")) if not enabled else False
        resolved_message = probe.get("message") if not enabled and ready else message
        return self._set_bootstrap_status_from_probe(
            probe,
            enabled=enabled,
            attempted=attempted,
            ready=ready,
            message=resolved_message,
        )

    def _ensure_bootstrap_scope(self, scope: str) -> dict[str, Any]:
        if not self.enabled:
            return self._bootstrap_guard_status(
                attempted=False,
                message="cluster automation disabled",
                enabled=False,
            )
        if not self.operator_bootstrap_enabled:
            return self._bootstrap_guard_status(
                attempted=False,
                message="bootstrap disabled",
                enabled=False,
            )
        if not self.bootstrap_tools_available():
            return self._bootstrap_guard_status(
                attempted=True,
                message="helm or kubectl not available in runtime",
                enabled=True,
            )

        with self._lock:
            try:
                self._bootstrap_status = {
                    "enabled": True,
                    "attempted": True,
                    "ready": False,
                    "message": f"checking {scope}",
                    "operators": self._bootstrap_status.get("operators", {}),
                    "platformOperators": self._bootstrap_status.get("platformOperators", {}),
                    "platformServices": self._bootstrap_status.get("platformServices", {}),
                }
                self._apply_cluster_bootstrap_overlay()
                probe = self._probe_bootstrap_state()

                if scope in {"brokers", "all"}:
                    if not probe.get("operators", {}).get("kafka", {}).get("ready"):
                        self._ensure_strimzi_operator()
                    if not probe.get("operators", {}).get("rabbitmq", {}).get("ready"):
                        self._ensure_rabbitmq_operator()
                    if not probe.get("operators", {}).get("artemis", {}).get("ready"):
                        self._ensure_artemis_operator()
                    if not probe.get("operators", {}).get("nats", {}).get("ready"):
                        self._ensure_nats_operator()
                    self._wait_for_crds(BROKER_CRD_NAMES)
                    probe = self._probe_bootstrap_state()

                if scope in {"platform-operators", "platform-data", "platform-services", "all"}:
                    if not probe.get("platformOperators", {}).get("cnpg", {}).get("ready"):
                        self._ensure_cnpg_operator()
                    if not probe.get("platformOperators", {}).get("minio", {}).get("ready"):
                        self._ensure_minio_operator()
                    self._wait_for_crds(PLATFORM_DATA_CRD_NAMES)
                    probe = self._probe_bootstrap_state()

                if scope in {"platform-data", "platform-services", "all"}:
                    if not probe.get("platformServices", {}).get("postgres", {}).get("ready"):
                        self._ensure_platform_database()
                    if not probe.get("platformServices", {}).get("objectStore", {}).get("ready"):
                        self._ensure_platform_object_store()
                    probe = self._probe_bootstrap_state()

                return self._set_bootstrap_status_from_probe(
                    probe,
                    enabled=True,
                    attempted=True,
                )
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or "").strip()
                stdout = (exc.stdout or "").strip()
                detail = stderr or stdout or str(exc)
                probe = self._probe_bootstrap_state()
                return self._set_bootstrap_status_from_probe(
                    probe,
                    enabled=True,
                    attempted=True,
                    ready=False,
                    message=detail,
                )
            except Exception as exc:  # pragma: no cover - defensive runtime path
                probe = self._probe_bootstrap_state()
                return self._set_bootstrap_status_from_probe(
                    probe,
                    enabled=True,
                    attempted=True,
                    ready=False,
                    message=str(exc),
                )

    def ensure_broker_operators(self) -> dict[str, Any]:
        return self._ensure_bootstrap_scope("brokers")

    def ensure_platform_data_operators(self) -> dict[str, Any]:
        return self._ensure_bootstrap_scope("platform-operators")

    def ensure_platform_data_services(self) -> dict[str, Any]:
        return self._ensure_bootstrap_scope("platform-services")

    def ensure_platform_data(self) -> dict[str, Any]:
        return self._ensure_bootstrap_scope("platform-data")

    def ensure_operators(self) -> dict[str, Any]:
        return self._ensure_bootstrap_scope("all")

    def prepare_run_environment(
        self,
        *,
        run_id: str,
        broker_id: str,
        config_mode: str,
        deployment_mode: str,
        broker_tuning: dict[str, Any] | None,
        producers: int = 1,
        consumers: int = 1,
        resource_config: dict[str, Any] | None = None,
        emit: Callable[[str, str], None],
    ) -> bool:
        try:
            namespace = run_namespace_for(run_id)
            target_broker_pods = _positive_int(
                (resource_config or {}).get("replicas"),
                3 if deployment_mode == "ha" else 1,
            )
            if broker_id == "kafka":
                controller_replicas = 3 if deployment_mode == "ha" else 1
                expected_runtime_pods = controller_replicas + target_broker_pods
                pod_label = "Kafka controller/broker pod(s)"
            else:
                expected_runtime_pods = target_broker_pods
                pod_label = "broker pod(s)"

            if not self.enabled:
                emit("prepare-error", "Cluster-backed execution is disabled for this runtime.")
                return False

            current_status = self.ensure_broker_operators()
            broker_operator = current_status.get("operators", {}).get(broker_id, {})
            if OPERATOR_TARGETS.get(broker_id, {}).get("requiresOperator", True) and not broker_operator.get("ready"):
                emit(
                    "prepare-error",
                    f"Operator for {broker_id} is not ready: {broker_operator.get('message', 'unknown')}",
                )
                return False

            capacity_shortfall = self._capacity_shortfall_detail(
                run_id=run_id,
                broker_id=broker_id,
                config_mode=config_mode,
                deployment_mode=deployment_mode,
                broker_tuning=broker_tuning,
                producers=producers,
                consumers=consumers,
                resource_config=resource_config,
            )
            if capacity_shortfall:
                emit("prepare-error", capacity_shortfall)
                return False

            namespace = run_namespace_for(run_id)
            broker_name = f"{broker_id}-{run_id.split('-')[0].lower()}"

            emit(
                "prepare-cleanup",
                "Removing orphan benchmark namespaces before provisioning the next run.",
            )
            self.delete_orphan_run_namespaces({run_id})
            if self.namespace_exists(run_id):
                emit(
                    "prepare-cleanup",
                    f"Deleting stale namespace {namespace} before provisioning.",
                )
                self.delete_run_namespace(run_id)
                self._wait_for_namespace_absent(namespace)

            emit("prepare-started", f"Preparing namespace {namespace}.")
            self._run(["kubectl", "create", "namespace", namespace], check=False)
            self._run(
                [
                    "kubectl",
                    "label",
                    "namespace",
                    namespace,
                    "benchmark.ninefinger9.io/owned-by=bus-platform",
                    f"benchmark.ninefinger9.io/run-id={run_id}",
                    f"benchmark.ninefinger9.io/broker={broker_id}",
                    "--overwrite",
                ],
                check=False,
            )
            use_nats_runtime_chart = broker_id == "nats" and shutil.which("helm") is not None
            if use_nats_runtime_chart:
                self._deploy_nats_runtime(
                    namespace=namespace,
                    broker_name=broker_name,
                    run_id=run_id,
                    config_mode=config_mode,
                    deployment_mode=deployment_mode,
                    broker_tuning=broker_tuning,
                    resource_config=resource_config,
                )
                emit(
                    "namespace-ready",
                    f"Namespace {namespace} now contains the NATS runtime chart release {broker_name}.",
                )
            else:
                patched_documents, _, _ = patch_broker_manifest_documents(
                    repo_root=self.repo_root,
                    run_id=run_id,
                    broker_id=broker_id,
                    config_mode=config_mode,
                    deployment_mode=deployment_mode,
                    broker_tuning=broker_tuning,
                    producers=producers,
                    consumers=consumers,
                    resource_config=resource_config,
                )
                rendered = yaml.safe_dump_all(patched_documents, sort_keys=False)
                self._run(["kubectl", "apply", "-f", "-"], stdin_text=rendered)
                emit(
                    "namespace-ready",
                    f"Namespace {namespace} now contains broker resources for {broker_name}.",
                )
            emit(
                "broker-waiting",
                f"Waiting for {expected_runtime_pods} {pod_label} to become ready.",
            )
            ready_pods = self._wait_for_ready_pods(namespace, expected_runtime_pods)
            emit(
                "broker-ready",
                f"{ready_pods} {pod_label} are ready.",
            )
            if broker_id == "kafka":
                self._wait_for_kafka_runtime(namespace, broker_name)
                emit("broker-runtime-ready", "Kafka bootstrap service and topic are ready.")
            if broker_id == "rabbitmq":
                rabbitmq_service = self._wait_for_rabbitmq_runtime(namespace, broker_name)
                emit(
                    "broker-runtime-ready",
                    f"RabbitMQ connection secret and AMQP service {rabbitmq_service} are ready.",
                )
            if broker_id == "artemis":
                artemis_services = self._wait_for_artemis_runtime(
                    namespace,
                    broker_name,
                    target_broker_pods,
                )
                emit(
                    "broker-runtime-ready",
                    f"Artemis AMQP services ready: {', '.join(artemis_services)}",
                )
            if broker_id == "nats":
                service_name = self._wait_for_nats_runtime(namespace, broker_name)
                emit(
                    "broker-runtime-ready",
                    f"NATS JetStream service {service_name} is ready.",
                )
                self._apply_nats_stream_definition(
                    run_id=run_id,
                    namespace=namespace,
                    broker_name=broker_name,
                    deployment_mode=deployment_mode,
                    replicas=_nats_jetstream_replicas(
                        (resource_config or {}).get("replicas"),
                        3 if deployment_mode == "ha" else 1,
                    ),
                )
                self._wait_for_nats_stream_definition(namespace=namespace)
                emit(
                    "broker-runtime-ready",
                    f"NATS benchmark stream {NATS_STREAM_NAME} is reconciled by the cluster NACK operator.",
                )
            return True
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or str(exc)).strip()
            emit("prepare-error", detail or "Cluster preparation failed.")
            return False
        except Exception as exc:  # pragma: no cover - defensive runtime path
            emit("prepare-error", str(exc))
            return False

    def _copy_pull_secret_to_namespace(self, namespace: str) -> None:
        if not self.agent_pull_secret:
            return
        source_namespace = "bench-platform"
        secret_name = self.agent_pull_secret
        source = self._run(
            [
                "kubectl",
                "-n",
                source_namespace,
                "get",
                "secret",
                secret_name,
                "-o",
                "json",
            ],
            check=False,
        )
        if source.returncode != 0:
            return
        payload = json.loads(source.stdout or "{}")
        metadata = payload.setdefault("metadata", {})
        metadata.pop("creationTimestamp", None)
        metadata.pop("resourceVersion", None)
        metadata.pop("uid", None)
        metadata.pop("managedFields", None)
        metadata["namespace"] = namespace
        payload.pop("status", None)
        self._run(["kubectl", "apply", "-f", "-"], stdin_text=json.dumps(payload))

    def _secret_exists(self, namespace: str, secret_name: str) -> bool:
        if not secret_name:
            return False
        result = self._run(
            ["kubectl", "-n", namespace, "get", "secret", secret_name, "-o", "name"],
            check=False,
        )
        return result.returncode == 0 and bool((result.stdout or "").strip())

    def _secret_value(self, namespace: str, secret_name: str, key: str) -> str:
        result = self._run(
            ["kubectl", "-n", namespace, "get", "secret", secret_name, "-o", "json"],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Secret {secret_name} not readable in {namespace}")
        payload = json.loads(result.stdout or "{}")
        encoded = str((((payload.get("data") or {}).get(key)) or "")).strip()
        if not encoded:
            raise RuntimeError(f"Secret {secret_name} in {namespace} is missing key {key}")
        return base64.b64decode(encoded).decode("utf-8")

    def _wait_for_job_pod_started(
        self, namespace: str, job_name: str, timeout_seconds: int = 180
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            result = self._run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    "pods",
                    "-l",
                    f"job-name={job_name}",
                    "-o",
                    "json",
                ],
                check=False,
            )
            if result.returncode == 0:
                payload = json.loads(result.stdout or "{}")
                items = payload.get("items", [])
                if items:
                    for item in items:
                        phase = str(item.get("status", {}).get("phase", "")).lower()
                        if phase in {"running", "succeeded"}:
                            return
                        if phase == "failed":
                            detail = self._job_failure_reason(namespace, job_name)
                            raise RuntimeError(
                                f"Benchmark job {job_name} failed before start: {detail or 'pod failed'}"
                            )
                        container_statuses = item.get("status", {}).get("containerStatuses", []) or []
                        for status in container_statuses:
                            waiting = (status.get("state", {}) or {}).get("waiting", {}) or {}
                            terminated = (status.get("state", {}) or {}).get("terminated", {}) or {}
                            waiting_reason = str(waiting.get("reason") or "").strip()
                            terminated_reason = str(terminated.get("reason") or "").strip()
                            if waiting_reason in {
                                "ErrImagePull",
                                "ImagePullBackOff",
                                "CreateContainerConfigError",
                                "CrashLoopBackOff",
                            }:
                                detail = self._job_failure_reason(namespace, job_name)
                                raise RuntimeError(
                                    f"Benchmark job {job_name} failed before start: {detail or waiting_reason}"
                                )
                            if terminated_reason:
                                detail = self._job_failure_reason(namespace, job_name)
                                raise RuntimeError(
                                    f"Benchmark job {job_name} terminated before start: {detail or terminated_reason}"
                                )
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for job pod {job_name} to start")

    def _benchmark_job_timeout_seconds(
        self,
        *,
        warmup_seconds: int,
        measurement_seconds: int,
        cooldown_seconds: int,
    ) -> int:
        total_window_seconds = max(
            1,
            int(warmup_seconds) + int(measurement_seconds) + int(cooldown_seconds),
        )
        slack_seconds = max(600, int(math.ceil(total_window_seconds * 0.25)))
        return max(self.job_timeout_seconds, total_window_seconds + slack_seconds)

    def _job_completion_state(self, namespace: str, job_name: str) -> str:
        result = self._run(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                f"job/{job_name}",
                "-o",
                "json",
            ],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Benchmark job {job_name} is no longer readable in {namespace}")
        payload = json.loads(result.stdout or "{}")
        status = payload.get("status", {}) or {}
        conditions = status.get("conditions", []) or []
        completions = int((payload.get("spec", {}) or {}).get("completions", 1) or 1)
        succeeded = int(status.get("succeeded", 0) or 0)
        failed = int(status.get("failed", 0) or 0)
        if succeeded >= completions:
            return "succeeded"

        failed_condition = next(
            (
                condition
                for condition in conditions
                if condition.get("type") == "Failed" and condition.get("status") == "True"
            ),
            None,
        )
        if failed > 0 or failed_condition is not None:
            reason = ""
            if failed_condition is not None:
                reason = str(
                    failed_condition.get("message")
                    or failed_condition.get("reason")
                    or ""
                ).strip()
            pod_reason = self._job_failure_reason(namespace, job_name)
            detail = pod_reason or reason or f"job failed with {failed} failed pod(s)"
            raise RuntimeError(f"Benchmark job {job_name} failed: {detail}")
        return "running"

    def _wait_for_jobs_completion(
        self,
        namespace: str,
        job_names: list[str],
        timeout_seconds: int | None = None,
    ) -> None:
        outstanding = {
            str(job_name).strip()
            for job_name in job_names
            if str(job_name or "").strip()
        }
        if not outstanding:
            return
        effective_timeout_seconds = max(1, int(timeout_seconds or self.job_timeout_seconds))
        deadline = time.time() + effective_timeout_seconds
        while time.time() < deadline:
            remaining: set[str] = set()
            for job_name in sorted(outstanding):
                if self._job_completion_state(namespace, job_name) != "succeeded":
                    remaining.add(job_name)
            if not remaining:
                return
            outstanding = remaining
            time.sleep(2)
        raise RuntimeError(
            "Timed out waiting for benchmark job(s) "
            + ", ".join(sorted(outstanding))
            + f" after {effective_timeout_seconds}s"
        )

    def _job_pod_names(self, namespace: str, job_name: str) -> list[str]:
        result = self._run(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-l",
                f"job-name={job_name}",
                "-o",
                "json",
            ],
            check=False,
        )
        if result.returncode != 0:
            return []
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            return []
        items = payload.get("items", []) or []
        ordered = sorted(
            items,
            key=lambda item: (
                str(item.get("metadata", {}).get("creationTimestamp") or ""),
                str(item.get("metadata", {}).get("name") or ""),
            ),
        )
        names = [
            str(item.get("metadata", {}).get("name") or "").strip()
            for item in ordered
        ]
        return [name for name in names if name]

    def _job_logs_to_file_safe(self, namespace: str, job_name: str, container_name: str) -> str:
        temp_path: Path | None = None
        pod_names = self._job_pod_names(namespace, job_name)
        log_commands: list[list[str]] = []
        for pod_name in pod_names:
            log_commands.append(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    f"pod/{pod_name}",
                    "-c",
                    container_name,
                ]
            )
            log_commands.append(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    f"pod/{pod_name}",
                    "-c",
                    container_name,
                    "--previous",
                ]
            )
        log_commands.extend(
            [
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    f"job/{job_name}",
                    "-c",
                    container_name,
                ],
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    f"job/{job_name}",
                    "-c",
                    container_name,
                    "--previous",
                ],
            ]
        )
        for arguments in log_commands:
            if temp_path is None:
                handle = tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    delete=False,
                    suffix=f"-{job_name}-{container_name}.log",
                )
                temp_path = Path(handle.name)
                handle.close()
            with temp_path.open("w", encoding="utf-8") as stdout_handle:
                result = subprocess.run(
                    arguments,
                    cwd=self.repo_root,
                    check=False,
                    text=True,
                    stdout=stdout_handle,
                    stderr=subprocess.PIPE,
                )
            if result.returncode == 0 and temp_path.stat().st_size > 0:
                return str(temp_path)
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)
        return ""

    def _wait_for_file_content(self, path: Path, timeout_seconds: float = 10.0) -> bool:
        deadline = time.time() + max(0.5, float(timeout_seconds))
        while time.time() < deadline:
            try:
                if path.exists() and path.stat().st_size > 0:
                    return True
            except OSError:
                pass
            time.sleep(0.5)
        try:
            return path.exists() and path.stat().st_size > 0
        except OSError:
            return False

    def _job_result_source_path(
        self,
        *,
        run_id: str,
        role: str,
        ordinal: int,
        namespace: str,
        job_name: str,
        container_name: str,
    ) -> str:
        structured_output_path = Path(
            self._agent_structured_output_path(run_id, role, ordinal)
        )
        if self._wait_for_file_content(structured_output_path):
            return str(structured_output_path)
        return self._job_logs_to_file_safe(namespace, job_name, container_name)

    def delete_run_structured_output(self, run_id: str) -> None:
        run_path = self.shared_artifact_dir / str(run_id)
        agent_log_dir = run_path / "agent-logs"
        if agent_log_dir.exists():
            shutil.rmtree(agent_log_dir, ignore_errors=True)
        try:
            if run_path.exists() and not any(run_path.iterdir()):
                run_path.rmdir()
        except OSError:
            return

    def _job_failure_reason(self, namespace: str, job_name: str) -> str:
        result = self._run(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-l",
                f"job-name={job_name}",
                "-o",
                "json",
            ],
            check=False,
        )
        if result.returncode != 0:
            return ""
        payload = json.loads(result.stdout or "{}")
        for item in payload.get("items", []):
            pod_name = item.get("metadata", {}).get("name", job_name)
            state = (
                ((item.get("status", {}) or {}).get("containerStatuses", []) or [{}])[0]
                .get("state", {})
                .get("terminated", {})
            )
            exit_code = state.get("exitCode")
            reason = state.get("reason")
            message = state.get("message")
            parts = [str(part).strip() for part in [reason, message] if str(part or "").strip()]
            if exit_code is not None:
                parts.append(f"exit code {exit_code}")
            log_result = self._run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    pod_name,
                    "--tail=20",
                ],
                check=False,
            )
            if log_result.returncode == 0:
                log_excerpt = " ".join(
                    line.strip()
                    for line in (log_result.stdout or "").splitlines()
                    if line.strip()
                )
                if log_excerpt:
                    parts.append(f"logs: {log_excerpt[:400]}")
            if parts:
                return f"{pod_name}: {', '.join(parts)}"
        return ""

    def _job_node_names(self, namespace: str, job_names: list[str]) -> list[str]:
        node_names: set[str] = set()
        for job_name in job_names:
            result = self._run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    "pods",
                    "-l",
                    f"job-name={job_name}",
                    "-o",
                    "json",
                ],
                check=False,
            )
            if result.returncode != 0:
                continue
            payload = json.loads(result.stdout or "{}")
            for item in payload.get("items", []):
                node_name = item.get("spec", {}).get("nodeName")
                if node_name:
                    node_names.add(str(node_name))
        return sorted(node_names)

    def _kafka_connection_runtime(
        self,
        namespace: str,
        broker_name: str,
        deployment_mode: str,
    ) -> dict[str, Any]:
        del deployment_mode
        return {
            "connectionTargets": [
                [f"--bootstrap-servers={broker_name}-kafka-bootstrap.{namespace}.svc.cluster.local:9092"],
            ],
            "env": [],
        }

    def _rabbitmq_connection_runtime(
        self,
        namespace: str,
        broker_name: str,
        deployment_mode: str,
    ) -> dict[str, Any]:
        del deployment_mode
        self._wait_for_rabbitmq_runtime(namespace, broker_name)
        broker_url = self._secret_value(namespace, f"{broker_name}-default-user", "connection_string")
        return {
            "connectionTargets": [[]],
            "env": [
                {
                    "name": "BROKER_URL",
                    "value": broker_url,
                }
            ],
        }

    def _artemis_connection_runtime(
        self,
        namespace: str,
        broker_name: str,
        deployment_mode: str,
    ) -> dict[str, Any]:
        expected_broker_pods = 3 if deployment_mode == "ha" else 1
        acceptor_services = self._wait_for_artemis_runtime(
            namespace,
            broker_name,
            expected_broker_pods,
        )
        broker_username = self._secret_value(namespace, f"{broker_name}-credentials-secret", "AMQ_USER")
        broker_password = self._secret_value(namespace, f"{broker_name}-credentials-secret", "AMQ_PASSWORD")
        return {
            "connectionTargets": [
                [f"--broker-url=amqp://{service_name}.{namespace}.svc.cluster.local:5672"]
                for service_name in acceptor_services
            ],
            "env": [
                {"name": "BROKER_USERNAME", "value": broker_username},
                {"name": "BROKER_PASSWORD", "value": broker_password},
            ],
        }

    def _nats_connection_runtime(
        self,
        namespace: str,
        broker_name: str,
        deployment_mode: str,
    ) -> dict[str, Any]:
        del deployment_mode
        self._wait_for_nats_runtime(namespace, broker_name)
        return {
            "connectionTargets": [
                [f"--broker-url=nats://{broker_name}.{namespace}.svc.cluster.local:4222"]
            ],
            "env": [],
        }

    def _broker_connection_args(
        self,
        broker_id: str,
        namespace: str,
        broker_name: str,
        deployment_mode: str,
    ) -> dict[str, Any]:
        builders = {
            "kafka": self._kafka_connection_runtime,
            "rabbitmq": self._rabbitmq_connection_runtime,
            "artemis": self._artemis_connection_runtime,
            "nats": self._nats_connection_runtime,
        }
        return builders[broker_id](namespace, broker_name, deployment_mode)

    def _job_phase(self, namespace: str, job_name: str) -> str:
        result = self._run(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "job",
                job_name,
                "-o",
                "json",
            ],
            check=False,
        )
        if result.returncode != 0:
            return "not_found"
        payload = json.loads(result.stdout or "{}")
        status = payload.get("status", {}) or {}
        conditions = status.get("conditions", []) or []
        completions = int((payload.get("spec", {}) or {}).get("completions", 1) or 1)
        succeeded = int(status.get("succeeded", 0) or 0)
        failed = int(status.get("failed", 0) or 0)
        if succeeded >= completions:
            return "succeeded"
        failed_condition = next(
            (
                condition
                for condition in conditions
                if condition.get("type") == "Failed" and condition.get("status") == "True"
            ),
            None,
        )
        if failed > 0 or failed_condition is not None:
            return "failed"
        return "running"

    def run_finalizer_state(self, run_id: str) -> str:
        if not self.enabled or shutil.which("kubectl") is None:
            return "not_found"
        return self._job_phase(self.platform_namespace, finalizer_job_name_for(run_id))

    def delete_run_finalizer_job(self, run_id: str) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        self._run(
            [
                "kubectl",
                "-n",
                self.platform_namespace,
                "delete",
                "job",
                finalizer_job_name_for(run_id),
                "--ignore-not-found=true",
                "--wait=false",
            ],
            check=False,
        )

    def delete_benchmark_jobs(self, run_id: str, *, producers: int, consumers: int) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        consumer_jobs, producer_jobs = benchmark_job_names_for(
            run_id,
            producers=producers,
            consumers=consumers,
        )
        for job_name in [*consumer_jobs, *producer_jobs]:
            self._run(
                [
                    "kubectl",
                    "-n",
                    self.platform_namespace,
                    "delete",
                    "job",
                    job_name,
                    "--ignore-not-found=true",
                    "--wait=false",
                ],
                check=False,
            )

    def launch_run_finalizer(self, run_id: str) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        job_name = finalizer_job_name_for(run_id)
        job_state = self._job_phase(self.platform_namespace, job_name)
        if job_state == "running":
            return
        if job_state in {"failed", "succeeded"}:
            self._run(
                [
                    "kubectl",
                    "-n",
                    self.platform_namespace,
                    "delete",
                    "job",
                    job_name,
                    "--ignore-not-found=true",
                    "--wait=true",
                ],
                check=False,
            )
        document = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.platform_namespace,
                "labels": {
                    "benchmark.ninefinger9.io/run-id": run_id,
                    "benchmark.ninefinger9.io/owned-by": "bus-platform",
                    "benchmark.ninefinger9.io/role": "finalizer",
                },
            },
            "spec": {
                "ttlSecondsAfterFinished": 900,
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "serviceAccountName": "bus-platform-control",
                        "containers": [
                            {
                                "name": "finalizer",
                                "image": self.platform_image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "python",
                                    "-m",
                                    "services.platform.finalizer",
                                ],
                                "args": [
                                    f"--run-id={run_id}",
                                ],
                                "env": [
                                    {"name": "BUS_STORAGE_MODE", "value": os.environ.get("BUS_STORAGE_MODE", "local")},
                                    {"name": "BUS_DB_PATH", "value": os.environ.get("BUS_DB_PATH", "/data/bus.db")},
                                    {"name": "BUS_ARTIFACT_DIR", "value": os.environ.get("BUS_ARTIFACT_DIR", "/data/artifacts")},
                                    {"name": "BUS_REPORT_DIR", "value": os.environ.get("BUS_REPORT_DIR", "/data/reports")},
                                    {"name": "BUS_ENABLE_CLUSTER_ACTIONS", "value": "true"},
                                    {"name": "BUS_ENABLE_OPERATOR_BOOTSTRAP", "value": "false"},
                                    {"name": "BUS_PLATFORM_DATA_NAMESPACE", "value": os.environ.get("BUS_PLATFORM_DATA_NAMESPACE", self.platform_data_namespace)},
                                    {"name": "BUS_POSTGRES_CLUSTER_NAME", "value": os.environ.get("BUS_POSTGRES_CLUSTER_NAME", self.postgres_cluster_name)},
                                    {"name": "BUS_POSTGRES_HOST", "value": os.environ.get("BUS_POSTGRES_HOST", f"{self.postgres_cluster_name}-rw.{self.platform_data_namespace}.svc.cluster.local")},
                                    {"name": "BUS_POSTGRES_PORT", "value": os.environ.get("BUS_POSTGRES_PORT", "5432")},
                                    {"name": "BUS_POSTGRES_DATABASE", "value": os.environ.get("BUS_POSTGRES_DATABASE", self.postgres_database)},
                                    {"name": "BUS_POSTGRES_USERNAME", "value": os.environ.get("BUS_POSTGRES_USERNAME", self.postgres_username)},
                                    {"name": "BUS_POSTGRES_PASSWORD", "value": os.environ.get("BUS_POSTGRES_PASSWORD", self.postgres_password)},
                                    {"name": "BUS_MINIO_TENANT_NAME", "value": os.environ.get("BUS_MINIO_TENANT_NAME", self.minio_tenant_name)},
                                    {"name": "BUS_S3_ENDPOINT", "value": os.environ.get("BUS_S3_ENDPOINT", f"http://{self.minio_tenant_name}-hl.{self.platform_data_namespace}.svc.cluster.local:9000")},
                                    {"name": "BUS_S3_REGION", "value": os.environ.get("BUS_S3_REGION", "us-east-1")},
                                    {"name": "BUS_MINIO_BUCKET", "value": os.environ.get("BUS_MINIO_BUCKET", self.minio_bucket)},
                                    {"name": "BUS_MINIO_APP_ACCESS_KEY", "value": os.environ.get("BUS_MINIO_APP_ACCESS_KEY", self.minio_app_access_key)},
                                    {"name": "BUS_MINIO_APP_SECRET_KEY", "value": os.environ.get("BUS_MINIO_APP_SECRET_KEY", self.minio_app_secret_key)},
                                    {"name": "BUS_BENCHMARK_AGENT_IMAGE", "value": self.agent_image},
                                    {"name": "BUS_AGENT_IMAGE_PULL_SECRET", "value": self.agent_pull_secret},
                                    {"name": "BUS_AGENT_IMAGE_PULL_POLICY", "value": self.agent_pull_policy},
                                    {"name": "BUS_AGENT_START_BARRIER_SECONDS", "value": str(self.agent_start_barrier_seconds)},
                                    {"name": "BUS_RESOURCE_POLL_SECONDS", "value": str(self.resource_poll_seconds)},
                                    {"name": "BUS_BENCHMARK_JOB_TIMEOUT_SECONDS", "value": str(self.job_timeout_seconds)},
                                    {"name": "BUS_PLATFORM_CPU_REQUEST", "value": str((self.platform_resource_requests.get("requests", {}) or {}).get("cpu", ""))},
                                    {"name": "BUS_PLATFORM_MEMORY_REQUEST", "value": str((self.platform_resource_requests.get("requests", {}) or {}).get("memory", ""))},
                                    {"name": "BUS_PLATFORM_CPU_LIMIT", "value": str((self.platform_resource_requests.get("limits", {}) or {}).get("cpu", ""))},
                                    {"name": "BUS_PLATFORM_MEMORY_LIMIT", "value": str((self.platform_resource_requests.get("limits", {}) or {}).get("memory", ""))},
                                    {"name": "BUS_PRODUCER_JOB_CPU_REQUEST", "value": str((self.producer_job_resources.get("requests", {}) or {}).get("cpu", ""))},
                                    {"name": "BUS_PRODUCER_JOB_MEMORY_REQUEST", "value": str((self.producer_job_resources.get("requests", {}) or {}).get("memory", ""))},
                                    {"name": "BUS_PRODUCER_JOB_CPU_LIMIT", "value": str((self.producer_job_resources.get("limits", {}) or {}).get("cpu", ""))},
                                    {"name": "BUS_PRODUCER_JOB_MEMORY_LIMIT", "value": str((self.producer_job_resources.get("limits", {}) or {}).get("memory", ""))},
                                    {"name": "BUS_CONSUMER_JOB_CPU_REQUEST", "value": str((self.consumer_job_resources.get("requests", {}) or {}).get("cpu", ""))},
                                    {"name": "BUS_CONSUMER_JOB_MEMORY_REQUEST", "value": str((self.consumer_job_resources.get("requests", {}) or {}).get("memory", ""))},
                                    {"name": "BUS_CONSUMER_JOB_CPU_LIMIT", "value": str((self.consumer_job_resources.get("limits", {}) or {}).get("cpu", ""))},
                                    {"name": "BUS_CONSUMER_JOB_MEMORY_LIMIT", "value": str((self.consumer_job_resources.get("limits", {}) or {}).get("memory", ""))},
                                ],
                                "resources": self.finalizer_job_resources,
                                "volumeMounts": [{"name": "runtime-data", "mountPath": "/data"}],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "runtime-data",
                                "persistentVolumeClaim": {"claimName": self.platform_data_pvc},
                            }
                        ],
                    }
                },
            },
        }
        rendered = yaml.safe_dump(document, sort_keys=False)
        self._run(["kubectl", "apply", "-f", "-"], stdin_text=rendered)

    def _agent_structured_output_path(self, run_id: str, role: str, ordinal: int) -> str:
        role_name = str(role or "").strip().lower() or "agent"
        return str(
            self.shared_artifact_dir
            / run_id
            / "agent-logs"
            / f"{role_name}-{max(1, int(ordinal))}.jsonl"
        )

    def _build_agent_jobs(
        self,
        *,
        run_id: str,
        broker_id: str,
        scenario_id: str | None,
        config_mode: str,
        deployment_mode: str,
        namespace: str,
        broker_tuning: dict[str, Any] | None,
        message_rate: int,
        message_size_bytes: int,
        producers: int,
        consumers: int,
        warmup_seconds: int,
        measurement_seconds: int,
        cooldown_seconds: int,
        transport_options: dict[str, Any] | None,
        resource_config: dict[str, Any] | None = None,
        scheduled_start_ns: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[str], list[str], int]:
        short_id = run_short_id(run_id)
        broker_name = f"{broker_id}-{short_id}"
        agent_namespace = self.platform_namespace
        scenario_name = scenario_id or f"{broker_id}-benchmark"
        topology_mode = "spsc"
        if producers > 1 and consumers > 1:
            topology_mode = "mpmc"
        elif producers > 1:
            topology_mode = "mpsc"
        elif consumers > 1:
            topology_mode = "spmc"
        total_window_seconds = max(1, warmup_seconds + measurement_seconds + cooldown_seconds)
        runtime_config = self._broker_connection_args(
            broker_id,
            namespace,
            broker_name,
            deployment_mode,
        )
        producer_tuning_args, consumer_tuning_args = build_supported_client_args(
            broker_id, broker_tuning
        )
        rate_profile_kind = "constant"
        peak_message_rate = int(message_rate)
        if transport_options:
            rate_profile_kind = str(transport_options.get("rateProfileKind", "constant")).strip().lower() or "constant"
            try:
                peak_message_rate = int(
                    transport_options.get(
                        "peakMessageRate",
                        _derived_peak_message_rate(message_rate, rate_profile_kind),
                    )
                    or _derived_peak_message_rate(message_rate, rate_profile_kind)
                )
            except (TypeError, ValueError):
                peak_message_rate = _derived_peak_message_rate(message_rate, rate_profile_kind)
        peak_message_rate = max(int(message_rate), peak_message_rate)
        image_pull_secrets = (
            [{"name": self.agent_pull_secret}]
            if self.agent_pull_secret and self._secret_exists(agent_namespace, self.agent_pull_secret)
            else []
        )
        job_active_deadline_seconds = self._benchmark_job_timeout_seconds(
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
        )
        consumer_jobs, producer_jobs = benchmark_job_names_for(
            run_id,
            producers=producers,
            consumers=consumers,
        )
        documents: list[dict[str, Any]] = []
        connection_targets = runtime_config.get("connectionTargets", [[]]) or [[]]
        scheduled_start_ns = scheduled_start_ns or (
            time.time_ns() + int(self.agent_start_barrier_seconds * 1_000_000_000)
        )
        nats_jetstream_replicas = _nats_jetstream_replicas(
            (resource_config or {}).get("replicas"),
            3 if deployment_mode == "ha" else 1,
        )
        nats_stream_managed_by = "nack" if broker_id == "nats" else "client"

        for index, job_name in enumerate(consumer_jobs):
            selected_connection_args = list(connection_targets[index % len(connection_targets)])
            env = [dict(item) for item in (runtime_config.get("env", []) or [])]
            consumer_idle_exit_seconds = max(5, min(10, cooldown_seconds + 2))
            structured_output_path = self._agent_structured_output_path(run_id, "consumer", index + 1)
            args = [
                "--role=consumer",
                f"--broker={broker_id}",
                "--destination=benchmark.events",
                f"--run-id={run_id}",
                f"--scenario-id={scenario_name}",
                f"--consumer-id=consumer-{index + 1}",
                f"--consumer-group=bench-{short_id}",
                "--message-limit=0",
                f"--scheduled-start-at={scheduled_start_ns}",
                f"--warmup-seconds={warmup_seconds}",
                f"--measurement-seconds={measurement_seconds}",
                f"--cooldown-seconds={cooldown_seconds}",
                f"--idle-exit-seconds={consumer_idle_exit_seconds}",
                f"--structured-output-path={structured_output_path}",
            ]
            if broker_id == "nats":
                args.append(f"--jetstream-replicas={nats_jetstream_replicas}")
                args.append(f"--jetstream-stream-managed-by={nats_stream_managed_by}")
            args.extend(selected_connection_args)
            args.extend(consumer_tuning_args)
            documents.append(
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "name": job_name,
                        "namespace": agent_namespace,
                        "labels": {
                            "benchmark.ninefinger9.io/run-id": run_id,
                            "benchmark.ninefinger9.io/role": "consumer",
                        },
                    },
                    "spec": {
                        "activeDeadlineSeconds": job_active_deadline_seconds,
                        "backoffLimit": 0,
                        "template": {
                            "spec": {
                                "restartPolicy": "Never",
                                "imagePullSecrets": image_pull_secrets,
                                "volumes": [
                                    {
                                        "name": "runtime-data",
                                        "persistentVolumeClaim": {"claimName": self.platform_data_pvc},
                                    }
                                ],
                                "containers": [
                                    {
                                        "name": "consumer",
                                        "image": self.agent_image,
                                        "imagePullPolicy": self.agent_pull_policy,
                                        "args": args,
                                        "env": env,
                                        "resources": self.consumer_job_resources,
                                        "volumeMounts": [{"name": "runtime-data", "mountPath": "/data"}],
                                    }
                                ],
                            }
                        },
                    },
                }
            )

        per_producer_rate = max(1, int(math.ceil(message_rate / max(1, producers))))
        per_producer_peak_rate = max(
            per_producer_rate,
            int(math.ceil(peak_message_rate / max(1, producers))),
        )
        total_message_count = max(1, per_producer_peak_rate * total_window_seconds * 2)
        for index, job_name in enumerate(producer_jobs):
            selected_connection_args = list(connection_targets[index % len(connection_targets)])
            env = [dict(item) for item in (runtime_config.get("env", []) or [])]
            structured_output_path = self._agent_structured_output_path(run_id, "producer", index + 1)
            args = [
                "--role=producer",
                f"--broker={broker_id}",
                "--destination=benchmark.events",
                f"--run-id={run_id}",
                f"--scenario-id={scenario_name}",
                f"--producer-id=producer-{index + 1}",
                "--receiver-id=consumer-pool",
                f"--config-mode={config_mode}",
                f"--topology-mode={topology_mode}",
                "--durability-mode=persistent",
                f"--message-size-bytes={message_size_bytes}",
                f"--message-rate={per_producer_rate}",
                f"--peak-message-rate={per_producer_peak_rate}",
                f"--rate-profile={rate_profile_kind}",
                f"--message-count={total_message_count}",
                f"--scheduled-start-at={scheduled_start_ns}",
                f"--warmup-seconds={warmup_seconds}",
                f"--measurement-seconds={measurement_seconds}",
                f"--cooldown-seconds={cooldown_seconds}",
                f"--structured-output-path={structured_output_path}",
            ]
            if broker_id == "nats":
                args.append(f"--jetstream-replicas={nats_jetstream_replicas}")
                args.append(f"--jetstream-stream-managed-by={nats_stream_managed_by}")
            args.extend(selected_connection_args)
            args.extend(producer_tuning_args)
            documents.append(
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "name": job_name,
                        "namespace": agent_namespace,
                        "labels": {
                            "benchmark.ninefinger9.io/run-id": run_id,
                            "benchmark.ninefinger9.io/role": "producer",
                        },
                    },
                    "spec": {
                        "activeDeadlineSeconds": job_active_deadline_seconds,
                        "backoffLimit": 0,
                        "template": {
                            "spec": {
                                "restartPolicy": "Never",
                                "imagePullSecrets": image_pull_secrets,
                                "volumes": [
                                    {
                                        "name": "runtime-data",
                                        "persistentVolumeClaim": {"claimName": self.platform_data_pvc},
                                    }
                                ],
                                "containers": [
                                    {
                                        "name": "producer",
                                        "image": self.agent_image,
                                        "imagePullPolicy": self.agent_pull_policy,
                                        "args": args,
                                        "env": env,
                                        "resources": self.producer_job_resources,
                                        "volumeMounts": [{"name": "runtime-data", "mountPath": "/data"}],
                                    }
                                ],
                            }
                        },
                    },
                }
            )

        return documents, consumer_jobs, producer_jobs, scheduled_start_ns

    def run_benchmark_agents(
        self,
        *,
        run_id: str,
        broker_id: str,
        scenario_id: str | None,
        config_mode: str,
        deployment_mode: str,
        message_rate: int,
        message_size_bytes: int,
        producers: int,
        consumers: int,
        warmup_seconds: int,
        measurement_seconds: int,
        cooldown_seconds: int,
        transport_options: dict[str, Any] | None,
        broker_tuning: dict[str, Any] | None,
        resource_config: dict[str, Any] | None,
        scheduled_start_ns: int | None,
        emit: Callable[[str, str], None],
    ) -> dict[str, Any]:
        namespace = run_namespace_for(run_id)
        agent_namespace = self.platform_namespace
        self._copy_pull_secret_to_namespace(agent_namespace)
        documents, consumer_jobs, producer_jobs, scheduled_start_ns = self._build_agent_jobs(
            run_id=run_id,
            broker_id=broker_id,
            scenario_id=scenario_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            namespace=namespace,
            broker_tuning=broker_tuning,
            message_rate=message_rate,
            message_size_bytes=message_size_bytes,
            producers=producers,
            consumers=consumers,
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
            transport_options=transport_options,
            resource_config=resource_config,
            scheduled_start_ns=scheduled_start_ns,
        )
        rendered = yaml.safe_dump_all(documents, sort_keys=False)
        self._run(["kubectl", "apply", "-f", "-"], stdin_text=rendered)
        emit("consumer-running", f"Consumer job(s) created in {agent_namespace}.")
        for job_name in consumer_jobs:
            self._wait_for_job_pod_started(agent_namespace, job_name)
        emit("producer-running", f"Producer job(s) created in {agent_namespace}.")
        for job_name in producer_jobs:
            self._wait_for_job_pod_started(agent_namespace, job_name)
        if time.time_ns() >= scheduled_start_ns:
            raise RuntimeError(
                "Producer synchronization barrier expired before all producer pods were ready."
            )
        emit(
            "execution-synchronized",
            f"Producer start barrier armed for {max(0, int((scheduled_start_ns - time.time_ns()) / 1_000_000_000))}s.",
        )
        return self.collect_benchmark_results(
            run_id=run_id,
            producer_jobs=producer_jobs,
            consumer_jobs=consumer_jobs,
            scheduled_start_ns=scheduled_start_ns,
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
        )

    def launch_benchmark_agents(
        self,
        *,
        run_id: str,
        broker_id: str,
        scenario_id: str | None,
        config_mode: str,
        deployment_mode: str,
        message_rate: int,
        message_size_bytes: int,
        producers: int,
        consumers: int,
        warmup_seconds: int,
        measurement_seconds: int,
        cooldown_seconds: int,
        transport_options: dict[str, Any] | None,
        broker_tuning: dict[str, Any] | None,
        resource_config: dict[str, Any] | None,
        scheduled_start_ns: int | None,
        emit: Callable[[str, str], None],
    ) -> dict[str, Any]:
        namespace = run_namespace_for(run_id)
        agent_namespace = self.platform_namespace
        self._copy_pull_secret_to_namespace(agent_namespace)
        documents, consumer_jobs, producer_jobs, scheduled_start_ns = self._build_agent_jobs(
            run_id=run_id,
            broker_id=broker_id,
            scenario_id=scenario_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            namespace=namespace,
            broker_tuning=broker_tuning,
            message_rate=message_rate,
            message_size_bytes=message_size_bytes,
            producers=producers,
            consumers=consumers,
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
            transport_options=transport_options,
            resource_config=resource_config,
            scheduled_start_ns=scheduled_start_ns,
        )
        rendered = yaml.safe_dump_all(documents, sort_keys=False)
        self._run(["kubectl", "apply", "-f", "-"], stdin_text=rendered)
        resource_samples: list[dict[str, Any]] = []
        emit("consumer-running", f"Consumer job(s) created in {agent_namespace}.")
        for job_name in consumer_jobs:
            self._wait_for_job_pod_started(agent_namespace, job_name)
        emit("producer-running", f"Producer job(s) created in {agent_namespace}.")
        for job_name in producer_jobs:
            self._wait_for_job_pod_started(agent_namespace, job_name)
        if time.time_ns() >= scheduled_start_ns:
            raise RuntimeError(
                "Producer synchronization barrier expired before all producer pods were ready."
            )
        emit(
            "execution-synchronized",
            f"Producer start barrier armed for {max(0, int((scheduled_start_ns - time.time_ns()) / 1_000_000_000))}s.",
        )
        del resource_samples
        return {
            "namespace": namespace,
            "agentNamespace": agent_namespace,
            "producerJobs": producer_jobs,
            "consumerJobs": consumer_jobs,
            "scheduledStartNs": scheduled_start_ns,
        }

    def collect_benchmark_results(
        self,
        *,
        run_id: str,
        producer_jobs: list[str],
        consumer_jobs: list[str],
        scheduled_start_ns: int,
        warmup_seconds: int,
        measurement_seconds: int,
        cooldown_seconds: int,
    ) -> dict[str, Any]:
        namespace = run_namespace_for(run_id)
        agent_namespace = self.platform_namespace
        resource_samples: list[dict[str, Any]] = []
        sampler_stop = threading.Event()
        sampler = threading.Thread(
            target=self._sample_namespace_resources,
            kwargs={
                "namespace": namespace,
                "start_ns": scheduled_start_ns,
                "stop_event": sampler_stop,
                "sink": resource_samples,
            },
            name=f"resource-sampler-{run_id[:8]}",
            daemon=True,
        )
        sampler.start()
        job_timeout_seconds = self._benchmark_job_timeout_seconds(
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
        )
        post_measurement_failure = ""
        try:
            self._wait_for_jobs_completion(
                agent_namespace,
                producer_jobs + consumer_jobs,
                timeout_seconds=job_timeout_seconds,
            )
        except RuntimeError as exc:
            if _measurement_window_ended(
                scheduled_start_ns=scheduled_start_ns,
                warmup_seconds=warmup_seconds,
                measurement_seconds=measurement_seconds,
            ):
                post_measurement_failure = str(exc)
            else:
                raise
        finally:
            sampler_stop.set()
            sampler.join(timeout=5)

        producer_log_paths = [
            self._job_result_source_path(
                run_id=run_id,
                role="producer",
                ordinal=index + 1,
                namespace=agent_namespace,
                job_name=job_name,
                container_name="producer",
            )
            for index, job_name in enumerate(producer_jobs)
        ]
        consumer_log_paths = [
            self._job_result_source_path(
                run_id=run_id,
                role="consumer",
                ordinal=index + 1,
                namespace=agent_namespace,
                job_name=job_name,
                container_name="consumer",
            )
            for index, job_name in enumerate(consumer_jobs)
        ]
        node_names = self._job_node_names(agent_namespace, consumer_jobs + producer_jobs)
        timing_confidence = "validated" if len(node_names) <= 1 else "degraded"
        payload = {
            "producerLogPaths": [item for item in producer_log_paths if item],
            "consumerLogPaths": [item for item in consumer_log_paths if item],
            "nodeNames": node_names,
            "timingConfidence": timing_confidence,
            "scheduledStartNs": scheduled_start_ns,
            "resourceSamples": resource_samples,
        }
        if post_measurement_failure:
            payload["postMeasurementFailure"] = post_measurement_failure
        return payload

    def delete_run_namespace(self, run_id: str) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        namespace = run_namespace_for(run_id)
        self._run(
            ["kubectl", "delete", "namespace", namespace, "--ignore-not-found=true", "--wait=false"],
            check=False,
        )

    def force_delete_namespace(self, run_id: str) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        self._force_delete_namespace(run_namespace_for(run_id))

    def wait_for_namespace_deleted(self, run_id: str, timeout_seconds: int = 60) -> bool:
        if not self.enabled or shutil.which("kubectl") is None:
            return True
        namespace = run_namespace_for(run_id)
        deadline = time.time() + timeout_seconds
        force_cleanup_applied = False
        while time.time() < deadline:
            result = self._run(
                ["kubectl", "get", "namespace", namespace, "-o", "name"],
                check=False,
            )
            if result.returncode != 0:
                return True
            if not force_cleanup_applied and time.time() >= deadline - max(30, timeout_seconds // 3):
                self._force_delete_namespace(namespace)
                force_cleanup_applied = True
            time.sleep(2)
        return False

    def namespace_exists(self, run_id: str) -> bool:
        if not self.enabled or shutil.which("kubectl") is None:
            return False
        namespace = run_namespace_for(run_id)
        result = self._run(
            ["kubectl", "get", "namespace", namespace, "-o", "name"],
            check=False,
        )
        return result.returncode == 0 and bool((result.stdout or "").strip())

    def _force_delete_namespace(self, namespace: str) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        resource_types = [
            "kafkatopics.kafka.strimzi.io",
            "kafkas.kafka.strimzi.io",
            "kafkanodepools.kafka.strimzi.io",
            "rabbitmqclusters.rabbitmq.com",
            "activemqartemises.broker.amq.io",
            "activemqartemisaddresses.broker.amq.io",
            "streams.jetstream.nats.io",
            "consumers.jetstream.nats.io",
            "accounts.jetstream.nats.io",
            "jobs.batch",
            "statefulsets.apps",
            "persistentvolumeclaims",
            "pods",
        ]
        for resource_type in resource_types:
            result = self._run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    resource_type,
                    "-o",
                    "json",
                ],
                check=False,
            )
            if result.returncode != 0:
                continue
            payload = json.loads(result.stdout or "{}")
            for item in payload.get("items", []):
                name = item.get("metadata", {}).get("name")
                if not name:
                    continue
                self._run(
                    [
                        "kubectl",
                        "-n",
                        namespace,
                        "patch",
                        resource_type,
                        name,
                        "--type=merge",
                        "-p",
                        '{"metadata":{"finalizers":[]}}',
                    ],
                    check=False,
                )
        self._run(
            [
                "kubectl",
                "patch",
                "namespace",
                namespace,
                "--type=merge",
                "-p",
                '{"spec":{"finalizers":[]}}',
            ],
            check=False,
        )

    def delete_legacy_broker_namespaces(self) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        self.purge_shared_broker_state()
        for namespace in LEGACY_BROKER_NAMESPACES:
            self._run(
                ["kubectl", "delete", "namespace", namespace, "--ignore-not-found=true", "--wait=false"],
                check=False,
            )

    def shared_broker_namespaces(self) -> tuple[str, ...]:
        configured = str(os.environ.get("BUS_SHARED_BROKER_NAMESPACES") or "").strip()
        extra = tuple(item.strip() for item in configured.split(",") if item.strip())
        return tuple(dict.fromkeys((*LEGACY_BROKER_NAMESPACES, *extra)))

    def purge_shared_broker_state(self) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        for namespace in self.shared_broker_namespaces():
            namespace_result = self._run(
                ["kubectl", "get", "namespace", namespace, "-o", "name"],
                check=False,
            )
            if namespace_result.returncode != 0:
                continue
            for resource_type in SHARED_BROKER_RESOURCE_TYPES:
                self._run(
                    [
                        "kubectl",
                        "-n",
                        namespace,
                        "delete",
                        resource_type,
                        "-l",
                        "benchmark.ninefinger9.io/owned-by=bus-platform",
                        "--ignore-not-found=true",
                        "--wait=false",
                    ],
                    check=False,
                )

    def list_owned_run_namespaces(self) -> list[dict[str, str]]:
        if not self.enabled or shutil.which("kubectl") is None:
            return []
        result = self._run(
            [
                "kubectl",
                "get",
                "namespaces",
                "-l",
                "benchmark.ninefinger9.io/owned-by=bus-platform",
                "-o",
                "json",
            ],
            check=False,
        )
        if result.returncode != 0:
            return []
        payload = json.loads(result.stdout or "{}")
        namespaces: list[dict[str, str]] = []
        for item in payload.get("items", []):
            metadata = item.get("metadata", {}) or {}
            name = str(metadata.get("name") or "").strip()
            if not name.startswith("bench-run-"):
                continue
            labels = metadata.get("labels", {}) or {}
            namespaces.append(
                {
                    "name": name,
                    "runId": str(labels.get("benchmark.ninefinger9.io/run-id") or "").strip(),
                }
            )
        return namespaces

    def delete_orphan_run_namespaces(self, preserved_run_ids: set[str] | None = None) -> None:
        if not self.enabled or shutil.which("kubectl") is None:
            return
        preserved = {str(run_id).strip() for run_id in (preserved_run_ids or set()) if str(run_id).strip()}
        for item in self.list_owned_run_namespaces():
            if item["runId"] in preserved:
                continue
            namespace = item["name"]
            self._run(
                ["kubectl", "delete", "namespace", namespace, "--ignore-not-found=true", "--wait=false"],
                check=False,
            )
            self._force_delete_namespace(namespace)
