from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
VERSION_CATALOG_PATH = REPO_ROOT / "deploy" / "k8s" / "operators" / "versions.yaml"

DEFAULT_VERSION_CATALOG: dict[str, Any] = {
    "cnpg": {
        "chartRef": "https://github.com/cloudnative-pg/charts/releases/download/cloudnative-pg-v0.27.1/cloudnative-pg-0.27.1.tgz",
        "chartVersion": "0.27.1",
        "operatorVersion": "1.28.1",
    },
    "minioOperator": {
        "chartRef": "minio-operator/operator",
        "chartVersion": "7.1.1",
        "operatorVersion": "7.1.1",
    },
    "minioTenant": {
        "chartRef": "minio-operator/tenant",
        "chartVersion": "7.1.1",
        "runtimeVersion": "7.1.1",
    },
    "strimzi": {
        "chartRef": "strimzi/strimzi-kafka-operator",
        "chartVersion": "0.51.0",
        "operatorVersion": "0.51.0",
    },
    "rabbitmq": {
        "manifestUrl": "https://github.com/rabbitmq/cluster-operator/releases/download/v2.17.0/cluster-operator.yml",
        "operatorVersion": "2.17.0",
    },
    "artemis": {
        "chartRef": "oci://quay.io/arkmq-org/helm-charts/arkmq-org-broker-operator",
        "chartVersion": "2.1.4",
        "operatorVersion": "2.1.4",
    },
    "nats": {
        "chartRef": "nats/nats",
        "chartVersion": "2.12.6",
        "runtimeVersion": "2.12.6",
    },
    "nack": {
        "chartRef": "nats/nack",
        "chartVersion": "0.33.2",
        "controllerVersion": "0.22.2",
    },
    "brokers": {
        "kafka": "4.2.0",
        "rabbitmq": "4.2.5",
        "artemis": "2.44.0",
        "nats": "2.12.6",
    },
}


@lru_cache(maxsize=1)
def load_version_catalog() -> dict[str, Any]:
    try:
        with VERSION_CATALOG_PATH.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
    except (FileNotFoundError, yaml.YAMLError):
        return dict(DEFAULT_VERSION_CATALOG)
    if not isinstance(loaded, dict):
        return dict(DEFAULT_VERSION_CATALOG)
    merged = dict(DEFAULT_VERSION_CATALOG)
    for key, value in loaded.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def _catalog_value(section: str, key: str) -> str:
    catalog = load_version_catalog()
    fallback = str(DEFAULT_VERSION_CATALOG.get(section, {}).get(key, ""))
    return str(catalog.get(section, {}).get(key, fallback)).strip()


def broker_version(broker_id: str) -> str:
    catalog = load_version_catalog()
    return str(catalog.get("brokers", {}).get(broker_id, "")).strip()


def cnpg_chart_ref() -> str:
    return _catalog_value("cnpg", "chartRef")


def cnpg_chart_version() -> str:
    return _catalog_value("cnpg", "chartVersion")


def cnpg_operator_version() -> str:
    return _catalog_value("cnpg", "operatorVersion")


def minio_operator_chart_ref() -> str:
    return _catalog_value("minioOperator", "chartRef")


def minio_operator_chart_version() -> str:
    return _catalog_value("minioOperator", "chartVersion")


def minio_operator_version() -> str:
    return _catalog_value("minioOperator", "operatorVersion")


def minio_tenant_chart_ref() -> str:
    return _catalog_value("minioTenant", "chartRef")


def minio_tenant_chart_version() -> str:
    return _catalog_value("minioTenant", "chartVersion")


def minio_tenant_runtime_version() -> str:
    return _catalog_value("minioTenant", "runtimeVersion")


def strimzi_chart_ref() -> str:
    return _catalog_value("strimzi", "chartRef")


def strimzi_chart_version() -> str:
    return _catalog_value("strimzi", "chartVersion")


def strimzi_operator_version() -> str:
    return _catalog_value("strimzi", "operatorVersion")


def rabbitmq_operator_manifest_url() -> str:
    return _catalog_value("rabbitmq", "manifestUrl")


def rabbitmq_operator_version() -> str:
    return _catalog_value("rabbitmq", "operatorVersion")


def artemis_chart_ref() -> str:
    return _catalog_value("artemis", "chartRef")


def artemis_chart_version() -> str:
    return _catalog_value("artemis", "chartVersion")


def artemis_operator_version() -> str:
    return _catalog_value("artemis", "operatorVersion")


def nats_chart_ref() -> str:
    return _catalog_value("nats", "chartRef")


def nats_chart_version() -> str:
    return _catalog_value("nats", "chartVersion")


def nats_runtime_version() -> str:
    return _catalog_value("nats", "runtimeVersion")


def nack_chart_ref() -> str:
    return _catalog_value("nack", "chartRef")


def nack_chart_version() -> str:
    return _catalog_value("nack", "chartVersion")


def nack_controller_version() -> str:
    return _catalog_value("nack", "controllerVersion")
