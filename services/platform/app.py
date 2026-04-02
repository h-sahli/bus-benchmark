from __future__ import annotations

import base64
import html
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, model_validator

from services.platform.brokers import (
    broker_catalog_entries,
    broker_definition,
    normalize_protocol_for_broker,
    sanitize_broker_tuning,
    target_replicas_for_mode,
)
from services.platform.cluster import ClusterAutomation, run_namespace_for
from services.platform.common import parse_iso, parse_metrics, utc_now
from services.platform.measurement_material import (
    cleanup_measurement_material,
    load_measurement_material,
    persist_run_artifact,
)
from services.platform.measurements import (
    aggregate_agent_results,
)
from services.platform.reports import (
    normalize_report_sections,
    slugify_file_name,
    write_report_pdf,
)
from services.platform.runtime_storage import (
    REPO_ROOT as STORAGE_REPO_ROOT,
    resolve_runtime_storage,
)


REPO_ROOT = STORAGE_REPO_ROOT
STATIC_DIR = Path(__file__).resolve().parent / "static"
SCENARIO_CATALOG = REPO_ROOT / "catalog" / "scenarios" / "defaults.yaml"
BROKER_PROFILE_CATALOG = REPO_ROOT / "catalog" / "broker-profiles.yaml"

RATE_PROFILE_KINDS = {"constant", "ramp-up", "ramp-down", "step-up", "burst"}
SCHEDULE_MODES = {"parallel", "sequential"}
QUEUE_STATES = {"queued", "waiting"}
ACTIVE_EXECUTION_STATUSES = {"scheduled", "preparing", "warmup", "measuring", "cooldown", "finalizing", "cleaning"}
RESOURCE_SETUP_FALLBACK = {
    "baseline": "defaults",
    "optimized": "latency",
}
BUILD_ID_PLACEHOLDER = "__BUS_BUILD_ID__"
ASSET_VERSION_PLACEHOLDER = "__BUS_ASSET_VERSION__"
RUN_EXPORT_KIND = "concerto-bus-runs-export"
RUN_EXPORT_VERSION = 1


def derived_peak_message_rate(message_rate: int, profile_kind: str) -> int:
    base_rate = max(1, int(message_rate))
    if profile_kind == "constant":
        return base_rate
    return max(base_rate, base_rate * 2)


def to_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def runtime_build_id() -> str:
    return str(
        os.environ.get("BUS_BUILD_ID")
        or os.environ.get("BUS_ASSET_VERSION")
        or "dev"
    ).strip() or "dev"


def runtime_asset_version() -> str:
    return str(
        os.environ.get("BUS_ASSET_VERSION")
        or os.environ.get("BUS_BUILD_ID")
        or "dev"
    ).strip() or "dev"


def _normalize_url(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"http://{candidate}"
    if not candidate.endswith("/"):
        candidate = f"{candidate}/"
    return candidate


def runtime_access_url() -> str:
    for env_name in ("BUS_ACCESS_URL", "PUBLIC_URL", "APP_URL"):
        resolved = _normalize_url(os.environ.get(env_name))
        if resolved:
            return resolved

    scheme = "https" if str(os.environ.get("BUS_HTTPS", "")).strip().lower() in {"1", "true", "yes", "on"} else "http"
    host = str(
        os.environ.get("BUS_BIND_HOST")
        or os.environ.get("HOST")
        or "127.0.0.1"
    ).strip() or "127.0.0.1"
    port = str(
        os.environ.get("BUS_PORT")
        or os.environ.get("PORT")
        or "8000"
    ).strip() or "8000"
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    default_port = (scheme == "http" and port == "80") or (scheme == "https" and port == "443")
    suffix = "" if default_port else f":{port}"
    return f"{scheme}://{host}{suffix}/"


def load_presets() -> dict[str, Any]:
    with SCENARIO_CATALOG.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_broker_profiles() -> dict[str, Any]:
    with BROKER_PROFILE_CATALOG.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def parse_transport_options(value: str | dict[str, Any] | None) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_report_run_ids(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []
    ids = parsed.get("ids", [])
    if not isinstance(ids, list):
        return []
    return [str(item) for item in ids if str(item).strip()]


def parse_report_sections(value: str | None) -> list[str]:
    if not value:
        return normalize_report_sections(None)
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return normalize_report_sections(None)
    if isinstance(parsed, dict):
        sections = parsed.get("ids", [])
    else:
        sections = parsed
    if not isinstance(sections, list):
        return normalize_report_sections(None)
    return normalize_report_sections([str(item) for item in sections if str(item).strip()])


def normalize_transport_options(
    value: str | dict[str, Any] | None, *, message_rate: int
) -> dict[str, Any]:
    options = parse_transport_options(value)
    profile_kind = str(options.get("rateProfileKind", "constant")).strip().lower() or "constant"
    if profile_kind not in RATE_PROFILE_KINDS:
        raise HTTPException(status_code=422, detail="Unsupported load profile")

    peak_message_rate = options.get("peakMessageRate")
    if peak_message_rate in {None, "", 0}:
        peak_rate = derived_peak_message_rate(message_rate, profile_kind)
    else:
        try:
            peak_rate = max(1, int(peak_message_rate))
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail="Invalid peak message rate") from None
    try:
        peak_message_rate = int(peak_rate)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid peak message rate") from None

    if profile_kind == "constant":
        peak_rate = max(1, int(message_rate))
    return {
        "rateProfileKind": profile_kind,
        "peakMessageRate": peak_rate,
        "payloadLimitHandling": str(options.get("payloadLimitHandling") or "auto").strip().lower() or "auto",
        "reusePayloadTemplate": bool(options.get("reusePayloadTemplate", False)),
    }


def normalize_scenario_id(value: str | None) -> str | None:
    if value is None:
        return None
    scenario_id = value.strip()
    if not scenario_id:
        return None
    presets = load_presets().get("presets", [])
    known_ids = {preset.get("id") for preset in presets}
    if scenario_id not in known_ids:
        raise HTTPException(status_code=422, detail="Unsupported scenario preset")
    return scenario_id


def normalize_run_name(value: str) -> str:
    name = value.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Run name cannot be empty")
    return name


def resolve_unique_run_name(connection: sqlite3.Connection, requested_name: str) -> str:
    existing_names = {
        str(row["name"] or "").strip().lower()
        for row in connection.execute("SELECT name FROM runs").fetchall()
    }
    if requested_name.lower() not in existing_names:
        return requested_name
    suffix = 2
    while True:
        candidate = f"{requested_name} ({suffix})"
        if candidate.lower() not in existing_names:
            return candidate
        suffix += 1


def normalize_report_title(value: str) -> str:
    title = str(value or "").strip()
    if not title:
        raise HTTPException(status_code=422, detail="Report title cannot be empty")
    return title


def resolve_unique_report_title(connection: sqlite3.Connection, requested_title: str) -> str:
    existing_titles = {
        str(row["title"] or "").strip().lower()
        for row in connection.execute("SELECT title FROM reports").fetchall()
    }
    if requested_title.lower() not in existing_titles:
        return requested_title
    base = requested_title
    while True:
        candidate = f"{base} {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        if candidate.lower() not in existing_titles:
            return candidate
        time.sleep(0.001)


def normalize_config_mode(value: str | None) -> str:
    mode = (value or "baseline").strip().lower()
    if mode not in {"baseline", "optimized"}:
        raise HTTPException(status_code=422, detail="Unsupported config mode")
    return mode


def normalize_deployment_mode(value: str | None) -> str:
    mode = (value or "normal").strip().lower()
    if mode not in {"normal", "ha"}:
        raise HTTPException(status_code=422, detail="Unsupported deployment mode")
    return mode


def normalize_schedule_mode(value: str | None) -> str:
    mode = str(value or "parallel").strip().lower() or "parallel"
    if mode not in SCHEDULE_MODES:
        raise HTTPException(status_code=422, detail="Unsupported schedule mode")
    return mode


def normalize_message_range(start: int | None, end: int | None) -> tuple[int, int]:
    range_start = max(1, int(start or 1))
    range_end = int(end or 0)
    if range_end < 0:
        raise HTTPException(status_code=422, detail="Message range end must be zero or greater")
    if range_end and range_end < range_start:
        raise HTTPException(status_code=422, detail="Message range end must be greater than or equal to the start")
    return range_start, range_end


def _estimate_base64_size(payload_bytes: int) -> int:
    size = max(1, int(payload_bytes))
    return 4 * ((size + 2) // 3)


def estimate_structured_cloudevent_wire_size(payload_bytes: int) -> int:
    payload_bytes = max(1, int(payload_bytes))
    # Structured JSON CloudEvents add field names, metadata, quoting, and the base64 expansion.
    return _estimate_base64_size(payload_bytes) + 960


def _nested_int(mapping: dict[str, Any] | None, *path: str, default: int = 0) -> int:
    current: Any = mapping or {}
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    try:
        return int(current or default)
    except (TypeError, ValueError):
        return default


def apply_payload_limit_policy(
    *,
    broker_id: str,
    message_size_bytes: int,
    broker_tuning: dict[str, Any],
    transport_options: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    tuning = json.loads(json.dumps(broker_tuning or {}))
    options = dict(transport_options or {})
    envelope_bytes = estimate_structured_cloudevent_wire_size(message_size_bytes)
    headroom_limit = int(max(message_size_bytes + 1024, envelope_bytes * 1.10))
    diagnostics = {
        "payloadBytes": int(message_size_bytes),
        "estimatedWireMessageBytes": envelope_bytes,
        "limitHandling": str(options.get("payloadLimitHandling") or "auto"),
    }
    handling = diagnostics["limitHandling"]

    if broker_id == "kafka":
        producer = tuning.setdefault("producer", {})
        broker_values = tuning.setdefault("broker", {})
        producer_limit = _nested_int(tuning, "producer", "maxRequestSizeBytes", default=1_048_576)
        broker_limit = _nested_int(tuning, "broker", "messageMaxBytes", default=1_048_576)
        effective_limit = max(1, min(producer_limit, broker_limit))
        diagnostics["effectivePayloadLimitBytes"] = effective_limit
        if envelope_bytes > effective_limit:
            if handling == "strict":
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Payload size {message_size_bytes} bytes expands to about {envelope_bytes} bytes on wire, "
                        f"which exceeds the current Kafka safe limit of {effective_limit} bytes."
                    ),
                )
            producer["maxRequestSizeBytes"] = max(producer_limit, headroom_limit)
            broker_values["messageMaxBytes"] = max(broker_limit, headroom_limit)
            broker_values["replicaFetchMaxBytes"] = max(
                _nested_int(tuning, "broker", "replicaFetchMaxBytes", default=broker_values.get("messageMaxBytes", headroom_limit)),
                headroom_limit,
            )
            diagnostics["effectivePayloadLimitBytes"] = headroom_limit
            diagnostics["autoAdjusted"] = True
    elif broker_id == "nats":
        broker_values = tuning.setdefault("broker", {})
        current_limit = _nested_int(tuning, "broker", "maxPayload", default=1_048_576)
        diagnostics["effectivePayloadLimitBytes"] = current_limit
        if envelope_bytes > current_limit:
            if handling == "strict":
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Payload size {message_size_bytes} bytes expands to about {envelope_bytes} bytes on wire, "
                        f"which exceeds the current NATS max payload of {current_limit} bytes."
                    ),
                )
            broker_values["maxPayload"] = max(current_limit, headroom_limit)
            diagnostics["effectivePayloadLimitBytes"] = broker_values["maxPayload"]
            diagnostics["autoAdjusted"] = True
    else:
        diagnostics["effectivePayloadLimitBytes"] = None

    if diagnostics.get("autoAdjusted"):
        options["payloadLimitHandling"] = "auto-adjusted"
    options["estimatedWireMessageBytes"] = envelope_bytes
    return tuning, options


def resource_setup_preset(
    *,
    config_mode: str,
    broker_tuning: dict[str, Any] | None = None,
) -> str:
    if isinstance(broker_tuning, dict):
        candidate = str(broker_tuning.get("setupPreset") or "").strip().lower()
        if candidate:
            return candidate
    return RESOURCE_SETUP_FALLBACK.get(config_mode, "defaults")


RESOURCE_CONFIG_STRING_KEYS = (
    "cpuRequest",
    "cpuLimit",
    "memoryRequest",
    "memoryLimit",
    "storageSize",
    "storageClassName",
)


def normalize_resource_config(
    value: dict[str, Any] | None,
    *,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for source in [defaults or {}, value or {}]:
        if not isinstance(source, dict):
            raise HTTPException(status_code=422, detail="Resource config must be an object")
        for key in RESOURCE_CONFIG_STRING_KEYS:
            candidate = source.get(key)
            if candidate in {None, ""}:
                continue
            rendered = str(candidate).strip()
            if rendered:
                merged[key] = rendered
        replicas = source.get("replicas")
        if replicas in {None, ""}:
            continue
        try:
            merged["replicas"] = max(1, int(replicas))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="Resource replicas must be an integer") from exc
    return merged


class BrokerProfileService:
    """Broker-agnostic profile abstraction used by API and UI."""

    def catalog(self) -> dict[str, Any]:
        return load_broker_profiles()

    def profiles(self) -> list[dict[str, Any]]:
        return self.catalog().get("profiles", [])

    def find(self, broker_id: str, config_mode: str) -> dict[str, Any] | None:
        for profile in self.profiles():
            if profile.get("brokerId") == broker_id and profile.get("mode") == config_mode:
                return profile
        return None

    def default_tuning(self, broker_id: str, config_mode: str) -> dict[str, Any]:
        profile = self.find(broker_id, config_mode)
        if profile is None:
            return {}
        return {
            "profileId": profile.get("id"),
            "mode": profile.get("mode"),
            "strategy": profile.get("strategy"),
            "operator": profile.get("operator"),
            "deployment": profile.get("deployment"),
            "clientDefaults": profile.get("clientDefaults"),
            "semanticMapping": profile.get("semanticMapping"),
        }

    def resource_defaults(
        self,
        *,
        broker_id: str,
        config_mode: str,
        deployment_mode: str,
        setup_preset: str | None = None,
    ) -> dict[str, Any]:
        profile = self.find(broker_id, config_mode) or {}
        deployment = profile.get("deployment", {}) or {}
        resources = deployment.get("resources", {}) or {}
        requests = resources.get("requests", {}) or {}
        limits = resources.get("limits", {}) or {}
        storage = deployment.get("storage", {}) or {}
        preset_key = str(setup_preset or resource_setup_preset(config_mode=config_mode)).strip().lower()
        preset = (profile.get("resourcePresets", {}) or {}).get(preset_key, {}) or {}
        cpu_request = str(requests.get("cpu") or limits.get("cpu") or "1").strip() or "1"
        cpu_limit = str(limits.get("cpu") or requests.get("cpu") or cpu_request).strip() or cpu_request
        memory_request = str(requests.get("memory") or limits.get("memory") or "1Gi").strip() or "1Gi"
        memory_limit = str(limits.get("memory") or requests.get("memory") or memory_request).strip() or memory_request
        storage_size = str(storage.get("size") or "20Gi").strip() or "20Gi"
        storage_class_name = str(storage.get("className") or "").strip()
        return {
            "cpuRequest": str(preset.get("cpuRequest") or cpu_request).strip() or cpu_request,
            "cpuLimit": str(preset.get("cpuLimit") or cpu_limit).strip() or cpu_limit,
            "memoryRequest": str(preset.get("memoryRequest") or memory_request).strip() or memory_request,
            "memoryLimit": str(preset.get("memoryLimit") or memory_limit).strip() or memory_limit,
            "storageSize": str(preset.get("storageSize") or storage_size).strip() or storage_size,
            "storageClassName": str(preset.get("storageClassName") or storage_class_name).strip(),
            "replicas": target_replicas_for_mode(deployment_mode),
        }

    def default_resource_config(
        self,
        *,
        broker_id: str,
        config_mode: str,
        deployment_mode: str,
        setup_preset: str | None = None,
    ) -> dict[str, Any]:
        return normalize_resource_config(
            self.resource_defaults(
                broker_id=broker_id,
                config_mode=config_mode,
                deployment_mode=deployment_mode,
                setup_preset=setup_preset,
            )
        )


class RunLifecycleEngine:
    """Single place for run phase math and serialization."""

    def status(self, row: sqlite3.Row, now: datetime) -> str:
        return derive_status(row, now)

    def serialize(self, row: sqlite3.Row, now: datetime) -> dict[str, Any]:
        return serialize_run(row, now)


def derive_status(row: sqlite3.Row, now: datetime) -> str:
    stopped_at = parse_iso(row["stopped_at"])
    topology_ready_at = parse_iso(row["topology_ready_at"])
    execution_started_at = parse_iso(row["execution_started_at"])
    completed_at = parse_iso(row["completed_at"])
    topology_deleted_at = parse_iso(row["topology_deleted_at"])
    starts_at = parse_iso(row["starts_at"])
    queue_state = str(row["queue_state"] or "").strip().lower()

    if stopped_at and topology_deleted_at:
        return "stopped"
    if stopped_at and not topology_deleted_at:
        return "cleaning"
    if queue_state in QUEUE_STATES and topology_ready_at is None and execution_started_at is None:
        return queue_state
    if starts_at is None:
        return "invalid"
    if topology_ready_at is None:
        return "preparing"
    if execution_started_at is None and now < starts_at:
        return "scheduled"
    if execution_started_at is None:
        return "preparing"
    if now < execution_started_at:
        return "preparing"
    if completed_at and not topology_deleted_at:
        return "cleaning"
    if completed_at and topology_deleted_at:
        return "completed"

    warmup_end = execution_started_at + timedelta(seconds=row["warmup_seconds"])
    measure_end = warmup_end + timedelta(seconds=row["measurement_seconds"])
    cooldown_end = measure_end + timedelta(seconds=row["cooldown_seconds"])

    if now < warmup_end:
        return "warmup"
    if now < measure_end:
        return "measuring"
    if now < cooldown_end:
        return "cooldown"
    return "finalizing"


def serialize_run(row: sqlite3.Row, now: datetime) -> dict[str, Any]:
    starts_at = parse_iso(row["starts_at"])
    topology_ready_at = parse_iso(row["topology_ready_at"])
    execution_started_at = parse_iso(row["execution_started_at"])
    completed_at = parse_iso(row["completed_at"])
    topology_deleted_at = parse_iso(row["topology_deleted_at"])
    status = derive_status(row, now)
    reference_time = parse_iso(row["stopped_at"]) or completed_at or now

    raw_mode = row["deployment_mode"] if "deployment_mode" in row.keys() else None
    if raw_mode:
        try:
            deployment_mode = normalize_deployment_mode(raw_mode)
        except HTTPException:
            deployment_mode = "ha" if bool(row["ha_mode"]) else "normal"
    else:
        deployment_mode = "ha" if bool(row["ha_mode"]) else "normal"
    resource_config = parse_metrics(row["resource_config_json"]) if row["resource_config_json"] else {}
    metrics = parse_metrics(row["metrics_json"])
    target_replicas = int(
        resource_config.get("replicas") or target_replicas_for_mode(deployment_mode)
    )
    diagnostics: list[dict[str, Any]] = []
    if metrics.get("source") == "benchmark-agent":
        summary = metrics.get("summary") or {}
        latency_summary = summary.get("endToEndLatencyMs") or {}
        latency_count = float(latency_summary.get("count") or 0.0)
        if latency_count <= 0.0:
            diagnostics.append(
                {
                    "code": "zero-samples",
                    "severity": "warning",
                    "message": "Completed without latency samples. Inspect delivery diagnostics and raw artifacts.",
                }
            )
        success_rate = float(summary.get("deliverySuccessRate") or 0.0)
        if success_rate < 100.0:
            diagnostics.append(
                {
                    "code": "degraded",
                    "severity": "warning",
                    "message": f"Delivery success is {success_rate:.2f}%. The run completed in degraded mode.",
                }
            )
        post_measurement_failure = str((metrics.get("measurement") or {}).get("postMeasurementFailure") or "").strip()
        if post_measurement_failure:
            diagnostics.append(
                {
                    "code": "late-job-failure",
                    "severity": "warning",
                    "message": post_measurement_failure,
                }
            )

    warmup_seconds = max(0, int(row["warmup_seconds"]))
    measurement_seconds = max(0, int(row["measurement_seconds"]))
    cooldown_seconds = max(0, int(row["cooldown_seconds"]))
    active_window_seconds = max(1, warmup_seconds + measurement_seconds + cooldown_seconds)
    if execution_started_at is not None:
        active_elapsed_seconds = max(
            0, int((reference_time - execution_started_at).total_seconds())
        )
    else:
        active_elapsed_seconds = 0

    deployment_progress = 100.0 if topology_ready_at else 5.0
    run_progress = (
        min(
            100.0 if completed_at else 99.0,
            round((active_elapsed_seconds / active_window_seconds) * 100.0, 2),
        )
        if execution_started_at
        else 0.0
    )

    if execution_started_at is not None:
        warmup_end = execution_started_at + timedelta(seconds=warmup_seconds)
        measure_end = warmup_end + timedelta(seconds=measurement_seconds)
        cooldown_end = measure_end + timedelta(seconds=cooldown_seconds)
    else:
        warmup_end = None
        measure_end = None
        cooldown_end = None

    return {
        "id": row["id"],
        "name": row["name"],
        "runtimeNamespace": run_namespace_for(row["id"]),
        "brokerId": row["broker_id"],
        "protocol": row["protocol"],
        "scenarioId": row["scenario_id"],
        "configMode": row["config_mode"],
        "deploymentMode": deployment_mode,
        "scheduleMode": str(row["schedule_mode"] or "parallel"),
        "queueState": str(row["queue_state"] or "").strip() or None,
        "targetReplicas": target_replicas,
        "startsAt": row["starts_at"],
        "warmupSeconds": row["warmup_seconds"],
        "measurementSeconds": row["measurement_seconds"],
        "cooldownSeconds": row["cooldown_seconds"],
        "messageRate": row["message_rate"],
        "messageSizeBytes": row["message_size_bytes"],
        "messageRangeStart": int(row["message_range_start"] or 1),
        "messageRangeEnd": int(row["message_range_end"] or 0),
        "producers": row["producers"],
        "consumers": row["consumers"],
        "transportOptions": parse_transport_options(row["transport_options"]),
        "brokerTuning": parse_metrics(row["broker_tuning_json"]),
        "metrics": metrics,
        "resourceConfig": resource_config,
        "diagnostics": diagnostics,
        "topologyReadyAt": to_iso(topology_ready_at),
        "executionStartedAt": to_iso(execution_started_at),
        "completedAt": to_iso(completed_at),
        "topologyDeletedAt": to_iso(topology_deleted_at),
        "createdAt": row["created_at"],
        "stoppedAt": row["stopped_at"],
        "status": status,
        "progress": {
            "deploymentPercent": deployment_progress,
            "runPercent": run_progress,
            "elapsedSeconds": active_elapsed_seconds,
            "totalSeconds": active_window_seconds,
        },
        "phaseWindow": {
            "prepareEnd": to_iso(topology_ready_at),
            "warmupEnd": to_iso(warmup_end),
            "measurementEnd": to_iso(measure_end),
            "cooldownEnd": to_iso(cooldown_end),
        },
    }


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(min_length=1, max_length=120)
    brokerId: str = Field(default="kafka")
    protocol: str | None = None
    scenarioId: str | None = None
    configMode: str = Field(default="baseline")
    deploymentMode: str = Field(default="normal")
    scheduleMode: str = Field(default="parallel")
    startsAt: datetime | None = None
    warmupSeconds: int = Field(default=10, ge=0, le=3600)
    measurementSeconds: int = Field(default=60, ge=1, le=86400)
    cooldownSeconds: int = Field(default=10, ge=0, le=3600)
    messageRate: int = Field(default=5000, ge=1, le=10000000)
    messageSizeBytes: int = Field(default=1024, ge=1, le=100000000)
    messageRangeStart: int = Field(default=1, ge=1, le=2000000000)
    messageRangeEnd: int = Field(default=0, ge=0, le=2000000000)
    producers: int = Field(default=1, ge=1, le=2000)
    consumers: int = Field(default=1, ge=1, le=2000)
    transportOptions: str | dict[str, Any] | None = None
    brokerTuning: dict[str, Any] | None = None
    resourceConfig: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_resource_config(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "resourceConfig" not in value and "resource_config" in value:
            normalized = dict(value)
            normalized["resourceConfig"] = normalized["resource_config"]
            return normalized
        return value


class StopRunResponse(BaseModel):
    id: str
    status: str
    stoppedAt: str


class CreateReportRequest(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    runIds: list[str] = Field(min_length=1, max_length=24)
    sections: list[str] | None = None


class ExportRunsRequest(BaseModel):
    runIds: list[str] | None = Field(default=None, max_length=256)


class ImportRunsRequest(BaseModel):
    kind: str | None = None
    version: int | None = None
    exportedAt: str | None = None
    sourceBuildId: str | None = None
    sourceAccessUrl: str | None = None
    runs: list[dict[str, Any]] = Field(default_factory=list)


class BootstrapRequest(BaseModel):
    scope: str = Field(default="brokers")


def create_app(
    db_path: Path | None = None,
    *,
    cluster_automation: ClusterAutomation | None = None,
) -> FastAPI:
    cluster_automation = cluster_automation or ClusterAutomation(REPO_ROOT)
    runtime_storage = resolve_runtime_storage(db_path=db_path)
    report_dir = runtime_storage.report_dir
    build_id = runtime_build_id()
    asset_version = runtime_asset_version()
    access_url = runtime_access_url()
    index_template = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    rendered_index = (
        index_template.replace(BUILD_ID_PLACEHOLDER, html.escape(build_id, quote=True))
        .replace(ASSET_VERSION_PLACEHOLDER, html.escape(asset_version, quote=True))
    )
    profile_service = BrokerProfileService()
    lifecycle_engine = RunLifecycleEngine()
    runtime_init_lock = threading.Lock()
    scheduler_lock = threading.Lock()
    runtime_state: dict[str, Any] = {
        "ready": False,
        "initializing": False,
        "error": None,
    }

    def storage_status_snapshot() -> dict[str, Any]:
        with runtime_init_lock:
            return {
                "selectedMode": runtime_storage.mode,
                "ready": bool(runtime_state["ready"]),
                "initializing": bool(runtime_state["initializing"]),
                "message": str(runtime_state["error"] or ("ready" if runtime_state["ready"] else "pending")),
            }

    def initialize_runtime_storage() -> None:
        with runtime_init_lock:
            if runtime_state["ready"] or runtime_state["initializing"]:
                return
            runtime_state["initializing"] = True
            runtime_state["error"] = None
        try:
            runtime_storage.ensure_ready()
            with runtime_init_lock:
                runtime_state["ready"] = True
            repair_report_artifacts()
            if cluster_automation.enabled:
                recover_interrupted_runs()
                reconcile_run_namespaces()
                kick_sequential_scheduler()
            with runtime_init_lock:
                runtime_state["initializing"] = False
                runtime_state["error"] = None
        except Exception as exc:
            with runtime_init_lock:
                runtime_state["ready"] = False
                runtime_state["initializing"] = False
                runtime_state["error"] = str(exc)

    def require_runtime_storage_ready() -> None:
        snapshot = storage_status_snapshot()
        if snapshot["ready"]:
            return
        if snapshot["initializing"]:
            raise HTTPException(status_code=503, detail="Runtime storage is initializing.")
        raise HTTPException(
            status_code=503,
            detail=f"Runtime storage is not ready: {snapshot['message']}",
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> Any:
        print(f"Concerto Bus Benchmark address: {access_url}", flush=True)
        if runtime_storage.mode == "postgres+s3" and db_path is None:
            threading.Thread(
                target=initialize_runtime_storage,
                name="runtime-storage-init",
                daemon=True,
            ).start()
        else:
            initialize_runtime_storage()
        yield

    app = FastAPI(title="Concerto Bus Benchmark", lifespan=lifespan)
    app.state.cluster_automation = cluster_automation
    app.state.lifecycle_engine = lifecycle_engine
    app.state.build_id = build_id
    app.state.asset_version = asset_version
    app.state.access_url = access_url
    app.state.runtime_storage = runtime_storage
    app.state.storage_mode = runtime_storage.mode
    app.state.storage_status = storage_status_snapshot
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.middleware("http")
    async def disable_cache(request: Any, call_next: Any) -> Response:
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @contextmanager
    def storage_db() -> Any:
        with runtime_storage.db() as connection:
            yield connection

    @contextmanager
    def db() -> Any:
        require_runtime_storage_ready()
        with storage_db() as connection:
            yield connection

    def record_event(
        connection: sqlite3.Connection, run_id: str | None, event_type: str, message: str
    ) -> None:
        connection.execute(
            """
            INSERT INTO run_events (id, run_id, event_type, message, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), run_id, event_type, message, utc_now().isoformat()),
        )

    def record_event_now(run_id: str | None, event_type: str, message: str) -> None:
        with db() as connection:
            record_event(connection, run_id, event_type, message)

    def get_row(run_id: str) -> sqlite3.Row | None:
        with db() as connection:
            return connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()

    def get_report_row(report_id: str) -> sqlite3.Row | None:
        with db() as connection:
            return connection.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()

    def get_run_artifact_rows(run_id: str) -> list[sqlite3.Row]:
        with db() as connection:
            return connection.execute(
                "SELECT * FROM run_artifacts WHERE run_id = ? ORDER BY created_at ASC",
                (run_id,),
            ).fetchall()

    def set_run_field(run_id: str, field_name: str, value: Any) -> None:
        with db() as connection:
            connection.execute(
                f"UPDATE runs SET {field_name} = ? WHERE id = ?",
                (value, run_id),
            )

    def active_execution_exists(*, exclude_run_id: str | None = None) -> bool:
        now = utc_now()
        with db() as connection:
            rows = connection.execute("SELECT * FROM runs ORDER BY created_at DESC").fetchall()
        return any(
            str(row["id"]) != str(exclude_run_id or "")
            and lifecycle_engine.status(row, now) in ACTIVE_EXECUTION_STATUSES
            for row in rows
        )

    def queued_run_rows(connection: Any) -> list[Any]:
        return connection.execute(
            """
            SELECT *
            FROM runs
            WHERE stopped_at IS NULL
              AND completed_at IS NULL
              AND topology_ready_at IS NULL
              AND execution_started_at IS NULL
              AND queue_state IS NOT NULL
            ORDER BY created_at ASC, starts_at ASC, name ASC
            """
        ).fetchall()

    def reconcile_queue_states(connection: Any) -> None:
        queued_rows = queued_run_rows(connection)
        has_active_execution = active_execution_exists()
        for index, row in enumerate(queued_rows):
            desired_state = "waiting"
            if not has_active_execution and index == 0:
                desired_state = "queued"
            current_state = str(row["queue_state"] or "").strip().lower()
            if current_state == desired_state:
                continue
            connection.execute(
                "UPDATE runs SET queue_state = ? WHERE id = ?",
                (desired_state, row["id"]),
            )
            if desired_state == "queued":
                record_event(
                    connection,
                    str(row["id"]),
                    "queued",
                    "Queued run is now first in line and will launch when the control plane is idle.",
                )
            else:
                record_event(
                    connection,
                    str(row["id"]),
                    "waiting",
                    "Queued behind another run. Waiting for the previous benchmark to complete.",
                )

    def kick_sequential_scheduler() -> None:
        if active_execution_exists():
            with db() as connection:
                reconcile_queue_states(connection)
            return

        def runner() -> None:
            with scheduler_lock:
                if active_execution_exists():
                    with db() as connection:
                        reconcile_queue_states(connection)
                    return
                selected_run_id: str | None = None
                with db() as connection:
                    reconcile_queue_states(connection)
                    selected = connection.execute(
                        """
                        SELECT *
                        FROM runs
                        WHERE queue_state = 'queued'
                          AND stopped_at IS NULL
                          AND completed_at IS NULL
                          AND topology_ready_at IS NULL
                          AND execution_started_at IS NULL
                        ORDER BY created_at ASC, starts_at ASC, name ASC
                        LIMIT 1
                        """
                    ).fetchone()
                    if selected is None:
                        return
                    selected_run_id = str(selected["id"])
                    connection.execute(
                        "UPDATE runs SET queue_state = NULL WHERE id = ?",
                        (selected_run_id,),
                    )
                    record_event(
                        connection,
                        selected_run_id,
                        "queue-dispatched",
                        "Run left the sequential queue and is entering topology preparation.",
                    )
                if selected_run_id:
                    launch_run_preparation(selected_run_id)

        threading.Thread(
            target=runner,
            name="sequential-run-scheduler",
            daemon=True,
        ).start()

    def run_is_measured(run: dict[str, Any]) -> bool:
        metrics = run.get("metrics") or {}
        return run.get("status") == "completed" and metrics.get("source") == "benchmark-agent"

    def serialize_report(row: sqlite3.Row) -> dict[str, Any]:
        has_download = bool(str(row["file_path"] or "").strip() or str(row["object_key"] or "").strip())
        return {
            "id": row["id"],
            "title": row["title"],
            "runIds": parse_report_run_ids(row["run_ids_json"]),
            "sections": parse_report_sections(row["sections_json"]),
            "status": row["status"],
            "fileName": row["file_name"],
            "sizeBytes": int(row["size_bytes"] or 0),
            "errorMessage": row["error_message"],
            "createdAt": row["created_at"],
            "completedAt": row["completed_at"],
            "downloadUrl": f"/api/reports/{row['id']}/download" if has_download else None,
        }

    def serialize_run_artifact(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "runId": row["run_id"],
            "artifactType": row["artifact_type"],
            "fileName": row["file_name"],
            "fileFormat": row["file_format"],
            "recordCount": int(row["record_count"] or 0),
            "sizeBytes": int(row["size_bytes"] or 0),
            "createdAt": row["created_at"],
            "downloadUrl": f"/api/runs/{row['run_id']}/artifacts/{row['id']}/download",
        }

    def serialize_export_run(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "brokerId": row["broker_id"],
            "protocol": row["protocol"],
            "scenarioId": row["scenario_id"],
            "configMode": row["config_mode"],
            "deploymentMode": row["deployment_mode"],
            "scheduleMode": row["schedule_mode"],
            "startsAt": row["starts_at"],
            "warmupSeconds": int(row["warmup_seconds"] or 0),
            "measurementSeconds": int(row["measurement_seconds"] or 0),
            "cooldownSeconds": int(row["cooldown_seconds"] or 0),
            "messageRate": int(row["message_rate"] or 0),
            "messageSizeBytes": int(row["message_size_bytes"] or 0),
            "messageRangeStart": int(row["message_range_start"] or 1),
            "messageRangeEnd": int(row["message_range_end"] or 0),
            "producers": int(row["producers"] or 0),
            "consumers": int(row["consumers"] or 0),
            "transportOptions": parse_transport_options(row["transport_options"]),
            "haMode": bool(row["ha_mode"]),
            "brokerTuning": parse_metrics(row["broker_tuning_json"]),
            "metrics": parse_metrics(row["metrics_json"]),
            "resourceConfig": parse_metrics(row["resource_config_json"]),
            "topologyReadyAt": row["topology_ready_at"],
            "executionStartedAt": row["execution_started_at"],
            "completedAt": row["completed_at"],
            "topologyDeletedAt": row["topology_deleted_at"],
            "createdAt": row["created_at"],
            "stoppedAt": row["stopped_at"],
        }

    def report_sections_json(sections: list[str] | None) -> str:
        return json.dumps({"ids": normalize_report_sections(sections)})

    def build_report_file_name(title: str, created_at: datetime | str | None) -> str:
        if isinstance(created_at, datetime):
            created_value = created_at
        else:
            created_value = parse_iso(created_at) if created_at else None
        created_label = (created_value or utc_now()).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{slugify_file_name(title)}-{created_label}.pdf"

    def load_report_runs(run_ids: list[str], *, now: datetime) -> list[dict[str, Any]]:
        if not run_ids:
            raise HTTPException(status_code=422, detail="At least one run is required")

        with db() as connection:
            rows = connection.execute(
                "SELECT * FROM runs WHERE id IN ({})".format(",".join("?" for _ in run_ids)),
                tuple(run_ids),
            ).fetchall()

        row_by_id = {str(row["id"]): row for row in rows}
        missing = [run_id for run_id in run_ids if run_id not in row_by_id]
        if missing:
            raise HTTPException(status_code=404, detail=f"Run not found: {missing[0]}")

        serialized_runs = [lifecycle_engine.serialize(row_by_id[run_id], now) for run_id in run_ids]
        invalid_run = next((run for run in serialized_runs if not run_is_measured(run)), None)
        if invalid_run is not None:
            raise HTTPException(
                status_code=422,
                detail=f"Run {invalid_run['name']} does not have completed benchmark results",
            )
        return serialized_runs

    def regenerate_report_artifact(
        report_row: sqlite3.Row,
        *,
        failure_prefix: str = "Report generation failed",
    ) -> sqlite3.Row:
        report_id = str(report_row["id"])
        title = str(report_row["title"] or "").strip() or "Concerto Bus Benchmark Report"
        run_ids = parse_report_run_ids(report_row["run_ids_json"])
        sections = parse_report_sections(report_row["sections_json"])
        file_name = str(report_row["file_name"] or "").strip() or build_report_file_name(title, report_row["created_at"])
        file_path = report_dir / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        now = utc_now()
        serialized_runs = load_report_runs(run_ids, now=now)

        with db() as connection:
            connection.execute(
                """
                UPDATE reports
                SET status = ?, file_name = ?, file_path = ?, storage_backend = ?, object_key = ?, error_message = ?, completed_at = ?
                WHERE id = ?
                """,
                ("rendering", file_name, str(file_path), "local", None, None, None, report_id),
            )

        try:
            write_report_pdf(
                output_path=file_path,
                title=title,
                runs=serialized_runs,
                generated_at=now,
                sections=sections,
            )
            stored = runtime_storage.put_report_file(report_id, file_name, file_path)
            size_bytes = int(stored["sizeBytes"] or 0)
        except Exception as exc:
            with db() as connection:
                connection.execute(
                    "UPDATE reports SET status = ?, error_message = ?, completed_at = ? WHERE id = ?",
                    ("failed", f"{failure_prefix}: {exc}", utc_now().isoformat(), report_id),
                )
                row = connection.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
            raise HTTPException(status_code=500, detail=f"{failure_prefix}: {exc}") from exc

        with db() as connection:
            connection.execute(
                """
                UPDATE reports
                SET status = ?, file_name = ?, file_path = ?, storage_backend = ?, object_key = ?, size_bytes = ?, error_message = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    "ready",
                    file_name,
                    str(stored["filePath"] or ""),
                    str(stored["storageBackend"] or "local"),
                    stored["objectKey"],
                    size_bytes,
                    None,
                    utc_now().isoformat(),
                    report_id,
                ),
            )
            row = connection.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
        if runtime_storage.mode != "local":
            file_path.unlink(missing_ok=True)
        return row

    def repair_report_artifacts() -> None:
        with storage_db() as connection:
            rows = connection.execute("SELECT * FROM reports ORDER BY created_at ASC").fetchall()
        for row in rows:
            if row["status"] == "ready" and runtime_storage.row_exists(row):
                continue
            try:
                regenerate_report_artifact(
                    row,
                    failure_prefix="Report recovery failed",
                )
            except HTTPException:
                continue

    def build_run_export_entry(row: sqlite3.Row) -> dict[str, Any]:
        run_id = str(row["id"])
        with db() as connection:
            events = connection.execute(
                """
                SELECT event_type, message, created_at
                FROM run_events
                WHERE run_id = ?
                ORDER BY created_at ASC
                """,
                (run_id,),
            ).fetchall()
            artifact_rows = connection.execute(
                "SELECT * FROM run_artifacts WHERE run_id = ? ORDER BY created_at ASC",
                (run_id,),
            ).fetchall()

        artifacts: list[dict[str, Any]] = []
        for artifact_row in artifact_rows:
            if not runtime_storage.row_exists(artifact_row):
                continue
            artifacts.append(
                {
                    "artifactType": artifact_row["artifact_type"],
                    "fileName": artifact_row["file_name"],
                    "fileFormat": artifact_row["file_format"],
                    "recordCount": int(artifact_row["record_count"] or 0),
                    "createdAt": artifact_row["created_at"],
                    "contentBase64": base64.b64encode(runtime_storage.row_bytes(artifact_row)).decode("ascii"),
                }
            )

        return {
            "run": serialize_export_run(row),
            "events": [
                {
                    "eventType": event["event_type"],
                    "message": event["message"],
                    "createdAt": event["created_at"],
                }
                for event in events
            ],
            "artifacts": artifacts,
        }

    def import_run_bundle(payload: ImportRunsRequest) -> dict[str, Any]:
        if payload.kind != RUN_EXPORT_KIND or payload.version != RUN_EXPORT_VERSION:
            raise HTTPException(status_code=422, detail="Unsupported run export format")
        if not payload.runs:
            raise HTTPException(status_code=422, detail="No runs found in import bundle")
        imported_runs: list[dict[str, str]] = []
        imported_at = utc_now().isoformat()

        with db() as connection:
            for item in payload.runs:
                if not isinstance(item, dict):
                    raise HTTPException(status_code=422, detail="Invalid run entry in import bundle")
                run_data = item.get("run")
                if not isinstance(run_data, dict):
                    raise HTTPException(status_code=422, detail="Import bundle entry is missing run data")

                requested_name = normalize_run_name(str(run_data.get("name") or ""))
                run_id = str(uuid.uuid4())
                run_name = resolve_unique_run_name(connection, requested_name)
                broker_id = str(run_data.get("brokerId") or "kafka").strip().lower() or "kafka"
                protocol = normalize_protocol_for_broker(broker_id, run_data.get("protocol"))
                config_mode = normalize_config_mode(run_data.get("configMode"))
                deployment_mode = normalize_deployment_mode(run_data.get("deploymentMode"))
                starts_at = str(run_data.get("startsAt") or imported_at)
                raw_completed_at = str(run_data.get("completedAt") or "").strip() or None
                raw_stopped_at = str(run_data.get("stoppedAt") or "").strip() or None
                completed_at = raw_completed_at or raw_stopped_at or imported_at
                stopped_at = raw_stopped_at if raw_stopped_at else (None if raw_completed_at else completed_at)
                topology_deleted_at = str(run_data.get("topologyDeletedAt") or completed_at or stopped_at or imported_at)
                transport_options = normalize_transport_options(
                    run_data.get("transportOptions"),
                    message_rate=int(run_data.get("messageRate") or 1),
                )
                schedule_mode = normalize_schedule_mode(run_data.get("scheduleMode"))
                message_range_start, message_range_end = normalize_message_range(
                    int(run_data.get("messageRangeStart") or 1),
                    int(run_data.get("messageRangeEnd") or 0),
                )
                broker_tuning = sanitize_broker_tuning(
                    broker_id,
                    run_data.get("brokerTuning") if isinstance(run_data.get("brokerTuning"), dict) else {},
                )
                resource_config = normalize_resource_config(
                    run_data.get("resourceConfig") if isinstance(run_data.get("resourceConfig"), dict) else {}
                )
                metrics_payload = run_data.get("metrics") if isinstance(run_data.get("metrics"), dict) else {}

                scenario_id = run_data.get("scenarioId")
                normalized_scenario_id = (
                    str(scenario_id).strip() if scenario_id not in {None, ""} else None
                )

                connection.execute(
                    """
                    INSERT INTO runs (
                      id, name, broker_id, protocol, scenario_id, config_mode, deployment_mode, schedule_mode, queue_state, starts_at,
                      warmup_seconds, measurement_seconds, cooldown_seconds, message_rate,
                      message_size_bytes, message_range_start, message_range_end, producers, consumers,
                      transport_options, ha_mode, broker_tuning_json,
                      metrics_json, resource_config_json, topology_ready_at, execution_started_at,
                      completed_at, topology_deleted_at, created_at, stopped_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        run_name,
                        broker_id,
                        protocol,
                        normalized_scenario_id,
                        config_mode,
                        deployment_mode,
                        schedule_mode,
                        None,
                        starts_at,
                        max(0, int(run_data.get("warmupSeconds") or 0)),
                        max(1, int(run_data.get("measurementSeconds") or 1)),
                        max(0, int(run_data.get("cooldownSeconds") or 0)),
                        max(1, int(run_data.get("messageRate") or 1)),
                        max(1, int(run_data.get("messageSizeBytes") or 1)),
                        message_range_start,
                        message_range_end,
                        max(1, int(run_data.get("producers") or 1)),
                        max(1, int(run_data.get("consumers") or 1)),
                        json.dumps(transport_options, sort_keys=True),
                        1 if bool(run_data.get("haMode")) else 0,
                        json.dumps(broker_tuning),
                        json.dumps(metrics_payload),
                        json.dumps(resource_config),
                        str(run_data.get("topologyReadyAt") or completed_at or imported_at),
                        str(run_data.get("executionStartedAt") or completed_at or imported_at),
                        completed_at,
                        topology_deleted_at,
                        str(run_data.get("createdAt") or imported_at),
                        stopped_at,
                    ),
                )

                for event in item.get("events", []) or []:
                    if not isinstance(event, dict):
                        continue
                    connection.execute(
                        """
                        INSERT INTO run_events (id, run_id, event_type, message, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            run_id,
                            str(event.get("eventType") or "imported"),
                            str(event.get("message") or ""),
                            str(event.get("createdAt") or imported_at),
                        ),
                    )

                connection.execute(
                    """
                    INSERT INTO run_events (id, run_id, event_type, message, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        run_id,
                        "imported",
                        "Run imported from a results export bundle.",
                        imported_at,
                    ),
                )

                artifact_items = item.get("artifacts", []) or []
                for artifact in artifact_items:
                    if not isinstance(artifact, dict):
                        continue
                    content_base64 = str(artifact.get("contentBase64") or "").strip()
                    if not content_base64:
                        continue
                    try:
                        content_bytes = base64.b64decode(content_base64.encode("ascii"))
                    except Exception as exc:
                        raise HTTPException(status_code=422, detail="Invalid artifact payload in import bundle") from exc
                    file_name = Path(str(artifact.get("fileName") or "artifact.bin")).name or "artifact.bin"
                    persist_run_artifact(
                        connection,
                        runtime_storage,
                        run_id=run_id,
                        artifact_type=str(artifact.get("artifactType") or "imported-artifact"),
                        file_name=file_name,
                        file_path=None,
                        file_format=str(artifact.get("fileFormat") or "binary"),
                        record_count=max(0, int(artifact.get("recordCount") or 0)),
                        content_bytes=content_bytes,
                    )

                imported_runs.append({"id": run_id, "name": run_name})

        return {
            "status": "imported",
            "importedRuns": imported_runs,
            "count": len(imported_runs),
        }

    def delete_report_artifacts(report_rows: list[sqlite3.Row]) -> None:
        for report_row in report_rows:
            try:
                runtime_storage.delete_row_object(report_row)
            except OSError:
                continue

    def delete_run_artifacts(artifact_rows: list[sqlite3.Row]) -> None:
        for artifact_row in artifact_rows:
            try:
                runtime_storage.delete_row_object(artifact_row)
            except OSError:
                continue

    def run_stop_requested(run_id: str) -> bool:
        row = get_row(run_id)
        return row is None or bool(row["stopped_at"])

    def wait_with_stop(run_id: str, seconds: int) -> bool:
        deadline = utc_now() + timedelta(seconds=max(0, seconds))
        while utc_now() < deadline:
            if run_stop_requested(run_id):
                return False
            remaining = (deadline - utc_now()).total_seconds()
            threading.Event().wait(min(0.25, max(0.0, remaining)))
        return not run_stop_requested(run_id)

    def wait_until_ns_with_stop(run_id: str, target_ns: int) -> bool:
        while time.time_ns() < target_ns:
            if run_stop_requested(run_id):
                return False
            remaining_seconds = max(0.0, (target_ns - time.time_ns()) / 1_000_000_000.0)
            threading.Event().wait(min(0.25, remaining_seconds))
        return not run_stop_requested(run_id)

    def finalizer_due_at(row: sqlite3.Row) -> datetime | None:
        execution_started_at = parse_iso(row["execution_started_at"])
        if execution_started_at is None:
            return None
        warmup_seconds = max(0, int(row["warmup_seconds"] or 0))
        measurement_seconds = max(1, int(row["measurement_seconds"] or 1))
        cooldown_seconds = max(0, int(row["cooldown_seconds"] or 0))
        return execution_started_at + timedelta(
            seconds=warmup_seconds + measurement_seconds + cooldown_seconds
        )

    def finalizer_launch_due(row: sqlite3.Row, now: datetime | None = None) -> bool:
        if row["completed_at"] or row["stopped_at"]:
            return False
        due_at = finalizer_due_at(row)
        if due_at is None:
            return False
        return (now or utc_now()) >= due_at

    def cleanup_run_topology(run_id: str, reason: str = "cleanup") -> None:
        row = get_row(run_id)
        if row is None or row["topology_deleted_at"]:
            return
        if not all(
            hasattr(cluster_automation, name)
            for name in ("delete_run_namespace", "wait_for_namespace_deleted")
        ):
            return
        record_event_now(run_id, "cleanup-started", f"Destroying topology for {reason}.")
        if hasattr(cluster_automation, "delete_benchmark_jobs"):
            cluster_automation.delete_benchmark_jobs(
                run_id,
                producers=int(row["producers"] or 1),
                consumers=int(row["consumers"] or 1),
            )
        cluster_automation.delete_run_namespace(run_id)
        deleted = cluster_automation.wait_for_namespace_deleted(run_id)
        if hasattr(cluster_automation, "delete_run_structured_output"):
            cluster_automation.delete_run_structured_output(run_id)
        with db() as connection:
            if deleted:
                connection.execute(
                    "UPDATE runs SET topology_deleted_at = ? WHERE id = ? AND topology_deleted_at IS NULL",
                    (utc_now().isoformat(), run_id),
                )
            record_event(
                connection,
                run_id,
                "topology-destroyed",
                "Topology removed. Operators remain installed."
                if deleted
                else "Topology deletion requested. Namespace is still terminating.",
            )
            reconcile_queue_states(connection)
        kick_sequential_scheduler()

    def reconcile_topology_deletions() -> None:
        if not hasattr(cluster_automation, "namespace_exists"):
            return
        now = utc_now()
        with db() as connection:
            rows = connection.execute(
                "SELECT * FROM runs WHERE topology_deleted_at IS NULL ORDER BY created_at ASC"
            ).fetchall()
        for row in rows:
            status = lifecycle_engine.status(row, now)
            if status != "cleaning":
                continue
            run_id = str(row["id"])
            if cluster_automation.namespace_exists(run_id):
                completed_at = parse_iso(row["completed_at"])
                stopped_at = parse_iso(row["stopped_at"])
                cleanup_started_at = completed_at or stopped_at
                cleanup_age_seconds = (
                    (now - cleanup_started_at).total_seconds() if cleanup_started_at is not None else 0.0
                )
                if cleanup_age_seconds < 90:
                    continue
                if hasattr(cluster_automation, "force_delete_namespace"):
                    cluster_automation.force_delete_namespace(run_id)
                elif hasattr(cluster_automation, "wait_for_namespace_deleted"):
                    cluster_automation.wait_for_namespace_deleted(run_id, timeout_seconds=10)
                if cluster_automation.namespace_exists(run_id):
                    continue
            with db() as connection:
                current = connection.execute(
                    "SELECT topology_deleted_at FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if current is None or current["topology_deleted_at"]:
                    continue
                connection.execute(
                    "UPDATE runs SET topology_deleted_at = ? WHERE id = ?",
                    (utc_now().isoformat(), run_id),
                )
                record_event(
                    connection,
                    run_id,
                    "topology-destroyed",
                    "Topology removed. Operators remain installed.",
                )

    def reconcile_detached_finalizers() -> None:
        if not hasattr(cluster_automation, "run_finalizer_state") or not hasattr(cluster_automation, "namespace_exists"):
            return
        now = utc_now()
        with db() as connection:
            rows = connection.execute(
                """
                SELECT * FROM runs
                WHERE completed_at IS NULL AND stopped_at IS NULL AND execution_started_at IS NOT NULL
                ORDER BY created_at ASC
                """
            ).fetchall()
        for row in rows:
            status = lifecycle_engine.status(row, now)
            if status not in {"warmup", "measuring", "cooldown", "finalizing", "cleaning"}:
                continue
            if not finalizer_launch_due(row, now):
                continue
            run_id = str(row["id"])
            namespace_present = cluster_automation.namespace_exists(run_id)
            if not namespace_present:
                continue
            finalizer_state = cluster_automation.run_finalizer_state(run_id)
            if finalizer_state == "running":
                continue
            if finalizer_state == "not_found" and hasattr(cluster_automation, "launch_run_finalizer"):
                cluster_automation.launch_run_finalizer(run_id)
                with db() as connection:
                    record_event(
                        connection,
                        run_id,
                        "finalizer-resumed",
                        "Detached run finalizer was missing or stale and has been relaunched.",
                    )

    def recover_interrupted_runs() -> None:
        if not hasattr(cluster_automation, "namespace_exists"):
            return
        now = utc_now()
        with storage_db() as connection:
            rows = connection.execute("SELECT * FROM runs ORDER BY created_at ASC").fetchall()
        recoverable_statuses = {"scheduled", "preparing", "warmup", "measuring", "cooldown", "finalizing", "cleaning"}
        for row in rows:
            status = lifecycle_engine.status(row, now)
            if status not in recoverable_statuses:
                continue
            run_id = str(row["id"])
            was_stopped = bool(row["stopped_at"])
            execution_started_at = parse_iso(row["execution_started_at"])
            finalizer_state = (
                cluster_automation.run_finalizer_state(run_id)
                if hasattr(cluster_automation, "run_finalizer_state")
                else "not_found"
            )
            namespace_present = cluster_automation.namespace_exists(run_id)
            finalizer_due = finalizer_launch_due(row, now)
            if was_stopped:
                with storage_db() as connection:
                    record_event(
                        connection,
                        run_id,
                        "cleanup-resumed",
                        "Platform restarted while topology cleanup was still in progress. Cleanup resumed.",
                    )
                cleanup_run_topology(run_id, "runtime recovery")
                continue
            if execution_started_at is not None and namespace_present:
                if finalizer_due and finalizer_state in {"not_found", "failed", "succeeded"} and hasattr(cluster_automation, "launch_run_finalizer"):
                    cluster_automation.launch_run_finalizer(run_id)
                    with storage_db() as connection:
                        record_event(
                            connection,
                            run_id,
                            "finalizer-resumed",
                            "Platform restarted and relaunched the detached run finalizer.",
                        )
                else:
                    with storage_db() as connection:
                        record_event(
                            connection,
                            run_id,
                            "runtime-resumed",
                            "Platform restarted while the run was active. Benchmark execution continues and detached finalization will resume when due."
                            if not finalizer_due
                            else "Platform restarted while detached finalization was active. Finalization continues independently.",
                        )
                continue
            with storage_db() as connection:
                connection.execute(
                    "UPDATE runs SET stopped_at = ? WHERE id = ? AND stopped_at IS NULL",
                    (utc_now().isoformat(), run_id),
                )
                record_event(
                    connection,
                    run_id,
                    "runtime-recovered",
                    "Platform restarted before the run could finish. The run was marked interrupted and cleanup resumed.",
                )
            cleanup_run_topology(run_id, "runtime recovery")

    def reconcile_run_namespaces() -> None:
        if not hasattr(cluster_automation, "delete_orphan_run_namespaces"):
            return
        now = utc_now()
        with storage_db() as connection:
            rows = connection.execute("SELECT * FROM runs ORDER BY created_at ASC").fetchall()
        preserved_run_ids = {
            str(row["id"])
            for row in rows
            if lifecycle_engine.status(row, now) in ACTIVE_EXECUTION_STATUSES
        }
        cluster_automation.delete_orphan_run_namespaces(preserved_run_ids)

    def start_run_execution(
        run_id: str,
        *,
        broker_id: str,
        scenario_id: str | None,
        config_mode: str,
        deployment_mode: str,
        broker_tuning: dict[str, Any] | None,
        resource_config: dict[str, Any] | None,
        requested_starts_at: datetime,
        message_rate: int,
        message_size_bytes: int,
        message_range_start: int,
        message_range_end: int,
        producers: int,
        consumers: int,
        warmup_seconds: int,
        measurement_seconds: int,
        cooldown_seconds: int,
        transport_options: dict[str, Any],
    ) -> None:
        if run_stop_requested(run_id):
            cleanup_run_topology(run_id, "stop")
            return

        now = utc_now()
        if requested_starts_at > now:
            record_event_now(
                run_id,
                "scheduled",
                f"Waiting until {requested_starts_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')} to start.",
            )
            wait_seconds = max(0, int((requested_starts_at - now).total_seconds()))
            if not wait_with_stop(run_id, wait_seconds):
                cleanup_run_topology(run_id, "stop")
                return

        scheduled_start_ns = time.time_ns() + int(
            cluster_automation.agent_start_barrier_seconds * 1_000_000_000
        )
        execution_started_at = datetime.fromtimestamp(
            scheduled_start_ns / 1_000_000_000,
            tz=timezone.utc,
        ).isoformat()

        def mark_execution_started() -> None:
            with db() as connection:
                connection.execute(
                    "UPDATE runs SET execution_started_at = ? WHERE id = ?",
                    (execution_started_at, run_id),
                )

        def announce_phase_transitions() -> None:
            if not wait_until_ns_with_stop(run_id, scheduled_start_ns):
                return
            record_event_now(run_id, "test-started", "Benchmark traffic started.")
            if warmup_seconds > 0:
                record_event_now(run_id, "warmup-started", "Warmup started.")
                if not wait_with_stop(run_id, warmup_seconds):
                    return
            record_event_now(run_id, "measurement-started", "Measurement window started.")
            if cooldown_seconds > 0:
                if not wait_with_stop(run_id, measurement_seconds):
                    return
                record_event_now(run_id, "cooldown-started", "Cooldown started.")

        if not cluster_automation.enabled:
            raise RuntimeError("Cluster-backed benchmark execution is disabled for this runtime")
        if hasattr(cluster_automation, "launch_benchmark_agents") and hasattr(cluster_automation, "launch_run_finalizer"):
            cluster_automation.launch_benchmark_agents(
                run_id=run_id,
                broker_id=broker_id,
                scenario_id=scenario_id,
                config_mode=config_mode,
                deployment_mode=deployment_mode,
                message_rate=message_rate,
                message_size_bytes=message_size_bytes,
                message_range_start=message_range_start,
                message_range_end=message_range_end,
                producers=producers,
                consumers=consumers,
                warmup_seconds=warmup_seconds,
                measurement_seconds=measurement_seconds,
                cooldown_seconds=cooldown_seconds,
                transport_options=transport_options,
                broker_tuning=broker_tuning,
                resource_config=resource_config,
                scheduled_start_ns=scheduled_start_ns,
                emit=lambda event_type, message: record_event_now(run_id, event_type, message),
            )
            mark_execution_started()
            threading.Thread(
                target=announce_phase_transitions,
                name=f"run-phases-{run_id[:8]}",
                daemon=True,
            ).start()
            def launch_finalizer_when_due() -> None:
                total_window_ns = int(
                    (max(0, warmup_seconds) + max(1, measurement_seconds) + max(0, cooldown_seconds))
                    * 1_000_000_000
                )
                if not wait_until_ns_with_stop(run_id, scheduled_start_ns + total_window_ns):
                    return
                finalizer_state = (
                    cluster_automation.run_finalizer_state(run_id)
                    if hasattr(cluster_automation, "run_finalizer_state")
                    else "not_found"
                )
                if finalizer_state == "running":
                    return
                cluster_automation.launch_run_finalizer(run_id)
                record_event_now(run_id, "finalizer-running", "Detached run finalizer job launched.")

            threading.Thread(
                target=launch_finalizer_when_due,
                name=f"run-finalizer-launch-{run_id[:8]}",
                daemon=True,
            ).start()
            return
        mark_execution_started()
        threading.Thread(
            target=announce_phase_transitions,
            name=f"run-phases-{run_id[:8]}",
            daemon=True,
        ).start()
        benchmark_logs = cluster_automation.run_benchmark_agents(
            run_id=run_id,
            broker_id=broker_id,
            scenario_id=scenario_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            message_rate=message_rate,
            message_size_bytes=message_size_bytes,
            message_range_start=message_range_start,
            message_range_end=message_range_end,
            producers=producers,
            consumers=consumers,
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
            transport_options=transport_options,
            broker_tuning=broker_tuning,
            resource_config=resource_config,
            scheduled_start_ns=scheduled_start_ns,
            emit=lambda event_type, message: record_event_now(run_id, event_type, message),
        )
        if run_stop_requested(run_id):
            cleanup_run_topology(run_id, "stop")
            return
        producer_log_paths = [
            str(item).strip()
            for item in (benchmark_logs.get("producerLogPaths", []) or [])
            if str(item or "").strip()
        ]
        consumer_log_paths = [
            str(item).strip()
            for item in (benchmark_logs.get("consumerLogPaths", []) or [])
            if str(item or "").strip()
        ]
        using_log_paths = bool(producer_log_paths or consumer_log_paths)
        producer_sources = (
            producer_log_paths
            if using_log_paths
            else [str(item or "") for item in (benchmark_logs.get("producerLogs", []) or [])]
        )
        consumer_sources = (
            consumer_log_paths
            if using_log_paths
            else [str(item or "") for item in (benchmark_logs.get("consumerLogs", []) or [])]
        )
        resource_samples = [
            item
            for item in (benchmark_logs.get("resourceSamples", []) or [])
            if isinstance(item, dict)
        ]
        measurement_material = load_measurement_material(
            runtime_storage,
            producer_sources=producer_sources,
            consumer_sources=consumer_sources,
            resource_samples=resource_samples,
            sources_are_paths=using_log_paths,
            run_id=run_id,
            db_factory=db,
        )
        try:
            producer_result_items = measurement_material["producerResults"]
            consumer_result_items = measurement_material["consumerResults"]
            artifact_metadata = measurement_material.get("artifactMetadata", {}) or {}
            has_producer_material = bool(producer_result_items) or int(
                artifact_metadata.get("producerTimeseriesCount", 0) or 0
            ) > 0
            has_consumer_material = bool(consumer_result_items) or int(
                artifact_metadata.get("consumerTimeseriesCount", 0) or 0
            ) > 0
            if not has_producer_material or not has_consumer_material:
                raise RuntimeError(
                    "Benchmark jobs finished without parsable producer/consumer summaries "
                    f"(producer summaries={len(producer_result_items)}/{len(producer_sources)}, "
                    f"consumer summaries={len(consumer_result_items)}/{len(consumer_sources)})"
                )
            metrics_payload = aggregate_agent_results(
                broker_id=broker_id,
                config_mode=config_mode,
                deployment_mode=deployment_mode,
                message_rate=message_rate,
                message_size_bytes=message_size_bytes,
                producers=producers,
                consumers=consumers,
                measurement_seconds=measurement_seconds,
                transport_options=transport_options,
                producer_records=producer_result_items,
                consumer_records=consumer_result_items,
                timing_confidence=str(benchmark_logs.get("timingConfidence", "validated")),
                producer_timeseries_records=measurement_material["producerTimeseriesRecords"],
                consumer_timeseries_records=measurement_material["consumerTimeseriesRecords"],
                resource_samples=resource_samples,
            )
        finally:
            cleanup_measurement_material(measurement_material)
        measurement = metrics_payload.setdefault("measurement", {})
        measurement["scheduledStartNs"] = int(benchmark_logs.get("scheduledStartNs") or 0)
        measurement["rawRecordStore"] = measurement_material["artifactMetadata"]
        post_measurement_failure = str(benchmark_logs.get("postMeasurementFailure") or "").strip()
        if post_measurement_failure:
            measurement["postMeasurementFailure"] = post_measurement_failure

        completed_at = utc_now().isoformat()
        with db() as connection:
            connection.execute(
                "UPDATE runs SET completed_at = ?, metrics_json = ? WHERE id = ? AND completed_at IS NULL",
                (completed_at, json.dumps(metrics_payload), run_id),
            )
            record_event(connection, run_id, "test-finished", "Benchmark run finished.")
            record_event(
                connection,
                run_id,
                "metrics-collected",
                f"Stored {metrics_payload.get('source', 'unknown')} measurement results.",
            )
            if post_measurement_failure:
                record_event(
                    connection,
                    run_id,
                    "degraded-completion",
                    f"Measured results were preserved after a late benchmark job failure: {post_measurement_failure}",
                )
            reconcile_queue_states(connection)
        cleanup_run_topology(run_id, "completion")

    def launch_run_preparation(run_id: str) -> None:
        def background_prepare() -> None:
            row = get_row(run_id)
            if row is None:
                return
            broker_id = str(row["broker_id"] or "kafka").strip().lower() or "kafka"
            scenario_id = normalize_scenario_id(row["scenario_id"]) if row["scenario_id"] else None
            config_mode = normalize_config_mode(row["config_mode"])
            deployment_mode = normalize_deployment_mode(row["deployment_mode"])
            broker_tuning = parse_metrics(row["broker_tuning_json"])
            resource_config = parse_metrics(row["resource_config_json"])
            transport_options = parse_transport_options(row["transport_options"])
            starts_at = parse_iso(row["starts_at"]) or utc_now()
            message_range_start = int(row["message_range_start"] or 1)
            message_range_end = int(row["message_range_end"] or 0)
            success = cluster_automation.prepare_run_environment(
                run_id=run_id,
                broker_id=broker_id,
                config_mode=config_mode,
                deployment_mode=deployment_mode,
                broker_tuning=broker_tuning,
                producers=int(row["producers"] or 1),
                consumers=int(row["consumers"] or 1),
                resource_config=resource_config,
                emit=lambda event_type, message: record_event_now(run_id, event_type, message),
            )
            if not success:
                stopped_at = utc_now().isoformat()
                with db() as connection:
                    connection.execute(
                        "UPDATE runs SET stopped_at = ? WHERE id = ? AND stopped_at IS NULL",
                        (stopped_at, run_id),
                    )
                    record_event(
                        connection,
                        run_id,
                        "prepare-failed",
                        "Topology preparation failed.",
                    )
                    reconcile_queue_states(connection)
                cleanup_run_topology(run_id, "prepare failure")
                return

            topology_ready_at = utc_now().isoformat()
            with db() as connection:
                connection.execute(
                    "UPDATE runs SET topology_ready_at = ?, queue_state = NULL WHERE id = ?",
                    (topology_ready_at, run_id),
                )
            try:
                start_run_execution(
                    run_id,
                    broker_id=broker_id,
                    scenario_id=scenario_id,
                    config_mode=config_mode,
                    deployment_mode=deployment_mode,
                    broker_tuning=broker_tuning,
                    resource_config=resource_config,
                    requested_starts_at=starts_at,
                    message_rate=int(row["message_rate"] or 1),
                    message_size_bytes=int(row["message_size_bytes"] or 1),
                    message_range_start=message_range_start,
                    message_range_end=message_range_end,
                    producers=int(row["producers"] or 1),
                    consumers=int(row["consumers"] or 1),
                    warmup_seconds=int(row["warmup_seconds"] or 0),
                    measurement_seconds=int(row["measurement_seconds"] or 1),
                    cooldown_seconds=int(row["cooldown_seconds"] or 0),
                    transport_options=transport_options,
                )
            except Exception as exc:
                if run_stop_requested(run_id):
                    cleanup_run_topology(run_id, "stop")
                    return
                failed_at = utc_now().isoformat()
                with db() as connection:
                    connection.execute(
                        "UPDATE runs SET stopped_at = ? WHERE id = ? AND stopped_at IS NULL",
                        (failed_at, run_id),
                    )
                    record_event(
                        connection,
                        run_id,
                        "execution-failed",
                        str(exc),
                    )
                    reconcile_queue_states(connection)
                cleanup_run_topology(run_id, "execution failure")

        threading.Thread(
            target=background_prepare,
            name=f"run-prepare-{run_id[:8]}",
            daemon=True,
        ).start()

    @app.get("/")
    def index() -> Response:
        return Response(
            content=rendered_index,
            media_type="text/html",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        storage_state = storage_status_snapshot()
        return {
            "status": "ok",
            "buildId": build_id,
            "assetVersion": asset_version,
            "accessUrl": access_url,
            "storageMode": runtime_storage.mode,
            "storageReady": storage_state["ready"],
            "storageMessage": storage_state["message"],
        }

    @app.get("/api/system/bootstrap")
    def bootstrap_status() -> dict[str, Any]:
        return {
            **cluster_automation.status(),
            "storage": storage_status_snapshot(),
        }

    @app.post("/api/system/bootstrap")
    def bootstrap_system(request: BootstrapRequest) -> dict[str, Any]:
        scope = str(request.scope or "brokers").strip().lower() or "brokers"
        if scope == "brokers":
            status = cluster_automation.ensure_broker_operators()
        elif scope in {"platform-operators", "platform_operators"}:
            status = cluster_automation.ensure_platform_data_operators()
        elif scope in {"platform-services", "platform_services"}:
            status = cluster_automation.ensure_platform_data_services()
        elif scope in {"platform-data", "platform_data"}:
            status = cluster_automation.ensure_platform_data()
        elif scope == "all":
            status = cluster_automation.ensure_operators()
        else:
            raise HTTPException(status_code=422, detail="Unsupported bootstrap scope")

        if runtime_storage.mode == "postgres+s3" and scope in {
            "platform-services",
            "platform_services",
            "platform-data",
            "platform_data",
            "all",
        }:
            initialize_runtime_storage()

        return {
            **status,
            "storage": storage_status_snapshot(),
        }

    @app.get("/api/brokers")
    def brokers() -> dict[str, Any]:
        operator_states = (cluster_automation.status() or {}).get("operators", {})
        enriched = []
        for entry in broker_catalog_entries():
            broker_id = str(entry.get("id") or "").strip().lower()
            control_plane_required = bool(
                entry.get("controlPlaneRequired", entry.get("operatorRequired", True))
            )
            operator_state = operator_states.get(broker_id, {})
            enriched.append(
                {
                    **entry,
                    "operatorStatus": operator_state,
                    "runtimeReady": bool(operator_state.get("ready")) or not control_plane_required,
                }
            )
        return {"brokers": enriched}

    @app.get("/api/presets")
    def presets() -> dict[str, Any]:
        return load_presets()

    @app.get("/api/broker-profiles")
    def broker_profiles() -> dict[str, Any]:
        catalog = profile_service.catalog()
        return {
            "version": catalog.get("version", 1),
            "profiles": catalog.get("profiles", []),
        }

    @app.get("/api/runs")
    def list_runs() -> dict[str, Any]:
        reconcile_detached_finalizers()
        reconcile_topology_deletions()
        now = utc_now()
        with db() as connection:
            rows = connection.execute("SELECT * FROM runs ORDER BY created_at DESC").fetchall()
            events = connection.execute(
                """
                SELECT run_id, event_type, message, created_at
                FROM run_events
                ORDER BY created_at DESC
                LIMIT 80
                """
            ).fetchall()

        return {
            "runs": [lifecycle_engine.serialize(row, now) for row in rows],
            "recentEvents": [dict(row) for row in events],
            "serverTime": now.isoformat(),
        }

    @app.post("/api/runs/export")
    def export_runs(payload: ExportRunsRequest) -> Response:
        requested_ids = []
        seen_ids: set[str] = set()
        for run_id in payload.runIds or []:
            candidate = str(run_id or "").strip()
            if not candidate or candidate in seen_ids:
                continue
            seen_ids.add(candidate)
            requested_ids.append(candidate)

        with db() as connection:
            if requested_ids:
                rows = connection.execute(
                    "SELECT * FROM runs WHERE id IN ({}) ORDER BY created_at DESC".format(",".join("?" for _ in requested_ids)),
                    tuple(requested_ids),
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM runs ORDER BY created_at DESC").fetchall()

        row_by_id = {str(row["id"]): row for row in rows}
        ordered_rows = [row_by_id[run_id] for run_id in requested_ids if run_id in row_by_id] if requested_ids else list(rows)
        if requested_ids and len(ordered_rows) != len(requested_ids):
            missing = next(run_id for run_id in requested_ids if run_id not in row_by_id)
            raise HTTPException(status_code=404, detail=f"Run not found: {missing}")
        if not ordered_rows:
            raise HTTPException(status_code=422, detail="No runs available to export")

        exported_at = utc_now().isoformat()
        bundle = {
            "kind": RUN_EXPORT_KIND,
            "version": RUN_EXPORT_VERSION,
            "exportedAt": exported_at,
            "sourceBuildId": build_id,
            "sourceAccessUrl": access_url,
            "runs": [build_run_export_entry(row) for row in ordered_rows],
        }
        file_name = f"concerto-bus-results-{utc_now().strftime('%Y%m%dT%H%M%SZ')}.json"
        return Response(
            content=json.dumps(bundle, separators=(",", ":"), ensure_ascii=True),
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{file_name}"',
            },
        )

    @app.post("/api/runs/import")
    def import_runs(payload: ImportRunsRequest) -> dict[str, Any]:
        return import_run_bundle(payload)

    @app.post("/api/runs")
    def create_run(payload: CreateRunRequest) -> dict[str, Any]:
        run_name = normalize_run_name(payload.name)
        if not cluster_automation.enabled:
            raise HTTPException(
                status_code=503,
                detail="Cluster-backed execution is disabled for this runtime",
            )

        broker_id = payload.brokerId.lower()
        broker = broker_definition(broker_id)

        scenario_id = normalize_scenario_id(payload.scenarioId)
        protocol = normalize_protocol_for_broker(broker_id, payload.protocol)
        config_mode = normalize_config_mode(payload.configMode)
        deployment_mode = normalize_deployment_mode(payload.deploymentMode)
        schedule_mode = normalize_schedule_mode(payload.scheduleMode)
        message_range_start, message_range_end = normalize_message_range(
            payload.messageRangeStart,
            payload.messageRangeEnd,
        )
        ha_mode = deployment_mode == "ha"
        broker_tuning = payload.brokerTuning or profile_service.default_tuning(
            broker_id=broker_id,
            config_mode=config_mode,
        )
        broker_tuning = sanitize_broker_tuning(broker_id, broker_tuning)
        setup_preset = resource_setup_preset(
            config_mode=config_mode,
            broker_tuning=broker_tuning,
        )
        default_resource_config = profile_service.default_resource_config(
            broker_id=broker_id,
            config_mode=config_mode,
            deployment_mode=deployment_mode,
            setup_preset=setup_preset,
        )
        resource_config = normalize_resource_config(
            payload.resourceConfig,
            defaults=default_resource_config,
        )
        transport_options = normalize_transport_options(
            payload.transportOptions,
            message_rate=payload.messageRate,
        )
        broker_tuning, transport_options = apply_payload_limit_policy(
            broker_id=broker_id,
            message_size_bytes=payload.messageSizeBytes,
            broker_tuning=broker_tuning,
            transport_options=transport_options,
        )

        if schedule_mode == "parallel" and active_execution_exists():
            with db() as connection:
                existing_run = connection.execute(
                    "SELECT id FROM runs WHERE lower(name) = lower(?)", (run_name,)
                ).fetchone()
                if existing_run:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Run name '{run_name}' already exists. Choose a different name.",
                    )
            raise HTTPException(
                status_code=409,
                detail="Only one run can be active at a time in parallel mode. Use sequential mode to queue it.",
            )

        with db() as connection:
            existing_run = connection.execute(
                "SELECT id FROM runs WHERE lower(name) = lower(?)", (run_name,)
            ).fetchone()
            if existing_run:
                raise HTTPException(status_code=409, detail=f"Run name '{run_name}' already exists. Choose a different name.")

        run_id = str(uuid.uuid4())
        starts_at = (payload.startsAt or utc_now()).astimezone(timezone.utc)
        created_at = utc_now().isoformat()
        initial_queue_state = "waiting" if schedule_mode == "sequential" else None

        with db() as connection:
            connection.execute(
                """
                INSERT INTO runs (
                  id, name, broker_id, protocol, scenario_id, config_mode, deployment_mode, schedule_mode, queue_state, starts_at,
                  warmup_seconds, measurement_seconds, cooldown_seconds, message_rate,
                  message_size_bytes, message_range_start, message_range_end, producers, consumers,
                  transport_options, ha_mode, broker_tuning_json,
                  metrics_json, resource_config_json, created_at, stopped_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    run_id,
                    run_name,
                    broker_id,
                    protocol,
                    scenario_id,
                    config_mode,
                    deployment_mode,
                    schedule_mode,
                    initial_queue_state,
                    starts_at.isoformat(),
                    payload.warmupSeconds,
                    payload.measurementSeconds,
                    payload.cooldownSeconds,
                    payload.messageRate,
                    payload.messageSizeBytes,
                    message_range_start,
                    message_range_end,
                    payload.producers,
                    payload.consumers,
                    json.dumps(transport_options, sort_keys=True),
                    1 if ha_mode else 0,
                    json.dumps(broker_tuning),
                    json.dumps({}),
                    json.dumps(resource_config),
                    created_at,
                ),
            )
            record_event(
                connection,
                run_id,
                "created",
                f"{broker['label']} run {run_name} created",
            )
            if schedule_mode == "sequential":
                reconcile_queue_states(connection)
                queued_row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
                queue_state = str(queued_row["queue_state"] or "waiting").strip().lower()
                record_event(
                    connection,
                    run_id,
                    queue_state,
                    "Queued behind the current workload."
                    if queue_state == "waiting"
                    else "Queued and ready to launch as soon as the platform is idle.",
                )
            else:
                record_event(
                    connection,
                    run_id,
                    "topology-started",
                    f"Creating benchmark topology in {run_namespace_for(run_id)} with broker replicas={int(resource_config.get('replicas') or target_replicas_for_mode(deployment_mode))}",
                )
                reconcile_queue_states(connection)
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if schedule_mode == "sequential":
            kick_sequential_scheduler()
        else:
            launch_run_preparation(run_id)

        return lifecycle_engine.serialize(row, utc_now())

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        reconcile_detached_finalizers()
        reconcile_topology_deletions()
        with db() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Run not found")
            events = connection.execute(
                """
                SELECT event_type, message, created_at
                FROM run_events
                WHERE run_id = ?
                ORDER BY created_at DESC
                """,
                (run_id,),
            ).fetchall()

        return {
            "run": lifecycle_engine.serialize(row, utc_now()),
            "events": [dict(event) for event in events],
        }

    @app.get("/api/runs/{run_id}/artifacts")
    def get_run_artifacts(run_id: str) -> dict[str, Any]:
        row = get_row(run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "artifacts": [serialize_run_artifact(item) for item in get_run_artifact_rows(run_id)]
        }

    @app.get("/api/runs/{run_id}/artifacts/{artifact_id}/download")
    def download_run_artifact(run_id: str, artifact_id: str) -> Response:
        with db() as connection:
            row = connection.execute(
                "SELECT * FROM run_artifacts WHERE id = ? AND run_id = ?",
                (artifact_id, run_id),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Artifact not found")
        if not runtime_storage.row_exists(row):
            raise HTTPException(status_code=404, detail="Artifact file not found")
        return Response(
            content=runtime_storage.row_bytes(row),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{str(row["file_name"] or "artifact.bin")}"'
            },
        )

    @app.get("/api/reports")
    def list_reports() -> dict[str, Any]:
        with db() as connection:
            rows = connection.execute("SELECT * FROM reports ORDER BY created_at DESC").fetchall()
        return {"reports": [serialize_report(row) for row in rows]}

    @app.post("/api/reports")
    def create_report(payload: CreateReportRequest) -> dict[str, Any]:
        title = normalize_report_title(payload.title)
        sections = normalize_report_sections(payload.sections)
        run_ids = []
        seen_run_ids: set[str] = set()
        for run_id in payload.runIds:
            candidate = str(run_id).strip()
            if not candidate or candidate in seen_run_ids:
                continue
            seen_run_ids.add(candidate)
            run_ids.append(candidate)
        if not run_ids:
            raise HTTPException(status_code=422, detail="At least one run is required")

        now = utc_now()
        with db() as connection:
            title = resolve_unique_report_title(connection, title)
        load_report_runs(run_ids, now=now)

        report_id = str(uuid.uuid4())
        created_at = now.isoformat()
        file_name = build_report_file_name(title, created_at)
        file_path = report_dir / file_name

        with db() as connection:
            connection.execute(
                """
                INSERT INTO reports (
                  id, title, run_ids_json, sections_json, status, file_name, file_path, storage_backend, object_key, size_bytes, error_message, created_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    title,
                    json.dumps({"ids": run_ids}),
                    report_sections_json(sections),
                    "queued",
                    file_name,
                    str(file_path),
                    "local",
                    None,
                    0,
                    None,
                    created_at,
                    None,
                ),
            )

        with db() as connection:
            row = connection.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
        row = regenerate_report_artifact(row)
        return serialize_report(row)

    @app.get("/api/reports/{report_id}")
    def get_report(report_id: str) -> dict[str, Any]:
        row = get_report_row(report_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Report not found")
        return serialize_report(row)

    @app.get("/api/reports/{report_id}/download")
    def download_report(report_id: str) -> Response:
        row = get_report_row(report_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Report not found")
        if not runtime_storage.row_exists(row):
            row = regenerate_report_artifact(row, failure_prefix="Report recovery failed")
        return Response(
            content=runtime_storage.row_bytes(row),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{str(row["file_name"] or "report.pdf")}"'
            },
        )

    @app.post("/api/runs/{run_id}/stop", response_model=StopRunResponse)
    def stop_run(run_id: str) -> StopRunResponse:
        stopped_at = utc_now().isoformat()
        with db() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Run not found")
            connection.execute("UPDATE runs SET stopped_at = ? WHERE id = ?", (stopped_at, run_id))
            record_event(connection, run_id, "stopped", f"Run {run_id} stopped by user")
            reconcile_queue_states(connection)

        def stop_and_cleanup() -> None:
            row = get_row(run_id)
            if row is None:
                return
            if not row["topology_ready_at"] and not row["execution_started_at"]:
                kick_sequential_scheduler()
                return
            if hasattr(cluster_automation, "delete_run_finalizer_job"):
                cluster_automation.delete_run_finalizer_job(run_id)
            cleanup_run_topology(run_id, "stop")

        threading.Thread(
            target=stop_and_cleanup,
            name=f"run-cleanup-{run_id[:8]}",
            daemon=True,
        ).start()

        return StopRunResponse(id=run_id, status="stopped", stoppedAt=stopped_at)

    @app.delete("/api/runs/reset-all")
    def reset_runs() -> dict[str, Any]:
        if active_execution_exists():
            raise HTTPException(
                status_code=409,
                detail="Stop the active run and wait for cleanup before clearing everything",
            )
        with db() as connection:
            rows = connection.execute("SELECT id FROM runs").fetchall()
            report_rows = connection.execute("SELECT * FROM reports").fetchall()
            artifact_rows = connection.execute("SELECT * FROM run_artifacts").fetchall()
            runs_deleted = connection.execute("DELETE FROM runs").rowcount
            connection.execute("DELETE FROM run_events")
            connection.execute("DELETE FROM reports")
            connection.execute("DELETE FROM run_artifacts")
        for row in rows:
            if hasattr(cluster_automation, "delete_run_finalizer_job"):
                cluster_automation.delete_run_finalizer_job(str(row["id"]))
            cluster_automation.delete_run_namespace(str(row["id"]))
        delete_report_artifacts(report_rows)
        delete_run_artifacts(artifact_rows)
        cluster_automation.delete_orphan_run_namespaces(set())
        cluster_automation.delete_legacy_broker_namespaces()
        return {"status": "reset", "runsDeleted": runs_deleted}

    @app.delete("/api/runs/{run_id}")
    def delete_run(run_id: str) -> dict[str, Any]:
        with db() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Run not found")
            status = lifecycle_engine.status(row, utc_now())
            if status in ACTIVE_EXECUTION_STATUSES:
                raise HTTPException(
                    status_code=409,
                    detail="Stop the run and wait for cleanup before clearing it",
                )
            if hasattr(cluster_automation, "delete_run_finalizer_job"):
                cluster_automation.delete_run_finalizer_job(run_id)
            cluster_automation.delete_run_namespace(run_id)
            report_rows = [
                report_row
                for report_row in connection.execute("SELECT * FROM reports ORDER BY created_at DESC").fetchall()
                if run_id in set(parse_report_run_ids(report_row["run_ids_json"]))
            ]
            artifact_rows = connection.execute(
                "SELECT * FROM run_artifacts WHERE run_id = ? ORDER BY created_at DESC",
                (run_id,),
            ).fetchall()
            deleted = connection.execute("DELETE FROM runs WHERE id = ?", (run_id,)).rowcount
            connection.execute("DELETE FROM run_events WHERE run_id = ?", (run_id,))
            for report_row in report_rows:
                connection.execute("DELETE FROM reports WHERE id = ?", (report_row["id"],))
            connection.execute("DELETE FROM run_artifacts WHERE run_id = ?", (run_id,))
            record_event(connection, None, "deleted", f"Run {run_id} erased")
        delete_report_artifacts(report_rows)
        delete_run_artifacts(artifact_rows)
        return {"status": "deleted", "id": run_id}

    return app


app = create_app()
