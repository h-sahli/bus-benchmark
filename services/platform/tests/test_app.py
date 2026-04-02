import base64
import gzip
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.platform.app import create_app
from services.platform.finalizer import finalize_run


def encode_measurement_chunk(
    role: str,
    rows: list[list[object]],
    schema: list[str],
    *,
    kind: str = "measurement-record-chunk",
) -> str:
    payload = gzip.compress(
        json.dumps({"schema": schema, "rows": rows}, separators=(",", ":"), sort_keys=False).encode("utf-8")
    )
    return json.dumps(
        {
            "kind": kind,
            "role": role,
            "payload": base64.b64encode(payload).decode("ascii"),
        }
    )


class FakeClusterAutomation:
    def __init__(self) -> None:
        self.enabled = True
        self.operator_bootstrap_enabled = False
        self.agent_start_barrier_seconds = 1
        self.namespace_exists_value = False
        self.wait_for_namespace_deleted_value = True
        self.force_delete_namespace_deletes = True
        self.deleted_structured_output_run_ids: list[str] = []
        self.deleted_benchmark_job_run_ids: list[str] = []
        self.force_deleted_namespace_run_ids: list[str] = []
        self.bootstrap_scope_calls: list[str] = []

    def status(self) -> dict:
        return {
            "enabled": True,
            "ready": True,
            "message": "ready",
            "operators": {
                "kafka": {"label": "Kafka", "ready": True, "message": "ready"},
                "rabbitmq": {"label": "RabbitMQ", "ready": True, "message": "ready"},
                "artemis": {"label": "Artemis", "ready": True, "message": "ready"},
                "nats": {"label": "NATS JetStream", "ready": True, "message": "ready"},
            },
        }

    def prepare_run_environment(self, *, emit, **_: dict) -> bool:
        emit("prepare-started", "Preparing namespace.")
        emit("namespace-ready", "Namespace ready.")
        emit("broker-waiting", "Waiting for broker pods.")
        emit("broker-ready", "Broker pods are ready.")
        return True

    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **_: dict) -> dict:
        emit("consumer-running", "Consumer jobs created.")
        emit("producer-running", "Producer jobs created.")
        producer = {
            "role": "producer",
            "result": {
                "attempted": message_rate,
                "sent": message_rate,
                "publishErrors": 0,
                "ackLatencyHistogramMs": [
                    {"lowerMs": 0.0, "upperMs": 1.0, "count": 20},
                    {"lowerMs": 1.0, "upperMs": 2.0, "count": 10},
                ],
                "timeseries": [
                    {"second": 0, "attemptedMessages": message_rate // 2, "sentMessages": message_rate // 2},
                    {"second": 1, "attemptedMessages": message_rate - (message_rate // 2), "sentMessages": message_rate - (message_rate // 2)},
                ],
            },
        }
        consumer = {
            "role": "consumer",
            "result": {
                "received": max(1, message_rate - 5),
                "receivedMeasured": max(1, message_rate - 5),
                "parseErrors": 0,
                "duplicates": 0,
                "outOfOrder": 0,
                "latencyHistogramMs": [
                    {"lowerMs": 0.0, "upperMs": 2.0, "count": 30},
                    {"lowerMs": 2.0, "upperMs": 5.0, "count": 20},
                ],
                "timeseries": [
                    {"second": 0, "deliveredMessages": max(1, (message_rate - 5) // 2), "latencyP50Ms": 1.6, "latencyP95Ms": 2.3, "latencyP99Ms": 2.3},
                    {"second": 1, "deliveredMessages": max(1, (message_rate - 5) - ((message_rate - 5) // 2)), "latencyP50Ms": 1.8, "latencyP95Ms": 2.7, "latencyP99Ms": 2.7},
                ],
            },
        }
        producer_chunk = encode_measurement_chunk(
            "producer",
            [
                ["event-1", "producer-1", 1, 1000000, 1000100, 1001500, 0.5, 100, 0, "benchmark.events", "0", "10"],
                ["event-2", "producer-1", 2, 2000000, 2000100, 2001800, 0.8, 1400, 1, "benchmark.events", "0", "11"],
            ],
            [
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
        consumer_chunk = encode_measurement_chunk(
            "consumer",
            [
                ["event-1", "consumer-1", "producer-1", 1, 1000000, 3500000, 2.5, 100, 0, "false", "1", "benchmark.events"],
                ["event-2", "consumer-1", "producer-1", 2, 2000000, 5300000, 3.3, 1400, 1, "false", "1", "benchmark.events"],
            ],
            [
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
        return {
            "producerLogs": [json.dumps(producer), producer_chunk],
            "consumerLogs": [json.dumps(consumer), consumer_chunk],
            "nodeNames": ["cluster-node-1"],
            "timingConfidence": "validated",
            "scheduledStartNs": scheduled_start_ns or 123456789,
            "resourceSamples": [
                {
                    "second": 0,
                    "brokerCpuCores": 0.45,
                    "brokerMemoryMB": 256.0,
                    "brokerNetworkRxMBps": 1.2,
                    "brokerNetworkTxMBps": 1.0,
                    "brokerStorageUsedMB": 64.0,
                    "producerCpuCores": 0.15,
                    "producerMemoryMB": 48.0,
                    "consumerCpuCores": 0.12,
                    "consumerMemoryMB": 52.0,
                }
            ],
        }

    def delete_run_namespace(self, run_id: str) -> None:
        del run_id

    def wait_for_namespace_deleted(self, run_id: str, timeout_seconds: int = 180) -> bool:
        del run_id, timeout_seconds
        return self.wait_for_namespace_deleted_value

    def namespace_exists(self, run_id: str) -> bool:
        del run_id
        return self.namespace_exists_value

    def force_delete_namespace(self, run_id: str) -> None:
        self.force_deleted_namespace_run_ids.append(str(run_id))
        if self.force_delete_namespace_deletes:
            self.namespace_exists_value = False

    def delete_legacy_broker_namespaces(self) -> None:
        return None

    def ensure_broker_operators(self) -> dict:
        self.bootstrap_scope_calls.append("brokers")
        return self.status()

    def ensure_platform_data_operators(self) -> dict:
        self.bootstrap_scope_calls.append("platform-operators")
        return self.status()

    def ensure_platform_data_services(self) -> dict:
        self.bootstrap_scope_calls.append("platform-services")
        return self.status()

    def ensure_platform_data(self) -> dict:
        self.bootstrap_scope_calls.append("platform-data")
        return self.status()

    def ensure_operators(self) -> dict:
        self.bootstrap_scope_calls.append("all")
        return self.status()

    def delete_orphan_run_namespaces(self, preserved_run_ids: set[str] | None = None) -> None:
        del preserved_run_ids
        return None

    def delete_benchmark_jobs(self, run_id: str, producers: int, consumers: int) -> None:
        del producers, consumers
        self.deleted_benchmark_job_run_ids.append(str(run_id))

    def delete_run_structured_output(self, run_id: str) -> None:
        self.deleted_structured_output_run_ids.append(str(run_id))


class LateFailureClusterAutomation(FakeClusterAutomation):
    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **_: dict) -> dict:
        payload = super().run_benchmark_agents(
            emit=emit,
            message_rate=message_rate,
            scheduled_start_ns=scheduled_start_ns,
        )
        payload["postMeasurementFailure"] = "Benchmark job producer-1 failed after measurement completed: exit code 2"
        return payload


class ChunkedSummaryClusterAutomation(FakeClusterAutomation):
    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **_: dict) -> dict:
        payload = super().run_benchmark_agents(
            emit=emit,
            message_rate=message_rate,
            scheduled_start_ns=scheduled_start_ns,
        )
        producer_summary = json.loads(payload["producerLogs"][0])
        consumer_summary = json.loads(payload["consumerLogs"][0])
        producer_summary["result"].pop("timeseries", None)
        consumer_summary["result"].pop("timeseries", None)
        producer_timeseries = encode_measurement_chunk(
            "producer",
            [
                [0, message_rate // 2, message_rate // 2, []],
                [1, message_rate - (message_rate // 2), message_rate - (message_rate // 2), []],
            ],
            ["second", "attemptedMessages", "sentMessages", "ackLatencyHistogramMs"],
            kind="measurement-timeseries-chunk",
        )
        delivered = max(1, message_rate - 5)
        consumer_timeseries = encode_measurement_chunk(
            "consumer",
            [
                [0, max(1, delivered // 2), [{"lowerMs": 1.0, "upperMs": 2.0, "count": 1}, {"lowerMs": 2.0, "upperMs": 5.0, "count": 1}]],
                [1, max(1, delivered - (delivered // 2)), [{"lowerMs": 1.0, "upperMs": 2.0, "count": 1}, {"lowerMs": 2.0, "upperMs": 5.0, "count": 1}]],
            ],
            ["second", "deliveredMessages", "latencyHistogramMs"],
            kind="measurement-timeseries-chunk",
        )
        payload["producerLogs"] = [json.dumps(producer_summary), producer_timeseries, payload["producerLogs"][1]]
        payload["consumerLogs"] = [json.dumps(consumer_summary), consumer_timeseries, payload["consumerLogs"][1]]
        return payload


class TimeseriesOnlyClusterAutomation(FakeClusterAutomation):
    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **_: dict) -> dict:
        payload = super().run_benchmark_agents(
            emit=emit,
            message_rate=message_rate,
            scheduled_start_ns=scheduled_start_ns,
        )
        producer_timeseries = encode_measurement_chunk(
            "producer",
            [
                [0, message_rate // 2, message_rate // 2, [{"lowerMs": 0.25, "upperMs": 0.5, "count": 10}]],
                [1, message_rate - (message_rate // 2), message_rate - (message_rate // 2), [{"lowerMs": 0.5, "upperMs": 1.0, "count": 10}]],
            ],
            ["second", "attemptedMessages", "sentMessages", "ackLatencyHistogramMs"],
            kind="measurement-timeseries-chunk",
        )
        delivered = max(1, message_rate - 5)
        consumer_timeseries = encode_measurement_chunk(
            "consumer",
            [
                [0, max(1, delivered // 2), [{"lowerMs": 1.0, "upperMs": 2.0, "count": max(1, delivered // 2)}]],
                [1, max(1, delivered - (delivered // 2)), [{"lowerMs": 2.0, "upperMs": 5.0, "count": max(1, delivered - (delivered // 2))}]],
            ],
            ["second", "deliveredMessages", "latencyHistogramMs"],
            kind="measurement-timeseries-chunk",
        )
        payload["producerLogs"] = [producer_timeseries]
        payload["consumerLogs"] = [consumer_timeseries]
        return payload


class FileBackedClusterAutomation(FakeClusterAutomation):
    def __init__(self, temp_root: Path) -> None:
        super().__init__()
        self.temp_root = temp_root

    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **_: dict) -> dict:
        payload = super().run_benchmark_agents(
            emit=emit,
            message_rate=message_rate,
            scheduled_start_ns=scheduled_start_ns,
        )
        self.temp_root.mkdir(parents=True, exist_ok=True)
        producer_path = self.temp_root / "producer.log"
        consumer_path = self.temp_root / "consumer.log"
        producer_path.write_text("\n".join(payload["producerLogs"]) + "\n", encoding="utf-8")
        consumer_path.write_text("\n".join(payload["consumerLogs"]) + "\n", encoding="utf-8")
        return {
            "producerLogPaths": [str(producer_path)],
            "consumerLogPaths": [str(consumer_path)],
            "nodeNames": payload["nodeNames"],
            "timingConfidence": payload["timingConfidence"],
            "scheduledStartNs": payload["scheduledStartNs"],
            "resourceSamples": payload["resourceSamples"],
        }


class DetachedFinalizerClusterAutomation(FakeClusterAutomation):
    def __init__(self, db_path: Path) -> None:
        super().__init__()
        self.db_path = db_path
        self.artifact_dir = db_path.parent / "artifacts"
        self.pending_runs: dict[str, dict[str, int]] = {}
        self.finalizer_states: dict[str, str] = {}
        self.deleted_finalizer_jobs: list[str] = []
        self.active_run_ids: set[str] = set()

    def prepare_run_environment(self, *, run_id, emit, **kwargs):
        self.active_run_ids.add(str(run_id))
        return super().prepare_run_environment(run_id=run_id, emit=emit, **kwargs)

    def launch_benchmark_agents(self, *, run_id, emit, message_rate, scheduled_start_ns=None, **kwargs):
        self.pending_runs[str(run_id)] = {
            "message_rate": int(message_rate),
            "scheduled_start_ns": int(scheduled_start_ns or 123456789),
        }
        emit("consumer-running", "Consumer jobs created.")
        emit("producer-running", "Producer jobs created.")
        emit("execution-synchronized", "Detached finalizer will collect results.")
        return {
            "namespace": f"bench-run-{str(run_id).split('-')[0]}",
            "producerJobs": ["producer-1"],
            "consumerJobs": ["consumer-1"],
            "scheduledStartNs": int(scheduled_start_ns or 123456789),
        }

    def collect_benchmark_results(self, *, run_id, scheduled_start_ns, **kwargs):
        run_config = self.pending_runs[str(run_id)]
        return super().run_benchmark_agents(
            emit=lambda *_args, **_inner: None,
            message_rate=int(run_config["message_rate"]),
            scheduled_start_ns=int(scheduled_start_ns),
        )

    def launch_run_finalizer(self, run_id: str) -> None:
        self.finalizer_states[str(run_id)] = "running"

        def worker() -> None:
            outcome = finalize_run(
                str(run_id),
                db_path=self.db_path,
                artifact_dir=self.artifact_dir,
                cluster_automation=self,
            )
            self.finalizer_states[str(run_id)] = "succeeded" if outcome == 0 else "failed"

        threading.Thread(target=worker, name=f"detached-finalizer-{str(run_id)[:8]}", daemon=True).start()

    def run_finalizer_state(self, run_id: str) -> str:
        return self.finalizer_states.get(str(run_id), "not_found")

    def delete_run_finalizer_job(self, run_id: str) -> None:
        self.deleted_finalizer_jobs.append(str(run_id))
        self.finalizer_states.pop(str(run_id), None)

    def delete_run_namespace(self, run_id: str) -> None:
        self.active_run_ids.discard(str(run_id))

    def namespace_exists(self, run_id: str) -> bool:
        return str(run_id) in self.active_run_ids


class BrokenDetachedFinalizerClusterAutomation(DetachedFinalizerClusterAutomation):
    def collect_benchmark_results(self, *, scheduled_start_ns, **kwargs):
        del kwargs
        return {
            "producerLogPaths": [],
            "consumerLogPaths": [],
            "nodeNames": ["cluster-node-1"],
            "timingConfidence": "validated",
            "scheduledStartNs": int(scheduled_start_ns),
            "resourceSamples": [],
        }


class FailingLaunchClusterAutomation(FakeClusterAutomation):
    def prepare_run_environment(self, *, run_id, emit, **kwargs):
        del run_id, kwargs
        emit("prepare-started", "Preparing namespace.")
        emit("namespace-ready", "Namespace ready.")
        emit("broker-ready", "Broker pods are ready.")
        return True

    def launch_benchmark_agents(self, *, emit, **kwargs):
        del kwargs
        emit("consumer-running", "Consumer jobs created.")
        raise RuntimeError("Timed out waiting for job pod consumer-1 to start")

    def launch_run_finalizer(self, run_id: str) -> None:
        del run_id
        return None


class SlowClusterAutomation(FakeClusterAutomation):
    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()

    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **kwargs: dict) -> dict:
        del kwargs
        emit("consumer-running", "Consumer jobs created.")
        emit("producer-running", "Producer jobs created.")
        self.release.wait(timeout=5.0)
        return super().run_benchmark_agents(
            emit=emit,
            message_rate=message_rate,
            scheduled_start_ns=scheduled_start_ns,
        )


class ZeroSampleClusterAutomation(FakeClusterAutomation):
    def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **kwargs: dict) -> dict:
        payload = super().run_benchmark_agents(
            emit=emit,
            message_rate=message_rate,
            scheduled_start_ns=scheduled_start_ns,
            **kwargs,
        )
        producer = json.loads(payload["producerLogs"][0])
        consumer = json.loads(payload["consumerLogs"][0])
        consumer["result"]["received"] = 0
        consumer["result"]["receivedMeasured"] = 0
        consumer["result"]["latencyHistogramMs"] = []
        consumer["result"]["timeseries"] = [
            {"second": 0, "deliveredMessages": 0, "latencyHistogramMs": []},
            {"second": 1, "deliveredMessages": 0, "latencyHistogramMs": []},
        ]
        payload["producerLogs"][0] = json.dumps(producer)
        payload["consumerLogs"][0] = json.dumps(consumer)
        return payload


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    with TestClient(
        create_app(tmp_path / "test.db", cluster_automation=FakeClusterAutomation())
    ) as test_client:
        yield test_client


def wait_for_run_status(
    client: TestClient,
    run_id: str,
    expected_status: str,
    timeout_seconds: float = 8.0,
) -> dict:
    deadline = time.time() + timeout_seconds
    latest = {}
    while time.time() < deadline:
        response = client.get(f"/api/runs/{run_id}")
        assert response.status_code == 200
        latest = response.json()["run"]
        if latest["status"] == expected_status:
            return latest
        time.sleep(0.2)
    return latest


def wait_for_run_status_in(
    client: TestClient,
    run_id: str,
    expected_statuses: set[str],
    timeout_seconds: float = 8.0,
) -> dict:
    deadline = time.time() + timeout_seconds
    latest = {}
    while time.time() < deadline:
        response = client.get(f"/api/runs/{run_id}")
        assert response.status_code == 200
        latest = response.json()["run"]
        if latest["status"] in expected_statuses:
            return latest
        time.sleep(0.2)
    return latest


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/api/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["buildId"]
    assert payload["assetVersion"]
    assert payload["accessUrl"].startswith("http://")


def test_index_includes_runtime_asset_version(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "/static/styles.css?v=dev" in response.text
    assert "/static/app.js?v=dev" in response.text
    assert '<meta name="bus-build-id" content="dev"' in response.text


def test_broker_profiles_endpoint(client: TestClient) -> None:
    response = client.get("/api/broker-profiles")
    assert response.status_code == 200
    payload = response.json()
    assert payload["version"] >= 1
    profile_ids = {item["id"] for item in payload["profiles"]}
    assert "kafka-baseline" in profile_ids
    assert "rabbitmq-baseline" in profile_ids
    assert "artemis-baseline" in profile_ids
    assert "nats-baseline" in profile_ids
    install_methods = {
        (item["brokerId"], item["mode"]): item["operator"]["installMethod"]
        for item in payload["profiles"]
    }
    assert install_methods[("kafka", "baseline")] == "helm"
    assert install_methods[("rabbitmq", "baseline")] == "manifest"
    assert install_methods[("artemis", "baseline")] == "helm"
    assert install_methods[("nats", "baseline")] == "helm"


def test_bootstrap_status_endpoint(client: TestClient) -> None:
    response = client.get("/api/system/bootstrap")
    assert response.status_code == 200
    payload = response.json()
    assert "enabled" in payload
    assert "message" in payload
    assert "operators" in payload
    assert {"kafka", "rabbitmq", "artemis", "nats"} <= set(payload["operators"].keys())


def test_default_kafka_resource_config_is_applied_from_profile(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "kafka profile resources",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = response.json()
    assert run["resourceConfig"]["cpuRequest"] == "1500m"
    assert run["resourceConfig"]["cpuLimit"] == "1500m"
    assert run["resourceConfig"]["memoryRequest"] == "3Gi"
    assert run["resourceConfig"]["memoryLimit"] == "3Gi"
    assert run["resourceConfig"]["storageSize"] == "20Gi"
    assert run["resourceConfig"]["replicas"] == 1


def test_setup_preset_updates_kafka_resource_defaults(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "kafka throughput resources",
            "brokerId": "kafka",
            "configMode": "optimized",
            "brokerTuning": {
                "setupPreset": "throughput",
                "broker": {},
                "producer": {},
                "consumer": {},
            },
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = response.json()
    assert run["resourceConfig"]["cpuRequest"] == "5"
    assert run["resourceConfig"]["cpuLimit"] == "5"
    assert run["resourceConfig"]["memoryRequest"] == "6Gi"
    assert run["resourceConfig"]["memoryLimit"] == "6Gi"
    assert run["resourceConfig"]["storageSize"] == "32Gi"
    assert run["targetReplicas"] == 1


def test_create_stop_and_reset_run(client: TestClient) -> None:
    create_response = client.post(
        "/api/runs",
        json={
            "name": "Functional test run",
            "brokerId": "rabbitmq",
            "protocol": "amqp-0-9-1",
            "messageRate": 8000,
            "messageSizeBytes": 1024,
            "producers": 2,
            "consumers": 2,
            "warmupSeconds": 0,
            "measurementSeconds": 30,
            "cooldownSeconds": 0,
        },
    )
    assert create_response.status_code == 200
    run = create_response.json()
    assert run["name"] == "Functional test run"
    assert run["brokerId"] == "rabbitmq"
    assert run["configMode"] == "baseline"
    assert run["deploymentMode"] == "normal"
    assert run["targetReplicas"] == 1
    assert run["runtimeNamespace"].startswith("bench-run-")
    assert run["progress"]["deploymentPercent"] >= 0
    assert run["metrics"] == {}

    stop_response = client.post(f"/api/runs/{run['id']}/stop")
    assert stop_response.status_code == 200
    assert stop_response.json()["status"] == "stopped"
    assert wait_for_run_status(client, run["id"], "stopped")["status"] == "stopped"

    reset_response = client.delete("/api/runs/reset-all")
    assert reset_response.status_code == 200
    assert reset_response.json()["status"] == "reset"

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()["runs"] == []


def test_transport_pattern_is_stored_and_affects_metrics(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Burst traffic",
            "brokerId": "kafka",
            "messageRate": 5000,
            "transportOptions": {
                "rateProfileKind": "burst",
            },
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = response.json()
    assert run["transportOptions"]["rateProfileKind"] == "burst"
    assert run["transportOptions"]["peakMessageRate"] == 10000
    assert run["transportOptions"]["payloadLimitHandling"] == "auto"
    assert run["transportOptions"]["reusePayloadTemplate"] is False
    assert run["transportOptions"]["estimatedWireMessageBytes"] >= 2000

    completed = wait_for_run_status(client, run["id"], "completed")
    assert completed["metrics"]["summary"]["loadProfile"]["rateProfileKind"] == "burst"
    assert completed["metrics"]["summary"]["loadProfile"]["peakMessageRate"] == 10000


def test_payload_limit_policy_auto_adjusts_large_kafka_messages(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "payload-auto.db", cluster_automation=FakeClusterAutomation())) as client:
        response = client.post(
            "/api/runs",
            json={
                "name": "large kafka payload",
                "brokerId": "kafka",
                "messageSizeBytes": 900000,
                "transportOptions": {"payloadLimitHandling": "auto"},
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = response.json()
        assert run["transportOptions"]["payloadLimitHandling"] == "auto-adjusted"
        assert run["transportOptions"]["estimatedWireMessageBytes"] > 1_048_576
        assert run["brokerTuning"]["producer"]["maxRequestSizeBytes"] > 1_048_576
        assert run["brokerTuning"]["broker"]["messageMaxBytes"] > 1_048_576


def test_payload_limit_policy_strict_rejects_large_kafka_messages(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "payload-strict.db", cluster_automation=FakeClusterAutomation())) as client:
        response = client.post(
            "/api/runs",
            json={
                "name": "strict kafka payload",
                "brokerId": "kafka",
                "messageSizeBytes": 900000,
                "transportOptions": {"payloadLimitHandling": "strict"},
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 422
        assert "expands to about" in response.json()["detail"]


def test_payload_limit_policy_auto_adjusts_large_rabbitmq_messages(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "payload-rabbitmq-auto.db", cluster_automation=FakeClusterAutomation())) as client:
        response = client.post(
            "/api/runs",
            json={
                "name": "large rabbitmq payload",
                "brokerId": "rabbitmq",
                "messageSizeBytes": 13_000_000,
                "transportOptions": {"payloadLimitHandling": "auto"},
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = response.json()
        assert run["transportOptions"]["payloadLimitHandling"] == "auto-adjusted"
        assert run["transportOptions"]["estimatedWireMessageBytes"] > 16 * 1024 * 1024
        assert run["brokerTuning"]["broker"]["maxMessageSizeBytes"] > 16 * 1024 * 1024


def test_payload_limit_policy_strict_rejects_large_rabbitmq_messages(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "payload-rabbitmq-strict.db", cluster_automation=FakeClusterAutomation())) as client:
        response = client.post(
            "/api/runs",
            json={
                "name": "strict rabbitmq payload",
                "brokerId": "rabbitmq",
                "messageSizeBytes": 13_000_000,
                "transportOptions": {"payloadLimitHandling": "strict"},
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 422
        assert "RabbitMQ max message size" in response.json()["detail"]


def test_payload_limit_policy_auto_adjusts_artemis_reject_threshold(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "payload-artemis-auto.db", cluster_automation=FakeClusterAutomation())) as client:
        response = client.post(
            "/api/runs",
            json={
                "name": "large artemis payload",
                "brokerId": "artemis",
                "messageSizeBytes": 900000,
                "brokerTuning": {
                    "broker": {
                        "maxSizeBytesRejectThresholdBytes": 1048576,
                    },
                    "producer": {},
                    "consumer": {},
                },
                "transportOptions": {"payloadLimitHandling": "auto"},
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = response.json()
        assert run["transportOptions"]["payloadLimitHandling"] == "auto-adjusted"
        assert run["brokerTuning"]["broker"]["maxSizeBytesRejectThresholdBytes"] > 1048576


def test_sequential_run_waits_behind_active_execution(tmp_path: Path) -> None:
    automation = SlowClusterAutomation()
    with TestClient(create_app(tmp_path / "sequential.db", cluster_automation=automation)) as client:
        first = client.post(
            "/api/runs",
            json={
                "name": "parallel active",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert first.status_code == 200
        active = wait_for_run_status_in(client, first.json()["id"], {"scheduled", "warmup", "measuring"})
        assert active["status"] in {"scheduled", "warmup", "measuring"}

        queued = client.post(
            "/api/runs",
            json={
                "name": "queued after active",
                "brokerId": "kafka",
                "scheduleMode": "sequential",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert queued.status_code == 200
        queued_run = queued.json()
        assert queued_run["status"] == "waiting"
        assert queued_run["queueState"] == "waiting"
        event_types = {event["event_type"] for event in client.get(f"/api/runs/{queued_run['id']}").json()["events"]}
        assert "waiting" in event_types

        automation.release.set()
        completed = wait_for_run_status(client, first.json()["id"], "completed")
        assert completed["status"] == "completed"
        started = wait_for_run_status(client, queued_run["id"], "completed")
        assert started["scheduleMode"] == "sequential"


def test_completed_zero_sample_run_includes_diagnostics(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "zero-sample.db", cluster_automation=ZeroSampleClusterAutomation())) as client:
        response = client.post(
            "/api/runs",
            json={
                "name": "zero sample run",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(client, response.json()["id"], "completed")
        diagnostic_codes = {item["code"] for item in run["diagnostics"]}
        assert "zero-samples" in diagnostic_codes


def test_only_one_active_run_allowed(client: TestClient) -> None:
    first = client.post(
        "/api/runs",
        json={
            "name": "first run",
            "brokerId": "kafka",
            "startsAt": (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
            "warmupSeconds": 5,
            "measurementSeconds": 5,
            "cooldownSeconds": 1,
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/api/runs",
        json={
            "name": "second run",
            "brokerId": "rabbitmq",
            "warmupSeconds": 1,
            "measurementSeconds": 1,
            "cooldownSeconds": 1,
        },
    )
    assert second.status_code == 409
    assert "Only one run can be active at a time" in second.text


def test_active_run_cannot_be_cleared_or_reset(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "protected active run",
            "brokerId": "kafka",
            "startsAt": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            "warmupSeconds": 2,
            "measurementSeconds": 5,
            "cooldownSeconds": 1,
        },
    )
    assert response.status_code == 200
    run = response.json()

    delete_response = client.delete(f"/api/runs/{run['id']}")
    assert delete_response.status_code == 409
    assert "wait for cleanup" in delete_response.text

    reset_response = client.delete("/api/runs/reset-all")
    assert reset_response.status_code == 409
    assert "wait for cleanup" in reset_response.text


def test_completed_run_auto_cleans_topology(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "auto cleanup",
            "brokerId": "artemis",
            "warmupSeconds": 1,
            "measurementSeconds": 1,
            "cooldownSeconds": 1,
        },
    )
    assert response.status_code == 200
    run = response.json()

    current = wait_for_run_status(client, run["id"], "completed")
    assert current["status"] == "completed"
    assert current["topologyDeletedAt"] is not None
    assert current["metrics"]["source"] == "benchmark-agent"
    assert current["metrics"]["measurement"]["timingConfidence"] == "validated"


def test_cleaning_run_is_reconciled_when_namespace_is_gone(tmp_path: Path) -> None:
    fake_cluster = FakeClusterAutomation()
    fake_cluster.wait_for_namespace_deleted_value = False
    fake_cluster.namespace_exists_value = False
    with TestClient(create_app(tmp_path / "cleanup-reconcile.db", cluster_automation=fake_cluster)) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "cleanup reconcile",
                "brokerId": "rabbitmq",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = response.json()
        completed = wait_for_run_status(test_client, run["id"], "completed")
        assert completed["topologyDeletedAt"] is not None
        reconciled = test_client.get(f"/api/runs/{run['id']}")
        assert reconciled.status_code == 200
        assert reconciled.json()["run"]["status"] == "completed"
        assert reconciled.json()["run"]["topologyDeletedAt"] is not None


def test_cleaning_run_force_cleans_stale_namespace(tmp_path: Path) -> None:
    fake_cluster = FakeClusterAutomation()
    fake_cluster.wait_for_namespace_deleted_value = False
    fake_cluster.namespace_exists_value = True
    db_path = tmp_path / "cleanup-force.db"
    with TestClient(create_app(db_path, cluster_automation=fake_cluster)) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "cleanup force reconcile",
                "brokerId": "nats",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = response.json()
        current = wait_for_run_status(test_client, run["id"], "cleaning")
        assert current["status"] == "cleaning"
        assert current["topologyDeletedAt"] is None

        stale_completed_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                "UPDATE runs SET completed_at = ? WHERE id = ?",
                (stale_completed_at, run["id"]),
            )
            connection.commit()

        reconciled = test_client.get(f"/api/runs/{run['id']}")
        assert reconciled.status_code == 200
        assert run["id"] in fake_cluster.force_deleted_namespace_run_ids
        assert reconciled.json()["run"]["status"] == "completed"
        assert reconciled.json()["run"]["topologyDeletedAt"] is not None


def test_report_generation_uses_measured_run_data(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "report source",
            "brokerId": "rabbitmq",
            "protocol": "amqp-0-9-1",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = response.json()
    completed = wait_for_run_status(client, run["id"], "completed")
    assert completed["metrics"]["measurement"]["primaryMetric"] == "producer_send_to_consumer_receive"

    report_response = client.post(
        "/api/reports",
        json={
            "title": "Measured run report",
            "runIds": [run["id"]],
        },
    )
    assert report_response.status_code == 200
    report = report_response.json()
    assert report["status"] == "ready"
    assert report["downloadUrl"].endswith("/download")
    assert report["runIds"] == [run["id"]]

    detail_response = client.get(f"/api/reports/{report['id']}")
    assert detail_response.status_code == 200
    assert detail_response.json()["title"] == "Measured run report"

    download_response = client.get(report["downloadUrl"])
    assert download_response.status_code == 200
    assert download_response.headers["content-type"].startswith("application/pdf")
    assert download_response.content.startswith(b"%PDF")


def test_report_generation_supports_more_than_two_measured_runs(client: TestClient) -> None:
    run_ids: list[str] = []
    for name in ["compare source 1", "compare source 2", "compare source 3"]:
        response = client.post(
            "/api/runs",
            json={
                "name": name,
                "brokerId": "rabbitmq",
                "protocol": "amqp-0-9-1",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = response.json()
        completed = wait_for_run_status(client, run["id"], "completed")
        assert completed["metrics"]["source"] == "benchmark-agent"
        run_ids.append(run["id"])

    report_response = client.post(
        "/api/reports",
        json={
            "title": "Measured comparison report",
            "runIds": run_ids,
        },
    )
    assert report_response.status_code == 200
    report = report_response.json()
    assert report["status"] == "ready"
    assert report["runIds"] == run_ids

    download_response = client.get(report["downloadUrl"])
    assert download_response.status_code == 200
    assert download_response.headers["content-type"].startswith("application/pdf")
    assert download_response.content.startswith(b"%PDF")


def test_create_run_accepts_resource_config_aliases(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Resource config aliases",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
            "resource_config": {
                "cpuRequest": "750m",
                "cpuLimit": "2",
                "memoryRequest": "1Gi",
                "memoryLimit": "2Gi",
                "storageSize": "12Gi",
                "replicas": 2,
            },
        },
    )
    assert response.status_code == 200
    run = response.json()
    assert run["resourceConfig"]["cpuRequest"] == "750m"
    assert run["resourceConfig"]["cpuLimit"] == "2"
    assert run["resourceConfig"]["replicas"] == 2


def test_report_generation_honors_selected_sections(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Sectioned report source",
            "brokerId": "rabbitmq",
            "protocol": "amqp-0-9-1",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = wait_for_run_status(client, response.json()["id"], "completed")

    report_response = client.post(
        "/api/reports",
        json={
            "title": "Sectioned report",
            "runIds": [run["id"]],
            "sections": ["summary", "configuration"],
        },
    )
    assert report_response.status_code == 200
    report = report_response.json()
    assert report["status"] == "ready"
    assert report["sections"] == ["summary", "configuration"]

    download_response = client.get(report["downloadUrl"])
    assert download_response.status_code == 200
    assert download_response.content.startswith(b"%PDF")


def test_report_artifact_is_regenerated_after_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report_dir = tmp_path / "reports"
    monkeypatch.setenv("BUS_REPORT_DIR", str(report_dir))

    db_path = tmp_path / "restart-recovery.db"
    with TestClient(create_app(db_path, cluster_automation=FakeClusterAutomation())) as first_client:
        response = first_client.post(
            "/api/runs",
            json={
                "name": "restart recovery source",
                "brokerId": "rabbitmq",
                "protocol": "amqp-0-9-1",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(first_client, response.json()["id"], "completed")

        report_response = first_client.post(
            "/api/reports",
            json={
                "title": "Restart recovery report",
                "runIds": [run["id"]],
                "sections": ["summary", "configuration"],
            },
        )
        assert report_response.status_code == 200
        report = report_response.json()
        assert report["status"] == "ready"

        pdf_path = report_dir / report["fileName"]
        assert pdf_path.exists()
        pdf_path.unlink()
        assert not pdf_path.exists()

    with TestClient(create_app(db_path, cluster_automation=FakeClusterAutomation())) as restarted_client:
        for _ in range(30):
            report_detail = restarted_client.get(f"/api/reports/{report['id']}")
            assert report_detail.status_code == 200
            if pdf_path.exists():
                break
            time.sleep(0.1)

        assert pdf_path.exists()
        detail_payload = report_detail.json()
        assert detail_payload["sections"] == ["summary", "configuration"]

        download_response = restarted_client.get(f"/api/reports/{report['id']}/download")
        assert download_response.status_code == 200
        assert download_response.content.startswith(b"%PDF")


def test_report_title_auto_suffixes_existing_case_insensitive_title(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Duplicate report source",
            "brokerId": "rabbitmq",
            "protocol": "amqp-0-9-1",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = wait_for_run_status(client, response.json()["id"], "completed")

    first = client.post(
        "/api/reports",
        json={
            "title": "Benchmark Report",
            "runIds": [run["id"]],
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/api/reports",
        json={
            "title": "benchmark report",
            "runIds": [run["id"]],
        },
    )
    assert second.status_code == 200
    assert second.json()["title"].startswith("benchmark report 20")


def test_report_title_suffix_keeps_incrementing(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "report duplicate source",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = wait_for_run_status(client, response.json()["id"], "completed")

    first_report = client.post(
        "/api/reports",
        json={
            "title": "Case Report",
            "runIds": [run["id"]],
        },
    )
    assert first_report.status_code == 200

    duplicate_report = client.post(
        "/api/reports",
        json={
            "title": "case report",
            "runIds": [run["id"]],
        },
    )
    assert duplicate_report.status_code == 200
    assert duplicate_report.json()["title"].startswith("case report 20")

    third_report = client.post(
        "/api/reports",
        json={
            "title": "Case Report",
            "runIds": [run["id"]],
        },
    )
    assert third_report.status_code == 200
    assert third_report.json()["title"].startswith("Case Report 20")


def test_completed_run_persists_measurement_artifacts(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "artifact source",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = wait_for_run_status(client, response.json()["id"], "completed")
    raw_store = run["metrics"]["measurement"]["rawRecordStore"]
    assert raw_store["producerRecordCount"] == 2
    assert raw_store["consumerRecordCount"] == 2
    assert raw_store["resourceSampleCount"] == 1

    artifact_response = client.get(f"/api/runs/{run['id']}/artifacts")
    assert artifact_response.status_code == 200
    artifacts = artifact_response.json()["artifacts"]
    assert {item["artifactType"] for item in artifacts} == {
        "producer-raw-records",
        "consumer-raw-records",
        "resource-samples",
    }

    download_response = client.get(artifacts[0]["downloadUrl"])
    assert download_response.status_code == 200
    assert download_response.content


def test_completed_run_accepts_chunked_timeseries_summary(tmp_path: Path) -> None:
    with TestClient(
        create_app(tmp_path / "chunked-timeseries.db", cluster_automation=ChunkedSummaryClusterAutomation())
    ) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "chunked timeseries source",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(test_client, response.json()["id"], "completed")
        assert run["metrics"]["summary"]["throughput"]["achieved"] > 0
        assert len(run["metrics"]["timeseries"]) == 2


def test_completed_run_accepts_timeseries_only_material(tmp_path: Path) -> None:
    with TestClient(
        create_app(tmp_path / "timeseries-only.db", cluster_automation=TimeseriesOnlyClusterAutomation())
    ) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "timeseries only source",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(test_client, response.json()["id"], "completed")
        assert run["metrics"]["summary"]["throughput"]["attempted"] > 0
        assert run["metrics"]["summary"]["throughput"]["achieved"] > 0
        assert len(run["metrics"]["timeseries"]) == 2


def test_completed_run_accepts_file_backed_log_sources(tmp_path: Path) -> None:
    with TestClient(
        create_app(
            tmp_path / "file-backed.db",
            cluster_automation=FileBackedClusterAutomation(tmp_path / "logs"),
        )
    ) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "file backed source",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(test_client, response.json()["id"], "completed")
        assert run["metrics"]["summary"]["throughput"]["achieved"] > 0
        assert not (tmp_path / "logs" / "producer.log").exists()
        assert not (tmp_path / "logs" / "consumer.log").exists()


def test_detached_finalizer_completes_run_and_preserves_export_import(tmp_path: Path) -> None:
    db_path = tmp_path / "detached-finalizer.db"
    automation = DetachedFinalizerClusterAutomation(db_path)
    with TestClient(create_app(db_path, cluster_automation=automation)) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "detached finalizer source",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(test_client, response.json()["id"], "completed")
        assert automation.run_finalizer_state(run["id"]) == "succeeded"

        details = test_client.get(f"/api/runs/{run['id']}")
        assert details.status_code == 200
        event_types = {event["event_type"] for event in details.json()["events"]}
        assert "finalizer-running" in event_types
        assert "finalizer-started" in event_types
        assert "metrics-collected" in event_types

        export_response = test_client.post("/api/runs/export", json={"runIds": [run["id"]]})
        assert export_response.status_code == 200
        bundle = export_response.json()

        reset_response = test_client.delete("/api/runs/reset-all")
        assert reset_response.status_code == 200
        assert run["id"] in automation.deleted_finalizer_jobs

        import_response = test_client.post("/api/runs/import", json=bundle)
        assert import_response.status_code == 200
        imported_id = import_response.json()["importedRuns"][0]["id"]
        imported_run = test_client.get(f"/api/runs/{imported_id}").json()["run"]
        assert imported_run["status"] == "completed"
        assert imported_run["metrics"]["summary"]["endToEndLatencyMs"]["count"] > 0


def test_detached_finalizer_is_not_launched_before_run_window_finishes(tmp_path: Path) -> None:
    db_path = tmp_path / "detached-finalizer-delay.db"
    automation = DetachedFinalizerClusterAutomation(db_path)
    with TestClient(create_app(db_path, cluster_automation=automation)) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "detached finalizer delay",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 5,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run_id = response.json()["id"]
        time.sleep(0.5)
        assert automation.run_finalizer_state(run_id) == "not_found"


def test_detached_finalizer_completion_advances_sequential_queue(tmp_path: Path) -> None:
    db_path = tmp_path / "detached-finalizer-queue.db"
    automation = DetachedFinalizerClusterAutomation(db_path)
    with TestClient(create_app(db_path, cluster_automation=automation)) as test_client:
        first = test_client.post(
            "/api/runs",
            json={
                "name": "detached queue first",
                "brokerId": "kafka",
                "scheduleMode": "sequential",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert first.status_code == 200
        second = test_client.post(
            "/api/runs",
            json={
                "name": "detached queue second",
                "brokerId": "kafka",
                "scheduleMode": "sequential",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert second.status_code == 200
        queued = second.json()
        assert queued["status"] == "waiting"
        assert queued["queueState"] == "waiting"

        first_run = wait_for_run_status(test_client, first.json()["id"], "completed", timeout_seconds=10.0)
        assert first_run["status"] == "completed"
        second_run = wait_for_run_status(test_client, queued["id"], "completed", timeout_seconds=12.0)
        assert second_run["status"] == "completed"
        assert automation.run_finalizer_state(second_run["id"]) == "succeeded"


def test_detached_finalizer_failure_preserves_structured_output_for_recovery(tmp_path: Path) -> None:
    db_path = tmp_path / "broken-detached-finalizer.db"
    automation = BrokenDetachedFinalizerClusterAutomation(db_path)
    with TestClient(create_app(db_path, cluster_automation=automation)) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "broken detached finalizer source",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(test_client, response.json()["id"], "stopped")
        assert automation.run_finalizer_state(run["id"]) == "failed"
        assert run["id"] in automation.deleted_benchmark_job_run_ids
        assert run["id"] not in automation.deleted_structured_output_run_ids


def test_execution_clock_does_not_start_before_benchmark_jobs_are_ready(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "launch-failure.db", cluster_automation=FailingLaunchClusterAutomation())) as test_client:
        response = test_client.post(
            "/api/runs",
            json={
                "name": "launch failure",
                "brokerId": "kafka",
                "warmupSeconds": 1,
                "measurementSeconds": 5,
                "cooldownSeconds": 1,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(test_client, response.json()["id"], "stopped")
        assert run["executionStartedAt"] is None

        details = test_client.get(f"/api/runs/{run['id']}")
        assert details.status_code == 200
        event_types = {event["event_type"] for event in details.json()["events"]}
        assert "test-started" not in event_types
        assert "warmup-started" not in event_types
        assert "measurement-started" not in event_types


def test_results_export_import_round_trip(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "portable result source",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = wait_for_run_status(client, response.json()["id"], "completed")

    export_response = client.post("/api/runs/export", json={"runIds": [run["id"]]})
    assert export_response.status_code == 200
    assert "attachment;" in export_response.headers["content-disposition"].lower()
    bundle = export_response.json()
    assert bundle["kind"] == "concerto-bus-runs-export"
    assert bundle["version"] == 1
    assert len(bundle["runs"]) == 1

    reset_response = client.delete("/api/runs/reset-all")
    assert reset_response.status_code == 200

    import_response = client.post("/api/runs/import", json=bundle)
    assert import_response.status_code == 200
    imported = import_response.json()
    assert imported["status"] == "imported"
    assert imported["count"] == 1

    imported_id = imported["importedRuns"][0]["id"]
    run_response = client.get(f"/api/runs/{imported_id}")
    assert run_response.status_code == 200
    imported_run = run_response.json()["run"]
    assert imported_run["status"] == "completed"
    assert imported_run["metrics"]["source"] == "benchmark-agent"
    assert imported_run["metrics"]["summary"]["endToEndLatencyMs"]["count"] > 0
    listing_response = client.get("/api/runs")
    assert listing_response.status_code == 200
    assert imported_id in {item["id"] for item in listing_response.json()["runs"]}

    artifact_response = client.get(f"/api/runs/{imported_id}/artifacts")
    assert artifact_response.status_code == 200
    artifacts = artifact_response.json()["artifacts"]
    assert {item["artifactType"] for item in artifacts} == {
        "producer-raw-records",
        "consumer-raw-records",
        "resource-samples",
    }


def test_create_app_defers_bootstrap_and_storage_init_until_runtime_phase(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    class StubStorage:
        def __init__(self) -> None:
            self.report_dir = tmp_path / "reports"
            self.artifact_dir = tmp_path / "artifacts"
            self.mode = "local"

        def ensure_ready(self) -> None:
            calls.append("storage-ready")

    class BootstrapClusterAutomation(FakeClusterAutomation):
        def __init__(self) -> None:
            super().__init__()
            self.operator_bootstrap_enabled = True

        def delete_legacy_broker_namespaces(self) -> None:
            calls.append("delete-legacy")

    monkeypatch.setattr(
        "services.platform.app.resolve_runtime_storage",
        lambda **_: calls.append("resolve-storage") or StubStorage(),
    )

    app = create_app(cluster_automation=BootstrapClusterAutomation())
    assert app is not None
    assert calls == ["resolve-storage"]

    with TestClient(app):
        pass

    assert calls == ["resolve-storage", "storage-ready"]


def test_bootstrap_endpoint_uses_requested_scope(client: TestClient) -> None:
    automation = client.app.state.cluster_automation
    assert automation.bootstrap_scope_calls == []

    response = client.post("/api/system/bootstrap", json={"scope": "platform-services"})
    assert response.status_code == 200
    assert automation.bootstrap_scope_calls == ["platform-services"]

    payload = response.json()
    assert payload["storage"]["ready"] is True
    assert payload["operators"]["kafka"]["ready"] is True


def test_completed_run_keeps_metrics_when_failure_happens_after_measurement(tmp_path: Path) -> None:
    with TestClient(
        create_app(tmp_path / "late-failure.db", cluster_automation=LateFailureClusterAutomation())
    ) as late_client:
        response = late_client.post(
            "/api/runs",
            json={
                "name": "late failure source",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 1,
            },
        )
        assert response.status_code == 200
        run = wait_for_run_status(late_client, response.json()["id"], "completed")
        assert run["metrics"]["summary"]["endToEndLatencyMs"]["count"] > 0
        assert "postMeasurementFailure" in run["metrics"]["measurement"]

        details = late_client.get(f"/api/runs/{run['id']}")
        assert details.status_code == 200
        event_types = {event["event_type"] for event in details.json()["events"]}
        assert "degraded-completion" in event_types


def test_status_progression_from_scheduled_to_completed(client: TestClient) -> None:
    create_response = client.post(
        "/api/runs",
        json={
            "name": "Window progression",
            "brokerId": "kafka",
            "warmupSeconds": 1,
            "measurementSeconds": 1,
            "cooldownSeconds": 1,
        },
    )
    run = create_response.json()

    run_response = client.get(f"/api/runs/{run['id']}")
    assert run_response.status_code == 200
    assert run_response.json()["run"]["status"] in {"preparing", "warmup", "measuring", "completed"}

    completed = wait_for_run_status(client, run["id"], "completed")
    assert completed["status"] == "completed"


def test_rejects_unknown_broker(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Bad broker",
            "brokerId": "unknown",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 422


def test_rejects_unsupported_protocol(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Bad protocol",
            "brokerId": "kafka",
            "protocol": "amqp-0-9-1",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 422


def test_rejects_invalid_rabbitmq_frame_max(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Bad rabbit frame",
            "brokerId": "rabbitmq",
            "protocol": "amqp-0-9-1",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
            "brokerTuning": {
                "producer": {
                    "frameMax": 262144,
                }
            },
        },
    )
    assert response.status_code == 422
    assert "131072" in response.json()["detail"]


def test_rejects_unsupported_config_mode(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Bad mode",
            "brokerId": "kafka",
            "configMode": "turbo",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 422


def test_ha_mode_sets_three_replicas(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "HA run",
            "brokerId": "kafka",
            "deploymentMode": "ha",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = response.json()
    assert run["deploymentMode"] == "ha"
    assert run["targetReplicas"] == 3


def test_resource_replica_override_wins_over_topology_default(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "HA override run",
            "brokerId": "kafka",
            "deploymentMode": "ha",
            "resourceConfig": {"replicas": 5},
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    run = response.json()
    assert run["deploymentMode"] == "ha"
    assert run["targetReplicas"] == 5
    assert run["resourceConfig"]["replicas"] == 5


def test_rejects_unsupported_deployment_mode(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Bad deployment mode",
            "brokerId": "kafka",
            "deploymentMode": "clustered",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 422


def test_rejects_unknown_scenario_preset(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "Bad preset",
            "brokerId": "kafka",
            "scenarioId": "not-a-real-preset",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 422


def test_rejects_blank_run_name_after_trim(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "   ",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 422


def test_trims_run_name_before_persisting(client: TestClient) -> None:
    response = client.post(
        "/api/runs",
        json={
            "name": "  tidy name  ",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 10,
            "cooldownSeconds": 0,
        },
    )
    assert response.status_code == 200
    assert response.json()["name"] == "tidy name"


def test_rejects_duplicate_run_name_case_insensitively(client: TestClient) -> None:
    first = client.post(
        "/api/runs",
        json={
            "name": "Duplicate Name",
            "brokerId": "kafka",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/api/runs",
        json={
            "name": "duplicate name",
            "brokerId": "rabbitmq",
            "warmupSeconds": 0,
            "measurementSeconds": 1,
            "cooldownSeconds": 0,
        },
    )
    assert second.status_code == 409
    assert "already exists" in second.text


def test_rejects_run_creation_when_cluster_execution_disabled(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "disabled.db")) as disabled_client:
        response = disabled_client.post(
            "/api/runs",
            json={
                "name": "disabled run",
                "brokerId": "kafka",
                "warmupSeconds": 0,
                "measurementSeconds": 1,
                "cooldownSeconds": 0,
            },
        )
    assert response.status_code == 503
