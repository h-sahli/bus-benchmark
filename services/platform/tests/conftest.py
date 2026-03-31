import sys
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class FakeClusterAutomation:
    def __init__(self) -> None:
        self.enabled = True
        self.operator_bootstrap_enabled = True
        self.agent_start_barrier_seconds = 1
        self.deleted_namespaces: list[str] = []

    def status(self) -> dict:
        return {
            "enabled": True,
            "attempted": True,
            "ready": True,
            "message": "control planes ready",
            "operators": {
                "kafka": {"label": "Kafka", "ready": True, "message": "ready"},
                "rabbitmq": {"label": "RabbitMQ", "ready": True, "message": "ready"},
                "artemis": {"label": "Artemis", "ready": True, "message": "ready"},
                "nats": {"label": "NATS JetStream", "ready": True, "message": "ready"},
            },
            "platformOperators": {
                "cnpg": {"label": "CloudNativePG", "ready": True, "message": "ready"},
                "minio": {"label": "MinIO Operator", "ready": True, "message": "ready"},
            },
            "platformServices": {
                "postgres": {"label": "PostgreSQL", "ready": True, "message": "ready"},
                "objectStore": {"label": "S3 Store", "ready": True, "message": "ready"},
            },
        }

    def ensure_operators(self) -> dict:
        return self.status()

    def delete_legacy_broker_namespaces(self) -> None:
        return None

    def delete_run_namespace(self, run_id: str) -> None:
        self.deleted_namespaces.append(run_id)

    def wait_for_namespace_deleted(self, run_id: str, timeout_seconds: int = 180) -> bool:
        del run_id, timeout_seconds
        return True

    def prepare_run_environment(
        self,
        *,
        run_id: str,
        broker_id: str,
        config_mode: str,
        deployment_mode: str,
        broker_tuning: dict | None,
        resource_config: dict | None = None,
        emit,
    ) -> bool:
        del broker_id, config_mode, deployment_mode, broker_tuning, resource_config
        emit("prepare-started", f"Preparing namespace for {run_id}.")
        emit("namespace-ready", "Namespace created.")
        emit("broker-waiting", "Waiting for broker readiness.")
        emit("broker-ready", "Broker ready.")
        return True

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
        transport_options: dict | None,
        broker_tuning: dict | None,
        resource_config: dict | None,
        scheduled_start_ns: int | None,
        emit,
    ) -> dict:
        del scenario_id, broker_tuning, deployment_mode, resource_config
        emit("consumer-running", "Consumer job created.")
        emit("producer-running", "Producer job created.")
        peak_rate = int((transport_options or {}).get("peakMessageRate", message_rate) or message_rate)
        rate_profile_kind = str((transport_options or {}).get("rateProfileKind", "constant") or "constant")
        producer_record = {
            "role": "producer",
            "runId": run_id,
            "broker": broker_id,
            "result": {
                "attempted": message_rate * measurement_seconds,
                "sent": message_rate * measurement_seconds,
                "publishErrors": 0,
                "ackLatencyHistogramMs": [
                    {"lowerMs": 0.0, "upperMs": 0.5, "count": 24},
                    {"lowerMs": 0.5, "upperMs": 1.0, "count": 40},
                    {"lowerMs": 1.0, "upperMs": 2.0, "count": 16},
                ],
                "timeseries": [
                    {
                        "second": second,
                        "attemptedMessages": message_rate,
                        "sentMessages": message_rate,
                    }
                    for second in range(max(1, measurement_seconds))
                ],
            },
        }
        consumer_record = {
            "role": "consumer",
            "runId": run_id,
            "broker": broker_id,
            "result": {
                "received": message_rate * measurement_seconds,
                "receivedMeasured": message_rate * measurement_seconds,
                "parseErrors": 0,
                "duplicates": 0,
                "outOfOrder": 0,
                "latencyHistogramMs": [
                    {"lowerMs": 0.5, "upperMs": 1.0, "count": 20},
                    {"lowerMs": 1.0, "upperMs": 2.0, "count": 42},
                    {"lowerMs": 2.0, "upperMs": 5.0, "count": 18},
                ],
                "timeseries": [
                    {
                        "second": second,
                        "deliveredMessages": message_rate,
                        "latencySamplesMs": [1.1, 1.3, 1.6, 2.2],
                    }
                    for second in range(max(1, measurement_seconds))
                ],
                "measurementContract": {
                    "primaryMetric": "producer_send_to_consumer_receive",
                },
            },
        }
        return {
            "producerLogs": [json.dumps(producer_record)],
            "consumerLogs": [json.dumps(consumer_record)],
            "nodeNames": ["wsl-node"],
            "timingConfidence": "validated",
            "scheduledStartNs": scheduled_start_ns or 123456789,
            "transportOptionsEcho": {
                "rateProfileKind": rate_profile_kind,
                "peakMessageRate": peak_rate,
                "messageSizeBytes": message_size_bytes,
                "producers": producers,
                "consumers": consumers,
                "warmupSeconds": warmup_seconds,
                "measurementSeconds": measurement_seconds,
                "cooldownSeconds": cooldown_seconds,
                "configMode": config_mode,
            },
        }
