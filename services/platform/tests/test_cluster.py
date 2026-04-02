import json
import time
from pathlib import Path
from types import SimpleNamespace

import yaml

from services.platform.cluster import (
    ClusterAutomation,
    _measurement_window_ended,
    _build_minio_tenant_values,
    _build_nats_helm_values,
    finalizer_job_name_for,
    patch_broker_manifest_documents,
    run_namespace_for,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_run_namespace_for_is_stable() -> None:
    assert run_namespace_for("12345678-abcd-efgh-ijkl-1234567890ab") == "bench-run-12345678"


def test_patch_kafka_manifest_rewrites_namespace_name_and_labels() -> None:
    documents, namespace, broker_name = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        config_mode="baseline",
        deployment_mode="normal",
        broker_tuning={"broker": {"minInSyncReplicas": 2, "defaultPartitions": 3}},
    )

    kafka_doc = next(document for document in documents if document["kind"] == "Kafka")
    controller_pool = next(
        document for document in documents
        if document["kind"] == "KafkaNodePool" and document["metadata"]["name"].endswith("-controllers")
    )
    broker_pool = next(
        document for document in documents
        if document["kind"] == "KafkaNodePool" and document["metadata"]["name"].endswith("-brokers")
    )
    topic_doc = next(document for document in documents if document["kind"] == "KafkaTopic")
    configmap = next(document for document in documents if document["kind"] == "ConfigMap")

    assert namespace == "bench-run-12345678"
    assert broker_name == "kafka-12345678"
    assert kafka_doc["metadata"]["namespace"] == namespace
    assert kafka_doc["metadata"]["name"] == broker_name
    assert controller_pool["metadata"]["labels"]["strimzi.io/cluster"] == broker_name
    assert broker_pool["metadata"]["labels"]["strimzi.io/cluster"] == broker_name
    assert controller_pool["spec"]["roles"] == ["controller"]
    assert broker_pool["spec"]["roles"] == ["broker"]
    assert kafka_doc["spec"]["kafka"]["config"]["min.insync.replicas"] == "2"
    assert kafka_doc["spec"]["kafka"]["config"]["num.partitions"] == "3"
    assert topic_doc["spec"]["topicName"] == "benchmark.events"
    assert topic_doc["spec"]["partitions"] == 3
    assert topic_doc["spec"]["config"]["min.insync.replicas"] == 1
    assert topic_doc["metadata"]["labels"]["strimzi.io/cluster"] == broker_name
    assert configmap["metadata"]["namespace"] == namespace


def test_patch_kafka_manifest_scales_topic_for_throughput_setup() -> None:
    documents, _, _ = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="ha",
        broker_tuning={
            "setupPreset": "throughput",
            "broker": {
                "defaultPartitions": 6,
                "defaultReplicationFactor": 3,
                "minInSyncReplicas": 2,
            },
        },
        producers=10,
        consumers=8,
    )

    topic_doc = next(document for document in documents if document["kind"] == "KafkaTopic")

    assert topic_doc["spec"]["partitions"] == 10
    assert topic_doc["spec"]["replicas"] == 3
    assert topic_doc["spec"]["config"]["min.insync.replicas"] == 2


def test_patch_kafka_manifest_uses_three_partitions_by_default() -> None:
    documents, _, _ = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        config_mode="baseline",
        deployment_mode="normal",
        broker_tuning={"broker": {}},
        producers=1,
        consumers=1,
    )

    topic_doc = next(document for document in documents if document["kind"] == "KafkaTopic")

    assert topic_doc["spec"]["partitions"] == 3


def test_patch_kafka_manifest_applies_resource_overrides_only_to_broker_pool() -> None:
    documents, _, broker_name = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="ha",
        broker_tuning={"broker": {"defaultReplicationFactor": 3}},
        resource_config={
            "cpuRequest": "5",
            "cpuLimit": "5",
            "memoryRequest": "10Gi",
            "memoryLimit": "10Gi",
            "storageSize": "40Gi",
            "replicas": 5,
        },
    )

    controller_pool = next(
        document for document in documents
        if document["kind"] == "KafkaNodePool" and document["metadata"]["name"] == f"{broker_name}-controllers"
    )
    broker_pool = next(
        document for document in documents
        if document["kind"] == "KafkaNodePool" and document["metadata"]["name"] == f"{broker_name}-brokers"
    )
    topic_doc = next(document for document in documents if document["kind"] == "KafkaTopic")

    assert controller_pool["spec"]["replicas"] == 3
    assert controller_pool["spec"]["resources"]["requests"]["cpu"] == "250m"
    assert broker_pool["spec"]["replicas"] == 5
    assert broker_pool["spec"]["resources"]["requests"]["cpu"] == "5"
    assert broker_pool["spec"]["resources"]["limits"]["memory"] == "10Gi"
    assert broker_pool["spec"]["storage"]["size"] == "40Gi"
    assert topic_doc["spec"]["replicas"] == 3


def test_patch_artemis_manifest_updates_apply_to_names() -> None:
    documents, namespace, broker_name = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="87654321-abcd-efgh-ijkl-1234567890ab",
        broker_id="artemis",
        config_mode="optimized",
        deployment_mode="ha",
        broker_tuning={"broker": {"globalMaxSizeMb": 2048, "maxSizeBytesRejectThresholdBytes": 2097152}},
    )

    broker_doc = next(document for document in documents if document["kind"] == "ActiveMQArtemis")
    address_doc = next(document for document in documents if document["kind"] == "ActiveMQArtemisAddress")

    assert namespace == "bench-run-87654321"
    assert broker_doc["metadata"]["name"] == broker_name
    assert address_doc["spec"]["applyToCrNames"] == [broker_name]
    assert "global-max-size=2048m" in broker_doc["spec"]["brokerProperties"]
    assert "address-setting.#.max-size-bytes-reject-threshold=2097152" in broker_doc["spec"]["brokerProperties"]


def test_patch_rabbitmq_manifest_adds_additional_config() -> None:
    documents, namespace, broker_name = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="feedface-abcd-efgh-ijkl-1234567890ab",
        broker_id="rabbitmq",
        config_mode="optimized",
        deployment_mode="ha",
        broker_tuning={"broker": {"defaultQueueType": "quorum", "consumerTimeoutMs": 60000, "maxMessageSizeBytes": 33554432}},
    )

    rabbit_doc = next(document for document in documents if document["kind"] == "RabbitmqCluster")
    assert namespace == "bench-run-feedface"
    assert rabbit_doc["metadata"]["name"] == broker_name
    additional_config = rabbit_doc["spec"]["rabbitmq"]["additionalConfig"]
    assert "default_queue_type = quorum" in additional_config
    assert "consumer_timeout = 60000" in additional_config
    assert "max_message_size = 33554432" in additional_config


def test_patch_nats_manifest_rewrites_identity_and_resources() -> None:
    documents, namespace, broker_name = patch_broker_manifest_documents(
        repo_root=REPO_ROOT,
        run_id="facefeed-abcd-efgh-ijkl-1234567890ab",
        broker_id="nats",
        config_mode="optimized",
        deployment_mode="ha",
        broker_tuning={"broker": {"maxPayload": 2097152, "maxMemoryStore": 4096}},
        resource_config={
            "cpuRequest": "3",
            "cpuLimit": "3",
            "memoryRequest": "6Gi",
            "memoryLimit": "6Gi",
            "storageSize": "30Gi",
            "replicas": 3,
        },
    )

    service_doc = next(document for document in documents if document["kind"] == "Service")
    statefulset_doc = next(document for document in documents if document["kind"] == "StatefulSet")

    assert namespace == "bench-run-facefeed"
    assert service_doc["metadata"]["name"] == broker_name
    assert service_doc["spec"]["selector"]["app"] == broker_name
    assert statefulset_doc["metadata"]["name"] == broker_name
    assert statefulset_doc["spec"]["serviceName"] == broker_name
    assert statefulset_doc["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] == "3"
    assert statefulset_doc["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"] == "30Gi"
    args = statefulset_doc["spec"]["template"]["spec"]["containers"][0]["args"]
    assert any(argument.startswith("--max_payload=2097152") for argument in args)
    assert any(argument.startswith("--max_mem_store=4096MB") for argument in args)
    assert any(broker_name in argument for argument in args if argument.startswith("--routes="))


def test_build_nats_helm_values_uses_chart_friendly_runtime_settings() -> None:
    values = _build_nats_helm_values(
        run_id="facefeed-abcd-efgh-ijkl-1234567890ab",
        broker_name="nats-facefeed",
        deployment_mode="ha",
        broker_tuning={"broker": {"maxPayload": 2097152, "maxMemoryStore": 4096}},
        resource_config={
            "cpuRequest": "3",
            "cpuLimit": "3",
            "memoryRequest": "6Gi",
            "memoryLimit": "6Gi",
            "storageSize": "30Gi",
            "storageClassName": "local-path",
            "replicas": 3,
        },
    )

    assert values["fullnameOverride"] == "nats-facefeed"
    assert values["config"]["cluster"]["enabled"] is True
    assert values["config"]["cluster"]["replicas"] == 3
    assert values["config"]["jetstream"]["fileStore"]["pvc"]["size"] == "30Gi"
    assert values["config"]["jetstream"]["fileStore"]["pvc"]["storageClassName"] == "local-path"
    assert values["config"]["jetstream"]["memoryStore"]["maxSize"] == "4096Mi"
    assert values["config"]["merge"]["max_payload"] == 2097152
    assert values["container"]["resources"]["requests"]["cpu"] == "3"
    assert values["container"]["resources"]["limits"]["memory"] == "6Gi"
    assert values["natsBox"]["enabled"] is False
    assert values["promExporter"]["enabled"] is False


def test_deploy_nats_runtime_uses_configured_chart_timeout(monkeypatch) -> None:
    monkeypatch.setenv("BUS_NATS_RUNTIME_CHART_TIMEOUT", "17m")
    automation = ClusterAutomation(REPO_ROOT)
    commands: list[list[str]] = []

    def fake_run(arguments: list[str], **_: object) -> SimpleNamespace:
        commands.append(arguments)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(automation, "_ensure_nats_chart_repo", lambda: None)
    monkeypatch.setattr(automation, "_run", fake_run)

    automation._deploy_nats_runtime(
        namespace="bench-run-facefeed",
        broker_name="nats-facefeed",
        run_id="facefeed-abcd-efgh-ijkl-1234567890ab",
        config_mode="optimized",
        deployment_mode="ha",
        broker_tuning={"broker": {"maxPayload": 2097152}},
        resource_config={
            "cpuRequest": "3",
            "cpuLimit": "3",
            "memoryRequest": "6Gi",
            "memoryLimit": "6Gi",
            "storageSize": "30Gi",
            "storageClassName": "local-path",
            "replicas": 3,
        },
    )

    helm_command = next(
        command for command in commands if command[:3] == ["helm", "upgrade", "--install"]
    )
    timeout_index = helm_command.index("--timeout")
    assert helm_command[timeout_index + 1] == "17m"


def test_build_minio_tenant_values_uses_object_entries_for_users() -> None:
    values = _build_minio_tenant_values(
        tenant_name="bus-platform-store",
        root_secret_name="bus-platform-store-root",
        app_user_secret_name="bus-platform-store-app-user",
        bucket_name="bus-platform-artifacts",
        storage_size="20Gi",
        storage_class_name="local-path",
    )

    tenant = values["tenant"]
    assert tenant["name"] == "bus-platform-store"
    assert tenant["configSecret"]["name"] == "bus-platform-store-root"
    assert tenant["users"] == [{"name": "bus-platform-store-app-user"}]
    assert tenant["buckets"][0]["name"] == "bus-platform-artifacts"
    assert tenant["pools"][0]["storageClassName"] == "local-path"


def test_capacity_shortfall_detail_reports_cluster_limit(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)

    def fake_run_json(arguments, *, check=False):
        del check
        if arguments[:3] == ["kubectl", "get", "nodes"]:
            return {
                "items": [
                    {
                        "spec": {},
                        "status": {
                            "allocatable": {
                                "cpu": "2",
                                "memory": "4Gi",
                            }
                        },
                    }
                ]
            }
        return {}

    monkeypatch.setattr(automation, "_run_json", fake_run_json)

    detail = automation._capacity_shortfall_detail(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="normal",
        broker_tuning={"setupPreset": "latency", "broker": {}, "producer": {}, "consumer": {}},
        producers=1,
        consumers=1,
        resource_config={
            "cpuRequest": "2500m",
            "cpuLimit": "2500m",
            "memoryRequest": "4Gi",
            "memoryLimit": "4Gi",
            "storageSize": "24Gi",
            "replicas": 1,
        },
    )

    assert detail is not None
    assert "Cluster capacity check failed" in detail
    assert "CPU request 3.73 exceeds schedulable allocatable 2" in detail
    assert "memory request 7Gi exceeds schedulable allocatable 4Gi" in detail
    assert "Broker pool from the UI/request is 2.5 CPU and 4Gi memory" in detail
    assert "fixed controller pool adds 250m CPU and 512Mi memory" in detail
    assert "benchmark workers add 225m CPU and 512Mi memory" in detail
    assert "platform runtime reserves 750m CPU and 2Gi memory" in detail


def test_capacity_shortfall_detail_returns_none_when_cluster_has_headroom(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)

    def fake_run_json(arguments, *, check=False):
        del check
        if arguments[:3] == ["kubectl", "get", "nodes"]:
            return {
                "items": [
                    {
                        "spec": {},
                        "status": {
                            "allocatable": {
                                "cpu": "8",
                                "memory": "16Gi",
                            }
                        },
                    }
                ]
            }
        return {}

    monkeypatch.setattr(automation, "_run_json", fake_run_json)

    detail = automation._capacity_shortfall_detail(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        config_mode="baseline",
        deployment_mode="normal",
        broker_tuning={"broker": {}, "producer": {}, "consumer": {}},
        producers=1,
        consumers=1,
        resource_config={
            "cpuRequest": "1500m",
            "cpuLimit": "1500m",
            "memoryRequest": "3Gi",
            "memoryLimit": "3Gi",
            "storageSize": "20Gi",
            "replicas": 1,
        },
    )

    assert detail is None


def test_build_agent_jobs_apply_worker_resources_and_deadline() -> None:
    automation = ClusterAutomation(REPO_ROOT)

    documents, consumer_jobs, producer_jobs, _ = automation._build_agent_jobs(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        scenario_id="kafka-partitioned-burst",
        config_mode="optimized",
        deployment_mode="normal",
        namespace="bench-run-12345678",
        broker_tuning={"broker": {}, "producer": {}, "consumer": {}},
        message_rate=12000,
        message_size_bytes=1024,
        producers=2,
        consumers=2,
        warmup_seconds=20,
        measurement_seconds=60,
        cooldown_seconds=20,
        transport_options={"rateProfileKind": "burst", "peakMessageRate": 36000},
        resource_config=None,
        scheduled_start_ns=123456789,
    )

    assert consumer_jobs == ["12345678-consumer-1", "12345678-consumer-2"]
    assert producer_jobs == ["12345678-producer-1", "12345678-producer-2"]

    consumer_job = next(
        document for document in documents if document["metadata"]["name"] == "12345678-consumer-1"
    )
    producer_job = next(
        document for document in documents if document["metadata"]["name"] == "12345678-producer-1"
    )

    assert consumer_job["spec"]["activeDeadlineSeconds"] == 1800
    assert producer_job["spec"]["activeDeadlineSeconds"] == 1800
    assert (
        consumer_job["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"]
        == "100m"
    )
    assert (
        producer_job["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["memory"]
        == "384Mi"
    )


def test_launch_run_finalizer_recreates_stale_job_with_explicit_command(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.enabled = True
    commands: list[tuple[list[str], str | None]] = []

    monkeypatch.setattr("services.platform.cluster.shutil.which", lambda name: "kubectl" if name == "kubectl" else None)
    monkeypatch.setattr(automation, "_job_phase", lambda namespace, name: "failed" if name == finalizer_job_name_for("12345678-abcd") else "not_found")

    def fake_run(arguments, *, stdin_text=None, check=True):
        del check
        commands.append((list(arguments), stdin_text))
        return ""

    monkeypatch.setattr(automation, "_run", fake_run)

    automation.launch_run_finalizer("12345678-abcd")

    delete_call = commands[0]
    apply_call = commands[1]
    assert delete_call[0][:4] == ["kubectl", "-n", automation.platform_namespace, "delete"]
    assert apply_call[0] == ["kubectl", "apply", "-f", "-"]
    assert apply_call[1] is not None
    document = yaml.safe_load(apply_call[1])
    container = document["spec"]["template"]["spec"]["containers"][0]
    assert document["spec"]["ttlSecondsAfterFinished"] == 900
    assert document["spec"]["backoffLimit"] == 0
    assert container["command"] == ["python", "-m", "services.platform.finalizer"]
    assert container["args"] == ["--run-id=12345678-abcd"]
    assert container["resources"]["limits"]["memory"] == "6Gi"


def test_job_logs_to_file_safe_prefers_pod_logs(monkeypatch, tmp_path: Path) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.repo_root = tmp_path

    monkeypatch.setattr(
        automation,
        "_run",
        lambda arguments, *, check=False: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "name": "12345678-consumer-1-abcde",
                                "creationTimestamp": "2026-03-29T20:33:00Z",
                            }
                        }
                    ]
                }
            ),
        )
        if arguments[:6] == ["kubectl", "-n", "bench-run-12345678", "get", "pods", "-l"]
        else SimpleNamespace(returncode=1, stdout=""),
    )

    calls: list[list[str]] = []

    def fake_subprocess_run(arguments, cwd, check, text, stdout, stderr):
        del cwd, check, text, stderr
        calls.append(list(arguments))
        if len(arguments) >= 5 and arguments[3] == "logs" and arguments[4].startswith("pod/"):
            stdout.write('{"role":"consumer","result":{"received":1}}\n')
            return SimpleNamespace(returncode=0)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr("services.platform.cluster.subprocess.run", fake_subprocess_run)

    path = automation._job_logs_to_file_safe(
        "bench-run-12345678",
        "12345678-consumer-1",
        "consumer",
    )

    assert path
    assert calls[0][:6] == [
        "kubectl",
        "-n",
        "bench-run-12345678",
        "logs",
        "pod/12345678-consumer-1-abcde",
        "-c",
    ]
    assert Path(path).read_text(encoding="utf-8").strip() == '{"role":"consumer","result":{"received":1}}'


def test_job_result_source_path_prefers_structured_output(monkeypatch, tmp_path: Path) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.shared_artifact_dir = tmp_path / "artifacts"
    structured_path = Path(
        automation._agent_structured_output_path(
            "12345678-abcd-efgh-ijkl-1234567890ab",
            "consumer",
            1,
        )
    )
    structured_path.parent.mkdir(parents=True, exist_ok=True)
    structured_path.write_text('{"role":"consumer","result":{"received":1}}\n', encoding="utf-8")

    fallback_calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        automation,
        "_job_logs_to_file_safe",
        lambda namespace, job_name, container_name: fallback_calls.append((namespace, job_name, container_name)) or "",
    )

    source = automation._job_result_source_path(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        role="consumer",
        ordinal=1,
        namespace="bench-run-12345678",
        job_name="12345678-consumer-1",
        container_name="consumer",
    )

    assert source == str(structured_path)
    assert fallback_calls == []


def test_job_result_source_path_keeps_empty_structured_output_for_accounting(monkeypatch, tmp_path: Path) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.shared_artifact_dir = tmp_path / "artifacts"
    structured_path = Path(
        automation._agent_structured_output_path(
            "12345678-abcd-efgh-ijkl-1234567890ab",
            "consumer",
            1,
        )
    )
    structured_path.parent.mkdir(parents=True, exist_ok=True)
    structured_path.write_text("", encoding="utf-8")

    fallback_calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        automation,
        "_job_logs_to_file_safe",
        lambda namespace, job_name, container_name: fallback_calls.append((namespace, job_name, container_name)) or "",
    )

    source = automation._job_result_source_path(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        role="consumer",
        ordinal=1,
        namespace="bench-run-12345678",
        job_name="12345678-consumer-1",
        container_name="consumer",
    )

    assert source == str(structured_path)
    assert fallback_calls == []


def test_benchmark_jobs_use_shared_runtime_volume() -> None:
    automation = ClusterAutomation(REPO_ROOT)
    documents, consumer_jobs, producer_jobs, _ = automation._build_agent_jobs(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        scenario_id=None,
        config_mode="baseline",
        deployment_mode="normal",
        namespace="bench-run-12345678",
        broker_tuning={},
        message_rate=1000,
        message_size_bytes=512,
        producers=1,
        consumers=1,
        warmup_seconds=5,
        measurement_seconds=15,
        cooldown_seconds=5,
        transport_options={"rateProfileKind": "constant", "peakMessageRate": 1000},
        resource_config={"replicas": 1},
    )

    assert consumer_jobs == ["12345678-consumer-1"]
    assert producer_jobs == ["12345678-producer-1"]
    for document in documents:
        if document["kind"] != "Job":
            continue
        spec = document["spec"]["template"]["spec"]
        assert spec["volumes"] == [
            {
                "name": "runtime-data",
                "persistentVolumeClaim": {"claimName": automation.platform_data_pvc},
            }
        ]
        assert spec["containers"][0]["volumeMounts"] == [{"name": "runtime-data", "mountPath": "/data"}]


def test_discover_rabbitmq_amqp_services_filters_non_amqp_services(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)

    monkeypatch.setattr(
        automation,
        "_run_json",
        lambda *_args, **_kwargs: {
            "items": [
                {
                    "metadata": {"name": "rabbitmq-run"},
                    "spec": {"ports": [{"port": 5672}]},
                },
                {
                    "metadata": {"name": "rabbitmq-run-nodes"},
                    "spec": {"ports": [{"port": 25672}]},
                },
                {
                    "metadata": {"name": "rabbitmq-run-metrics"},
                    "spec": {"ports": [{"port": 15692}]},
                },
            ]
        },
    )

    assert automation._discover_rabbitmq_amqp_services("bench-run-test", "rabbitmq-run") == ["rabbitmq-run"]


def test_ensure_operators_uses_probe_when_bootstrap_disabled(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.enabled = True
    automation.operator_bootstrap_enabled = False

    monkeypatch.setattr(
        automation,
        "_probe_bootstrap_state",
        lambda: {
            "ready": True,
            "message": "control planes ready",
            "operators": {
                "kafka": {"label": "Kafka", "namespace": "bench-kafka-operator", "ready": True, "message": "ready"},
                "rabbitmq": {"label": "RabbitMQ", "namespace": "rabbitmq-system", "ready": True, "message": "ready"},
                "artemis": {"label": "Artemis", "namespace": "bench-artemis-operator", "ready": True, "message": "ready"},
                "nats": {"label": "NATS JetStream", "namespace": "bench-nats-operator", "ready": True, "message": "ready"},
            },
            "platformOperators": {
                "cnpg": {"label": "CloudNativePG", "namespace": "bench-cnpg-operator", "ready": True, "message": "ready"},
                "minio": {"label": "MinIO Operator", "namespace": "bench-minio-operator", "ready": True, "message": "ready"},
            },
            "platformServices": {
                "postgres": {"label": "PostgreSQL", "namespace": "bench-platform-data", "ready": True, "message": "ready"},
                "objectStore": {"label": "S3 Store", "namespace": "bench-platform-data", "ready": True, "message": "ready"},
            },
        },
    )

    status = automation.ensure_operators()

    assert status["enabled"] is False
    assert status["attempted"] is False
    assert status["ready"] is True
    assert status["message"] == "control planes ready"
    assert status["platformOperators"]["cnpg"]["ready"] is True
    assert status["platformServices"]["objectStore"]["ready"] is True


def test_operator_bootstrap_defaults_to_enabled(monkeypatch) -> None:
    monkeypatch.delenv("BUS_ENABLE_OPERATOR_BOOTSTRAP", raising=False)
    automation = ClusterAutomation(REPO_ROOT)
    assert automation.operator_bootstrap_enabled is True


def test_probe_operator_states_marks_nats_not_ready_without_kubectl(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.enabled = True

    monkeypatch.setattr(automation, "tools_available", lambda: False)

    status = automation._probe_operator_states()

    assert status["ready"] is False
    assert status["operators"]["kafka"]["ready"] is False
    assert status["operators"]["nats"]["ready"] is False
    assert status["operators"]["nats"]["message"] == "kubectl unavailable"


def test_probe_bootstrap_state_includes_platform_dependencies(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.enabled = True

    monkeypatch.setattr(
        automation,
        "_probe_operator_states",
        lambda: {
            "ready": True,
            "operators": {
                "kafka": {"label": "Kafka", "namespace": "bench-kafka-operator", "ready": True, "message": "ready"},
                "rabbitmq": {"label": "RabbitMQ", "namespace": "rabbitmq-system", "ready": True, "message": "ready"},
                "artemis": {"label": "Artemis", "namespace": "bench-artemis-operator", "ready": True, "message": "ready"},
                "nats": {"label": "NATS JetStream", "namespace": "bench-nats-operator", "ready": True, "message": "ready"},
            },
        },
    )
    monkeypatch.setattr(
        automation,
        "_probe_platform_operator_states",
        lambda: {
            "ready": True,
            "platformOperators": {
                "cnpg": {"label": "CloudNativePG", "namespace": "bench-cnpg-operator", "ready": True, "message": "ready"},
                "minio": {"label": "MinIO Operator", "namespace": "bench-minio-operator", "ready": True, "message": "ready"},
            },
        },
    )
    monkeypatch.setattr(
        automation,
        "_probe_platform_service_states",
        lambda: {
            "ready": True,
            "platformServices": {
                "postgres": {"label": "PostgreSQL", "namespace": "bench-platform-data", "ready": True, "message": "ready"},
                "objectStore": {"label": "S3 Store", "namespace": "bench-platform-data", "ready": True, "message": "ready"},
            },
        },
    )

    status = automation._probe_bootstrap_state()

    assert status["ready"] is True
    assert status["platformOperators"]["cnpg"]["ready"] is True
    assert status["platformServices"]["postgres"]["ready"] is True


def test_ensure_operators_bootstraps_platform_dependencies(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.enabled = True
    calls: list[str] = []
    probe_counter = {"count": 0}

    monkeypatch.setattr(automation, "bootstrap_tools_available", lambda: True)
    monkeypatch.setattr(automation, "_apply_cluster_bootstrap_overlay", lambda scope="all": calls.append(f"overlay:{scope}"))
    monkeypatch.setattr(automation, "_ensure_strimzi_operator", lambda: calls.append("strimzi"))
    monkeypatch.setattr(automation, "_ensure_rabbitmq_operator", lambda: calls.append("rabbitmq"))
    monkeypatch.setattr(automation, "_ensure_artemis_operator", lambda: calls.append("artemis"))
    monkeypatch.setattr(automation, "_ensure_nats_operator", lambda: calls.append("nats"))
    monkeypatch.setattr(automation, "_ensure_cnpg_operator", lambda: calls.append("cnpg"))
    monkeypatch.setattr(automation, "_ensure_minio_operator", lambda: calls.append("minio"))
    monkeypatch.setattr(automation, "_wait_for_crds", lambda *_args: calls.append("crds"))
    monkeypatch.setattr(automation, "_ensure_platform_database", lambda: calls.append("postgres"))
    monkeypatch.setattr(automation, "_ensure_platform_object_store", lambda: calls.append("objectStore"))

    def fake_probe() -> dict[str, object]:
        probe_counter["count"] += 1
        ready = probe_counter["count"] >= 4
        return {
            "ready": ready,
            "message": "control planes ready" if ready else "bootstrap dependencies not ready",
            "operators": {
                key: {
                    "label": value["label"],
                    "namespace": value["namespace"],
                    "ready": ready,
                    "message": "ready" if ready else "not installed",
                }
                for key, value in automation._bootstrap_status["operators"].items()
            },
            "platformOperators": {
                key: {
                    "label": value["label"],
                    "namespace": value["namespace"],
                    "ready": ready,
                    "message": "ready" if ready else "not installed",
                }
                for key, value in automation._bootstrap_status["platformOperators"].items()
            },
            "platformServices": {
                key: {
                    "label": value["label"],
                    "namespace": value["namespace"],
                    "ready": ready,
                    "message": "ready" if ready else "not installed",
                }
                for key, value in automation._bootstrap_status["platformServices"].items()
            },
        }

    monkeypatch.setattr(automation, "_probe_bootstrap_state", fake_probe)

    status = automation.ensure_operators()

    assert status["ready"] is True
    assert calls == [
        "overlay:all",
        "strimzi",
        "rabbitmq",
        "artemis",
        "nats",
        "crds",
        "cnpg",
        "minio",
        "crds",
        "postgres",
        "objectStore",
    ]


def test_cluster_bootstrap_overlay_paths_are_scope_specific() -> None:
    automation = ClusterAutomation(REPO_ROOT)

    assert automation._cluster_bootstrap_overlay_path("brokers").name == "brokers"
    assert automation._cluster_bootstrap_overlay_path("platform-data").name == "platform-data"
    assert automation._cluster_bootstrap_overlay_path("platform-services").name == "platform-data"
    assert automation._cluster_bootstrap_overlay_path("all").name == "all"


def test_probe_operator_states_marks_legacy_rabbitmq_namespace_not_ready(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)

    def fake_probe(targets: dict[str, dict[str, object]]) -> dict[str, object]:
        return {
            "ready": False,
            "states": {
                key: {
                    "label": value["label"],
                    "namespace": value["namespace"],
                    "ready": False,
                    "message": "not installed",
                }
                for key, value in targets.items()
            },
        }

    monkeypatch.setattr(automation, "_probe_deployment_targets", fake_probe)

    status = automation._probe_operator_states()

    assert status["operators"]["rabbitmq"]["ready"] is False
    assert status["operators"]["rabbitmq"]["namespace"] == "rabbitmq-system"


def test_ensure_rabbitmq_operator_uses_upstream_manifest(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    calls: list[list[str]] = []

    monkeypatch.setattr(
        automation,
        "_run",
        lambda arguments, **kwargs: calls.append(arguments) or SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(automation, "_wait_for_deployments", lambda namespace, timeout="10m": calls.append(["wait", namespace, timeout]))

    automation._ensure_rabbitmq_operator()

    assert [
        "kubectl",
        "delete",
        "namespace",
        "bench-rabbitmq-operator",
        "--ignore-not-found=true",
        "--wait=false",
    ] in calls
    assert ["kubectl", "apply", "-f", automation.rabbitmq_operator_manifest_url] in calls
    assert ["wait", "rabbitmq-system", "10m"] in calls


def test_purge_shared_broker_state_deletes_owned_resources_in_existing_shared_namespaces(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.enabled = True
    calls: list[list[str]] = []

    monkeypatch.setattr("services.platform.cluster.shutil.which", lambda _name: "kubectl")

    def fake_run(arguments, *, check=True, stdin_text=None):
        del check, stdin_text
        calls.append(arguments)
        if arguments[:3] == ["kubectl", "get", "namespace"]:
            namespace = arguments[3]
            return SimpleNamespace(returncode=0 if namespace == "bench-kafka" else 1, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(automation, "_run", fake_run)

    automation.purge_shared_broker_state()

    delete_calls = [arguments for arguments in calls if arguments[:4] == ["kubectl", "-n", "bench-kafka", "delete"]]
    assert delete_calls
    assert any("streams.jetstream.nats.io" in arguments for arguments in delete_calls)
    assert any("persistentvolumeclaims" in arguments for arguments in delete_calls)


def test_build_agent_jobs_passes_consumer_start_barrier() -> None:
    automation = ClusterAutomation(REPO_ROOT)
    documents, consumer_jobs, producer_jobs, scheduled_start_ns = automation._build_agent_jobs(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="kafka",
        scenario_id=None,
        config_mode="optimized",
        deployment_mode="normal",
        namespace="bench-run-12345678",
        broker_tuning=None,
        message_rate=10,
        message_size_bytes=512,
        producers=1,
        consumers=1,
        warmup_seconds=5,
        measurement_seconds=10,
        cooldown_seconds=5,
        transport_options={"rateProfileKind": "constant", "peakMessageRate": 10},
    )

    assert consumer_jobs == ["12345678-consumer-1"]
    assert producer_jobs == ["12345678-producer-1"]

    consumer_job = next(
        document
        for document in documents
        if document.get("kind") == "Job" and document.get("metadata", {}).get("name") == consumer_jobs[0]
    )
    consumer_args = consumer_job["spec"]["template"]["spec"]["containers"][0]["args"]
    assert f"--scheduled-start-at={scheduled_start_ns}" in consumer_args
    assert any(argument.startswith("--idle-exit-seconds=") for argument in consumer_args)


def test_build_agent_jobs_passes_nats_jetstream_replica_settings() -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation._wait_for_nats_runtime = lambda _namespace, _broker_name: "nats-12345678"
    documents, consumer_jobs, producer_jobs, _ = automation._build_agent_jobs(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="nats",
        scenario_id=None,
        config_mode="optimized",
        deployment_mode="ha",
        namespace="bench-run-12345678",
        broker_tuning=None,
        message_rate=10,
        message_size_bytes=512,
        producers=1,
        consumers=1,
        warmup_seconds=5,
        measurement_seconds=10,
        cooldown_seconds=5,
        transport_options={"rateProfileKind": "constant", "peakMessageRate": 10},
        resource_config={"replicas": 3},
    )

    consumer_job = next(
        document
        for document in documents
        if document.get("kind") == "Job" and document.get("metadata", {}).get("name") == consumer_jobs[0]
    )
    producer_job = next(
        document
        for document in documents
        if document.get("kind") == "Job" and document.get("metadata", {}).get("name") == producer_jobs[0]
    )

    consumer_args = consumer_job["spec"]["template"]["spec"]["containers"][0]["args"]
    producer_args = producer_job["spec"]["template"]["spec"]["containers"][0]["args"]

    assert "--jetstream-replicas=3" in consumer_args
    assert "--jetstream-replicas=3" in producer_args
    assert "--jetstream-stream-managed-by=nack" in consumer_args
    assert "--jetstream-stream-managed-by=nack" in producer_args


def test_build_agent_jobs_passes_nats_pipeline_tuning_args() -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation._wait_for_nats_runtime = lambda _namespace, _broker_name: "nats-12345678"
    documents, consumer_jobs, producer_jobs, _ = automation._build_agent_jobs(
        run_id="12345678-abcd-efgh-ijkl-1234567890ab",
        broker_id="nats",
        scenario_id=None,
        config_mode="optimized",
        deployment_mode="ha",
        namespace="bench-run-12345678",
        broker_tuning={
            "broker": {},
            "producer": {"maxPendingPublishes": 4096},
            "consumer": {"maxAckPending": 8192},
        },
        message_rate=10,
        message_size_bytes=512,
        producers=1,
        consumers=1,
        warmup_seconds=5,
        measurement_seconds=10,
        cooldown_seconds=5,
        transport_options={"rateProfileKind": "constant", "peakMessageRate": 10},
        resource_config={"replicas": 3},
    )

    consumer_job = next(
        document
        for document in documents
        if document.get("kind") == "Job" and document.get("metadata", {}).get("name") == consumer_jobs[0]
    )
    producer_job = next(
        document
        for document in documents
        if document.get("kind") == "Job" and document.get("metadata", {}).get("name") == producer_jobs[0]
    )

    consumer_args = consumer_job["spec"]["template"]["spec"]["containers"][0]["args"]
    producer_args = producer_job["spec"]["template"]["spec"]["containers"][0]["args"]

    assert "--max-pending-publishes=4096" in producer_args
    assert "--max-ack-pending=8192" in consumer_args


def test_rabbitmq_connection_runtime_embeds_connection_string_from_broker_namespace(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    monkeypatch.setattr(automation, "_wait_for_rabbitmq_runtime", lambda *_args: None)
    monkeypatch.setattr(
        automation,
        "_secret_value",
        lambda namespace, secret_name, key: f"{namespace}:{secret_name}:{key}",
    )

    runtime = automation._rabbitmq_connection_runtime(
        "bench-run-test",
        "rabbitmq-test",
        "normal",
    )

    assert runtime["connectionTargets"] == [[]]
    assert runtime["env"] == [
        {
            "name": "BROKER_URL",
            "value": "bench-run-test:rabbitmq-test-default-user:connection_string",
        }
    ]


def test_artemis_connection_runtime_embeds_credentials_from_broker_namespace(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    monkeypatch.setattr(
        automation,
        "_wait_for_artemis_runtime",
        lambda *_args: ["artemis-test-ss-0-svc"],
    )
    monkeypatch.setattr(
        automation,
        "_secret_value",
        lambda namespace, secret_name, key: f"{namespace}:{secret_name}:{key}",
    )

    runtime = automation._artemis_connection_runtime(
        "bench-run-test",
        "artemis-test",
        "normal",
    )

    assert runtime["connectionTargets"] == [
        ["--broker-url=amqp://artemis-test-ss-0-svc.bench-run-test.svc.cluster.local:5672"]
    ]
    assert runtime["env"] == [
        {
            "name": "BROKER_USERNAME",
            "value": "bench-run-test:artemis-test-credentials-secret:AMQ_USER",
        },
        {
            "name": "BROKER_PASSWORD",
            "value": "bench-run-test:artemis-test-credentials-secret:AMQ_PASSWORD",
        },
    ]


def test_benchmark_job_timeout_scales_with_requested_window() -> None:
    automation = ClusterAutomation(REPO_ROOT)
    automation.job_timeout_seconds = 1800

    assert automation._benchmark_job_timeout_seconds(
        warmup_seconds=20,
        measurement_seconds=3600,
        cooldown_seconds=20,
    ) == 4550


def test_measurement_window_ended_helper_returns_true_after_measurement() -> None:
    scheduled_start_ns = time.time_ns() - (15 * 1_000_000_000)

    assert _measurement_window_ended(
        scheduled_start_ns=scheduled_start_ns,
        warmup_seconds=5,
        measurement_seconds=5,
    ) is True


def test_wait_for_jobs_completion_returns_when_all_jobs_finish(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)
    polls = {"job-a": 0, "job-b": 0}

    def fake_run(arguments, *, check=False, stdin_text=None):
        del check, stdin_text
        job_name = arguments[4].split("/", 1)[1]
        polls[job_name] += 1
        if job_name == "job-a":
            status = {"succeeded": 1} if polls[job_name] >= 2 else {}
        else:
            status = {"succeeded": 1}
        payload = {"spec": {"completions": 1}, "status": status}
        return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(automation, "_run", fake_run)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    automation._wait_for_jobs_completion("bench-run-test", ["job-a", "job-b"], timeout_seconds=5)

    assert polls["job-a"] >= 2
    assert polls["job-b"] >= 1


def test_wait_for_jobs_completion_fails_fast_when_any_job_fails(monkeypatch) -> None:
    automation = ClusterAutomation(REPO_ROOT)

    def fake_run(arguments, *, check=False, stdin_text=None):
        del check, stdin_text
        job_name = arguments[4].split("/", 1)[1]
        status = {"failed": 1} if job_name == "job-b" else {}
        if job_name == "job-b":
            status["conditions"] = [
                {"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}
            ]
        payload = {"spec": {"completions": 1}, "status": status}
        return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(automation, "_run", fake_run)
    monkeypatch.setattr(
        automation,
        "_job_failure_reason",
        lambda _namespace, job_name: "OOMKilled" if job_name == "job-b" else "",
    )
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    try:
        automation._wait_for_jobs_completion(
            "bench-run-test",
            ["job-a", "job-b"],
            timeout_seconds=5,
        )
    except RuntimeError as exc:
        assert str(exc) == "Benchmark job job-b failed: OOMKilled"
    else:
        raise AssertionError("Expected a failed benchmark job to abort the wait loop")
