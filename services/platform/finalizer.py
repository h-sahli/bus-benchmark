from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

from services.platform.cluster import ClusterAutomation, benchmark_job_names_for
from services.platform.common import parse_iso, parse_metrics, utc_now
from services.platform.measurement_material import cleanup_measurement_material, load_measurement_material
from services.platform.measurements import aggregate_agent_results
from services.platform.runtime_storage import REPO_ROOT, RuntimeStorage, resolve_runtime_storage


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize a benchmark run independently from the web pod.")
    parser.add_argument("--run-id", required=True)
    return parser.parse_args(argv)


def resolve_storage(
    db_path_override: Path | None = None,
    artifact_dir_override: Path | None = None,
) -> RuntimeStorage:
    runtime_storage = resolve_runtime_storage(
        db_path=db_path_override,
        artifact_dir=artifact_dir_override,
    )
    runtime_storage.ensure_ready()
    return runtime_storage


def record_event(connection: Any, run_id: str | None, event_type: str, message: str) -> None:
    connection.execute(
        """
        INSERT INTO run_events (id, run_id, event_type, message, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (str(uuid.uuid4()), run_id, event_type, message, utc_now().isoformat()),
    )


def record_event_now(runtime_storage: RuntimeStorage, run_id: str | None, event_type: str, message: str) -> None:
    with runtime_storage.db() as connection:
        record_event(connection, run_id, event_type, message)


def get_run_row(runtime_storage: RuntimeStorage, run_id: str) -> Any:
    with runtime_storage.db() as connection:
        return connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()


def run_stop_requested(runtime_storage: RuntimeStorage, run_id: str) -> bool:
    row = get_run_row(runtime_storage, run_id)
    return row is None or bool(row["stopped_at"])


def cleanup_run_topology(
    runtime_storage: RuntimeStorage,
    cluster_automation: ClusterAutomation,
    run_id: str,
    reason: str,
    *,
    preserve_structured_output: bool = False,
) -> None:
    row = get_run_row(runtime_storage, run_id)
    if row is None or row["topology_deleted_at"]:
        return
    record_event_now(runtime_storage, run_id, "cleanup-started", f"Destroying topology for {reason}.")
    if hasattr(cluster_automation, "delete_benchmark_jobs"):
        cluster_automation.delete_benchmark_jobs(
            run_id,
            producers=int(row["producers"] or 1),
            consumers=int(row["consumers"] or 1),
        )
    cluster_automation.delete_run_namespace(run_id)
    deleted = cluster_automation.wait_for_namespace_deleted(run_id)
    if not preserve_structured_output and hasattr(cluster_automation, "delete_run_structured_output"):
        cluster_automation.delete_run_structured_output(run_id)
    with runtime_storage.db() as connection:
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


def finalize_run(
    run_id: str,
    *,
    db_path: Path | None = None,
    artifact_dir: Path | None = None,
    cluster_automation: ClusterAutomation | None = None,
) -> int:
    runtime_storage = resolve_storage(db_path, artifact_dir)
    cluster_automation = cluster_automation or ClusterAutomation(REPO_ROOT)
    row = get_run_row(runtime_storage, run_id)
    if row is None:
        print(f"Run {run_id} not found.", flush=True)
        return 1
    if row["completed_at"]:
        return 0
    if run_stop_requested(runtime_storage, run_id):
        cleanup_run_topology(runtime_storage, cluster_automation, run_id, "stop")
        return 0

    record_event_now(runtime_storage, run_id, "finalizer-started", "Detached run finalizer started.")

    broker_id = str(row["broker_id"] or "kafka").strip().lower()
    config_mode = str(row["config_mode"] or "baseline").strip() or "baseline"
    deployment_mode = str(row["deployment_mode"] or "normal").strip() or "normal"
    message_rate = int(row["message_rate"] or 0)
    message_size_bytes = int(row["message_size_bytes"] or 0)
    producers = int(row["producers"] or 1)
    consumers = int(row["consumers"] or 1)
    warmup_seconds = int(row["warmup_seconds"] or 0)
    measurement_seconds = int(row["measurement_seconds"] or 1)
    cooldown_seconds = int(row["cooldown_seconds"] or 0)
    transport_options = json.loads(str(row["transport_options"] or "{}") or "{}")
    broker_tuning = parse_metrics(row["broker_tuning_json"])
    resource_config = parse_metrics(row["resource_config_json"])
    execution_started_at = parse_iso(row["execution_started_at"])
    if execution_started_at is None:
        raise RuntimeError("Run execution did not start before finalizer launch.")
    scheduled_start_ns = int(execution_started_at.timestamp() * 1_000_000_000)
    producer_jobs, consumer_jobs = benchmark_job_names_for(
        run_id,
        producers=producers,
        consumers=consumers,
    )

    try:
        benchmark_logs = cluster_automation.collect_benchmark_results(
            run_id=run_id,
            producer_jobs=producer_jobs,
            consumer_jobs=consumer_jobs,
            scheduled_start_ns=scheduled_start_ns,
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
        )
        if run_stop_requested(runtime_storage, run_id):
            cleanup_run_topology(runtime_storage, cluster_automation, run_id, "stop")
            return 0

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
            db_factory=runtime_storage.db,
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
        measurement["scheduledStartNs"] = int(benchmark_logs.get("scheduledStartNs") or scheduled_start_ns)
        measurement["rawRecordStore"] = measurement_material["artifactMetadata"]
        post_measurement_failure = str(benchmark_logs.get("postMeasurementFailure") or "").strip()
        if post_measurement_failure:
            measurement["postMeasurementFailure"] = post_measurement_failure

        with runtime_storage.db() as connection:
            connection.execute(
                "UPDATE runs SET completed_at = ?, metrics_json = ? WHERE id = ? AND completed_at IS NULL",
                (utc_now().isoformat(), json.dumps(metrics_payload), run_id),
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
        cleanup_run_topology(runtime_storage, cluster_automation, run_id, "completion")
        return 0
    except Exception as exc:
        if run_stop_requested(runtime_storage, run_id):
            cleanup_run_topology(runtime_storage, cluster_automation, run_id, "stop")
            return 0
        with runtime_storage.db() as connection:
            connection.execute(
                "UPDATE runs SET stopped_at = ? WHERE id = ? AND stopped_at IS NULL",
                (utc_now().isoformat(), run_id),
            )
            record_event(connection, run_id, "execution-failed", str(exc))
        cleanup_run_topology(
            runtime_storage,
            cluster_automation,
            run_id,
            "execution failure",
            preserve_structured_output=True,
        )
        print(f"Finalizer failed for {run_id}: {exc}", flush=True)
        return 1


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    return finalize_run(args.run_id)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
