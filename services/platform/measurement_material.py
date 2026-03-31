from __future__ import annotations

import gzip
import json
import uuid
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Callable, Iterator

from services.platform.common import utc_now
from services.platform.measurements import iter_expanded_measurement_chunks, iter_json_lines
from services.platform.runtime_storage import RuntimeStorage


class JsonlGzipWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = None
        self.count = 0

    def write(self, record: dict[str, Any]) -> None:
        if self._handle is None:
            self._handle = gzip.open(self.path, "wt", encoding="utf-8")
        self._handle.write(json.dumps(record, separators=(",", ":"), sort_keys=False))
        self._handle.write("\n")
        self.count += 1

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        if self.count <= 0 and self.path.exists():
            self.path.unlink(missing_ok=True)


def iter_jsonl_gzip_records(path: Path | str) -> Iterator[dict[str, Any]]:
    candidate = Path(path)
    if not candidate.exists():
        return
    with gzip.open(candidate, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            row = str(line or "").strip()
            if not row or not row.startswith("{"):
                continue
            try:
                parsed = json.loads(row)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                yield parsed


def cleanup_measurement_material(material: dict[str, Any]) -> None:
    for item in material.get("temporaryPaths", []) or []:
        Path(str(item)).unlink(missing_ok=True)


def _is_run_structured_output_path(candidate: Path, run_path: Path) -> bool:
    try:
        return candidate.resolve().is_relative_to((run_path / "agent-logs").resolve())
    except OSError:
        return False


def persist_run_artifact(
    connection: Any,
    runtime_storage: RuntimeStorage,
    *,
    run_id: str,
    artifact_type: str,
    file_name: str,
    file_format: str,
    record_count: int,
    file_path: Path | None = None,
    content_bytes: bytes | None = None,
) -> dict[str, Any]:
    if content_bytes is not None:
        stored = runtime_storage.put_imported_artifact(run_id, file_name, content_bytes)
    elif file_path is not None:
        stored = runtime_storage.put_artifact_file(run_id, file_name, file_path)
    else:
        raise ValueError("Artifact persistence requires file content or a source path")
    if content_bytes is None and file_path is not None and runtime_storage.mode != "local":
        file_path.unlink(missing_ok=True)
    artifact_id = str(uuid.uuid4())
    size_bytes = int(stored["sizeBytes"] or 0)
    created_at = utc_now().isoformat()
    connection.execute(
        """
        INSERT INTO run_artifacts (
          id, run_id, artifact_type, file_name, file_path, storage_backend, object_key, file_format, record_count, size_bytes, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact_id,
            run_id,
            artifact_type,
            file_name,
            str(stored["filePath"] or ""),
            str(stored["storageBackend"] or "local"),
            stored["objectKey"],
            file_format,
            int(record_count),
            int(size_bytes),
            created_at,
        ),
    )
    return {
        "id": artifact_id,
        "artifactType": artifact_type,
        "fileName": file_name,
        "filePath": str(stored["filePath"] or ""),
        "storageBackend": str(stored["storageBackend"] or "local"),
        "objectKey": stored["objectKey"],
        "fileFormat": file_format,
        "recordCount": int(record_count),
        "sizeBytes": int(size_bytes),
        "createdAt": created_at,
    }


def iter_log_records(source: str | Path, *, from_file: bool) -> Any:
    if from_file:
        path = Path(str(source))
        if not path.exists():
            return
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                candidate = line.strip()
                if not candidate or not candidate.startswith("{"):
                    continue
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    yield parsed
        return
    yield from iter_json_lines(str(source or ""))


def load_measurement_material(
    runtime_storage: RuntimeStorage,
    *,
    run_id: str,
    producer_sources: list[str],
    consumer_sources: list[str],
    resource_samples: list[dict[str, Any]],
    sources_are_paths: bool,
    db_factory: Callable[[], AbstractContextManager[Any]],
) -> dict[str, Any]:
    run_path = runtime_storage.artifact_dir / run_id
    producer_result_items: list[dict[str, Any]] = []
    consumer_result_items: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {"artifacts": []}
    producer_writer = JsonlGzipWriter(run_path / "producer-records.jsonl.gz")
    consumer_writer = JsonlGzipWriter(run_path / "consumer-records.jsonl.gz")
    producer_timeseries_writer = JsonlGzipWriter(run_path / "producer-timeseries.tmp.jsonl.gz")
    consumer_timeseries_writer = JsonlGzipWriter(run_path / "consumer-timeseries.tmp.jsonl.gz")

    try:
        for role, sources, result_sink, timeseries_writer, raw_writer in (
            ("producer", producer_sources, producer_result_items, producer_timeseries_writer, producer_writer),
            ("consumer", consumer_sources, consumer_result_items, consumer_timeseries_writer, consumer_writer),
        ):
            for source in sources:
                for record in iter_log_records(source, from_file=sources_are_paths):
                    if record.get("role") == role and isinstance(record.get("result"), dict):
                        result_sink.append(record)
                        continue
                    kind = str(record.get("kind") or "").strip()
                    if kind == "measurement-record-chunk" and record.get("role") == role:
                        for item in iter_expanded_measurement_chunks([record], role, kind=kind):
                            raw_writer.write(item)
                        continue
                    if kind == "measurement-timeseries-chunk" and record.get("role") == role:
                        for item in iter_expanded_measurement_chunks([record], role, kind=kind):
                            timeseries_writer.write(item)
    finally:
        producer_writer.close()
        consumer_writer.close()
        producer_timeseries_writer.close()
        consumer_timeseries_writer.close()
        if sources_are_paths:
            for source in producer_sources + consumer_sources:
                candidate = Path(str(source))
                if _is_run_structured_output_path(candidate, run_path):
                    continue
                if candidate.exists():
                    candidate.unlink(missing_ok=True)

    with db_factory() as connection:
        if producer_writer.count > 0:
            metadata["artifacts"].append(
                persist_run_artifact(
                    connection,
                    runtime_storage,
                    run_id=run_id,
                    artifact_type="producer-raw-records",
                    file_name=producer_writer.path.name,
                    file_path=producer_writer.path,
                    file_format="jsonl+gzip",
                    record_count=producer_writer.count,
                )
            )
        if consumer_writer.count > 0:
            metadata["artifacts"].append(
                persist_run_artifact(
                    connection,
                    runtime_storage,
                    run_id=run_id,
                    artifact_type="consumer-raw-records",
                    file_name=consumer_writer.path.name,
                    file_path=consumer_writer.path,
                    file_format="jsonl+gzip",
                    record_count=consumer_writer.count,
                )
            )
        if resource_samples:
            run_path.mkdir(parents=True, exist_ok=True)
            resource_path = run_path / "resource-samples.json"
            with resource_path.open("w", encoding="utf-8") as handle:
                json.dump(resource_samples, handle, separators=(",", ":"), sort_keys=False)
            metadata["artifacts"].append(
                persist_run_artifact(
                    connection,
                    runtime_storage,
                    run_id=run_id,
                    artifact_type="resource-samples",
                    file_name=resource_path.name,
                    file_path=resource_path,
                    file_format="json",
                    record_count=len(resource_samples),
                )
            )

    metadata["producerRecordCount"] = producer_writer.count
    metadata["consumerRecordCount"] = consumer_writer.count
    metadata["producerTimeseriesCount"] = producer_timeseries_writer.count
    metadata["consumerTimeseriesCount"] = consumer_timeseries_writer.count
    metadata["resourceSampleCount"] = len(resource_samples)
    temporary_paths = [
        str(path)
        for path in (
            producer_timeseries_writer.path if producer_timeseries_writer.count > 0 else None,
            consumer_timeseries_writer.path if consumer_timeseries_writer.count > 0 else None,
        )
        if path is not None
    ]
    return {
        "producerResults": producer_result_items,
        "consumerResults": consumer_result_items,
        "producerTimeseriesRecords": (
            iter_jsonl_gzip_records(producer_timeseries_writer.path)
            if producer_timeseries_writer.count > 0
            else None
        ),
        "consumerTimeseriesRecords": (
            iter_jsonl_gzip_records(consumer_timeseries_writer.path)
            if consumer_timeseries_writer.count > 0
            else None
        ),
        "artifactMetadata": metadata,
        "temporaryPaths": temporary_paths,
    }
