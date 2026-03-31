from __future__ import annotations

import os
import shutil
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import boto3
except ImportError:  # pragma: no cover - installed in container/runtime
    boto3 = None

try:
    import psycopg
except ImportError:  # pragma: no cover - installed in container/runtime
    psycopg = None


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = Path(os.environ.get("BUS_DB_PATH", REPO_ROOT / "runtime" / "bus.db"))


RUNS_TABLE_COLUMNS = [
    "id",
    "name",
    "broker_id",
    "protocol",
    "scenario_id",
    "config_mode",
    "deployment_mode",
    "starts_at",
    "warmup_seconds",
    "measurement_seconds",
    "cooldown_seconds",
    "message_rate",
    "message_size_bytes",
    "producers",
    "consumers",
    "transport_options",
    "ha_mode",
    "broker_tuning_json",
    "metrics_json",
    "resource_config_json",
    "topology_ready_at",
    "execution_started_at",
    "completed_at",
    "topology_deleted_at",
    "created_at",
    "stopped_at",
]


class RowRecord(Mapping[str, Any]):
    def __init__(self, columns: list[str], values: list[Any]) -> None:
        self._columns = list(columns)
        self._values = list(values)
        self._data = dict(zip(self._columns, self._values))

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class QueryResult:
    def __init__(self, rows: list[RowRecord], rowcount: int) -> None:
        self._rows = rows
        self.rowcount = rowcount
        self._index = 0

    def fetchone(self) -> RowRecord | None:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self) -> list[RowRecord]:
        if self._index <= 0:
            self._index = len(self._rows)
            return list(self._rows)
        remaining = self._rows[self._index :]
        self._index = len(self._rows)
        return list(remaining)


def _rows_from_cursor(cursor: Any) -> list[RowRecord]:
    if not getattr(cursor, "description", None):
        return []
    columns = [str(item[0]) for item in cursor.description]
    return [RowRecord(columns, list(row)) for row in cursor.fetchall()]


class SQLiteConnectionAdapter:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> QueryResult:
        cursor = self._connection.execute(sql, tuple(params))
        rows = _rows_from_cursor(cursor)
        return QueryResult(rows, cursor.rowcount)

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


class PostgresConnectionAdapter:
    def __init__(self, dsn: str) -> None:
        if psycopg is None:  # pragma: no cover - dependency installed in runtime
            raise RuntimeError("psycopg is not installed")
        self._connection = psycopg.connect(dsn)

    @staticmethod
    def _translate_sql(sql: str) -> str:
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> QueryResult:
        cursor = self._connection.cursor()
        cursor.execute(self._translate_sql(sql), tuple(params))
        rows = _rows_from_cursor(cursor)
        rowcount = cursor.rowcount
        cursor.close()
        return QueryResult(rows, rowcount)

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    username: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.username} password={self.password}"
        )


class SQLiteDatabaseBackend:
    dialect = "sqlite"

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[SQLiteConnectionAdapter]:
        connection = SQLiteConnectionAdapter(self.db_path)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def ensure_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  broker_id TEXT NOT NULL DEFAULT 'kafka',
                  protocol TEXT NOT NULL DEFAULT 'kafka',
                  scenario_id TEXT,
                  config_mode TEXT NOT NULL DEFAULT 'baseline',
                  deployment_mode TEXT NOT NULL DEFAULT 'normal',
                  starts_at TEXT NOT NULL,
                  warmup_seconds INTEGER NOT NULL,
                  measurement_seconds INTEGER NOT NULL,
                  cooldown_seconds INTEGER NOT NULL,
                  message_rate INTEGER NOT NULL DEFAULT 5000,
                  message_size_bytes INTEGER NOT NULL DEFAULT 1024,
                  producers INTEGER NOT NULL DEFAULT 1,
                  consumers INTEGER NOT NULL DEFAULT 1,
                  transport_options TEXT,
                  ha_mode INTEGER NOT NULL DEFAULT 0,
                  broker_tuning_json TEXT NOT NULL DEFAULT '{}',
                  metrics_json TEXT NOT NULL DEFAULT '{}',
                  resource_config_json TEXT NOT NULL DEFAULT '{}',
                  topology_ready_at TEXT,
                  execution_started_at TEXT,
                  completed_at TEXT,
                  topology_deleted_at TEXT,
                  created_at TEXT NOT NULL,
                  stopped_at TEXT
                )
                """
            )
            self._ensure_sqlite_column(connection, "runs", "broker_id", "TEXT NOT NULL DEFAULT 'kafka'")
            self._ensure_sqlite_column(connection, "runs", "protocol", "TEXT NOT NULL DEFAULT 'kafka'")
            self._ensure_sqlite_column(connection, "runs", "config_mode", "TEXT NOT NULL DEFAULT 'baseline'")
            self._ensure_sqlite_column(connection, "runs", "deployment_mode", "TEXT NOT NULL DEFAULT 'normal'")
            self._ensure_sqlite_column(connection, "runs", "message_rate", "INTEGER NOT NULL DEFAULT 5000")
            self._ensure_sqlite_column(connection, "runs", "message_size_bytes", "INTEGER NOT NULL DEFAULT 1024")
            self._ensure_sqlite_column(connection, "runs", "producers", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_sqlite_column(connection, "runs", "consumers", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_sqlite_column(connection, "runs", "transport_options", "TEXT")
            self._ensure_sqlite_column(connection, "runs", "ha_mode", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_sqlite_column(connection, "runs", "broker_tuning_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_sqlite_column(connection, "runs", "metrics_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_sqlite_column(connection, "runs", "resource_config_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_sqlite_column(connection, "runs", "topology_ready_at", "TEXT")
            self._ensure_sqlite_column(connection, "runs", "execution_started_at", "TEXT")
            self._ensure_sqlite_column(connection, "runs", "completed_at", "TEXT")
            self._ensure_sqlite_column(connection, "runs", "topology_deleted_at", "TEXT")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_events (
                  id TEXT PRIMARY KEY,
                  run_id TEXT,
                  event_type TEXT NOT NULL,
                  message TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reports (
                  id TEXT PRIMARY KEY,
                  title TEXT NOT NULL,
                  run_ids_json TEXT NOT NULL,
                  sections_json TEXT NOT NULL DEFAULT '{"ids":["summary","comparison","details","configuration","artifacts"]}',
                  status TEXT NOT NULL,
                  file_name TEXT,
                  file_path TEXT,
                  storage_backend TEXT NOT NULL DEFAULT 'local',
                  object_key TEXT,
                  size_bytes INTEGER NOT NULL DEFAULT 0,
                  error_message TEXT,
                  created_at TEXT NOT NULL,
                  completed_at TEXT
                )
                """
            )
            self._ensure_sqlite_column(
                connection,
                "reports",
                "sections_json",
                """TEXT NOT NULL DEFAULT '{"ids":["summary","comparison","details","configuration","artifacts"]}'""",
            )
            self._ensure_sqlite_column(connection, "reports", "storage_backend", "TEXT NOT NULL DEFAULT 'local'")
            self._ensure_sqlite_column(connection, "reports", "object_key", "TEXT")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_artifacts (
                  id TEXT PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  artifact_type TEXT NOT NULL,
                  file_name TEXT NOT NULL,
                  file_path TEXT NOT NULL,
                  storage_backend TEXT NOT NULL DEFAULT 'local',
                  object_key TEXT,
                  file_format TEXT NOT NULL,
                  record_count INTEGER NOT NULL DEFAULT 0,
                  size_bytes INTEGER NOT NULL DEFAULT 0,
                  created_at TEXT NOT NULL
                )
                """
                )
            self._ensure_sqlite_column(connection, "run_artifacts", "storage_backend", "TEXT NOT NULL DEFAULT 'local'")
            self._ensure_sqlite_column(connection, "run_artifacts", "object_key", "TEXT")
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _ensure_sqlite_column(connection: sqlite3.Connection, table_name: str, column_name: str, sql_fragment: str) -> None:
        existing_columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if column_name not in existing_columns:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_fragment}")


class PostgresDatabaseBackend:
    dialect = "postgres"

    def __init__(self, config: PostgresConfig) -> None:
        self.config = config

    @contextmanager
    def connect(self) -> Iterator[PostgresConnectionAdapter]:
        connection = PostgresConnectionAdapter(self.config.dsn)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def ensure_schema(self) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  broker_id TEXT NOT NULL DEFAULT 'kafka',
                  protocol TEXT NOT NULL DEFAULT 'kafka',
                  scenario_id TEXT,
                  config_mode TEXT NOT NULL DEFAULT 'baseline',
                  deployment_mode TEXT NOT NULL DEFAULT 'normal',
                  starts_at TEXT NOT NULL,
                  warmup_seconds INTEGER NOT NULL,
                  measurement_seconds INTEGER NOT NULL,
                  cooldown_seconds INTEGER NOT NULL,
                  message_rate INTEGER NOT NULL DEFAULT 5000,
                  message_size_bytes INTEGER NOT NULL DEFAULT 1024,
                  producers INTEGER NOT NULL DEFAULT 1,
                  consumers INTEGER NOT NULL DEFAULT 1,
                  transport_options TEXT,
                  ha_mode INTEGER NOT NULL DEFAULT 0,
                  broker_tuning_json TEXT NOT NULL DEFAULT '{}',
                  metrics_json TEXT NOT NULL DEFAULT '{}',
                  resource_config_json TEXT NOT NULL DEFAULT '{}',
                  topology_ready_at TEXT,
                  execution_started_at TEXT,
                  completed_at TEXT,
                  topology_deleted_at TEXT,
                  created_at TEXT NOT NULL,
                  stopped_at TEXT
                )
                """
            )
            for column_name, sql_fragment in (
                ("broker_id", "TEXT NOT NULL DEFAULT 'kafka'"),
                ("protocol", "TEXT NOT NULL DEFAULT 'kafka'"),
                ("config_mode", "TEXT NOT NULL DEFAULT 'baseline'"),
                ("deployment_mode", "TEXT NOT NULL DEFAULT 'normal'"),
                ("message_rate", "INTEGER NOT NULL DEFAULT 5000"),
                ("message_size_bytes", "INTEGER NOT NULL DEFAULT 1024"),
                ("producers", "INTEGER NOT NULL DEFAULT 1"),
                ("consumers", "INTEGER NOT NULL DEFAULT 1"),
                ("transport_options", "TEXT"),
                ("ha_mode", "INTEGER NOT NULL DEFAULT 0"),
                ("broker_tuning_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("metrics_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("resource_config_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("topology_ready_at", "TEXT"),
                ("execution_started_at", "TEXT"),
                ("completed_at", "TEXT"),
                ("topology_deleted_at", "TEXT"),
            ):
                connection.execute(
                    f"ALTER TABLE runs ADD COLUMN IF NOT EXISTS {column_name} {sql_fragment}"
                )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_events (
                  id TEXT PRIMARY KEY,
                  run_id TEXT,
                  event_type TEXT NOT NULL,
                  message TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reports (
                  id TEXT PRIMARY KEY,
                  title TEXT NOT NULL,
                  run_ids_json TEXT NOT NULL,
                  sections_json TEXT NOT NULL DEFAULT '{"ids":["summary","comparison","details","configuration","artifacts"]}',
                  status TEXT NOT NULL,
                  file_name TEXT,
                  file_path TEXT,
                  storage_backend TEXT NOT NULL DEFAULT 's3',
                  object_key TEXT,
                  size_bytes INTEGER NOT NULL DEFAULT 0,
                  error_message TEXT,
                  created_at TEXT NOT NULL,
                  completed_at TEXT
                )
                """
            )
            connection.execute(
                """
                ALTER TABLE reports
                ADD COLUMN IF NOT EXISTS sections_json TEXT NOT NULL DEFAULT '{"ids":["summary","comparison","details","configuration","artifacts"]}'
                """
            )
            connection.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 's3'")
            connection.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS object_key TEXT")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_artifacts (
                  id TEXT PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  artifact_type TEXT NOT NULL,
                  file_name TEXT NOT NULL,
                  file_path TEXT NOT NULL DEFAULT '',
                  storage_backend TEXT NOT NULL DEFAULT 's3',
                  object_key TEXT,
                  file_format TEXT NOT NULL,
                  record_count INTEGER NOT NULL DEFAULT 0,
                  size_bytes INTEGER NOT NULL DEFAULT 0,
                  created_at TEXT NOT NULL
                )
                """
            )
            connection.execute("ALTER TABLE run_artifacts ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 's3'")
            connection.execute("ALTER TABLE run_artifacts ADD COLUMN IF NOT EXISTS object_key TEXT")


class LocalObjectStore:
    backend_name = "local"

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def put_file(self, source_path: Path, *, object_key: str) -> dict[str, Any]:
        destination = self.root / object_key
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source_path.resolve() != destination.resolve():
            shutil.copy2(source_path, destination)
        size_bytes = destination.stat().st_size if destination.exists() else 0
        return {
            "storageBackend": "local",
            "filePath": str(destination),
            "objectKey": None,
            "sizeBytes": int(size_bytes),
        }

    def put_bytes(self, payload: bytes, *, object_key: str) -> dict[str, Any]:
        destination = self.root / object_key
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return {
            "storageBackend": "local",
            "filePath": str(destination),
            "objectKey": None,
            "sizeBytes": int(destination.stat().st_size),
        }

    def get_bytes(self, *, file_path: str | None = None, object_key: str | None = None) -> bytes:
        candidate = Path(file_path or self.root / str(object_key or ""))
        return candidate.read_bytes()

    def exists(self, *, file_path: str | None = None, object_key: str | None = None) -> bool:
        candidate = Path(file_path or self.root / str(object_key or ""))
        return candidate.exists()

    def delete(self, *, file_path: str | None = None, object_key: str | None = None) -> None:
        candidate = Path(file_path or self.root / str(object_key or ""))
        candidate.unlink(missing_ok=True)


class S3ObjectStore:
    backend_name = "s3"

    def __init__(
        self,
        *,
        endpoint_url: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region_name: str = "us-east-1",
        secure: bool = False,
    ) -> None:
        if boto3 is None:  # pragma: no cover - dependency installed in runtime
            raise RuntimeError("boto3 is not installed")
        self.endpoint_url = endpoint_url
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            use_ssl=secure,
        )

    def ensure_ready(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception:
            self.client.create_bucket(Bucket=self.bucket)

    def put_file(self, source_path: Path, *, object_key: str) -> dict[str, Any]:
        self.client.upload_file(str(source_path), self.bucket, object_key)
        return {
            "storageBackend": "s3",
            "filePath": "",
            "objectKey": object_key,
            "sizeBytes": int(source_path.stat().st_size if source_path.exists() else 0),
        }

    def put_bytes(self, payload: bytes, *, object_key: str) -> dict[str, Any]:
        self.client.put_object(Bucket=self.bucket, Key=object_key, Body=payload)
        return {
            "storageBackend": "s3",
            "filePath": "",
            "objectKey": object_key,
            "sizeBytes": int(len(payload)),
        }

    def get_bytes(self, *, file_path: str | None = None, object_key: str | None = None) -> bytes:
        del file_path
        response = self.client.get_object(Bucket=self.bucket, Key=str(object_key or ""))
        return response["Body"].read()

    def exists(self, *, file_path: str | None = None, object_key: str | None = None) -> bool:
        del file_path
        try:
            self.client.head_object(Bucket=self.bucket, Key=str(object_key or ""))
            return True
        except Exception:
            return False

    def delete(self, *, file_path: str | None = None, object_key: str | None = None) -> None:
        del file_path
        if not object_key:
            return
        self.client.delete_object(Bucket=self.bucket, Key=object_key)


@dataclass
class RuntimeStorage:
    database: SQLiteDatabaseBackend | PostgresDatabaseBackend
    object_store: LocalObjectStore | S3ObjectStore
    local_db_path: Path
    artifact_dir: Path
    report_dir: Path
    mode: str

    @contextmanager
    def db(self) -> Iterator[Any]:
        with self.database.connect() as connection:
            yield connection

    def ensure_ready(self) -> None:
        self.database.ensure_schema()
        self.object_store.ensure_ready()
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def put_artifact_file(self, run_id: str, file_name: str, source_path: Path) -> dict[str, Any]:
        return self.object_store.put_file(
            source_path,
            object_key=f"artifacts/{run_id}/{Path(file_name).name}",
        )

    def put_report_file(self, report_id: str, file_name: str, source_path: Path) -> dict[str, Any]:
        del report_id
        return self.object_store.put_file(
            source_path,
            object_key=f"reports/{Path(file_name).name}",
        )

    def put_imported_artifact(self, run_id: str, file_name: str, payload: bytes) -> dict[str, Any]:
        return self.object_store.put_bytes(
            payload,
            object_key=f"artifacts/{run_id}/{Path(file_name).name}",
        )

    def row_bytes(self, row: Mapping[str, Any]) -> bytes:
        storage_backend = str(row.get("storage_backend") or "local").strip().lower() or "local"
        file_path = str(row.get("file_path") or "").strip() or None
        object_key = str(row.get("object_key") or "").strip() or None
        if storage_backend == "s3":
            return self.object_store.get_bytes(object_key=object_key)
        return LocalObjectStore(self.local_db_path.parent).get_bytes(file_path=file_path, object_key=object_key)

    def row_exists(self, row: Mapping[str, Any]) -> bool:
        storage_backend = str(row.get("storage_backend") or "local").strip().lower() or "local"
        file_path = str(row.get("file_path") or "").strip() or None
        object_key = str(row.get("object_key") or "").strip() or None
        if storage_backend == "s3":
            return self.object_store.exists(object_key=object_key)
        return LocalObjectStore(self.local_db_path.parent).exists(file_path=file_path, object_key=object_key)

    def delete_row_object(self, row: Mapping[str, Any]) -> None:
        storage_backend = str(row.get("storage_backend") or "local").strip().lower() or "local"
        file_path = str(row.get("file_path") or "").strip() or None
        object_key = str(row.get("object_key") or "").strip() or None
        if storage_backend == "s3":
            self.object_store.delete(object_key=object_key)
            return
        LocalObjectStore(self.local_db_path.parent).delete(file_path=file_path, object_key=object_key)


def _postgres_config_from_env() -> PostgresConfig | None:
    platform_data_namespace = str(os.environ.get("BUS_PLATFORM_DATA_NAMESPACE", "bench-platform-data")).strip() or "bench-platform-data"
    cluster_name = str(os.environ.get("BUS_POSTGRES_CLUSTER_NAME", "")).strip()
    host = str(os.environ.get("BUS_POSTGRES_HOST", "")).strip()
    database = str(os.environ.get("BUS_POSTGRES_DATABASE", "")).strip()
    username = str(os.environ.get("BUS_POSTGRES_USERNAME", "")).strip()
    password = str(os.environ.get("BUS_POSTGRES_PASSWORD", "")).strip()
    if not host and cluster_name:
        host = f"{cluster_name}-rw.{platform_data_namespace}.svc.cluster.local"
    if not all([host, database, username, password]):
        return None
    try:
        port = max(1, int(os.environ.get("BUS_POSTGRES_PORT", "5432")))
    except ValueError:
        port = 5432
    return PostgresConfig(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )


def _s3_store_from_env() -> S3ObjectStore | None:
    platform_data_namespace = str(os.environ.get("BUS_PLATFORM_DATA_NAMESPACE", "bench-platform-data")).strip() or "bench-platform-data"
    tenant_name = str(os.environ.get("BUS_MINIO_TENANT_NAME", "")).strip()
    endpoint_url = str(os.environ.get("BUS_S3_ENDPOINT", "")).strip()
    if not endpoint_url and tenant_name:
        endpoint_url = f"http://{tenant_name}-hl.{platform_data_namespace}.svc.cluster.local:9000"
    bucket = str(os.environ.get("BUS_MINIO_BUCKET", "")).strip()
    access_key = str(os.environ.get("BUS_MINIO_APP_ACCESS_KEY", "")).strip()
    secret_key = str(os.environ.get("BUS_MINIO_APP_SECRET_KEY", "")).strip()
    if not all([endpoint_url, bucket, access_key, secret_key]):
        return None
    return S3ObjectStore(
        endpoint_url=endpoint_url,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        region_name=str(os.environ.get("BUS_S3_REGION", "us-east-1")).strip() or "us-east-1",
        secure=str(os.environ.get("BUS_S3_SECURE", "")).strip().lower() in {"1", "true", "yes", "on"},
    )


def resolve_runtime_storage(
    *,
    db_path: Path | None = None,
    artifact_dir: Path | None = None,
    report_dir: Path | None = None,
) -> RuntimeStorage:
    local_db_path = Path(db_path or DEFAULT_DB_PATH)
    local_root = local_db_path.parent
    local_artifact_dir = Path(artifact_dir or os.environ.get("BUS_ARTIFACT_DIR", local_root / "artifacts"))
    local_report_dir = Path(report_dir or os.environ.get("BUS_REPORT_DIR", local_root / "reports"))

    mode = str(os.environ.get("BUS_STORAGE_MODE", "local")).strip().lower() or "local"
    if mode not in {"auto", "local", "external"}:
        mode = "local"

    local_backend = SQLiteDatabaseBackend(local_db_path)
    local_store = LocalObjectStore(local_root)
    if mode == "local":
        return RuntimeStorage(
            database=local_backend,
            object_store=local_store,
            local_db_path=local_db_path,
            artifact_dir=local_artifact_dir,
            report_dir=local_report_dir,
            mode="local",
        )

    postgres_config = _postgres_config_from_env()
    s3_store = _s3_store_from_env()
    if mode == "external":
        if postgres_config is None:
            raise RuntimeError(
                "External storage mode requires PostgreSQL configuration in BUS_POSTGRES_* variables."
            )
        if s3_store is None:
            raise RuntimeError(
                "External storage mode requires S3 configuration in BUS_S3_* / BUS_MINIO_* variables."
            )
        return RuntimeStorage(
            database=PostgresDatabaseBackend(postgres_config),
            object_store=s3_store,
            local_db_path=local_db_path,
            artifact_dir=local_artifact_dir,
            report_dir=local_report_dir,
            mode="postgres+s3",
        )

    if mode == "auto" and postgres_config and s3_store:
        return RuntimeStorage(
            database=PostgresDatabaseBackend(postgres_config),
            object_store=s3_store,
            local_db_path=local_db_path,
            artifact_dir=local_artifact_dir,
            report_dir=local_report_dir,
            mode="postgres+s3",
        )

    return RuntimeStorage(
        database=local_backend,
        object_store=local_store,
        local_db_path=local_db_path,
        artifact_dir=local_artifact_dir,
        report_dir=local_report_dir,
        mode="local",
    )
