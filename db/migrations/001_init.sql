CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  broker_id TEXT NOT NULL,
  protocol TEXT NOT NULL,
  scenario_id TEXT,
  config_mode TEXT NOT NULL DEFAULT 'baseline',
  deployment_mode TEXT NOT NULL DEFAULT 'normal',
  starts_at TEXT NOT NULL,
  warmup_seconds INTEGER NOT NULL,
  measurement_seconds INTEGER NOT NULL,
  cooldown_seconds INTEGER NOT NULL,
  message_rate INTEGER NOT NULL,
  message_size_bytes INTEGER NOT NULL,
  producers INTEGER NOT NULL,
  consumers INTEGER NOT NULL,
  transport_options TEXT,
  ha_mode INTEGER NOT NULL DEFAULT 0,
  broker_tuning_json TEXT NOT NULL DEFAULT '{}',
  metrics_json TEXT NOT NULL DEFAULT '{}',
  topology_ready_at TEXT,
  execution_started_at TEXT,
  completed_at TEXT,
  topology_deleted_at TEXT,
  created_at TEXT NOT NULL,
  stopped_at TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
  id TEXT PRIMARY KEY,
  run_id TEXT,
  event_type TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  run_ids_json TEXT NOT NULL,
  status TEXT NOT NULL,
  file_name TEXT,
  file_path TEXT,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  created_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS run_artifacts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  file_format TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
