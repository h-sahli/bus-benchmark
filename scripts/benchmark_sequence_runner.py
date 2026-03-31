from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import traceback
import urllib.error
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ACTIVE_STATUSES = {"preparing", "scheduled", "warmup", "measuring", "cooldown", "finalizing", "cleaning"}
CREATE_RUN_KEYS = (
    "name",
    "brokerId",
    "protocol",
    "scenarioId",
    "configMode",
    "deploymentMode",
    "startsAt",
    "warmupSeconds",
    "measurementSeconds",
    "cooldownSeconds",
    "messageRate",
    "messageSizeBytes",
    "producers",
    "consumers",
    "transportOptions",
    "brokerTuning",
    "resourceConfig",
)


LOG_PATH: Path | None = None


def utc_log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    line = f"[{stamp}] {message}"
    print(line, flush=True)
    if LOG_PATH is not None:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def write_state(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class ApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def request_json(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None
        headers: dict[str, str] = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base_url + path, data=data, headers=headers, method=method)
        for attempt in range(1, 6):
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    raw = response.read().decode("utf-8")
                    return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", "replace")
                raise RuntimeError(f"{method} {path} failed: {exc.code} {body}") from exc
            except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
                if attempt == 5:
                    raise RuntimeError(f"{method} {path} failed after retries: {exc}") from exc
                time.sleep(min(20, attempt * 5))
        return {}

    def list_runs(self) -> list[dict[str, Any]]:
        return list(self.request_json("GET", "/api/runs").get("runs", []))

    def get_run(self, run_id: str) -> dict[str, Any]:
        return dict(self.request_json("GET", f"/api/runs/{run_id}").get("run", {}))

    def get_run_details(self, run_id: str) -> dict[str, Any]:
        return self.request_json("GET", f"/api/runs/{run_id}")

    def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request_json("POST", "/api/runs", payload)


def load_spec(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def wait_until_idle(client: ApiClient, poll_seconds: int, state_path: Path | None = None) -> None:
    while True:
        try:
            runs = client.list_runs()
        except Exception as exc:
            utc_log(f"Idle check failed: {exc}")
            write_state(
                state_path,
                {
                    "phase": "waiting-for-idle",
                    "lastError": str(exc),
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                },
            )
            time.sleep(max(10, poll_seconds))
            continue
        active = [
            run for run in runs
            if str(run.get("status") or "").strip().lower() in ACTIVE_STATUSES
        ]
        if not active:
            write_state(
                state_path,
                {
                    "phase": "idle",
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                },
            )
            return
        description = ", ".join(f"{run.get('name')}={run.get('status')}" for run in active)
        utc_log(f"Waiting for active slot: {description}")
        write_state(
            state_path,
            {
                "phase": "waiting-for-idle",
                "activeRuns": [
                    {
                        "id": run.get("id"),
                        "name": run.get("name"),
                        "status": run.get("status"),
                    }
                    for run in active
                ],
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )
        time.sleep(max(5, poll_seconds))


def terminal_events(details: dict[str, Any]) -> list[dict[str, str]]:
    events = details.get("events")
    if not isinstance(events, list):
        return []
    trimmed: list[dict[str, str]] = []
    for event in events[:5]:
        if not isinstance(event, dict):
            continue
        trimmed.append(
            {
                "createdAt": str(event.get("created_at") or ""),
                "type": str(event.get("event_type") or ""),
                "message": str(event.get("message") or ""),
            }
        )
    return trimmed


def wait_for_terminal(client: ApiClient, run_id: str, poll_seconds: int, state_path: Path | None = None) -> dict[str, Any]:
    while True:
        try:
            details = client.get_run_details(run_id)
        except Exception as exc:
            utc_log(f"Run poll failed for {run_id}: {exc}")
            write_state(
                state_path,
                {
                    "phase": "monitoring-run",
                    "runId": run_id,
                    "lastError": str(exc),
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                },
            )
            time.sleep(max(10, poll_seconds))
            continue
        run = dict(details.get("run") or {})
        status = str(run.get("status") or "").strip().lower()
        progress = run.get("progress")
        progress_text = f" progress={progress}%" if progress is not None else ""
        utc_log(f"{run.get('name') or run_id} status={status}{progress_text}")
        write_state(
            state_path,
            {
                "phase": "monitoring-run",
                "runId": run_id,
                "runName": run.get("name"),
                "status": status,
                "progress": progress,
                "events": terminal_events(details),
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )
        if status in {"completed", "stopped"}:
            for event in terminal_events(details):
                utc_log(f"event {event['createdAt']} {event['type']}: {event['message']}")
            return run
        time.sleep(max(5, poll_seconds))


def payload_from_run(run: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in CREATE_RUN_KEYS:
        if key in run:
            payload[key] = deepcopy(run[key])
    return payload


def find_run_by_name(runs: list[dict[str, Any]], run_name: str) -> dict[str, Any]:
    candidate = next((run for run in runs if str(run.get("name") or "").lower() == run_name.lower()), None)
    if candidate is None:
        raise RuntimeError(f"Run named '{run_name}' was not found")
    return candidate


def build_payload(client: ApiClient, item: dict[str, Any]) -> dict[str, Any]:
    if item.get("cloneFromRunName"):
        base_run = find_run_by_name(client.list_runs(), str(item["cloneFromRunName"]))
        payload = payload_from_run(base_run)
        payload.update(deepcopy(item.get("payload") or {}))
        return payload
    payload = deepcopy(item.get("payload") or {})
    if not payload:
        raise RuntimeError("Sequence item is missing payload")
    return payload


def run_sequence(client: ApiClient, spec: dict[str, Any], state_path: Path | None = None) -> None:
    runs = spec.get("runs")
    if not isinstance(runs, list) or not runs:
        raise RuntimeError("Spec must contain a non-empty 'runs' list")
    poll_seconds = int(spec.get("pollSeconds") or 30)
    continue_on_error = bool(spec.get("continueOnError", False))

    utc_log("Benchmark sequence starting")
    write_state(
        state_path,
        {
            "phase": "starting",
            "totalRuns": len(runs),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        },
    )
    for index, item in enumerate(runs, start=1):
        if not isinstance(item, dict):
            raise RuntimeError("Each sequence item must be an object")
        wait_until_idle(client, poll_seconds, state_path)
        payload = build_payload(client, item)
        run_name = str(payload.get("name") or "").strip() or f"run-{index}"
        utc_log(f"Creating {run_name}")
        write_state(
            state_path,
            {
                "phase": "creating-run",
                "index": index,
                "totalRuns": len(runs),
                "runName": run_name,
                "payload": payload,
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )
        try:
            created = client.create_run(payload)
        except Exception as exc:
            utc_log(f"Failed to create {run_name}: {exc}")
            write_state(
                state_path,
                {
                    "phase": "create-failed",
                    "index": index,
                    "totalRuns": len(runs),
                    "runName": run_name,
                    "lastError": str(exc),
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                },
            )
            if not continue_on_error:
                raise
            continue
        run_id = str(created.get("id") or "")
        utc_log(f"Created {run_name} with id={run_id}")
        write_state(
            state_path,
            {
                "phase": "run-created",
                "index": index,
                "totalRuns": len(runs),
                "runId": run_id,
                "runName": run_name,
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )
        final_run = wait_for_terminal(client, run_id, poll_seconds, state_path)
        final_status = str(final_run.get("status") or "").strip().lower()
        utc_log(f"Finished {run_name} with status={final_status}")
        write_state(
            state_path,
            {
                "phase": "run-finished",
                "index": index,
                "totalRuns": len(runs),
                "runId": run_id,
                "runName": run_name,
                "status": final_status,
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )
        if final_status == "stopped" and not continue_on_error:
            raise RuntimeError(f"Run {run_name} stopped before completion")
    utc_log("Benchmark sequence finished")
    write_state(
        state_path,
        {
            "phase": "finished",
            "totalRuns": len(runs),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        },
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a sequential benchmark queue against the Bus platform API.")
    parser.add_argument("--base-url", required=True, help="Platform base URL, for example http://localhost:8099")
    parser.add_argument("--spec", required=True, help="Path to the JSON sequence spec")
    parser.add_argument("--state-file", help="Optional path to a JSON state file for external monitoring")
    parser.add_argument("--log-file", help="Optional path to a plain-text log file")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    global LOG_PATH
    args = parse_args(argv)
    LOG_PATH = Path(args.log_file) if args.log_file else None
    client = ApiClient(args.base_url)
    spec = load_spec(Path(args.spec))
    run_sequence(client, spec, Path(args.state_file) if args.state_file else None)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Exception:
        utc_log("Fatal error in benchmark sequence runner")
        for line in traceback.format_exc().strip().splitlines():
            utc_log(line)
        raise
