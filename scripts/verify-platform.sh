#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd kubectl
require_cmd curl
python_cmd="$(repo_python_cmd runtime)"
versions_file="${REPO_ROOT}/deploy/k8s/operators/versions.yaml"

BASE_URL="${BASE_URL:-http://127.0.0.1:18080}"
EXPECTED_STORAGE_MODE="${EXPECTED_STORAGE_MODE:-auto}"
PORT_FORWARD_PID=""
HEALTH_BASE_URL="${BASE_URL%/}"

cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" 2>/dev/null || true
  fi
}

wait_for_url() {
  local url="$1"
  local timeout_seconds="${2:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'Timed out waiting for %s\n' "${url}" >&2
      exit 1
    fi
    sleep 1
  done
}

trap cleanup EXIT

kubectl -n bench-platform rollout status deployment/bus-platform --timeout=300s >/dev/null

if ! curl -fsS "${HEALTH_BASE_URL}/api/health" >/dev/null 2>&1; then
  if ! wait_for_url "${HEALTH_BASE_URL}/api/health" 120 >/dev/null 2>&1; then
    "${SCRIPT_DIR}/port-forward-platform.sh" >/tmp/bus-platform-port-forward.log 2>&1 &
    PORT_FORWARD_PID=$!
    HEALTH_BASE_URL="http://127.0.0.1:18080"
    wait_for_url "${HEALTH_BASE_URL}/api/health" 180
  fi
fi

health_payload="$(curl -fsS "${HEALTH_BASE_URL}/api/health")"
bootstrap_payload="$(curl -fsS "${HEALTH_BASE_URL}/api/system/bootstrap")"

"${python_cmd}" - "${health_payload}" "${bootstrap_payload}" "${EXPECTED_STORAGE_MODE}" <<'PY'
import json
import sys

health = json.loads(sys.argv[1])
bootstrap = json.loads(sys.argv[2])
expected_storage_mode = str(sys.argv[3] or "auto").strip().lower() or "auto"

if health.get("status") != "ok":
    raise SystemExit("runtime health check failed")

storage_mode = str(health.get("storageMode") or "").strip()
if expected_storage_mode not in {"auto", "local", "external"}:
    raise SystemExit(f"unsupported EXPECTED_STORAGE_MODE: {expected_storage_mode!r}")
if expected_storage_mode == "local" and storage_mode != "local":
    raise SystemExit(f"unexpected storage mode: {storage_mode!r}")
if expected_storage_mode == "external" and storage_mode != "postgres+s3":
    raise SystemExit(f"unexpected storage mode: {storage_mode!r}")
if expected_storage_mode == "auto" and storage_mode not in {"local", "postgres+s3"}:
    raise SystemExit(f"unexpected storage mode: {storage_mode!r}")

if not health.get("storageReady", True):
    raise SystemExit(f"runtime storage not ready: {health.get('storageMessage')}")

access_url = str(health.get("accessUrl") or "").strip()
if not access_url.startswith(("http://", "https://")):
    raise SystemExit("runtime did not publish a reachable access URL")

operators = bootstrap.get("operators") or {}
required = ("kafka", "rabbitmq", "artemis", "nats")
not_ready = [name for name in required if not operators.get(name, {}).get("ready")]
if not_ready:
    raise SystemExit(f"operators not ready: {', '.join(not_ready)}")

if storage_mode == "postgres+s3":
    platform_operators = bootstrap.get("platformOperators") or {}
    platform_services = bootstrap.get("platformServices") or {}
    storage_not_ready = [
        name
        for name in ("cnpg", "minio")
        if not platform_operators.get(name, {}).get("ready")
    ]
    if storage_not_ready:
        raise SystemExit(f"platform operators not ready: {', '.join(storage_not_ready)}")

    services_not_ready = [
        name
        for name in ("postgres", "objectStore")
        if not platform_services.get(name, {}).get("ready")
    ]
    if services_not_ready:
        raise SystemExit(f"platform services not ready: {', '.join(services_not_ready)}")
PY

for namespace in bench-kafka bench-rabbitmq bench-artemis; do
  if kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    printf 'Legacy namespace still present: %s\n' "${namespace}" >&2
    exit 1
  fi
done

if kubectl get namespace bench-rabbitmq-operator >/dev/null 2>&1; then
  printf 'Legacy RabbitMQ operator namespace still present: bench-rabbitmq-operator\n' >&2
  exit 1
fi

if [[ "${EXPECTED_STORAGE_MODE}" == "external" ]]; then
  expected_cnpg_version="$(yaml_value "${versions_file}" cnpg operatorVersion)"
  if [[ -n "${expected_cnpg_version}" ]]; then
    cnpg_image="$(kubectl -n bench-cnpg-operator get deploy cloudnative-pg -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
    if [[ "${cnpg_image}" != *":${expected_cnpg_version}" ]]; then
      printf 'Unexpected CNPG operator image: %s (expected tag %s)\n' "${cnpg_image}" "${expected_cnpg_version}" >&2
      exit 1
    fi
  fi
fi

log "Platform runtime is healthy. Access URL: $(printf '%s' "${health_payload}" | "${python_cmd}" -c 'import json,sys; print(json.loads(sys.stdin.read()).get("accessUrl",""))')"
