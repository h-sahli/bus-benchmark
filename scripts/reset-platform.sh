#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd kubectl

wipe_data=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wipe-data)
      wipe_data=1
      shift
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

if command -v helm >/dev/null 2>&1; then
  helm -n bench-platform uninstall bus-platform --wait --timeout 5m >/dev/null 2>&1 || true
fi

kubectl delete namespace bench-platform --ignore-not-found=true --wait=false >/dev/null 2>&1 || true

while IFS= read -r namespace; do
  [[ -z "${namespace}" ]] && continue
  kubectl delete namespace "${namespace}" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
done < <(list_namespaces_by_prefix "bench-run-")

delete_legacy_broker_namespaces

if [[ "${wipe_data}" == "1" ]]; then
  kubectl delete namespace bench-platform-data --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
  clear_local_runtime_state
fi

wait_for_namespace_absent "bench-platform" 300 || true
if [[ "${wipe_data}" == "1" ]]; then
  wait_for_namespace_absent "bench-platform-data" 300 || true
  log "Platform runtime, run namespaces, external platform data, and local runtime data were cleared."
else
  log "Platform runtime and run namespaces were cleared. External platform data and local run history were preserved."
fi
