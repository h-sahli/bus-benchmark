#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

cleanup_stale_port_forwards() {
  local pids=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    pids+=("${line%% *}")
  done < <(ps -eo pid=,args= | awk '/kubectl/ && /port-forward/ && /bench-platform/ && /18080:80/ { print $1 " " $0 }')

  for pid in "${pids[@]}"; do
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" 2>/dev/null || true
  done
}

cleanup_stale_port_forwards

if command -v lsof >/dev/null 2>&1 && lsof -ti tcp:18080 >/dev/null 2>&1; then
  printf 'Local port 18080 is already in use by a non-platform process.\n' >&2
  exit 1
fi

exec kubectl -n bench-platform port-forward --address 127.0.0.1 service/bus-platform 18080:80
