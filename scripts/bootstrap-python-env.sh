#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

profile="runtime"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      profile="dev"
      shift
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

python_path="$(repo_python_cmd "${profile}")"
log "Python environment ready (${profile}) at ${python_path}"
