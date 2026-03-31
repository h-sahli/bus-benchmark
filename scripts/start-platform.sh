#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

clean_restart=1
wipe_data=0
deploy_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-clean)
      clean_restart=0
      shift
      ;;
    --wipe-data)
      wipe_data=1
      shift
      ;;
    *)
      deploy_args+=("$1")
      shift
      ;;
  esac
done

if [[ "${clean_restart}" == "1" ]]; then
  log "Resetting the previous platform runtime before redeploy."
  reset_args=()
  if [[ "${wipe_data}" == "1" ]]; then
    reset_args+=(--wipe-data)
  fi
  "${SCRIPT_DIR}/reset-platform.sh" "${reset_args[@]}"
fi

exec "${SCRIPT_DIR}/deploy-platform.sh" "${deploy_args[@]}"
