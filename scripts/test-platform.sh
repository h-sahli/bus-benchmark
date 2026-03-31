#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

python_cmd="$(resolve_python_cmd)"
venv_dir="${VENV_DIR:-${REPO_ROOT}/.venv}"

if [[ ! -x "${venv_dir}/bin/python" ]]; then
  log "Creating virtual environment in ${venv_dir}"
  "${python_cmd}" -m venv "${venv_dir}"
fi

# shellcheck disable=SC1090
source "${venv_dir}/bin/activate"

python -m pip install --upgrade pip >/dev/null
python -m pip install -r "${REPO_ROOT}/services/platform/requirements-dev.txt" -r "${REPO_ROOT}/services/benchmark-agent/requirements.txt" >/dev/null

if [[ "${SKIP_PLAYWRIGHT_INSTALL:-0}" != "1" ]]; then
  python -m playwright install chromium >/dev/null
fi

python -m pytest "$@"
