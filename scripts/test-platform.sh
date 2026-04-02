#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

python_cmd="$(repo_python_cmd dev)"

if [[ "${SKIP_PLAYWRIGHT_INSTALL:-0}" != "1" ]]; then
  "${python_cmd}" -m playwright install chromium >/dev/null
fi

"${python_cmd}" -m pytest "$@"
