#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd kubectl
require_cmd helm
python_cmd="$(resolve_python_cmd)"

log "Bootstrapping platform data operators and services from the shared runtime automation path"
"${python_cmd}" -m services.platform.bootstrap --scope platform-data
log "Platform data control plane is ready."
