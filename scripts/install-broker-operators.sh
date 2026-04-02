#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd kubectl
require_cmd helm
python_cmd="$(repo_python_cmd runtime)"

log "Bootstrapping broker operators from the shared runtime automation path"
"${python_cmd}" -m services.platform.bootstrap --scope brokers
log "Broker operators installed and ready."
