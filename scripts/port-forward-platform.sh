#!/usr/bin/env bash
set -euo pipefail

exec kubectl -n bench-platform port-forward --address 127.0.0.1 service/bus-platform 18080:80
