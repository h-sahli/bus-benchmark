from __future__ import annotations

import argparse
import json
import os
import sys

from services.platform.cluster import ClusterAutomation
from services.platform.runtime_storage import REPO_ROOT


VALID_SCOPES = {
    "brokers": "ensure_broker_operators",
    "platform-operators": "ensure_platform_data_operators",
    "platform-services": "ensure_platform_data_services",
    "platform-data": "ensure_platform_data",
    "all": "ensure_operators",
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap broker and platform data control planes.")
    parser.add_argument(
        "--scope",
        choices=sorted(VALID_SCOPES.keys()),
        default="brokers",
        help="Bootstrap scope to execute.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    os.environ.setdefault("BUS_ENABLE_CLUSTER_ACTIONS", "true")
    os.environ.setdefault("BUS_ENABLE_OPERATOR_BOOTSTRAP", "true")
    automation = ClusterAutomation(REPO_ROOT)
    handler_name = VALID_SCOPES[args.scope]
    handler = getattr(automation, handler_name)
    status = handler()
    print(json.dumps(status, indent=2, sort_keys=True), flush=True)
    return 0 if bool(status.get("ready")) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
