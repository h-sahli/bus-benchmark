from pathlib import Path

import yaml


def test_cross_broker_ha_burst_preset_is_consistent() -> None:
    payload = yaml.safe_load(
        Path("catalog/scenarios/defaults.yaml").read_text(encoding="utf-8")
    )
    presets = payload.get("presets") or []
    preset = next(item for item in presets if item.get("id") == "cross-broker-ha-burst")

    assert preset["producers"] == 10
    assert preset["consumers"] == 10
    assert preset["payloadSizeBytes"] == 1024
    assert preset["rateProfile"] == "endurance-burst"
    assert preset["deploymentMode"] == "ha"

    for broker_id in ("kafka", "rabbitmq", "artemis", "nats"):
        target = (preset.get("brokerTargets") or {}).get(broker_id) or {}
        assert target.get("deploymentMode") == "ha"
        resource_config = target.get("resourceConfig") or {}
        assert int((resource_config.get("replicas")) or 0) == 3
        assert resource_config.get("cpuRequest") == "3"
        assert resource_config.get("cpuLimit") == "3"
        assert resource_config.get("memoryRequest") == "4352Mi"
        assert resource_config.get("memoryLimit") == "4352Mi"
        assert resource_config.get("storageSize") == "32Gi"
