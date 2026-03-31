from services.platform.versions import (
    artemis_chart_ref,
    artemis_chart_version,
    artemis_operator_version,
    cnpg_operator_version,
    nack_chart_ref,
    nack_chart_version,
    nack_controller_version,
    nats_chart_ref,
    nats_chart_version,
    nats_runtime_version,
    rabbitmq_operator_manifest_url,
    rabbitmq_operator_version,
    strimzi_chart_ref,
    strimzi_chart_version,
    strimzi_operator_version,
)


def test_operator_version_catalog_is_consistent() -> None:
    assert strimzi_chart_ref() == "strimzi/strimzi-kafka-operator"
    assert strimzi_chart_version() == "0.51.0"
    assert strimzi_operator_version() == "0.51.0"
    assert cnpg_operator_version() == "1.28.1"

    assert rabbitmq_operator_manifest_url().startswith(
        "https://github.com/rabbitmq/cluster-operator/releases/download/v2.17.0/"
    )
    assert rabbitmq_operator_version() == "2.17.0"

    assert artemis_chart_ref().startswith(
        "oci://quay.io/arkmq-org/helm-charts/arkmq-org-broker-operator"
    )
    assert artemis_chart_version() == "2.1.4"
    assert artemis_operator_version() == "2.1.4"

    assert nats_chart_ref() == "nats/nats"
    assert nats_chart_version() == "2.12.6"
    assert nats_runtime_version() == "2.12.6"

    assert nack_chart_ref() == "nats/nack"
    assert nack_chart_version() == "0.33.2"
    assert nack_controller_version() == "0.22.2"
