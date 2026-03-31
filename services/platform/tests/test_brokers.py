from services.platform.brokers import (
    broker_definition,
    broker_catalog_entries,
    build_supported_client_args,
    normalize_protocol_for_broker,
    sanitize_broker_tuning,
    target_replicas_for_mode,
)
from fastapi import HTTPException


def test_broker_catalog_entries_include_supported_brokers() -> None:
    broker_ids = {item["id"] for item in broker_catalog_entries()}
    assert broker_ids == {"kafka", "rabbitmq", "artemis", "nats"}
    assert broker_definition("nats")["controlPlaneRequired"] is True


def test_normalize_protocol_for_broker_uses_default_and_validates() -> None:
    assert normalize_protocol_for_broker("kafka", None) == "kafka"
    assert normalize_protocol_for_broker("rabbitmq", "amqp-0-9-1") == "amqp-0-9-1"


def test_build_supported_client_args_renders_known_flags() -> None:
    producer_args, consumer_args = build_supported_client_args(
        "kafka",
        {
            "producer": {"lingerMs": 5, "acks": "all", "deliveryTimeoutMs": 45000, "socketNagleDisable": True},
            "consumer": {"fetchMinBytes": 1, "enableAutoCommit": False, "maxPollIntervalMs": 60000, "socketNagleDisable": True},
        },
    )

    assert "--linger-ms=5" in producer_args
    assert "--acks=all" in producer_args
    assert "--delivery-timeout-ms=45000" in producer_args
    assert "--producer-socket-nagle-disable=true" in producer_args
    assert "--fetch-min-bytes=1" in consumer_args
    assert "--enable-auto-commit=false" in consumer_args
    assert "--max-poll-interval-ms=60000" in consumer_args
    assert "--consumer-socket-nagle-disable=true" in consumer_args


def test_target_replicas_for_mode_maps_normal_and_ha() -> None:
    assert target_replicas_for_mode("normal") == 1
    assert target_replicas_for_mode("ha") == 3


def test_sanitize_broker_tuning_rejects_invalid_rabbitmq_frame_max() -> None:
    try:
        sanitize_broker_tuning("rabbitmq", {"producer": {"frameMax": 262144}})
    except HTTPException as exc:
        assert exc.status_code == 422
        assert "131072" in str(exc.detail)
    else:
        raise AssertionError("Expected RabbitMQ frame max validation to fail")
