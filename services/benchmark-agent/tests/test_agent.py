import asyncio
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
from types import SimpleNamespace


AGENT_PATH = Path(__file__).resolve().parents[1] / "agent.py"
SPEC = spec_from_file_location("benchmark_agent_module", AGENT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark agent module from {AGENT_PATH}")
agent = module_from_spec(SPEC)
sys.modules[SPEC.name] = agent
SPEC.loader.exec_module(agent)


def test_build_and_parse_cloudevent_roundtrip() -> None:
    payload = b"abc123"
    event, send_ns = agent.build_cloudevent(
        run_id="run-1",
        scenario_id="scenario-1",
        broker_id="kafka",
        producer_id="producer-1",
        receiver_id="consumer-1",
        sequence=7,
        payload=payload,
        destination="benchmark.events",
        config_mode="optimized",
        topology_mode="spsc",
        durability_mode="persistent",
        measurement_phase="measure",
        phase_elapsed_ms=1250,
    )

    assert event["specversion"] == "1.0"
    assert event["benchmarkrunid"] == "run-1"
    assert event["producerseq"] == 7
    assert event["producersendstartns"] == send_ns
    assert event["measurementphase"] == "measure"
    assert event["phaseelapsedms"] == 1250

    raw = agent.json.dumps(event).encode("utf-8")
    parsed = agent.parse_cloudevent(raw)
    assert parsed["id"] == event["id"]
    assert parsed["benchmarkrunid"] == "run-1"
    assert parsed["destinationname"] == "benchmark.events"


def test_parse_cloudevent_rejects_missing_required_fields() -> None:
    raw = agent.json.dumps({"id": "broken"}).encode("utf-8")
    try:
        agent.parse_cloudevent(raw)
    except ValueError as exc:
        assert "specversion" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing CloudEvent fields")


def test_summarize_latencies_handles_empty_and_non_empty_sets() -> None:
    empty = agent.summarize_latencies([])
    assert empty["count"] == 0.0
    assert empty["p95"] == 0.0

    summary = agent.summarize_latencies([1.0, 2.0, 3.0, 4.0, 5.0])
    assert summary["count"] == 5.0
    assert summary["min"] == 1.0
    assert summary["max"] == 5.0
    assert summary["mean"] == 3.0
    assert summary["p95"] >= 4.0
    assert summary["p99"] >= summary["p95"]


def test_parser_defaults_are_stable() -> None:
    parser = agent.build_parser()
    args = parser.parse_args(
        [
            "--role=producer",
            "--broker=kafka",
            "--destination=benchmark.events",
            "--run-id=run-1",
            "--scenario-id=scenario-1",
        ]
    )
    assert args.config_mode == "baseline"
    assert args.topology_mode == "spsc"
    assert args.durability_mode == "persistent"
    assert args.measurement_seconds == 60
    assert args.idle_exit_seconds == 15


def test_parser_accepts_optional_broker_credentials() -> None:
    parser = agent.build_parser()
    args = parser.parse_args(
        [
            "--role=consumer",
            "--broker=artemis",
            "--destination=benchmark.events",
            "--run-id=run-1",
            "--scenario-id=scenario-1",
            "--broker-username=bench",
            "--broker-password=secret",
        ]
    )
    assert args.broker_username == "bench"
    assert args.broker_password == "secret"


def test_parser_accepts_nats_broker() -> None:
    parser = agent.build_parser()
    args = parser.parse_args(
        [
            "--role=producer",
            "--broker=nats",
            "--destination=benchmark.events",
            "--run-id=run-1",
            "--scenario-id=scenario-1",
            "--broker-url=nats://nats:4222",
        ]
    )
    assert args.broker == "nats"
    assert args.broker_url == "nats://nats:4222"


def test_jetstream_stream_name_sanitizes_subject() -> None:
    assert agent.jetstream_stream_name("benchmark.events") == "BENCHMARK-EVENTS"


def test_jetstream_config_helpers_use_file_storage_and_replication(monkeypatch) -> None:
    fake_api = SimpleNamespace(
        StreamConfig=lambda **kwargs: kwargs,
        ConsumerConfig=lambda **kwargs: kwargs,
        StorageType=SimpleNamespace(FILE="file"),
        AckPolicy=SimpleNamespace(EXPLICIT="explicit"),
        DeliverPolicy=SimpleNamespace(NEW="new"),
    )
    monkeypatch.setattr(agent, "nats_js_api", fake_api)

    stream_config = agent.jetstream_stream_config("BENCHMARK-EVENTS", "benchmark.events", 3)
    consumer_config = agent.jetstream_consumer_config(3)

    assert stream_config["name"] == "BENCHMARK-EVENTS"
    assert stream_config["subjects"] == ["benchmark.events"]
    assert stream_config["storage"] == "file"
    assert stream_config["num_replicas"] == 3
    assert consumer_config["ack_policy"] == "explicit"
    assert consumer_config["deliver_policy"] == "new"
    assert consumer_config["num_replicas"] == 3


def test_nats_adapter_publish_uses_replicated_stream_config(monkeypatch) -> None:
    added_configs: list[dict] = []

    class DummyJs:
        async def add_stream(self, config=None, **_kwargs):
            added_configs.append(config)
            return None

        async def update_stream(self, config=None, **_kwargs):
            added_configs.append(config)
            return None

        async def publish(self, _destination, _payload):
            return SimpleNamespace(seq=7)

    class DummyNc:
        def __init__(self):
            self._js = DummyJs()

        def jetstream(self):
            return self._js

        async def close(self):
            return None

    async def fake_connect(**_kwargs):
        return DummyNc()

    fake_api = SimpleNamespace(
        StreamConfig=lambda **kwargs: kwargs,
        ConsumerConfig=lambda **kwargs: kwargs,
        StorageType=SimpleNamespace(FILE="file"),
        AckPolicy=SimpleNamespace(EXPLICIT="explicit"),
        DeliverPolicy=SimpleNamespace(NEW="new"),
    )

    monkeypatch.setattr(agent, "nats", SimpleNamespace(connect=fake_connect))
    monkeypatch.setattr(agent, "nats_js_api", fake_api)

    adapter = agent.NatsJetStreamAdapter(
        "nats://nats:4222",
        stream_name="BENCHMARK-EVENTS",
        jetstream_replicas=3,
        connect_timeout_seconds=1,
    )

    metadata = adapter.publish("benchmark.events", b"{}", key="event-1")

    assert metadata["offset"] == "7"
    assert added_configs[0]["num_replicas"] == 3
    assert added_configs[0]["storage"] == "file"


def test_nats_adapter_publish_skips_stream_mutation_when_nack_manages_stream(monkeypatch) -> None:
    add_calls: list[dict] = []

    class DummyJs:
        async def add_stream(self, config=None, **_kwargs):
            add_calls.append(config)
            return None

        async def update_stream(self, config=None, **_kwargs):
            add_calls.append(config)
            return None

        async def publish(self, _destination, _payload):
            return SimpleNamespace(seq=8)

    class DummyNc:
        def __init__(self):
            self._js = DummyJs()

        def jetstream(self):
            return self._js

        async def close(self):
            return None

    async def fake_connect(**_kwargs):
        return DummyNc()

    fake_api = SimpleNamespace(
        StreamConfig=lambda **kwargs: kwargs,
        ConsumerConfig=lambda **kwargs: kwargs,
        StorageType=SimpleNamespace(FILE="file"),
        AckPolicy=SimpleNamespace(EXPLICIT="explicit"),
        DeliverPolicy=SimpleNamespace(NEW="new"),
    )

    monkeypatch.setattr(agent, "nats", SimpleNamespace(connect=fake_connect))
    monkeypatch.setattr(agent, "nats_js_api", fake_api)

    adapter = agent.NatsJetStreamAdapter(
        "nats://nats:4222",
        stream_name="BENCHMARK-EVENTS",
        jetstream_replicas=3,
        stream_managed_by="nack",
        connect_timeout_seconds=1,
    )

    metadata = adapter.publish("benchmark.events", b"{}", key="event-2")

    assert metadata["offset"] == "8"
    assert add_calls == []


def test_nats_adapter_async_publish_batches_ack_collection(monkeypatch) -> None:
    class DummyJs:
        async def publish(self, _destination, _payload):
            await asyncio.sleep(0)
            return SimpleNamespace(seq=11)

    class DummyNc:
        def __init__(self):
            self._js = DummyJs()

        def jetstream(self):
            return self._js

        async def close(self):
            return None

    async def fake_connect(**_kwargs):
        return DummyNc()

    monkeypatch.setattr(agent, "nats", SimpleNamespace(connect=fake_connect))
    monkeypatch.setattr(agent, "nats_js_api", SimpleNamespace())

    adapter = agent.NatsJetStreamAdapter(
        "nats://nats:4222",
        stream_name="BENCHMARK-EVENTS",
        jetstream_replicas=3,
        stream_managed_by="nack",
        connect_timeout_seconds=1,
    )

    metadata = adapter.publish(
        "benchmark.events",
        b"{}",
        key="event-async",
        context={"phase": "measure", "phaseSecond": 0},
    )
    assert metadata["offset"] == ""

    adapter.flush()
    deliveries = adapter.drain_deliveries()

    assert len(deliveries) == 1
    assert deliveries[0]["eventId"] == "event-async"
    assert deliveries[0]["offset"] == "11"


def test_nats_consume_uses_replicated_consumer_config(monkeypatch) -> None:
    added_configs: list[dict] = []
    subscription_calls: list[tuple[str, str | None, dict]] = []

    class DummyMessage:
        def __init__(self):
            self.data = b"{}"
            self.metadata = SimpleNamespace(
                num_delivered=1,
                sequence=SimpleNamespace(stream=42),
            )

        async def ack(self):
            return None

    class DummySubscription:
        async def next_msg(self, timeout):
            del timeout
            return DummyMessage()

    class DummyJs:
        async def add_stream(self, config=None, **_kwargs):
            added_configs.append(config)
            return None

        async def update_stream(self, config=None, **_kwargs):
            added_configs.append(config)
            return None

        async def subscribe(self, subject, queue=None, **kwargs):
            subscription_calls.append((subject, queue, kwargs))
            return DummySubscription()

    class DummyNc:
        def __init__(self):
            self._js = DummyJs()

        def jetstream(self):
            return self._js

        async def close(self):
            return None

    async def fake_connect(**_kwargs):
        return DummyNc()

    fake_api = SimpleNamespace(
        StreamConfig=lambda **kwargs: kwargs,
        ConsumerConfig=lambda **kwargs: kwargs,
        StorageType=SimpleNamespace(FILE="file"),
        AckPolicy=SimpleNamespace(EXPLICIT="explicit"),
        DeliverPolicy=SimpleNamespace(NEW="new"),
    )

    monkeypatch.setattr(agent, "nats", SimpleNamespace(connect=fake_connect))
    monkeypatch.setattr(agent, "nats_js_api", fake_api)

    result = agent.NatsJetStreamAdapter.consume(
        broker_url="nats://nats:4222",
        subject="benchmark.events",
        consumer_group="bench-consumers",
        handler=lambda _body, _transport: {
            "event_id": "e1",
            "sequence": 1,
            "latency_ms": 1.0,
        },
        message_limit=1,
        idle_exit_seconds=1,
        idle_exit_not_before_ns=None,
        connect_timeout_seconds=1,
        jetstream_replicas=3,
    )

    assert result.received == 1
    assert added_configs[0]["num_replicas"] == 3
    assert subscription_calls[0][1] == "bench-consumers"
    assert subscription_calls[0][2]["config"]["num_replicas"] == 3


def test_parse_scheduled_start_ns_preserves_nanoseconds_from_control_plane() -> None:
    nanoseconds = 1_774_175_670_208_925_418
    milliseconds = 1_774_175_670_208
    seconds = 1_774_175_670

    assert agent.parse_scheduled_start_ns(str(nanoseconds)) == nanoseconds
    assert agent.parse_scheduled_start_ns(str(milliseconds)) == milliseconds * 1_000_000
    assert agent.parse_scheduled_start_ns(str(seconds)) == seconds * 1_000_000_000


def test_consumer_idle_exit_waits_for_window_end(monkeypatch) -> None:
    scheduled_start_ns = 1_000_000_000
    idle_exit_not_before_ns = agent.consumer_idle_exit_not_before_ns(
        scheduled_start_ns,
        warmup_seconds=5,
        measurement_seconds=10,
        cooldown_seconds=5,
    )

    monotonic_now = {"value": 100.0}
    wall_now = {"value": scheduled_start_ns + (10 * 1_000_000_000)}

    monkeypatch.setattr(agent.time, "monotonic", lambda: monotonic_now["value"])
    monkeypatch.setattr(agent.time, "time_ns", lambda: wall_now["value"])

    assert not agent.should_exit_consumer_idle(
        idle_exit_not_before_ns=idle_exit_not_before_ns,
        idle_exit_seconds=5,
        last_activity=90.0,
    )

    wall_now["value"] = scheduled_start_ns + (21 * 1_000_000_000)
    monotonic_now["value"] = 106.0

    assert agent.should_exit_consumer_idle(
        idle_exit_not_before_ns=idle_exit_not_before_ns,
        idle_exit_seconds=5,
        last_activity=100.0,
    )


def test_consumer_stop_after_adds_grace_to_window_end() -> None:
    idle_exit_not_before_ns = 1_000_000_000

    assert agent.consumer_stop_after_ns(
        idle_exit_not_before_ns,
        idle_exit_seconds=7,
    ) == 8_000_000_000


def test_should_force_exit_consumer_when_grace_deadline_passes(monkeypatch) -> None:
    deadline = 8_000_000_000
    monkeypatch.setattr(agent.time, "time_ns", lambda: deadline - 1)
    assert not agent.should_force_exit_consumer(stop_after_ns=deadline)

    monkeypatch.setattr(agent.time, "time_ns", lambda: deadline)
    assert agent.should_force_exit_consumer(stop_after_ns=deadline)


def test_is_retryable_connect_error_matches_transient_connection_failures() -> None:
    assert agent.is_retryable_connect_error(RuntimeError("Connection refused to all addresses"))
    assert agent.is_retryable_connect_error(RuntimeError("connection timed out"))
    assert agent.is_retryable_connect_error(RuntimeError("NoBrokersAvailable"))
    assert agent.is_retryable_connect_error(RuntimeError("StreamLostError: connection workflow failed"))
    assert not agent.is_retryable_connect_error(RuntimeError("unauthorized access"))


def test_raw_message_bytes_handles_binary_body_variants() -> None:
    assert agent.raw_message_bytes(b"abc") == b"abc"
    assert agent.raw_message_bytes(bytearray(b"abc")) == b"abc"
    assert agent.raw_message_bytes(memoryview(b"abc")) == b"abc"
    assert agent.raw_message_bytes("abc") == b"abc"


def test_artemis_adapter_retries_transient_connect_failures(monkeypatch) -> None:
    calls: list[str] = []

    class DummyConnection:
        def close(self) -> None:
            return None

    def fake_connection(url: str, timeout: int) -> DummyConnection:
        calls.append(f"{url}|{timeout}")
        if len(calls) < 3:
            raise agent.ProtonConnectionException("Connection refused to all addresses")
        return DummyConnection()

    monkeypatch.setattr(agent, "ProtonBlockingConnection", fake_connection)
    monkeypatch.setattr(agent.time, "sleep", lambda _: None)
    monkeypatch.setattr(agent, "STOP_REQUESTED", False)

    adapter = agent.ArtemisAdapter("amqp://broker:5672", connect_timeout_seconds=5)
    assert isinstance(adapter.connection, DummyConnection)
    assert len(calls) == 3


def test_kafka_consumer_does_not_sync_commit_each_message(monkeypatch) -> None:
    class DummyRecord:
        def error(self):
            return None

        def value(self):
            return b'{"id":"e1","specversion":"1.0","source":"bench","type":"benchmark.message","time":"2026-03-22T00:00:00Z","datacontenttype":"application/json","benchmarkrunid":"run-1","scenarioid":"s1","brokerid":"kafka","produceragentid":"p1","receiverid":"c1","payloadsizebytes":3,"producerseq":1,"testmode":"benchmark","topologymode":"spsc","durabilitymode":"persistent","configmode":"optimized","producersendstartns":1000,"measurementphase":"measure","phaseelapsedms":0,"data":{"payload":"abc"}}'

        def partition(self):
            return 0

        def offset(self):
            return 1

    class DummyConsumer:
        def __init__(self):
            self.commit_calls = 0
            self.config = {}
            self.last_timeout = None

        def subscribe(self, _topics):
            return None

        def consume(self, num_messages, timeout):
            self.last_timeout = timeout
            return [DummyRecord()]

        def commit(self, **_kwargs):
            self.commit_calls += 1

        def close(self):
            return None

    dummy = DummyConsumer()
    monkeypatch.setattr(agent, "KafkaConsumer", lambda config: setattr(dummy, "config", config) or dummy)

    result = agent.KafkaAdapter.consume(
        bootstrap_servers="kafka:9092",
        topic="benchmark.events",
        group_id="bench",
        handler=lambda _body, _transport: {
            "event_id": "e1",
            "sequence": 1,
            "latency_ms": 1.0,
            "phase": "measure",
            "phase_elapsed_ms": 0,
            "measured": True,
            "transport": _transport,
        },
        message_limit=1,
        idle_exit_seconds=1,
        idle_exit_not_before_ns=None,
        fetch_min_bytes=1,
        fetch_max_wait_ms=1,
        max_poll_records=1,
        max_partition_fetch_bytes=1048576,
        fetch_max_bytes=52428800,
        session_timeout_ms=30000,
        heartbeat_interval_ms=3000,
        enable_auto_commit=False,
        max_poll_interval_ms=60000,
        socket_nagle_disable=True,
    )

    assert result.received == 1
    assert dummy.commit_calls == 0
    assert dummy.config["max.poll.interval.ms"] == 60000
    assert dummy.config["socket.nagle.disable"] is True
    assert dummy.last_timeout is not None and dummy.last_timeout < 1.0


def test_kafka_publish_does_not_flush_each_message(monkeypatch) -> None:
    class DummyMessage:
        def partition(self):
            return 2

        def offset(self):
            return 9

    class DummyProducer:
        def __init__(self, config):
            self.config = config
            self.flush_calls = 0
            self.callbacks = []

        def produce(self, destination, key, value, on_delivery):
            self.callbacks.append(on_delivery)

        def poll(self, _timeout):
            callbacks = list(self.callbacks)
            self.callbacks.clear()
            for callback in callbacks:
                callback(None, DummyMessage())

        def flush(self, _timeout):
            self.flush_calls += 1
            self.poll(0)

    monkeypatch.setattr(agent, "KafkaProducer", DummyProducer)
    adapter = agent.KafkaAdapter(
        "kafka:9092",
        "producer-1",
        acks="1",
        linger_ms=0,
        batch_size_bytes=16384,
        compression_type="none",
        socket_nagle_disable=True,
        max_in_flight_requests=5,
        request_timeout_ms=15000,
        buffer_memory_bytes=67108864,
        max_request_size_bytes=2097152,
        delivery_timeout_ms=45000,
    )

    metadata = adapter.publish("benchmark.events", b"{}", key="event-1", context={"x": 1})
    assert metadata["partition"] == ""
    assert adapter.producer.flush_calls == 0
    assert adapter.producer.config["delivery.timeout.ms"] == 45000
    assert adapter.producer.config["socket.nagle.disable"] is True

    deliveries = adapter.drain_deliveries()
    assert len(deliveries) == 1
    assert deliveries[0]["eventId"] == "event-1"
    assert deliveries[0]["partition"] == "2"
    assert deliveries[0]["offset"] == "9"


def test_rabbitmq_publish_declares_queue_once(monkeypatch) -> None:
    declared: list[str] = []
    published: list[dict[str, object]] = []

    class DummyChannel:
        def queue_declare(self, queue, durable):
            declared.append(f"{queue}:{durable}")

        def basic_publish(self, **kwargs):
            published.append(dict(kwargs))

    class DummyConnection:
        def __init__(self, _parameters):
            self._channel = DummyChannel()

        def channel(self):
            return self._channel

        def close(self):
            return None

    class DummyParams:
        def __init__(self, _broker_url):
            self.heartbeat = 0
            self.frame_max = 0
            self.channel_max = 0
            self.socket_timeout = 0.0

    monkeypatch.setattr(agent, "ensure_dependency", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(agent, "BlockingConnection", DummyConnection)
    monkeypatch.setattr(agent, "URLParameters", DummyParams)
    monkeypatch.setattr(agent, "BasicProperties", lambda **kwargs: kwargs)

    adapter = agent.RabbitMqAdapter(
        "amqp://guest:guest@rabbitmq:5672/%2F",
        publisher_confirms=False,
        heartbeat_sec=30,
        frame_max=131072,
        channel_max=2047,
        connection_timeout_ms=30000,
    )
    adapter.publish("benchmark.events", b"{}", key="event-1", mandatory=False)
    adapter.publish("benchmark.events", b"{}", key="event-2", mandatory=False)

    assert declared == ["benchmark.events:True"]
    assert len(published) == 2


def test_rabbitmq_consume_uses_streaming_consumer(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    acked: list[int] = []
    cancel_calls = 0

    class DummyMethod:
        routing_key = "benchmark.events"
        redelivered = False
        delivery_tag = 7

    class DummyChannel:
        def queue_declare(self, queue, durable):
            calls.append({"declare": queue, "durable": durable})

        def basic_qos(self, prefetch_count, global_qos):
            calls.append({"prefetch": prefetch_count, "global": global_qos})

        def consume(self, queue, auto_ack, inactivity_timeout):
            calls.append(
                {
                    "queue": queue,
                    "auto_ack": auto_ack,
                    "inactivity_timeout": inactivity_timeout,
                }
            )
            yield DummyMethod(), SimpleNamespace(headers={}), b"payload"

        def basic_ack(self, delivery_tag):
            acked.append(delivery_tag)

        def basic_nack(self, delivery_tag, requeue):
            raise AssertionError(f"unexpected nack {delivery_tag} {requeue}")

        def cancel(self):
            nonlocal cancel_calls
            cancel_calls += 1

        def close(self):
            return None

    class DummyConnection:
        def __init__(self, _parameters):
            self._channel = DummyChannel()

        def channel(self):
            return self._channel

        def close(self):
            return None

    monkeypatch.setattr(agent, "ensure_dependency", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(agent, "BlockingConnection", DummyConnection)
    monkeypatch.setattr(agent, "URLParameters", lambda _broker_url: object())

    result = agent.RabbitMqAdapter.consume(
        broker_url="amqp://guest:guest@rabbitmq:5672/%2F",
        queue_name="benchmark.events",
        prefetch=256,
        prefetch_global=False,
        auto_ack=False,
        handler=lambda _body, _transport: {
            "event_id": "event-1",
            "producer_id": "producer-1",
            "sequence": 1,
        },
        message_limit=1,
        idle_exit_seconds=5,
        idle_exit_not_before_ns=None,
    )

    assert result.received == 1
    assert acked == [7]
    assert cancel_calls == 1
    assert any(call.get("queue") == "benchmark.events" for call in calls if "queue" in call)


def test_rabbitmq_consumer_stops_at_deadline_even_with_busy_queue(monkeypatch) -> None:
    acked: list[int] = []
    cancel_calls = 0

    class DummyMethod:
        routing_key = "benchmark.events"
        redelivered = False
        delivery_tag = 7

    class DummyChannel:
        def queue_declare(self, queue, durable):
            return None

        def basic_qos(self, prefetch_count, global_qos):
            return None

        def consume(self, queue, auto_ack, inactivity_timeout):
            while True:
                yield DummyMethod(), SimpleNamespace(headers={}), b"payload"

        def basic_ack(self, delivery_tag):
            acked.append(delivery_tag)

        def basic_nack(self, delivery_tag, requeue):
            raise AssertionError(f"unexpected nack {delivery_tag} {requeue}")

        def cancel(self):
            nonlocal cancel_calls
            cancel_calls += 1

        def close(self):
            return None

    class DummyConnection:
        def __init__(self, _parameters):
            self._channel = DummyChannel()

        def channel(self):
            return self._channel

        def close(self):
            return None

    calls = {"count": 0}
    stop_after_ns = 6_000_000_000

    def fake_time_ns():
        calls["count"] += 1
        return stop_after_ns - 1 if calls["count"] == 1 else stop_after_ns

    monkeypatch.setattr(agent, "ensure_dependency", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(agent, "BlockingConnection", DummyConnection)
    monkeypatch.setattr(agent, "URLParameters", lambda _broker_url: object())
    monkeypatch.setattr(agent.time, "time_ns", fake_time_ns)

    result = agent.RabbitMqAdapter.consume(
        broker_url="amqp://guest:guest@rabbitmq:5672/%2F",
        queue_name="benchmark.events",
        prefetch=256,
        prefetch_global=False,
        auto_ack=False,
        handler=lambda _body, _transport: {
            "event_id": "event-1",
            "producer_id": "producer-1",
            "sequence": 1,
        },
        message_limit=0,
        idle_exit_seconds=5,
        idle_exit_not_before_ns=1_000_000_000,
    )

    assert result.received == 1
    assert acked == [7]
    assert cancel_calls == 1


def test_recent_id_window_stays_bounded() -> None:
    window = agent.RecentIdWindow(max_items=3)

    assert window.seen_before("a") is False
    assert window.seen_before("b") is False
    assert window.seen_before("c") is False
    assert window.seen_before("a") is True
    assert window.seen_before("d") is False
    assert window.seen_before("a") is False


def test_raw_record_chunk_buffer_samples_instead_of_retaining_full_history(monkeypatch, capsys) -> None:
    monkeypatch.setattr(agent.random, "randrange", lambda _count: 0)
    buffer = agent.RawRecordChunkBuffer(
        role="consumer",
        schema=["eventId", "latencyMs"],
        chunk_size=2,
        sample_limit=2,
    )

    buffer.add({"eventId": "e1", "latencyMs": 1.0})
    buffer.add({"eventId": "e2", "latencyMs": 2.0})
    buffer.add({"eventId": "e3", "latencyMs": 3.0})

    emitted = buffer.emit(run_id="run-1", broker="kafka", agent_id="consumer-1")
    output = capsys.readouterr().out

    assert emitted == 2
    assert '"sampled":true' in output
    assert '"observedCount":3' in output
