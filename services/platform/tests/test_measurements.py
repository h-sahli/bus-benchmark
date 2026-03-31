from services.platform.measurements import (
    aggregate_agent_results,
    expand_measurement_record_chunks,
    expand_measurement_timeseries_chunks,
    histogram_from_values,
    parse_json_lines,
    summary_from_histogram,
)

try:
    from hdrh.histogram import HdrHistogram
except ImportError:  # pragma: no cover - exercised by runtime environment
    HdrHistogram = None


def test_parse_json_lines_ignores_noise() -> None:
    records = parse_json_lines(
        "\n".join(
            [
                "plain text",
                '{"role":"producer","result":{"sent":10}}',
                "{broken",
                '{"role":"consumer","result":{"receivedMeasured":9}}',
            ]
        )
    )
    assert len(records) == 2
    assert records[0]["role"] == "producer"
    assert records[1]["role"] == "consumer"


def test_summary_from_histogram_returns_percentiles() -> None:
    histogram = histogram_from_values([0.8, 1.1, 1.5, 2.4, 5.1, 7.0])
    summary = summary_from_histogram(histogram)
    assert summary["count"] == 6.0
    assert summary["p50"] >= 1.0
    assert summary["p99"] >= summary["p95"]


def test_aggregate_agent_results_builds_real_measurement_shape() -> None:
    producer_records = [
        {
            "role": "producer",
            "result": {
                "attempted": 100,
                "sent": 98,
                "publishErrors": 2,
                "ackLatencyHistogramMs": histogram_from_values([0.4, 0.6, 0.9, 1.2]),
                "timeseries": [
                    {"second": 0, "attemptedMessages": 50, "sentMessages": 49},
                    {"second": 1, "attemptedMessages": 50, "sentMessages": 49},
                ],
            },
        }
    ]
    consumer_records = [
        {
            "role": "consumer",
            "result": {
                "received": 98,
                "receivedMeasured": 96,
                "parseErrors": 1,
                "duplicates": 0,
                "outOfOrder": 0,
                "latencyHistogramMs": histogram_from_values([1.2, 1.4, 1.6, 2.5, 3.4, 5.0]),
                "timeseries": [
                    {
                        "second": 0,
                        "deliveredMessages": 48,
                        "latencySamplesMs": [1.2, 1.4, 1.6],
                    },
                    {
                        "second": 1,
                        "deliveredMessages": 48,
                        "latencySamplesMs": [2.5, 3.4, 5.0],
                    },
                ],
            },
        }
    ]

    metrics = aggregate_agent_results(
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="normal",
        message_rate=100,
        message_size_bytes=1024,
        producers=1,
        consumers=1,
        measurement_seconds=2,
        transport_options={"rateProfileKind": "ramp-up", "peakMessageRate": 140},
        producer_records=producer_records,
        consumer_records=consumer_records,
        timing_confidence="validated",
    )

    assert metrics["source"] == "benchmark-agent"
    assert metrics["measurement"]["primaryMetric"] == "producer_send_to_consumer_receive"
    assert metrics["summary"]["throughput"]["target"] == 100
    assert metrics["summary"]["throughput"]["achieved"] == 48
    assert metrics["summary"]["loadProfile"]["rateProfileKind"] == "ramp-up"
    assert metrics["summary"]["loadProfile"]["peakMessageRate"] == 140
    assert metrics["summary"]["endToEndLatencyMs"]["p50"] == 1.6
    assert metrics["summary"]["endToEndLatencyMs"]["p95"] == 3.4
    assert metrics["summary"]["endToEndLatencyMs"]["p99"] == 3.4
    assert metrics["histogramMs"] == histogram_from_values([1.2, 1.4, 1.6, 2.5, 3.4, 5.0])
    assert len(metrics["timeseries"]) == 2
    assert metrics["timeseries"][0]["latencyP95Ms"] == 1.4
    assert metrics["timeseries"][1]["latencyP99Ms"] == 3.4


def test_aggregate_agent_results_combines_all_scaled_producer_and_consumer_pods() -> None:
    producer_records = [
        {
            "role": "producer",
            "result": {
                "attempted": 60,
                "sent": 60,
                "publishErrors": 0,
                "ackLatencyHistogramMs": histogram_from_values([0.5, 0.7, 0.8]),
                "timeseries": [
                    {"second": 0, "attemptedMessages": 30, "sentMessages": 30},
                    {"second": 1, "attemptedMessages": 30, "sentMessages": 30},
                ],
            },
        },
        {
            "role": "producer",
            "result": {
                "attempted": 40,
                "sent": 40,
                "publishErrors": 0,
                "ackLatencyHistogramMs": histogram_from_values([0.9, 1.1]),
                "timeseries": [
                    {"second": 0, "attemptedMessages": 20, "sentMessages": 20},
                    {"second": 1, "attemptedMessages": 20, "sentMessages": 20},
                ],
            },
        },
    ]
    consumer_records = [
        {
            "role": "consumer",
            "result": {
                "received": 50,
                "receivedMeasured": 50,
                "parseErrors": 0,
                "duplicates": 1,
                "outOfOrder": 0,
                "latencyHistogramMs": histogram_from_values([1.0, 1.5, 2.0, 2.5]),
                "timeseries": [
                    {
                        "second": 0,
                        "deliveredMessages": 25,
                        "latencySamplesMs": [1.0, 1.5],
                    },
                    {
                        "second": 1,
                        "deliveredMessages": 25,
                        "latencySamplesMs": [2.0, 2.5],
                    },
                ],
            },
        },
        {
            "role": "consumer",
            "result": {
                "received": 50,
                "receivedMeasured": 50,
                "parseErrors": 0,
                "duplicates": 0,
                "outOfOrder": 1,
                "latencyHistogramMs": histogram_from_values([3.0, 3.5, 4.0, 4.5]),
                "timeseries": [
                    {
                        "second": 0,
                        "deliveredMessages": 25,
                        "latencySamplesMs": [3.0, 3.5],
                    },
                    {
                        "second": 1,
                        "deliveredMessages": 25,
                        "latencySamplesMs": [4.0, 4.5],
                    },
                ],
            },
        },
    ]

    metrics = aggregate_agent_results(
        broker_id="rabbitmq",
        config_mode="optimized",
        deployment_mode="ha",
        message_rate=100,
        message_size_bytes=2048,
        producers=2,
        consumers=2,
        measurement_seconds=2,
        transport_options={"rateProfileKind": "constant"},
        producer_records=producer_records,
        consumer_records=consumer_records,
        timing_confidence="validated",
    )

    assert metrics["summary"]["throughput"]["attempted"] == 50
    assert metrics["summary"]["throughput"]["achieved"] == 50
    assert metrics["summary"]["producerRate"] == 50
    assert metrics["summary"]["consumerRate"] == 50
    assert metrics["summary"]["duplicates"] == 1
    assert metrics["summary"]["outOfOrder"] == 1
    assert metrics["summary"]["scaling"] == {
        "producers": 2,
        "consumers": 2,
        "factor": 4,
    }
    assert metrics["summary"]["endToEndLatencyMs"]["count"] == 8.0
    assert metrics["summary"]["endToEndLatencyMs"]["p50"] == 2.5
    assert metrics["summary"]["endToEndLatencyMs"]["p95"] == 4.0
    assert metrics["summary"]["endToEndLatencyMs"]["p99"] == 4.0
    assert metrics["timeseries"] == [
        {
            "second": 0,
            "latencyMs": 3.0,
            "latencyP50Ms": 1.5,
            "latencyP95Ms": 3.0,
            "latencyP99Ms": 3.0,
            "throughput": 50,
            "producerRate": 50,
            "consumerRate": 50,
            "successRate": 100.0,
            "failureCount": 0,
        },
        {
            "second": 1,
            "latencyMs": 4.0,
            "latencyP50Ms": 2.5,
            "latencyP95Ms": 4.0,
            "latencyP99Ms": 4.0,
            "throughput": 50,
            "producerRate": 50,
            "consumerRate": 50,
            "successRate": 100.0,
            "failureCount": 0,
        },
    ]


def test_aggregate_agent_results_trims_startup_until_stabilized() -> None:
    metrics = aggregate_agent_results(
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="normal",
        message_rate=100,
        message_size_bytes=1024,
        producers=1,
        consumers=1,
        measurement_seconds=3,
        transport_options={"rateProfileKind": "constant"},
        producer_records=[
            {
                "role": "producer",
                "result": {
                    "attempted": 101,
                    "sent": 101,
                    "timeseries": [
                        {"second": 0, "attemptedMessages": 1, "sentMessages": 1},
                        {"second": 1, "attemptedMessages": 50, "sentMessages": 50},
                        {"second": 2, "attemptedMessages": 50, "sentMessages": 50},
                    ],
                },
            }
        ],
        consumer_records=[
            {
                "role": "consumer",
                "result": {
                    "receivedMeasured": 101,
                    "timeseries": [
                        {"second": 0, "deliveredMessages": 1, "latencySamplesMs": [12.0]},
                        {"second": 1, "deliveredMessages": 50, "latencySamplesMs": [2.0, 2.2]},
                        {"second": 2, "deliveredMessages": 50, "latencySamplesMs": [2.4, 2.6]},
                    ],
                },
            }
        ],
        timing_confidence="validated",
    )

    assert metrics["measurement"]["stabilizationTrimSeconds"] == 1
    assert metrics["measurement"]["effectiveWindowSeconds"] == 2
    assert metrics["summary"]["throughput"]["effectiveWindowSeconds"] == 2
    assert metrics["summary"]["throughput"]["attempted"] == 50
    assert metrics["summary"]["throughput"]["achieved"] == 50
    assert metrics["summary"]["endToEndLatencyMs"]["count"] == 4.0
    assert [point["second"] for point in metrics["timeseries"]] == [1, 2]


def test_expand_measurement_record_chunks_decodes_raw_records() -> None:
    import base64
    import gzip
    import json

    encoded = base64.b64encode(
        gzip.compress(
            json.dumps(
                {
                    "schema": ["eventId", "latencyMs"],
                    "rows": [["event-1", 1.2], ["event-2", 2.4]],
                },
                separators=(",", ":"),
            ).encode("utf-8")
        )
    ).decode("ascii")

    records = expand_measurement_record_chunks(
        [{"kind": "measurement-record-chunk", "role": "consumer", "payload": encoded}],
        "consumer",
    )

    assert records == [
        {"eventId": "event-1", "latencyMs": 1.2, "role": "consumer"},
        {"eventId": "event-2", "latencyMs": 2.4, "role": "consumer"},
    ]


def test_expand_measurement_timeseries_chunks_decodes_chunked_timeseries() -> None:
    import base64
    import gzip
    import json

    encoded = base64.b64encode(
        gzip.compress(
            json.dumps(
                {
                    "schema": ["second", "deliveredMessages", "latencyHdrUs"],
                    "rows": [[0, 48, [{"valueUs": 1200, "count": 24}]], [1, 50, [{"valueUs": 1500, "count": 25}]]],
                },
                separators=(",", ":"),
            ).encode("utf-8")
        )
    ).decode("ascii")

    records = expand_measurement_timeseries_chunks(
        [{"kind": "measurement-timeseries-chunk", "role": "consumer", "payload": encoded}],
        "consumer",
    )

    assert records == [
        {"second": 0, "deliveredMessages": 48, "latencyHdrUs": [{"valueUs": 1200, "count": 24}], "role": "consumer"},
        {"second": 1, "deliveredMessages": 50, "latencyHdrUs": [{"valueUs": 1500, "count": 25}], "role": "consumer"},
    ]


def test_expand_measurement_timeseries_chunks_decodes_compact_histogram_timeseries() -> None:
    import base64
    import gzip
    import json

    encoded = base64.b64encode(
        gzip.compress(
            json.dumps(
                {
                    "schema": ["second", "deliveredMessages", "latencyHistogramMs"],
                    "rows": [
                        [0, 48, [{"lowerMs": 1.0, "upperMs": 2.0, "count": 24}]],
                        [1, 50, [{"lowerMs": 2.0, "upperMs": 5.0, "count": 25}]],
                    ],
                },
                separators=(",", ":"),
            ).encode("utf-8")
        )
    ).decode("ascii")

    records = expand_measurement_timeseries_chunks(
        [{"kind": "measurement-timeseries-chunk", "role": "consumer", "payload": encoded}],
        "consumer",
    )

    assert records == [
        {"second": 0, "deliveredMessages": 48, "latencyHistogramMs": [{"lowerMs": 1.0, "upperMs": 2.0, "count": 24}], "role": "consumer"},
        {"second": 1, "deliveredMessages": 50, "latencyHistogramMs": [{"lowerMs": 2.0, "upperMs": 5.0, "count": 25}], "role": "consumer"},
    ]


def test_aggregate_agent_results_uses_raw_records_and_resource_samples() -> None:
    metrics = aggregate_agent_results(
        broker_id="artemis",
        config_mode="baseline",
        deployment_mode="normal",
        message_rate=20,
        message_size_bytes=512,
        producers=1,
        consumers=1,
        measurement_seconds=2,
        transport_options={"rateProfileKind": "constant"},
        producer_records=[{"role": "producer", "result": {"attempted": 2, "sent": 2, "timeseries": []}}],
        consumer_records=[{"role": "consumer", "result": {"receivedMeasured": 2, "timeseries": []}}],
        timing_confidence="validated",
        raw_producer_records=[
            {"ackLatencyMs": 0.8, "phaseSecond": 0},
            {"ackLatencyMs": 1.1, "phaseSecond": 1},
        ],
        raw_consumer_records=[
            {"latencyMs": 2.5, "phaseSecond": 0},
            {"latencyMs": 3.3, "phaseSecond": 1},
        ],
        resource_samples=[
            {
                "second": 0,
                "brokerCpuCores": 0.4,
                "brokerMemoryMB": 128.0,
                "brokerNetworkRxMBps": 1.5,
                "brokerNetworkTxMBps": 1.2,
                "brokerStorageUsedMB": 64.0,
                "producerCpuCores": 0.1,
                "producerMemoryMB": 32.0,
                "consumerCpuCores": 0.1,
                "consumerMemoryMB": 36.0,
            }
        ],
    )

    assert metrics["summary"]["endToEndLatencyMs"]["count"] == 2.0
    assert metrics["summary"]["endToEndLatencyMs"]["p99"] == 2.5
    assert metrics["summary"]["resourceUsage"]["latest"]["brokerCpuCores"] == 0.4
    assert metrics["summary"]["resourceUsage"]["peaks"]["brokerMemoryMB"] == 128.0
    assert metrics["summary"]["throughput"]["attempted"] == 1
    assert metrics["summary"]["throughput"]["achieved"] == 1
    assert metrics["timeseries"] == [
        {
            "second": 0,
            "latencyMs": 2.5,
            "latencyP50Ms": 2.5,
            "latencyP95Ms": 2.5,
            "latencyP99Ms": 2.5,
            "throughput": 1,
            "producerRate": 0,
            "consumerRate": 1,
            "successRate": 100.0,
            "failureCount": 0,
            "cpuCores": 0.4,
            "memoryMB": 128.0,
            "networkRxMBps": 1.5,
            "networkTxMBps": 1.2,
            "storageUsedMB": 64.0,
            "producerCpuCores": 0.1,
            "producerMemoryMB": 32.0,
            "consumerCpuCores": 0.1,
            "consumerMemoryMB": 36.0,
        },
        {
            "second": 1,
            "latencyMs": 3.3,
            "latencyP50Ms": 3.3,
            "latencyP95Ms": 3.3,
            "latencyP99Ms": 3.3,
            "throughput": 1,
            "producerRate": 0,
            "consumerRate": 1,
            "successRate": 100.0,
            "failureCount": 0,
        },
    ]
    assert metrics["timeseries"][0]["cpuCores"] == 0.4
    assert metrics["timeseries"][0]["producerCpuCores"] == 0.1


def test_aggregate_agent_results_prefers_hdr_snapshots_for_long_runs() -> None:
    if HdrHistogram is None:
        return

    def snapshot(*entries: tuple[float, int] | float) -> list[dict[str, int]]:
        histogram = HdrHistogram(1, 60_000_000, 3)
        for entry in entries:
            if isinstance(entry, tuple):
                value_ms, count = entry
            else:
                value_ms, count = entry, 1
            histogram.record_value(int(round(value_ms * 1000.0)), count)
        return [
            {
                "valueUs": int(item.value_iterated_to),
                "count": int(item.count_added_in_this_iter_step),
            }
            for item in histogram.get_recorded_iterator()
        ]

    metrics = aggregate_agent_results(
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="ha",
        message_rate=200,
        message_size_bytes=1024,
        producers=1,
        consumers=1,
        measurement_seconds=2,
        transport_options={"rateProfileKind": "constant"},
        producer_records=[
            {
                "role": "producer",
                "result": {
                    "attempted": 100,
                    "sent": 100,
                    "ackLatencyHdrUs": snapshot(0.4, 0.7, 1.1),
                    "timeseries": [
                        {"second": 0, "attemptedMessages": 50, "sentMessages": 50, "ackLatencyHdrUs": snapshot(0.4, 0.7)},
                        {"second": 1, "attemptedMessages": 50, "sentMessages": 50, "ackLatencyHdrUs": snapshot(1.1)},
                    ],
                },
            }
        ],
        consumer_records=[
            {
                "role": "consumer",
                "result": {
                    "received": 100,
                    "receivedMeasured": 100,
                    "latencyHdrUs": snapshot((1.2, 25), (1.8, 25), (2.7, 25), (3.1, 25)),
                    "timeseries": [
                        {"second": 0, "deliveredMessages": 50, "latencyHdrUs": snapshot((1.2, 25), (1.8, 25))},
                        {"second": 1, "deliveredMessages": 50, "latencyHdrUs": snapshot((2.7, 25), (3.1, 25))},
                    ],
                },
            }
        ],
        timing_confidence="validated",
    )

    assert metrics["summary"]["endToEndLatencyMs"]["count"] == 100.0
    assert metrics["summary"]["endToEndLatencyMs"]["p95"] >= 2.7
    assert metrics["summary"]["producerAckLatencyMs"]["count"] == 3.0
    assert metrics["summary"]["throughput"]["achieved"] == 50
    assert metrics["timeseries"][0]["latencyP95Ms"] >= 1.2
    assert metrics["timeseries"][1]["latencyP99Ms"] >= 2.7


def test_aggregate_agent_results_accepts_chunked_timeseries_records() -> None:
    metrics = aggregate_agent_results(
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="ha",
        message_rate=100,
        message_size_bytes=1024,
        producers=1,
        consumers=1,
        measurement_seconds=2,
        transport_options={"rateProfileKind": "constant"},
        producer_records=[
            {
                "role": "producer",
                "result": {
                    "attempted": 100,
                    "sent": 100,
                    "publishErrors": 0,
                    "ackLatencyHistogramMs": histogram_from_values([0.4, 0.6, 0.9, 1.2]),
                },
            }
        ],
        consumer_records=[
            {
                "role": "consumer",
                "result": {
                    "received": 96,
                    "receivedMeasured": 96,
                    "parseErrors": 0,
                    "duplicates": 0,
                    "outOfOrder": 0,
                    "latencyHistogramMs": histogram_from_values([1.2, 1.4, 1.6, 2.5, 3.4, 5.0]),
                },
            }
        ],
        timing_confidence="validated",
        producer_timeseries_records=iter(
            [
                {"second": 0, "attemptedMessages": 50, "sentMessages": 49},
                {"second": 1, "attemptedMessages": 50, "sentMessages": 49},
            ]
        ),
        consumer_timeseries_records=iter(
            [
                {"second": 0, "deliveredMessages": 48, "latencyHdrUs": [{"valueUs": 1200, "count": 16}, {"valueUs": 1400, "count": 16}, {"valueUs": 1600, "count": 16}]},
                {"second": 1, "deliveredMessages": 48, "latencyHdrUs": [{"valueUs": 2500, "count": 16}, {"valueUs": 3400, "count": 16}, {"valueUs": 5000, "count": 16}]},
            ]
        ),
    )

    assert metrics["summary"]["throughput"]["attempted"] == 50
    assert metrics["summary"]["throughput"]["achieved"] == 48
    assert metrics["timeseries"][0]["throughput"] == 48
    assert metrics["timeseries"][1]["latencyP99Ms"] >= 5.0


def test_aggregate_agent_results_accepts_compact_histogram_timeseries_records() -> None:
    metrics = aggregate_agent_results(
        broker_id="kafka",
        config_mode="optimized",
        deployment_mode="ha",
        message_rate=100,
        message_size_bytes=1024,
        producers=1,
        consumers=1,
        measurement_seconds=2,
        transport_options={"rateProfileKind": "constant"},
        producer_records=[],
        consumer_records=[],
        timing_confidence="validated",
        producer_timeseries_records=iter(
            [
                {
                    "second": 0,
                    "attemptedMessages": 50,
                    "sentMessages": 49,
                    "ackLatencyHistogramMs": [{"lowerMs": 0.25, "upperMs": 0.5, "count": 20}],
                },
                {
                    "second": 1,
                    "attemptedMessages": 50,
                    "sentMessages": 49,
                    "ackLatencyHistogramMs": [{"lowerMs": 0.5, "upperMs": 1.0, "count": 20}],
                },
            ]
        ),
        consumer_timeseries_records=iter(
            [
                {
                    "second": 0,
                    "deliveredMessages": 48,
                    "latencyHistogramMs": [{"lowerMs": 1.0, "upperMs": 2.0, "count": 48}],
                },
                {
                    "second": 1,
                    "deliveredMessages": 48,
                    "latencyHistogramMs": [{"lowerMs": 2.0, "upperMs": 5.0, "count": 48}],
                },
            ]
        ),
    )

    assert metrics["summary"]["throughput"]["attempted"] == 50
    assert metrics["summary"]["throughput"]["achieved"] == 48
    assert metrics["summary"]["endToEndLatencyMs"]["count"] == 96.0
    assert metrics["timeseries"][0]["latencyP95Ms"] == 2.0
    assert metrics["timeseries"][1]["latencyP99Ms"] == 5.0
