from reportlab.platypus import Paragraph

from services.platform.reports import (
    _artifact_definition_rows,
    _calculation_rule_rows,
    _metric_rows,
    _methodology_run_rows,
    _overview_table,
    _summary_table_definition_rows,
    _sparse_category_labels,
    run_duration_label,
    resource_config_label,
)


def _sample_run(*, measured: bool = True) -> dict:
    summary = {
        "throughput": {"achieved": 1200, "achievedBytesPerSec": 1228800},
        "deliverySuccessRate": 99.9,
        "loadProfile": {"rateProfileKind": "constant", "baseMessageRate": 1200},
        "endToEndLatencyMs": {"count": 32.0, "p50": 1.2, "p95": 2.6, "p99": 3.8},
    }
    if not measured:
        summary["throughput"] = {}
        summary["deliverySuccessRate"] = 0.0
        summary["endToEndLatencyMs"] = {"count": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    return {
        "name": "k-long-name-01",
        "brokerId": "kafka",
        "configMode": "optimized",
        "deploymentMode": "ha",
        "producers": 10,
        "consumers": 10,
        "warmupSeconds": 10,
        "measurementSeconds": 240,
        "cooldownSeconds": 10,
        "messageRate": 1200,
        "messageSizeBytes": 1024,
        "transportOptions": {"rateProfileKind": "constant"},
        "metrics": {
            "summary": summary,
            "timeseries": [],
            "measurement": {
                "effectiveWindowSeconds": 240,
                "stabilizationTrimSeconds": 3,
                "rawRecordStore": {
                    "producerRecordCount": 20000,
                    "consumerRecordCount": 20000,
                    "resourceSampleCount": 128,
                },
            },
        },
        "resourceConfig": {
            "cpuRequest": "2",
            "cpuLimit": "4",
            "memoryRequest": "4Gi",
            "memoryLimit": "4Gi",
            "storageSize": "32Gi",
            "storageClassName": "local-path",
            "replicas": 3,
        },
    }


def test_metric_rows_hide_unmeasured_summary_values() -> None:
    rows = _metric_rows([_sample_run(measured=False)])
    assert rows[1][7:13] == ["-", "-", "-", "-", "-", "-"]


def test_overview_table_fits_landscape_width() -> None:
    table = _overview_table([_sample_run()])
    assert sum(table._argW) <= 720
    assert isinstance(table._cellvalues[0][0], Paragraph)


def test_resource_config_label_is_compact() -> None:
    label = resource_config_label(_sample_run())
    assert label == "2/4 CPU, 4Gi, x3, 32Gi"


def test_run_duration_label_shows_total_and_effective_window() -> None:
    label = run_duration_label(_sample_run())
    assert label == "4m 20s total, 4m effective"


def test_sparse_category_labels_reduce_dense_second_axis() -> None:
    labels = [str(index) for index in range(180)]
    sparse = _sparse_category_labels(labels, max_visible=10)
    assert sparse[0] == "0"
    assert sparse[-1] == "179"
    assert sum(1 for label in sparse if label) <= 11


def test_calculation_rule_rows_document_stabilization_and_success_formula() -> None:
    rows = _calculation_rule_rows()
    assert any("70% of their non-zero medians" in row[1] for row in rows)
    assert any("deliveredTotal / attemptedTotal" in row[1] for row in rows)


def test_summary_table_definition_rows_explain_report_columns() -> None:
    rows = _summary_table_definition_rows()
    assert any(row[0] == "Delivered (msg/s)" and "after stabilization trim" in row[1] for row in rows)
    assert any(row[0] == "Duration" and "effective measured window" in row[1] for row in rows)


def test_artifact_definition_rows_explain_sampled_records() -> None:
    rows = _artifact_definition_rows()
    assert any(row[0] == "producer-raw-records" and "20,000 rows" in row[1] for row in rows)
    assert any(row[0] == "report PDF" and "rebuild it from the stored report metadata" in row[1] for row in rows)


def test_methodology_run_rows_include_effective_window_trim_and_artifacts() -> None:
    rows = _methodology_run_rows([_sample_run()])
    assert rows[1][1] == "4m 20s"
    assert rows[1][2] == "4m"
    assert rows[1][3] == "3s"
    assert "20,000 producer rows" in rows[1][7]
