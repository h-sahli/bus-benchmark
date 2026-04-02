import os
import re
import socket
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest


playwright_sync = pytest.importorskip("playwright.sync_api")


REPO_ROOT = Path(__file__).resolve().parents[1]


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_health(base_url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/api/health", timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {base_url}/api/health")


@pytest.fixture()
def ui_server(tmp_path: Path):
    port = find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    database_path = tmp_path / "ui-smoke.db"
    app_module_path = tmp_path / "ui_smoke_app.py"
    app_module_path.write_text(
        textwrap.dedent(
            f"""
            import json
            from pathlib import Path

            from services.platform.app import create_app


            class FakeClusterAutomation:
                enabled = True
                operator_bootstrap_enabled = False
                agent_start_barrier_seconds = 1

                def status(self):
                    return {{
                        "enabled": True,
                        "ready": True,
                        "message": "ready",
                        "operators": {{
                            "kafka": {{"label": "Kafka", "ready": True, "message": "ready"}},
                            "rabbitmq": {{"label": "RabbitMQ", "ready": True, "message": "ready"}},
                            "artemis": {{"label": "Artemis", "ready": True, "message": "ready"}},
                            "nats": {{"label": "NATS JetStream", "ready": True, "message": "ready"}},
                        }},
                    }}

                def prepare_run_environment(self, *, emit, **_):
                    emit("prepare-started", "Preparing namespace.")
                    emit("namespace-ready", "Namespace ready.")
                    emit("broker-waiting", "Waiting for broker pods.")
                    emit("broker-ready", "Broker pods are ready.")
                    return True

                def run_benchmark_agents(self, *, emit, message_rate, scheduled_start_ns=None, **_):
                    emit("consumer-running", "Consumer jobs created.")
                    emit("producer-running", "Producer jobs created.")
                    producer = {{
                        "role": "producer",
                        "result": {{
                            "attempted": message_rate,
                            "sent": message_rate,
                            "publishErrors": 0,
                            "ackLatencyHistogramMs": [
                                {{"lowerMs": 0.0, "upperMs": 1.0, "count": 20}},
                                {{"lowerMs": 1.0, "upperMs": 2.0, "count": 10}},
                            ],
                            "timeseries": [
                                {{"second": 0, "attemptedMessages": max(1, message_rate // 2), "sentMessages": max(1, message_rate // 2)}},
                                {{"second": 1, "attemptedMessages": max(1, message_rate - (message_rate // 2)), "sentMessages": max(1, message_rate - (message_rate // 2))}},
                            ],
                        }},
                    }}
                    consumer = {{
                        "role": "consumer",
                        "result": {{
                            "received": max(1, message_rate - 5),
                            "receivedMeasured": max(1, message_rate - 5),
                            "parseErrors": 0,
                            "duplicates": 0,
                            "outOfOrder": 0,
                            "latencyHistogramMs": [
                                {{"lowerMs": 0.0, "upperMs": 2.0, "count": 30}},
                                {{"lowerMs": 2.0, "upperMs": 5.0, "count": 20}},
                            ],
                            "timeseries": [
                                {{"second": 0, "deliveredMessages": max(1, (message_rate - 5) // 2), "latencySamplesMs": [1.2, 1.6, 2.3]}},
                                {{"second": 1, "deliveredMessages": max(1, (message_rate - 5) - ((message_rate - 5) // 2)), "latencySamplesMs": [1.4, 1.8, 2.7]}},
                            ],
                        }},
                    }}
                    return {{
                        "producerLogs": [json.dumps(producer)],
                        "consumerLogs": [json.dumps(consumer)],
                        "nodeNames": ["cluster-node-1"],
                        "timingConfidence": "validated",
                        "scheduledStartNs": scheduled_start_ns or 123456789,
                    }}

                def delete_run_namespace(self, run_id):
                    return None

                def wait_for_namespace_deleted(self, run_id, timeout_seconds=180):
                    return True

                def delete_legacy_broker_namespaces(self):
                    return None


            app = create_app(Path(r"{database_path}"), cluster_automation=FakeClusterAutomation())
            """
        ),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = (
        f"{tmp_path}{os.pathsep}{REPO_ROOT}{os.pathsep}{environment['PYTHONPATH']}"
        if environment.get("PYTHONPATH")
        else f"{tmp_path}{os.pathsep}{REPO_ROOT}"
    )
    environment["BUS_DB_PATH"] = str(database_path)

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "ui_smoke_app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=tmp_path,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_health(base_url)
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def test_guided_ui_flow(ui_server: str) -> None:
    with playwright_sync.sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1200})
        expect = playwright_sync.expect

        page.goto(ui_server, wait_until="domcontentloaded")
        page.locator("header.app-header h1").wait_for()
        expect(page).to_have_title("Concerto Bus Benchmark")
        expect(page.locator("header.app-header h1")).to_have_text("Bus Benchmark")
        page.get_by_role("tab", name="Benchmark").wait_for()
        assert page.locator("#startBenchmarkButton").is_disabled()
        page.locator("#brokerSelect").wait_for()

        page.locator("#brokerSelect").select_option("rabbitmq")
        page.locator("#configModeSelect").select_option("latency")
        page.locator("#deploymentModeSelect").select_option("ha")
        page.locator("#protocolSelect").select_option("amqp-0-9-1")
        page.locator("#config-broker-handshakeTimeoutMs").fill("6000")
        page.get_by_role("button", name="Next").click()

        page.locator("#scenarioSelect").select_option(index=1)
        expect(page.locator("#scenarioSummaryHint")).to_contain_text("Interactive latency")
        page.locator("#scheduleModeSelect").select_option("sequential")
        page.locator("#messageRateInput").fill("4000")
        page.locator("#rateProfileSelect").select_option("burst")
        expect(page.locator("#loadShapeSummaryHint")).to_contain_text("Burst")
        page.locator("#messageSizeBytesInput").fill("2048")
        page.locator("#messageRangeStartInput").fill("10")
        page.locator("#messageRangeEndInput").fill("25")
        page.locator("#payloadLimitHandlingSelect").select_option("strict")
        page.locator("#payloadGenerationModeSelect").select_option("reuse-template")
        page.locator("#producersInput").fill("2")
        page.locator("#consumersInput").fill("2")
        expect(page.locator("#scalingSummaryValue")).to_contain_text("2 producers + 2 consumers = 4 workers")
        expect(page.locator("#scalingSummaryHint")).to_contain_text("route")
        page.locator("#warmupSecondsInput").fill("1")
        page.locator("#measurementSecondsInput").fill("3")
        page.locator("#cooldownSecondsInput").fill("1")
        expect(page.locator("#windowSummaryHint")).to_contain_text("Measure 3s")
        page.get_by_role("button", name="Next").click()

        page.locator("#runNameInput").fill("ui smoke 1")
        assert not page.locator("#startBenchmarkButton").is_disabled()
        page.get_by_role("button", name="Start").click()

        page.get_by_role("tab", name="Runs").click()
        first_run_card = page.locator("#runList .run-card").filter(has_text="ui smoke 1").first
        first_run_card.wait_for()
        first_run_card.locator(".progress-row").filter(has_text="Progress").wait_for()
        first_run_card.locator(".stage-row").wait_for()
        first_run_card.get_by_text("COMPLETED", exact=True).wait_for()
        expect(first_run_card.get_by_text("Result", exact=True)).to_have_count(0)
        expect(first_run_card).not_to_contain_text("Delivered")
        first_run_card.get_by_role("button", name="Reuse", exact=True).click()
        page.get_by_role("tab", name="Benchmark").wait_for()
        expect(page.locator("#runNameInput")).to_have_value(re.compile(r"^ui smoke 1-\d{8}T\d{6}Z$"))
        expect(page.locator("#messageRateInput")).to_have_value("4000")
        expect(page.locator("#messageSizeBytesInput")).to_have_value("2048")
        expect(page.locator("#messageRangeStartInput")).to_have_value("10")
        expect(page.locator("#messageRangeEndInput")).to_have_value("25")
        expect(page.locator("#payloadLimitHandlingSelect")).to_have_value("strict")
        expect(page.locator("#payloadGenerationModeSelect")).to_have_value("reuse-template")
        expect(page.locator("#scheduleModeSelect")).to_have_value("sequential")
        expect(page.locator("#producersInput")).to_have_value("2")
        expect(page.locator("#consumersInput")).to_have_value("2")
        expect(page.locator("#warmupSecondsInput")).to_have_value("1")
        expect(page.locator("#measurementSecondsInput")).to_have_value("3")
        expect(page.locator("#cooldownSecondsInput")).to_have_value("1")

        page.get_by_role("tab", name="Results").click()
        page.locator("[data-panel='results'] h2").wait_for()
        page.get_by_role("button", name="Refresh").wait_for()
        page.locator("#vizRunPicker .viz-run-option").first.wait_for()
        page.locator("#latencyChart").wait_for()
        expect(page.locator("#vizSummary")).to_contain_text("ui smoke 1")

        page.get_by_role("tab", name="Reports").click()
        page.locator("[data-panel='reports'] h2").wait_for()
        page.locator("#reportRunPicker .viz-run-option.selected").filter(has_text="ui smoke 1").first.wait_for()
        page.locator("#reportTitleInput").fill("UI Smoke Report")
        page.get_by_role("button", name="Refresh table").click()
        page.locator("#reportPreview").get_by_text("UI Smoke Report").wait_for()
        page.locator("#reportPreview .report-run-card").filter(has_text="ui smoke 1").first.wait_for()
        with page.expect_download() as download_info:
            page.get_by_role("button", name="Generate PDF").click()
            download = download_info.value
            assert download.suggested_filename.endswith(".pdf")

            page.get_by_role("tab", name="Runs").click()
            first_run_card.get_by_role("button", name="Clear", exact=True).click()
            first_run_card.wait_for(state="detached")

        browser.close()
