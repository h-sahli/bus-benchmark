import os

from services.platform import bootstrap


class FakeAutomation:
    def __init__(self, _repo_root):
        self.called = []

    def ensure_broker_operators(self):
        self.called.append("brokers")
        return {"ready": True}


def test_bootstrap_cli_enables_cluster_actions(monkeypatch) -> None:
    monkeypatch.delenv("BUS_ENABLE_CLUSTER_ACTIONS", raising=False)
    monkeypatch.delenv("BUS_ENABLE_OPERATOR_BOOTSTRAP", raising=False)
    monkeypatch.setattr(bootstrap, "ClusterAutomation", FakeAutomation)

    assert bootstrap.main(["--scope", "brokers"]) == 0
    assert os.environ["BUS_ENABLE_CLUSTER_ACTIONS"] == "true"
    assert os.environ["BUS_ENABLE_OPERATOR_BOOTSTRAP"] == "true"
