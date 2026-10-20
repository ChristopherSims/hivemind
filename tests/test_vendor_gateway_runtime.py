"""Gateway opt-ins and private runtime-state contracts; no vendor launches."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind import vendor_gateway, vendor_runtimes, vendor_sandbox


class DummyRuntime(vendor_runtimes.VendorRuntime):
    vendor = "oh-my-pi"
    tool_names = ["read"]


def runtime(tmp_path, **options):
    workspace, private = tmp_path / "workspace", tmp_path / "private"
    workspace.mkdir()
    private.mkdir()
    config = private / "client.json"
    config.write_text(json.dumps({"project_id": "project_1", "agent_id": "agent_1",
                                 "base_url": "http://127.0.0.1:8765", "token": "synthetic-only"}))
    return DummyRuntime({"workspace": str(workspace), "state_file": str(private / "state.json"),
                         "config": str(config), "enabled": True, "allow_model_calls": True,
                         **options})


@pytest.mark.parametrize("settings", [
    {"vendor_network_gateway": {"model_origins": ["https://model.example"]}},
    {"require_os_isolation": True, "vendor_network_gateway": {}},
    {"require_os_isolation": True, "vendor_network_gateway": {"model_origins": [], "extra": True}},
])
def test_ambiguous_gateway_configuration_refuses_before_private_state_write(tmp_path, settings):
    adapter = runtime(tmp_path, **settings)
    with pytest.raises(ValueError, match="strict OS isolation"):
        adapter._prepare({"id": "task_1"}, {})
    assert not adapter.path.exists()


def test_strict_prepare_keeps_hook_and_readiness_files_in_mounted_private_state(tmp_path, monkeypatch):
    monkeypatch.setattr(vendor_sandbox, "assess_vendor_sandbox",
                        lambda *args, **kwargs: SimpleNamespace(allowed=True))
    monkeypatch.setattr(vendor_gateway, "GatewayPolicy", lambda **kwargs: kwargs)
    created = []
    class Broker:
        def __init__(self, policy, parent):
            created.append(policy)
            self.socket_path = parent / "fake.sock"
    monkeypatch.setattr(vendor_gateway, "ManagedGateway", Broker)
    adapter = runtime(tmp_path, require_os_isolation=True,
                      vendor_network_gateway={"model_origins": ["https://model.example"]})
    context = {"project_id": "project_1", "agent_id": "agent_1",
               "run_id": "run_1", "execution_id": "execution_1"}
    adapter._prepare({"id": "task_1", "title": "Synthetic task"}, context)
    config = json.loads(adapter._config.read_text())
    assert Path(config["state_dir"]).is_relative_to(adapter.path.parent / "state-sessions")
    assert created[0]["hivemind_origin"] == "http://127.0.0.1:8765"
    with pytest.raises(vendor_runtimes.vendor_recovery.RecoveryRefused):
        adapter._prepare({"id": "task_1", "title": "Synthetic task"}, context)
    assert len(created) == 1  # Unknown prior turns cannot replace a broker or start another turn.
