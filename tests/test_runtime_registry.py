import pytest

from hivemind import cli
from hivemind.runtime_registry import RUNTIME_FACTORIES, create_runtime, descriptions, factory_path
from test_vendor_runtimes import options


def test_runtime_choices_are_explicit_and_self_described(tmp_path):
    assert set(RUNTIME_FACTORIES) == {"factory", "oh_my_pi", "a2a", "acp"}
    assert set(descriptions()) == set(RUNTIME_FACTORIES)
    assert factory_path("acp") == "hivemind.protocol_runtimes:acp_runtime"
    config = options(tmp_path)
    with pytest.raises(ValueError):
        create_runtime("model-supplied.module:execute", config)
    factory = create_runtime("factory", config)
    assert factory.enabled is False and factory.session is None
    factory.close()


def test_cli_named_runtime_does_not_implicitly_enable_execution(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr("sys.argv", ["hivemind", "worker", "--runtime", "a2a",
        "--config", str(tmp_path / "not-opened.json"), "--journal", str(tmp_path / "journal.sqlite"),
        "--workspace", str(tmp_path)])
    assert cli.main() == 1
    assert "disabled" in capsys.readouterr().err
    assert not (tmp_path / "journal.sqlite").exists()


def test_push_and_slot_flags_require_concurrent_messaging(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr("sys.argv", ["hivemind", "worker", "--runtime", "factory",
        "--enable", "--messaging", "--message-sender", "agent_x",
        "--message-push", "--message-slots", "2",
        "--config", str(tmp_path / "config.json"), "--journal", str(tmp_path / "journal.sqlite"),
        "--workspace", str(tmp_path)])
    assert cli.main() == 1
    assert "require --concurrent-messaging" in capsys.readouterr().err


def test_discovery_exposes_honest_protocol_and_coordination_status(world):
    response = world.client.get("/.well-known/hivemind")
    assert response.status_code == 200
    value = response.json()
    assert value["capability_status"]["runtime.acp"]["os_sandbox"] is False
    assert value["capability_status"]["runtime.a2a"]["pause_resume"] is False
    assert value["capability_status"]["coordination.policy"]["unknown_execution_action"] == "manual_hold"
    assert value["capability_status"]["coordination.policy"]["background_enabled"] is False
    context = world.call("GET", "tool-context")
    assert context["feature_descriptions"]["protocol_runtimes"]
    assert context["feature_descriptions"]["coordination_policy"]
