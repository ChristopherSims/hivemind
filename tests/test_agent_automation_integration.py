import json

import pytest

from hivemind.model_wiring import integration_options


def test_service_installs_disabled_nexus_automation(world):
    automation = world.app.state.nexus_git_automation
    assert automation.policy.enabled is False
    assert automation.scheduled_tick() is None
    assert world.call("GET", f"projects/{world.project}/git-automation") == {
        "enabled": False, "review_runtime_configured": False, "last_error": None,
        "isolation_mode": "disabled", "isolation_controls": {"isolation": "disabled"}}


def test_private_nexus_policy_configuration(tmp_path):
    config = tmp_path / "integrations.json"
    config.write_text(json.dumps({"nexus_git_automation": {"enabled": False}}))
    options = integration_options(config)
    assert options["agent_automation"]["nexus_policy"].enabled is False
    config.write_text(json.dumps({"nexus_git_automation": {"enabled": "false"}}))
    with pytest.raises(ValueError, match="boolean"):
        integration_options(config)


def test_required_identity_policy_cannot_claim_unverified_boundary(tmp_path):
    config = tmp_path / "integrations.json"
    config.write_text(json.dumps({"os_isolation": {
        "mode": "required", "separate_service_identity": True}}))
    with pytest.raises(PermissionError, match="attestation"):
        integration_options(config)
