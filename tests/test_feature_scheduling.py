import pytest

from hivemind.feature_scheduling import CoordinationRunner, CoordinationSchedule
from hivemind.coordination_policy import PolicyUpdate, ResourceDeclaration
from hivemind.model_wiring import integration_options
from hivemind.storage import Problem, canonical


def test_configured_coordination_ticks_refetch_operator_authority(world):
    store = world.app.state.store
    owner = store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone()
    calls = []

    class Policies:
        def tick(self, principal, project, body):
            world.app.state.service.project_access(principal, project)
            if principal["role"] != "admin":
                raise Problem("administrator_required", 403)
            calls.append((principal["id"], project, body.limit))

    runner = CoordinationRunner(world.app.state.service, Policies(), CoordinationSchedule(
        enabled=True, operator_principal_id=owner["id"], project_ids=[world.project], task_limit=4))
    runner.scheduled_tick()
    assert calls == [(owner["id"], world.project, 4)]
    runner.scheduled_tick()
    assert len(calls) == 1
    with store.transaction():
        store.db.execute("DELETE FROM principals WHERE id=?", (owner["id"],))
    runner.last = None
    with pytest.raises(Problem, match="configured_operator_unavailable"):
        runner.scheduled_tick()
    assert len(calls) == 1


def test_coordination_background_is_explicit_bounded_and_configurable(tmp_path):
    for invalid in ({"enabled": "true"}, {"enabled": True},
                    {"interval_seconds": float("nan")}, {"task_limit": True}):
        with pytest.raises(ValueError):
            CoordinationSchedule(**invalid)
    config = tmp_path / "integration.json"
    config.write_text(canonical({"coordination_policy": {
        "policy": {"enabled": True}, "background": {"enabled": False}}}))
    result = integration_options(config)
    assert result["coordination_policy"]["policy"]["enabled"] is True
    assert result["coordination_policy"]["background"]["enabled"] is False


def test_policy_effect_opt_ins_and_resource_shapes_are_strict():
    for invalid in ({"enabled": "true"}, {"auto_reassign": 1},
                    {"require_coordinator_lease": "false"}, {"max_reassignments": True}):
        with pytest.raises(ValueError):
            PolicyUpdate(**invalid)
    for invalid in (None, 1, {"path:file": "write"}, "path:file"):
        with pytest.raises(ValueError):
            ResourceDeclaration(expected_task_revision=1, reads=invalid)
