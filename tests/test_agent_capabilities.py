def test_agent_declares_task_capabilities_without_runtime_control_grants(world):
    agent = world.call("POST", "agents", {
        "project_id": world.project, "display_name": "Python worker",
        "capabilities": ["python", "testing", "csv-export"]})
    assert agent["capabilities"] == ["python", "testing", "csv-export"]
    assert agent["supported_actions"] == []
    assert not agent["scope_permit_enforcement"]


def test_agent_capabilities_are_bounded_and_do_not_allow_identity_overrides(world):
    base = {"project_id": world.project, "display_name": "Worker"}
    for values in ([""] , ["x" * 129], ["capability"] * 33):
        world.call("POST", "agents", {**base, "capabilities": values}, status=422)
    agent = world.agent()
    token = world.credential(agent)
    world.call("POST", "agents", {**base, "capabilities": ["admin"]}, token, status=403)
