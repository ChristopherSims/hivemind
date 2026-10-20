import json

from test_task_execution import checkpoint, setup, start


def test_agents_include_live_task_checkpoint_and_all_run_context(world):
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(active, run), token)
    auxiliary = world.run(agent, token)
    roster = world.call("GET", f"agents?project_id={world.project}", token=token)
    entry = next(item for item in roster if item["id"] == agent["id"])
    assert entry["status"] == "running"
    assert entry["current_task"]["id"] == task["id"]
    assert entry["current_task"]["progress"] == 40
    assert entry["summary"] == "Export implementation in progress"
    assert entry["context"]["run_id"] == run["id"]
    assert {item["run_id"] for item in entry["context"]["runs"]} == {run["id"], auxiliary["id"]}
    assert entry["context"]["presence"] == "online"
    assert entry["display_name"] == agent["display_name"]


def test_roster_never_leaks_private_task_or_derived_run_summary(world):
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(active, run, summary="PRIVATE"), token)
    other = world.agent("Reader")
    reader = world.credential(other)
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (agent["id"], task["id"]))
        body = store.get(run["id"], {"role": "admin"})
        body["summary"] = {"text": "PRIVATE", "origin": "agent_reported"}
        store.db.execute("UPDATE resources SET body=? WHERE id=?", (json.dumps(body), run["id"]))
    roster = world.call("GET", f"agents?project_id={world.project}", token=reader)
    entry = next(item for item in roster if item["id"] == agent["id"])
    assert entry["current_task"] is None and entry["summary"] is None
    assert "PRIVATE" not in json.dumps(entry)
    assert task["id"] not in json.dumps(entry)


def test_completion_clears_current_task_and_no_run_agents_have_idle_context(world):
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint",
               checkpoint(active, run, status="completed", progress=100), token)
    idle = world.agent("No run")
    roster = world.call("GET", f"agents?project_id={world.project}", token=token)
    entries = {item["id"]: item for item in roster}
    assert entries[agent["id"]]["current_task"] is None
    assert entries[idle["id"]]["context"]["runs"] == []
    assert entries[idle["id"]]["status"] == "idle"


def test_v1_discovery_and_routes_replace_old_api_prefix(world):
    discovery = world.client.get("/.well-known/hivemind").json()
    assert discovery["api_base"] == "/v1"
    assert discovery["agent_tool_context"]["url"] == "/v1/tool-context"
    assert world.client.get("/v0.1/agents").status_code == 404
    assert not any(route.path.startswith("/v0.1/") for route in world.app.routes)
