import json
import time


def appearance(world, agent, token=None):
    people = world.call("GET", f"projects/{world.project}/agent-appearances", token=token)
    return next(person for person in people if person["id"] == agent["id"])


def test_roster_task_ownership_and_run_blockers(world):
    agent = world.agent()
    run = world.run(agent)
    scope = world.scope()
    task = world.call("POST", "tasks", {
        "project_id": world.project, "title": "Build the header", "scope_ids": [scope["id"]],
    })
    world.call("POST", "work-claims", {
        "project_id": world.project, "agent_id": agent["id"], "run_id": run["id"],
        "task_id": task["id"], "scope_ids": [scope["id"]], "intent": "Build",
    })
    store = world.app.state.store
    with store.transaction():
        row = store.raw(run["id"])
        store.save("run", world.project, row["owner"], {**run, "status": "waiting_input"})
    person = appearance(world, agent)
    assert person["current_tasks"] == [{
        "id": task["id"], "title": "Build the header", "status": "queued",
    }]
    assert person["owned_scopes"] == [{"id": scope["id"], "display_name": scope["display_name"]}]
    assert any(blocker["code"] == "run_waiting_input" for blocker in person["blockers"])


def test_roster_excludes_expired_reserved_and_non_owner_claims(world):
    agent = world.agent()
    run = world.run(agent)
    scope = world.scope()
    claim = world.claim(agent, run, scope)
    store = world.app.state.store
    for changes in ({"expires_at": time.time() - 1}, {"state": "reserved"},
                    {"mode": "collaborator"}, {"mode": "observer"}):
        with store.transaction():
            row = store.raw(claim["id"])
            store.save("work_claim", world.project, row["owner"], {**claim, **changes})
        assert appearance(world, agent)["owned_scopes"] == []


def test_roster_excludes_terminal_runs_and_hidden_references(world):
    agent = world.agent()
    run = world.run(agent)
    scope = world.scope()
    world.claim(agent, run, scope)
    human = world.credential(role="human")
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (agent["id"], scope["id"]))
    assert appearance(world, agent, human)["owned_scopes"] == []
    assert appearance(world, agent)["owned_scopes"]
    with store.transaction():
        row = store.raw(run["id"])
        store.save("run", world.project, row["owner"], {**run, "status": "completed"})
    person = appearance(world, agent)
    assert person["current_tasks"] == person["owned_scopes"] == person["blockers"] == []


def test_roster_tracks_execution_without_a_claim(world):
    agent = world.agent()
    run = world.run(agent)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Implement roster"})
    world.call("POST", f"tasks/{task['id']}:start", {
        "expected_revision": task["revision"], "run_id": run["id"],
    })
    assert appearance(world, agent)["current_tasks"] == [{
        "id": task["id"], "title": task["title"], "status": "running",
    }]


def test_roster_canonical_task_blockers_and_hidden_task(world):
    agent = world.agent()
    run = world.run(agent)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Private paused work"})
    world.call("POST", f"tasks/{task['id']}:start", {
        "expected_revision": task["revision"], "run_id": run["id"],
    })
    store = world.app.state.store
    with store.transaction():
        row = store.raw(task["id"])
        current = json.loads(row["body"])
        store.save("task", world.project, row["owner"], {**current, "status": "paused"})
    person = appearance(world, agent)
    assert any(blocker["code"] == "task_paused" for blocker in person["blockers"])
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (agent["id"], task["id"]))
    person = appearance(world, agent, world.credential(role="human"))
    assert person["current_tasks"] == person["blockers"] == []
