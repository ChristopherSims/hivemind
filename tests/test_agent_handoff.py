import json
import time

from test_task_execution import checkpoint, setup, start


def test_handoff_retains_checkpoint_waits_for_claim_and_fences_old_execution(world):
    old, old_token, old_run, scope, task = setup(world)
    task = start(world, task, old_run, old_token)
    task = world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(task, old_run), old_token)
    receiver = world.agent("Receiver")
    token = world.credential(receiver)
    run = world.run(receiver, token)
    body = {"expected_revision": task["revision"], "target_agent_id": receiver["id"],
            "target_run_id": run["id"], "reason": "Transfer interrupted local work"}
    world.call("POST", f"tasks/{task['id']}:handoff", body, status=409)
    world.call("POST", f"tasks/{task['id']}:handoff", body, old_token, status=403)
    store = world.app.state.store
    with store.transaction():
        row = store.raw(old_run["id"])
        value = json.loads(row["body"])
        value["last_seen_at"] = time.time() - 60
        store.save("run", row["project"], row["owner"], value, expected=value["revision"])
    handoff = world.call("POST", f"tasks/{task['id']}:handoff", body)
    assert handoff["handoff"]["checkpoint"]["summary"] == "Export implementation in progress"
    assert handoff["handoff"]["progress"] == 40
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(task, old_run), old_token, status=409)
    dispatched = {"agent_id": receiver["id"], "run_id": run["id"]}
    denied = world.call("POST", f"projects/{world.project}/tasks:dispatch", dispatched, token, status=409)
    assert denied["task"] is None
    with store.transaction():
        rows = store.db.execute("SELECT * FROM resources WHERE kind='work_claim' AND deleted=0").fetchall()
        for row in rows:
            value = json.loads(row["body"])
            value["expires_at"] = time.time() - 1
            store.save("work_claim", row["project"], row["owner"], value, expected=value["revision"])
    accepted = world.call("POST", f"projects/{world.project}/tasks:dispatch", dispatched, token)
    assert accepted["claim"]["run_id"] == run["id"]
    assert accepted["task"]["execution"]["id"] != task["execution"]["id"]
    assert accepted["task"]["execution"]["progress"] == 40
    assert accepted["task"]["execution"]["checkpoint"]["id"] == task["execution"]["checkpoint"]["id"]
    assert accepted["task"]["handoff"]["state"] == "accepted"
