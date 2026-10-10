import time

from hivemind.storage import uid


def setup(world, claim=True):
    agent = world.agent("Task worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope("tasks/export", paths=["export"])
    task = world.call("POST", "tasks", {
        "project_id": world.project, "title": "Implement export", "scope_ids": [scope["id"]]})
    if claim:
        world.claim(agent, run, scope, token)
    return agent, token, run, scope, task


def start(world, task, run, token, recover=False, **kwargs):
    return world.call("POST", f"tasks/{task['id']}:start", {
        "expected_revision": task["revision"], "run_id": run["id"], "recover": recover}, token, **kwargs)


def checkpoint(task, run, **overrides):
    return {"expected_revision": task["revision"], "run_id": run["id"],
            "execution_id": task["execution"]["id"], "checkpoint_id": uid("checkpoint"),
            "status": "running", "progress": 40, "summary": "Export implementation in progress", **overrides}


def test_start_progress_completion_and_idempotent_checkpoints(world):
    _, token, run, _, task = setup(world)
    active = start(world, task, run, token, key="task-start")
    assert start(world, task, run, token, key="task-start") == active
    payload = checkpoint(active, run)
    progress = world.call("POST", f"tasks/{task['id']}:checkpoint", payload, token, key="checkpoint")
    assert world.call("POST", f"tasks/{task['id']}:checkpoint", payload, token, key="checkpoint") == progress
    payload["expected_revision"] = progress["revision"]
    assert world.call("POST", f"tasks/{task['id']}:checkpoint", payload, token)["revision"] == progress["revision"]
    completed = world.call("POST", f"tasks/{task['id']}:checkpoint",
                           checkpoint(progress, run, progress=100, status="completed"), token)
    assert completed["status"] == "completed"
    start(world, completed, run, token, status=409)
    assert world.call("GET", f"runs/{run['id']}", token=token)["task_id"] is None


def test_owned_scopes_actor_and_progress_fences(world):
    _, token, run, _, task = setup(world, claim=False)
    start(world, task, run, token, status=403)
    other = world.credential(world.agent("Other worker"))
    start(world, task, run, other, status=403)
    human = world.credential(role="human")
    start(world, task, run, human, status=403)


def test_recovery_waits_for_old_run_and_fences_old_execution(world, monkeypatch):
    agent, token, run, scope, task = setup(world)
    active = start(world, task, run, token, key="old-start")
    progress = world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(active, run), token)
    replacement = world.run(agent, token)
    start(world, progress, replacement, token, recover=True, status=409)
    clock = time.time() + 121
    monkeypatch.setattr(time, "time", lambda: clock)
    world.claim(agent, replacement, scope, token)
    recovered = start(world, progress, replacement, token, recover=True)
    assert recovered["execution"]["id"] != active["execution"]["id"]
    assert recovered["execution"]["checkpoint"]["id"] == progress["execution"]["checkpoint"]["id"]
    assert recovered["execution"]["progress"] == 40
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(progress, run), token, status=409)
    start(world, task, run, token, key="old-start", status=409)


def test_checkpoint_survives_fresh_connection_and_revoked_evidence_is_hidden(world):
    from hivemind.service import Service
    from hivemind.storage import Store
    _, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    proof = world.post("Checkpoint evidence")
    payload = checkpoint(active, run, evidence_refs=[{"kind": "board_post", "id": proof["id"]}])
    progress = world.call("POST", f"tasks/{task['id']}:checkpoint", payload, token, key="proof-checkpoint")
    memory = world.call("POST", "memories", {
        "project_id": world.project, "title": "Task evidence", "scope_owner_id": world.project,
        "content": {"media_type": "text/plain", "text": "A task checkpoint supports this report"},
        "evidence_refs": [{"kind": "task", "id": task["id"], "revision": progress["revision"]}]})
    world.call("POST", f"memories/{memory['id']}:verify", {"expected_revision": 1})
    restored = Store(world.app.state.store.directory)
    try:
        principal = dict(restored.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        assert restored.get(task["id"], principal)["execution"]["checkpoint"]["id"] == payload["checkpoint_id"]
        assert Service(restored).store.get(task["id"], principal)["execution"]["progress"] == 40
    finally:
        restored.close()
    world.call("DELETE", f"posts/{proof['id']}", {"expected_revision": 1})
    detail = world.call("GET", f"tasks/{task['id']}", token=token)
    assert detail["execution"]["checkpoint"]["evidence_unavailable"]
    assert "summary" not in detail["execution"]["checkpoint"]
    assert not world.call("GET", f"memories?project_id={world.project}")
    cached = world.call("POST", f"tasks/{task['id']}:checkpoint", payload, token, key="proof-checkpoint")
    assert cached["execution"]["checkpoint"]["evidence_unavailable"]
    assert progress["status"] == "running"


def test_progress_regression_and_expired_claims_cannot_commit(world, monkeypatch):
    _, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    progress = world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(active, run), token)
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(progress, run, progress=20), token, status=422)
    world.call("POST", f"tasks/{task['id']}:checkpoint",
               checkpoint(progress, run, status="completed", progress=50), token, status=422)
    clock = time.time() + 121
    monkeypatch.setattr(time, "time", lambda: clock)
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(progress, run), token, status=403)


def test_concurrent_starts_and_checkpoint_identity_are_fenced(world):
    from concurrent.futures import ThreadPoolExecutor
    _, token, run, _, task = setup(world)
    def take(_):
        response = world.client.post(f"/v0.1/tasks/{task['id']}:start", json={
            "expected_revision": 1, "run_id": run["id"]}, headers={
                "Authorization": f"Bearer {token}", "Idempotency-Key": uid("start")})
        return response.status_code
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(take, [0, 1])) == [200, 412]
    active = world.call("GET", f"tasks/{task['id']}", token=token)
    body = checkpoint(active, run)
    progress = world.call("POST", f"tasks/{task['id']}:checkpoint", body, token)
    body.update(expected_revision=progress["revision"], summary="Different contents under same checkpoint")
    world.call("POST", f"tasks/{task['id']}:checkpoint", body, token, status=409)


def test_steering_assignment_change_prevents_old_task_updates(world):
    _, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    store = world.app.state.store
    with store.transaction():
        principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        body = store.get(run["id"], principal)
        body["task_id"] = "replacement"
        store.save("run", world.project, principal["id"], body)
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(active, run), token, status=409)
