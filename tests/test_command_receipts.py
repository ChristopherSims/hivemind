from hivemind.storage import digest


def test_own_committed_receipt_proves_task_creation_not_model_effects(world):
    body = {"project_id": world.project, "title": "Canonical creation"}
    task = world.call("POST", "tasks", body, key="task-create-receipt")
    operation = world.app.state.store.db.execute(
        "SELECT operation FROM commands WHERE key=?", ("task-create-receipt",)).fetchone()[0]
    receipt = world.call("GET", f"commands/receipt?operation={operation}&key=task-create-receipt")
    assert receipt["state"] == "committed" and receipt["resource_id"] == task["id"]
    assert receipt["resource"]["id"] == task["id"]
    # Pydantic defaults are part of the actual durable command checksum.
    stored = world.app.state.store.db.execute(
        "SELECT hash FROM commands WHERE key=?", ("task-create-receipt",)).fetchone()[0]
    assert receipt["payload_sha256"] == stored
    assert receipt["response_sha256"] == digest(task)
    assert receipt["external_effects_proved"] is False
    agent = world.agent()
    token = world.credential(agent)
    world.call("GET", f"commands/receipt?operation={operation}&key=task-create-receipt",
               token=token, status=404)


def test_receipt_does_not_resurrect_deleted_or_hidden_cached_resource(world):
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Gone"},
                      key="task-deleted-receipt")
    operation = world.app.state.store.db.execute(
        "SELECT operation FROM commands WHERE key=?", ("task-deleted-receipt",)).fetchone()[0]
    with world.app.state.store.transaction():
        world.app.state.store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (task["id"],))
    world.call("GET", f"commands/receipt?operation={operation}&key=task-deleted-receipt", status=404)
