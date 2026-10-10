import json
from types import SimpleNamespace

from hivemind.agent_worker import AgentWorker
from hivemind.api import hash_token
from hivemind.review_automation import ReviewPolicy
from test_agent_runtime_integration import connection


def test_verified_dependency_waits_for_review_and_current_checkpoint(world):
    producer, reviewer = world.agent("Producer"), world.agent("Reviewer")
    token, review_token = world.credential(producer), world.credential(reviewer)
    reviewer_id = world.app.state.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(review_token),)).fetchone()[0]
    run = world.run(producer, token)
    plan = world.call("POST", "task-plans", {"project_id": world.project, "title": "Gated",
        "tasks": [{"key": "one", "title": "Build"},
                  {"key": "two", "title": "Consume", "depends_on": ["one"],
                   "require_verified_dependencies": True}]})
    class Runtime:
        def execute(self, task, context, checkpoint):
            return {"status": "completed", "summary": "Local result"}
    client = connection(world, token)
    try:
        worker = AgentWorker(client, world.project, producer["id"], run["id"], Runtime(),
                             enabled=True, submit_verification=True)
        assert worker.tick()["status"] == "completed"
        assert worker.tick()["task_id"] is None
        assert not client.task_plan(plan["plan"]["id"])["tasks"][1]["dependencies_met"]
        world.app.state.trusted_verifier = SimpleNamespace(
            run=lambda name, paths: SimpleNamespace(as_record=lambda: {"name": name, "ok": True}))
        world.app.state.review_automation.policy = ReviewPolicy(
            enabled=True, reviewer_principal_ids=(reviewer_id,), checks=("unit",))
        world.call("POST", "review-automation:tick", {"project_id": world.project})
        assert client.task_plan(plan["plan"]["id"])["tasks"][1]["dependencies_met"]
        store = world.app.state.store
        with store.transaction():
            row = store.raw(plan["tasks"][0]["id"])
            body = json.loads(row["body"])
            body["execution"]["checkpoint"]["id"] = "checkpoint_changed"
            store.save("task", row["project"], row["owner"], body, expected=body["revision"])
        assert not client.task_plan(plan["plan"]["id"])["tasks"][1]["dependencies_met"]
        assert worker.tick()["task_id"] is None
    finally:
        client.close()
