from types import SimpleNamespace

from hivemind.agent_worker import AgentWorker
from hivemind.api import hash_token
from hivemind.review_automation import ReviewPolicy
from test_agent_runtime_integration import connection


def test_review_defaults_disabled_and_agents_cannot_operate_it(world):
    agent = world.agent()
    token = world.credential(agent)
    body = {"project_id": world.project}
    world.call("POST", "review-automation:tick", body, status=409)
    world.call("POST", "review-automation:tick", body, token, status=403)


def test_independent_review_correction_delivery_and_fresh_checks(world):
    producer, reviewer = world.agent("Producer"), world.agent("Reviewer")
    producer_token, reviewer_token = world.credential(producer), world.credential(reviewer)
    reviewer_id = world.app.state.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(reviewer_token),)).fetchone()[0]
    run = world.run(producer, producer_token)
    world.call("POST", "task-plans", {"project_id": world.project, "title": "Correction loop",
        "tasks": [{"key": "one", "title": "Local result"}]})
    passing, calls = [False], []
    def check(name, paths):
        calls.append(name)
        return SimpleNamespace(as_record=lambda: {"name": name, "ok": passing[0]})
    world.app.state.trusted_verifier = SimpleNamespace(run=check)
    world.app.state.review_automation.policy = ReviewPolicy(
        enabled=True, reviewer_principal_ids=(reviewer_id,), checks=("unit",))
    executions = []
    class Runtime:
        def execute(self, task, context, checkpoint):
            executions.append(task["description"])
            return {"status": "completed", "summary": "Local self-report"}
    client = connection(world, producer_token)
    try:
        worker = AgentWorker(client, world.project, producer["id"], run["id"], Runtime(),
                             enabled=True, submit_verification=True, process_corrections=True)
        assert worker.tick()["status"] == "completed"
        result = world.call("POST", "review-automation:tick", {"project_id": world.project})
        assert result["results"][0]["state"] == "needs_corrections"
        assert worker.tick()["status"] == "completed"
        assert "Independent correction requests" in executions[1]
        current = world.call("GET", f"verifications?project_id={world.project}")["verifications"][0]
        assert current["status"] == "pending" and current["checks"] == []
        assert current["corrections"]["open_ids"] == []
        passing[0] = True
        result = world.call("POST", "review-automation:tick", {"project_id": world.project})
        assert result["results"][0]["state"] == "verified" and calls == ["unit", "unit"]
        current = client.verification(current["id"])
        assert current["independently_verified"] and current["reviewer_agent_id"] == reviewer["id"]
    finally:
        client.close()


def test_producer_identity_is_never_selected_as_its_own_reviewer(world):
    producer = world.agent()
    token = world.credential(producer)
    identifier = world.app.state.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()[0]
    automation = world.app.state.review_automation
    automation.policy = ReviewPolicy(enabled=True, reviewer_principal_ids=(identifier,), checks=("unit",))
    assert automation._reviewers(world.project, producer["id"]) == []
