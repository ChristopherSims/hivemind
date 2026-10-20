"""Plan revision must retain completed producer contracts as dependencies."""
import json

from fastapi.testclient import TestClient

from hivemind.api import create_app
from hivemind.artifact_contracts import ContractPolicy
from hivemind.cli import initialize
from hivemind.goal_orchestration import GoalRunner, GoalSchedule
from hivemind.storage import uid


def test_automatic_revision_preserves_external_artifact_dependency(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False, capability_options={
        "artifact_contracts": {"policy": ContractPolicy(enabled=True)}})
    with TestClient(app) as http:
        def call(method, path, body=None, status=200):
            response = http.request(method, "/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + config["token"], "Idempotency-Key": uid("key")})
            assert response.status_code == status, response.text
            return response.json().get("data", response.json())

        project = config["project_id"]
        agent = call("POST", "agents", {"project_id": project, "display_name": "Plan worker"})
        run = call("POST", "runs", {"project_id": project, "agent_id": agent["id"]})
        goal = call("POST", "goals", {
            "project_id": project, "title": "Dependency preserving goal", "statement": "Produce then consume",
            "steps": [{"key": "producer", "title": "Produce"},
                      {"key": "consumer", "title": "Consume", "depends_on": ["producer"], "max_attempts": 1}]})
        goal_id = goal["id"]
        tasks = goal["goal"]["tasks"]

        def outcome(task_id, outcome):
            call("POST", f"goals/{goal_id}:tick", {"limit": 1})
            task = call("GET", f"tasks/{task_id}")
            task = call("POST", f"tasks/{task_id}:checkpoint", {
                "run_id": run["id"], "execution_id": task["execution"]["id"],
                "expected_revision": task["revision"], "checkpoint_id": uid("checkpoint"),
                "status": "running", "progress": 100 if outcome == "succeeded" else 0,
                "summary": "Recorded attempt result"})
            return call("POST", f"tasks/{task_id}:dispatch-outcome", {
                "expected_revision": task["revision"], "agent_id": agent["id"],
                "run_id": run["id"], "execution_id": task["execution"]["id"],
                "outcome": outcome, "summary": "Reported terminal outcome"})

        producer = outcome(tasks[0]["id"], "succeeded")
        failed = outcome(tasks[1]["id"], "failed")
        assert failed["status"] == "failed"
        call("POST", "artifact-contracts", {
            "project_id": project, "task_id": producer["id"], "title": "Required deliverable",
            "outputs": [{"name": "result", "type": "json", "schema": {"type": "object"}}]})
        principal = app.state.store.db.execute(
            "SELECT id FROM principals WHERE role='admin'").fetchone()["id"]
        runner = GoalRunner(app.state.service, app.state.goal_orchestration, GoalSchedule(
            enabled=True, operator_principal_id=principal, project_ids=[project],
            revise_failed=True, max_plan_revisions=2))
        runner.scheduled_tick()
        revised = call("GET", f"goals/{goal_id}")
        assert revised["goal"]["plan_revision"] == 2
        replacement = revised["tasks"][0]
        assert replacement["depends_on"] == [producer["id"]]
        assert revised["ready"] == []
        assert call("POST", f"projects/{project}/tasks:dispatch", {
            "agent_id": agent["id"], "run_id": run["id"]})["task"] is None
        direct = call("POST", f"tasks/{replacement['id']}:start", {
            "expected_revision": replacement["revision"], "run_id": run["id"]}, status=409)
        assert direct["error"]["code"] == "task_dependencies_held"
