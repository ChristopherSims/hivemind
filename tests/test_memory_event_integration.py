"""Canonical task changes produce an operator-readable memory review queue."""
import json
import time

import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.memory_event_bridge import BridgePolicy
from hivemind.memory_lifecycle_automation import LifecyclePolicy
from hivemind.storage import uid


def test_registered_task_source_review_is_readable_through_api(tmp_path):
    config = json.loads(initialize(tmp_path / "canonical").read_text())
    app = create_app(tmp_path / "canonical", workers=False, capability_options={
        "memory_lifecycle_automation": {"policy": LifecyclePolicy(
            enabled=True, operator_principal_id=config["principal_id"])},
        "memory_event_bridge": {"policy": BridgePolicy(enabled=True, heartbeat_seconds=0.05)}})
    headers = {"Authorization": "Bearer " + config["token"]}
    with TestClient(app) as http:
        def post(path, body):
            response = http.post("/v1/" + path, json=body,
                                 headers={**headers, "Idempotency-Key": uid("key")})
            assert response.status_code == 200, response.json().get("error", {}).get("code")
            return response.json()["data"]

        project = config["project_id"]
        agent = post("agents", {"project_id": project, "display_name": "Source worker"})
        run = post("runs", {"project_id": project, "agent_id": agent["id"]})
        task = post("tasks", {"project_id": project, "title": "Supported task"})
        task = post(f"tasks/{task['id']}:start", {
            "run_id": run["id"], "expected_revision": task["revision"]})
        memory = post("memories", {
            "project_id": project, "title": "Source fact", "scope_kind": "project",
            "scope_owner_id": project, "content": {"media_type": "text/plain", "text": "Source evidence"},
            "evidence_refs": [{"kind": "task", "id": task["id"], "revision": task["revision"]}]})
        post(f"tasks/{task['id']}:checkpoint", {
            "expected_revision": task["revision"], "run_id": run["id"],
            "execution_id": task["execution"]["id"], "checkpoint_id": "source-checkpoint",
            "status": "running", "progress": 25, "summary": "Source revision changed"})
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            response = http.get(f"/v1/memory-events:reviews?project_id={project}", headers=headers)
            assert response.status_code == 200
            reviews = response.json()["data"]["reviews"]
            if reviews:
                break
            time.sleep(0.05)
        else:
            state = http.get(f"/v1/memory-events:status?project_id={project}", headers=headers).json()["data"]
            pytest.fail(f"Source review missing: {state['decisions']}, error={state['last_error']}, "
                        f"worker={state['worker']}")
        assert reviews[0]["memory_id"] == memory["id"]
        assert reviews[0]["reason"] == "supporting_source_revision_changed"
        assert reviews[0]["status_effect"] == "review_only"
    assert not app.state.memory_event_bridge._thread.is_alive()


@pytest.mark.parametrize("name", ["queue_capacity", "batch_limit", "retry_limit", "support_page_limit"])
def test_bridge_integer_limits_are_not_coerced(name):
    with pytest.raises(ValueError):
        BridgePolicy(**{name: True})
    with pytest.raises(ValueError):
        BridgePolicy(**{name: 32.5})
