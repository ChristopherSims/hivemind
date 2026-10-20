"""SDK routing is independent of the server's opt-in remediation policy."""
import json

import httpx
import pytest

from hivemind.model_wiring import integration_options
from hivemind.sdk import Client


def test_blocked_task_facades_use_v1_and_fenced_request_body():
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(200, json={"protocol": "hivemind", "protocol_version": "0.1.0",
                                        "data": {"ok": True}})

    client = Client("http://testserver", "test-only-token", transport=httpx.MockTransport(receive))
    try:
        client.blocked_task_status("project_one", "task_one", run_id="run_one", agent_id="agent_one")
        client.request_task_resolution("project_one", {
            "task_id": "task_one", "expected_task_revision": 2, "action": "dependency_review"}, key="request-one")
        client.task_resolution_requests("project_one", task_id="task_one")
        client.task_resolution_request("resolution_one")
        client.acknowledge_task_resolution("resolution_one", {"note": "Investigating"}, key="ack-one")
        client.close_task_resolution("resolution_one", {"outcome": "declined"}, key="close-one")
    finally:
        client.close()
    assert [call.url.path for call in calls] == [
        "/v1/projects/project_one/tasks/task_one/blocked-status",
        "/v1/projects/project_one/blocked-tasks:request",
        "/v1/projects/project_one/blocked-task-requests",
        "/v1/blocked-task-requests/resolution_one",
        "/v1/blocked-task-requests/resolution_one:acknowledge",
        "/v1/blocked-task-requests/resolution_one:resolve",
    ]
    assert calls[0].url.params["run_id"] == "run_one"
    assert calls[1].headers["Idempotency-Key"] == "request-one"
    assert calls[-1].headers["Idempotency-Key"] == "close-one"


@pytest.mark.parametrize("section", [
    {"requests_enabled": 1}, {"explain_enabled": "true"},
    {"requests_enabled": True, "explain_enabled": False},
    {"recovery_source": "arbitrary executable"},
])
def test_blocked_config_rejects_coercion_and_untrusted_hooks(tmp_path, section):
    path = tmp_path / "integrations.json"
    path.write_text(json.dumps({"blocked_tasks": section}), encoding="utf-8")
    with pytest.raises(ValueError):
        integration_options(path)


def test_blocked_config_explicit_opt_in(tmp_path):
    path = tmp_path / "integrations.json"
    path.write_text(json.dumps({"blocked_tasks": {
        "explain_enabled": True, "requests_enabled": True}}), encoding="utf-8")
    policy = integration_options(path)["blocked_tasks"]["policy"]
    assert policy.explain_enabled is True
    assert policy.requests_enabled is True


def test_blocked_workflow_is_default_off_in_shared_registration(world):
    handle = world.app.state.blocked_tasks
    assert handle.policy.explain_enabled is False
    assert handle.policy.requests_enabled is False
    assert not any("blocked-status" in getattr(route, "path", "")
                   or "blocked-task-requests" in getattr(route, "path", "")
                   for route in world.app.routes)


def test_enabled_shared_registration_and_sdk_resolution_workflow(tmp_path):
    from fastapi.testclient import TestClient

    from hivemind.api import create_app
    from hivemind.blocked_tasks import BlockedTasksPolicy
    from hivemind.cli import initialize

    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False, capability_options={
        "blocked_tasks": {"policy": BlockedTasksPolicy(
            explain_enabled=True, requests_enabled=True)}})
    with TestClient(app) as server:
        def receive(request):
            response = server.request(request.method, str(request.url),
                                      content=request.content, headers=dict(request.headers))
            return httpx.Response(response.status_code, content=response.content,
                                  headers=response.headers)

        client = Client("http://testserver", config["token"],
                        transport=httpx.MockTransport(receive))
        try:
            task = client.request("POST", "tasks", {
                "project_id": config["project_id"], "title": "Resolve dependency"},
                key="task-create")
            status = client.blocked_task_status(config["project_id"], task["id"])
            assert status["ready"] is True
            request = client.request_task_resolution(config["project_id"], {
                "task_id": task["id"], "expected_task_revision": task["revision"],
                "action": "dependency_review", "note": "Check current readiness"},
                key="resolution-create")
            acknowledged = client.acknowledge_task_resolution(request["id"], {}, key="resolution-ack")
            assert acknowledged["state"] == "acknowledged"
            closed = client.close_task_resolution(request["id"], {
                "outcome": "resolved"}, key="resolution-close")
            assert closed["state"] == "resolved"
            assert client.task_resolution_request(request["id"])["state"] == "resolved"
        finally:
            client.close()
