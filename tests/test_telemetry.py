import json

import httpx
import pytest

from hivemind.storage import uid
from hivemind.telemetry import Telemetry, TelemetryPolicy, TracedRuntime


def configured(tmp_path, **fields):
    return TelemetryPolicy(enabled=True, directory=str(tmp_path), **fields)


def test_trace_queue_is_bounded_content_free_and_survives_restart(tmp_path):
    project, agent = uid("project"), uid("agent")
    telemetry = Telemetry(configured(tmp_path, max_spans=2))
    with telemetry.span("agent.turn", project_id=project, agent_id=agent,
                        prompt="password=NEVER", token="NEVER", task_id="task-secret-not-an-id"):
        with telemetry.span("agent.message", project_id=project):
            pass
    with telemetry.span("recovery.effect", project_id=project):
        pass
    assert telemetry.status(project)["queued"] == 2 and telemetry.dropped == 1
    rows = [json.loads(row[0]) for row in telemetry.db.execute("SELECT body FROM spans ORDER BY id")]
    assert rows[0]["traceId"] == rows[1]["traceId"]
    assert rows[0]["parentSpanId"] == rows[1]["spanId"]
    assert len(rows[0]["traceId"]) == 32 and len(rows[0]["spanId"]) == 16
    encoded = json.dumps(rows)
    assert "NEVER" not in encoded and "task-secret" not in encoded and "prompt" not in encoded
    telemetry.close()
    reopened = Telemetry(configured(tmp_path))
    assert reopened.status(project)["queued"] == 2
    reopened.close()


@pytest.mark.parametrize("response", [
    httpx.Response(500, json={}),
    httpx.Response(200, json={"partialSuccess": {"rejectedSpans": 1}}),
    httpx.Response(200, text="not JSON"),
    httpx.Response(302, headers={"Location": "https://not-approved.example/v1/traces"}),
])
def test_unknown_partial_and_redirect_exports_never_drop_spans(tmp_path, response):
    project = uid("project")
    telemetry = Telemetry(configured(tmp_path, endpoint="http://127.0.0.1:4318/v1/traces",
                                     allow_export=True),
                          transport=httpx.MockTransport(lambda request: response))
    try:
        with telemetry.span("agent.task", project_id=project):
            pass
        with pytest.raises((RuntimeError, ValueError)):
            telemetry.export(project)
        assert telemetry.status(project)["queued"] == 1
    finally:
        telemetry.close()


def test_otlp_wire_and_project_scoping(tmp_path):
    first, second = uid("project"), uid("project")
    captured = []

    def collect(request):
        assert request.url.path == "/v1/traces"
        assert request.headers["Content-Type"] == "application/json"
        captured.append(json.loads(request.content))
        return httpx.Response(200, json={})

    telemetry = Telemetry(configured(tmp_path, endpoint="http://127.0.0.1:4318/v1/traces",
                                     allow_export=True), transport=httpx.MockTransport(collect))
    try:
        for project in (first, second):
            with telemetry.span("canonical.command", project_id=project):
                pass
        assert telemetry.export(first) == {"exported": 1, "remaining": 0}
        spans = captured[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]
        assert spans[0]["attributes"] == [{"key": "hivemind.project_id",
                                          "value": {"stringValue": first}}]
        assert telemetry.status(second)["queued"] == 1
    finally:
        telemetry.close()


def test_diagnostic_failure_never_swallows_runtime_error(tmp_path):
    telemetry = Telemetry(configured(tmp_path))
    telemetry.close()

    class Runtime:
        def execute(self, *args):
            raise RuntimeError("real operation failed")

    runtime = TracedRuntime(Runtime(), telemetry)
    with pytest.raises(RuntimeError, match="real operation"):
        runtime.execute({"id": uid("task")}, {"project_id": uid("project")}, None)


@pytest.mark.parametrize("endpoint", [
    "http://collector.example/v1/traces", "https://user:secret@collector.example/v1/traces",
    "https://collector.example/v1/traces?token=secret", "file:///v1/traces",
])
def test_export_requires_explicit_safe_endpoint(tmp_path, endpoint):
    with pytest.raises(ValueError):
        configured(tmp_path, endpoint=endpoint, allow_export=True)


def test_real_service_command_spans_and_export_authority(world, tmp_path):
    collector = []
    telemetry = Telemetry(configured(tmp_path, endpoint="http://127.0.0.1:4318/v1/traces",
                                     allow_export=True),
                          transport=httpx.MockTransport(
                              lambda request: collector.append(json.loads(request.content))
                              or httpx.Response(200, json={})))
    world.app.state.telemetry = telemetry
    try:
        world.call("POST", "tasks", {"project_id": world.project, "title": "DO NOT CAPTURE THIS"})
        status = world.call("GET", f"projects/{world.project}/telemetry")
        assert status["enabled"] and status["queued"] >= 1
        agent = world.agent("DO NOT CAPTURE THIS EITHER")
        token = world.credential(agent)
        response = world.client.post(f"/v1/projects/{world.project}/telemetry:export",
                                     headers={"Authorization": "Bearer " + token})
        assert response.status_code == 403 and collector == []
        result = world.call("POST", f"projects/{world.project}/telemetry:export", {})
        assert result["exported"] >= 1
        assert "DO NOT CAPTURE" not in json.dumps(collector)
        assert token not in json.dumps(collector)
    finally:
        telemetry.close()
