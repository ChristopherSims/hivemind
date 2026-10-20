"""Shared scheduling fences run before effects and reject malformed decisions."""
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.scheduling import Scheduling, TaskDispatch
from hivemind.storage import Problem, uid


def scheduler(dispatch=(), dependencies=()):
    task = {"id": "task_candidate", "project_id": "project_one",
            "status": "queued", "depends_on": ["task_source"]}
    run = {"id": "run_one", "agent_id": "agent_one",
           "project_id": "project_one", "status": "idle"}
    store = SimpleNamespace(get=lambda *_: {
        "id": "task_source", "status": "completed", "project_id": "project_one"})
    service = SimpleNamespace(
        store=store, project_access=lambda *_: None, represent=lambda *_: None,
        run=lambda *_: run, task_dispatch_gates=dispatch,
        task_dependency_gates=dependencies)
    subject = Scheduling(service)
    subject.capability_set = lambda *_: set()
    subject._candidate = lambda *_: task
    effects = []
    subject._acquire = lambda *_: effects.append("claim")
    subject._start = lambda *_: effects.append("start") or {"task": task}
    return subject, task, effects


def test_dispatch_gate_defers_before_claim_or_start():
    subject, task, effects = scheduler(
        dispatch=(lambda *_: {"action": "defer", "reason": "budget_hold"},))
    result = subject.dispatch({}, "project_one", TaskDispatch(
        agent_id="agent_one", run_id="run_one"))
    assert result["task"] is None
    assert result["deferral"] == {"task_id": task["id"], "reason": "budget_hold"}
    assert effects == []


@pytest.mark.parametrize("decision", [False, True, {}, {"action": "unknown"}])
def test_malformed_dispatch_gate_fails_closed(decision):
    subject, _, effects = scheduler(dispatch=(lambda *_: decision,))
    with pytest.raises(Problem) as error:
        subject.dispatch({}, "project_one", TaskDispatch(
            agent_id="agent_one", run_id="run_one"))
    assert error.value.code == "invalid_dispatch_gate"
    assert effects == []


def test_allow_and_unconfigured_gates_preserve_existing_dispatch():
    subject, task, effects = scheduler(
        dispatch=(lambda *_: None, lambda *_: {"action": "allow"}))
    result = subject.dispatch({}, "project_one", TaskDispatch(
        agent_id="agent_one", run_id="run_one"))
    assert result["task"] == task
    assert effects == ["claim", "start"]


def test_dependency_gate_blocks_completed_but_unaccepted_deliverable():
    subject, task, _ = scheduler(
        dependencies=(lambda *_: {"action": "defer", "reason": "artifact_invalid"},))
    assert subject._dependencies_met({}, task) is False


@pytest.mark.parametrize("decision", [False, {}, {"action": "unknown"}])
def test_malformed_dependency_gate_fails_closed(decision):
    subject, task, _ = scheduler(dependencies=(lambda *_: decision,))
    assert subject._dependencies_met({}, task) is False


def test_http_dispatch_hold_does_not_consume_attempt_or_start_execution(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False)
    with TestClient(app) as client:
        def call(path, body):
            response = client.post("/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + config["token"], "Idempotency-Key": uid("key")})
            assert response.status_code == 200, response.text
            return response.json()["data"]

        project = config["project_id"]
        agent = call("agents", {"project_id": project, "display_name": "Gated worker"})
        run = call("runs", {"project_id": project, "agent_id": agent["id"]})
        plan = call("task-plans", {
            "project_id": project, "title": "Budget governed goal",
            "tasks": [{"key": "first", "title": "Do bounded work"}]})
        task_id = plan["tasks"][0]["id"]
        app.state.service.task_dispatch_gates = (
            lambda *_: {"action": "defer", "reason": "budget_hold"},)
        result = call(f"projects/{project}/tasks:dispatch",
                      {"agent_id": agent["id"], "run_id": run["id"]})
        assert result["task"] is None
        principal = dict(app.state.store.db.execute(
            "SELECT * FROM principals WHERE role='admin'").fetchone())
        stored = app.state.store.get(task_id, principal, "task")
        assert stored["status"] == "queued"
        assert not stored.get("execution")
        assert stored["scheduling"]["attempts"] == 0
        direct = client.post(f"/v1/tasks/{task_id}:start", json={
            "run_id": run["id"], "expected_revision": stored["revision"]},
            headers={"Authorization": "Bearer " + config["token"],
                     "Idempotency-Key": uid("key")})
        assert direct.status_code == 409, direct.text
        assert direct.json()["error"]["code"] == "task_dispatch_held"
        assert not app.state.store.get(task_id, principal, "task").get("execution")
        app.state.service.task_dispatch_gates = (lambda *_: {"action": "allow"},)
        started = call(f"projects/{project}/tasks:dispatch",
                       {"agent_id": agent["id"], "run_id": run["id"]})["task"]
        assert started["id"] == task_id
        assert started["execution"]["run_id"] == run["id"]
        assert started["scheduling"]["attempts"] == 1


def test_exact_dispatch_never_selects_higher_priority_unrelated_plan(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False)
    with TestClient(app) as client:
        def call(path, body):
            response = client.post("/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + config["token"], "Idempotency-Key": uid("key")})
            assert response.status_code == 200, response.text
            return response.json()["data"]

        project = config["project_id"]
        agent = call("agents", {"project_id": project, "display_name": "Exact worker"})
        run = call("runs", {"project_id": project, "agent_id": agent["id"]})
        unrelated = call("task-plans", {
            "project_id": project, "title": "Unrelated goal",
            "tasks": [{"key": "unrelated", "title": "Do not dispatch", "priority": 1000}]})
        intended = call("task-plans", {
            "project_id": project, "title": "Selected goal",
            "tasks": [{"key": "intended", "title": "Dispatch exactly this", "max_attempts": 1}]})
        task_id = intended["tasks"][0]["id"]
        service = app.state.service
        principal = dict(app.state.store.db.execute(
            "SELECT * FROM principals WHERE role='admin'").fetchone())
        request = TaskDispatch(agent_id=agent["id"], run_id=run["id"])
        result = service.mutate(principal, "integration-dispatch-exact", uid("key"),
                                {"task_id": task_id, **request.model_dump()},
                                lambda: Scheduling(service).dispatch_exact(
                                    principal, project, request, task_id))
        assert result["task"]["id"] == task_id
        assert result["task"]["scheduling"]["attempts"] == 1
        untouched = app.state.store.get(unrelated["tasks"][0]["id"], principal, "task")
        assert untouched["status"] == "queued"
        assert not untouched.get("execution")
