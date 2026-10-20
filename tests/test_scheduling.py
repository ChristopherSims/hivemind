"""Task scheduling: plans, dependencies, dispatch, attempts, escalation, reconciliation.

These tests drive the scheduling domain and its ``install`` endpoints through a locally
built application (the same store, service, principal dependency, and envelope helper
that ``hivemind.api`` uses). ``test_main_application_wiring`` additionally checks the
real application once ``api.py`` installs the module.
"""

import json
import secrets
import time

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from hivemind import models as m
from hivemind import scheduling
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.service import Service
from hivemind.storage import Problem, Store, uid


def build(directory):
    """Mirror hivemind.api wiring so install() can be exercised before api.py calls it."""
    app = create_app(directory, workers=False)
    service, store = app.state.service, app.state.store

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"), "data": data}

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) if isinstance(request, m.Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    installed = scheduling.install(app, service, principal, command, envelope)
    return app, installed


class Harness:
    def __init__(self, app, client, config):
        self.app, self.client = app, client
        self.project = config["project_id"]
        self.owner = config["token"]
        self.store = app.state.store
        self.service = app.state.service

    def request(self, method, path, body=None, token=None, key=None):
        return self.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner), "Idempotency-Key": key or uid("key")})

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.request(method, path, body, token, key)
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Worker", actions=None):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name,
                                            "supported_actions": actions or []})

    def credential(self, agent=None, role="agent"):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                                  (uid("principal"), hash_token(token), role, self.project,
                                   agent["id"] if agent else None, 0, 1))
        return token

    def run(self, agent, token):
        return self.call("POST", "runs", {"project_id": self.project, "agent_id": agent["id"]}, token)

    def started(self, name="Worker", actions=None):
        agent = self.agent(name, actions)
        token = self.credential(agent)
        return agent, token, self.run(agent, token)

    def scope(self, key):
        return self.call("POST", "scopes", {"project_id": self.project, "canonical_key": key,
                                            "display_name": key, "allowed_paths": []})

    def plan(self, steps, title="Plan"):
        return self.call("POST", "task-plans", {"project_id": self.project, "title": title, "tasks": steps})

    def step(self, key, scope, **overrides):
        body = {"key": key, "title": key.title(), "scope_ids": [scope["id"]]}
        body.update(overrides)
        return body

    def dispatch(self, agent, run, token, status=200, key=None):
        return self.call("POST", f"projects/{self.project}/tasks:dispatch",
                         {"agent_id": agent["id"], "run_id": run["id"]}, token, key=key, status=status)

    def report(self, task, agent, run, token, outcome="failed", status=200, **overrides):
        body = {"expected_revision": task["revision"], "agent_id": agent["id"], "run_id": run["id"],
                "outcome": outcome, "summary": "Attempt finished", **overrides}
        return self.call("POST", f"tasks/{task['id']}:dispatch-outcome", body, token, status=status)

    def complete(self, task, run, token):
        return self.call("POST", f"tasks/{task['id']}:checkpoint", {
            "expected_revision": task["revision"], "run_id": run["id"],
            "execution_id": task["execution"]["id"], "checkpoint_id": uid("checkpoint"),
            "status": "completed", "progress": 100, "summary": "Completed by core checkpoint"}, token)

    def reconcile(self, task, action, token=None, status=200, reason="Recovered", revision=None):
        return self.call("POST", f"tasks/{task['id']}:reconcile", {
            "expected_revision": task["revision"] if revision is None else revision,
            "action": action, "reason": reason}, token, status=status)

    def post(self, text="Scheduling evidence"):
        board = self.call("GET", f"boards?project_id={self.project}")[0]
        return self.call("POST", f"boards/{board['id']}/posts", {
            "client_post_id": uid("client"), "kind": "finding",
            "content": {"media_type": "text/plain", "text": text}})

    def stored(self, kind):
        return [json.loads(row["body"]) for row in
                self.store.db.execute("SELECT body FROM resources WHERE kind=? AND deleted=0",
                                      (kind,)).fetchall()]

    def admin(self):
        return dict(self.store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())


@pytest.fixture
def sched(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, installed = build(tmp_path)
    assert len(installed["endpoints"]) == 9 and installed["models"][0] == "PlannedTask"
    assert "TaskHandoff" in installed["models"]
    with TestClient(app) as client:
        yield Harness(app, client, config)


# ---- model validation -----------------------------------------------------


def test_plan_models_reject_cycles_unknown_references_and_unbounded_values():
    step = scheduling.PlannedTask(key="a", title="A", depends_on=["b"])
    other = scheduling.PlannedTask(key="b", title="B")
    assert [item.key for item in scheduling.topological_order([step, other])] == ["b", "a"]
    assert scheduling.PlannedTask(key=" a ", title=" A ", depends_on=[" b "]).key == "a"
    with pytest.raises(ValidationError):
        scheduling.TaskPlanCreate(project_id="p", title="Plan", tasks=[step])  # unknown dependency key
    with pytest.raises(ValidationError):
        scheduling.TaskPlanCreate(project_id="p", title="Plan", tasks=[step, step, other])  # duplicate key
    with pytest.raises(ValidationError):
        scheduling.PlannedTask(key="a", title="A", depends_on=["a"])
    with pytest.raises(ValidationError):
        scheduling.TaskPlanCreate(project_id="p", title="Plan", tasks=[
            step, scheduling.PlannedTask(key="b", title="B", depends_on=["a"])])  # cycle
    with pytest.raises(ValidationError):
        scheduling.PlannedTask(key="a", title="A", priority=scheduling.MAX_PRIORITY + 1)
    with pytest.raises(ValidationError):
        scheduling.PlannedTask(key="a", title="A", max_attempts=scheduling.MAX_ATTEMPTS_LIMIT + 1)
    with pytest.raises(ValidationError):
        scheduling.PlannedTask(key="a", title="A", surprise=True)  # extra fields forbidden
    with pytest.raises(ValidationError):
        scheduling.PlannedTask(key="a", title="   ")  # blank after stripping
    with pytest.raises(ValidationError):
        scheduling.TaskDispatch(agent_id="a", run_id="r", capability_override=["review"])
    with pytest.raises(ValidationError):
        scheduling.TaskReconcile(expected_revision=1, action="delete", reason="reason")
    with pytest.raises(ValidationError):
        scheduling.AgentCapabilities(capabilities=["review", "review"])


# ---- plans ---------------------------------------------------------------


def test_plan_creation_records_dependencies_priority_and_capabilities(sched):
    first, blocked = sched.scope("sched/first"), sched.scope("sched/blocked")
    plan = sched.plan([
        {"key": "blocked", "title": "Blocked", "scope_ids": [blocked["id"]], "depends_on": ["first"],
         "priority": 50, "max_attempts": 2, "required_capabilities": ["review"]},
        sched.step("first", first, priority=10),
    ], title="Scheduled work")
    assert [task["plan_key"] for task in plan["tasks"]] == ["first", "blocked"]  # topological order
    created = {task["plan_key"]: task for task in plan["tasks"]}
    assert created["first"]["priority"] == 10 and created["first"]["max_attempts"] == 3
    assert created["first"]["status"] == "queued" and created["first"]["scheduling"]["attempts"] == 0
    assert created["blocked"]["depends_on"] == [created["first"]["id"]]
    assert created["blocked"]["required_capabilities"] == ["review"]
    assert created["blocked"]["max_attempts"] == 2
    flags = {task["plan_key"]: (task["dispatchable"], task["dependencies_met"]) for task in plan["tasks"]}
    assert flags == {"first": (True, True), "blocked": (False, False)}
    relations = [tuple(row) for row in sched.store.db.execute(
        "SELECT source,target,kind FROM relations WHERE kind='DEPENDS_ON'").fetchall()]
    assert relations == [(created["blocked"]["id"], created["first"]["id"], "DEPENDS_ON")]
    assert sched.call("GET", f"task-plans?project_id={sched.project}")[0]["id"] == plan["plan"]["id"]
    assert sched.call("GET", f"task-plans/{plan['plan']['id']}")["tasks"][0]["plan_id"] == plan["plan"]["id"]
    tasks = sched.call("GET", f"tasks?project_id={sched.project}")
    assert {task["id"] for task in tasks} == {task["id"] for task in plan["tasks"]}


# ---- dispatch selection --------------------------------------------------


def test_dispatch_respects_dependencies_priority_and_capabilities(sched):
    scopes = {name: sched.scope("sched/" + name) for name in ("first", "blocked", "third", "cap")}
    agent, token, run = sched.started()
    plan = sched.plan([
        sched.step("third", scopes["third"], priority=70, depends_on=["blocked"]),
        sched.step("cap", scopes["cap"], priority=60, required_capabilities=["review"]),
        sched.step("blocked", scopes["blocked"], priority=50, depends_on=["first"]),
        sched.step("first", scopes["first"], priority=10),
    ])

    chosen = sched.dispatch(agent, run, token, key="dispatch-1")
    assert chosen["task"]["plan_key"] == "first"  # dependencies and capability gate higher priorities
    assert chosen["task"]["status"] == "running"
    sched.report(chosen["task"], agent, run, token, outcome="succeeded")
    blocked = sched.dispatch(agent, run, token, key="dispatch-2")
    assert blocked["task"]["plan_key"] == "blocked"
    sched.complete(blocked["task"], run, token)  # core completion satisfies dependents
    third = sched.dispatch(agent, run, token, key="dispatch-3")
    assert third["task"]["plan_key"] == "third"
    assert sched.report(third["task"], agent, run, token, outcome="failed")["status"] == "queued"

    sched.call("POST", f"agents/{agent['id']}/capabilities", {"capabilities": ["review"]}, token)
    capable = sched.dispatch(agent, run, token, key="dispatch-4")
    assert capable["task"]["plan_key"] == "cap"
    sched.report(capable["task"], agent, run, token, outcome="succeeded")

    retrying = sched.call("GET", f"tasks/{third['task']['id']}")
    assert sched.report(retrying, agent, run, token, outcome="succeeded", status=409)["error"]["code"] == \
        "task_not_dispatched"
    assert sched.dispatch(agent, run, token, key="dispatch-5") == {"task": None, "claim": None}
    statuses = {task["plan_key"]: task["status"] for task in
                sched.call("GET", f"task-plans/{plan['plan']['id']}")["tasks"]}
    assert statuses == {"first": "completed", "blocked": "completed", "third": "queued", "cap": "completed"}


def test_dispatch_acquires_exact_scope_claim_and_is_idempotent(sched):
    scope = sched.scope("sched/idempotent")
    agent, token, run = sched.started()
    sched.plan([sched.step("only", scope)])
    first = sched.dispatch(agent, run, token, key="dispatch-key")
    assert set(first) == {"task", "claim"}
    assert first["claim"]["mode"] == "exclusive_owner" and first["claim"]["state"] == "active"
    assert first["claim"]["scope_ids"] == [scope["id"]] and first["claim"]["task_id"] == first["task"]["id"]
    assert first["claim"]["run_id"] == run["id"]
    assert first["task"]["status"] == "running" and first["task"]["execution"]["run_id"] == run["id"]
    assert sched.call("GET", f"runs/{run['id']}", token=token)["task_id"] == first["task"]["id"]

    assert sched.dispatch(agent, run, token, key="dispatch-key") == first
    claims = sched.stored("work_claim")
    assert len(claims) == 1 and claims[0]["intent"].startswith("Dispatch scheduled task")
    assert sched.call("GET", f"tasks/{first['task']['id']}")["scheduling"]["attempts"] == 1

    assert sched.dispatch(agent, run, token, key="another-key") == {"task": None, "claim": None}


def test_dispatch_conflict_is_deterministic_and_preserved(sched):
    scope = sched.scope("sched/contested")
    holder, holder_token, holder_run = sched.started("Holder")
    other, other_token, other_run = sched.started("Other")
    plan = sched.plan([sched.step("alpha", scope), sched.step("beta", scope)])
    alpha, beta = plan["tasks"]
    assert sched.dispatch(holder, holder_run, other_token, status=403)  # actor binding
    taken = sched.dispatch(holder, holder_run, holder_token)
    assert taken["task"]["id"] == alpha["id"]

    denied = sched.dispatch(other, other_run, other_token, status=409)
    assert denied["task"] is None and denied["claim"] is None
    assert denied["conflict"]["state"] == "denied"
    assert denied["conflict"]["winner_claim_id"] == taken["claim"]["id"]
    assert [conflict["id"] for conflict in sched.call("GET", f"conflicts?project_id={sched.project}")] == \
        [denied["conflict"]["id"]]
    stored = [task for task in sched.stored("task") if task["id"] == beta["id"]][0]
    assert stored["status"] == "queued" and stored["scheduling"]["attempts"] == 0

    sched.complete(taken["task"], holder_run, holder_token)
    again = sched.dispatch(other, other_run, other_token, status=409)
    assert again["conflict"]["winner_claim_id"] == taken["claim"]["id"]  # deterministic arbitration
    assert len(sched.call("GET", f"conflicts?project_id={sched.project}")) == 2
    claim = sched.call("GET", "work-claims?project_id=" + sched.project)[0]
    sched.call("DELETE", f"work-claims/{claim['id']}", {"expected_revision": claim["revision"]}, holder_token)
    assert sched.dispatch(other, other_run, other_token)["task"]["id"] == beta["id"]


def test_dispatch_without_eligible_work_and_actor_errors(sched):
    agent, token, run = sched.started()
    empty = sched.dispatch(agent, run, token)
    assert set(empty) == {"task", "claim"} and empty == {"task": None, "claim": None}
    assert sched.call("GET", f"runs/{run['id']}", token=token)["task_id"] is None

    scope = sched.scope("sched/gated")
    sched.plan([sched.step("gated", scope, required_capabilities=["review"])])
    assert sched.dispatch(agent, run, token) == {"task": None, "claim": None}

    _, stranger_token, _ = sched.started("Stranger")
    sched.call("POST", "projects/" + sched.project + "/tasks:dispatch",
               {"agent_id": agent["id"], "run_id": run["id"]}, stranger_token, status=403)
    sched.call("POST", "projects/" + sched.project + "/tasks:dispatch",
               {"agent_id": agent["id"], "run_id": uid("run")}, token, status=404)
    with sched.store.transaction():
        body = sched.store.get(run["id"], sched.admin(), "run")
        body["status"] = "completed"
        sched.store.save("run", sched.project, sched.admin()["id"], body)
    sched.dispatch(agent, run, token, status=409)
    assert [task["scheduling"]["attempts"] for task in sched.stored("task")] == [0]
    assert not sched.stored("work_claim")


# ---- attempts, backoff, escalation ---------------------------------------


def test_failed_attempts_back_off_then_escalate_and_requeue(sched, monkeypatch):
    scope = sched.scope("sched/retry")
    agent, token, run = sched.started()
    sched.plan([sched.step("retry", scope, max_attempts=3)])
    clock = time.time()

    dispatched = sched.dispatch(agent, run, token, key="attempt-1")
    task = dispatched["task"]
    assert task["scheduling"]["attempts"] == 1 and task["scheduling"]["state"] == "dispatched"
    failed = sched.report(task, agent, run, token, outcome="failed", summary="Toolchain missing")
    assert failed["status"] == "queued" and failed["scheduling"]["attempts"] == 1
    assert failed["scheduling"]["state"] == "retry_scheduled"
    assert failed["scheduling"]["retry_after"] > clock
    assert failed["scheduling"]["attempt_history"][-1]["outcome"] == "failed"
    assert failed["execution"] is None and failed["execution_history"][-1]["result"]["self_report"] is True
    assert sched.call("GET", f"runs/{run['id']}", token=token)["task_id"] is None
    assert len(sched.stored("work_claim")) == 1 and sched.stored("work_claim")[0]["state"] == "active"
    assert sched.dispatch(agent, run, token, key="attempt-1b") == {"task": None, "claim": None}

    monkeypatch.setattr(time, "time", lambda: clock + scheduling.BACKOFF_BASE_SECONDS + 1)
    second = sched.dispatch(agent, run, token, key="attempt-2")["task"]
    assert second["scheduling"]["attempts"] == 2
    assert sched.report(second, agent, run, token, outcome="failed")["scheduling"]["retry_after"] > clock + 6

    monkeypatch.setattr(time, "time", lambda: clock + 60)
    third = sched.dispatch(agent, run, token, key="attempt-3")["task"]
    assert third["scheduling"]["attempts"] == 3
    escalated = sched.report(third, agent, run, token, outcome="failed", summary="Still broken")
    assert escalated["status"] == "failed" and escalated["scheduling"]["escalated"] is True
    assert escalated["scheduling"]["state"] == "escalated" and escalated["scheduling"]["retry_after"] is None
    assert escalated["scheduling"]["escalation"]["reason"] == "attempt_budget_exhausted"
    assert escalated["scheduling"]["escalation"]["attempts"] == 3
    assert len(escalated["scheduling"]["attempt_history"]) == 3
    assert sched.dispatch(agent, run, token, key="attempt-4") == {"task": None, "claim": None}

    monkeypatch.setattr(time, "time", lambda: clock + 600)
    requeued = sched.reconcile(escalated, "requeue", reason="Retried after dependency fix")
    assert requeued["status"] == "queued" and requeued["scheduling"]["attempts"] == 0
    assert requeued["scheduling"]["escalated"] is False
    final = sched.dispatch(agent, run, token, key="attempt-5")
    assert final["task"]["scheduling"]["attempts"] == 1
    assert sched.report(final["task"], agent, run, token, outcome="succeeded")["status"] == "completed"


def test_outcome_reports_are_fenced_self_reports(sched):
    scope = sched.scope("sched/report")
    agent, token, run = sched.started()
    plan = sched.plan([sched.step("report", scope), sched.step("waiting", scope, depends_on=["report"])])
    sched.report(plan["tasks"][0], agent, run, token, status=409)  # nothing dispatched yet

    task = sched.dispatch(agent, run, token)["task"]
    other, other_token, other_run = sched.started("Impostor")
    assert sched.report(task, other, other_run, other_token, status=403)["error"]["code"] == \
        "task_assigned_elsewhere"
    sched.report(task, agent, run, token, status=412, expected_revision=task["revision"] + 5)
    sched.report(task, agent, run, token, status=404,
                 evidence_refs=[{"kind": "board_post", "id": uid("board_post")}])

    proof = sched.post("Dispatch attempt evidence")
    completed = sched.report(task, agent, run, token, outcome="succeeded",
                             evidence_refs=[{"kind": "board_post", "id": proof["id"]}])
    assert completed["status"] == "completed"
    assert completed["scheduling"]["last_outcome"]["evidence_refs"] == [
        {"kind": "board_post", "id": proof["id"], "revision": proof["revision"]}]
    assert completed["execution_history"][-1]["result"]["self_report"] is True
    assert completed["execution_history"][-1]["result"]["outcome"] == "succeeded"
    assert sched.report(completed, agent, run, token, outcome="succeeded", status=409)

    follower = sched.dispatch(agent, run, token)
    assert follower["task"]["plan_key"] == "waiting"  # dependency satisfied by the self-report


# ---- explicit reconciliation --------------------------------------------


def test_reconcile_requires_privilege_and_refuses_live_execution(sched, monkeypatch):
    scope = sched.scope("sched/recover")
    agent, token, run = sched.started()
    sched.plan([sched.step("recover", scope)])
    dispatched = sched.dispatch(agent, run, token, key="recover-1")
    task, claim = dispatched["task"], dispatched["claim"]

    assert sched.reconcile(task, "requeue", token, status=403)["error"]["code"] == "forbidden"
    assert sched.reconcile(task, "requeue", status=409)["error"]["code"] == "task_execution_active"
    assert sched.call("GET", f"tasks/{task['id']}")["status"] == "running"

    clock = time.time() + scheduling.STALE_EXECUTION_SECONDS + 91
    monkeypatch.setattr(time, "time", lambda: clock)
    recovered = sched.reconcile(task, "requeue", reason="Run never heartbeated again")
    assert recovered["status"] == "queued" and recovered["execution"] is None
    assert recovered["execution_history"][-1]["result"]["action"] == "requeue"
    assert recovered["scheduling"]["last_reconcile"]["reason"] == "Run never heartbeated again"
    assert sched.call("GET", f"runs/{run['id']}", token=token)["task_id"] is None
    stored_claim = [item for item in sched.stored("work_claim") if item["id"] == claim["id"]][0]
    assert stored_claim["state"] == "active", "reconcile must not release live claims"
    assert not any(item["state"] == "released" for item in sched.stored("work_claim"))

    escalated = sched.reconcile(recovered, "escalate", reason="Needs a human decision")
    assert escalated["scheduling"]["escalated"] is True and escalated["status"] == "queued"
    assert escalated["scheduling"]["escalation"]["reason"] == "review_requested"
    assert sched.dispatch(agent, run, token, key="recover-2") == {"task": None, "claim": None}

    reassigned = sched.reconcile(escalated, "reassign", reason="Hand to another agent")
    assert reassigned["scheduling"]["reassignments"] == 1
    assert reassigned["scheduling"]["assigned_agent_id"] is None
    assert reassigned["scheduling"]["last_assigned_agent_id"] == agent["id"]
    assert reassigned["scheduling"]["escalated"] is False

    retaken = sched.dispatch(agent, run, token, key="recover-3")
    assert retaken["task"]["scheduling"]["attempts"] == 1
    finished = sched.report(retaken["task"], agent, run, token, outcome="succeeded")
    assert sched.reconcile(finished, "requeue", status=409)["error"]["code"] == "terminal_task"
    assert sched.reconcile(finished, "requeue", status=412, revision=1)["error"]["code"] == "revision_conflict"


# ---- capability matching -------------------------------------------------


def test_capability_metadata_is_self_registered_and_additive(sched):
    scope = sched.scope("sched/capability")
    agent, token, run = sched.started("Capable", actions=["assign_task"])
    sched.plan([sched.step("control", scope, required_capabilities=["assign_task"]),
                sched.step("review", scope, required_capabilities=["review"])])
    dispatched = sched.dispatch(agent, run, token)
    assert dispatched["task"]["plan_key"] == "control"  # supported_actions is a capability source
    sched.report(dispatched["task"], agent, run, token, outcome="succeeded")

    view = sched.call("GET", f"agents/{agent['id']}/capabilities", token=token)
    assert view["effective"] == ["assign_task"] and view["capabilities"] == []
    sched.call("POST", f"agents/{agent['id']}/capabilities", {"capabilities": ["review", "python"]}, token)
    view = sched.call("GET", f"agents/{agent['id']}/capabilities", token=token)
    assert view["effective"] == ["assign_task", "python", "review"]
    assert sched.dispatch(agent, run, token)["task"]["plan_key"] == "review"

    _, other_token, _ = sched.started("Other")
    sched.call("POST", f"agents/{agent['id']}/capabilities", {"capabilities": ["steer"]}, other_token, status=403)
    sched.call("POST", f"agents/{agent['id']}/capabilities", {"capabilities": []}, token)
    assert sched.call("GET", f"agents/{agent['id']}/capabilities", token=token)["effective"] == ["assign_task"]


# ---- domain layer --------------------------------------------------------


def test_domain_class_commits_within_a_service_transaction(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    store = Store(tmp_path)
    try:
        service = Service(store)
        principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        agent = service.create(principal, "agent", {
            "project_id": config["project_id"], "display_name": "Domain worker", "description": "",
            "runtime": "generic", "supported_actions": [], "scope_permit_enforcement": False})
        scope = service.create(principal, "scope", {
            "project_id": config["project_id"], "canonical_key": "sched/domain",
            "display_name": "sched/domain", "allowed_paths": []})
        run = service.start_run(principal, m.RunCreate(project_id=config["project_id"], agent_id=agent["id"]))
        domain = scheduling.Scheduling(service)

        def mutate(operation, payload, function):
            return service.mutate(principal, operation, uid("key"), payload, function)

        plan = mutate("plans", {"plan": 1}, lambda: domain.create_plan(principal, scheduling.TaskPlanCreate(
            project_id=config["project_id"], title="Domain plan", tasks=[
                scheduling.PlannedTask(key="a", title="A", scope_ids=[scope["id"]], priority=5),
                scheduling.PlannedTask(key="b", title="B", scope_ids=[scope["id"]], depends_on=["a"])])))
        assert [task["plan_key"] for task in plan["tasks"]] == ["a", "b"]
        assert domain.plans(principal, config["project_id"])[0]["id"] == plan["plan"]["id"]

        dispatched = mutate("dispatch", {"agent_id": agent["id"], "run_id": run["id"]},
                            lambda: domain.dispatch(principal, config["project_id"],
                                                    scheduling.TaskDispatch(agent_id=agent["id"], run_id=run["id"])))
        assert dispatched["task"]["plan_key"] == "a"
        assert dispatched["claim"]["task_id"] == dispatched["task"]["id"]
        completed = mutate("outcome", {"outcome": "succeeded"},
                           lambda: domain.report(principal, dispatched["task"]["id"], scheduling.TaskDispatchOutcome(
                               expected_revision=dispatched["task"]["revision"], agent_id=agent["id"],
                               run_id=run["id"], outcome="succeeded", summary="Domain attempt done")))
        assert completed["status"] == "completed"

        # Reopened storage proves the domain writes are durable, not in-memory views.
        restored = Store(tmp_path)
        try:
            reread = dict(restored.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
            body = json.loads(restored.raw(dispatched["task"]["id"], "task")["body"])
            assert body["status"] == "completed" and body["scheduling"]["attempts"] == 1
            assert Service(restored).store.get(dispatched["task"]["id"], reread)["status"] == "completed"
        finally:
            restored.close()
    finally:
        store.close()


# ---- main application wiring --------------------------------------------


def test_main_application_wiring(world):
    paths = {getattr(route, "path", "") for route in world.app.routes}
    if "/v1/task-plans" not in paths:
        pytest.skip("api.py does not install hivemind.scheduling yet")
    agent = world.agent("Wired worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope("sched/wired")
    plan = world.call("POST", "task-plans", {"project_id": world.project, "title": "Wired",
                                            "tasks": [{"key": "only", "title": "Only",
                                                       "scope_ids": [scope["id"]]}]})
    dispatched = world.call("POST", f"projects/{world.project}/tasks:dispatch",
                            {"agent_id": agent["id"], "run_id": run["id"]}, token)
    assert dispatched["task"]["id"] == plan["tasks"][0]["id"]
    assert world.call("GET", f"tasks/{dispatched['task']['id']}", token=token)["status"] == "running"
