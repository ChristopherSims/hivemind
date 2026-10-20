"""Goal orchestration: canonical DAG plans, ready dispatch, bounded revision, holds.

These tests drive the goal domain and its ``install`` endpoints through a locally
built application (the same store, service, principal dependency, and envelope helper
that ``hivemind.api`` uses). No model or provider calls are made; the provider path
is exercised with a synchronous in-process stub.
"""

import secrets

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import goal_orchestration
from hivemind import models as m
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.goal_orchestration import (GoalOrchestration, GoalRevise, GoalRunner,
                                         GoalSchedule, GoalTick, OPERATION_DESCRIPTORS,
                                         extract_steps)
from hivemind.storage import Problem, uid


def build(directory, **options):
    """Mirror hivemind.api wiring so install() can be exercised before api.py calls it."""
    app = create_app(directory, workers=False)
    service, store = app.state.service, app.state.store

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

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
        payload = request.model_dump(by_alias=True, exclude_none=True) \
            if isinstance(request, m.Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    installed = goal_orchestration.install(app, service, principal, command, envelope,
                                           **options)
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
            "Authorization": "Bearer " + (token or self.owner),
            "Idempotency-Key": key or uid("key")})

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.request(method, path, body, token, key)
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Worker", actions=None):
        return self.call("POST", "agents", {"project_id": self.project,
                                            "display_name": name,
                                            "supported_actions": actions or []})

    def credential(self, agent=None, role="agent", coordinator=False):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute(
                "INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                (uid("principal"), hash_token(token), role, self.project,
                 agent["id"] if agent else None, int(coordinator), 1))
        return token

    def run(self, agent, token):
        return self.call("POST", "runs", {"project_id": self.project,
                                          "agent_id": agent["id"]}, token)

    def started(self, name="Worker", actions=None):
        agent = self.agent(name, actions)
        token = self.credential(agent)
        return agent, token, self.run(agent, token)

    def scope(self, key):
        return self.call("POST", "scopes", {"project_id": self.project,
                                            "canonical_key": key, "display_name": key,
                                            "allowed_paths": []})

    def step(self, key, scope=None, **overrides):
        body = {"key": key, "title": key.title(),
                "scope_ids": [scope["id"]] if scope else []}
        body.update(overrides)
        return body

    def goal(self, body=None, **overrides):
        payload = {"project_id": self.project, "title": "Ship website",
                   "statement": "Ship the website"}
        payload.update(body or {})
        payload.update(overrides)
        return self.call("POST", "goals", payload)

    def view(self, goal_id, token=None):
        return self.call("GET", f"goals/{goal_id}", token=token)

    def tick(self, goal_id, token=None, status=200, **overrides):
        return self.call("POST", f"goals/{goal_id}:tick", overrides, token,
                         status=status)

    def revise(self, goal, token=None, status=200, key=None, **overrides):
        body = {"expected_revision": goal["revision"], "reason": "Replan"}
        body.update(overrides)
        return self.call("POST", f"goals/{goal['id']}:revise", body, token, key=key,
                         status=status)

    def report(self, task, agent, run, token, outcome="failed", status=200):
        return self.call("POST", f"tasks/{task['id']}:dispatch-outcome", {
            "expected_revision": task["revision"], "agent_id": agent["id"],
            "run_id": run["id"], "outcome": outcome, "summary": "Attempt finished"},
            token, status=status)

    def task(self, task_id):
        return self.call("GET", f"tasks/{task_id}")


@pytest.fixture
def world(tmp_path):
    import json
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, installed = build(tmp_path)
    with TestClient(app) as client:
        harness = Harness(app, client, config)
        harness.installed = installed
        yield harness


def test_install_metadata_and_descriptors_are_plain(world):
    assert set(world.installed["endpoints"]) == {
        "POST /v1/goals", "GET /v1/goals", "GET /v1/goals/{goal_id}",
        "POST /v1/goals/{goal_id}:revise", "POST /v1/goals/{goal_id}:tick",
        "POST /v1/goals/{goal_id}:propose"}
    assert world.installed["models"] == list(goal_orchestration.MODELS)
    fields = {"name", "purpose", "input", "output", "authority", "side_effects",
              "limits", "retry_safety"}
    for name, descriptor in OPERATION_DESCRIPTORS.items():
        assert descriptor["name"] == name
        assert fields <= set(descriptor)
    assert set(goal_orchestration.descriptors()) == set(OPERATION_DESCRIPTORS)


def test_supplied_steps_form_canonical_dag_and_ready_dispatch(world):
    scope = world.scope("website/header")
    created = world.goal(steps=[
        world.step("header", scope),
        world.step("nav", scope, depends_on=["header"]),
        world.step("footer", depends_on=["header"])])
    view = created["goal"]
    assert created["id"] == view["goal"]["id"]
    assert view["state"] == "active"
    assert view["plan"] and view["plan"]["id"] == view["goal"]["current_plan_id"]
    # Canonical artifacts: a real task_plan with real tasks and dependency mapping.
    tasks = {task["plan_key"]: task for task in view["tasks"]}
    assert set(tasks) == {"header", "nav", "footer"}
    assert tasks["nav"]["depends_on"] == [tasks["header"]["id"]]
    assert len(view["tasks"]) <= 64
    ready = {task["plan_key"] for task in view["tasks"]
             if task["id"] in view["ready"]}
    assert ready == {"header"}
    # Ready work dispatches through canonical scheduling: exact claims + execution.
    agent, token, run = world.started()
    result = world.tick(created["id"])
    assert result["dispatched"] == [{"task_id": tasks["header"]["id"],
                                     "run_id": run["id"], "agent_id": agent["id"],
                                     "execution_id": result["dispatched"][0]
                                     ["execution_id"]}]
    assert result["dispatched"][0]["execution_id"]
    claims = world.call("GET", f"work-claims?project_id={world.project}")
    assert [claim["task_id"] for claim in claims] == [tasks["header"]["id"]]
    assert claims[0]["scope_ids"] == [scope["id"]]
    assert claims[0]["mode"] == "exclusive_owner"
    # The run is now busy and the other tasks are dependency-blocked: nothing moves.
    again = world.tick(created["id"])
    assert again["dispatched"] == [] and again["skipped"] == []
    held = {item["task_id"] for item in again["held"]}
    assert tasks["header"]["id"] in held  # unknown-active: never reassigned
    # Completing the dependency unblocks the next rank.
    task = world.task(tasks["header"]["id"])
    world.report(task, agent, run, token, outcome="succeeded")
    after = world.tick(created["id"])
    assert after["dispatched"][0]["task_id"] in {tasks["nav"]["id"],
                                                 tasks["footer"]["id"]}


def test_extractive_decomposition_is_deterministic_and_bounded(world):
    listed = world.goal(statement="Ship the site:\n- build header\n- build footer\n"
                                  "1. wire navigation")
    tasks = listed["goal"]["tasks"]
    assert [task["title"] for task in tasks] == ["build header", "build footer",
                                                 "wire navigation"]
    assert all(not task["depends_on"] for task in tasks)
    plain = world.goal(statement="Ship the website end to end")
    assert len(plain["goal"]["tasks"]) == 1
    assert plain["goal"]["goal"]["decomposition"]["mode"] == "extractive"
    too_many = "\n".join(f"- item {index}" for index in range(65))
    world.call("POST", "goals", {"project_id": world.project, "title": "Too big",
                                 "statement": too_many}, status=422)
    with pytest.raises(Problem, match="goal_step_limit_exceeded"):
        extract_steps(too_many)
    with pytest.raises(Problem, match="goal_decomposition_empty"):
        extract_steps("   \n  ")


def test_failed_task_drives_bounded_plan_revision(world):
    scope = world.scope("website/header")
    created = world.goal(steps=[world.step("header", scope),
                                world.step("nav", depends_on=["header"])])
    goal = created["goal"]["goal"]
    agent, token, run = world.started()
    dispatched = world.tick(created["id"])["dispatched"][0]
    task = world.task(dispatched["task_id"])
    world.report(task, agent, run, token, outcome="failed")
    # Revise with an explicit replacement plan; the old plan stays immutable.
    revised = world.revise(goal, steps=[world.step("header-v2", scope),
                                        world.step("nav", depends_on=["header-v2"])])
    assert revised["revised"] is True
    record = revised["revision"]
    assert record["revision"] == 2 and record["source"] == "supplied"
    assert record["previous_plan_id"] == goal["current_plan_id"]
    # Observed outcomes are canonical reads; the failure is a self-report, not proof.
    observed = {item["plan_key"]: item for item in record["observed_outcomes"]}
    assert observed["header"]["status"] == "queued"  # retry_scheduled after failure
    assert observed["header"]["self_reported_outcome"] == "failed"
    assert observed["header"]["externally_verified"] is False
    view = revised["goal"]
    assert view["goal"]["epoch"] == 2 and view["goal"]["plan_revision"] == 2
    assert {task["plan_key"] for task in view["tasks"]} == {"header-v2", "nav"}
    old_plan = world.call("GET", f"task-plans/{goal['current_plan_id']}")
    assert {task["plan_key"] for task in old_plan["tasks"]} == {"header", "nav"}
    # Revisions append immutably and stay bounded.
    current = view["goal"]
    for index in range(25):
        current = world.revise(
            current, steps=[world.step(f"only-{index}")])["goal"]["goal"]
    assert len(current["revisions"]) == goal_orchestration.REVISION_LIMIT
    assert current["revisions"][-1]["revision"] == current["plan_revision"]
    assert [r["revision"] for r in current["revisions"]] == \
        list(range(current["plan_revision"] - 19, current["plan_revision"] + 1))


def test_stale_revision_epoch_cycle_and_replay_are_definite(world):
    created = world.goal(steps=[world.step("only")])
    goal = created["goal"]["goal"]
    # Stale store revision -> 412.
    world.revise(goal, steps=[world.step("next")], status=412, expected_revision=99)
    # Stale goal epoch -> 409.
    world.revise(goal, steps=[world.step("next")], status=409, expected_epoch=99)
    world.tick(goal["id"], status=409, expected_epoch=99)
    # Cycles and dangling keys are rejected as invalid plans, never partially saved.
    for bad in ([{"key": "a", "title": "A", "depends_on": ["b"]},
                 {"key": "b", "title": "B", "depends_on": ["a"]}],
                [{"key": "a", "title": "A", "depends_on": ["ghost"]}]):
        response = world.request("POST", "goals", {"project_id": world.project,
                                                   "title": "Bad", "statement": "Bad",
                                                   "steps": bad})
        assert response.status_code == 422, response.text
        assert "invalid_plan_steps" in response.text or "depend" in response.text.lower()
    response = world.request("POST", f"goals/{goal['id']}:revise", {
        "expected_revision": goal["revision"], "reason": "cycle",
        "steps": [{"key": "a", "title": "A", "depends_on": ["a"]}]})
    assert response.status_code == 422
    # Idempotent replay: same key and body returns the same goal; a changed body
    # under the same key is a definite conflict.
    key = uid("key")
    body = {"project_id": world.project, "title": "Replay", "statement": "Replay me"}
    first = world.call("POST", "goals", body, key=key)
    second = world.call("POST", "goals", body, key=key)
    assert first["id"] == second["id"]
    world.call("POST", "goals", {**body, "title": "Changed"}, key=key, status=409)


def test_revocation_and_identity_are_enforced(world):
    created = world.goal(steps=[world.step("only")])
    goal = created["goal"]["goal"]
    # A plain agent principal cannot revise goals.
    agent, token, run = world.started()
    world.revise(goal, token=token, steps=[world.step("next")], status=403)
    # A tick by an agent dispatches only its own runs, never another agent's.
    other, other_token, other_run = world.started("Other")
    result = world.tick(goal["id"], token=token)
    assert {item["agent_id"] for item in result["dispatched"]} == {agent["id"]}
    # A revoked credential is rejected outright.
    with world.store.transaction():
        world.store.db.execute("DELETE FROM principals WHERE token_hash=?",
                               (hash_token(other_token),))
    world.tick(goal["id"], token=other_token, status=401)
    # Ticking with a bogus coordinator epoch is fenced off.
    world.tick(goal["id"], coordinator_epoch=4242, status=409)


def test_unknown_active_executions_hold_revision_instead_of_reassign(world):
    scope = world.scope("website/header")
    created = world.goal(steps=[world.step("header", scope),
                                world.step("nav")])
    goal = created["goal"]["goal"]
    agent, token, run = world.started()
    dispatched = world.tick(created["id"])["dispatched"][0]
    held_response = world.revise(goal, steps=[world.step("replacement")], status=409)
    assert held_response["revised"] is False
    assert held_response["hold"]["reason"] == "unknown_active_executions"
    assert held_response["hold"]["task_ids"] == [dispatched["task_id"]]
    # The goal is durably held; the unknown work kept its agent and run.
    view = world.view(created["id"])
    assert view["state"] == "blocked"
    assert view["held"] == [dispatched["task_id"]]
    task = world.task(dispatched["task_id"])
    assert task["scheduling"]["assigned_agent_id"] == agent["id"]
    assert task["execution"]["run_id"] == run["id"]
    # Once the canonical outcome lands, revision proceeds and clears the hold.
    world.report(task, agent, run, token, outcome="succeeded")
    revised = world.revise(view["goal"], steps=[world.step("nav-only")])
    assert revised["revised"] is True
    assert revised["goal"]["goal"]["hold"] is None
    assert revised["goal"]["state"] == "active"


def test_completed_goal_carryover_and_terminal_state(world):
    created = world.goal(steps=[world.step("only")])
    agent, token, run = world.started()
    world.tick(created["id"])
    task = world.task(world.view(created["id"])["tasks"][0]["id"])
    world.report(task, agent, run, token, outcome="succeeded")
    view = world.view(created["id"])
    assert view["state"] == "completed"
    world.revise(view["goal"], status=409)  # carryover of nothing: goal_already_complete


def test_provider_two_phase_intent_is_durable_and_single_use(tmp_path):
    import json
    calls = []

    def provider(summary, *, limit):
        calls.append(summary["id"])
        return [{"key": "phase-1", "title": "Phase one"},
                "phase two from a plain string"]

    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, plan_provider=provider)
    with TestClient(app) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("seed")])
        goal = created["goal"]["goal"]
        key = uid("key")
        proposed = world.call("POST", f"goals/{goal['id']}:propose", {}, key=key)
        assert calls == [goal["id"]]
        intent = proposed["intent"]
        assert intent["goal_id"] == goal["id"] and intent["consumed"] is False
        assert intent["state"] == "done" and intent["digest"]
        assert intent["goal_revision"] == goal["revision"]
        assert intent["goal_epoch"] == goal["epoch"]
        assert intent["idempotency_key"] == key
        # Replays re-authorize and return the durable result: no provider re-invoke.
        replayed = world.call("POST", f"goals/{goal['id']}:propose", {}, key=key)
        assert replayed["intent"]["id"] == intent["id"]
        assert calls == [goal["id"]]
        # The durable intent applies once, under epoch validation.
        revised = world.revise(goal, intent_id=intent["id"])
        assert revised["revised"] is True
        assert revised["revision"]["source"] == "provider"
        tasks = {task["plan_key"] for task in revised["goal"]["tasks"]}
        assert tasks == {"phase-1", "step-2"}
        world.revise(revised["goal"]["goal"], intent_id=intent["id"], status=409)


def test_provider_reservation_never_reinvokes_and_pins_source(tmp_path):
    import json
    calls = []

    def crashing(summary, *, limit):
        calls.append(summary["id"])
        raise RuntimeError("provider died mid-compute")

    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, plan_provider=crashing)
    with TestClient(app, raise_server_exceptions=False) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("seed")])
        goal = created["goal"]["goal"]
        key = uid("key")
        assert world.request("POST", f"goals/{goal['id']}:propose", {},
                             key=key).status_code == 500
        assert calls == [goal["id"]]
        # The orphaned reservation is held: replays never re-invoke the provider.
        response = world.request("POST", f"goals/{goal['id']}:propose", {}, key=key)
        assert response.status_code == 409
        assert "plan_intent_held" in response.text
        assert calls == [goal["id"]]
        intents = [row for row in world.store.db.execute(
            "SELECT body FROM resources WHERE kind='goal_plan_intent'").fetchall()]
        assert len(intents) == 1
        assert json.loads(intents[0]["body"])["state"] == "reserved"
        # A held reservation can never be applied as a plan source.
        intent_id = json.loads(intents[0]["body"])["id"]
        world.revise(goal, intent_id=intent_id, status=409)

    # A goal that moves while the provider computes fails the commit definitely.
    calls.clear()

    def sneaky(summary, *, limit):
        calls.append(summary["id"])
        with world2.store.transaction():
            row = world2.store.raw(goal2["id"], "goal")
            body = json.loads(row["body"])
            body["epoch"] = body["epoch"] + 1
            world2.store.save("goal", row["project"], row["owner"], body,
                              expected=row["revision"])
        return [{"key": "late", "title": "Late"}]

    app, _ = build(tmp_path, plan_provider=sneaky)
    with TestClient(app) as client:
        world2 = Harness(app, client, config)
        created = world2.goal(steps=[world2.step("seed")])
        goal2 = created["goal"]["goal"]
        key = uid("key")
        response = world2.request("POST", f"goals/{goal2['id']}:propose", {}, key=key)
        assert response.status_code == 409, response.text
        assert "plan_intent_source_changed" in response.text
        assert calls == [goal2["id"]]
        # The stale proposal is never rebound or re-computed: replays of the same
        # reservation hold definitely and the provider invocation count stays 1.
        replayed = world2.request("POST", f"goals/{goal2['id']}:propose", {}, key=key)
        assert replayed.status_code == 409
        assert "plan_intent_held" in replayed.text
        assert calls == [goal2["id"]]


def test_provider_is_optional_and_validated_definitely(tmp_path):
    import json
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path)  # no provider configured
    with TestClient(app) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("seed")])
        world.call("POST", f"goals/{created['id']}:propose", {}, status=422)

    def garbage(summary, *, limit):
        return [{"title": "missing key"}, {"key": "ok", "title": "Ok", "bogus": 1}]

    app, _ = build(tmp_path, plan_provider=garbage)
    with TestClient(app) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("seed")])
        world.call("POST", f"goals/{created['id']}:propose", {}, status=422)
        # Nothing half-applied: the durable reservation stays held and no plan moved.
        admin = {"role": "admin", "project": world.project, "id": "x", "agent": None}
        intents = world.store.list("goal_plan_intent", world.project, admin, 100)
        assert [intent["state"] for intent in intents] == ["reserved"]
        assert world.view(created["id"])["goal"]["plan_revision"] == 1


def test_scheduled_tick_is_default_off_and_uses_fresh_operator(world):
    # Strict, explicit configuration: enabled requires an operator and projects.
    for invalid in ({"enabled": "true"}, {"enabled": True},
                    {"interval_seconds": float("nan")}, {"tick_limit": True}):
        with pytest.raises(ValueError):
            GoalSchedule(**invalid)
    orchestration = GoalOrchestration(world.service)
    runner = GoalRunner(world.service, orchestration, GoalSchedule())
    runner.scheduled_tick()  # disabled: no-op
    owner = world.store.db.execute(
        "SELECT * FROM principals WHERE role='admin'").fetchone()
    created = world.goal(steps=[world.step("only")])
    agent, token, run = world.started()
    runner = GoalRunner(world.service, orchestration, GoalSchedule(
        enabled=True, operator_principal_id=owner["id"], project_ids=[world.project],
        interval_seconds=1))
    runner.scheduled_tick()
    task = world.view(created["id"])["tasks"][0]
    assert task["status"] == "in_progress" or task.get("execution")
    # Interval gating: a second immediate tick does nothing new.
    runner.scheduled_tick()
    # A missing operator fails definitely rather than acting under stale authority.
    with world.store.transaction():
        world.store.db.execute("DELETE FROM principals WHERE id=?", (owner["id"],))
    runner.last = None
    with pytest.raises(Problem, match="configured_operator_unavailable"):
        runner.scheduled_tick()


def test_gate_hooks_are_injected_by_the_parent(tmp_path):
    import json
    seen = {}

    def budgets(principal, project_id):
        seen["budgets"] = project_id
        return {"allowed": False, "reason": "budget exhausted"}

    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, hooks={"budgets": budgets})
    with TestClient(app) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("only")])
        world.started()
        result = world.tick(created["id"])
        assert result["dispatched"] == []
        assert result["budget_denied"] == {"allowed": False,
                                           "reason": "budget exhausted"}
        assert seen["budgets"] == world.project

    def routing(principal, task, run):
        return {"action": "deny", "reason": "policy"}

    app, _ = build(tmp_path, hooks={"routing": routing})
    with TestClient(app) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("only")])
        world.started()
        result = world.tick(created["id"])
        assert result["dispatched"] == []
        assert result["skipped"] == [{"task_id": result["goal"]["tasks"][0]["id"],
                                      "reason": "no_authorized_run"}]

    artifacts_calls = []

    def artifacts(principal, goal, record):
        artifacts_calls.append((goal["id"], record["revision"]))

    app, _ = build(tmp_path, hooks={"artifacts": artifacts})
    with TestClient(app) as client:
        world = Harness(app, client, config)
        created = world.goal(steps=[world.step("only")])
        revised = world.revise(created["goal"]["goal"], steps=[world.step("next")])
        assert artifacts_calls == [(created["id"], revised["revision"]["revision"])]


def test_coordinator_fenced_revision(world):
    created = world.goal(steps=[world.step("only")])
    goal = created["goal"]["goal"]
    coordinator, coordinator_token, run = world.started("Nexus")
    with world.store.transaction():
        world.store.db.execute("UPDATE principals SET coordinator=1 WHERE agent=?",
                               (coordinator["id"],))
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": coordinator["id"], "run_id": run["id"],
                        "requested_ttl_seconds": 60}, token=coordinator_token)
    revised = world.revise(goal, token=coordinator_token,
                           steps=[world.step("coordinated")],
                           coordinator_epoch=lease["coordinator_epoch"])
    assert revised["revised"] is True
    assert revised["revision"]["by"] != goal["created_by"]


def test_tick_never_dispatches_another_goals_work(world):
    first = world.goal(title="Goal one", steps=[world.step("one")])
    second = world.goal(title="Goal two", steps=[world.step("two")])
    agent, token, run = world.started()
    result = world.tick(first["id"])
    assert [item["task_id"] for item in result["dispatched"]] == \
        [first["goal"]["tasks"][0]["id"]]
    # The other goal's eligible task was never a candidate for this tick.
    assert world.view(second["id"])["tasks"][0]["status"] == "queued"


def test_gate_blocked_ready_task_never_falls_back_to_wrong_goal(tmp_path):
    import json

    def routing(principal, task, run):
        return {"action": "defer", "reason": "not now"}

    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, hooks={"routing": routing})
    with TestClient(app) as client:
        world = Harness(app, client, config)
        first = world.goal(title="Goal one", steps=[world.step("one")])
        world.goal(title="Goal two", steps=[world.step("two")])
        world.started()
        result = world.tick(first["id"])
        assert result["dispatched"] == []
        # Exactly nothing was claimed for anyone.
        assert world.call("GET", f"work-claims?project_id={world.project}") == []


def test_revision_supersedes_old_queued_tasks_without_touching_evidence(world):
    scope = world.scope("website/header")
    created = world.goal(steps=[world.step("header", scope),
                                world.step("nav", depends_on=["header"])])
    agent, token, run = world.started()
    dispatched = world.tick(created["id"])["dispatched"][0]
    task = world.task(dispatched["task_id"])
    world.report(task, agent, run, token, outcome="succeeded")
    view = world.view(created["id"])
    old_nav = next(t for t in view["tasks"] if t["plan_key"] == "nav")
    revised = world.revise(view["goal"], steps=[world.step("nav-v2")])
    record = revised["revision"]
    assert record["superseded_task_ids"] == [old_nav["id"]]
    superseded = world.task(old_nav["id"])
    assert superseded["superseded"]["goal_epoch"] == revised["goal"]["goal"]["epoch"]
    assert superseded["superseded"]["plan_revision"] == record["revision"]
    assert superseded["scheduling"]["escalated"] is True
    assert superseded["scheduling"]["state"] == "superseded"
    # Regular worker dispatch can never pick up the obsolete queued task.
    agent2, token2, run2 = world.started("Second")
    plain = world.call("POST", f"projects/{world.project}/tasks:dispatch",
                       {"agent_id": agent2["id"], "run_id": run2["id"]}, token2)
    assert plain["task"]["plan_key"] == "nav-v2"
    # Completion evidence on the old plan is preserved verbatim.
    old_header = world.task(dispatched["task_id"])
    assert old_header["status"] == "completed" and old_header["execution"]["id"]


def test_stale_heartbeat_alone_never_releases_unknown_active_hold(world):
    import time as clock
    created = world.goal(steps=[world.step("only")])
    goal = created["goal"]["goal"]
    agent, token, run = world.started()
    world.tick(created["id"])
    # Stale the run's heartbeat far beyond the staleness window: still not quiescent.
    with world.store.transaction():
        row = world.store.raw(run["id"], "run")
        import json
        body = json.loads(row["body"])
        body["last_seen_at"] = clock.time() - 3600
        world.store.save("run", row["project"], row["owner"], body,
                         expected=row["revision"])
    held = world.revise(goal, steps=[world.step("replacement")], status=409)
    assert held["revised"] is False
    assert held["hold"]["reason"] == "unknown_active_executions"
    task = world.task(held["hold"]["task_ids"][0])
    assert task["scheduling"]["assigned_agent_id"] == agent["id"]


def test_declared_goal_scopes_bound_generated_and_supplied_steps(world):
    allowed = world.scope("website/header")
    other = world.scope("website/footer")
    # Extractive steps inherit the declared, authorized goal scopes.
    created = world.goal(statement="- build header\n- build footer",
                         scope_ids=[allowed["id"]])
    assert all(task["scope_ids"] == [allowed["id"]] for task in created["goal"]["tasks"])
    # Supplied steps outside the declared goal scopes are rejected.
    world.call("POST", "goals", {"project_id": world.project, "title": "Escape",
                                 "statement": "Escape", "scope_ids": [allowed["id"]],
                                 "steps": [world.step("x", other)]}, status=422)
    # Undeclared goal scopes: per-step scopes are the explicit contract.
    explicit = world.goal(steps=[world.step("x", other)])
    assert explicit["goal"]["goal"]["scope_ids"] == []
    assert explicit["goal"]["tasks"][0]["scope_ids"] == [other["id"]]


def test_historical_revision_sources_are_reauthorized_at_read(world):
    created = world.goal(steps=[world.step("old"), world.step("keep")])
    goal = created["goal"]["goal"]
    old_task = created["goal"]["tasks"][0]["id"]
    revised = world.revise(goal, steps=[world.step("new")])
    assert len(revised["revision"]["observed_outcomes"]) == 2
    # Delete a source task: derived history must not leak its cached record.
    with world.store.transaction():
        world.store.db.execute("UPDATE resources SET deleted=1 WHERE id=?",
                               (old_task,))
    view = world.view(created["id"])
    record = view["goal"]["revisions"][-1]
    assert {entry["task_id"] for entry in record["observed_outcomes"]} == \
        {next(t["id"] for t in created["goal"]["tasks"] if t["plan_key"] == "keep")}
    listed = world.call("GET", f"goals?project_id={world.project}")
    assert {entry["task_id"]
            for entry in listed[0]["revisions"][-1]["observed_outcomes"]} == \
        {entry["task_id"] for entry in record["observed_outcomes"]}


def test_strict_models_reject_ambiguity(world):
    with pytest.raises(ValueError):
        GoalRevise(expected_revision=1, reason="both",
                   statement="x", steps=[{"key": "a", "title": "A"}])
    with pytest.raises(ValueError):
        GoalRevise(expected_revision=1, reason="empty", steps=[])
    with pytest.raises(ValueError):
        GoalTick(dispatch="yes")
