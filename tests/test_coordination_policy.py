"""Coordination policy: bounded escalation/reassignment/dead-lettering, coordinator
lease fencing, declared-resource conflict handling, and the hardened evidence rules:
heartbeat age is never termination/no-effect evidence, acknowledgments are not
evidence, declarations freeze in flight, and resource keys are authorized.

The harness mirrors ``hivemind.api`` wiring (store, service, principal dependency,
command/envelope helpers) and installs both the scheduling module (the dispatch seam)
and the coordination-policy module under test against the real local store.
"""

import json
import secrets
import time

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from hivemind import coordination_policy as cp
from hivemind import models as m
from hivemind import scheduling
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.storage import uid


def build(directory, policy=None, evidence=None):
    app = create_app(directory, workers=False)
    service, store = app.state.service, app.state.store

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    async def principal(request: Request):
        from hivemind.storage import Problem
        auth = request.headers.get("authorization", "")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone() \
            if auth.startswith("Bearer ") else None
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) \
            if isinstance(request, m.Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    scheduling.install(app, service, principal, command, envelope)
    installed = cp.install(app, service, principal, command, envelope,
                           policy=policy, evidence=evidence)
    return app, installed


class Harness:
    def __init__(self, app, client, config):
        self.app, self.client = app, client
        self.project = config["project_id"]
        self.owner = config["token"]
        self.store = app.state.store
        self.service = app.state.service

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner),
            "Idempotency-Key": key or uid("key")})
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Worker", actions=None):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name,
                                            "supported_actions": actions or []})

    def credential(self, agent=None, role="agent", coordinator=False):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                                  (uid("principal"), hash_token(token), role, self.project,
                                   agent["id"] if agent else None, int(coordinator), 1))
        return token

    def run(self, agent, token):
        return self.call("POST", "runs", {"project_id": self.project, "agent_id": agent["id"]},
                         token)

    def started(self, name="Worker"):
        agent = self.agent(name)
        token = self.credential(agent)
        return agent, token, self.run(agent, token)

    def scope(self, key):
        return self.call("POST", "scopes", {"project_id": self.project, "canonical_key": key,
                                            "display_name": key, "allowed_paths": []})

    def plan(self, steps):
        return self.call("POST", "task-plans", {"project_id": self.project,
                                                "title": "Policy plan", "tasks": steps})

    def one_task(self):
        scope = self.scope("policy/" + uid("s"))
        plan = self.plan([{"key": "only", "title": "Only", "scope_ids": [scope["id"]]}])
        return plan["tasks"][0]

    def dispatch(self, agent, run, token, status=200, key=None):
        return self.call("POST", f"projects/{self.project}/tasks:dispatch",
                         {"agent_id": agent["id"], "run_id": run["id"]}, token,
                         key=key, status=status)

    def report(self, task, agent, run, token, outcome="failed", status=200, **overrides):
        body = {"expected_revision": task["revision"], "agent_id": agent["id"],
                "run_id": run["id"], "outcome": outcome, "summary": "done", **overrides}
        return self.call("POST", f"tasks/{task['id']}:dispatch-outcome", body, token,
                         status=status)

    def checkpoint(self, task, run, token, status="failed"):
        execution = task["execution"]
        return self.call("POST", f"tasks/{task['id']}:checkpoint", {
            "expected_revision": task["revision"], "run_id": run["id"],
            "execution_id": execution["id"], "checkpoint_id": uid("checkpoint"),
            "status": status, "progress": execution.get("progress", 0),
            "summary": "canonical checkpoint"}, token)

    def reconcile(self, task, action):
        return self.call("POST", f"tasks/{task['id']}:reconcile", {
            "expected_revision": task["revision"], "action": action, "reason": "recover"})

    def handoff(self, task, target_agent):
        return self.call("POST", f"tasks/{task['id']}:handoff", {
            "expected_revision": task["revision"], "target_agent_id": target_agent["id"],
            "reason": "operator handoff"})

    def set_policy(self, token=None, status=200, key=None, **policy):
        return self.call("PUT", f"projects/{self.project}/coordination-policy", policy,
                         token, key=key, status=status)

    def tick(self, token=None, status=200, key=None, **body):
        return self.call("POST", f"projects/{self.project}/coordination-policy:tick", body,
                         token, key=key, status=status)

    def declare(self, task, body, token=None, status=200):
        return self.call("PUT", f"tasks/{task['id']}/resource-intents",
                         {"expected_task_revision": task["revision"], **body}, token,
                         status=status)

    def resolve(self, action, body, token=None, status=200):
        return self.call("POST", f"coordination-actions/{action['id']}:resolve", body,
                         token, status=status)

    def get_task(self, task_id):
        return self.call("GET", f"tasks/{task_id}")

    def edit_task(self, task_id, mutate):
        row = self.store.raw(task_id, "task")
        body = json.loads(row["body"])
        mutate(body)
        with self.store.transaction():
            return self.store.save("task", row["project"], row["owner"], body,
                                   expected=row["revision"])

    def edit_run(self, run_id, mutate):
        row = self.store.raw(run_id, "run")
        body = json.loads(row["body"])
        mutate(body)
        with self.store.transaction():
            return self.store.save("run", row["project"], row["owner"], body,
                                   expected=row["revision"])

    def run_task_pointer(self, run_id):
        return json.loads(self.store.raw(run_id, "run")["body"]).get("task_id")

    def clear_retry(self, task_id):
        self.edit_task(task_id, lambda body: body["scheduling"].update(retry_after=None))

    def admin(self):
        return dict(self.store.db.execute(
            "SELECT * FROM principals WHERE role='admin'").fetchone())

    def stored(self, kind):
        return [json.loads(row["body"]) for row in self.store.db.execute(
            "SELECT body FROM resources WHERE kind=? AND deleted=0", (kind,)).fetchall()]

    def nexus(self, agent, run, token):
        return self.call("POST", f"projects/{self.project}/nexus:acquire",
                         {"agent_id": agent["id"], "run_id": run["id"],
                          "requested_ttl_seconds": 60}, token)


@pytest.fixture
def world(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, installed = build(tmp_path)
    assert len(installed["endpoints"]) == 8
    assert installed["models"] == list(cp.MODELS)
    with TestClient(app) as client:
        yield Harness(app, client, config)


# ---- models and pure conflict evaluation --------------------------------------


def test_policy_models_are_typed_bounded_and_strict():
    assert cp.PolicyUpdate().enabled is False
    assert cp.PolicyUpdate(enabled=True, max_escalations=3).max_escalations == 3
    with pytest.raises(ValidationError):
        cp.PolicyUpdate(max_escalations=cp.MAX_ESCALATIONS_LIMIT + 1)
    with pytest.raises(ValidationError):
        cp.PolicyUpdate(attempt_deadline_seconds=cp.MIN_DEADLINE_SECONDS - 1)
    with pytest.raises(ValidationError):
        cp.PolicyUpdate(max_reassignments=cp.MAX_REASSIGNMENTS_LIMIT + 1)
    with pytest.raises(ValidationError):
        cp.PolicyUpdate(surprise=True)
    with pytest.raises(ValidationError):
        cp.PolicyTick(limit=cp.TICK_MAX_LIMIT + 1)
    with pytest.raises(ValidationError):
        cp.PolicyTick(coordinator_epoch=0)
    with pytest.raises(ValidationError):
        cp.ResourceDeclaration(expected_task_revision=1, writes=["no prefix"])
    with pytest.raises(ValidationError):
        cp.ResourceDeclaration(expected_task_revision=1, writes=["unknown:x"])
    with pytest.raises(ValidationError):
        cp.ResourceDeclaration(expected_task_revision=1, reads=["scope:a"],
                               writes=["scope:a"])
    with pytest.raises(ValidationError):
        cp.ResourceDeclaration(expected_task_revision=1, reads=["path:a", "path:a"])
    with pytest.raises(ValidationError):
        cp.HoldResolution(expected_revision=1, action="release_claims", reason="x")


def test_resource_keys_are_normalized_and_paths_are_canonicalized():
    assert cp.normalize_resource_key(" PATH:./src//app/ ") == "path:src/app"
    assert cp.normalize_resource_key("interface:Users.API") == "interface:Users.API"
    with pytest.raises(ValueError):
        cp.normalize_resource_key("path:../escape")
    with pytest.raises(ValueError):
        cp.normalize_resource_key("path:a/../b")
    with pytest.raises(ValueError):
        cp.normalize_resource_key("path:has space")
    with pytest.raises(ValueError):
        cp.normalize_resource_key("scope:")


def test_declaration_conflicts_are_deterministic_and_read_read_is_safe():
    left = {"reads": ["interface:users"], "writes": ["artifact:report"]}
    right = {"reads": ["artifact:report"], "writes": []}
    assert cp.declaration_conflicts(left, right) == [
        {"kind": "write_read", "key": "artifact:report"}]
    assert cp.declaration_conflicts(right, left) == [
        {"kind": "write_read", "key": "artifact:report"}]
    both_read = {"reads": ["interface:users"]}
    assert cp.declaration_conflicts(both_read, dict(both_read)) == []
    writes = {"writes": ["interface:users"]}
    assert cp.declaration_conflicts(writes, dict(writes)) == [
        {"kind": "write_write", "key": "interface:users"}]
    # label: keys are non-authoritative hints and never conflict.
    labels = {"writes": ["label:area/users"]}
    assert cp.declaration_conflicts(labels, dict(labels)) == []


# ---- policy administration -----------------------------------------------------


def test_policy_set_requires_operator_and_replays_without_duplicates(world):
    agent, token, _ = world.started()
    world.set_policy(token, status=403, enabled=True)
    key = uid("key")
    first = world.set_policy(key=key, enabled=True, max_escalations=1,
                             auto_dead_letter=True, escalation_cooldown_seconds=0)
    replayed = world.set_policy(key=key, enabled=True, max_escalations=1,
                                auto_dead_letter=True, escalation_cooldown_seconds=0)
    assert first["id"] == replayed["id"] and replayed["revision"] == 1
    assert len(world.stored("coordination_policy")) == 1
    status = world.call("GET", f"projects/{world.project}/coordination-policy")
    assert status["policy"]["enabled"] is True and status["policy"]["max_escalations"] == 1
    conflict = world.set_policy(key=key, enabled=False, status=409)
    assert conflict["error"]["code"] == "idempotency_conflict"


def test_descriptors_endpoint_is_machine_readable(world):
    entries = world.call("GET", "coordination-policy/descriptors")
    names = {entry["name"] for entry in entries}
    assert {"coordination_policy.set", "coordination_policy.tick", "resources.declare",
            "coordination_action.resolve"} <= names
    for entry in entries:
        for field in ("purpose", "permissions", "evidence_required", "inputs", "outputs",
                      "limits", "retry_semantics", "endpoint", "method"):
            assert entry[field], (entry["name"], field)
    tick = next(entry for entry in entries if entry["name"] == "coordination_policy.tick")
    assert "heartbeat" in tick["purpose"]  # honest evidence wording is part of the contract


# ---- tick authorization --------------------------------------------------------


def test_tick_is_noop_until_policy_enabled(world):
    result = world.tick()
    assert result["evaluated"] == 0 and result["actions"] == []
    assert "disabled" in result["detail"]


def test_tick_requires_nexus_lease_fence_for_coordinators(world):
    world.set_policy(enabled=True)
    agent = world.agent("Coordinator")
    token = world.credential(agent, coordinator=True)
    run = world.run(agent, token)
    world.tick(token, status=422)  # coordinator without an epoch is refused
    world.tick(token, coordinator_epoch=1, status=409)  # no lease: epoch cannot fence
    lease = world.nexus(agent, run, token)
    world.tick(token, coordinator_epoch=lease["coordinator_epoch"] + 1, status=409)  # stale
    result = world.tick(token, coordinator_epoch=lease["coordinator_epoch"])
    assert result["fence"]["fenced_by"] == "nexus_lease"
    plain_agent, plain_token, _ = world.started("Plain")
    world.tick(plain_token, coordinator_epoch=lease["coordinator_epoch"], status=403)


# ---- heartbeat silence is ambiguity, never evidence ----------------------------


def _silent_policy(world, **overrides):
    world.set_policy(enabled=True, auto_reassign=True, max_reassignments=3,
                     escalation_cooldown_seconds=0, attempt_deadline_seconds=30,
                     auto_dead_letter=True, dead_letter_after_escalations=1, **overrides)


def test_stale_heartbeat_while_model_still_runs_holds_everything(world, monkeypatch):
    task = world.one_task()
    agent, token, run = world.started()
    _silent_policy(world)
    world.dispatch(agent, run, token)
    base = time.time()
    monkeypatch.setattr(cp, "wall_time", lambda: base + 400)  # heartbeat now looks stale
    result = world.tick()
    action = result["actions"][0]
    assert action["action"] == "manual_hold" and action["state"] == "manual_hold"
    assert action["reason"] == "execution_liveness_unknown"
    held = world.get_task(task["id"])
    assert held["status"] == "running" and held.get("execution") is not None
    assert held["scheduling"]["assigned_agent_id"] == agent["id"]
    assert world.run_task_pointer(run["id"]) == task["id"]  # nothing cleared or archived
    assert world.stored("dead_letter") == []


def test_terminal_run_is_still_ambiguous_without_authoritative_adjudication(world,
                                                                            monkeypatch):
    task = world.one_task()
    agent, token, run = world.started()
    _silent_policy(world)
    world.dispatch(agent, run, token)
    world.edit_run(run["id"], lambda body: body.update(status="failed"))  # interrupted
    base = time.time()
    monkeypatch.setattr(cp, "wall_time", lambda: base + 400)
    action = world.tick()["actions"][0]
    assert action["action"] == "manual_hold"
    held = world.get_task(task["id"])
    assert held.get("execution") is not None and held["status"] == "running"
    assert world.run_task_pointer(run["id"]) == task["id"]


def test_dispatched_state_without_execution_record_is_inconsistent_and_held(world):
    task = world.one_task()
    agent, token, run = world.started()
    _silent_policy(world)
    world.dispatch(agent, run, token)
    world.edit_task(task["id"], lambda body: body.pop("execution"))  # inconsistent state
    action = world.tick()["actions"][0]
    assert action["action"] == "manual_hold" and action["reason"] == "execution_record_missing"
    assert world.get_task(task["id"])["scheduling"]["assigned_agent_id"] == agent["id"]


def test_active_task_past_dead_letter_budget_is_held_not_terminalized(world, monkeypatch):
    task = world.one_task()
    agent, token, run = world.started()
    _silent_policy(world)  # auto_dead_letter with threshold 1
    world.dispatch(agent, run, token)

    def exhaust(body):
        body["scheduling"].update(escalated=True, state="escalated")
        body["scheduling"]["escalation"] = {"reason": "attempt_budget_exhausted",
                                            "attempts": 3, "at": "2026-01-01T00:00:00Z"}
        body["scheduling"]["policy"] = {"escalations": 2, "dead_lettered": False,
                                        "cooldown_until": None, "history": []}
    world.edit_task(task["id"], exhaust)
    base = time.time()
    monkeypatch.setattr(cp, "wall_time", lambda: base + 400)
    action = world.tick()["actions"][0]
    assert action["action"] == "manual_hold"
    assert action["reason"] == "escalated_execution_ambiguous"
    held = world.get_task(task["id"])
    assert held["status"] == "running" and held.get("execution") is not None
    assert held["scheduling"]["policy"]["escalations"] == 2
    assert world.stored("dead_letter") == []  # budget never terminalizes ambiguity


# ---- safe automatic actions -----------------------------------------------------


def test_never_started_assignment_is_safely_reassigned(world):
    task = world.one_task()
    agent_a, token_a, run_a = world.started("A")
    agent_b = world.agent("B")
    world.set_policy(enabled=True, auto_reassign=True, max_reassignments=3,
                     escalation_cooldown_seconds=0, attempt_deadline_seconds=30)
    world.dispatch(agent_a, run_a, token_a)
    # Canonical operator handoff archives the execution and leaves the task assigned
    # to B but never started: no execution record, status queued.
    world.edit_run(run_a["id"], lambda body: body.update(last_seen_at=time.time() - 120))
    handed = world.handoff(world.get_task(task["id"]), agent_b)
    assert handed["status"] == "queued" and handed.get("execution") is None
    assert handed["scheduling"]["assigned_agent_id"] == agent_b["id"]
    action = world.tick()["actions"][0]
    assert action["action"] == "reassign" and action["state"] == "applied"
    assert action["reason"] == "assignment_never_started"
    reassigned = world.get_task(task["id"])
    assert reassigned["scheduling"]["assigned_agent_id"] is None
    assert reassigned["scheduling"]["reassignments"] == 2  # handoff + policy
    claims = world.service.active_claims(world.admin(), world.project)
    assert any(claim["task_id"] == task["id"] and claim["state"] == "active"
               for claim in claims)  # claims are never stolen or released


def test_settled_execution_reassigns_with_binding_matched_proof(world):
    task = world.one_task()
    agent, token, run = world.started()
    world.set_policy(enabled=True, auto_reassign=True, max_reassignments=1,
                     escalation_cooldown_seconds=0)
    dispatched = world.dispatch(agent, run, token)["task"]
    # Canonical terminal checkpoint applied under the current task/run/execution.
    failed = world.checkpoint(world.get_task(task["id"]), run, token, status="failed")
    assert failed["status"] == "failed" and failed.get("execution") is not None
    action = world.tick()["actions"][0]
    assert action["action"] == "reassign" and action["state"] == "applied"
    assert action["evidence"]["adjudication"]["basis"] == "terminal_task_checkpoint"
    assert action["evidence"]["adjudication"]["execution_id"] == dispatched["execution"]["id"]
    reassigned = world.get_task(task["id"])
    assert reassigned["status"] == "queued"
    assert reassigned["scheduling"]["assigned_agent_id"] is None
    archived = reassigned["execution_history"][-1]
    assert archived["result"]["adjudication"]["basis"] == "terminal_task_checkpoint"


def test_evidence_provider_is_the_only_seam_that_settles_silent_executions(tmp_path,
                                                                           monkeypatch):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    calls = []

    def evidence(principal, task):
        calls.append(task["id"])
        return {"adjudication": "settled",
                "basis": {"recovery_record": "rec_1", "adjudicated_by": "recovery_automation"}}

    app, _ = build(tmp_path, evidence=evidence)
    with TestClient(app) as client:
        world = Harness(app, client, config)
        world.one_task()
        agent, token, run = world.started()
        _silent_policy(world)
        world.dispatch(agent, run, token)
        base = time.time()
        monkeypatch.setattr(cp, "wall_time", lambda: base + 400)
        action = world.tick()["actions"][0]
        assert action["action"] == "reassign" and action["state"] == "applied"
        assert action["evidence"]["adjudication"]["basis"] == "evidence_provider"
        assert calls  # the provider was consulted for the ambiguous execution


def test_provider_without_opinion_leaves_execution_held(tmp_path, monkeypatch):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, evidence=lambda principal, task: None)
    with TestClient(app) as client:
        world = Harness(app, client, config)
        world.one_task()
        agent, token, run = world.started()
        _silent_policy(world)
        world.dispatch(agent, run, token)
        base = time.time()
        monkeypatch.setattr(cp, "wall_time", lambda: base + 400)
        action = world.tick()["actions"][0]
        assert action["action"] == "manual_hold"


# ---- escalation and dead-letter lifecycle ----------------------------------------


def test_exhausted_task_dead_letters_with_envelope_preserved(world):
    task = world.one_task()
    agent, token, run = world.started()
    world.set_policy(enabled=True, max_escalations=0, dead_letter_after_escalations=1,
                     auto_dead_letter=True, escalation_cooldown_seconds=0)
    for _ in range(3):  # default max_attempts; skip the real backoff window deterministically
        dispatched = world.dispatch(agent, run, token)["task"]
        assert dispatched and dispatched["id"] == task["id"]
        reported = world.report(world.get_task(task["id"]), agent, run, token)
        if reported["scheduling"]["escalated"]:
            break
        world.clear_retry(task["id"])
    escalated = world.get_task(task["id"])
    assert escalated["status"] == "failed" and escalated["scheduling"]["escalated"] is True
    assert escalated.get("execution") is None  # archived by the canonical report seam

    action = world.tick()["actions"][0]
    assert action["action"] == "dead_letter" and action["state"] == "applied"
    letters = world.stored("dead_letter")
    assert len(letters) == 1 and letters[0]["task_id"] == task["id"]
    envelope = letters[0]["envelope"]
    assert envelope["id"] == task["id"] and envelope["scheduling"]["attempt_history"]
    assert envelope["status"] == "failed"  # terminal non-success; no invented success
    final = world.get_task(task["id"])
    assert final["status"] == "failed"
    assert final["scheduling"]["policy"]["dead_lettered"] is True
    second = world.tick()  # dead-lettered tasks are never reprocessed
    assert second["evaluated"] == 0 and len(world.stored("dead_letter")) == 1


def test_escalation_budget_is_bounded(world):
    task = world.one_task()
    agent, token, run = world.started()
    world.set_policy(enabled=True, max_escalations=1, escalation_cooldown_seconds=0)
    world.dispatch(agent, run, token)
    world.checkpoint(world.get_task(task["id"]), run, token, status="failed")  # settled
    first = world.tick()
    assert first["actions"][0]["action"] == "escalate"
    assert first["actions"][0]["state"] == "applied"
    escalated = world.get_task(task["id"])
    assert escalated["scheduling"]["escalated"] is True
    assert escalated.get("execution") is not None  # escalate never archives or clears
    # Canonically recover (operator seam), redispatch, and settle again.
    world.edit_run(run["id"], lambda body: body.update(last_seen_at=time.time() - 120))
    world.reconcile(escalated, "requeue")
    world.dispatch(agent, run, token)
    world.checkpoint(world.get_task(task["id"]), run, token, status="failed")
    second = world.tick()
    assert second["actions"][0]["state"] == "refused"
    assert second["actions"][0]["outcome"]["error"] == "policy_escalation_budget_exhausted"


def test_tick_replay_returns_cached_outcome_without_duplicate_actions(world, monkeypatch):
    world.one_task()
    agent, token, run = world.started()
    _silent_policy(world)
    world.dispatch(agent, run, token)
    base = time.time()
    monkeypatch.setattr(cp, "wall_time", lambda: base + 400)
    key = uid("key")
    first = world.tick(key=key)
    replayed = world.tick(key=key)
    assert first == replayed
    assert first["actions"][0]["action"] == "manual_hold"
    assert len(world.stored("coordination_action")) == 1


# ---- hold resolution --------------------------------------------------------------


def _ambiguous_hold(world, monkeypatch):
    task = world.one_task()
    agent, token, run = world.started()
    _silent_policy(world)
    world.dispatch(agent, run, token)
    base = time.time()
    monkeypatch.setattr(cp, "wall_time", lambda: base + 400)
    hold = world.tick()["actions"][0]
    assert hold["state"] == "manual_hold"
    return task, agent, token, run, hold


def test_acknowledgment_is_not_evidence_and_never_bypasses_settlement(world, monkeypatch):
    task, agent, token, run, hold = _ambiguous_hold(world, monkeypatch)
    held = world.call("GET", f"coordination-actions/{hold['id']}")
    for action in ("reassign", "dead_letter"):
        response = world.resolve(held, {"expected_revision": held["revision"],
                                        "action": action,
                                        "reason": "Operator is confident the run is gone",
                                        "acknowledge_unknown_effects": True}, status=409)
        assert response["error"]["code"] == "execution_not_settled"
    # Forged extra proof fields are rejected outright.
    world.call("POST", f"coordination-actions/{held['id']}:resolve",
               {"expected_revision": held["revision"], "action": "reassign",
                "reason": "x", "proof": {"settled": True}}, status=422)
    unchanged = world.get_task(task["id"])
    assert unchanged.get("execution") is not None
    assert unchanged["scheduling"]["assigned_agent_id"] == agent["id"]
    assert world.run_task_pointer(run["id"]) == task["id"]
    assert world.stored("dead_letter") == []


def test_operator_can_escalate_or_dismiss_without_altering_ownership(world, monkeypatch):
    task, agent, token, run, hold = _ambiguous_hold(world, monkeypatch)
    other_agent, other_token, _ = world.started("NotOperator")
    world.resolve(hold, {"expected_revision": 2, "action": "dismiss", "reason": "no"},
                  other_token, status=403)
    held = world.call("GET", f"coordination-actions/{hold['id']}")
    resolved = world.resolve(held, {"expected_revision": held["revision"],
                                    "action": "escalate", "reason": "needs review"})
    assert resolved["state"] == "resolved"
    escalated = world.get_task(task["id"])
    assert escalated["scheduling"]["escalated"] is True
    assert escalated.get("execution") is not None  # escalate never archives
    assert escalated["scheduling"]["assigned_agent_id"] == agent["id"]
    world.resolve(held, {"expected_revision": held["revision"] + 1, "action": "dismiss",
                         "reason": "late"}, status=422)  # already resolved


def test_hold_resolution_reassigns_once_execution_is_canonically_settled(world,
                                                                         monkeypatch):
    task, agent, token, run, hold = _ambiguous_hold(world, monkeypatch)
    # The canonical checkpoint seam settles the execution under its binding.
    world.checkpoint(world.get_task(task["id"]), run, token, status="failed")
    held = world.call("GET", f"coordination-actions/{hold['id']}")
    resolved = world.resolve(held, {"expected_revision": held["revision"],
                                    "action": "reassign",
                                    "reason": "Terminal checkpoint applied; hand to pool"})
    assert resolved["state"] == "resolved"
    assert resolved["outcome"]["resolution"] == "reassign"
    reassigned = world.get_task(task["id"])
    assert reassigned["scheduling"]["assigned_agent_id"] is None
    assert reassigned["execution_history"][-1]["result"]["adjudication"]["basis"] == \
        "terminal_task_checkpoint"


# ---- resource declarations ---------------------------------------------------------


def test_declarations_freeze_while_an_execution_exists(world):
    task = world.one_task()
    agent, token, run = world.started()
    world.declare(task, {"writes": ["interface:users"]})  # pre-dispatch: allowed
    world.dispatch(agent, run, token)
    current = world.get_task(task["id"])
    world.declare(current, {"writes": ["interface:users"], "reads": ["interface:orders"]},
                  status=409)  # cannot extend mid-flight
    world.declare(current, {"writes": []}, status=409)  # cannot erase writes mid-flight
    response = world.call("PUT", f"tasks/{task['id']}/resource-intents",
                          {"expected_task_revision": current["revision"], "writes": []},
                          status=409)
    assert response["error"]["code"] == "declaration_frozen"
    view = world.call("GET", f"tasks/{task['id']}/resource-intents")
    assert view["declaration"]["writes"] == ["interface:users"]  # unchanged


def test_spoofed_forged_and_malformed_declarations_are_rejected(world):
    task = world.one_task()
    agent, token, run = world.started()
    world.declare(task, {"writes": ["interface:users"]}, token, status=403)  # unassigned
    world.declare(task, {"writes": ["scope:scope_nonexistent"]}, status=404)
    world.declare(task, {"writes": ["artifact:artifact_nonexistent"]}, status=404)
    world.declare(task, {"dependencies": ["dependency:task_nonexistent"]}, status=404)
    world.call("PUT", f"tasks/{task['id']}/resource-intents",
               {"expected_task_revision": task["revision"], "writes": ["path:has space"]},
               status=422)
    declared = world.declare(task, {"writes": ["interface:users"]})
    assert declared["writes"] == ["interface:users"]
    view = world.call("GET", f"tasks/{task['id']}/resource-intents")
    assert view["declaration"]["declared_by"]
    world.declare(world.get_task(task["id"]), {"expected_task_revision": 99,
                                               "reads": ["interface:users"]}, status=412)


def test_cross_project_references_are_rejected(world):
    task = world.one_task()
    with world.store.transaction():  # a foreign project with its own scope and task
        world.store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)",
                               ("project_foreign", "Foreign"))
        admin = world.admin()
        world.store.save("scope", "project_foreign", admin["id"],
                         {"id": "scope_foreign", "project_id": "project_foreign",
                          "canonical_key": "foreign", "display_name": "foreign",
                          "allowed_paths": [], "created_at": "2026-01-01T00:00:00Z"})
        world.store.save("task", "project_foreign", admin["id"],
                         {"id": "task_foreign", "project_id": "project_foreign",
                          "title": "foreign", "status": "queued",
                          "created_at": "2026-01-01T00:00:00Z"})
    world.declare(task, {"writes": ["scope:scope_foreign"]}, status=404)
    world.declare(task, {"dependencies": ["dependency:task_foreign"]}, status=404)


def test_artifact_keys_are_authorized_against_real_artifacts(world):
    task = world.one_task()
    artifact = world.call("POST", "artifacts", {
        "project_id": world.project, "title": "report",
        "content": {"media_type": "text/plain", "text": "report"}})
    declared = world.declare(task, {"writes": [f"artifact:{artifact['id']}"]})
    assert declared["writes"] == [f"artifact:{artifact['id']}"]


# ---- declared-resource conflicts at dispatch ----------------------------------------


def test_read_read_dispatch_proceeds_and_write_read_defers_deterministically(world):
    first = world.one_task()
    plan = world.plan([{"key": "b", "title": "B", "scope_ids": [world.scope("policy/b")["id"]]}])
    second = plan["tasks"][0]
    agent_a, token_a, run_a = world.started("A")
    agent_b, token_b, run_b = world.started("B")
    world.declare(first, {"reads": ["interface:users"]}, token_a, status=403)  # unassigned
    world.declare(first, {"reads": ["interface:users"]})  # operator declares pre-dispatch
    world.declare(second, {"reads": ["interface:users"]})
    world.set_policy(enabled=True, conflict_cooldown_seconds=0)
    dispatched = world.dispatch(agent_a, run_a, token_a)["task"]
    assert dispatched["id"] == first["id"]
    allowed = world.dispatch(agent_b, run_b, token_b)
    assert allowed["task"]["id"] == second["id"]  # read/read is never a conflict

    # Free both runs, then exercise a writer in flight against a queued reader.
    world.report(world.get_task(first["id"]), agent_a, run_a, token_a, outcome="succeeded")
    world.report(world.get_task(second["id"]), agent_b, run_b, token_b, outcome="succeeded")
    plan = world.plan([{"key": "w", "title": "W", "scope_ids": [world.scope("policy/w")["id"]],
                        "priority": 10}])
    writer = plan["tasks"][0]
    world.declare(writer, {"writes": ["interface:users"]})
    plan = world.plan([{"key": "r", "title": "R", "scope_ids": [world.scope("policy/r")["id"]]}])
    reader = plan["tasks"][0]
    world.declare(reader, {"reads": ["interface:users"]})
    taken = world.dispatch(agent_a, run_a, token_a)["task"]
    assert taken["id"] == writer["id"]  # no peer in flight: writer starts
    blocked = world.dispatch(agent_b, run_b, token_b)
    assert blocked["task"] is None and blocked["deferral"]["reason"] == "resource_conflict"
    conflict = blocked["deferral"]["evidence"]["conflicts"][0]
    assert conflict["kind"] == "write_read" and conflict["key"] == "interface:users"
    reader_after = world.get_task(reader["id"])
    assert reader_after["scheduling"]["attempts"] == 0  # deferral consumes no attempt budget
    defers = [a for a in world.stored("coordination_action") if a["action"] == "defer"]
    assert len(defers) == 1 and defers[0]["state"] == "applied"


def test_strict_policy_defers_undeclared_effects_otherwise_allows(world):
    task = world.one_task()
    agent, token, run = world.started()
    world.set_policy(enabled=True, strict_resources=True, conflict_cooldown_seconds=0)
    blocked = world.dispatch(agent, run, token)
    assert blocked["task"] is None
    assert blocked["deferral"]["reason"] == "undeclared_effects"
    world.set_policy(enabled=True, strict_resources=False)
    assert world.dispatch(agent, run, token)["task"]["id"] == task["id"]
