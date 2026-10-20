"""Governed blocked-task resolution: explanations, privacy, durable requests.

The module under test is installed explicitly on the world harness (the real
API app), alongside sibling features when a test needs them (artifact
contracts, task budgets). All drives go through the HTTP surface or the
canonical store; no paid calls, no network, no host changes.
"""

import json
import time

import pytest
from fastapi import Request

from hivemind import artifact_contracts as ac
from hivemind import blocked_tasks as bt
from hivemind import task_budgets as tb
from hivemind.agent_feature_gates import set_feature_gates
from hivemind.api import envelope, hash_token
from hivemind.artifact_contracts import ContractPolicy
from hivemind.blocked_tasks import BlockedTasksPolicy
from hivemind.models import Model
from hivemind.storage import Problem, uid
from hivemind.task_budgets import BudgetPolicy, TaskBudget


def install_on(world, policy=None, *, budgets=None, contracts=None):
    """Install blocked_tasks (and optional sibling features) on the live app,
    mirroring hivemind.api wiring."""
    app, service = world.app, world.app.state.service
    store = service.store

    def principal(request: Request):
        authorization = request.headers.get("authorization", "")
        if not authorization.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        row = store.db.execute(
            "SELECT * FROM principals WHERE token_hash=?",
            (hash_token(authorization[7:]),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = request.model_dump(exclude_none=True) if isinstance(request, Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    if contracts is not None:
        ac.install(app, service, principal, command, envelope, policy=contracts)
    if budgets is not None:
        tb.install(app, service, principal, command, envelope, policy=budgets)
    return bt.install(app, service, principal, command, envelope, policy=policy)


@pytest.fixture
def explainer(world):
    return install_on(world)


@pytest.fixture
def requests_on(world):
    return install_on(world, BlockedTasksPolicy(requests_enabled=True))


def make_task(world, title="Queued work", **overrides):
    return world.call("POST", "tasks", {"project_id": world.project, "title": title, **overrides})


def planned(world, steps):
    view = world.call("POST", "task-plans",
                      {"project_id": world.project, "title": "Plan", "tasks": steps})
    return {task["plan_key"]: task for task in view["tasks"]}


def blocked_status(world, task, token=None, status=200, project=None, **query):
    suffix = ("?" + "&".join(f"{key}={value}" for key, value in query.items())) if query else ""
    return world.call(
        "GET", f"projects/{project or world.project}/tasks/{task['id']}/blocked-status{suffix}",
        token=token, status=status)


def edit_task(world, task, **changes):
    store = world.app.state.store
    body = store.get(task["id"], {"role": "admin"})
    body.update(changes)
    with store.transaction():
        row = store.raw(task["id"], "task")
        return store.save("task", row["project"], row["owner"], body, expected=row["revision"])


def other_project(world):
    store = world.app.state.store
    with store.transaction():
        store.db.execute("INSERT OR IGNORE INTO projects(id, title) VALUES(?,?)",
                         ("project_other", "Other"))
    return "project_other"


def file_request(world, task, token=None, key=None, action="dependency_review",
                 status=200, **overrides):
    body = {"task_id": task["id"], "expected_task_revision": task["revision"],
            "action": action, "note": "Please review", **overrides}
    return world.call("POST", f"projects/{world.project}/blocked-tasks:request",
                      body, token, key=key, status=status)


def start_task(world, task, agent, token, run, scope):
    world.claim(agent, run, scope, token)
    return world.call("POST", f"tasks/{task['id']}:start",
                      {"expected_revision": task["revision"], "run_id": run["id"]}, token)


def complete_started(world, active, run, token):
    return world.call("POST", f"tasks/{active['id']}:checkpoint", {
        "expected_revision": active["revision"], "run_id": run["id"],
        "execution_id": active["execution"]["id"], "checkpoint_id": uid("checkpoint"),
        "status": "completed", "progress": 100, "summary": "Done"}, token)


def test_read_only_default_ready_task(world, explainer):
    task = make_task(world)
    result = blocked_status(world, task)
    assert result["ready"] is True and result["readiness"] == "startable"
    assert result["blockers"] == [] and result["suggested_actions"] == []
    assert result["authority"] == "explanation_snapshot_not_execution_permission"
    assert result["task_revision"] == task["revision"]
    # Requests are an explicit opt-in: no routes exist under the default policy.
    world.call("POST", f"projects/{world.project}/blocked-tasks:request",
               {"task_id": task["id"], "expected_task_revision": task["revision"],
                "action": "operator_attention"}, status=404)


def test_status_fencing_exact_project_and_ids(world, explainer):
    task = make_task(world)
    # The path project fences the task: the same task under another project
    # path is not found.
    blocked_status(world, task, project=other_project(world), status=404)
    # Caller-supplied ids are bounded and fenced even without a run.
    blocked_status(world, task, agent_id="x" * 129, status=422)
    blocked_status(world, task, run_id="x" * 129, status=422)
    blocked_status(world, task, agent_id="agent_missing", status=404)
    # An agent from another project is not leaked through the endpoint.
    agent = world.agent("Outsider")
    store = world.app.state.store
    with store.transaction():
        row = store.raw(agent["id"], "agent")
        body = json.loads(row["body"])
        store.save("agent", "project_other", row["owner"],
                   {**body, "project_id": "project_other"}, expected=row["revision"])
    blocked_status(world, task, agent_id=agent["id"], status=404)


def test_running_and_terminal_are_never_ready(world, explainer):
    scope = world.scope(f"tasks/running/{uid('scope')}")
    task = make_task(world, scope_ids=[scope["id"]])
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    active = start_task(world, task, agent, token, run, scope)
    result = blocked_status(world, active, token)
    assert result["ready"] is False and result["readiness"] == "already_running"
    done = complete_started(world, active, run, token)
    result = blocked_status(world, done, token)
    assert result["ready"] is False and result["readiness"] == "terminal"
    assert result["blockers"] == []


def test_dependency_incomplete_and_hidden_dependency(world, explainer):
    tasks = planned(world, [{"key": "a", "title": "First"},
                            {"key": "b", "title": "Second", "depends_on": ["a"]}])
    result = blocked_status(world, tasks["b"])
    assert result["ready"] is False and result["readiness"] == "blocked"
    incomplete = [b for b in result["blockers"] if b["code"] == "dependency_incomplete"]
    assert incomplete and incomplete[0]["dependency_id"] == tasks["a"]["id"]
    assert incomplete[0]["action"] == "dependency_review"
    assert incomplete[0]["provenance"] == "canonical_snapshot"

    # A dependency this principal cannot see collapses to the generic blocker:
    # its identity is never disclosed. Make it private to another agent and
    # query as an unrelated (non-admin) requester.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         ("agent_someone_else", tasks["a"]["id"]))
    requester = world.agent("Requester")
    result = blocked_status(world, tasks["b"], world.credential(requester))
    generics = [b for b in result["blockers"] if b["code"] == "blocker_unavailable"]
    assert generics and all("dependency_id" not in b for b in generics)
    assert tasks["a"]["id"] not in json.dumps(result)


def test_cross_project_task_is_not_found(world, explainer):
    task = make_task(world)
    other_project(world)
    agent = world.agent("Outsider")
    token = world.credential(agent, project="project_other")
    blocked_status(world, task, token=token, status=404)


def test_attempt_budget_and_backoff_blockers(world, explainer):
    task = make_task(world)
    settings = {"state": "queued", "attempts": 5, "dispatches": 5, "reassignments": 0,
                "assigned_agent_id": None, "last_assigned_agent_id": None,
                "last_dispatch_run_id": None, "last_dispatch_at": None,
                "retry_after": time.time() + 3600, "escalated": True, "escalation": None,
                "last_outcome": None, "last_reconcile": None, "attempt_history": []}
    task = edit_task(world, task, scheduling=settings)
    result = blocked_status(world, task)
    codes = {b["code"] for b in result["blockers"]}
    assert {"attempt_budget_exhausted", "retry_backoff", "escalated_hold"} <= codes


def test_capability_mismatch_with_run(world, explainer):
    tasks = planned(world, [{"key": "a", "title": "Needs SQL",
                             "required_capabilities": ["sql"]}])
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    result = blocked_status(world, tasks["a"], token, run_id=run["id"], agent_id=agent["id"])
    assert any(b["code"] == "capability_mismatch" and b["action"] == "operator_attention"
               for b in result["blockers"])
    # A run id bound to another agent is refused, not leaked through the endpoint.
    other = world.agent("Other")
    blocked_status(world, tasks["a"], world.credential(other),
                   run_id=run["id"], agent_id=agent["id"], status=403)


def test_assignment_fence_without_routing(world, explainer):
    # The assignment fence applies from canonical scheduling metadata even when
    # no routing feature is installed.
    tasks = planned(world, [{"key": "a", "title": "Assigned"}])
    task = edit_task(world, tasks["a"], scheduling={
        "state": "queued", "attempts": 0, "dispatches": 0, "reassignments": 0,
        "assigned_agent_id": "agent_someone_else", "last_assigned_agent_id": None,
        "last_dispatch_run_id": None, "last_dispatch_at": None, "retry_after": None,
        "escalated": False, "escalation": None, "last_outcome": None,
        "last_reconcile": None, "attempt_history": []})
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    result = blocked_status(world, task, token, run_id=run["id"])
    assert any(b["code"] == "assignment_mismatch" for b in result["blockers"])


def test_scope_permit_and_claim_conflict(world, explainer):
    scope = world.scope("website/blocked")
    task = make_task(world, scope_ids=[scope["id"]])
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    # No claim: the permit fence blocks with a permission remediation.
    result = blocked_status(world, task, token, run_id=run["id"])
    assert any(b["code"] == "scope_not_owned" and b["action"] == "permission_review"
               for b in result["blockers"])
    # A conflicting exclusive claim by ANOTHER run blocks; the holder identity
    # is never disclosed.
    holder = world.agent("Holder")
    holder_token = world.credential(holder)
    holder_run = world.run(holder, holder_token)
    world.claim(holder, holder_run, scope, holder_token)
    result = blocked_status(world, task, token, run_id=run["id"])
    assert any(b["code"] == "scope_claimed" for b in result["blockers"])
    assert holder_run["id"] not in json.dumps(result)
    assert holder["id"] not in json.dumps(result)


def test_private_claim_holds_generically(world, explainer):
    scope = world.scope(f"website/private/{uid('scope')}")
    task = make_task(world, scope_ids=[scope["id"]])
    holder = world.agent("Holder")
    holder_token = world.credential(holder)
    holder_run = world.run(holder, holder_token)
    claim = world.claim(holder, holder_run, scope, holder_token)
    # The claim becomes private to the holder: the raw authoritative snapshot
    # still holds the task, generically, for any other principal.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (holder["id"], claim["id"]))
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    result = blocked_status(world, task, token, run_id=run["id"])
    assert any(b["code"] == "scope_conflict_unavailable"
               and b["action"] == "permission_review" for b in result["blockers"])
    text = json.dumps(result)
    assert claim["id"] not in text and holder_run["id"] not in text
    assert holder["id"] not in text


def test_pending_control_holds_run(world, explainer):
    task = make_task(world)
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    store = world.app.state.store
    with store.transaction():
        store.save("control", world.project, "test", {
            "id": uid("control"), "project_id": world.project,
            "target_run_id": run["id"], "state": "queued",
            "expires_at": time.time() + 600, "action": "pause",
            "coordinator_epoch": 1})
    result = blocked_status(world, task, token, run_id=run["id"])
    assert any(b["code"] == "control_hold" for b in result["blockers"])


def test_private_control_holds_generically(world, explainer):
    task = make_task(world)
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    store = world.app.state.store
    with store.transaction():
        row = store.save("control", world.project, "test", {
            "id": uid("control"), "project_id": world.project,
            "target_run_id": run["id"], "state": "queued",
            "expires_at": time.time() + 600, "action": "pause",
            "coordinator_epoch": 1})
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         ("agent_someone_else", row["id"]))
    result = blocked_status(world, task, token, run_id=run["id"])
    # The hidden control still holds; its existence is generic, never its id.
    assert any(b["code"] == "control_state_unavailable" for b in result["blockers"])
    assert row["id"] not in json.dumps(result)


def test_unverifiable_prior_execution_is_generic(world, explainer):
    scope = world.scope(f"tasks/prior/{uid('scope')}")
    task = make_task(world, scope_ids=[scope["id"]])
    producer = world.agent("Producer")
    producer_token = world.credential(producer)
    run = world.run(producer, producer_token)
    active = start_task(world, task, producer, producer_token, run, scope)
    # Requeue the task with a live execution, then make the run private.
    queued = edit_task(world, active, status="queued")
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (producer["id"], run["id"]))
    requester = world.agent("Requester")
    result = blocked_status(world, queued, world.credential(requester))
    assert result["ready"] is False
    assert any(b["code"] == "blocker_unavailable" and b["source"] == "execution"
               for b in result["blockers"])
    assert result["execution"]["run_redacted"] is True
    assert run["id"] not in json.dumps(result)


def test_malformed_and_raising_gates_fail_closed(world, explainer):
    service = world.app.state.service
    set_feature_gates(service, "broken_dependency",
                      dependency=lambda p, task, dep: {"action": "explode"})
    set_feature_gates(service, "raising_dispatch",
                      dispatch=lambda p, task, run: (_ for _ in ()).throw(RuntimeError("x")))
    tasks = planned(world, [{"key": "a", "title": "First"},
                            {"key": "b", "title": "Second", "depends_on": ["a"]}])
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    result = blocked_status(world, tasks["b"], token, run_id=run["id"])
    assert result["ready"] is False
    sources = {b["source"] for b in result["blockers"] if b["code"] == "blocker_unavailable"}
    assert {"dependency_gates", "dispatch_gates"} <= sources
    try:
        set_feature_gates(service, "broken_dependency")
        set_feature_gates(service, "raising_dispatch")
    except Problem:
        pass


def test_budget_blockers(world):
    directory = str(world.app.state.store.directory) + "-budgets"
    install_on(world, budgets=BudgetPolicy(enabled=True, directory=directory))
    task = make_task(world)
    # Enabled budgets with no configured budget hold the task.
    result = blocked_status(world, task)
    assert any(b["code"] == "budget_not_configured" and b["action"] == "budget_review"
               for b in result["blockers"])
    budget = TaskBudget(task_id=task["id"], project_id=world.project,
                        model_id="local-test", counter="exact",
                        token_budget=1000, seconds_budget=600.0, cost_budget=0.0,
                        enforce_cost=False, currency="USD", output_reserve=16)
    ledger = world.app.state.task_budgets
    ledger.set_budget(budget)
    # An unknown (unsettled/untrusted) hold blocks until authorized resolution.
    with ledger.lock:
        ledger.db.execute(
            "INSERT INTO reservations(key, task_id, run_id, execution_id, turn_key, state,"
            " reserved_tokens, reserved_cost, currency, cost_known, wall_start,"
            " expires_at_wall, boot_nonce, elapsed_seconds, accounted_tokens,"
            " accounted_cost, accounted_unknown, actual, prompt_fingerprint,"
            " frozen_budget, frozen_prices)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (uid("reservation"), task["id"], "run_x", "execution_x", "turn_x", "settled",
             10, None, "USD", 0, "2026-01-01T00:00:00Z", time.time() + 600,
             ledger.boot_nonce, 0.0, 10, None, 1, None, "fingerprint", "{}", None))
    result = blocked_status(world, task)
    assert any(b["code"] == "budget_settlement_unknown" for b in result["blockers"])
    assert result["ready"] is False


def test_zero_budgets_are_exhausted(world):
    directory = str(world.app.state.store.directory) + "-budgets"
    install_on(world, budgets=BudgetPolicy(enabled=True, directory=directory))
    ledger = world.app.state.task_budgets
    token_task = make_task(world, title="Zero token budget")
    ledger.set_budget(TaskBudget(
        task_id=token_task["id"], project_id=world.project, model_id="m", counter="exact",
        token_budget=0, seconds_budget=600.0, cost_budget=0.0, enforce_cost=False,
        currency="USD", output_reserve=16))
    result = blocked_status(world, token_task)
    # A zero token budget can never fit the mandatory output reserve floor.
    assert any(b["code"] == "token_budget_insufficient" for b in result["blockers"])
    cost_task = make_task(world, title="Zero enforced cost budget")
    ledger.set_budget(TaskBudget(
        task_id=cost_task["id"], project_id=world.project, model_id="m", counter="exact",
        token_budget=1000, seconds_budget=600.0, cost_budget=0.0, enforce_cost=True,
        currency="USD", output_reserve=16))
    result = blocked_status(world, cost_task)
    assert any(b["code"] == "cost_budget_insufficient" for b in result["blockers"])
    assert result["ready"] is False


def test_artifact_hidden_applicability_is_generic(world):
    install_on(world, contracts=ContractPolicy(enabled=True))
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope(f"tasks/hidden/{uid('scope')}")
    dependency = make_task(world, scope_ids=[scope["id"]])
    active = start_task(world, dependency, agent, token, run, scope)
    done = complete_started(world, active, run, token)
    contract = world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": done["id"], "title": "Deliverable",
        "outputs": [{"name": "delivx", "type": "text", "media_type": "text/plain"}]})
    dependent = make_task(world)
    edit_task(world, dependent, depends_on=[done["id"]])
    requester = world.agent("Requester")
    requester_token = world.credential(requester)
    # Visible applicable contract: the artifact need is explained.
    result = blocked_status(world, dependent, requester_token)
    assert any(b["code"] == "artifact_missing" and b["action"] == "artifact_needed"
               for b in result["blockers"])
    # A hidden applicable contract still holds, but collapses to the generic
    # blocker: no contract id, title, or output names are disclosed.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         ("someone-else", contract["id"]))
    result = blocked_status(world, dependent, requester_token)
    assert any(b["code"] == "blocker_unavailable" for b in result["blockers"])
    text = json.dumps(result)
    assert contract["id"] not in text and "delivx" not in text
    assert not any(b["code"] == "artifact_missing" for b in result["blockers"])


def test_dependency_cycle_independent_of_contracts(world, explainer):
    first, second = make_task(world, title="One"), make_task(world, title="Two")
    store = world.app.state.store
    first = edit_task(world, first, depends_on=[second["id"]])
    with store.transaction():
        store.relate(world.project, first["id"], second["id"], "DEPENDS_ON")
        store.relate(world.project, second["id"], first["id"], "DEPENDS_ON")
    result = blocked_status(world, first)
    assert any(b["code"] == "dependency_cycle" and b["action"] == "dependency_review"
               for b in result["blockers"])
    # Over-bound dependency lists fail closed with a generic blocker.
    many = [make_task(world, title=f"Dep {index}")["id"] for index in range(66)]
    bloated = edit_task(world, make_task(world, title="Bloated"), depends_on=many)
    result = blocked_status(world, bloated)
    assert any(b["code"] == "blocker_unavailable" for b in result["blockers"])


def test_dependency_cycle_with_contracts(world):
    install_on(world, contracts=ContractPolicy(enabled=True))
    first, second = make_task(world, title="One"), make_task(world, title="Two")
    store = world.app.state.store
    first = edit_task(world, first, depends_on=[second["id"]])
    with store.transaction():
        store.relate(world.project, first["id"], second["id"], "DEPENDS_ON")
        store.relate(world.project, second["id"], first["id"], "DEPENDS_ON")
    result = blocked_status(world, first)
    assert any(b["code"] == "dependency_cycle" and b["action"] == "dependency_review"
               for b in result["blockers"])


def test_recovery_source_hook_reason_classes(world):
    install_on(world, policy=BlockedTasksPolicy(
        recovery_source=lambda service, principal, task:
            {"blocked": True, "reason_class": "effects_unknown"}))
    result = blocked_status(world, make_task(world))
    blocker = next(b for b in result["blockers"] if b["source"] == "recovery_source")
    assert blocker["code"] == "effects_unknown"
    assert blocker["action"] == "recovery_review"
    assert blocker["provenance"] == "source_hook"
    # Malformed hook output collapses to the generic blocker.
    install_on(world, policy=BlockedTasksPolicy(
        recovery_source=lambda service, principal, task: {"blocked": True,
                                                          "reason_class": "nonsense"}))
    result = blocked_status(world, make_task(world))
    assert any(b["code"] == "blocker_unavailable" and b["provenance"] == "source_hook"
               for b in result["blockers"])
    # A raising hook fails closed, never a 500.
    def raising(service, principal, task):
        raise RuntimeError("hook exploded")
    install_on(world, policy=BlockedTasksPolicy(recovery_source=raising))
    result = blocked_status(world, make_task(world))
    assert any(b["code"] == "blocker_unavailable" and b["provenance"] == "source_hook"
               for b in result["blockers"])


def test_request_lifecycle_and_replay(world, requests_on):
    tasks = planned(world, [{"key": "a", "title": "First"},
                            {"key": "b", "title": "Second", "depends_on": ["a"]}])
    dependent = tasks["b"]
    agent = world.agent("Requester")
    token = world.credential(agent)
    key = uid("key")
    created = file_request(world, dependent, token, key=key)
    assert created["state"] == "open" and created["action"] == "dependency_review"
    assert created["request_fingerprint"] and created["source_fingerprint"]
    # Replay with the same key returns the durable receipt, not a new record.
    replay = file_request(world, dependent, token, key=key)
    assert replay["id"] == created["id"]
    assert len(world.call("GET", f"projects/{world.project}/blocked-task-requests")) == 1
    # Source revision drift refuses: the task moved since the caller looked.
    file_request(world, {**dependent, "revision": dependent["revision"] + 9},
                 token, status=412)
    # Acknowledgment requires an operator or the responsible actor.
    world.call("POST", f"blocked-task-requests/{created['id']}:acknowledge",
               {"note": "looking"}, token, status=403)
    acknowledged = world.call("POST", f"blocked-task-requests/{created['id']}:acknowledge",
                              {"note": "looking"})
    assert acknowledged["state"] == "acknowledged"
    assert acknowledged["acknowledged_by"] is not None
    # Acknowledgment is not resolution: the blocker still exists.
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    # Actual resolution evidence: the dependency is really completed now.
    scope = world.scope(f"tasks/resolve/{uid('scope')}")
    producer = world.agent("Producer")
    producer_token = world.credential(producer)
    run = world.run(producer, producer_token)
    dependency = edit_task(world, tasks["a"], scope_ids=[scope["id"]])
    active = start_task(world, dependency, producer, producer_token, run, scope)
    complete_started(world, active, run, producer_token)
    resolved = world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
                          {"outcome": "resolved"})
    assert resolved["state"] == "resolved"
    # A different outcome on a closed request conflicts; same-outcome replay is a receipt.
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "declined"}, status=409)
    again = world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
                       {"outcome": "resolved"})
    assert again["id"] == created["id"] and again["state"] == "resolved"


def test_arbitrary_evidence_cannot_resolve(world, requests_on):
    tasks = planned(world, [{"key": "a", "title": "First"},
                            {"key": "b", "title": "Second", "depends_on": ["a"]}])
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    created = file_request(world, tasks["b"], action="artifact_needed",
                           run_id=run["id"], agent_id=agent["id"])
    assert created["candidate"] == {"run_id": run["id"], "agent_id": agent["id"]}
    artifact = world.call("POST", "artifacts", {
        "project_id": world.project, "title": "Manual deliverable",
        "content": {"media_type": "text/plain", "text": "produced out of band"},
        "run_id": run["id"]}, token)
    # No arbitrary evidence override exists: unknown extra fields are rejected
    # and a note is never proof while canonical blockers remain.
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "resolved",
                "evidence_refs": [{"kind": "artifact", "id": artifact["id"]}]}, status=422)
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "resolved", "note": "trust me, fixed"}, status=409)
    # Acknowledgment plus operator intent is still not proof.
    world.call("POST", f"blocked-task-requests/{created['id']}:acknowledge", {"note": "on it"})
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    # The dependency really completing clears the requested condition; the
    # bound candidate is re-authorized and re-evaluated at close.
    scope = world.scope(f"tasks/proof/{uid('scope')}")
    dependency = edit_task(world, tasks["a"], scope_ids=[scope["id"]])
    active = start_task(world, dependency, agent, token, run, scope)
    complete_started(world, active, run, token)
    resolved = world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
                          {"outcome": "resolved"})
    assert resolved["state"] == "resolved"


def test_artifact_condition_requires_candidate_proof(world):
    install_on(world, BlockedTasksPolicy(requests_enabled=True),
               contracts=ContractPolicy(enabled=True, revocation_releases=True))
    producer = world.agent("Producer")
    producer_token = world.credential(producer)
    producer_run = world.run(producer, producer_token)
    scope = world.scope(f"tasks/candidate/{uid('scope')}")
    dependency = make_task(world, scope_ids=[scope["id"]])
    active = start_task(world, dependency, producer, producer_token, producer_run, scope)
    done = complete_started(world, active, producer_run, producer_token)
    contract = world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": done["id"], "title": "Deliverable",
        "outputs": [{"name": "delivx", "type": "text", "media_type": "text/plain"}]})
    dependent = make_task(world)
    dependent = edit_task(world, dependent, depends_on=[done["id"]])
    worker = world.agent("Worker")
    worker_token = world.credential(worker)
    worker_run = world.run(worker, worker_token)
    # The artifact blocker is visible to the candidate run.
    result = blocked_status(world, dependent, worker_token, run_id=worker_run["id"])
    assert any(b["code"] == "artifact_missing" for b in result["blockers"])
    unscoped = file_request(world, dependent, worker_token, action="artifact_needed")
    scoped = file_request(world, dependent, worker_token, action="artifact_needed",
                          run_id=worker_run["id"], agent_id=worker["id"])
    # While the contract holds, even the candidate-scoped request cannot resolve.
    world.call("POST", f"blocked-task-requests/{scoped['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    # Authoritative release clears the dependency gate; the candidate-scoped
    # action still requires the frozen candidate's exact proof.
    world.call("POST", f"artifact-contracts/{contract['id']}:revoke",
               {"expected_revision": contract["revision"]})
    world.call("POST", f"blocked-task-requests/{unscoped['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    resolved = world.call("POST", f"blocked-task-requests/{scoped['id']}:resolve",
                          {"outcome": "resolved"})
    assert resolved["state"] == "resolved"


def test_bound_candidate_readiness_and_closure_holds(world, requests_on):
    scope = world.scope(f"tasks/bound/{uid('scope')}")
    task = make_task(world, scope_ids=[scope["id"]])
    worker = world.agent("Worker")
    worker_token = world.credential(worker)
    worker_run = world.run(worker, worker_token)
    created = file_request(world, task, worker_token, action="permission_review",
                           run_id=worker_run["id"], agent_id=worker["id"])
    assert created["candidate"] == {"run_id": worker_run["id"], "agent_id": worker["id"]}
    # The permit fence still holds for the bound candidate: no closure.
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    # Granting the exact scopes to the bound run clears the requested condition.
    world.claim(worker, worker_run, scope, worker_token)
    resolved = world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
                          {"outcome": "resolved"})
    assert resolved["state"] == "resolved"

    # A hidden conflicting claim holds candidate-scope readiness and closure.
    task_two = make_task(world, title="Second", scope_ids=[scope["id"]])
    holder = world.agent("Holder")
    holder_token = world.credential(holder)
    holder_run = world.run(holder, holder_token)
    store = world.app.state.store
    with store.transaction():
        row = store.save("work_claim", world.project, "test", {
            "id": uid("claim"), "project_id": world.project, "agent_id": holder["id"],
            "run_id": holder_run["id"], "task_id": task_two["id"],
            "scope_ids": [scope["id"]], "mode": "exclusive_owner", "intent": "hold",
            "state": "active", "expires_at": time.time() + 600, "ownership_epoch": 1})
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (holder["id"], row["id"]))
    result = blocked_status(world, task_two, worker_token, run_id=worker_run["id"])
    assert any(b["code"] == "scope_conflict_unavailable" for b in result["blockers"])
    assert holder_run["id"] not in json.dumps(result)
    held = file_request(world, task_two, worker_token, action="permission_review",
                        run_id=worker_run["id"], agent_id=worker["id"])
    world.call("POST", f"blocked-task-requests/{held['id']}:resolve",
               {"outcome": "resolved"}, status=409)

    # A candidate that is gone at close time cannot prove anything.
    with store.transaction():
        store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (worker_run["id"],))
    response = world.call("POST", f"blocked-task-requests/{held['id']}:resolve",
                          {"outcome": "resolved"}, status=409)
    assert response["error"]["code"] == "resolution_candidate_unavailable"


def test_completed_task_never_clears_unknown_holds(world):
    directory = str(world.app.state.store.directory) + "-budgets"
    install_on(world, BlockedTasksPolicy(
        requests_enabled=True,
        recovery_source=lambda service, principal, task:
            {"blocked": True, "reason_class": "effects_unknown"}),
        budgets=BudgetPolicy(enabled=True, directory=directory))
    ledger = world.app.state.task_budgets
    task = make_task(world)
    ledger.set_budget(TaskBudget(
        task_id=task["id"], project_id=world.project, model_id="m", counter="exact",
        token_budget=1000, seconds_budget=600.0, cost_budget=0.0, enforce_cost=False,
        currency="USD", output_reserve=16))
    with ledger.lock:
        ledger.db.execute(
            "INSERT INTO reservations(key, task_id, run_id, execution_id, turn_key, state,"
            " reserved_tokens, reserved_cost, currency, cost_known, wall_start,"
            " expires_at_wall, boot_nonce, elapsed_seconds, accounted_tokens,"
            " accounted_cost, accounted_unknown, actual, prompt_fingerprint,"
            " frozen_budget, frozen_prices)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (uid("reservation"), task["id"], "run_x", "execution_x", "turn_x", "settled",
             10, None, "USD", 0, "2026-01-01T00:00:00Z", time.time() + 600,
             ledger.boot_nonce, 0.0, 10, None, 1, None, "fingerprint", "{}", None))
    # Terminal completion metadata is NOT proof that the unknown budget hold or
    # the unknown-effects report cleared.
    task = edit_task(world, task, status="completed")
    budget_request = file_request(world, task, action="budget_review")
    world.call("POST", f"blocked-task-requests/{budget_request['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    recovery_request = file_request(world, task, action="recovery_review")
    world.call("POST", f"blocked-task-requests/{recovery_request['id']}:resolve",
               {"outcome": "resolved"}, status=409)
    # The explicitly-not-fixed outcomes remain available.
    declined = world.call("POST", f"blocked-task-requests/{budget_request['id']}:resolve",
                          {"outcome": "declined", "note": "hold accepted as-is"})
    assert declined["state"] == "declined"


@pytest.mark.parametrize("action", [
    "artifact_needed", "permission_review", "recovery_review", "operator_attention"])
def test_terminal_metadata_cannot_prove_candidate_conditions(world, requests_on, action):
    task = edit_task(world, make_task(world), status="completed")
    request = file_request(world, task, action=action)
    world.call("POST", f"blocked-task-requests/{request['id']}:resolve",
               {"outcome": "resolved"}, status=409)


def test_gates_evaluated_truthful(world, explainer):
    service = world.app.state.service
    set_feature_gates(service, "probe", dispatch=lambda p, task, run: None)
    task = make_task(world)
    # Without a candidate run no dispatch/permit/claim phase executes, and the
    # response says so — the registry contents are never claimed as evaluated.
    result = blocked_status(world, task)
    assert result["readiness_scope"] == "task_only"
    assert result["run_checks_evaluated"] is False
    assert "dispatch_gates" not in result["gates_evaluated"]
    assert "permit_fence" not in result["gates_evaluated"]
    assert "scheduling" in result["gates_evaluated"]
    assert "dependency_graph" in result["gates_evaluated"]
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    result = blocked_status(world, task, token, run_id=run["id"], agent_id=agent["id"])
    assert result["readiness_scope"] == "candidate"
    assert result["run_checks_evaluated"] is True
    assert {"dispatch_gates", "permit_fence", "claims", "controls"} <= \
        set(result["gates_evaluated"])
    # Terminal tasks evaluate no gates at all.
    done = edit_task(world, task, status="completed")
    result = blocked_status(world, done)
    assert result["gates_evaluated"] == [] and result["run_checks_evaluated"] is False
    set_feature_gates(service, "probe")


def test_running_task_cannot_resolve(world, requests_on):
    scope = world.scope(f"tasks/inflight/{uid('scope')}")
    task = make_task(world, scope_ids=[scope["id"]])
    created = file_request(world, task, action="operator_attention")
    agent = world.agent("Worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    start_task(world, task, agent, token, run, scope)
    # A merely-started task is not readiness proof.
    world.call("POST", f"blocked-task-requests/{created['id']}:resolve",
               {"outcome": "resolved"}, status=409)


def test_decline_and_cancel_outcomes(world, requests_on):
    dependency = make_task(world, title="Blocked dep")
    dependent = make_task(world)
    dependent = edit_task(world, dependent, depends_on=[dependency["id"]])
    declined = file_request(world, dependent, action="operator_attention")
    declined = world.call("POST", f"blocked-task-requests/{declined['id']}:resolve",
                          {"outcome": "declined", "note": "won't fix"})
    assert declined["state"] == "declined"
    agent = world.agent("Requester")
    token = world.credential(agent)
    own = file_request(world, dependent, token)
    other = world.agent("Bystander")
    world.call("POST", f"blocked-task-requests/{own['id']}:resolve",
               {"outcome": "request_canceled"}, world.credential(other), status=403)
    canceled = world.call("POST", f"blocked-task-requests/{own['id']}:resolve",
                          {"outcome": "request_canceled"}, token)
    assert canceled["state"] == "request_canceled"


def test_request_action_allowlist(world, requests_on):
    task = make_task(world)
    file_request(world, task, action="run_shell_command", status=422)
    file_request(world, task, action="budget_increase", status=422)


def test_private_basis_rejected_and_withdrawn(world, requests_on):
    task = make_task(world)
    agent = world.agent("Requester")
    token = world.credential(agent)
    run = world.run(agent, token)
    artifact = world.call("POST", "artifacts", {
        "project_id": world.project, "title": "Context",
        "content": {"media_type": "text/plain", "text": "basis"},
        "run_id": run["id"]}, token)
    basis = [{"kind": "artifact", "id": artifact["id"], "revision": artifact["revision"]}]
    created = file_request(world, task, token, basis=basis)
    assert created["basis"] == basis and created["basis_redacted"] is False
    # Basis pinned to a stale revision is rejected.
    file_request(world, task, token,
                 basis=[{"kind": "artifact", "id": artifact["id"], "revision": 99}],
                 status=409)
    # Private basis across a namespace boundary is rejected outright.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         ("agent_someone_else", artifact["id"]))
    file_request(world, task, token,
                 basis=[{"kind": "artifact", "id": artifact["id"]}], status=404)
    # The previously filed request withdraws the now-invisible basis on every read.
    fetched = world.call("GET", f"blocked-task-requests/{created['id']}", token=token)
    assert fetched["basis"] == [] and fetched["basis_redacted"] is True
    assert artifact["id"] not in json.dumps(fetched)


def test_source_note_withdrawal_and_generic_receipt_projection(world, requests_on):
    task = make_task(world)
    agent = world.agent("Requester")
    token = world.credential(agent)
    run = world.run(agent, token)
    artifact = world.call("POST", "artifacts", {
        "project_id": world.project, "title": "Context",
        "content": {"media_type": "text/plain", "text": "basis"},
        "run_id": run["id"]}, token)
    key = uid("key")
    created = file_request(world, task, token, key=key,
                           basis=[{"kind": "artifact", "id": artifact["id"]}],
                           note="Source-derived context")
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         ("agent_someone_else", artifact["id"]))
    # Even an administrator's view must respect the original source principal.
    fetched = world.call("GET", f"blocked-task-requests/{created['id']}")
    assert fetched["note"] is None
    assert fetched["basis"] == []
    receipt = world.call("GET", f"commands/receipt?operation=blocked-tasks:request:"
                         f"{world.project}&key={key}", token=token)
    assert receipt["resource"]["note"] is None
    assert receipt["resource"]["basis"] == []


def test_private_basis_cannot_publish_into_shared_task_request(world, requests_on):
    task = make_task(world)
    agent = world.agent("Private requester")
    token = world.credential(agent)
    run = world.run(agent, token)
    artifact = world.call("POST", "artifacts", {
        "project_id": world.project, "title": "Private context",
        "content": {"media_type": "text/plain", "text": "basis"},
        "run_id": run["id"]}, token)
    with world.app.state.store.transaction():
        world.app.state.store.db.execute(
            "UPDATE resources SET private_agent=? WHERE id=?", (agent["id"], artifact["id"]))
    file_request(world, task, token, basis=[{"kind": "artifact", "id": artifact["id"]}],
                 status=403)


def test_cached_replay_re_redacts(world, requests_on):
    task = make_task(world)
    agent = world.agent("Requester")
    token = world.credential(agent)
    run = world.run(agent, token)
    artifact = world.call("POST", "artifacts", {
        "project_id": world.project, "title": "Context",
        "content": {"media_type": "text/plain", "text": "basis"},
        "run_id": run["id"]}, token)
    basis = [{"kind": "artifact", "id": artifact["id"]}]
    key = uid("key")
    created = file_request(world, task, token, key=key, basis=basis)
    assert created["basis"]
    # Replay is the durable receipt with the basis intact.
    replay = file_request(world, task, token, key=key, basis=basis)
    assert replay["id"] == created["id"] and replay["basis"] == basis
    # After the basis source is withdrawn, even the cached mutation receipt is
    # re-authorized and re-redacted against current state.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         ("agent_someone_else", artifact["id"]))
    replayed = file_request(world, task, token, key=key, basis=basis)
    assert replayed["id"] == created["id"]
    assert replayed["basis"] == [] and replayed["basis_redacted"] is True


def test_revocation_redacts_source_details(world, requests_on):
    task = make_task(world)
    agent = world.agent("Requester")
    token = world.credential(agent)
    created = file_request(world, task, token, note="sensitive context")
    fetched = world.call("GET", f"blocked-task-requests/{created['id']}")
    assert fetched["note"] == "sensitive context" and fetched["source_redacted"] is False
    # Revoke the requester: source-derived details disappear from every view.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("DELETE FROM principals WHERE agent=?", (agent["id"],))
    redacted = world.call("GET", f"blocked-task-requests/{created['id']}")
    assert redacted["note"] is None and redacted["basis"] == []
    assert redacted["source_redacted"] is True
    listed = world.call("GET", f"projects/{world.project}/blocked-task-requests")
    assert listed[0]["source_redacted"] is True and listed[0]["note"] is None
    # The revoked requester itself is unauthenticated.
    world.call("GET", f"blocked-task-requests/{created['id']}", token=token, status=401)


def test_concurrent_acknowledge_and_close_conflict(world, requests_on, monkeypatch):
    task = make_task(world)
    agent = world.agent("Requester")
    created = file_request(world, task, world.credential(agent))
    store = world.app.state.store
    original_save = store.save

    def interleaved_save(kind, project, owner, body, **kwargs):
        # A concurrent triage update lands between this request's read and its
        # CAS publish: the publish must conflict, never silently overwrite.
        if kind == "resolution_request" and kwargs.get("expected") is not None:
            fresh = store.raw(body["id"], "resolution_request")
            external = json.loads(fresh["body"])
            external["note"] = "concurrent edit"
            original_save("resolution_request", project, owner, external,
                          expected=fresh["revision"])
        return original_save(kind, project, owner, body, **kwargs)

    monkeypatch.setattr(store, "save", interleaved_save)
    world.call("POST", f"blocked-task-requests/{created['id']}:acknowledge",
               {"note": "race"}, status=412)
    monkeypatch.undo()
    # The conflict aborts the whole command transaction: no lost update, no
    # partial write; the original record is untouched and still open.
    fetched = world.call("GET", f"blocked-task-requests/{created['id']}")
    assert fetched["state"] == "open" and fetched["note"] == "Please review"
    # Two distinct operators cannot both acknowledge; the second conflicts.
    acknowledged = world.call("POST", f"blocked-task-requests/{created['id']}:acknowledge",
                              {"note": "first"})
    assert acknowledged["state"] == "acknowledged"
    second_operator = world.credential(role="human")
    world.call("POST", f"blocked-task-requests/{created['id']}:acknowledge",
               {"note": "second"}, second_operator, status=409)


def test_request_limit_is_bounded(world):
    install_on(world, BlockedTasksPolicy(requests_enabled=True, max_requests_per_task=2))
    task = make_task(world)
    file_request(world, task)
    file_request(world, task)
    file_request(world, task, status=429)


def test_descriptors_and_policy_validation():
    described = bt.describe_operations()
    assert described["version"] == bt.VERSION
    assert {op["name"] for op in described["descriptors"]} == {
        "blocked_tasks.explain", "blocked_tasks.request", "blocked_tasks.triage"}
    with pytest.raises(ValueError):
        BlockedTasksPolicy(requests_enabled="yes")
    with pytest.raises(ValueError):
        BlockedTasksPolicy(recovery_source="not-callable")
