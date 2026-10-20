"""Capability-based workload routing: declarations, ranking, gates, fencing.

These tests drive the routing domain and its ``install`` endpoints through a locally
built application (the same store, service, principal dependency, and envelope helper
that ``hivemind.api`` uses), with the scheduling module installed alongside it — the
same composition the real application wiring produces.
"""

import json
import secrets
import time

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import capability_routing, scheduling
from hivemind import models as m
from hivemind.api import create_app, hash_token
from hivemind.capability_routing import CapabilityRouting, RoutingPolicy
from hivemind.cli import initialize
from hivemind.storage import now, uid


def build(directory, policy=None):
    """Mirror hivemind.api wiring so install() can be exercised before api.py calls it."""
    app = create_app(directory, workers=False)
    service, store = app.state.service, app.state.store

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            from hivemind.storage import Problem
            raise Problem("authentication_required", 401)
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            from hivemind.storage import Problem
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) \
            if isinstance(request, m.Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    installed = {"scheduling": scheduling.install(app, service, principal, command, envelope),
                 "capability_routing": capability_routing.install(
                     app, service, principal, command, envelope, policy=policy)}
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

    def agent(self, name="Worker", actions=None, runtime="generic"):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name,
                                            "supported_actions": actions or [],
                                            "runtime": runtime})

    def credential(self, agent=None, role="agent", project=None, coordinator=False):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                                  (uid("principal"), hash_token(token), role,
                                   project or self.project, agent["id"] if agent else None,
                                   int(coordinator), 1))
        return token

    def update_run(self, run, **changes):
        body = self.store.get(run["id"], {"role": "admin"})
        body.update(changes)
        with self.store.transaction():
            self.store.db.execute("UPDATE resources SET body=? WHERE id=?",
                                  (json.dumps(body), run["id"]))
        return body

    def run(self, agent, token, worker=False):
        run = self.call("POST", "runs",
                        {"project_id": self.project, "agent_id": agent["id"]}, token)
        if worker:
            self.update_run(run, purpose="task_worker")
        return run

    def started(self, name="Worker", actions=None, runtime="generic"):
        agent = self.agent(name, actions, runtime)
        token = self.credential(agent)
        return agent, token, self.run(agent, token, worker=True)

    def plan(self, steps, title="Plan"):
        return self.call("POST", "task-plans",
                         {"project_id": self.project, "title": title, "tasks": steps})

    def declare(self, agent, token, runtime="factory", tools=("read", "edit"),
                strict_sandbox=False, status=200, **extra):
        return self.call("POST", f"agents/{agent['id']}/routing-declaration",
                         {"runtime": runtime, "tools": list(tools),
                          "strict_sandbox": strict_sandbox, **extra}, token, status=status)

    def query(self, task, token=None, candidates=None, status=200):
        return self.call("POST", f"projects/{self.project}/routing:query",
                         {"task_id": task["id"],
                          "candidate_agent_ids": candidates or []}, token, status=status)

    def dispatch(self, task, agent, run, token, status=200):
        result = self.call("POST", f"projects/{self.project}/tasks:dispatch",
                           {"agent_id": agent["id"], "run_id": run["id"]}, token, status=status)
        if status == 200:
            assert (result["task"] or {}).get("id") in (None, task["id"])
        return result

    def stale(self, run, age=120.0):
        self.update_run(run, last_seen_at=time.time() - age)

    def hold(self, agent, run, state="queued"):
        with self.store.transaction():
            self.store.save("control", self.project, "test", {
                "id": uid("control"), "created_at": now(), "project_id": self.project,
                "action": "pause", "target_agent_id": agent["id"],
                "target_run_id": run["id"], "state": state,
                "expires_at": time.time() + 600})

    def update_task_status(self, task, status):
        body = self.store.get(task["id"], {"role": "admin"})
        body["status"] = status
        with self.store.transaction():
            self.store.db.execute("UPDATE resources SET body=? WHERE id=?",
                                  (json.dumps(body), task["id"]))
        return self.store.get(task["id"], {"role": "admin"})

    def set_routing(self, task, routing):
        body = self.store.get(task["id"], {"role": "admin"})
        body["routing"] = routing
        with self.store.transaction():
            self.store.db.execute("UPDATE resources SET body=? WHERE id=?",
                                  (json.dumps(body), task["id"]))
        return self.store.get(task["id"], {"role": "admin"})

    def admin(self):
        return {"role": "admin", "project": self.project, "agent": None, "id": "admin"}

    def canonical(self, resource_id):
        return self.store.get(resource_id, {"role": "admin"})


@pytest.fixture
def world(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, RoutingPolicy(enabled=True))
    with TestClient(app) as client:
        yield Harness(app, client, config)


@pytest.fixture
def disabled_world(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path)  # No policy: default-off.
    with TestClient(app) as client:
        yield Harness(app, client, config)


# ---- policy -----------------------------------------------------------------

def test_policy_is_default_off_and_strictly_bounded():
    policy = RoutingPolicy()
    assert policy.enabled is False and policy.max_candidates == 32
    assert policy.require_declaration is True and policy.allow_busy_agents is False
    assert policy.require_worker_run is True and policy.sandbox_verifier is None
    for bad in ({"enabled": "yes"}, {"max_candidates": 0}, {"max_candidates": 65},
                {"max_candidates": True}, {"max_heartbeat_gap_seconds": 0.5},
                {"allow_busy_agents": 1}, {"require_worker_run": 1},
                {"allowed_runtimes": ("factory", "factory")},
                {"worker_run_ids": ("run_a", "run_a")}, {"worker_run_ids": ("",)},
                {"sandbox_verifier": "not-callable"}):
        with pytest.raises(ValueError):
            RoutingPolicy(**bad)
    with pytest.raises(ValueError):
        RoutingPolicy(allowed_runtimes=("https://evil.example/runtime",))
    assert "sandbox_verifier_configured" in RoutingPolicy(enabled=True).describe()


def test_disabled_policy_gates_query_and_dispatch_consistently(disabled_world):
    agent, token, run = disabled_world.started()
    assert "feature_disabled" in disabled_world.declare(agent, token, status=403)["error"]["code"]
    task = disabled_world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    # The advisory query is equally gated: no query without the operator policy.
    response = disabled_world.request("POST", f"projects/{disabled_world.project}/routing:query",
                                      {"task_id": task["id"], "candidate_agent_ids": []}, token)
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "feature_disabled"
    response = disabled_world.request(
        "POST", f"projects/{disabled_world.project}/routing:dispatch",
        {"task_id": task["id"], "agent_id": agent["id"], "run_id": run["id"], "reason": "try"},
        token)
    assert response.status_code == 403
    routing = CapabilityRouting(disabled_world.service, RoutingPolicy())
    gate = routing.dispatch_gate(disabled_world.admin(), disabled_world.canonical(task["id"]),
                                 disabled_world.canonical(run["id"]))
    assert gate == {**gate, "allowed": False, "reason": "feature_disabled"}
    assert routing.start_gate(disabled_world.admin(), disabled_world.canonical(task["id"]),
                              disabled_world.canonical(run["id"]))["action"] == "defer"


def test_install_reports_endpoints_and_models(tmp_path):
    owner = initialize(tmp_path)
    json.loads(owner.read_text())
    _, installed = build(tmp_path, RoutingPolicy(enabled=True))
    assert "POST /v1/projects/{project_id}/routing:query" in installed["capability_routing"]["endpoints"]
    assert set(installed["capability_routing"]["models"]) == set(capability_routing.MODELS)
    assert set(capability_routing.descriptors()) == {
        "capability_routing.query", "capability_routing.dispatch", "capability_routing.declare"}


# ---- declarations --------------------------------------------------------------

def test_declaration_requires_approved_registry_names_and_canonical_tools(world):
    agent, token, _ = world.started()
    assert world.declare(agent, token, runtime="import:evil.module",
                         status=422)["error"]["code"] == "unsupported_runtime"
    assert world.declare(agent, token, tools=["read", "rm_rf"],
                         status=422)["error"]["code"] == "unsupported_tool"
    # a2a's registry limits declare strict OS sandbox controls unsupported; even
    # advisory intent is meaningless there.
    assert world.declare(agent, token, runtime="a2a", strict_sandbox=True,
                         status=422)["error"]["code"] == "strict_sandbox_unsupported"
    saved = world.declare(agent, token, runtime="factory", tools=["read", "edit"])
    assert saved["runtime"] == "factory" and saved["agent_id"] == agent["id"]
    view = world.call("GET", f"agents/{agent['id']}/routing-declaration", token=token)
    assert view["effective"]["source"] == "declaration"
    assert view["effective"]["tools"] == ["read", "edit"]
    assert "factory" in view["approved_runtimes"]
    assert "advisory_only" in view


def test_declaration_requires_self_or_admin(world):
    agent, token, _ = world.started()
    other, other_token, _ = world.started("Other")
    response = world.request("POST", f"agents/{agent['id']}/routing-declaration",
                             {"runtime": "factory", "tools": [], "strict_sandbox": False},
                             other_token)
    assert response.status_code == 403
    assert world.declare(agent, world.owner, runtime="factory")["runtime"] == "factory"
    del token, other


def test_agent_metadata_is_an_authorized_declaration_source(world):
    agent, token, _ = world.started(runtime="factory")
    view = world.call("GET", f"agents/{agent['id']}/routing-declaration", token=token)
    assert view["declaration"] is None
    assert view["effective"]["source"] == "agent_metadata"
    assert view["effective"]["runtime"] == "factory"


def test_declaration_cas_and_in_flight_freeze(world):
    agent, token, run = world.started(runtime="factory")
    saved = world.declare(agent, token, runtime="factory", tools=["read"])
    # CAS: a stale expected revision conflicts.
    assert world.declare(agent, token, runtime="factory", tools=["read", "edit"],
                         expected_revision=saved["revision"] + 1,
                         status=412)["error"]["code"] == "revision_conflict"
    updated = world.declare(agent, token, runtime="factory", tools=["read", "edit"],
                            expected_revision=saved["revision"])
    assert updated["tools"] == ["read", "edit"]
    # In-flight: a bound live task freezes any declaration change.
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    world.dispatch(task, agent, run, token)
    assert world.declare(agent, token, runtime="oh_my_pi",
                         status=409)["error"]["code"] == "declaration_in_flight"
    assert world.declare(agent, token, runtime="factory", tools=["read"],
                         status=409)["error"]["code"] == "declaration_in_flight"
    # An identical re-declaration is a no-op and stays permitted.
    assert world.declare(agent, token, runtime="factory",
                         tools=["read", "edit"])["revision"] == updated["revision"] + 1


# ---- query / ranking -------------------------------------------------------------

def test_query_routes_capable_available_worker_run(world):
    agent, token, run = world.started("Capable", actions=["pause"], runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A", "required_capabilities": ["pause"]}])["tasks"][0]
    result = world.query(task)
    assert result["blocked"] is None and result["rejected"] == []
    assert [entry["agent_id"] for entry in result["eligible"]] == [agent["id"]]
    entry = result["eligible"][0]
    assert entry["run_id"] == run["id"] and entry["rank"] == 0
    assert entry["runtime"] == "factory" and entry["workload"] == 0
    assert result["authority"] == "advisory_query_grant_is_not_dispatch_authority"


def test_mismatched_capability_is_rejected_with_explicit_reason(world):
    agent, token, _ = world.started("Limited", actions=["pause"], runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A",
                        "required_capabilities": ["cancel"]}])["tasks"][0]
    result = world.query(task)
    assert result["eligible"] == []
    assert result["rejected"] == [{"agent_id": agent["id"], "reason": "capability_mismatch"}]


def test_undeclared_generic_agent_is_rejected(world):
    world.started("Generic")  # runtime "generic" is not an approved registry name.
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    result = world.query(task)
    assert result["eligible"] == []
    assert {item["reason"] for item in result["rejected"]} == {"declaration_missing"}


def test_observer_run_is_never_routable_work_capacity(world):
    agent, token, worker_run = world.started("Worker", runtime="factory")
    world.declare(agent, token)
    observer = world.run(agent, token)  # Newer, unbound observer run.
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    result = world.query(task)
    # The newer observer run does not win; the bound worker run is chosen.
    assert result["eligible"][0]["run_id"] == worker_run["id"]
    assert result["eligible"][0]["run_id"] != observer["id"]
    # An agent with only an observer run has no routable capacity.
    lone, lone_token, _ = world.started("ObserverOnly", runtime="factory")
    world.declare(lone, lone_token)
    lone_run = next(r for r in world.store.list("run", world.project, world.admin(), 10)
                    if r["agent_id"] == lone["id"])
    world.update_run(lone_run, purpose="observer")
    result = world.query(world.plan([{"key": "b", "title": "B"}])["tasks"][0],
                         candidates=[lone["id"]])
    assert result["rejected"] == [{"agent_id": lone["id"], "reason": "worker_run_required"}]


def test_busy_agent_disqualified_and_auxiliary_run_does_not_mask(world):
    busy, busy_token, busy_run = world.started("Busy", actions=["pause"], runtime="factory")
    world.declare(busy, busy_token)
    tasks = world.plan([{"key": "a", "title": "A"}, {"key": "b", "title": "B"}])["tasks"]
    first, task = tasks[0], tasks[1]
    world.dispatch(first, busy, busy_run, busy_token)
    world.run(busy, busy_token)  # Independent auxiliary run: must not mask the busy state.
    result = world.query(task)
    assert {item["reason"] for item in result["rejected"]} == {"agent_busy"}
    assert result["eligible"] == []


def test_allow_busy_agents_policy_routes_the_idle_worker_run(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, RoutingPolicy(enabled=True, allow_busy_agents=True))
    with TestClient(app) as client:
        world = Harness(app, client, config)
        agent, token, first = world.started("Busy", runtime="factory")
        world.declare(agent, token)
        tasks = world.plan([{"key": "a", "title": "A"}, {"key": "b", "title": "B"}])["tasks"]
        world.dispatch(tasks[0], agent, first, token)
        auxiliary = world.run(agent, token, worker=True)
        result = world.query(tasks[1])
        assert result["eligible"][0]["run_id"] == auxiliary["id"]
        assert result["eligible"][0]["workload"] == 1


def test_stale_or_silent_run_is_not_availability(world):
    agent, token, run = world.started("Stale", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    world.stale(run)
    result = world.query(task)
    assert result["eligible"] == []
    assert {item["reason"] for item in result["rejected"]} == {"no_live_run"}


def test_pending_control_hold_excludes_the_run(world):
    agent, token, run = world.started("Held", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    world.hold(agent, run)
    result = world.query(task)
    assert result["eligible"] == []
    assert {item["reason"] for item in result["rejected"]} == {"control_hold"}


def test_paused_run_status_is_not_routed(world):
    agent, token, run = world.started("Paused", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    world.update_run(run, status="paused")
    assert world.query(task)["eligible"] == []


# ---- constraints / privacy ---------------------------------------------------------

def test_exact_runtime_tool_and_sandbox_constraints(world):
    factory_agent, factory_token, _ = world.started("Factory", runtime="factory")
    world.declare(factory_agent, factory_token, runtime="factory",
                  tools=["read", "edit"], strict_sandbox=True)
    a2a_agent, a2a_token, _ = world.started("Remote", runtime="factory")
    world.declare(a2a_agent, a2a_token, runtime="a2a", tools=["read"])
    task = world.set_routing(
        world.plan([{"key": "a", "title": "A"}])["tasks"][0],
        {"required_runtime": "factory", "required_tools": ["edit"]})
    result = world.query(task)
    assert [entry["agent_id"] for entry in result["eligible"]] == [factory_agent["id"]]
    reasons = {item["agent_id"]: item["reason"] for item in result["rejected"]}
    assert reasons[a2a_agent["id"]] == "runtime_mismatch"


def test_tool_constraint_subset(world):
    reader, reader_token, _ = world.started("Reader", runtime="factory")
    world.declare(reader, reader_token, runtime="oh_my_pi", tools=["read"])
    task = world.set_routing(world.plan([{"key": "a", "title": "A"}])["tasks"][0],
                             {"required_tools": ["edit"]})
    assert world.query(task)["rejected"][0]["reason"] == "tool_constraint_mismatch"


def test_strict_sandbox_self_claim_is_never_proof(world):
    """A declared boolean — even on a sandbox-capable runtime family — cannot
    authorize a strict-sandbox task. Without the operator's run-bound verifier
    (e.g. unsupported platform such as Windows), the agent is excluded."""
    agent, token, _ = world.started("Claimed", runtime="factory")
    world.declare(agent, token, runtime="factory", strict_sandbox=True)
    task = world.set_routing(world.plan([{"key": "a", "title": "A"}])["tasks"][0],
                             {"strict_sandbox": True})
    result = world.query(task)
    assert result["eligible"] == []
    assert result["rejected"][0]["reason"] == "strict_sandbox_unsupported"
    # The gate agrees: self-claimed sandbox posture is advisory intent only.
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    run = next(r for r in world.store.list("run", world.project, world.admin(), 10)
               if r["agent_id"] == agent["id"])
    gate = routing.dispatch_gate(world.admin(), world.canonical(task["id"]),
                                 world.canonical(run["id"]))
    assert gate["allowed"] is False and gate["reason"] == "strict_sandbox_unsupported"


def test_strict_sandbox_requires_run_bound_verifier(tmp_path):
    calls = []

    def verifier(principal, task, run, declaration):
        calls.append(run["id"])
        return run.get("sandbox_attestation") == {"platform": "linux", "boundary": "enforced"}

    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, RoutingPolicy(enabled=True, sandbox_verifier=verifier))
    with TestClient(app) as client:
        world = Harness(app, client, config)
        agent, token, run = world.started("Sandboxed", runtime="factory")
        world.declare(agent, token, runtime="factory", strict_sandbox=True)
        task = world.set_routing(world.plan([{"key": "a", "title": "A"}])["tasks"][0],
                                 {"strict_sandbox": True})
        # The verifier rejects this run: no enforced launch attestation.
        assert world.query(task)["rejected"][0]["reason"] == "strict_sandbox_unsupported"
        assert calls == [run["id"]]
        # A run with operator-verified launch attestation passes.
        world.update_run(run, sandbox_attestation={"platform": "linux",
                                                   "boundary": "enforced"})
        assert world.query(task)["eligible"][0]["run_id"] == run["id"]
        # A crashing verifier is never proof.
        app2, _ = build(tmp_path, RoutingPolicy(
            enabled=True, sandbox_verifier=lambda *args: 1 / 0))
        with TestClient(app2) as client2:
            world2 = Harness(app2, client2, config)
            assert world2.query(task)["rejected"][0]["reason"] == "strict_sandbox_unsupported"


def test_invalid_task_requirements_fail_closed(world):
    agent, token, _ = world.started("Capable", runtime="factory")
    world.declare(agent, token)
    task = world.set_routing(world.plan([{"key": "a", "title": "A"}])["tasks"][0],
                             {"required_runtime": "not-a-registry-runtime"})
    result = world.query(task)
    assert result["blocked"] == "invalid_routing_requirements"
    assert result["eligible"] == []
    unknown_tool = world.set_routing(task, {"required_tools": ["rm_rf"]})
    assert world.query(unknown_tool)["blocked"] == "invalid_routing_requirements"
    malformed = world.set_routing(task, {"required_tools": "edit", "surprise": True})
    assert world.query(malformed)["blocked"] == "invalid_routing_requirements"


def test_cross_project_candidates_are_invisible_and_nothing_leaks(world):
    foreign = uid("project")
    with world.store.transaction():
        world.store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (foreign, "Other"))
    # The owner is an administrator and may register an agent in the foreign project.
    other_agent = world.call("POST", "agents", {"project_id": foreign, "display_name": "Foreign",
                                                "runtime": "factory"})
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    result = world.query(task, candidates=[other_agent["id"]])
    assert result["rejected"] == [{"agent_id": other_agent["id"],
                                   "reason": "agent_not_visible"}]
    # No foreign project identity, workload count, or task detail leaks anywhere.
    assert foreign not in json.dumps(result)
    assert "workload" not in json.dumps(result["rejected"])


def test_private_task_workload_never_leaks_to_other_agents(world):
    busy, busy_token, busy_run = world.started("Busy", runtime="factory")
    world.declare(busy, busy_token)
    idle, idle_token, _ = world.started("Idle", runtime="factory")
    world.declare(idle, idle_token)
    tasks = world.plan([{"key": "a", "title": "A"}, {"key": "b", "title": "B"}])["tasks"]
    private_task, queued = tasks
    world.dispatch(private_task, busy, busy_run, busy_token)
    with world.store.transaction():
        world.store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                               (busy["id"], private_task["id"]))
    # The idle agent cannot see the private task binding: its advisory view never
    # contains the private task id, and rejections carry no counts either way.
    result = world.query(queued, token=idle_token)
    assert private_task["id"] not in json.dumps(result)
    # The administrator's view applies the real busy state; the idle agent routes.
    visible = world.query(queued, token=world.owner)
    assert next(item for item in visible["eligible"]
                if item["agent_id"] == idle["id"])["workload"] == 0
    assert {item["reason"] for item in visible["rejected"]} == {"agent_busy"}


def test_workload_tie_break_is_stable_and_deterministic(world):
    first, first_token, _ = world.started("First", runtime="factory")
    world.declare(first, first_token)
    second, second_token, worker_run = world.started("Second", runtime="factory")
    world.declare(second, second_token)
    scope = world.call("POST", "scopes", {"project_id": world.project, "canonical_key": "k",
                                          "display_name": "k", "allowed_paths": []})
    world.call("POST", "work-claims", {"project_id": world.project, "agent_id": second["id"],
                                       "run_id": worker_run["id"], "scope_ids": [scope["id"]],
                                       "intent": "Hold a claim"}, second_token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    one, two = world.query(task), world.query(task)
    assert one == two  # Deterministic across calls.
    ordered = [(entry["agent_id"], entry["workload"], entry["active_claims"])
               for entry in one["eligible"]]
    assert ordered == sorted(ordered, key=lambda item: (item[1], item[2], item[0]))
    assert [entry["rank"] for entry in one["eligible"]] == [0, 1]
    # Equal workload and claims tie-break on the canonical agent id.
    plain = world.query(world.plan([{"key": "b", "title": "B"}])["tasks"][0])
    ids = [entry["agent_id"] for entry in plain["eligible"]]
    assert ids == sorted(ids)[: len(ids)]


def test_changed_authority_changes_routing(world):
    agent, token, _ = world.started("Mutable", runtime="factory")
    world.declare(agent, token, runtime="factory", tools=["read", "edit"])
    task = world.set_routing(world.plan([{"key": "a", "title": "A"}])["tasks"][0],
                             {"required_tools": ["edit"]})
    assert world.query(task)["eligible"][0]["agent_id"] == agent["id"]
    # The declaration is re-authorized and narrowed: routing follows current authority.
    world.declare(agent, token, runtime="factory", tools=["read"])
    assert world.query(task)["rejected"] == [{"agent_id": agent["id"],
                                              "reason": "tool_constraint_mismatch"}]
    narrowed = world.set_routing(task, {"required_runtime": "a2a"})
    assert world.query(narrowed)["rejected"][0]["reason"] == "runtime_mismatch"


def test_allowed_runtimes_policy_excludes_unlisted(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app, _ = build(tmp_path, RoutingPolicy(enabled=True, allowed_runtimes=("factory",)))
    with TestClient(app) as client:
        world = Harness(app, client, config)
        agent, token, _ = world.started("Remote", runtime="factory")
        world.declare(agent, token, runtime="a2a", tools=["read"])
        task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
        assert world.query(task)["rejected"][0]["reason"] == "runtime_not_allowed"


def test_candidate_bound_is_enforced_deterministically(world):
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    for index in range(4):
        agent, token, _ = world.started(f"A{index}", runtime="factory")
        world.declare(agent, token)
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True, max_candidates=2))
    result = routing.choose(world.admin(), world.canonical(task["id"]), None)
    assert result["candidates_truncated"] is True
    assert len(result["eligible"]) == 2


# ---- dispatch gate / start gate / fencing -------------------------------------------

def test_dispatch_gate_allows_exact_pair_and_preserves_holds(world):
    agent, token, run = world.started("Gated", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    canonical_task, canonical_run = world.canonical(task["id"]), world.canonical(run["id"])
    gate = routing.dispatch_gate(world.admin(), canonical_task, canonical_run)
    assert gate["allowed"] is True and gate["agent_id"] == agent["id"]
    # A pending control hold preserves the hold; staleness is never availability.
    world.hold(agent, run)
    assert routing.dispatch_gate(world.admin(), canonical_task,
                                 world.canonical(run["id"]))["reason"] == "control_hold"
    world.stale(run)
    assert routing.dispatch_gate(world.admin(), canonical_task,
                                 world.canonical(run["id"]))["allowed"] is False


def test_gate_rejects_busy_and_already_dispatched_tasks(world):
    agent, token, run = world.started("Worker", runtime="factory")
    world.declare(agent, token)
    tasks = world.plan([{"key": "a", "title": "A"}, {"key": "b", "title": "B"}])["tasks"]
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    world.dispatch(tasks[0], agent, run, token)
    # The still-queued second task must not gate onto the task-bound run.
    gate = routing.dispatch_gate(world.admin(), world.canonical(tasks[1]["id"]),
                                 world.canonical(run["id"]))
    assert gate["allowed"] is False and gate["reason"] == "agent_busy"
    # A running (dispatched) task is not routable at all.
    assert routing.dispatch_gate(world.admin(), world.canonical(tasks[0]["id"]),
                                 world.canonical(run["id"]))["reason"] == "task_not_dispatchable"


def test_gate_private_busy_peer_fails_closed_without_leaking(world):
    """An unverifiable peer binding (private task) blocks the exact worker
    dispatch with a generic reason — no private identity, no count."""
    agent, token, bound_run = world.started("Bound", runtime="factory")
    world.declare(agent, token)
    other = world.agent("Other")
    tasks = world.plan([{"key": "a", "title": "A"}, {"key": "b", "title": "B"}])["tasks"]
    private_task, queued = tasks
    world.dispatch(private_task, agent, bound_run, token)
    with world.store.transaction():
        # The task becomes visible only to a different agent; even this agent's
        # own principal can no longer verify the binding of its own run.
        world.store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                               (other["id"], private_task["id"]))
    free_run = world.run(agent, token, worker=True)
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    caller = {"role": "agent", "project": world.project, "agent": agent["id"], "id": "p"}
    gate = routing.dispatch_gate(caller, world.canonical(queued["id"]),
                                 world.canonical(free_run["id"]))
    assert gate["allowed"] is False
    assert gate["reason"] == "agent_not_available"
    assert private_task["id"] not in json.dumps(gate)
    # The administrator can verify the binding and sees the honest busy reason.
    admin_gate = routing.dispatch_gate(world.admin(), world.canonical(queued["id"]),
                                       world.canonical(free_run["id"]))
    assert admin_gate["allowed"] is False and admin_gate["reason"] == "agent_busy"


def test_gate_callback_matches_parent_contract(world):
    agent, token, run = world.started("Callback", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    verdict = routing.gate_callback(world.admin(), world.canonical(task["id"]),
                                    world.canonical(run["id"]))
    assert verdict["action"] == "allow" and verdict["reason"] is None
    assert verdict["retry_after"] is None
    world.stale(run)
    verdict = routing.gate_callback(world.admin(), world.canonical(task["id"]),
                                    world.canonical(run["id"]))
    assert verdict["action"] == "defer" and verdict["reason"] == "no_live_run"
    assert verdict["retry_after"] > time.time()


def test_start_gate_allows_the_final_attempt_after_real_dispatch(world):
    """Parent task_start rechecks gates after scheduling._start incremented the
    attempt: with max_attempts=1 the generic queued gates would deny, so the
    start gate validates the exact dispatch record instead."""
    agent, token, run = world.started("Final", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A", "max_attempts": 1}])["tasks"][0]
    world.dispatch(task, agent, run, token)
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    dispatched = world.canonical(task["id"])
    assert dispatched["scheduling"]["state"] == "dispatched"
    assert dispatched["scheduling"]["attempts"] == 1  # Budget already consumed.
    # The parent rechecks mid-task_start: the exact record scheduling._start
    # saved (state dispatched) before the canonical execution flips the task to
    # running. Emulate that precise record.
    mid_start = world.update_task_status(dispatched, "queued")
    verdict = routing.start_gate(world.admin(), mid_start, world.canonical(run["id"]))
    assert verdict == {**verdict, "action": "allow", "reason": None}
    # A different run has no dispatch record for this attempt.
    other_run = world.run(agent, token, worker=True)
    verdict = routing.start_gate(world.admin(), mid_start,
                                 world.canonical(other_run["id"]))
    assert verdict["action"] == "defer" and verdict["reason"] == "dispatch_record_mismatch"
    # After a terminal self-report there is no live dispatch record to start.
    failed = world.call("POST", f"tasks/{task['id']}:dispatch-outcome",
                        {"expected_revision": dispatched["revision"], "agent_id": agent["id"],
                         "run_id": run["id"], "outcome": "failed", "summary": "no"}, token)
    assert failed["scheduling"]["escalated"] is True
    verdict = routing.start_gate(world.admin(), world.canonical(task["id"]),
                                 world.canonical(run["id"]))
    assert verdict["action"] == "defer" and verdict["reason"] == "no_dispatch_record"


def test_fenced_dispatch_records_advisory_approval(world):
    agent, token, run = world.started("Approved", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    result = world.call("POST", f"projects/{world.project}/routing:dispatch",
                        {"task_id": task["id"], "agent_id": agent["id"],
                         "run_id": run["id"], "reason": "Operator approved"})
    assert result["kind"] == "durable_approval"
    assert result["fence"]["fenced_by"] == "operator"
    assert result["gate"]["allowed"] is True
    assert result["decision"]["decision"] == "approved"
    assert result["authority"].startswith("advisory_approval_only")
    # The approval started nothing: the run is still unassigned and the task queued.
    assert world.canonical(run["id"])["task_id"] is None
    assert world.canonical(task["id"])["status"] == "queued"
    # A parent hook can pick up the pending approval for the exact pair.
    routing = CapabilityRouting(world.service, RoutingPolicy(enabled=True))
    pending = routing.approved_decision(world.admin(), world.project, task["id"],
                                        agent["id"], run["id"])
    assert pending["id"] == result["decision"]["id"]
    assert routing.approved_decision(world.admin(), world.project, task["id"],
                                     agent["id"], uid("run")) is None


def test_fenced_dispatch_rejects_failed_gates_and_bad_fences(world):
    agent, token, run = world.started("Checked", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    world.hold(agent, run)
    response = world.request("POST", f"projects/{world.project}/routing:dispatch",
                             {"task_id": task["id"], "agent_id": agent["id"],
                              "run_id": run["id"], "reason": "Held run"})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "routing_rejected"
    # An agent principal needs the Nexus lease fence; no epoch is a 422, a wrong
    # epoch is a stale-epoch conflict, and no agent impersonation occurs either way.
    missing = world.request("POST", f"projects/{world.project}/routing:dispatch",
                            {"task_id": task["id"], "agent_id": agent["id"],
                             "run_id": run["id"], "reason": "No epoch"}, token)
    assert missing.status_code == 422
    assert missing.json()["error"]["code"] == "coordinator_epoch_required"
    stale = world.request("POST", f"projects/{world.project}/routing:dispatch",
                          {"task_id": task["id"], "agent_id": agent["id"],
                           "run_id": run["id"], "coordinator_epoch": 1,
                           "reason": "Wrong epoch"}, token)
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "stale_coordinator_epoch"


def test_fenced_dispatch_is_idempotent(world):
    agent, token, run = world.started("Once", runtime="factory")
    world.declare(agent, token)
    task = world.plan([{"key": "a", "title": "A"}])["tasks"][0]
    key = uid("key")
    body = {"task_id": task["id"], "agent_id": agent["id"],
            "run_id": run["id"], "reason": "Exactly once"}
    first = world.call("POST", f"projects/{world.project}/routing:dispatch", body, key=key)
    replay = world.call("POST", f"projects/{world.project}/routing:dispatch", body, key=key)
    assert replay["decision"]["id"] == first["decision"]["id"]
