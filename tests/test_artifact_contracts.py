"""Task-bound artifact contract tests: exact evidence, gate invalidation, safety bounds.

The module under test is installed explicitly (default-off feature); the world
harness supplies principals, tasks, runs, and canonical artifacts. Trusted checks
use synthetic evidence-binding verifiers; no paid runs, hosts, or remote effects.
"""

import hashlib
import json
import threading
import time

import pytest

from hivemind import artifact_contracts as ac
from hivemind.api import hash_token
from hivemind.artifact_contracts import ContractPolicy
from hivemind.storage import Problem, canonical, uid


@pytest.fixture
def contracts(world):
    return install_on(world)


def install_on(world, verifier=None, policy=None):
    app, service = world.app, world.app.state.service
    from fastapi import Request

    from hivemind.api import envelope
    from hivemind.models import Model

    store = service.store

    def principal(request: Request):
        authorization = request.headers.get("authorization", "")
        if not authorization.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        row = store.db.execute(
            "SELECT * FROM principals WHERE token_hash=?",
            (hashlib.sha256(authorization[7:].encode()).hexdigest(),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = request.model_dump(exclude_none=True) if isinstance(request, Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    return ac.install(app, service, principal, command, envelope,
                      policy=policy or ContractPolicy(enabled=True), verifier=verifier)


def test_real_native_profile_certifies_exact_canonical_execution(world, tmp_path):
    import sys
    from hivemind.native_artifact_verifier import NativeArtifactVerifier, NativeCheckProfile
    stage = tmp_path / "native-checks"
    stage.mkdir()
    verifier = NativeArtifactVerifier(staging_root=stage, enabled=True, require_os_isolation=False,
        profiles=[NativeCheckProfile("syntax", world.project,
            (sys.executable, "-m", "py_compile", "artifact.py"), input_path="artifact.py")])
    install_on(world, verifier=verifier)
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    task = completed_task(world, agent, token, run)
    artifact = make_artifact(world, token, run, "x = 1\n")
    contract = world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": task["id"], "title": "Checked output",
        "outputs": [{"name": "report", "type": "file", "media_type": "text/plain",
                     "max_bytes": 1024, "checks": ["syntax"]}]})
    result = submit(world, token, contract, task, artifact, key="native-exact")
    assert result["verdict"] == "satisfied"
    assert submit(world, token, contract, task, artifact, key="native-exact") == result
    assert len(list(stage.iterdir())) == 1


def completed_task(world, agent, token, run, key="tasks/contract"):
    scope = world.scope(f"{key}/{uid('scope')}")
    task = world.call("POST", "tasks", {"project_id": world.project,
                                        "title": "Produce deliverable",
                                        "scope_ids": [scope["id"]]})
    world.claim(agent, run, scope, token)
    active = world.call("POST", f"tasks/{task['id']}:start",
                        {"expected_revision": task["revision"], "run_id": run["id"]}, token)
    return world.call("POST", f"tasks/{task['id']}:checkpoint", {
        "expected_revision": active["revision"], "run_id": run["id"],
        "execution_id": active["execution"]["id"], "checkpoint_id": uid("checkpoint"),
        "status": "completed", "progress": 100, "summary": "Delivered"}, token)


def make_artifact(world, token, run, text, media="text/plain", title="Deliverable"):
    return world.call("POST", "artifacts", {
        "project_id": world.project, "title": title,
        "content": {"media_type": media, "text": text}, "run_id": run["id"]}, token)


JSON_SCHEMA = {"type": "object",
               "properties": {"version": {"type": "integer", "minimum": 1},
                              "name": {"type": "string", "minLength": 1}},
               "required": ["version", "name"], "additionalProperties": False}


def json_contract(world, task):
    return world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": task["id"], "title": "Export contract",
        "outputs": [{"name": "report", "type": "json", "schema": JSON_SCHEMA}]})


def pin(artifact, name="report"):
    return {"name": name, "artifact_id": artifact["id"],
            "revision": artifact["revision"],
            "sha256": ac.content_checksum(artifact)}


def submit(world, token, contract, task, artifact, status=200, key=None, **overrides):
    body = {"contract_revision": contract["revision"], "task_id": task["id"],
            "run_id": task["execution"]["run_id"] if task.get("execution") else overrides.pop("run_id"),
            "execution_id": (task.get("execution") or {}).get("id") or overrides.pop("execution_id"),
            "artifacts": [pin(artifact)]}
    body.update(overrides)
    return world.call("POST", f"artifact-contracts/{contract['id']}:submit", body,
                      token, key=key, status=status)


def principal_of(world, token):
    return dict(world.app.state.store.db.execute(
        "SELECT * FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone())


def admin_principal(world):
    return dict(world.app.state.store.db.execute(
        "SELECT * FROM principals WHERE role='admin' LIMIT 1").fetchone())


def gate(world, dependent, dependency, token=None):
    handle = world.app.state.service.artifact_contracts
    row = world.app.state.store.raw(dependent["id"], "task")
    task = json.loads(row["body"])
    principal = principal_of(world, token) if token else admin_principal(world)
    return handle.dependency_gate(principal, task, dependency["id"])


def test_compliant_submission_readies_dependent(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "export"}))
    submission = submit(world, token, contract, done, artifact)
    assert submission["verdict"] == "satisfied" and submission["self_report"] is False
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    verdict = gate(world, dependent, done)
    assert verdict["approved"] is True and verdict["reason"] == "satisfied"


def test_invalid_deliverable_blocks_dependent(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    bad = make_artifact(world, token, run, json.dumps({"version": 0, "extra": True}))
    submission = submit(world, token, contract, done, bad)
    assert submission["verdict"] == "rejected"
    assert any("report" in item for item in submission["diagnostics"])
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    verdict = gate(world, dependent, done)
    # The producer's completion exists; the dependent stays blocked.
    assert verdict["approved"] is False and verdict["reason"] == "contract_unsatisfied"


def test_missing_submission_blocks(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    json_contract(world, done)
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    verdict = gate(world, dependent, done)
    assert verdict["approved"] is False and verdict["reason"] == "submission_missing"
    assert verdict["missing_outputs"] == ["report"]


def test_tampered_artifact_invalidates_satisfaction(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["approved"] is True
    store = world.app.state.store
    with store.transaction():
        row = store.raw(artifact["id"], "artifact")
        body = json.loads(row["body"])
        body["content"]["text"] = json.dumps({"version": 99, "name": "tampered"})
        store.save("artifact", row["project"], row["owner"], body, expected=row["revision"])
    # Superseded revision is caught before the checksum is even compared.
    assert gate(world, dependent, done)["reason"] == "artifact_superseded"


def test_same_revision_tamper_fails_checksum(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    store = world.app.state.store
    with store.transaction():
        # Rewrite the stored body in place without a revision bump (hostile store edit).
        row = store.raw(artifact["id"], "artifact")
        body = json.loads(row["body"])
        body["content"]["text"] = json.dumps({"version": 2, "name": "forged"})
        store.db.execute("UPDATE resources SET body=? WHERE id=?",
                         (canonical(body), artifact["id"]))
    assert gate(world, dependent, done)["reason"] == "artifact_checksum_mismatch"


def test_checksum_mismatch_refused_at_submit(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    forged = {**pin(artifact), "sha256": hashlib.sha256(b"forged").hexdigest()}
    submit(world, token, contract, done, artifact, status=422, artifacts=[forged])


def test_revision_conflict_refused_at_submit(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    stale = {**pin(artifact), "revision": artifact["revision"] + 1}
    submit(world, token, contract, done, artifact, status=412, artifacts=[stale])


def test_deleted_artifact_invalidates(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["approved"] is True
    store = world.app.state.store
    with store.transaction():
        store.delete(artifact["id"], admin_principal(world), artifact["revision"])
    assert gate(world, dependent, done)["reason"] == "artifact_deleted"


def test_deleted_contract_never_liberates_dependency(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["approved"] is True
    store = world.app.state.store
    with store.transaction():
        store.delete(contract["id"], admin_principal(world), contract["revision"])
    # A physical deletion tombstone holds the gate; only explicit revocation
    # under an operator release policy could release it.
    verdict = gate(world, dependent, done)
    assert verdict["approved"] is False and verdict["reason"] == "contract_deleted_unreleased"


def test_private_artifact_not_visible_to_gate(world, contracts):
    agent, other = world.agent("Producer"), world.agent("Other")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    store = world.app.state.store
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done, token=token)["approved"] is True
    with store.transaction():
        # Reprivatize the evidence to a different agent only.
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (other["id"], artifact["id"]))
    # Current visibility is re-checked on every gate evaluation.
    verdict = gate(world, dependent, done, token=token)
    assert verdict["approved"] is False and verdict["reason"] == "artifact_not_visible"
    # And the producer can no longer submit against evidence it cannot see.
    submit(world, token, contract, done, artifact, status=404)


def test_private_contract_holds_gate_without_leaking(world, contracts):
    agent, other = world.agent("Producer"), world.agent("Other")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (other["id"], contract["id"]))
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    # A hidden applicable contract must HOLD the gate; it can never look absent.
    verdict = gate(world, dependent, done, token=token)
    assert verdict["approved"] is False and verdict["reason"] == "contract_not_visible"
    assert "contract_id" not in verdict  # no hidden-id leak
    assert gate(world, dependent, done)["approved"] is True  # admin sees it


def test_cross_project_artifact_refused(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    store = world.app.state.store
    foreign = uid("project")
    with store.transaction():
        store.db.execute("INSERT INTO projects(id, title) VALUES(?,?)", (foreign, "Foreign"))
        store.db.execute("UPDATE resources SET project=? WHERE id=?", (foreign, artifact["id"]))
    # The owner (admin) can see the row, but the project mismatch is refused.
    submit(world, world.owner, contract, done, artifact, status=422)


def test_execution_binding_enforced(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    submit(world, token, contract, done, artifact, status=422,
           run_id=run["id"], execution_id=uid("execution"))
    # Task id must match the contract's bound task.
    other = completed_task(world, agent, token, run, key="tasks/other")
    submit(world, token, contract, done, artifact, status=422,
           task_id=other["id"])


def test_historical_execution_never_binds(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    store = world.app.state.store
    # Move the current execution into history with a fresh current execution:
    # submissions pinned to the historical execution are refused, and an already
    # satisfied submission goes stale at the gate.
    historical = done["execution"]["id"]
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    with store.transaction():
        row = store.raw(done["id"], "task")
        body = json.loads(row["body"])
        body["execution_history"] = [body["execution"]]
        body["execution"] = {**body["execution"], "id": uid("execution")}
        store.save("task", row["project"], row["owner"], body, expected=row["revision"])
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["reason"] == "execution_binding_stale"
    submit(world, token, contract, {**done, "execution": body["execution"]}, artifact,
           status=422, execution_id=historical)


def test_wrong_agent_cannot_submit(world, contracts):
    agent, intruder = world.agent("Producer"), world.agent("Intruder")
    token = world.credential(agent)
    intruder_token = world.credential(intruder)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    # The intruder does not own the run behind the current execution.
    submit(world, intruder_token, contract, done, artifact, status=403)


def test_agent_cannot_manage_contracts(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": done["id"], "title": "Self rules",
        "outputs": [{"name": "report", "type": "json", "schema": JSON_SCHEMA}]},
        token, status=403)
    contract = json_contract(world, done)
    world.call("POST", f"artifact-contracts/{contract['id']}:revise",
               {"expected_revision": contract["revision"], "title": "Hijack"},
               token, status=403)
    world.call("POST", f"artifact-contracts/{contract['id']}:revoke",
               {"expected_revision": contract["revision"]}, token, status=403)


def test_revised_contract_invalidates_old_satisfaction(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["approved"] is True
    revised = world.call("POST", f"artifact-contracts/{contract['id']}:revise",
                         {"expected_revision": contract["revision"],
                          "outputs": [{"name": "report", "type": "json",
                                       "schema": {**JSON_SCHEMA, "required": ["version"]}}]})
    assert revised["revision"] == contract["revision"] + 1
    # Same output names but a new contract revision: old satisfaction is void.
    verdict = gate(world, dependent, done)
    assert verdict["approved"] is False and verdict["reason"] == "contract_revised"
    # Submissions must pin the current contract revision (exact CAS).
    submit(world, token, contract, done, artifact, status=412)
    submission = submit(world, token, revised, done, artifact)
    assert submission["verdict"] == "satisfied"
    assert gate(world, dependent, done)["approved"] is True


def test_revoked_contract_invalidates(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["approved"] is True
    current = world.call("GET", f"artifact-contracts/{contract['id']}")
    world.call("POST", f"artifact-contracts/{contract['id']}:revoke",
               {"expected_revision": current["revision"]})
    assert gate(world, dependent, done)["reason"] == "contract_revoked"
    # Revoked contracts refuse further submissions.
    submit(world, token, current, done, artifact, status=409)


def test_revocation_release_policy(world):
    install_on(world, policy=ContractPolicy(enabled=True, revocation_releases=True))
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    world.call("POST", f"artifact-contracts/{contract['id']}:revoke",
               {"expected_revision": contract["revision"]})
    # Explicit authoritative revocation under the release policy frees the gate.
    assert gate(world, dependent, done)["approved"] is True


def test_contract_frozen_while_execution_active(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope(f"tasks/active/{uid('scope')}")
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Running",
                                        "scope_ids": [scope["id"]]})
    world.claim(agent, run, scope, token)
    world.call("POST", f"tasks/{task['id']}:start",
               {"expected_revision": task["revision"], "run_id": run["id"]}, token)
    # Creating a contract on a task with a live execution is refused.
    world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": task["id"], "title": "Late rules",
        "outputs": [{"name": "report", "type": "json", "schema": JSON_SCHEMA}]}, status=409)
    done = completed_task(world, agent, token, world.run(agent, token), key="tasks/freeze")
    contract = json_contract(world, done)
    # A completed task allows revision with exact CAS; wrong revisions conflict.
    world.call("POST", f"artifact-contracts/{contract['id']}:revise",
               {"expected_revision": contract["revision"] + 9, "title": "New"}, status=412)
    revised = world.call("POST", f"artifact-contracts/{contract['id']}:revise",
                         {"expected_revision": contract["revision"], "title": "New title"})
    assert revised["revision"] == contract["revision"] + 1


def test_schema_ref_and_unsafe_constructs_refused(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    base = {"project_id": world.project, "task_id": done["id"], "title": "C"}
    for schema in (
        {"$ref": "https://example.com/schema.json"},
        {"type": "object", "properties": {"x": {"$ref": "#/definitions/x"}}},
        {"type": "string", "pattern": "^(a+)+$"},
        {"anyOf": [{"type": "string"}, {"type": "integer"}]},
        {"type": "function"},
        {"type": ["object", "null"]},
    ):
        world.call("POST", "artifact-contracts",
                   {**base, "outputs": [{"name": "o", "type": "json", "schema": schema}]},
                   status=422)
    # Unsafe output types and missing media metadata are refused.
    world.call("POST", "artifact-contracts",
               {**base, "outputs": [{"name": "o", "type": "xml"}]}, status=422)
    world.call("POST", "artifact-contracts",
               {**base, "outputs": [{"name": "o", "type": "text"}]}, status=422)
    world.call("POST", "artifact-contracts",
               {**base, "outputs": [{"name": "o", "type": "file"}]}, status=422)


def test_size_bounds_refused(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    base = {"project_id": world.project, "task_id": done["id"], "title": "C"}
    huge_schema = {"type": "object",
                   "properties": {f"k{i}": {"type": "string"} for i in range(2000)}}
    world.call("POST", "artifact-contracts",
               {**base, "outputs": [{"name": "o", "type": "json", "schema": huge_schema}]},
               status=422)
    deep = current = {"type": "object", "properties": {}}
    for index in range(40):
        child = {"type": "object", "properties": {}}
        current["properties"][f"n{index}"] = child
        current = child
    world.call("POST", "artifact-contracts",
               {**base, "outputs": [{"name": "o", "type": "json", "schema": deep}]}, status=422)
    many = [{"name": f"o{i}", "type": "text", "media_type": "text/plain"} for i in range(17)]
    world.call("POST", "artifact-contracts", {**base, "outputs": many}, status=422)
    # More properties than allowed in one object.
    wide = {"type": "object",
            "properties": {f"k{i}": {"type": "string"} for i in range(65)}}
    world.call("POST", "artifact-contracts",
               {**base, "outputs": [{"name": "o", "type": "json", "schema": wide}]}, status=422)


def test_dependency_diamond_is_valid_cycle_fails_closed(world, contracts):
    store = world.app.state.store
    one = world.call("POST", "tasks", {"project_id": world.project, "title": "One"})
    two = world.call("POST", "tasks", {"project_id": world.project, "title": "Two"})
    three = world.call("POST", "tasks", {"project_id": world.project, "title": "Three"})
    four = world.call("POST", "tasks", {"project_id": world.project, "title": "Four"})
    with store.transaction():
        # Diamond: one -> two, one -> three, two -> four, three -> four.
        for source, target in ((one, two), (one, three), (two, four), (three, four)):
            store.relate(world.project, source["id"], target["id"], "DEPENDS_ON")
    # Revisiting a fully-explored node (the diamond join) is not a cycle.
    assert gate(world, one, two)["reason"] == "no_contract"
    assert gate(world, one, three)["approved"] is True
    with store.transaction():
        store.relate(world.project, four["id"], one["id"], "DEPENDS_ON")
    verdict = gate(world, one, two)
    assert verdict["approved"] is False and verdict["reason"] == "dependency_cycle"


def test_no_contract_is_passthrough(world, contracts):
    one = world.call("POST", "tasks", {"project_id": world.project, "title": "One"})
    two = world.call("POST", "tasks", {"project_id": world.project, "title": "Two"})
    verdict = gate(world, one, two)
    assert verdict["approved"] is True and verdict["reason"] == "no_contract"
    # Module-level parent hook without an installed handle also passes through.
    from types import SimpleNamespace
    verdict = ac.dependency_gate(SimpleNamespace(), {}, one, two["id"])
    assert verdict["approved"] is True and verdict["reason"] == "contracts_disabled"


def test_text_and_file_outputs(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": done["id"], "title": "Docs",
        "outputs": [{"name": "notes", "type": "text", "media_type": "text/markdown",
                     "min_length": 3},
                    {"name": "bundle", "type": "file", "media_type": "text/plain",
                     "max_bytes": 64}]})
    notes = make_artifact(world, token, run, "# ok", media="text/markdown", title="Notes")
    bundle = make_artifact(world, token, run, "bytes", media="text/plain", title="Bundle")
    body = {"contract_revision": contract["revision"], "task_id": done["id"],
            "run_id": done["execution"]["run_id"],
            "execution_id": done["execution"]["id"],
            "artifacts": [{**pin(notes), "name": "notes"}, {**pin(bundle), "name": "bundle"}]}
    submission = world.call("POST", f"artifact-contracts/{contract['id']}:submit", body, token)
    assert submission["verdict"] == "satisfied"
    # Wrong media metadata on the text output records a rejected verdict.
    plain = make_artifact(world, token, run, "plain", media="text/plain", title="Notes2")
    body["artifacts"][0] = {**pin(plain), "name": "notes"}
    submission = world.call("POST", f"artifact-contracts/{contract['id']}:submit", body, token)
    assert submission["verdict"] == "rejected"
    assert any("media_type" in item for item in submission["diagnostics"])
    # Unknown/missing named outputs refuse the submission outright.
    world.call("POST", f"artifact-contracts/{contract['id']}:submit",
               {**body, "artifacts": body["artifacts"][:1]}, token, status=422)
    world.call("POST", f"artifact-contracts/{contract['id']}:submit",
               {**body, "artifacts": [*body["artifacts"],
                                      {**pin(bundle), "name": "mystery"}]}, token, status=422)


class BoundVerifier:
    """Synthetic trusted verifier implementing the evidence-binding interface."""

    def __init__(self, ok=True, echo=True, delay=0, on_run=None):
        self.ok, self.echo, self.delay, self.on_run = ok, echo, delay, on_run
        self.calls = []

    def run_bound(self, name, evidence):
        self.calls.append((name, evidence))
        if self.on_run:
            self.on_run()
        if self.delay:
            time.sleep(self.delay)
        return {"name": name, "ok": self.ok,
                "evidence": evidence if self.echo else {"tampered": True}}


def checked_contract(world, done, checks=("unit-tests",), outputs=("bundle",)):
    return world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": done["id"], "title": "Checked",
        "outputs": [{"name": name, "type": "file", "media_type": "text/plain",
                     "checks": list(checks)} for name in outputs]})


def submit_bundle(world, token, contract, done, artifact, key=None, status=200):
    body = {"contract_revision": contract["revision"], "task_id": done["id"],
            "run_id": done["execution"]["run_id"], "execution_id": done["execution"]["id"],
            "artifacts": [{**pin(artifact), "name": "bundle"}]}
    return world.call("POST", f"artifact-contracts/{contract['id']}:submit", body,
                      token, key=key, status=status)


def test_trusted_checks_two_phase(world):
    verifier = BoundVerifier()
    install_on(world, verifier=verifier)
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = checked_contract(world, done)
    bundle = make_artifact(world, token, run, "bytes")
    submission = submit_bundle(world, token, contract, done, bundle)
    assert submission["verdict"] == "satisfied"
    assert len(verifier.calls) == 1
    name, evidence = verifier.calls[0]
    assert name == "unit-tests"
    # The check was bound to the exact evidence, not run in a vacuum.
    assert evidence["contract_id"] == contract["id"]
    assert evidence["contract_revision"] == contract["revision"]
    assert evidence["task_id"] == done["id"] and evidence["output"] == "bundle"
    assert evidence["artifacts"][0]["sha256"] == ac.content_checksum(bundle)
    receipt = next(r for r in submission["receipts"] if r["kind"] == "trusted_check")
    assert receipt["ok"] is True and receipt["digest"] and receipt["output"] == "bundle"
    # A failing trusted check records a rejected verdict, never a pass.
    world.app.state.service.artifact_contracts.verifier = BoundVerifier(ok=False)
    submission = submit_bundle(world, token, contract, done, bundle, key=uid("key"))
    assert submission["verdict"] == "rejected"


def test_unbound_verifier_is_advisory_only(world):
    from types import SimpleNamespace
    verifier = SimpleNamespace(run=lambda name, paths: SimpleNamespace(
        as_record=lambda: {"name": name, "ok": True}))
    install_on(world, verifier=verifier)
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = checked_contract(world, done)
    bundle = make_artifact(world, token, run, "bytes")
    # A generic run() success is advisory and never satisfies an artifact check.
    submission = submit_bundle(world, token, contract, done, bundle)
    assert submission["verdict"] == "rejected"
    assert any("trusted_verifier_unbound" in item for item in submission["diagnostics"])


def test_check_evidence_mismatch_rejected(world):
    install_on(world, verifier=BoundVerifier(echo=False))
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = checked_contract(world, done)
    bundle = make_artifact(world, token, run, "bytes")
    submission = submit_bundle(world, token, contract, done, bundle)
    assert submission["verdict"] == "rejected"
    assert any("check_evidence_mismatch" in item for item in submission["diagnostics"])


def test_every_required_check_runs_and_binds_its_output(world):
    verifier = BoundVerifier()
    install_on(world, verifier=verifier)
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": done["id"], "title": "Multi",
        "outputs": [{"name": "a", "type": "file", "media_type": "text/plain",
                     "checks": ["lint", "unit"]},
                    {"name": "b", "type": "file", "media_type": "text/plain",
                     "checks": ["lint", "packaging"]}]})
    first = make_artifact(world, token, run, "one", title="A")
    second = make_artifact(world, token, run, "two", title="B")
    body = {"contract_revision": contract["revision"], "task_id": done["id"],
            "run_id": done["execution"]["run_id"], "execution_id": done["execution"]["id"],
            "artifacts": [{**pin(first), "name": "a"}, {**pin(second), "name": "b"}]}
    submission = world.call("POST", f"artifact-contracts/{contract['id']}:submit", body, token)
    assert submission["verdict"] == "satisfied"
    # No required check is dropped or merged away: 2 outputs x 2 checks.
    assert sorted((name, evidence["output"]) for name, evidence in verifier.calls) == [
        ("lint", "a"), ("lint", "b"), ("packaging", "b"), ("unit", "a")]
    receipts = [r for r in submission["receipts"] if r["kind"] == "trusted_check"]
    assert len(receipts) == 4 and all(r["ok"] for r in receipts)


def test_native_check_intent_exactly_once_and_crash_unknown(world):
    verifier = BoundVerifier()
    install_on(world, verifier=verifier)
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = checked_contract(world, done)
    bundle = make_artifact(world, token, run, "bytes")
    key = uid("key")
    first = submit_bundle(world, token, contract, done, bundle, key=key)
    assert first["verdict"] == "satisfied" and len(verifier.calls) == 1
    # Replay with the same key: cached response, native check NOT repeated.
    replay = submit_bundle(world, token, contract, done, bundle, key=key)
    assert replay["id"] == first["id"] and len(verifier.calls) == 1
    # Simulate a crash between reserving the intent and finishing the checks.
    store = world.app.state.store
    payload = {"contract_id": contract["id"],
               "contract_revision": contract["revision"], "task_id": done["id"],
               "run_id": done["execution"]["run_id"],
               "execution_id": done["execution"]["id"],
               "artifacts": [{**pin(bundle), "name": "bundle"}],
               "idempotency_key": "crash-key"}
    check_key = hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()
    principal = principal_of(world, token)
    with store.transaction():
        store.db.execute("INSERT INTO commands VALUES(?,?,?,?,?)", (
            principal["id"], f"artifact-contract-checks:{contract['id']}", check_key,
            "0" * 64, canonical({"state": "reserved", "fingerprint": "f", "refs": []})))
    body = {key_: value for key_, value in payload.items()
            if key_ not in ("idempotency_key", "contract_id")}
    world.call("POST", f"artifact-contracts/{contract['id']}:submit", body, token,
               key="crash-key", status=409)
    record = json.loads(store.db.execute(
        "SELECT response FROM commands WHERE principal=? AND key=?",
        (principal["id"], check_key)).fetchone()[0])
    # The uncertain native effect is marked unknown and was never repeated.
    assert record["state"] == "unknown" and len(verifier.calls) == 1


def test_revoked_principal_during_detached_compute_fails(world):
    store_holder = {}

    def revoke_midflight():
        store = store_holder["store"]
        with store.transaction():
            store.db.execute("DELETE FROM principals WHERE token_hash=?",
                             (hash_token(store_holder["token"]),))

    install_on(world, verifier=BoundVerifier(on_run=revoke_midflight))
    agent = world.agent("Producer")
    token = world.credential(agent)
    store_holder["store"] = world.app.state.store
    store_holder["token"] = token
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = checked_contract(world, done)
    bundle = make_artifact(world, token, run, "bytes")
    # The principal vanished between the authorized snapshot and the record.
    submit_bundle(world, token, contract, done, bundle, status=401)


def test_checks_run_off_the_canonical_mutex(world):
    install_on(world, verifier=BoundVerifier(delay=0.6))
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = checked_contract(world, done)
    bundle = make_artifact(world, token, run, "bytes")
    handle = world.app.state.service.artifact_contracts
    request = ac.SubmissionCreate(
        contract_revision=contract["revision"], task_id=done["id"],
        run_id=done["execution"]["run_id"], execution_id=done["execution"]["id"],
        artifacts=[ac.SubmissionArtifact(**{**pin(bundle), "name": "bundle"})])
    principal = principal_of(world, token)
    outcome = {}
    worker = threading.Thread(
        target=lambda: outcome.setdefault(
            "result", handle.submit(principal, contract["id"], request, uid("key"))))
    started = time.monotonic()
    worker.start()
    time.sleep(0.1)  # let the worker reach the off-lock check phase
    store = world.app.state.store
    lock_started = time.monotonic()
    with store.lock:
        pass  # heartbeat: the canonical mutex is free during native checks
    held_off = time.monotonic() - lock_started
    worker.join(timeout=10)
    assert held_off < 0.4, f"canonical lock blocked {held_off:.2f}s during checks"
    assert outcome["result"]["verdict"] == "satisfied"
    assert time.monotonic() - started >= 0.6


def test_submission_views_reauthorize_sources(world, contracts):
    agent, other = world.agent("Producer"), world.agent("Other")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    listed = world.call("GET", f"artifact-contracts/{contract['id']}/submissions",
                        token=token)
    assert len(listed) == 1
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (other["id"], artifact["id"]))
    # Derived source ids/diagnostics are never emitted for invisible evidence.
    assert world.call("GET", f"artifact-contracts/{contract['id']}/submissions",
                      token=token) == []
    assert len(world.call("GET", f"artifact-contracts/{contract['id']}/submissions")) == 1


def test_gate_route_dependency_fence(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    unrelated = world.call("POST", "tasks", {"project_id": world.project, "title": "Unrelated"})
    # dependency_id must be a declared dependency of the task.
    world.call("GET", f"artifact-contracts:gate?task_id={unrelated['id']}"
                      f"&dependency_id={done['id']}", status=404)
    store = world.app.state.store
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consumer"})
    with store.transaction():
        row = store.raw(dependent["id"], "task")
        body = json.loads(row["body"])
        body["depends_on"] = [done["id"]]
        store.save("task", row["project"], row["owner"], body, expected=row["revision"])
        store.relate(world.project, dependent["id"], done["id"], "DEPENDS_ON")
    verdict = world.call("GET", f"artifact-contracts:gate?task_id={dependent['id']}"
                                f"&dependency_id={done['id']}")
    assert verdict["approved"] is True and verdict["reason"] == "satisfied"


def test_default_off_installs_nothing(world):
    from hivemind.api import envelope
    service = world.app.state.service
    result = ac.install(world.app, service, lambda: None, None, envelope)
    assert result is None
    assert world.app.state.artifact_contracts is None
    assert service.artifact_contracts is None
    assert not getattr(service, "task_dependency_gates", ())
    world.call("POST", "artifact-contracts", {
        "project_id": world.project, "task_id": uid("task"), "title": "C",
        "outputs": [{"name": "o", "type": "json", "schema": {}}]}, status=404)


def test_policy_requires_strict_bools():
    with pytest.raises(ValueError):
        ContractPolicy(enabled=1)
    with pytest.raises(ValueError):
        ContractPolicy(enabled=True, revocation_releases="yes")


def test_gate_action_adapter(world, contracts):
    service = world.app.state.service
    one = world.call("POST", "tasks", {"project_id": world.project, "title": "One"})
    two = world.call("POST", "tasks", {"project_id": world.project, "title": "Two"})
    verdict = service.task_dependency_gates[0](admin_principal(world),
                                            {"id": one["id"], "project_id": world.project},
                                            two["id"])
    assert verdict["action"] == "allow" and verdict["reason"] == "no_contract"


def test_stale_execution_binding_invalidates(world, contracts):
    agent = world.agent("Producer")
    token = world.credential(agent)
    run = world.run(agent, token)
    done = completed_task(world, agent, token, run)
    contract = json_contract(world, done)
    artifact = make_artifact(world, token, run, json.dumps({"version": 1, "name": "x"}))
    assert submit(world, token, contract, done, artifact)["verdict"] == "satisfied"
    dependent = world.call("POST", "tasks", {"project_id": world.project, "title": "Consume"})
    assert gate(world, dependent, done)["approved"] is True
    store = world.app.state.store
    with store.transaction():
        # Rewrite the task's execution identity (re-execution) without touching artifacts.
        row = store.raw(done["id"], "task")
        body = json.loads(row["body"])
        body["execution"] = {**body["execution"], "id": uid("execution")}
        store.save("task", row["project"], row["owner"], body, expected=row["revision"])
    assert gate(world, dependent, done)["reason"] == "execution_binding_stale"
