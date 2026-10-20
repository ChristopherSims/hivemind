"""Real-local-service tests for recipient-accepted structured handoffs.

No paid calls, no external network: the canonical service runs in-process via
``world``/``TestClient`` and the module is installed explicitly (the parent
shared wiring owns default registration).
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import Request

from hivemind.api import hash_token
from hivemind.storage import Problem, uid
from hivemind.structured_handoffs import (OPERATION_DESCRIPTORS, RunTaskContext,
                                          StructuredHandoffPolicy,
                                          consume_accepted_context,
                                          descriptors, install)

from test_task_execution import checkpoint, setup, start


def principal_for(app):
    """Test-local equivalent of api.create_app's principal dependency."""

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            row = app.state.store.db.execute(
                "SELECT * FROM principals WHERE token_hash=?",
                (hash_token(auth[7:]),)).fetchone()
            if row:
                return dict(row)
        raise Problem("authentication_required", 401)

    return principal


def install_on(world, **kwargs):
    app = world.app

    def command(p, operation, key, body, function):
        payload = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else body
        return {"data": app.state.service.mutate(p, operation, key, payload, function)}

    return install(app, app.state.service, principal_for(app), command,
                   lambda data: {"data": data}, **kwargs)


@pytest.fixture
def handoffs(world):
    install_on(world)
    return world


@pytest.fixture
def transfer_handoffs(world):
    install_on(world, policy=StructuredHandoffPolicy(allow_ownership_transfer=True))
    return world


def executing(world):
    """A sender agent with a live, checkpointed task execution."""
    agent, token, run, scope, task = setup(world)
    task = start(world, task, run, token)
    task = world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(task, run), token)
    receiver = world.agent("Receiver")
    receiver_token = world.credential(receiver)
    receiver_run = world.run(receiver, receiver_token)
    return agent, token, run, task, receiver, receiver_token, receiver_run


def offer_body(task, run, receiver, receiver_run=None, **overrides):
    body = {"project_id": task["project_id"], "task_id": task["id"],
            "expected_task_revision": task["revision"], "source_run_id": run["id"],
            "source_execution_id": task["execution"]["id"],
            "recipient_agent_id": receiver["id"],
            "summary": "Export implementation is 40% done; see checkpoint"}
    if receiver_run is not None:
        body["recipient_run_id"] = receiver_run["id"]
    body.update(overrides)
    return body


def offer(world, task, run, receiver, token=None, **overrides):
    return world.call("POST", "structured-handoffs",
                      offer_body(task, run, receiver, **overrides), token)


def complete(world, task, run, token):
    """Canonically settle the execution: terminal completed checkpoint."""
    return world.call("POST", f"tasks/{task['id']}:checkpoint",
                      checkpoint(task, run, progress=100, status="completed"), token)


def settle_old_run(world, run):
    """Merely age the heartbeat: NEVER external quiescence proof."""
    store = world.app.state.store
    with store.transaction():
        row = store.raw(run["id"])
        value = json.loads(row["body"])
        value["last_seen_at"] = time.time() - 60
        store.save("run", row["project"], row["owner"], value, expected=value["revision"])


def expire_claims(world):
    store = world.app.state.store
    with store.transaction():
        rows = store.db.execute(
            "SELECT * FROM resources WHERE kind='work_claim' AND deleted=0").fetchall()
        for row in rows:
            value = json.loads(row["body"])
            value["expires_at"] = time.time() - 1
            store.save("work_claim", row["project"], row["owner"], value, expected=value["revision"])


def consume_body(task, run):
    return {"run_id": run["id"], "task_id": task["id"],
            "execution_id": task["execution"]["id"],
            "expected_task_revision": task["revision"]}


# --------------------------------------------------------------------- wiring
def test_install_returns_conventional_endpoints_models_descriptors_and_hook(world):
    original = world.app.state.service.structured_handoffs
    assert original is world.app.state.structured_handoffs
    result = install_on(world)
    assert world.app.state.service.structured_handoffs is not original
    assert world.app.state.service.structured_handoffs is world.app.state.structured_handoffs
    assert set(result["models"]) == {"HandoffOffer", "HandoffAccept", "HandoffReject",
                                     "HandoffRevoke", "RunTaskContext"}
    assert "POST /v1/structured-handoffs:consume" in result["endpoints"]
    assert "POST /v1/structured-handoffs/{handoff_id}:accept" in result["endpoints"]
    described = descriptors()
    assert described == OPERATION_DESCRIPTORS
    for descriptor in described.values():
        for field in ("purpose", "input", "output", "authority", "side_effects",
                      "limits", "retry_safety"):
            assert descriptor[field], field


def test_policy_is_strict_and_transfer_action_is_default_off(handoffs):
    with pytest.raises(ValueError):
        StructuredHandoffPolicy(allow_ownership_transfer=1)
    with pytest.raises(ValueError):
        StructuredHandoffPolicy(max_open_offers_per_task=0)
    with pytest.raises(ValueError):
        StructuredHandoffPolicy(max_open_offers_per_task=True)
    _, token, run, task, receiver, _, receiver_run = executing(handoffs)
    handoffs.call("POST", "structured-handoffs",
                  offer_body(task, run, receiver, receiver_run, transfer="ownership"),
                  token, status=403)


# -------------------------------------------------------------- offer/accept
def test_two_agents_offer_and_accept_preserves_all_ownership_invariants(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    proof = handoffs.post("Export design note")
    made = offer(handoffs, task, run, receiver, token,
                 context_refs=[{"kind": "board_post", "id": proof["id"], "revision": 1}],
                 unresolved_questions=["Which formats are in scope?"])
    assert made["state"] == "pending" and made["sender_agent_id"] != receiver["id"]
    assert made["checkpoint"]["progress"] == 40
    assert made["checkpoint"]["summary"] == "Export implementation in progress"
    assert made["context_available"] is True
    assert made["freshness"] == {"checked": True, "stale": False, "reasons": []}
    listed = handoffs.call("GET", f"structured-handoffs?project_id={handoffs.project}"
                                  f"&task_id={task['id']}&state=pending", token=receiver_token)
    assert [item["id"] for item in listed] == [made["id"]]
    accepted = handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                             {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                             receiver_token, key="accept-1")
    assert accepted["state"] == "accepted"
    assert accepted["decision"]["recipient_run_id"] == receiver_run["id"]
    assert accepted["decision"]["ownership"] == {"requested": False, "effected": False}
    # Idempotent replay returns the same canonical decision; a new key cannot re-accept.
    assert handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                         {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                         receiver_token, key="accept-1")["id"] == made["id"]
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 2, "recipient_run_id": receiver_run["id"]},
                  receiver_token, status=409)
    # Acceptance changed no ownership state: task still running under the old
    # execution, the source run still owns the task, and the claim stays active.
    current = handoffs.call("GET", f"tasks/{task['id']}", token=token)
    assert current["status"] == "running"
    assert current["execution"]["id"] == task["execution"]["id"]
    assert handoffs.call("GET", f"runs/{run['id']}", token=token)["task_id"] == task["id"]
    store = handoffs.app.state.store
    claims = store.db.execute(
        "SELECT body FROM resources WHERE kind='work_claim' AND deleted=0").fetchall()
    assert any(json.loads(row["body"])["run_id"] == run["id"] for row in claims)


def test_offer_fences_revision_execution_identity_cross_project_and_refs(handoffs):
    agent, token, run, task, receiver, _, receiver_run = executing(handoffs)
    body = offer_body(task, run, receiver)
    stranger = handoffs.credential(handoffs.agent("Stranger"))
    handoffs.call("POST", "structured-handoffs", body, stranger, status=403)  # not the sender agent
    handoffs.call("POST", "structured-handoffs",
                  {**body, "expected_task_revision": task["revision"] + 1}, token, status=412)
    handoffs.call("POST", "structured-handoffs",
                  {**body, "source_execution_id": uid("execution")}, token, status=409)
    handoffs.call("POST", "structured-handoffs",
                  {**body, "recipient_agent_id": agent["id"]}, token, status=422)
    foreign = handoffs.call("POST", "structured-handoffs",
                            {**body, "project_id": uid("project")}, token, status=404)
    assert foreign["error"]["code"] == "not_found"
    handoffs.call("POST", "structured-handoffs",
                  {**body, "recipient_run_id": run["id"]}, token, status=409)  # run is the sender's
    handoffs.call("POST", "structured-handoffs",
                  {**body, "required_capabilities": ["never-registered"]}, token, status=409)
    handoffs.call("POST", "structured-handoffs",
                  {**body, "context_refs": [{"kind": "board_post", "id": uid("post"), "revision": 1}]},
                  token, status=404)
    proof = handoffs.post("Pinned at revision one")
    handoffs.call("POST", "structured-handoffs",
                  {**body, "context_refs": [{"kind": "board_post", "id": proof["id"], "revision": 99}]},
                  token, status=409)
    handoffs.call("POST", "structured-handoffs",
                  {**body, "context_refs": [{"kind": "task", "id": task["id"], "revision": task["revision"]}]},
                  token, status=422)
    # Unknown fields, bad literals and oversize question lists are refused by schema.
    handoffs.call("POST", "structured-handoffs", {**body, "mystery": True}, token, status=422)
    handoffs.call("POST", "structured-handoffs", {**body, "transfer": "maybe"}, token, status=422)
    handoffs.call("POST", "structured-handoffs",
                  {**body, "unresolved_questions": [f"q{i}" for i in range(9)]}, token, status=422)


def test_accept_fences_recipient_run_identity_and_authority(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    made = offer(handoffs, task, run, receiver, token, recipient_run_id=receiver_run["id"])
    other = handoffs.agent("Bystander")
    other_token = handoffs.credential(other)
    other_run = handoffs.run(other, other_token)
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 1, "recipient_run_id": other_run["id"]},
                  other_token, status=403)  # not the recipient agent
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 1, "recipient_run_id": other_run["id"]},
                  receiver_token, status=403)  # run does not belong to the recipient
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 1, "recipient_run_id": handoffs.run(receiver, receiver_token)["id"]},
                  receiver_token, status=409)  # offer pinned a different recipient run
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 99, "recipient_run_id": receiver_run["id"]},
                  receiver_token, status=412)


def test_reject_is_recipient_only_and_final(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    made = offer(handoffs, task, run, receiver, token)
    handoffs.call("POST", f"structured-handoffs/{made['id']}:reject",
                  {"expected_revision": 1, "reason": "Not mine"}, token, status=403)
    rejected = handoffs.call("POST", f"structured-handoffs/{made['id']}:reject",
                             {"expected_revision": 1, "reason": "Missing rollout plan"},
                             receiver_token)
    assert rejected["state"] == "rejected" and rejected["decision"]["reason"] == "Missing rollout plan"
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 2, "recipient_run_id": receiver_run["id"]},
                  receiver_token, status=409)


def test_revoke_pending_scrubs_all_context_and_blocks_accept(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    proof = handoffs.post("Revocable evidence")
    made = offer(handoffs, task, run, receiver, token,
                 context_refs=[{"kind": "board_post", "id": proof["id"], "revision": 1}],
                 deliverables=[{"kind": "board_post", "id": handoffs.post("Deliverable")["id"],
                                "revision": 1, "note": "Draft export"}],
                 unresolved_questions=["Open question"])
    handoffs.call("POST", f"structured-handoffs/{made['id']}:revoke",
                  {"expected_revision": 1, "reason": "x"}, receiver_token, status=403)
    revoked = handoffs.call("POST", f"structured-handoffs/{made['id']}:revoke",
                            {"expected_revision": 1, "reason": "Superseded by direct work"}, token)
    assert revoked["state"] == "revoked" and revoked["decision"]["previous_state"] == "pending"
    store = handoffs.app.state.store
    stored = json.loads(store.raw(made["id"])["body"])
    # No hidden cached context survives revocation: not even ref/deliverable identities.
    assert stored["summary"] == "" and stored["checkpoint"] is None
    assert stored["context_refs"] == [] and stored["deliverables"] == []
    assert stored["unresolved_questions"] == [] and stored["context_scrubbed"] is True
    read = handoffs.call("GET", f"structured-handoffs/{made['id']}", token=receiver_token)
    assert read["summary"] is None and read["checkpoint"] is None
    assert read["unresolved_questions"] == [] and read["context_note"] == "revoked_context_scrubbed"
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 2, "recipient_run_id": receiver_run["id"]},
                  receiver_token, status=409)
    handoffs.call("POST", f"structured-handoffs/{made['id']}:revoke",
                  {"expected_revision": 2, "reason": "again"}, token, status=409)


def test_stale_source_snapshot_fails_closed_at_read_and_accept(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    made = offer(handoffs, task, run, receiver, token)
    moved = handoffs.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(task, run), token)
    assert moved["revision"] != task["revision"]
    read = handoffs.call("GET", f"structured-handoffs/{made['id']}", token=receiver_token)
    assert read["freshness"]["stale"] and "task_revision_changed" in read["freshness"]["reasons"]
    # Context-derived payload is suppressed while any source is stale.
    assert read["context_available"] is False and read["checkpoint"] is None
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                  receiver_token, status=409)


def test_revoked_checkpoint_evidence_blocks_accept(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    proof = handoffs.post("Checkpoint evidence")
    task = handoffs.call("POST", f"tasks/{task['id']}:checkpoint",
                         checkpoint(task, run, evidence_refs=[{"kind": "board_post", "id": proof["id"]}]),
                         token)
    made = offer(handoffs, task, run, receiver, token)
    assert made["checkpoint"]["evidence_refs"] == [{"kind": "board_post", "id": proof["id"],
                                                    "revision": 1}]
    handoffs.call("DELETE", f"posts/{proof['id']}", {"expected_revision": 1})
    read = handoffs.call("GET", f"structured-handoffs/{made['id']}", token=receiver_token)
    assert "checkpoint_evidence_unavailable" in read["freshness"]["reasons"]
    denied = handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                           {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                           receiver_token, status=409)
    assert denied["error"]["code"] == "checkpoint_evidence_unavailable"


def test_private_checkpoint_evidence_blocks_accept_and_suppresses_reads(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    proof = handoffs.post("Checkpoint evidence")
    task = handoffs.call("POST", f"tasks/{task['id']}:checkpoint",
                         checkpoint(task, run, evidence_refs=[{"kind": "board_post",
                                                               "id": proof["id"]}]), token)
    made = offer(handoffs, task, run, receiver, token)
    assert handoffs.call("GET", f"structured-handoffs/{made['id']}",
                         token=receiver_token)["context_available"] is True
    # The checkpoint evidence becomes private to the sender after the offer.
    store = handoffs.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (run["agent_id"], proof["id"]))
    # The sender (who can still see it) keeps the full context; the recipient's
    # read suppresses all context-derived payload with a generic reason.
    assert handoffs.call("GET", f"structured-handoffs/{made['id']}",
                         token=token)["context_available"] is True
    read = handoffs.call("GET", f"structured-handoffs/{made['id']}", token=receiver_token)
    assert read["context_available"] is False and read["checkpoint"] is None
    assert set(read["freshness"]["reasons"]) == {"checkpoint_evidence_unavailable",
                                                 "context_ref_unavailable"}
    assert read["context_refs"] == [] and read["unresolved_questions"] == []
    denied = handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                           {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                           receiver_token, status=409)
    assert denied["error"]["code"] == "checkpoint_evidence_unavailable"


def test_private_artifact_refs_are_reauthorized_at_offer_and_accept(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    store = handoffs.app.state.store
    with store.transaction():
        admin = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        hidden = store.save("artifact", handoffs.project, admin["id"], {
            "id": uid("artifact"), "created_at": "2026-01-01T00:00:00Z",
            "project_id": handoffs.project, "title": "Private artifact",
            "content": {"media_type": "text/plain", "text": "hidden"},
            "run_id": None}, private=receiver["id"])  # visible to the recipient only
    ref = {"kind": "artifact", "id": hidden["id"], "revision": hidden["revision"]}
    # The sender cannot see it: the offer fails closed.
    handoffs.call("POST", "structured-handoffs",
                  offer_body(task, run, receiver, context_refs=[ref]), token, status=404)
    # An administrator can offer it, but the accepting recipient principal is
    # re-authorized: a ref private to a THIRD agent blocks the accept.
    with store.transaction():
        hidden_third = store.save("artifact", handoffs.project, admin["id"], {
            "id": uid("artifact"), "created_at": "2026-01-01T00:00:00Z",
            "project_id": handoffs.project, "title": "Third-party artifact",
            "content": {"media_type": "text/plain", "text": "hidden"},
            "run_id": None}, private=run["agent_id"])  # private to the SENDER
    made = handoffs.call("POST", "structured-handoffs",
                         offer_body(task, run, receiver, context_refs=[
                             {"kind": "artifact", "id": hidden_third["id"],
                              "revision": hidden_third["revision"]}]))
    assert made["state"] == "pending"
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                  receiver_token, status=404)


def test_unrelated_project_member_gets_a_blind_view(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    made = offer(handoffs, task, run, receiver, token,
                 unresolved_questions=["Which formats are in scope?"])
    bystander = handoffs.credential(handoffs.agent("Bystander"))
    blind = handoffs.call("GET", f"structured-handoffs/{made['id']}", token=bystander)
    # Identity/state only: no context-derived payload, no per-resource freshness.
    assert blind["state"] == "pending" and blind["context_available"] is False
    assert blind["freshness"] == {"checked": False, "stale": None, "reasons": []}
    for field in ("summary", "checkpoint", "context_refs", "deliverables",
                  "unresolved_questions", "decision", "consumed", "offered_by"):
        assert field not in blind, field
    listed = handoffs.call("GET", f"structured-handoffs?project_id={handoffs.project}",
                           token=bystander)
    assert listed[0]["context_available"] is False and "summary" not in listed[0]
    # Human project members are not the sender/recipient: also blind.
    human = handoffs.credential(role="human")
    assert handoffs.call("GET", f"structured-handoffs/{made['id']}",
                         token=human)["context_available"] is False


# ---------------------------------------------------------- ownership transfer
def test_ownership_accept_holds_while_source_effects_are_active(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    made = offer(transfer_handoffs, task, run, receiver, token,
                 recipient_run_id=receiver_run["id"], transfer="ownership")
    denied = transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                                    {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                                    receiver_token, status=409)
    assert denied["error"]["code"] == "source_effects_active"


def test_stale_heartbeat_and_unknown_source_always_hold(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    made = offer(transfer_handoffs, task, run, receiver, token,
                 recipient_run_id=receiver_run["id"], transfer="ownership")
    # Heartbeat absence is NEVER external quiescence proof.
    settle_old_run(transfer_handoffs, run)
    denied = transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                                    {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                                    receiver_token, status=409)
    assert denied["error"]["code"] == "source_effects_active"
    store = transfer_handoffs.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (run["id"],))
    denied = transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                                    {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                                    receiver_token, status=409)
    # Unknown liveness is never treated as "no effects" either.
    assert denied["error"]["code"] == "source_effects_active"


def test_canonical_terminal_checkpoint_is_the_only_settled_evidence(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    made = offer(transfer_handoffs, task, run, receiver, token,
                 recipient_run_id=receiver_run["id"], transfer="ownership")
    completed = complete(transfer_handoffs, task, run, token)
    accepted = transfer_handoffs.call(
        "POST", f"structured-handoffs/{made['id']}:accept",
        {"expected_revision": 1, "recipient_run_id": receiver_run["id"]}, receiver_token)
    ownership = accepted["decision"]["ownership"]
    assert ownership["requested"] is True and ownership["effected"] is False
    assert ownership["support"] == "settled_execution_evidence"
    # Ownership invariants: acceptance changed nothing about the task or runs.
    current = transfer_handoffs.call("GET", f"tasks/{task['id']}", token=token)
    assert current["revision"] == completed["revision"] and current["status"] == "completed"
    assert current["execution"]["id"] == task["execution"]["id"]


def test_explicit_task_handoff_is_intent_only_not_settled_evidence(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    made = offer(transfer_handoffs, task, run, receiver, token,
                 recipient_run_id=receiver_run["id"], transfer="ownership")
    settle_old_run(transfer_handoffs, run)
    handed = transfer_handoffs.call("POST", f"tasks/{task['id']}:handoff", {
        "expected_revision": task["revision"], "target_agent_id": receiver["id"],
        "target_run_id": receiver_run["id"], "reason": "Transfer interrupted local work"})
    assert handed["handoff"]["state"] == "pending"
    # The admin intent authorizes accepting context across the moved snapshot, but
    # the ownership request is explicitly labeled unverified, never a safe handoff.
    accepted = transfer_handoffs.call(
        "POST", f"structured-handoffs/{made['id']}:accept",
        {"expected_revision": 1, "recipient_run_id": receiver_run["id"]}, receiver_token)
    ownership = accepted["decision"]["ownership"]
    assert ownership["effected"] is False
    assert ownership["support"] == "intent_only_unverified"
    assert ownership["mechanism"] == "unsupported_without_settled_evidence"


def test_concurrent_ownership_accepts_have_exactly_one_winner(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    first = offer(transfer_handoffs, task, run, receiver, token,
                  recipient_run_id=receiver_run["id"], transfer="ownership")
    second = offer(transfer_handoffs, task, run, receiver, token,
                   recipient_run_id=receiver_run["id"], transfer="ownership")
    complete(transfer_handoffs, task, run, token)  # canonical settled evidence

    def take(handoff_id):
        response = transfer_handoffs.client.post(
            f"/v1/structured-handoffs/{handoff_id}:accept",
            json={"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
            headers={"Authorization": f"Bearer {receiver_token}", "Idempotency-Key": uid("accept")})
        return response.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(take, [first["id"], second["id"]])) == [200, 409]


# ------------------------------------------------------------- worker consume
def test_accepted_context_delivery_is_replayable_run_bound_and_reauthorized(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    proof = transfer_handoffs.post("Context evidence")
    made = offer(transfer_handoffs, task, run, receiver, token,
                 recipient_run_id=receiver_run["id"], transfer="ownership",
                 context_refs=[{"kind": "board_post", "id": proof["id"], "revision": 1}],
                 unresolved_questions=["Which formats are in scope?"])
    # Canonical order: admin handoff intent, recipient accept, then real dispatch.
    settle_old_run(transfer_handoffs, run)
    transfer_handoffs.call("POST", f"tasks/{task['id']}:handoff", {
        "expected_revision": task["revision"], "target_agent_id": receiver["id"],
        "target_run_id": receiver_run["id"], "reason": "Transfer interrupted local work"})
    transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                           {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                           receiver_token)
    expire_claims(transfer_handoffs)
    dispatched = transfer_handoffs.call(
        "POST", f"projects/{transfer_handoffs.project}/tasks:dispatch",
        {"agent_id": receiver["id"], "run_id": receiver_run["id"]}, receiver_token)
    task_now = dispatched["task"]
    assert task_now["handoff"]["state"] == "accepted"
    fences = consume_body(task_now, receiver_run)
    delivered = transfer_handoffs.call("POST", "structured-handoffs:consume", fences,
                                       receiver_token, key="consume-1")
    assert delivered["id"] == made["id"]
    context = delivered["context"]
    assert context["summary"] == "Export implementation is 40% done; see checkpoint"
    assert context["unresolved_questions"] == ["Which formats are in scope?"]
    assert context["context_refs"] == [{"kind": "board_post", "id": proof["id"], "revision": 1}]
    assert context["ownership"]["support"] == "intent_only_unverified"
    # Command receipt replay (same key) and fenced re-delivery (new key) both
    # return the same context, reauthorized fresh: a lost response loses nothing.
    assert transfer_handoffs.call("POST", "structured-handoffs:consume", fences,
                                  receiver_token, key="consume-1") == delivered
    assert transfer_handoffs.call("POST", "structured-handoffs:consume", fences,
                                  receiver_token, key="consume-2") == delivered
    # Delivery is bound to the exact recipient run and the exact current execution.
    other_run = transfer_handoffs.run(receiver, receiver_token)
    transfer_handoffs.call("POST", "structured-handoffs:consume",
                           consume_body(task_now, other_run), receiver_token, status=409)
    transfer_handoffs.call("POST", "structured-handoffs:consume",
                           {**fences, "execution_id": uid("execution")}, receiver_token, status=409)
    transfer_handoffs.call("POST", "structured-handoffs:consume",
                           {**fences, "expected_task_revision": fences["expected_task_revision"] + 1},
                           receiver_token, status=412)
    # Replay reauthorizes current visibility: revoking the source evidence refuses
    # even the canonical receipt replay instead of serving cached context.
    transfer_handoffs.call("DELETE", f"posts/{proof['id']}", {"expected_revision": 1})
    transfer_handoffs.call("POST", "structured-handoffs:consume", fences,
                           receiver_token, key="consume-1", status=404)
    # Consumed deliveries are final: the sender can no longer revoke.
    current = transfer_handoffs.call("GET", f"structured-handoffs/{made['id']}", token=token)
    transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:revoke",
                           {"expected_revision": current["revision"], "reason": "too late"},
                           token, status=409)


def test_accepted_unconsumed_offer_revocation_blocks_consumption(transfer_handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(transfer_handoffs)
    made = offer(transfer_handoffs, task, run, receiver, token,
                 recipient_run_id=receiver_run["id"], transfer="ownership")
    transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                           {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                           receiver_token, status=409)  # active source: holds
    complete(transfer_handoffs, task, run, token)
    transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                           {"expected_revision": 1, "recipient_run_id": receiver_run["id"]},
                           receiver_token)
    revoked = transfer_handoffs.call("POST", f"structured-handoffs/{made['id']}:revoke",
                                     {"expected_revision": 2, "reason": "Context withdrawn"}, token)
    assert revoked["state"] == "revoked" and revoked["decision"]["previous_state"] == "accepted"
    stored = json.loads(transfer_handoffs.app.state.store.raw(made["id"])["body"])
    assert stored["context_refs"] == [] and stored["checkpoint"] is None
    assert stored["summary"] == "" and stored["deliverables"] == []


def test_consume_fences_the_exact_current_run_and_task(handoffs):
    _, token, run, task, receiver, receiver_token, receiver_run = executing(handoffs)
    made = offer(handoffs, task, run, receiver, token)
    handoffs.call("POST", f"structured-handoffs/{made['id']}:accept",
                  {"expected_revision": 1, "recipient_run_id": receiver_run["id"]}, receiver_token)
    # The recipient run does not currently hold the task.
    handoffs.call("POST", "structured-handoffs:consume",
                  {"run_id": receiver_run["id"], "task_id": task["id"],
                   "execution_id": task["execution"]["id"],
                   "expected_task_revision": task["revision"]}, receiver_token, status=409)
    # The sender's run holds the task but is not the recipient: no context.
    delivered = handoffs.call("POST", "structured-handoffs:consume",
                              consume_body(task, run), token)
    assert delivered == {"id": None, "context": None}
    # The in-process helper enforces the same fences for the parent worker wiring.
    service = handoffs.app.state.service
    store = handoffs.app.state.store
    with store.transaction():
        principal = dict(store.db.execute(
            "SELECT * FROM principals WHERE agent=?", (receiver["id"],)).fetchone())
        with pytest.raises(Problem) as held:
            consume_accepted_context(service, principal, RunTaskContext(
                run_id=receiver_run["id"], task_id=task["id"],
                execution_id=task["execution"]["id"], expected_task_revision=task["revision"]))
        assert held.value.code == "run_task_mismatch"
