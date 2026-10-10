"""Tests for the additive context assembly surface.

These tests own only ``hivemind/context_assembly.py``. They never load the full embedding
model: the default counter uses a lightweight locally cached tokenizer, and most tests
inject a deterministic counter so the exact budget accounting can be verified.
"""

import json
import pytest

from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from hivemind import context_assembly as ca
from hivemind import models as m
from hivemind.api import envelope, hash_token
from hivemind.storage import Problem, canonical, now, uid


def test_numeric_token_count_cycles_are_settled_with_inert_padding():
    packet = {"token_count": 0, "token_budget": 1000, "token_budget_remaining": 1000}
    def counter(text):
        body = json.loads(text)
        if body.get("token_count_padding"):
            return 502
        return 501 if body["token_count"] == 500 else 500
    assert ca._settle(packet, counter, lambda data: data) == 502
    assert counter(canonical(packet)) == packet["token_count"]
    assert packet["token_budget_remaining"] == 498


def test_nonconvergent_host_counter_fails_explicitly():
    packet = {"token_count": 0, "token_budget": 1000, "token_budget_remaining": 1000}
    with pytest.raises(Problem) as failure:
        ca._settle(packet, lambda text: json.loads(text)["token_count"] + 1, lambda data: data)
    assert failure.value.code == "token_counter_unstable"


def char_counter(text):
    """One token per character: exact, monotone, and easy to recheck in assertions."""
    return len(text)


def principal_for(world, token=None):
    store = world.app.state.store
    row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                           (hash_token(token or world.owner),)).fetchone()
    return dict(row)


def put(store, kind, project, owner, body, private=None):
    resource_id = body["id"]
    revision = body.get("revision", 1)
    seq = store.tick(project)
    encoded = canonical({**body, "revision": revision, "updated_at": now(), "ingestion_seq": seq})
    store.db.execute(
        "INSERT INTO resources(id,kind,project,owner,private_agent,revision,seq,body) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (resource_id, kind, project, owner, private, revision, seq, encoded))
    store.db.execute("INSERT INTO versions VALUES(?,?,?)", (resource_id, revision, encoded))
    return json.loads(encoded)


def memory_body(project, run, *, title, text, status="verified", scope_kind="project",
                scope_owner=None, private=False, expires_at=None, invalidation_reason=None,
                active_dispute_ids=None, validity=None, evidence=None):
    body = {
        "id": uid("memory"), "project_id": project, "title": title,
        "content": {"media_type": "text/plain", "text": text},
        "evidence_refs": evidence if evidence is not None else [
            {"kind": "run", "id": run["id"], "revision": run["revision"]}],
        "validity": validity or {}, "expires_at": expires_at, "category": "semantic",
        "scope_kind": scope_kind, "scope_owner_id": scope_owner or project,
        "private": private, "status": status, "origin": "explicit", "created_at": now(),
    }
    if invalidation_reason:
        body["invalidation_reason"] = invalidation_reason
    if active_dispute_ids:
        body["active_dispute_ids"] = active_dispute_ids
    return body


def request_for(world, agent_id, *, query="", budget=6000, include=None, requester_run_id=None,
                require_verified=True, target_tokenizer=None):
    return m.ContextQuery(
        project_id=world.project,
        agent_selector=m.Selector(agent_ids=[agent_id], active_only=False),
        query=query, token_budget=budget, require_verified_memory=require_verified,
        include=include or ["ledger", "work_claims", "tasks", "board", "memory", "artifacts",
                            "graph"],
        requester_run_id=requester_run_id, target_tokenizer=target_tokenizer)


def item_ids(packet):
    return [item["resource_id"] for item in packet["items"]]


def test_default_counter_is_real_or_honestly_labeled(world):
    ca.reset_tokenizer_cache()
    counter = ca.default_counter()
    assert counter.id
    if ca.local_tokenizer_available():
        assert counter.method == "local_tokenizer"
        assert counter.fallback is False
        assert counter("") == 0
        sample = "the quick brown fox jumps over the lazy dog"
        # A real tokenizer is not a byte count.
        assert 0 < counter(sample) < len(sample.encode())
    else:
        assert counter.method == "conservative_utf8_bytes_upper_bound"
        assert counter.fallback is True
        assert counter("abc") == 3
    request = request_for(world, "agent")
    meta = ca._tokenization_meta(counter, request)
    assert meta["covers"].startswith("full response envelope")
    assert meta["method"] in ("local_tokenizer", "conservative_utf8_bytes_upper_bound")


def test_injected_counter_envelope_inclusive_exact_budget(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Counter worker")
    run = world.run(agent)
    for index in range(40):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Memory {index}", text="alpha beta gamma delta " * 4))
    counter = char_counter
    request = request_for(world, agent["id"], query="alpha", budget=5000)
    packet = ca.assemble(world.app.state.service, principal, request, counter=counter)

    measured = counter(canonical(ca.default_envelope(packet)))
    assert packet["token_count"] == measured
    assert packet["token_count"] <= request.token_budget
    assert packet["token_budget_remaining"] == request.token_budget - packet["token_count"]
    assert packet["items"]
    assert packet["truncated"] is True
    assert packet["tokenization"]["method"] == "injected"
    assert packet["tokenization"]["exact_for_target_tokenizer"] is True
    # Every item's own count is also measured, never estimated.
    for item in packet["items"]:
        assert item["token_count"] == counter(canonical(item))


def test_scan_finds_relevant_record_older_than_recent_thousand(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Scanner")
    run = world.run(agent)
    relevant = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra migration", text="zebra migration unique relevant"))
    for index in range(1200):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Filler {index}", text=f"common filler content {index}"))
    counter = char_counter
    request = request_for(world, agent["id"], query="zebra", budget=32000)
    packet = ca.assemble(world.app.state.service, principal, request, counter=counter)

    recent = {body["id"] for body in store.list("memory", world.project, principal, 1000)}
    assert relevant["id"] not in recent  # Store.list really does drop it
    assert relevant["id"] in item_ids(packet)  # the SQL scan does not
    assert packet["scan"]["scanned"]["memory"] >= 1201
    assert packet["items"][0]["resource_id"] == relevant["id"]
    assert packet["token_count"] <= request.token_budget


def test_private_expired_disputed_and_stale_memory_excluded(world):
    store = world.app.state.store
    owner_agent = world.agent("Memory owner")
    other_agent = world.agent("Other agent")
    run = world.run(owner_agent)
    token = world.credential(owner_agent)
    principal = principal_for(world, token)
    assert principal["role"] == "agent" and principal["agent"] == owner_agent["id"]

    valid = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Valid memory", text="valid current memory"))
    private = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Private memory", text="private memory", private=True),
        private=other_agent["id"])
    expired = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Expired memory", text="expired memory",
        expires_at="2000-01-01T00:00:00Z"))
    disputed = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Disputed memory", text="disputed memory",
        active_dispute_ids=["dispute_1"]))
    not_yet = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Future memory", text="not yet valid memory",
        validity={"valid_from": "2999-01-01T00:00:00Z"}))
    stale = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Stale memory", text="stale evidence memory",
        evidence=[{"kind": "run", "id": run["id"], "revision": 999}]))
    invalidated = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Invalidated memory", text="invalidated memory",
        invalidation_reason="source_deleted"))

    request = request_for(world, owner_agent["id"], include=["memory"], budget=32000)
    packet = ca.assemble(world.app.state.service, principal, request, counter=char_counter)
    found = set(item_ids(packet))
    assert valid["id"] in found
    for excluded in (private, expired, disputed, not_yet, stale, invalidated):
        assert excluded["id"] not in found


def test_ranking_prioritizes_task_claim_memory_and_source(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Ranker")
    run = world.run(agent)
    scope = world.scope()
    world.claim(agent, run, scope)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Zebra task"})
    memory = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra memory", text="zebra navigation memory"))
    post = world.post("Zebra navigation finding")

    request = request_for(world, agent["id"], query="zebra", budget=32000)
    packet = ca.assemble(world.app.state.service, principal, request, counter=char_counter)
    categories = [item["category"] for item in packet["items"]]
    assert categories[:4] == ["task", "work_claim", "memory", "board_post"]
    ranked = item_ids(packet)
    assert ranked[0] == task["id"]
    assert memory["id"] in ranked and post["id"] in ranked
    assert packet["items"][0]["bindings"]["task_id"] is None or isinstance(
        packet["items"][0]["bindings"]["task_id"], str)
    # Deterministic ranking: identical inputs give identical item order.
    again = ca.assemble(world.app.state.service, principal, request, counter=char_counter)
    assert item_ids(again) == ranked
    assert all(item["untrusted_evidence"] is True for item in packet["items"])


def test_compaction_truncates_oversized_content_within_budget(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Compactor")
    run = world.run(agent)
    huge = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Huge memory", text="token " * 5000))
    counter = char_counter
    request = request_for(world, agent["id"], include=["memory"], budget=4000)
    packet = ca.assemble(world.app.state.service, principal, request, counter=counter)
    assert packet["token_count"] <= request.token_budget
    item = next(item for item in packet["items"] if item["resource_id"] == huge["id"])
    assert item["content_truncated"] is True
    assert len(item["data"]["content"]) < len("token " * 5000)
    assert item["token_count"] <= 2048


def test_install_registers_endpoint_with_validation_and_envelope(world):
    store = world.app.state.store
    agent = world.agent("Endpoint worker")
    run = world.run(agent)
    principal = principal_for(world)
    put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Endpoint memory", text="endpoint assembly memory"))

    app = FastAPI()

    def principal_dependency(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="authentication_required")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="authentication_required")
        return dict(row)

    ca.install(app, world.app.state.service, principal_dependency, None, envelope)
    client = TestClient(app)
    body = {
        "project_id": world.project,
        "agent_selector": {"agent_ids": [agent["id"]], "active_only": False},
        "query": "endpoint",
        "token_budget": 4000,
        "include": ["memory", "work_claims", "tasks"],
    }
    assert client.post("/v0.1/context:assemble", json=body).status_code == 401
    response = client.post("/v0.1/context:assemble", json=body,
                           headers={"Authorization": "Bearer " + world.owner})
    assert response.status_code == 200
    payload = response.json()
    assert payload["protocol"] == "hivemind" and payload["request_id"]
    packet = payload["data"]
    assert packet["bindings"]["project_id"] == world.project
    assert packet["bindings"]["requester_principal_id"] == principal["id"]
    assert packet["token_count"] <= packet["token_budget"]
    assert packet["evidence_notice"] == ca.EVIDENCE_NOTICE
    assert packet["tokenization"]["covers"].startswith("full response envelope")
    bad = client.post("/v0.1/context:assemble", json={"project_id": world.project},
                      headers={"Authorization": "Bearer " + world.owner})
    assert bad.status_code == 422
