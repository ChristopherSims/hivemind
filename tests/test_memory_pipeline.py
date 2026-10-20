"""Memory pipeline extension: opt-in model proposals, advisory suggestions, freshness.

These tests own a small harness that installs the pipeline with the exact injected
contract the service uses (`install(app, service, principal, command, envelope)`), so
no shared module needs to change. The extractor and encoder are always injected local
stubs; no network, paid model, or heavy model is loaded.
"""

import contextlib
import itertools
import json
import secrets
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from hivemind import memory_pipeline as pipeline_module
from hivemind import models as m
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.memory_pipeline import (JOB_KIND, MARK_KIND, EnqueueRequest, LocalHashingEncoder,
                                      MemoryPipeline, PipelineConfig, ProcessRequest, git_snapshot,
                                      install, repository_identity)
from hivemind.service import Service
from hivemind.storage import Problem, Store, uid


def make_dependencies(store, service):
    """The same dependency shapes create_app injects: principal, command, envelope."""

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0", "data": data}

    def command(p, operation, key, request, function):
        payload = (request.model_dump(by_alias=True, exclude_none=True)
                   if isinstance(request, m.Model) else request)
        return envelope(service.mutate(p, operation, key, payload, function))

    return principal, command, envelope


class PipelineWorld:
    """The standard service app plus the pipeline extension, wired with the same contract."""

    def __init__(self, directory, **options):
        config = json.loads(initialize(directory).read_text())
        self.directory, self.project, self.owner = Path(directory), config["project_id"], config["token"]
        self.app = create_app(directory, workers=False)
        self.store, self.service = self.app.state.store, self.app.state.service
        principal, command, envelope = make_dependencies(self.store, self.service)
        self.pipeline = install(self.app, self.service, principal, command, envelope, **options)
        self.stack = contextlib.ExitStack()
        self.client = self.stack.enter_context(TestClient(self.app))

    def close(self):
        self.stack.close()

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner), "Idempotency-Key": key or uid("key")})
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Pipeline worker"):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name,
                                           "supported_actions": [], "scope_permit_enforcement": False})

    def credential(self, agent=None, role="agent"):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                                  (uid("principal"), hash_token(token), role, self.project,
                                   agent["id"] if agent else None, 0, 0))
        return token

    def principal_id(self, token):
        return self.store.db.execute("SELECT id FROM principals WHERE token_hash=?",
                                     (hash_token(token),)).fetchone()["id"]

    def run(self, agent, token=None):
        return self.call("POST", "runs", {"project_id": self.project, "agent_id": agent["id"]}, token)

    def board(self):
        return self.call("GET", f"boards?project_id={self.project}")[0]

    def post(self, text="The export uses UTC timestamps", token=None):
        return self.call("POST", f"boards/{self.board()['id']}/posts", {
            "client_post_id": uid("client"), "kind": "finding",
            "content": {"media_type": "text/plain", "text": text}}, token)

    def artifact(self, title, text, token=None):
        return self.call("POST", "artifacts", {
            "project_id": self.project, "title": title,
            "content": {"media_type": "text/markdown", "text": text}}, token)

    def memory(self, post, title="Format", token=None, **fields):
        return self.call("POST", "memories", {
            "project_id": self.project, "title": title,
            "content": {"media_type": "text/plain", "text": post["content"]["text"]},
            "scope_kind": "project", "scope_owner_id": self.project,
            "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": post["revision"]}],
            **fields}, token)

    def enqueue(self, body, token=None, **kwargs):
        return self.call("POST", "memory-pipeline:enqueue", body, token, **kwargs)

    def process(self, body, token=None, **kwargs):
        return self.call("POST", "memory-pipeline:process", body, token, **kwargs)

    def freshness(self, body, token=None, **kwargs):
        return self.call("POST", "memory-pipeline:freshness", body, token, **kwargs)

    def retrieve(self, body, token=None, **kwargs):
        return self.call("POST", "memory-pipeline:retrieve", body, token, **kwargs)

    def suggestions(self, token=None, **query):
        suffix = "".join(f"&{key}={value}" for key, value in query.items())
        return self.call("GET", f"memory-pipeline:suggestions?project_id={self.project}{suffix}", token=token)

    def jobs(self, token=None, **query):
        suffix = "".join(f"&{key}={value}" for key, value in query.items())
        return self.call("GET", f"memory-pipeline:jobs?project_id={self.project}{suffix}", token=token)

    def prune_private(self, resource_id, agent_id):
        with self.store.transaction():
            self.store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (agent_id, resource_id))

    def marks(self):
        return self.store.db.execute("SELECT count(*) AS n FROM resources WHERE kind=?", (MARK_KIND,)).fetchone()["n"]


def scripted(*outputs):
    """Local model stub: one scripted output per call. Never contacts anything."""
    queue, calls = list(outputs), []

    def extractor(prompt):
        calls.append(prompt)
        assert queue, "extractor called more often than scripted"
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    extractor.calls = calls
    extractor.model_id = "test-local-extractor"
    return extractor


def arm(world, *outputs):
    extractor = scripted(*outputs)
    world.pipeline.extractor = extractor
    return extractor


def proposal_json(source_id, text, title=None, **extra):
    proposal = {"title": title or text[:60], "text": text, "category": "semantic",
                "source_ref_ids": [source_id], "confidence": 0.5}
    proposal.update(extra)
    return json.dumps({"proposals": [proposal]})


def enqueue_body(world, agent, run, source, **overrides):
    body = {"project_id": world.project, "agent_id": agent["id"], "requester_run_id": run["id"],
            "source_refs": [{"kind": "board_post", "id": source["id"], "revision": source["revision"]}]}
    body.update(overrides)
    return body


def setup(world, text="The export uses UTC timestamps", **overrides):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    source = world.post(text, token)
    return agent, token, run, source, enqueue_body(world, agent, run, source, **overrides)


@pytest.fixture
def builder(tmp_path):
    counter = itertools.count()
    worlds = []

    def build(**options):
        directory = tmp_path / f"data-{next(counter)}"
        directory.mkdir()
        world = PipelineWorld(directory, **options)
        worlds.append(world)
        return world

    yield build
    for world in worlds:
        world.close()


def test_pipeline_is_disabled_by_default_and_missing_model_fails_explicitly(builder):
    world = builder()
    agent, token, run, source, body = setup(world)
    world.enqueue(body, token, status=409)
    world.process({"project_id": world.project}, token, status=409)
    world.call("POST", "memory-pipeline:tick", {"limit": 1}, status=409)
    world.call("GET", f"memory-pipeline:jobs?project_id={world.project}", token=token, status=409)
    world.call("GET", f"memory-pipeline:suggestions?project_id={world.project}", token=token, status=409)
    world.freshness({"project_id": world.project}, token, status=409)
    world.retrieve({"project_id": world.project}, token, status=409)
    status = world.call("GET", f"memory-pipeline:status?project_id={world.project}", token=token)
    assert status["enabled"] is False and status["model"]["configured"] is False
    assert status["encoder"] == "local-hash-v1" and status["status_effect"] == "none"
    assert world.store.db.execute("SELECT count(*) AS n FROM resources WHERE kind=?",
                                  (JOB_KIND,)).fetchone()["n"] == 0

    enabled = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(enabled)
    queued = enabled.enqueue(body, token)
    assert queued["created"] is True and queued["job"]["state"] == "pending"
    enabled.process({"project_id": enabled.project}, token, status=409)
    enabled.call("POST", "memory-pipeline:tick", {"limit": 1}, status=409)
    assert enabled.call("GET", f"memory-pipeline:status?project_id={enabled.project}",
                        token=token)["model"]["configured"] is False
    # Advisory surfaces work while model processing stays explicitly unavailable.
    assert enabled.jobs(token=token)["counts"]["pending"] == 1
    assert enabled.suggestions(token=token)["count"] == 0
    enabled.retrieve({"project_id": enabled.project}, token, status=503)


def test_processing_creates_unverified_private_model_proposals(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    extractor = arm(world, proposal_json(source["id"], "The export uses UTC timestamps"))
    queued = world.enqueue(body, token)
    job = queued["job"]
    assert job["state"] == "pending" and job["private"] is True and job["status_effect"] == "none"
    assert job["source_refs"][0]["revision"] == source["revision"]
    assert job["model_id"] == "test-local-extractor" and job["enqueued_by_agent"] == agent["id"]

    result = world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)
    assert result["processed"] == 1 and result["results"][0]["state"] == "done"
    memory_id = result["results"][0]["memories"][0]
    memory = world.call("GET", f"memories/{memory_id}", token=token)
    assert memory["status"] == "proposed" and memory["effective_status"] == "proposed"
    assert memory["origin"] == "extracted" and memory["private"] is True
    assert memory["scope_kind"] == "agent" and memory["scope_owner_id"] == agent["id"]
    assert memory.get("verified_by") is None and memory.get("reviewed_by") is None
    assert memory["evidence_refs"] == [{"kind": "board_post", "id": source["id"],
                                        "revision": source["revision"]}]
    assert memory["content"]["media_type"] == "text/plain"
    assert memory["content"]["text"] == "The export uses UTC timestamps"
    extraction = memory["extraction"]
    assert extraction["method"] == "model-proposal-v1" and extraction["model_id"] == "test-local-extractor"
    assert extraction["job_id"] == job["id"] and extraction["agent_id"] == agent["id"]
    assert extraction["status"] == "unverified_proposal" and len(extraction["output_digest"]) == 64
    assert "source_fingerprint" in extraction and extraction["requester_run_id"] == run["id"]
    assert len(extractor.calls) == 1 and source["id"] in extractor.calls[0]

    # Nothing auto-verifies, quarantines, disputes, supersedes, or notifies.
    assert world.call("GET", f"memories?project_id={world.project}", token=token)[0]["status"] == "proposed"
    assert world.store.db.execute(
        "SELECT count(*) AS n FROM resources WHERE kind IN ('notification','memory_dispute','memory_proposal')"
    ).fetchone()["n"] == 0
    stored = world.jobs(token=token)["jobs"][0]
    assert stored["state"] == "done" and stored["outputs"]["memories"] == [memory_id]
    assert stored["outputs"]["model_output_digest"] == extraction["output_digest"]
    assert "proposals" not in stored and "UTC timestamps" not in json.dumps(stored)

    # Re-enqueue and reprocess are idempotent.
    assert world.enqueue(body, token)["job"]["id"] == job["id"]
    repeat = world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)
    assert repeat["results"][0]["skipped"] is True and not repeat["results"][0]["memories"]
    assert len(world.call("GET", f"memories?project_id={world.project}", token=token)) == 1


def test_private_sources_and_jobs_never_leave_their_scope(builder):
    world = builder(config=PipelineConfig(enabled=True))
    owner_agent, owner_token, _, _, owner_body = setup(world)
    secret = world.artifact("Hidden notes", "The internal token rotates nightly", owner_token)
    world.prune_private(secret["id"], owner_agent["id"])
    peer = world.agent("Peer worker")
    peer_token = world.credential(peer)

    peer_body = {"project_id": world.project, "agent_id": peer["id"],
                 "source_refs": [{"kind": "artifact", "id": secret["id"]}]}
    world.enqueue(peer_body, peer_token, status=404)
    private_ref = [{"kind": "artifact", "id": secret["id"]}]
    world.enqueue({**owner_body, "source_refs": private_ref, "scope_kind": "project",
                   "scope_owner_id": world.project}, owner_token, status=403)
    world.enqueue({**owner_body, "source_refs": private_ref, "scope_kind": "task",
                   "scope_owner_id": world.project}, owner_token, status=404)

    job = world.enqueue({**owner_body, "source_refs": private_ref}, owner_token)["job"]
    assert job["private"] is True
    assert world.jobs(token=peer_token)["jobs"] == []
    arm(world, proposal_json(secret["id"], "The internal token rotates nightly"))
    world.process({"project_id": world.project, "job_ids": [job["id"]]}, peer_token, status=404)

    done = world.process({"project_id": world.project, "job_ids": [job["id"]]}, owner_token)
    memory_id = done["results"][0]["memories"][0]
    memory = world.call("GET", f"memories/{memory_id}", token=owner_token)
    assert memory["private"] is True and memory["scope_kind"] == "agent"
    world.call("GET", f"memories/{memory_id}", token=peer_token, status=404)


def test_changed_and_deleted_sources_fail_the_job_without_proposals(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    job = world.enqueue(body, token)["job"]
    world.call("PATCH", f"posts/{source['id']}", {
        "expected_revision": source["revision"],
        "content": {"media_type": "text/plain", "text": "The export now uses ISO timestamps"}}, token)
    extractor = arm(world, proposal_json(source["id"], "never used"))
    failed = world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)
    # A pinned revision that no longer exists can never succeed, so the job is terminal.
    assert failed["results"][0]["state"] == "dead_letter"
    assert failed["results"][0]["error"] == "stale_evidence"
    assert not failed["results"][0]["memories"] and not extractor.calls
    assert not world.call("GET", f"memories?project_id={world.project}", token=token)

    rebased = world.enqueue({**body, "source_refs": [{"kind": "board_post", "id": source["id"],
                                                      "revision": 2}]}, token)
    assert rebased["created"] is True and rebased["job"]["id"] != job["id"]
    assert rebased["fingerprint"] != job["fingerprint"]
    # The superseded revision cannot be re-asserted for the same source.
    world.enqueue(body, token, status=409)

    world.call("DELETE", f"posts/{source['id']}", {"expected_revision": 2}, token)
    terminal = world.process({"project_id": world.project, "job_ids": [rebased["job"]["id"]]}, token)
    assert terminal["results"][0]["state"] == "dead_letter"
    assert terminal["results"][0]["error"] == "source_unavailable"

    # Stale revision conflict still resolves explicitly at enqueue time.
    world.enqueue({**body, "source_refs": [{"kind": "board_post", "id": source["id"],
                                            "revision": 1}]}, token, status=404)


def test_malformed_model_output_is_rejected_with_bounded_reasons(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, _, _ = setup(world)
    cases = [
        ("not json", lambda s: "I cannot help with that.", "not_json"),
        ("top level list", lambda s: json.dumps([{"proposals": []}]), "not_an_object"),
        ("wrong top level key", lambda s: json.dumps({"items": []}), "unexpected_keys"),
        ("extra top level key", lambda s: json.dumps({"proposals": [], "notes": "hi"}), "unexpected_keys"),
        ("proposals not a list", lambda s: json.dumps({"proposals": {}}), "proposals_not_a_list"),
        ("extra proposal key", lambda s: json.dumps({"proposals": [
            {"title": "t", "text": "x", "source_ref_ids": [s], "tool": "run shell"}]}),
         "proposal_unexpected_keys"),
        ("unpinned source", lambda s: proposal_json("post_forged", "forged"), "unpinned_source_ref"),
        ("no source refs", lambda s: proposal_json(s, "x", source_ref_ids=[]), "invalid_source_ref_ids"),
        ("category injection", lambda s: proposal_json(s, "x", category="shell"), "invalid_category"),
        ("confidence out of range", lambda s: proposal_json(s, "x", confidence=4.2), "invalid_confidence"),
        ("nonstring title", lambda s: proposal_json(s, "x", title=["t"]), "invalid_title"),
        ("control characters", lambda s: json.dumps({"proposals": [
            {"title": "t", "text": "a\u0000b", "source_ref_ids": [s]}]}), "control_characters"),
        ("fenced garbage", lambda s: "```json\nnot json\n```", "not_json"),
    ]
    for label, render, reason in cases:
        source = world.post(f"Evidence for {label}", token)
        arm(world, render(source["id"]))
        job = world.enqueue(enqueue_body(world, agent, run, source), token)["job"]
        entry = world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)["results"][0]
        assert entry["state"] == "failed", label
        assert entry["error"] == reason, label
        assert not entry["memories"], label
        stored = world.jobs(token=token)["jobs"][0]
        assert "proposals" not in stored and stored["attempts"] == 1, label
    assert not world.call("GET", f"memories?project_id={world.project}", token=token)


def test_oversized_output_budget_and_extractor_failure_are_terminal(builder):
    world = builder(config=PipelineConfig(enabled=True, max_output_bytes=512, max_attempts=2))
    agent, token, run, source, body = setup(world)
    arm(world, json.dumps({"proposals": [], "padding": "x" * 4000}))
    job = world.enqueue(body, token)["job"]
    first = world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)["results"][0]
    assert first["state"] == "failed" and first["error"] == "output_too_large"
    second = world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)["results"][0]
    assert second["state"] == "dead_letter" and second["attempts"] == 2
    world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)

    raise_world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(raise_world)
    arm(raise_world, RuntimeError("model offline"))
    failed = raise_world.enqueue(body, token)["job"]
    entry = raise_world.process({"project_id": raise_world.project,
                                 "job_ids": [failed["id"]]}, token)["results"][0]
    assert entry["error"] == "extractor_error" and not entry["memories"]
    assert raise_world.jobs(token=token)["jobs"][0]["error_type"] == "RuntimeError"

    budget = builder(config=PipelineConfig(enabled=True, max_source_bytes=64))
    agent, token, run, source, body = setup(budget, "short fact")
    arm(budget, proposal_json(source["id"], "y" * 500))
    entry = budget.process({"project_id": budget.project,
                            "job_ids": [budget.enqueue(body, token)["job"]["id"]]}, token)["results"][0]
    assert entry["error"] == "text_too_large" and not entry["memories"]

    limited = builder(config=PipelineConfig(enabled=True, max_candidates=1))
    agent, token, run, source, body = setup(limited)
    arm(limited, json.dumps({"proposals": [{"title": "a", "text": "x", "source_ref_ids": [source["id"]]},
                                          {"title": "b", "text": "y", "source_ref_ids": [source["id"]]}]}))
    entry = limited.process({"project_id": limited.project,
                             "job_ids": [limited.enqueue(body, token)["job"]["id"]]}, token)["results"][0]
    assert entry["error"] == "too_many_proposals" and not entry["memories"]


def test_duplicate_and_contradiction_suggestions_are_advisory_only(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, first, body = setup(world)
    second = world.post("The export uses UTC timestamps", token)
    third = world.post("The export does not use UTC timestamps", token)
    arm(world, proposal_json(first["id"], "The export uses UTC timestamps"),
        proposal_json(second["id"], "The export uses UTC timestamps"),
        proposal_json(third["id"], "The export does not use UTC timestamps"))
    for source in (first, second, third):
        job = world.enqueue({**body, "source_refs": [
            {"kind": "board_post", "id": source["id"], "revision": source["revision"]}]}, token)["job"]
        world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)

    listed = world.suggestions(token=token)
    assert {item["kind"] for item in listed["suggestions"]} == {"duplicate", "contradiction"}
    duplicate = [item for item in listed["suggestions"] if item["kind"] == "duplicate"][0]
    contradiction = [item for item in listed["suggestions"] if item["kind"] == "contradiction"][0]
    assert duplicate["similarity"] >= 0.92 and duplicate["state"] == "proposed"
    assert contradiction["similarity"] < 0.92 and "negation_conflict" in contradiction["signals"]
    assert all(item["state"] == "proposed" and item["status_effect"] == "none"
               for item in listed["suggestions"])
    assert {item["encoder"] for item in listed["suggestions"]} == {"local-hash-v1"}
    assert all(item["left"]["effective_status"] == "proposed" for item in listed["suggestions"])

    memories = world.call("GET", f"memories?project_id={world.project}", token=token)
    assert {memory["status"] for memory in memories} == {"proposed"}
    assert all(memory["revision"] == 1 for memory in memories)
    assert world.store.db.execute("SELECT count(*) AS n FROM resources WHERE kind='memory_dispute'"
                                  ).fetchone()["n"] == 0

    peer_token = world.credential(world.agent("Peer"))
    assert world.suggestions(token=peer_token)["suggestions"] == []
    world.call("GET", f"memory-pipeline:suggestions?project_id={world.project}", token=peer_token)

    decision = {"expected_revision": 1, "reason": "Reviewed both rows"}
    dismissed = world.call("POST", f"memory-pipeline/suggestions/{duplicate['id']}:dismiss", decision,
                           token, key="dismiss-once")
    assert dismissed["state"] == "dismissed" and dismissed["status_effect"] == "none"
    assert dismissed["decided_by"] == world.principal_id(token)
    assert world.call("POST", f"memory-pipeline/suggestions/{duplicate['id']}:dismiss", decision,
                      token, key="dismiss-once") == dismissed
    world.call("POST", f"memory-pipeline/suggestions/{duplicate['id']}:dismiss", decision, token, status=412)
    assert duplicate["id"] not in {item["id"] for item in world.suggestions(token=token)["suggestions"]}
    hidden = world.suggestions(token=token, include_dismissed="true")
    assert duplicate["id"] in {item["id"] for item in hidden["suggestions"]}
    assert {memory["status"] for memory in world.call(
        "GET", f"memories?project_id={world.project}", token=token)} == {"proposed"}


def test_commit_drift_marks_currentness_and_filtering_is_explicit(builder):
    snapshot = {"repository_id": "repo_test", "branch": "main", "commit": "a" * 40}
    world = builder(config=PipelineConfig(enabled=True), snapshot_provider=lambda: dict(snapshot))
    agent, token, run, source, body = setup(world)
    memory = world.memory(source, title="Pinned fact", token=token,
                          validity={"repository_id": "repo_test", "branch": "main",
                                    "observed_commit": "a" * 40})
    world.call("POST", f"memories/{memory['id']}:verify", {"expected_revision": 1})

    current = world.freshness({"project_id": world.project, "memory_ids": [memory["id"]]}, token)
    mark = current["marks"][0]
    assert mark["state"] == "current" and mark["reasons"] == [] and mark["status_effect"] == "none"
    assert mark["snapshot"]["source"] == "provider" and mark["pinned"]["observed_commit"] == "a" * 40
    repeat = world.freshness({"project_id": world.project, "memory_ids": [memory["id"]]}, token)
    assert [item["id"] for item in repeat["marks"]] == [mark["id"]]

    snapshot.update(commit="b" * 40, branch="feature/export-refactor")
    drifted = world.freshness({"project_id": world.project, "memory_ids": [memory["id"]]}, token)
    assert drifted["marks"][0]["state"] == "drifted"
    assert set(drifted["marks"][0]["reasons"]) == {"branch_drift", "commit_drift"}
    stored = world.call("GET", f"memories/{memory['id']}", token=token)
    assert stored["status"] == "verified" and stored.get("invalidation_reason") is None
    assert not stored.get("active_dispute_ids") and not stored.get("supersedes_version_id")

    marks_before = world.marks()
    revisions_before = {item["id"]: item["revision"] for item in world.call(
        "GET", f"memories?project_id={world.project}", token=token)}
    filtered = world.retrieve({"project_id": world.project, "memory_ids": [memory["id"]],
                               "require_current": True}, token)
    assert filtered["mutated"] is False
    assert [item["memory_id"] for item in filtered["excluded"]] == [memory["id"]]
    assert filtered["excluded"][0]["excluded_because"] == "repository_drift"
    relaxed = world.retrieve({"project_id": world.project, "memory_ids": [memory["id"]],
                              "require_current": False}, token)
    assert [item["memory_id"] for item in relaxed["included"]] == [memory["id"]]
    untracked = world.retrieve({"project_id": world.project, "memory_ids": [memory["id"]],
                                "require_current": False, "include_untracked": False}, token)
    assert [item["memory_id"] for item in untracked["included"]] == [memory["id"]]
    assert world.marks() == marks_before
    assert {item["id"]: item["revision"] for item in world.call(
        "GET", f"memories?project_id={world.project}", token=token)} == revisions_before


def test_injected_snapshots_require_explicit_opt_in(builder):
    strict = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(strict)
    strict.freshness({"project_id": strict.project}, token, status=503)
    strict.freshness({"project_id": strict.project, "snapshot": {"commit": "a" * 40}}, token, status=403)
    strict.retrieve({"project_id": strict.project}, token, status=503)

    permissive = builder(config=PipelineConfig(enabled=True, allow_injected_snapshot=True))
    agent, token, run, source, body = setup(permissive)
    memory = permissive.memory(source, token=token, validity={"repository_id": "repo_test",
                                                              "branch": "main",
                                                              "observed_commit": "a" * 40})
    result = permissive.freshness({"project_id": permissive.project,
                                   "memory_ids": [memory["id"]],
                                   "snapshot": {"repository_id": "repo_test", "branch": "main",
                                                "commit": "a" * 40}}, token)
    assert result["snapshot"]["source"] == "injected"
    assert result["marks"][0]["state"] == "current"


def test_suggestions_annotate_pinned_commit_drift_without_changing_facts(builder):
    snapshot = {"repository_id": "repo_test", "branch": "main", "commit": "a" * 40}
    world = builder(config=PipelineConfig(enabled=True), snapshot_provider=lambda: dict(snapshot))
    agent, token, run, first, body = setup(world)
    second = world.post("The export uses UTC timestamps", token)
    pin = {"repository_id": "repo_test", "branch": "main", "observed_commit": "a" * 40}
    arm(world, proposal_json(first["id"], "The export uses UTC timestamps"),
        proposal_json(second["id"], "The export uses UTC timestamps"))
    for source in (first, second):
        job = world.enqueue({**body, "pin": pin, "source_refs": [
            {"kind": "board_post", "id": source["id"], "revision": source["revision"]}]}, token)["job"]
        world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)
    memory = world.call("GET", f"memories?project_id={world.project}", token=token)[0]
    assert memory["validity"]["observed_commit"] == "a" * 40

    listed = world.suggestions(token=token)
    assert len(listed["suggestions"]) == 1 and listed["snapshot"]["commit"] == "a" * 40
    assert listed["suggestions"][0]["left"]["currentness"] == "current"
    snapshot.update(commit="b" * 40, branch="release")
    drifted = world.suggestions(token=token)
    assert len(drifted["suggestions"]) == 1 and drifted["suggestions"][0]["stale"] is False
    assert drifted["suggestions"][0]["left"]["currentness"] == "drifted"
    assert "commit_drift" in drifted["suggestions"][0]["left"]["currentness_reasons"]
    assert world.call("GET", f"memories?project_id={world.project}", token=token) == []
    assert world.call("GET", f"memories/{memory['id']}", token=token)["status"] == "proposed"

    # A reviewed revision makes the observation stale until it is explicitly requested.
    world.call("POST", f"memories/{memory['id']}:review",
               {"expected_revision": 1, "decision": "reject", "reason": "Superseded by review"})
    hidden = world.suggestions(token=token)
    assert hidden["suggestions"] == [] and hidden["hidden_stale"] == 1
    shown = world.suggestions(token=token, include_stale="true")
    assert len(shown["suggestions"]) == 1
    entry = shown["suggestions"][0]
    assert len(entry["stale_reasons"]) == 1 and entry["stale_reasons"][0].endswith("_revision_changed")
    reviewed = entry["stale_reasons"][0].split("_", 1)[0]
    assert entry[reviewed]["effective_status"] == "rejected" and entry[reviewed]["revision"] == 2


class FixedEncoder:
    name = "fixed-test-encoder"

    def vectors(self, texts):
        return [[1.0, 0.0] for _ in texts]


class BrokenEncoder:
    name = "broken-test-encoder"

    def vectors(self, texts):
        return ["not a vector" for _ in texts]


def test_similarity_uses_the_injected_encoder_and_invalid_output_is_survivable(builder):
    world = builder(config=PipelineConfig(enabled=True), encoder=FixedEncoder())
    agent, token, run, first, body = setup(world)
    second = world.post("Navigation links power the header", token)
    arm(world, proposal_json(first["id"], "The header uses navigation links"),
        proposal_json(second["id"], "Navigation links power the header"))
    for source in (first, second):
        job = world.enqueue({**body, "source_refs": [
            {"kind": "board_post", "id": source["id"], "revision": source["revision"]}]}, token)["job"]
        world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)
    listed = world.suggestions(token=token)
    assert len(listed["suggestions"]) == 1
    assert listed["suggestions"][0]["kind"] == "duplicate" and listed["suggestions"][0]["similarity"] == 1.0
    assert listed["suggestions"][0]["encoder"] == "fixed-test-encoder"
    assert world.call("GET", f"memory-pipeline:status?project_id={world.project}",
                      token=token)["encoder"] == "fixed-test-encoder"

    broken = builder(config=PipelineConfig(enabled=True), encoder=BrokenEncoder())
    agent, token, run, first, body = setup(broken)
    second = broken.post("The export uses UTC timestamps", token)
    arm(broken, proposal_json(first["id"], "The export uses UTC timestamps"),
        proposal_json(second["id"], "The export uses UTC timestamps"))
    for source in (first, second):
        job = broken.enqueue({**body, "source_refs": [
            {"kind": "board_post", "id": source["id"], "revision": source["revision"]}]}, token)["job"]
        broken.process({"project_id": broken.project, "job_ids": [job["id"]]}, token)
    latest = broken.jobs(token=token)["jobs"][0]
    assert latest["state"] == "done" and latest["suggestion_error"] == "similarity_encoder_invalid_output"
    assert len(broken.call("GET", f"memories?project_id={broken.project}", token=token)) == 2


def test_pipeline_loads_no_heavy_model(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    arm(world, proposal_json(source["id"], "A durable fact"))
    before = set(sys.modules)
    world.process({"project_id": world.project, "job_ids": [world.enqueue(body, token)["job"]["id"]]}, token)
    imported = set(sys.modules) - before
    assert not {"torch", "transformers", "sentence_transformers", "hivemind.indexes"} & imported
    source_code = Path(pipeline_module.__file__).read_text(encoding="utf-8")
    assert "sentence_transformers" not in source_code and "import torch" not in source_code
    assert isinstance(LocalHashingEncoder().vectors(["a b"])[0], list)


def test_background_tick_runs_jobs_with_the_enqueuing_authority(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, first, body = setup(world)
    second = world.post("SQLite WAL keeps one writer", token)
    arm(world, proposal_json(first["id"], "The export uses UTC timestamps"),
        proposal_json(second["id"], "SQLite WAL keeps one writer"))
    jobs = [world.enqueue({**body, "source_refs": [
        {"kind": "board_post", "id": source["id"], "revision": source["revision"]}]}, token)["job"]
        for source in (first, second)]
    world.call("POST", "memory-pipeline:tick", {"limit": 1}, token, status=403)
    # An administrator may trigger processing, but the work keeps the enqueuer's authority.
    triggered = world.process({"project_id": world.project, "job_ids": [jobs[0]["id"]]})
    assert triggered["results"][0]["state"] == "done"
    assert world.call("POST", "memory-pipeline:tick", {"limit": 1})["ticked"] == 1
    rest = world.call("POST", "memory-pipeline:tick", {"limit": 4})
    assert rest["ticked"] == 0
    memories = world.call("GET", f"memories?project_id={world.project}", token=token)
    assert len(memories) == 2 and {memory["status"] for memory in memories} == {"proposed"}
    agent_principal = world.principal_id(token)
    assert {world.store.raw(memory["id"])["owner"] for memory in memories} == {agent_principal}
    worker = world.pipeline.start_worker(0.05)
    assert worker["started"] is True and world.pipeline.stop_worker()["stopped"] is True


def test_fingerprints_survive_restart_and_prevent_duplicate_proposals(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    arm(world, proposal_json(source["id"], "The export uses UTC timestamps"))
    job = world.enqueue(body, token)["job"]
    world.process({"project_id": world.project, "job_ids": [job["id"]]}, token)
    memory_id = world.call("GET", f"memories?project_id={world.project}", token=token)[0]["id"]
    directory = world.directory
    world.close()

    store = Store(directory)
    try:
        principal = dict(store.db.execute("SELECT * FROM principals WHERE agent=?", (agent["id"],)).fetchone())
        pipeline = MemoryPipeline(Service(store), config=PipelineConfig(enabled=True))
        pipeline.extractor = scripted(proposal_json(source["id"], "The export uses UTC timestamps"))
        reopened = pipeline.enqueue(principal, EnqueueRequest.model_validate(body))
        assert reopened["created"] is False and reopened["job"]["id"] == job["id"]
        again = pipeline.process(principal, ProcessRequest.model_validate(
            {"project_id": world.project, "job_ids": [job["id"]]}))
        assert again["results"][0]["skipped"] is True

        # Deleting the queue row must not resurrect a second copy of the same proposal.
        with store.transaction():
            store.delete(job["id"], principal, store.raw(job["id"])["revision"])
        recreated = pipeline.enqueue(principal, EnqueueRequest.model_validate(body))
        assert recreated["created"] is True and recreated["job"]["id"] != job["id"]
        result = pipeline.process(principal, ProcessRequest.model_validate(
            {"project_id": world.project, "job_ids": [recreated["job"]["id"]]}))
        assert result["results"][0]["memories"] == [memory_id]
        assert len(store.list("memory", world.project, principal)) == 1
    finally:
        store.close()


def test_install_provides_an_error_envelope_on_a_bare_app(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    store = Store(tmp_path)
    service = Service(store)
    app = FastAPI()
    principal, command, envelope = make_dependencies(store, service)
    try:
        install(app, service, principal, command, envelope, config=PipelineConfig(enabled=True))
        with TestClient(app) as client:
            headers = {"Authorization": "Bearer " + config["token"]}
            status = client.get(f"/v1/memory-pipeline:status?project_id={config['project_id']}",
                                headers=headers)
            assert status.status_code == 200 and status.json()["data"]["enabled"] is True
            missing = client.get("/v1/memory-pipeline:status?project_id=project_missing", headers=headers)
            assert missing.status_code == 404 and missing.json()["error"]["code"] == "not_found"
    finally:
        store.close()


def test_capability_registry_installs_the_pipeline_disabled(tmp_path, monkeypatch):
    capabilities = pytest.importorskip("hivemind.capabilities")
    if not hasattr(capabilities, "INSTALLED_FEATURE_MODULES") or not hasattr(capabilities, "install"):
        pytest.skip("capability registry shape changed")
    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False)
    principal, command, envelope = make_dependencies(app.state.store, app.state.service)
    monkeypatch.setattr(capabilities, "INSTALLED_FEATURE_MODULES", ("memory_pipeline",))
    capabilities.install(app, app.state.service, principal, command, envelope)
    assert app.state.memory_pipeline.config.enabled is False
    with TestClient(app) as client:
        headers = {"Authorization": "Bearer " + config["token"], "Idempotency-Key": uid("key")}
        response = client.post("/v1/memory-pipeline:enqueue", headers=headers, json={
            "project_id": config["project_id"], "agent_id": "agent_missing",
            "source_refs": [{"kind": "board_post", "id": "post_missing"}]})
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "memory_pipeline_disabled"


def test_install_coexists_with_the_standard_app(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    memory = world.memory(source, token=token)
    fetched = world.call("GET", f"memories/{memory['id']}", token=token)
    assert fetched["id"] == memory["id"] and fetched["status"] == "proposed"
    assert world.call("GET", f"memories?project_id={world.project}", token=token)[0]["id"] == memory["id"]
    assert world.app.state.memory_pipeline is world.pipeline
    paths = {route.path for route in world.app.routes}
    assert "/v1/memory-pipeline:enqueue" in paths
    # Pipeline routes live outside the /v1/memories/{memory_id} namespace so no standard
    # route is shadowed by registration order.
    assert not any(path.startswith("/v1/memories/") and "pipeline" in path for path in paths)


def test_git_snapshot_is_read_only_and_refuses_non_repositories(builder, tmp_path):
    plain = tmp_path / "plain"
    plain.mkdir()
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    inside_repository = subprocess.run(["git", "-C", str(plain), "rev-parse", "--show-toplevel"],
                                       capture_output=True, text=True).returncode == 0
    if inside_repository:
        with pytest.raises(Problem):
            git_snapshot(tmp_path / "definitely-missing")
    else:
        with pytest.raises(Problem) as failure:
            git_snapshot(plain)
        assert failure.value.code == "workspace_not_a_repository"
    with pytest.raises(Problem) as missing:
        git_snapshot(tmp_path / "definitely-missing")
    assert missing.value.code == "workspace_unavailable"

    repository = tmp_path / "workspace"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True, capture_output=True)
    before = sorted((str(path.relative_to(repository)), path.stat().st_mtime_ns)
                    for path in repository.rglob("*") if path.is_file())
    first = git_snapshot(repository)
    second = git_snapshot(repository)
    after = sorted((str(path.relative_to(repository)), path.stat().st_mtime_ns)
                   for path in repository.rglob("*") if path.is_file())
    assert first == second and first["read_only"] is True and first["source"] == "git"
    assert first["repository_id"].startswith("repo_") and len(first["repository_id"]) == 21
    assert first["commit"] is None and first["branch"] is None
    assert before == after
    assert repository_identity(repository) == repository_identity(repository / ".")
    assert str(repository) not in json.dumps(first)

    world = builder(config=PipelineConfig(enabled=True, workspace=str(repository)))
    agent, token, run, source, body = setup(world)
    memory = world.memory(source, token=token, validity={"repository_id": first["repository_id"],
                                                         "branch": "main", "observed_commit": "a" * 40})
    marks = world.freshness({"project_id": world.project, "memory_ids": [memory["id"]]}, token)
    assert marks["snapshot"]["source"] == "git" and marks["snapshot"]["repository_id"] == first["repository_id"]
    assert marks["marks"][0]["state"] == "drifted"
    assert set(marks["marks"][0]["reasons"]) == {"snapshot_branch_unknown", "snapshot_commit_unknown"}


def test_enqueue_is_idempotent_across_keys_and_threads(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    first = world.enqueue(body, token)
    assert world.enqueue(body, token, key="explicit-key")["job"]["id"] == first["job"]["id"]
    assert world.enqueue(body, token)["created"] is False

    def take(_):
        return world.enqueue(body, token)["job"]["id"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = set(pool.map(take, range(4)))
    assert ids == {first["job"]["id"]}
    assert world.store.db.execute("SELECT count(*) AS n FROM resources WHERE kind=?",
                                  (JOB_KIND,)).fetchone()["n"] == 1

    wider = world.enqueue({**body, "max_candidates": 2}, token)
    assert wider["created"] is True and wider["job"]["id"] != first["job"]["id"]
    scoped = world.enqueue({**body, "scope_kind": "project", "scope_owner_id": world.project}, token)
    assert scoped["created"] is True and scoped["job"]["private"] is False


def test_scope_actor_and_run_bindings_match_extraction_rules(builder):
    world = builder(config=PipelineConfig(enabled=True))
    agent, token, run, source, body = setup(world)
    other = world.agent("Other actor")
    world.enqueue({**body, "agent_id": other["id"]}, token, status=403)
    world.enqueue({**body, "scope_owner_id": other["id"]}, token, status=422)
    world.enqueue({**body, "scope_kind": "run", "scope_owner_id": None}, token, status=422)
    world.enqueue({**body, "kind": "verified"}, token, status=422)
    world.enqueue({**body, "source_refs": [{"kind": "message", "id": source["id"]}]}, token, status=422)
    world.enqueue({**body, "source_refs": []}, token, status=422)
    world.enqueue({**body, "source_refs": [{"kind": "artifact", "id": source["id"]}]}, token, status=404)
    assert world.store.db.execute("SELECT count(*) AS n FROM resources WHERE kind=?",
                                  (JOB_KIND,)).fetchone()["n"] == 0

    # An ordinary board chat post is skipped, exactly like deterministic extraction.
    chat = world.call("POST", f"boards/{world.board()['id']}/posts", {
        "client_post_id": uid("chat"), "content": {"media_type": "text/plain", "text": "Ordinary chat"}}, token)
    result = world.enqueue({**body, "source_refs": [{"kind": "board_post", "id": chat["id"]}]}, token)
    assert result["created"] is False and result["job"] is None
    assert result["skipped"] == [{"source_id": chat["id"], "reason": "not_a_finding"}]

    # A finished requester run cannot enqueue new work.
    with world.store.transaction():
        world.store.save("run", world.project, world.store.raw(run["id"])["owner"],
                         {**run, "status": "completed"})
    world.enqueue(body, token, status=409)
