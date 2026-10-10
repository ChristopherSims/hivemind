"""Offline tests for the opt-in independent reviewer runtime.

Native Factory/OhMyPi adapters are exercised with their local fake SDK/transport
contracts (no paid or live model calls). The structured-verdict return is provided by a
test-local subclass standing in for the parent-owned VendorRuntime structured-output
opt-in; the plain native adapters prove the fail-closed legacy path."""
import json
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from hivemind.api import hash_token
from hivemind.git_reviewer_runtime import (GitReviewerRuntime, MergeReviewOutcome,
                                           ReviewerRuntimePolicy, ReviewerVerdict)
from hivemind.recovery import Journal
from hivemind.storage import Problem, uid
from hivemind.vendor_runtimes import FactoryRuntime, OhMyPiRuntime
from test_git_collaboration import contribution
from test_vendor_runtimes import SDK, OmpTransport, Session


def verdict_for(request, decision="approve", reason="Exact candidate reviewed in isolation"):
    return {"decision": decision, "reason": reason, "findings": [],
            **{key: request[key] for key in ("source_oid", "target_oid", "candidate_oid")}}


class VerdictFactoryRuntime(FactoryRuntime):
    """Native adapter plus the parent-owned structured-output opt-in contract."""
    verdict = None
    probe = None

    def execute(self, task, context, checkpoint):
        if self.probe:
            self.probe(self, task, context)
        return super().execute(task, context, checkpoint)

    def _settled(self, status, result=""):
        outcome = super()._settled(status, result)
        if status == "completed" and self.verdict is not None:
            outcome["structured"] = self.verdict
        return outcome


class VerdictOmpRuntime(OhMyPiRuntime):
    verdict = None

    def _settled(self, status, result=""):
        outcome = super()._settled(status, result)
        if status == "completed" and self.verdict is not None:
            outcome["structured"] = self.verdict
        return outcome


def principal_id(world, token):
    return world.app.state.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()[0]


def factory_for(world, tmp_path, reviewer, token, kind="droid", verdict=None, probe=None):
    private = tmp_path / "private"
    private.mkdir(exist_ok=True)

    def factory(workspace, context):
        config = private / (uid("cfg") + ".json")
        config.write_text(json.dumps({"project_id": world.project, "agent_id": reviewer["id"],
                                      "base_url": "http://testserver", "token": token}))
        options = {"workspace": str(workspace), "config": str(config),
                   "state_file": str(private / (uid("state") + ".json")), "timeout_seconds": 5,
                   "enabled": True, "allow_model_calls": True}
        if kind == "droid":
            runtime = VerdictFactoryRuntime(options, sdk=SDK)
        else:
            runtime = VerdictOmpRuntime(options, transport_factory=OmpTransport)
        runtime.verdict = verdict
        runtime.probe = probe
        return runtime

    return factory


def make_executor(world, factory, **policy):
    options = {"enabled": True, "allow_model_calls": True, **policy}
    return GitReviewerRuntime(world.app.state.git_collaboration, runtime_factory=factory,
                              policy=ReviewerRuntimePolicy(**options))


def review_ids(world, token):
    return {"reviewer_principal_id": principal_id(world, token)}


def test_disabled_and_identity_gates(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    ids = {"reviewer_principal_id": principal_id(world, token),
           "reviewer_agent_id": reviewer["id"]}
    disabled = GitReviewerRuntime(world.app.state.git_collaboration)
    with pytest.raises(Problem) as failure:
        disabled.review_merge(request["id"], **ids)
    assert failure.value.code == "git_reviewer_runtime_disabled"
    factory = factory_for(world, tmp_path, reviewer, token, verdict=verdict_for(request))
    no_model = make_executor(world, factory, allow_model_calls=False)
    with pytest.raises(PermissionError):
        no_model.review_merge(request["id"], **ids)
    executor = make_executor(world, factory)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], **{**ids, "reviewer_agent_id": "agent-other"})
    assert failure.value.code == "git_reviewer_identity_mismatch"
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id="principal-missing",
                              reviewer_agent_id=reviewer["id"])
    assert failure.value.code == "git_reviewer_unavailable"


def test_producer_can_never_review_its_own_candidate(world, tmp_path):
    producer, token, _, _, _, request = contribution(world)
    factory = factory_for(world, tmp_path, producer, token, verdict=verdict_for(request))
    executor = make_executor(world, factory)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=producer["id"])
    assert failure.value.code == "independent_reviewer_required"


def test_factory_native_contract_canonical_ids_tools_workspace_no_held_lock(world, tmp_path):
    producer, _, _, _, _, request = contribution(world)
    reviewer, token = world.agent("Reviewer"), None
    token = world.credential(reviewer)
    seen = {}
    store = world.app.state.store

    def probe(runtime, task, context):
        seen["in_transaction"] = store.db.in_transaction
        seen["context"] = context
        seen["task"] = task

    verdict = verdict_for(request)
    factory = factory_for(world, tmp_path, reviewer, token, verdict=verdict, probe=probe)
    executor = make_executor(world, factory)
    outcome = executor.review_merge(request["id"],
                                    reviewer_principal_id=principal_id(world, token),
                                    reviewer_agent_id=reviewer["id"])
    assert isinstance(outcome, MergeReviewOutcome) and outcome.decision == "approve"
    assert outcome.already_reviewed is False and outcome.snapshot_verified_unchanged is True
    assert outcome.receipts["execution"]["deduplicated"] is False
    # Canonical review landed through the ordinary exact-hash path.
    current = world.call("GET", f"merge-requests/{request['id']}")
    assert current["reviews"][0]["agent_id"] == reviewer["id"] != producer["id"]
    assert current["reviews"][0]["candidate_oid"] == request["candidate_oid"]
    assert current["reviews"][0]["reason"].startswith("[independent-runtime-review] ")
    # Real canonical run/task/execution identities, never synthetic.
    run = world.call("GET", f"runs/{outcome.review_run_id}", token=token)
    assert run["agent_id"] == reviewer["id"] and run["status"] in ("running", "idle")
    task = world.call("GET", f"tasks/{outcome.review_task_id}", token=token)
    assert task["status"] == "completed" and task["scope_ids"] == []
    assert task["execution"]["id"] == outcome.review_execution_id
    assert seen["context"]["run_id"] == outcome.review_run_id
    assert seen["context"]["execution_id"] == outcome.review_execution_id
    assert seen["task"]["id"] == outcome.review_task_id
    # Model execution never held the canonical store transaction/lock.
    assert seen["in_transaction"] is False
    # The managed hook config binds the real canonical IDs and reviewer identity.
    configs = list((tmp_path / "private").glob("cfg*.json"))
    runtime_states = list((tmp_path / "private").glob("*.agent.json"))
    assert runtime_states
    managed = json.loads(runtime_states[-1].read_text())
    assert (managed["project_id"], managed["agent_id"]) == (world.project, reviewer["id"])
    assert managed["managed_run_id"] == outcome.review_run_id
    assert managed["managed_task_id"] == outcome.review_task_id
    assert managed["managed_execution_id"] == outcome.review_execution_id
    assert managed["workspace_root"] == outcome.snapshot
    assert managed["allowed_tool_names"] == ["Read", "Grep", "Glob", "LS"]
    assert configs  # reviewer credential config was supplied by the factory
    # The runtime was constructed bound to the exact snapshot of the candidate.
    assert Path(outcome.snapshot).name.startswith("snapshot")
    assert executor.domain._ref(Path(outcome.snapshot), "HEAD") == request["candidate_oid"]
    # Durable receipts survive reopening the journal.
    with Journal(executor.journal_path) as journal:
        kinds = {operation["path"] for operation in journal.operations(state="completed")}
        assert {"git-review-execute", "git-review-submit"} <= kinds
        assert not journal.has_unknown_effects()


def test_omp_native_contract_and_readonly_tools(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    factory = factory_for(world, tmp_path, reviewer, token, kind="omp",
                          verdict=verdict_for(request, decision="reject", reason="Missing tests"))
    executor = make_executor(world, factory)
    outcome = executor.review_merge(request["id"],
                                    reviewer_principal_id=principal_id(world, token),
                                    reviewer_agent_id=reviewer["id"])
    assert outcome.decision == "reject"
    states = list((tmp_path / "private").glob("*.agent.json"))
    managed = json.loads(states[-1].read_text())
    assert managed["allowed_tool_names"] == ["read", "grep", "glob"]
    assert managed["managed_execution_id"] == outcome.review_execution_id
    reviews = world.call("GET", f"merge-requests/{request['id']}")["reviews"]
    assert reviews[0]["decision"] == "reject"


def test_hash_only_native_output_fails_closed_and_settles_task(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    # Plain native adapter: the structured-output opt-in is absent, so no verdict exists.
    private = tmp_path / "private"
    private.mkdir()

    def factory(workspace, context):
        config = private / (uid("cfg") + ".json")
        config.write_text(json.dumps({"project_id": world.project, "agent_id": reviewer["id"],
                                      "base_url": "http://testserver", "token": token}))
        return FactoryRuntime({"workspace": str(workspace), "config": str(config),
                               "state_file": str(private / (uid("state") + ".json")),
                               "timeout_seconds": 5, "enabled": True,
                               "allow_model_calls": True}, sdk=SDK)

    executor = make_executor(world, factory)
    before = store_tasks(world)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=reviewer["id"])
    assert failure.value.code == "review_structured_output_unavailable"
    assert world.call("GET", f"merge-requests/{request['id']}")["reviews"] == []
    tasks = store_tasks(world)
    assert len(tasks) == len(before) + 1 and tasks[-1]["status"] == "failed"
    assert "review_structured_output_unavailable" in tasks[-1]["execution"]["checkpoint"]["summary"]


def store_tasks(world):
    rows = world.app.state.store.db.execute(
        "SELECT body FROM resources WHERE kind='task' AND deleted=0").fetchall()
    return [json.loads(row["body"]) for row in rows]


def test_timeout_fences_unknown_and_never_submits(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)

    class SlowRuntime:
        vendor = "droid"
        tool_names = ["Read"]

        def __init__(self, workspace):
            self.workspace = workspace

        def execute(self, task, context, checkpoint):
            time.sleep(5)
            return {"status": "completed", "structured": verdict_for(request)}

    executor = make_executor(world, lambda workspace, context: SlowRuntime(workspace),
                             timeout_seconds=1)
    started = time.monotonic()
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=reviewer["id"])
    assert time.monotonic() - started < 4
    assert failure.value.code == "git_review_effect_unknown"
    # No verdict from the abandoned thread was submitted; the task stays unreconciled.
    assert world.call("GET", f"merge-requests/{request['id']}")["reviews"] == []
    assert store_tasks(world)[-1]["status"] == "running"
    with Journal(executor.journal_path) as journal:
        assert journal.has_unknown_effects()


def test_misbound_verdict_fails_closed_and_settles_task(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    wrong = {**verdict_for(request), "candidate_oid": "0" * 40}
    factory = factory_for(world, tmp_path, reviewer, token, verdict=wrong)
    executor = make_executor(world, factory)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=reviewer["id"])
    assert failure.value.code == "review_verdict_binding_mismatch"
    assert world.call("GET", f"merge-requests/{request['id']}")["reviews"] == []
    assert store_tasks(world)[-1]["status"] == "failed"


def test_snapshot_tampering_cancels_before_submission(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    domain = world.app.state.git_collaboration

    def probe(runtime, task, context):
        snapshot = sorted((domain.root / "reviewer-snapshots").iterdir())[-1]
        (snapshot / "export" / "result.txt").write_text("tampered", encoding="utf-8")

    factory = factory_for(world, tmp_path, reviewer, token,
                          verdict=verdict_for(request), probe=probe)
    executor = make_executor(world, factory)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=reviewer["id"])
    assert failure.value.code == "review_snapshot_modified"
    assert world.call("GET", f"merge-requests/{request['id']}")["reviews"] == []


def test_workspace_mismatch_is_refused_before_any_model_call(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)

    class ForeignRuntime:
        vendor = "droid"
        tool_names = ["Read"]
        called = False

        def __init__(self):
            self.workspace = tmp_path  # Not the snapshot.

        def execute(self, task, context, checkpoint):
            self.called = True

    holder = {}

    def factory(workspace, context):
        holder["runtime"] = ForeignRuntime()
        return holder["runtime"]

    executor = make_executor(world, factory)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=reviewer["id"])
    assert failure.value.code == "review_runtime_workspace_mismatch"
    assert holder["runtime"].called is False
    assert store_tasks(world)[-1]["status"] == "failed"


def test_duplicate_review_is_recognized_without_rerunning(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    factory = factory_for(world, tmp_path, reviewer, token, verdict=verdict_for(request))
    executor = make_executor(world, factory)
    ids = {"reviewer_principal_id": principal_id(world, token),
           "reviewer_agent_id": reviewer["id"]}
    first = executor.review_merge(request["id"], **ids)
    sessions = len(Session.created)
    def count_runs():
        return world.app.state.store.db.execute(
            "SELECT count(*) FROM resources WHERE kind='run' AND deleted=0").fetchone()[0]
    runs = count_runs()
    second = executor.review_merge(request["id"], **ids)
    assert second.already_reviewed is True and second.decision == first.decision
    assert second.review_run_id is None
    assert len(Session.created) == sessions
    assert count_runs() == runs
    reviews = world.call("GET", f"merge-requests/{request['id']}")["reviews"]
    assert len(reviews) == 1


def test_oversized_diff_never_creates_work_or_reaches_model(world, tmp_path):
    *_, request = contribution(world, "x" * 8192)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    factory = factory_for(world, tmp_path, reviewer, token, verdict=verdict_for(request))
    executor = make_executor(world, factory, max_diff_bytes=1024)
    sessions = len(Session.created)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, token),
                              reviewer_agent_id=reviewer["id"])
    assert failure.value.code == "review_diff_exceeds_bound" and failure.value.status == 413
    assert len(Session.created) == sessions and len(store_tasks(world)) == 1


def test_notification_bridge_and_stale_revision_fence(world, tmp_path):
    *_, request = contribution(world)
    reviewer = world.agent("Reviewer")
    token = world.credential(reviewer)
    factory = factory_for(world, tmp_path, reviewer, token, verdict=verdict_for(request))
    pid = principal_id(world, token)
    executor = make_executor(world, factory, reviewer_principal_id=pid)
    assert executor.review_notification({"type": "unrelated"})["handled"] is False
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=pid,
                              reviewer_agent_id=reviewer["id"],
                              expected_revision=request["revision"] + 1)
    assert failure.value.code == "revision_conflict"
    handled = executor.review_notification({"merge_request_id": request["id"]})
    assert handled["handled"] is True
    assert handled["outcome"]["decision"] == "approve"
    # A policy allowlist rejects a different reviewer principal.
    other = world.agent("Other")
    other_token = world.credential(other)
    with pytest.raises(Problem) as failure:
        executor.review_merge(request["id"], reviewer_principal_id=principal_id(world, other_token),
                              reviewer_agent_id=other["id"])
    assert failure.value.code == "git_reviewer_not_configured"


def test_verdict_schema_is_strict():
    base = {"reason": "ok", "source_oid": "0" * 40, "target_oid": "0" * 40,
            "candidate_oid": "0" * 40}
    with pytest.raises(ValidationError):
        ReviewerVerdict(decision="approve", **base, unexpected=True)
    with pytest.raises(ValidationError):
        ReviewerVerdict(decision="lgtm", **base)
