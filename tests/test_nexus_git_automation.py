import json
import sys
import time

import pytest

from hivemind.api import hash_token
from hivemind.nexus_git_automation import (MergeReviewOutcome, NexusGitAutomation,
                                           NexusGitPolicy)
from hivemind.storage import Problem, now, uid
from test_git_collaboration import contribution, nexus


class Runtime:
    """Injectable review runtime stub honoring the MergeReviewRuntime protocol."""

    reviewer_principal_id = None  # Optional protocol binding; None means unbound.

    def __init__(self, decision="approve"):
        self.decision, self.seen, self.observations = decision, [], []
        self.store = None        # Attach to assert on transaction state.
        self.probe = None        # Attach a canonical service callback.
        self.callback = None     # Attach to simulate effects during a long review.

    def review_merge(self, request):
        self.seen.append(dict(request))
        if self.store is not None:
            self.observations.append(bool(self.store.db.in_transaction))
        if self.probe is not None:
            self.observations.append(self.probe())
        if self.callback is not None:
            self.callback()
        return MergeReviewOutcome(self.decision, "Checked the exact candidate")


def principal_id(world, token=None, role=None):
    store = world.app.state.store
    if role:
        return store.db.execute("SELECT id FROM principals WHERE role=?", (role,)).fetchone()[0]
    return store.db.execute("SELECT id FROM principals WHERE token_hash=?",
                            (hash_token(token),)).fetchone()[0]


def automation(world, decision="approve", reviewers=True, checks=("smoke",), **overrides):
    domain = world.app.state.git_collaboration
    if checks:
        script = domain.root / "smoke.py"
        script.write_text("from pathlib import Path\nassert Path('export/result.txt').is_file()\n",
                          encoding="utf-8")
        domain.checks = {name: [sys.executable, str(script)] for name in checks}
    reviewer_ids, reviewer_token = (), None
    if reviewers:
        reviewer = world.agent("Reviewer")
        reviewer_token = world.credential(reviewer)
        reviewer_ids = (principal_id(world, reviewer_token),)
    nexus_agent = world.agent("Nexus")
    nexus_token = world.credential(nexus_agent, coordinator=True)
    nexus_run = world.run(nexus_agent, nexus_token)
    policy = NexusGitPolicy(enabled=True, project_id=world.project,
                            nexus_principal_id=principal_id(world, nexus_token),
                            nexus_run_id=nexus_run["id"],
                            operator_principal_id=principal_id(world, role="admin"),
                            reviewer_principal_ids=reviewer_ids, checks=checks, **overrides)
    runtime = Runtime(decision)
    loop = NexusGitAutomation(world.app.state.service, domain, policy, runtime)
    loop.reviewer_token, loop.nexus_token = reviewer_token, nexus_token
    return loop, runtime


def main_oid(world):
    domain = world.app.state.git_collaboration
    return domain._ref(domain._repo(world.project), "refs/heads/main")


def test_automation_defaults_disabled_and_bounds_validated(world):
    git = world.app.state.git_collaboration
    disabled = NexusGitAutomation(world.app.state.service, git)
    assert disabled.scheduled_tick() is None
    with pytest.raises(Problem) as failure:
        disabled.tick()
    assert failure.value.code == "nexus_git_automation_disabled"
    with pytest.raises(ValueError):
        NexusGitAutomation(world.app.state.service, git, NexusGitPolicy(interval_seconds=0))
    with pytest.raises(ValueError):
        NexusGitAutomation(world.app.state.service, git, NexusGitPolicy(requested_ttl_seconds=5))
    with pytest.raises(ValueError):
        NexusGitAutomation(world.app.state.service, git,
                           NexusGitPolicy(renew_margin_seconds=60, requested_ttl_seconds=60))
    with pytest.raises(ValueError):
        NexusGitAutomation(world.app.state.service, git, NexusGitPolicy(enabled=True))
    with pytest.raises(ValueError):
        NexusGitAutomation(world.app.state.service, git, NexusGitPolicy(enabled=1))


def test_scheduled_tick_checks_reviews_and_merges_exact_candidate(world):
    *_, request = contribution(world)
    target = request["target_oid"]
    loop, runtime = automation(world)
    result = loop.scheduled_tick()
    assert result["state"] == "active"
    assert result["results"] == [{"id": request["id"], "state": "merged",
                                  "merged_oid": request["candidate_oid"]}]
    assert main_oid(world) == request["candidate_oid"] != target
    store = world.app.state.store
    admin = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    merged = store.get(request["id"], admin, "merge_request")
    assert merged["status"] == "merged" and merged["lease_id"] == result["lease_id"]
    assert [item["name"] for item in merged["checks"]] == ["smoke"]
    review, = merged["reviews"]
    assert review["decision"] == "approve" and review["agent_id"] != merged["agent_id"]
    assert all(review[key] == request[key] for key in ("source_oid", "target_oid", "candidate_oid"))
    snapshot, = runtime.seen
    assert snapshot["id"] == request["id"] and snapshot["candidate_oid"] == request["candidate_oid"]
    assert snapshot["reviewer_principal_id"] == review["principal_id"]
    assert snapshot["reviewer_agent_id"] == review["agent_id"]
    assert loop.scheduled_tick() is None  # Interval throttling; no double-merge retry.
    second = loop.tick()
    assert second["state"] == "active" and second["results"] == []  # Idempotent: nothing pending.


def test_runtime_runs_outside_transaction_with_service_callback(world):
    *_, request = contribution(world)
    loop, runtime = automation(world)
    store = world.app.state.store
    nexus_principal = loop._principal(loop.policy.nexus_principal_id)
    runtime.store = store
    runtime.probe = lambda: loop.service.run(nexus_principal, loop.policy.nexus_run_id)["id"]
    runtime.reviewer_principal_id = loop.policy.reviewer_principal_ids[0]  # Bound correctly.
    result = loop.tick()
    assert result["results"][0]["state"] == "merged"
    # The reviewer callback ran with no active SQLite transaction and could
    # safely call back into canonical service reads.
    assert runtime.observations[0] is False
    assert runtime.observations[1] == loop.policy.nexus_run_id


def test_runtime_identity_mismatch_never_records_another_reviewer(world):
    *_, request = contribution(world)
    target = main_oid(world)
    loop, runtime = automation(world)
    runtime.reviewer_principal_id = "principal_someone_else"  # Misbound adapter.
    assert loop.tick()["results"][0]["state"] == "review_abstained"
    loop.runtime = Runtime()
    def wrong_identity(request):
        return MergeReviewOutcome("approve", "Forged identity",
                                  reviewer_principal_id="principal_someone_else")
    loop.runtime.review_merge = wrong_identity
    assert loop.tick()["results"][0]["state"] == "review_abstained"
    current = world.call("GET", f"merge-requests/{request['id']}")
    assert current["status"] == "open" and current["reviews"] == []
    assert main_oid(world) == target


def test_long_review_renews_own_lease_or_yields_to_replacement(world):
    contribution(world)
    store = world.app.state.store
    loop, runtime = automation(world)

    def shorten_own_lease():
        principal = loop._principal(loop.policy.nexus_principal_id)
        lease = loop.service.lease(principal, world.project)
        row = store.raw(lease["id"])
        body = json.loads(row["body"])
        body["expires_at"] = time.time() + 2  # Simulates a long review near expiry.
        store.save("nexus_lease", row["project"], row["owner"], body)

    runtime.callback = shorten_own_lease
    result = loop.tick()  # Renewed in place after the review; merge proceeds.
    assert result["results"][0]["state"] == "merged"
    renewed = json.loads(store.raw(result["lease_id"])["body"])
    assert renewed["expires_at"] > time.time() + 30


def test_runtime_recorded_review_is_recognized_not_duplicated(world):
    *_, request = contribution(world)
    loop, runtime = automation(world)
    runtime.decision = "abstain"  # The bridge itself records canonically instead.

    def bridge_submits_review():
        current = world.call("GET", f"merge-requests/{request['id']}")
        world.call("POST", f"merge-requests/{request['id']}:review", {
            "expected_revision": current["revision"],
            **{key: current[key] for key in ("source_oid", "target_oid", "candidate_oid")},
            "decision": "approve", "reason": "Reviewer bridge recorded directly"},
            loop.reviewer_token)

    runtime.callback = bridge_submits_review
    result = loop.tick()
    assert result["results"][0]["state"] == "merged"
    merged = world.call("GET", f"merge-requests/{request['id']}")
    review, = merged["reviews"]  # Exactly one record; never duplicated against an old revision.
    assert review["principal_id"] == loop.policy.reviewer_principal_ids[0]
    assert review["decision"] == "approve"


def test_obsolete_check_policy_receipt_is_refreshed(world):
    *_, request = contribution(world)
    loop, runtime = automation(world, decision="abstain")
    domain = world.app.state.git_collaboration
    assert loop.tick()["results"][0]["state"] == "review_abstained"  # Check recorded, no review.
    stale = domain._check_policy("smoke")
    domain.checks = {"smoke": [sys.executable, str(domain.root / "smoke.py"), "v2"]}
    runtime.decision = "approve"
    result = loop.tick()  # Refreshes the obsolete receipt instead of failing forever.
    assert result["results"][0]["state"] == "merged"
    merged = world.call("GET", f"merge-requests/{request['id']}")
    check, = merged["checks"]
    current = domain._check_policy("smoke")
    assert check["policy_sha256"] == current != stale


def test_long_review_maintenance_renews_shortened_lease(world):
    contribution(world)
    loop, runtime = automation(world, requested_ttl_seconds=10, renew_margin_seconds=8)
    store = world.app.state.store

    def shorten_and_review_long():
        principal = loop._principal(loop.policy.nexus_principal_id)
        lease = loop.service.lease(principal, world.project)
        row = store.raw(lease["id"])
        body = json.loads(row["body"])
        body["expires_at"] = time.time() + 6  # Dies at t+6 without maintenance.
        store.save("nexus_lease", row["project"], row["owner"], body)
        time.sleep(7)  # The review outlives the shortened lease.

    runtime.callback = shorten_and_review_long
    result = loop.tick()  # Maintenance (4s interval) renews at ~t+4, before t+6.
    assert result["results"][0]["state"] == "merged"
    lease = json.loads(store.raw(result["lease_id"])["body"])
    assert lease["expires_at"] > time.time()
    run = json.loads(store.raw(loop.policy.nexus_run_id)["body"])
    assert run["last_seen_at"] > time.time() - 30  # Own-run heartbeat stayed fresh.


def test_pending_control_for_own_run_blocks_publication(world):
    *_, request = contribution(world)
    loop, runtime = automation(world)
    store = world.app.state.store

    def raise_control():
        store.save("control", world.project, loop.policy.nexus_principal_id, {
            "id": uid("control"), "created_at": now(), "project_id": world.project,
            "state": "queued", "action": "halt", "coordinator_epoch": 1,
            "target_run_id": loop.policy.nexus_run_id})

    runtime.callback = raise_control
    result = loop.tick()
    assert result["results"][0]["state"] == "control_pending"
    assert main_oid(world) == request["target_oid"]  # Control applies before any merge.


def test_lease_lost_during_long_review_yields_without_publishing(world):
    *_, request = contribution(world)
    target = main_oid(world)
    store = world.app.state.store
    loop, runtime = automation(world)

    def replace_coordinator():
        principal = loop._principal(loop.policy.nexus_principal_id)
        lease = loop.service.lease(principal, world.project)
        row = store.raw(lease["id"])
        body = json.loads(row["body"])
        body["expires_at"] = time.time() - 1  # Lease died during the long review.
        store.save("nexus_lease", row["project"], row["owner"], body)
        nexus(world)  # A replacement coordinator acquires the project.

    runtime.callback = replace_coordinator
    result = loop.tick()
    assert result["results"][0]["state"] == "lease_lost"
    current = world.call("GET", f"merge-requests/{request['id']}")
    assert current["status"] == "open" and current["reviews"] == []
    assert main_oid(world) == target  # Nothing published on a stale lease.


def test_tick_renews_held_lease_and_yields_to_other_coordinator(world):
    *_, request = contribution(world)
    loop, _ = automation(world)
    store = world.app.state.store
    token, run, held = nexus(world)  # Another coordinator acquires the only active lease.
    assert loop.tick()["state"] == "yield"
    assert main_oid(world) == request["target_oid"]  # Nothing merged while yielding.
    with store.transaction():
        row = store.raw(held["id"])
        body = json.loads(row["body"])
        body["expires_at"] = time.time() - 1
        store.save("nexus_lease", row["project"], row["owner"], body)
    result = loop.tick()  # Stale foreign lease expired; automation acquires its own.
    assert result["state"] == "active" and result["results"][0]["state"] == "merged"
    # Renewal: shorten the automation's own lease; the next tick extends it in place.
    lease = json.loads(store.raw(result["lease_id"])["body"])
    with store.transaction():
        lease["expires_at"] = time.time() + 5
        store.save("nexus_lease", world.project, "x", lease)
    loop.tick()
    renewed = json.loads(store.raw(result["lease_id"])["body"])
    assert renewed["expires_at"] > time.time() + 30


def test_abstain_reject_and_missing_reviewer_never_merge(world):
    from dataclasses import replace
    *_, request = contribution(world)
    target = main_oid(world)
    loop, runtime = automation(world, decision="abstain")
    original = loop.policy
    assert loop.tick()["results"][0]["state"] == "review_abstained"
    loop.policy = replace(loop.policy, reviewer_principal_ids=())
    assert loop.tick()["results"][0]["state"] == "waiting_independent_reviewer"
    loop.policy = original
    no_runtime = NexusGitAutomation(world.app.state.service, world.app.state.git_collaboration,
                                    loop.policy)
    assert no_runtime.tick()["results"][0]["state"] == "review_abstained"
    runtime.decision = "reject"
    assert loop.tick()["results"][0]["state"] == "rejected"
    current = world.call("GET", f"merge-requests/{request['id']}")
    assert current["status"] == "open" and main_oid(world) == target
    assert [item["decision"] for item in current["reviews"]] == ["reject"]


def test_stale_candidate_and_failed_check_block_without_retry(world):
    agent, token, run, task, workspace, request = contribution(world)
    loop, _ = automation(world)
    # Advance the agent branch after submission: exact-hash fences must block the merge.
    from pathlib import Path
    path = Path(workspace["workspace_path"])
    (path / "export/result.txt").write_text("later revision", encoding="utf-8")
    binding = {"task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"]}
    committed = world.call("POST", f"git-workspaces/{workspace['id']}:commit", {
        **binding, "expected_revision": workspace["revision"], "message": "Revise export",
        "paths": ["export/result.txt"]}, token)
    world.call("POST", f"git-workspaces/{workspace['id']}:push",
               {**binding, "source_oid": committed["head_oid"]}, token)
    result = loop.tick()
    assert result["results"][0]["state"] == "blocked"
    assert result["results"][0]["code"] == "git_head_changed"
    assert main_oid(world) == request["target_oid"]


def test_unconfigured_check_reports_blocked_item(world):
    contribution(world)
    domain = world.app.state.git_collaboration
    loop, _ = automation(world, checks=("smoke",))
    domain.checks = {}  # Policy names a check the trusted verifier does not have.
    result = loop.tick()
    assert result["results"][0]["state"] == "blocked"
    assert result["results"][0]["code"] == "git_check_not_configured"
