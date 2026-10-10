import json
import sys
import time
from pathlib import Path

import pytest

from hivemind.api import hash_token
from hivemind.git_collaboration import GitReconcile, MergeRequestMerge
from test_task_execution import setup, start


def contribution(world, text="first", agent_setup=None):
    initialized = world.call("POST", f"projects/{world.project}/git:initialize")
    agent, token, run, scope, task = agent_setup or setup(world)
    task = start(world, task, run, token)
    workspace = world.call("POST", "git-workspaces", {"project_id": world.project, "agent_id": agent["id"]}, token)
    path = Path(workspace["workspace_path"])
    (path / "export").mkdir(exist_ok=True)
    (path / "export" / "result.txt").write_text(text, encoding="utf-8")
    binding = {"task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"]}
    committed = world.call("POST", f"git-workspaces/{workspace['id']}:commit", {
        **binding, "expected_revision": workspace["revision"],
        "message": "Implement export result", "paths": ["export/result.txt"]}, token)
    world.call("POST", f"git-workspaces/{workspace['id']}:push",
        {**binding, "source_oid": committed["head_oid"]}, token)
    request = world.call("POST", "merge-requests", {"project_id": world.project,
        "workspace_id": workspace["id"], "task_id": task["id"], "source_oid": committed["head_oid"],
        "target_oid": initialized["main_oid"], "title": "Export implementation"}, token)
    return agent, token, run, task, committed, request


def approve_check(world, request):
    domain = world.app.state.git_collaboration
    script = domain.root / "smoke.py"
    script.write_text("from pathlib import Path\nassert Path('export/result.txt').is_file()\n", encoding="utf-8")
    domain.checks = {"smoke": [sys.executable, str(script)]}
    checked = world.call("POST", f"merge-requests/{request['id']}:check", {
        "expected_revision": request["revision"], "check_name": "smoke"})
    hashes = {key: checked[key] for key in ("source_oid", "target_oid", "candidate_oid")}
    return world.call("POST", f"merge-requests/{request['id']}:review", {
        **hashes, "expected_revision": checked["revision"], "decision": "approve", "reason": "Exact candidate checked"})


def nexus(world):
    agent = world.agent("Nexus")
    token = world.credential(agent, coordinator=True)
    run = world.run(agent, token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
        {"agent_id": agent["id"], "run_id": run["id"], "requested_ttl_seconds": 120}, token)
    return token, run, lease


def merge_body(request, run, lease):
    return {"expected_revision": request["revision"],
        **{key: request[key] for key in ("source_oid", "target_oid", "candidate_oid")},
        "coordinator_epoch": lease["coordinator_epoch"], "lease_id": lease["id"], "run_id": run["id"]}


def test_standard_git_commit_push_candidate_checks_and_fenced_nexus_merge(world):
    agent, token, run, task, workspace, request = contribution(world)
    domain = world.app.state.git_collaboration
    path = Path(workspace["workspace_path"])
    assert "Co-authored-by: factory-droid[bot]" in domain._git(path, "log", "-1", "--format=%B")
    reviewed = approve_check(world, request)
    nexus_token, nexus_run, lease = nexus(world)
    body = merge_body(reviewed, nexus_run, lease)
    world.call("POST", f"merge-requests/{request['id']}:merge", body, status=403)
    merged = world.call("POST", f"merge-requests/{request['id']}:merge", body, nexus_token, key="merge-once")
    assert merged["merged_oid"] == reviewed["candidate_oid"]
    assert domain._ref(domain._repo(world.project), "refs/heads/main") == reviewed["candidate_oid"]
    assert world.call("POST", f"merge-requests/{request['id']}:merge", body, nexus_token, key="merge-once") == merged
    assert world.app.state.store.db.execute("SELECT count(*) FROM audit WHERE operation LIKE 'git:%'").fetchone()[0] >= 10


def test_direct_cli_protected_push_and_self_review_are_rejected(world):
    agent, token, run, task, workspace, request = contribution(world)
    domain = world.app.state.git_collaboration
    path = Path(workspace["workspace_path"])
    code, _ = domain._run(path, "push", str(domain._repo(world.project)), "HEAD:refs/heads/main", allowed=(0, 1))
    assert code == 1
    world.call("POST", f"merge-requests/{request['id']}:review", {
        "expected_revision": request["revision"],
        **{key: request[key] for key in ("source_oid", "target_oid", "candidate_oid")},
        "decision": "approve", "reason": "Self approval"}, token, status=403)


def test_exact_hash_review_and_stale_nexus_fences(world):
    *_, request = contribution(world)
    hashes = {key: request[key] for key in ("source_oid", "target_oid", "candidate_oid")}
    world.call("POST", f"merge-requests/{request['id']}:review", {
        **hashes, "candidate_oid": "0" * 40, "expected_revision": request["revision"],
        "decision": "approve", "reason": "Wrong candidate"}, status=409)
    reviewed = approve_check(world, request)
    token, run, lease = nexus(world)
    store = world.app.state.store
    with store.transaction():
        row = store.raw(lease["id"])
        body = json.loads(row["body"])
        body["expires_at"] = time.time() - 1
        store.save("nexus_lease", row["project"], row["owner"], body)
    new_token, new_run, new_lease = nexus(world)
    world.call("POST", f"merge-requests/{request['id']}:merge", merge_body(reviewed, run, lease), token, status=409)
    assert world.call("POST", f"merge-requests/{request['id']}:merge",
        merge_body(reviewed, new_run, new_lease), new_token)["status"] == "merged"


def test_native_git_conflicts_return_to_agent(world):
    first_contribution = contribution(world, "first")
    first = first_contribution[-1]
    # Prepare both contributors against the original protected tip.
    agent = world.agent("Other contributor")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = json.loads(world.app.state.store.raw(first_contribution[3]["scope_ids"][0])["body"])
    task = world.call("POST", "tasks", {
        "project_id": world.project, "title": "Other export implementation", "scope_ids": [scope["id"]]})
    other = agent, token, run, scope, task
    # Independent clones can develop separately; release the first task's exclusive claim.
    store = world.app.state.store
    with store.transaction():
        rows = store.db.execute("SELECT * FROM resources WHERE kind='work_claim'").fetchall()
        for row in rows:
            claim = json.loads(row["body"])
            claim["expires_at"] = time.time() - 1
            store.save("work_claim", row["project"], row["owner"], claim)
    world.claim(other[0], other[2], other[3], other[1])
    agent, token, run, task, workspace, second = contribution(world, "second", other)
    reviewed = approve_check(world, first)
    n_token, n_run, lease = nexus(world)
    world.call("POST", f"merge-requests/{first['id']}:merge", merge_body(reviewed, n_run, lease), n_token)
    domain = world.app.state.git_collaboration
    target = domain._ref(domain._repo(world.project), "refs/heads/main")
    conflicted = world.call("POST", "merge-requests", {
        "project_id": world.project, "workspace_id": workspace["id"], "task_id": task["id"],
        "source_oid": second["source_oid"], "target_oid": target, "title": "Resolve export conflict"}, token)
    assert conflicted["status"] == "conflicts" and "export/result.txt" in conflicted["conflict_paths"]
    notices = world.call("GET", f"agents/{agent['id']}/notifications?project_id={world.project}", token=token)
    assert any(item["type"] == "git_merge_conflict" for item in notices)


def test_merge_publication_reconciles_after_canonical_rollback(world):
    *_, request = contribution(world)
    reviewed = approve_check(world, request)
    token, run, lease = nexus(world)
    store = world.app.state.store
    actor = dict(store.db.execute("SELECT * FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone())
    domain = world.app.state.git_collaboration
    with pytest.raises(RuntimeError):
        with store.transaction():
            domain.merge(actor, request["id"], MergeRequestMerge(**merge_body(reviewed, run, lease)))
            raise RuntimeError("Simulate response transaction rollback")
    operator = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    with store.transaction():
        reconciled = domain.reconcile(operator, request["id"], GitReconcile(expected_revision=reviewed["revision"]))
    assert reconciled["status"] == "merged"


def test_changed_check_policy_and_git_fence_never_publish(world):
    *_, request = contribution(world)
    reviewed = approve_check(world, request)
    token, run, lease = nexus(world)
    domain = world.app.state.git_collaboration
    configured = dict(domain.checks)
    domain.checks["smoke"] = [sys.executable, "--version"]
    rejected = world.call("POST", f"merge-requests/{request['id']}:merge",
        merge_body(reviewed, run, lease), token, status=409)
    assert rejected["error"]["code"] == "git_passing_checks_required"
    domain.checks = configured
    repo = domain._repo(world.project)
    domain._git(repo, "update-ref", "refs/hivemind/nexus-fence", request["source_oid"])
    rejected = world.call("POST", f"merge-requests/{request['id']}:merge",
        merge_body(reviewed, run, lease), token, status=409)
    assert rejected["error"]["code"] == "stale_coordinator_epoch"
    assert domain._ref(repo, "refs/heads/main") == request["target_oid"]


def test_source_advancement_invalidates_prior_exact_hash_approval(world):
    agent, token, run, task, workspace, request = contribution(world)
    reviewed = approve_check(world, request)
    path = Path(workspace["workspace_path"])
    (path / "export/result.txt").write_text("new source revision", encoding="utf-8")
    binding = {"task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"]}
    next_workspace = world.call("POST", f"git-workspaces/{workspace['id']}:commit", {
        **binding, "expected_revision": workspace["revision"], "message": "Update export result",
        "paths": ["export/result.txt"]}, token)
    world.call("POST", f"git-workspaces/{workspace['id']}:push",
        {**binding, "source_oid": next_workspace["head_oid"]}, token)
    n_token, n_run, lease = nexus(world)
    rejected = world.call("POST", f"merge-requests/{request['id']}:merge",
        merge_body(reviewed, n_run, lease), n_token, status=409)
    assert rejected["error"]["code"] == "git_head_changed"
