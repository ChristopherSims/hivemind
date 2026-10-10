import json
import time
from pathlib import Path

import httpx
import pytest

from hivemind import git_worker_automation as gwa
from hivemind.git_worker_automation import (GitAutomationError, GitAutomationUnknown,
                                            GitWorkerAutomation, VendorGitConflictResolver,
                                            WorkerGitFacade, status_paths)
from hivemind.sdk import Client
from test_git_collaboration import approve_check, contribution, merge_body, nexus
from test_task_execution import setup, start


def sdk_client(world, token):
    def forward(request):
        response = world.client.request(request.method, request.url.raw_path.decode(),
                                        content=request.content, headers=dict(request.headers))
        return httpx.Response(response.status_code, headers=dict(response.headers),
                              content=response.content)

    client = Client("http://testserver", token)
    client.http.close()
    client.http = httpx.Client(base_url="http://testserver/v0.1/",
                               headers={"Authorization": f"Bearer {token}"},
                               transport=httpx.MockTransport(forward))
    return client


def automation(world, agent, run, tmp_path, token=None, **overrides):
    options = {"enabled": True, "allow_native_git": True,
               "state_file": tmp_path / "git-automation-state.json", **overrides}
    return GitWorkerAutomation(sdk_client(world, token or world.owner), world.project,
                               agent["id"], run["id"], **options)


def initialized(world):
    return world.call("POST", f"projects/{world.project}/git:initialize")


def publish_contribution(world, auto, task, text="first", title="Export implementation"):
    workspace = auto.provision()
    path = Path(workspace["workspace_path"])
    (path / "export").mkdir(exist_ok=True)
    (path / "export" / "result.txt").write_text(text, encoding="utf-8")
    return workspace, auto.publish(workspace, task, task["execution"]["id"],
                                   "Implement export result", ["export/result.txt"], title)


def test_disabled_by_default_performs_no_requests(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    auto = automation(world, agent, run, tmp_path, token, enabled=False)
    for operation in (auto.provision, auto.repository):
        with pytest.raises(GitAutomationError) as failure:
            operation()
        assert failure.value.code == "git_automation_disabled"
    assert not (tmp_path / "git-automation-state.json").exists()


def test_bounded_provisioning_is_idempotent_and_capped(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    auto = automation(world, agent, run, tmp_path, token, max_workspaces=1)
    first = auto.provision()
    assert auto.provision()["id"] == first["id"]  # Durable reuse, no second workspace.
    fresh = automation(world, agent, run, tmp_path, token)
    assert fresh.provision()["id"] == first["id"]  # Resumable across process restart.
    # The workspace budget cap is a hard fence, not a hint.
    capped = automation(world, agent, run, tmp_path / "capped", token, max_workspaces=1)
    capped.state["workspaces"]["agent_foreign"] = "git_workspace_foreign"
    with pytest.raises(GitAutomationError) as failure:
        capped.provision()
    assert failure.value.code == "git_workspace_budget_exhausted"


def test_publish_records_exact_hashes_and_preserves_identity(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    start(world, task, run, token)
    task = world.call("GET", f"tasks/{task['id']}", token=token)
    auto = automation(world, agent, run, tmp_path, token)
    workspace, result = publish_contribution(world, auto, task)
    request = result["merge_request"]
    assert result["status"] == "open" and request["candidate_oid"]
    assert request["source_oid"] == result["head_oid"]
    assert request["target_oid"] == initialized(world)["main_oid"]
    path = Path(workspace["workspace_path"])
    domain = world.app.state.git_collaboration
    ident = domain._run(path, "var", "GIT_AUTHOR_IDENT")[1]
    name_email = ident.split(">")[0] + ">"
    assert domain._git(path, "log", "-1", "--format=%an <%ae>") == name_email
    assert "Co-authored-by: factory-droid[bot]" in domain._git(path, "log", "-1", "--format=%B")
    # Exact stored hashes survive a restart of the automation object.
    assert automation(world, agent, run, tmp_path, token).state["workspaces"][agent["id"]] == workspace["id"]


def test_client_side_scope_fence_rejects_before_any_request(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    start(world, task, run, token)
    task = world.call("GET", f"tasks/{task['id']}", token=token)
    auto = automation(world, agent, run, tmp_path, token)
    workspace = auto.provision()
    for paths in (["../escape.txt"], ["export/../escape.txt"], [".env"], ["export\\result.txt"], []):
        with pytest.raises(GitAutomationError) as failure:
            auto.commit(workspace, task, task["execution"]["id"], "Nope", paths)
        assert failure.value.code in ("git_path_not_permitted", "git_change_set_invalid")
    status = auto.workspace_status(workspace["id"])
    assert status["revision"] == workspace["revision"]  # No commit command reached the server.


def test_stale_execution_binding_is_refused(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    start(world, task, run, token)
    task = world.call("GET", f"tasks/{task['id']}", token=token)
    auto = automation(world, agent, run, tmp_path, token)
    workspace = auto.provision()
    with pytest.raises(GitAutomationError) as failure:
        auto.commit(workspace, task, "execution_foreign", "Nope", ["export/result.txt"])
    assert failure.value.code == "stale_task_execution"


def second_contributor(world, first_task):
    store = world.app.state.store
    with store.transaction():
        for row in store.db.execute("SELECT * FROM resources WHERE kind='work_claim'").fetchall():
            claim = json.loads(row["body"])
            claim["expires_at"] = time.time() - 1
            store.save("work_claim", row["project"], row["owner"], claim)
    scope = json.loads(world.app.state.store.raw(first_task["scope_ids"][0])["body"])
    agent = world.agent("Other contributor")
    token = world.credential(agent)
    run = world.run(agent, token)
    task = world.call("POST", "tasks", {
        "project_id": world.project, "title": "Other export implementation", "scope_ids": [scope["id"]]})
    world.claim(agent, run, scope, token)
    start(world, task, run, token)
    return agent, token, run, world.call("GET", f"tasks/{task['id']}", token=token)


def merge_first(world, first):
    reviewed = approve_check(world, first)
    n_token, n_run, lease = nexus(world)
    world.call("POST", f"merge-requests/{first['id']}:merge", merge_body(reviewed, n_run, lease), n_token)
    return reviewed


def test_conflict_is_durable_bounded_and_resumable(world, tmp_path):
    agent, token, run, task, workspace, first = contribution(world)
    other_agent, other_token, other_run, other_task = second_contributor(world, task)
    auto = GitWorkerAutomation(sdk_client(world, other_token), world.project,
                               other_agent["id"], other_run["id"], enabled=True,
                               allow_native_git=True, state_file=tmp_path / "other-state.json")
    # Provision before the first merge so the branch stems from the original tip.
    other_workspace = auto.provision()
    reviewed = merge_first(world, first)
    # Preserve dirty user work across a protected-change fetch.
    path = Path(other_workspace["workspace_path"])
    (path / "export").mkdir(exist_ok=True)
    (path / "export" / "notes.txt").write_text("uncommitted user work", encoding="utf-8")
    fetched = auto.fetch_protected(other_workspace)
    assert fetched["changed"] and fetched["after_oid"] == reviewed["candidate_oid"]
    assert (path / "export" / "notes.txt").read_text(encoding="utf-8") == "uncommitted user work"
    (path / "export" / "result.txt").write_text("second", encoding="utf-8")
    result = auto.publish(other_workspace, other_task, other_task["execution"]["id"],
                          "Implement export result", ["export/result.txt"], "Resolve export conflict")
    request = result["merge_request"]
    assert result["status"] == "conflicts" and "export/result.txt" in request["conflict_paths"]
    session = result["conflict"]
    assert session["state"] == "unresolved" and session["attempts"] == 0
    # Durable and resumable in a fresh automation instance.
    resumed = GitWorkerAutomation(sdk_client(world, other_token), world.project,
                                  other_agent["id"], other_run["id"], enabled=True,
                                  allow_native_git=True, state_file=tmp_path / "other-state.json")
    assert [item["merge_request_id"] for item in resumed.conflicts()] == [request["id"]]

    def failing_resolver(workspace_path, conflict_paths, session):
        return conflict_paths  # Claims resolution without doing anything.

    outcome = resumed.resolve_conflict(request["id"], failing_resolver)
    assert outcome["status"] == "unresolved"  # Verification never invents success.
    assert outcome["session"]["attempts"] == 1

    def resolver(workspace_path, conflict_paths, session):
        resumed._git(workspace_path, "merge", "--no-edit", "origin/main", allowed=(0, 1))
        Path(workspace_path, "export/result.txt").write_text("merged result", encoding="utf-8")
        resumed._git(workspace_path, "add", "--", *conflict_paths)
        return conflict_paths

    resolved = resumed.resolve_conflict(request["id"], resolver)
    assert resolved["status"] == "open"
    followup = resolved["merge_request"]
    assert followup["id"] != request["id"] and followup["candidate_oid"]
    assert resolved["session"]["state"] == "resubmitted"
    assert resumed.conflicts() == [] and resumed.conflicts(include_resubmitted=True)
    original = world.call("GET", f"merge-requests/{request['id']}", token=other_token)
    assert original["status"] == "conflicts"  # The conflicted request remains durable.
    domain = world.app.state.git_collaboration
    parents = domain._git(domain._repo(world.project), "show", "--no-patch",
                          "--format=%P", followup["candidate_oid"]).split()
    assert len(parents) == 2  # A true merge commit, not an invented linear resolution.


def test_resolution_attempt_budget_is_bounded(world, tmp_path):
    _, _, _, first_task, _, first = contribution(world)
    other_agent, other_token, other_run, other_task = second_contributor(world, first_task)
    auto = GitWorkerAutomation(sdk_client(world, other_token), world.project,
                               other_agent["id"], other_run["id"], enabled=True,
                               allow_native_git=True, max_resolution_attempts=2,
                               state_file=tmp_path / "state.json")
    workspace = auto.provision()  # Before the first merge: branch from the original tip.
    merge_first(world, first)
    path = Path(workspace["workspace_path"])
    (path / "export").mkdir(exist_ok=True)
    (path / "export" / "result.txt").write_text("second", encoding="utf-8")
    result = auto.publish(workspace, other_task, other_task["execution"]["id"],
                          "Implement export result", ["export/result.txt"], "Second export")
    assert result["status"] == "conflicts"
    merge_request_id = result["merge_request"]["id"]

    def noop_resolver(workspace_path, conflict_paths, session):
        return conflict_paths

    for expected_attempts in (1, 2):
        outcome = auto.resolve_conflict(merge_request_id, noop_resolver)
        assert outcome["status"] == "unresolved"
        assert outcome["session"]["attempts"] == expected_attempts
    with pytest.raises(GitAutomationError) as failure:
        auto.resolve_conflict(merge_request_id, noop_resolver)
    assert failure.value.code == "git_resolution_budget_exhausted"
    # The session is still durable and inspectable after the budget is exhausted.
    assert auto.conflicts()[0]["attempts"] == 2


def test_native_git_requires_explicit_opt_in(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    auto = automation(world, agent, run, tmp_path, token, allow_native_git=False)
    workspace = auto.provision()
    with pytest.raises(GitAutomationError) as failure:
        auto.fetch_protected(workspace)
    assert failure.value.code == "git_native_access_disabled"


def test_unknown_native_outcomes_are_fenced(world, tmp_path, monkeypatch):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    auto = automation(world, agent, run, tmp_path, token)
    workspace = auto.provision()

    def timeout(*args, **kwargs):
        raise TimeoutError("deadline")

    monkeypatch.setattr(gwa, "run_bounded", timeout)
    with pytest.raises(GitAutomationUnknown):
        auto.fetch_protected(workspace)


class FakeRuntime:
    def __init__(self, workspace):
        self.workspace = workspace
        self.calls = []

    def execute(self, task, context, checkpoint):
        self.calls.append({"task_id": task["id"], "context": context})
        return {"status": "completed", "summary": "done"}


def test_status_paths_parses_porcelain():
    assert status_paths(" M export/result.txt\0?? notes.txt\0") == [
        (" M", "export/result.txt"), ("??", "notes.txt")]
    assert status_paths("R  new.txt\0old.txt\0") == [("R ", "new.txt")]
    with pytest.raises(GitAutomationUnknown):
        status_paths("garbage")


def test_prepare_binds_runtime_and_refuses_user_work(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    start(world, task, run, token)
    task = world.call("GET", f"tasks/{task['id']}", token=token)
    auto = automation(world, agent, run, tmp_path, token)
    facade = WorkerGitFacade(auto)
    workspace = auto.provision()
    path = Path(workspace["workspace_path"])
    runtime = FakeRuntime(path)
    assert facade.prepare(task, runtime)["id"] == workspace["id"]  # Clean clone binds.
    with pytest.raises(GitAutomationError) as failure:
        facade.prepare(task, FakeRuntime(tmp_path))  # Wrong workspace.
    assert failure.value.code == "git_runtime_workspace_mismatch"
    (path / "user-draft.txt").write_text("user work", encoding="utf-8")
    with pytest.raises(GitAutomationError) as failure:
        facade.prepare(task, runtime)  # Untracked user work present.
    assert failure.value.code == "git_workspace_dirty"
    (path / "user-draft.txt").unlink()
    (path / "export").mkdir(exist_ok=True)
    (path / "export" / "staged.txt").write_text("staged", encoding="utf-8")
    auto._git(str(path), "add", "--", "export/staged.txt")
    with pytest.raises(GitAutomationError) as failure:
        facade.prepare(task, runtime)  # Staged user work present.
    assert failure.value.code == "git_workspace_dirty"


def test_finish_publishes_literal_changes_and_clean_is_honest(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    start(world, task, run, token)
    task = world.call("GET", f"tasks/{task['id']}", token=token)
    auto = automation(world, agent, run, tmp_path, token)
    facade = WorkerGitFacade(auto)
    provisioned = auto.provision()
    workspace = facade.prepare(task, FakeRuntime(Path(provisioned["workspace_path"])))
    path = Path(workspace["workspace_path"])
    runtime = FakeRuntime(path)
    checkpoints = []

    def checkpoint(summary, progress=None, status="running", evidence_refs=None):
        checkpoints.append({"summary": summary, "status": status})
        return {}

    context = {"phase_id": None}
    clean = facade.finish(task, task["execution"]["id"], runtime, context, checkpoint)
    assert clean == {"status": "clean", "changed_paths": [], "published": False}
    assert checkpoints == []  # Nothing claimed for a no-change turn.
    (path / "export").mkdir(exist_ok=True)
    (path / "export" / "result.txt").write_text("turn output", encoding="utf-8")
    result = facade.finish(task, task["execution"]["id"], runtime, context, checkpoint)
    assert result["status"] == "open" and result["published"]
    assert result["changed_paths"] == ["export/result.txt"]
    assert result["merge_request"]["source_oid"] == result["head_oid"]
    assert checkpoints and checkpoints[0]["status"] == "running"  # Recorded while running.
    running = world.call("GET", f"tasks/{task['id']}", token=token)
    assert running["status"] not in ("completed", "failed")  # Canonical task still running.
    with pytest.raises(GitAutomationError) as failure:
        facade.finish(task, "execution_foreign", runtime, context, checkpoint)
    assert failure.value.code == "stale_task_execution"


def test_vendor_resolver_with_conflict_and_protected_baseline(world, tmp_path):
    initialized(world)
    # First contributor: two files under the same scope, one becomes protected baseline.
    agent, token, run, scope, task = setup(world)
    start(world, task, run, token)
    first_auto = automation(world, agent, run, tmp_path / "first", token)
    first_ws = first_auto.provision()
    first_path = Path(first_ws["workspace_path"])
    (first_path / "export").mkdir(exist_ok=True)
    (first_path / "export" / "result.txt").write_text("first", encoding="utf-8")
    (first_path / "export" / "base.txt").write_text("protected baseline v2", encoding="utf-8")
    task = world.call("GET", f"tasks/{task['id']}", token=token)
    published = first_auto.publish(first_ws, task, task["execution"]["id"],
                                   "Result and baseline", ["export/result.txt", "export/base.txt"],
                                   "Result and baseline")
    first = published["merge_request"]
    assert first["status"] == "open"
    # Second contributor branches from the original tip and conflicts on result.txt.
    other_agent, other_token, other_run, other_task = second_contributor(world, task)
    auto = GitWorkerAutomation(sdk_client(world, other_token), world.project,
                               other_agent["id"], other_run["id"], enabled=True,
                               allow_native_git=True, state_file=tmp_path / "other.json")
    other_ws = auto.provision()
    reviewed = approve_check(world, first)
    n_token, n_run, lease = nexus(world)
    world.call("POST", f"merge-requests/{first['id']}:merge", merge_body(reviewed, n_run, lease), n_token)
    other_path = Path(other_ws["workspace_path"])
    (other_path / "export").mkdir(exist_ok=True)
    (other_path / "export" / "result.txt").write_text("second", encoding="utf-8")
    result = auto.publish(other_ws, other_task, other_task["execution"]["id"],
                          "Second result", ["export/result.txt"], "Second result")
    request = result["merge_request"]
    assert result["status"] == "conflicts"
    assert sorted(set(request["conflict_paths"])) == ["export/result.txt"]

    class ResolvingRuntime(FakeRuntime):
        enabled = True
        allowed = True

        def execute(self, task, context, checkpoint):
            super().execute(task, context, checkpoint)
            Path(self.workspace, "export/result.txt").write_text("vendor merged", encoding="utf-8")
            return {"status": "completed", "summary": "resolved"}

    runtime = ResolvingRuntime(other_path)
    context = {"project_id": world.project, "agent_id": other_agent["id"],
               "run_id": other_run["id"], "execution_id": other_task["execution"]["id"],
               "phase_id": None}
    resolver = VendorGitConflictResolver(auto, runtime, other_task, context, lambda *a, **k: {})
    resolved = auto.resolve_conflict(request["id"], resolver)
    assert resolved["status"] == "open"
    followup = resolved["merge_request"]
    # The vendor turn ran on the real task/execution with a new deterministic phase.
    call = runtime.calls[0]
    assert call["task_id"] == other_task["id"]
    assert call["context"]["phase_id"] == "git-conflict-" + request["id"]
    assert call["context"]["execution_id"] == other_task["execution"]["id"]
    assert sorted(set(call["context"]["git_conflict_paths"])) == ["export/result.txt"]
    domain = world.app.state.git_collaboration
    repo = domain._repo(world.project)
    # True merge commit; conflict file resolved by the vendor turn.
    parents = domain._git(repo, "show", "--no-patch", "--format=%P", followup["candidate_oid"]).split()
    assert len(parents) == 2
    merged = domain._git(repo, "show", followup["candidate_oid"] + ":export/result.txt")
    assert merged == "vendor merged"
    # The protected-baseline file came through the merge byte-identical to main.
    baseline = domain._git(repo, "show", followup["candidate_oid"] + ":export/base.txt")
    assert baseline == "protected baseline v2"
    assert baseline == domain._git(repo, "show", reviewed["candidate_oid"] + ":export/base.txt")


def test_vendor_resolver_refuses_without_runtime_opt_ins(world, tmp_path):
    initialized(world)
    agent, token, run, scope, task = setup(world)
    auto = automation(world, agent, run, tmp_path, token)

    class Disabled:
        enabled = False
        allowed = False
        workspace = None

        def execute(self, *args):
            raise AssertionError("must not run")

    with pytest.raises(GitAutomationError) as failure:
        VendorGitConflictResolver(auto, Disabled(), task, {}, None)
    assert failure.value.code == "git_resolver_runtime_disabled"
    plain = automation(world, agent, run, tmp_path / "plain", token, allow_native_git=False)
    with pytest.raises(GitAutomationError) as failure:
        VendorGitConflictResolver(plain, FakeRuntime(tmp_path), task, {}, None)
    assert failure.value.code == "git_native_access_disabled"
