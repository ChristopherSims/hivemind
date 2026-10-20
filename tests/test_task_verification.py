"""Objective verification tests: independent review, trusted checks, corrections.

Only the verification module and its routes are exercised. Every external collaborator
(principal resolution, command/idempotency wrapper, envelope, trusted runner) is either
supplied by the harness or injected, and the trusted runner is synthetic unless a test
explicitly checks the real bounded subprocess path.
"""

import hashlib
import json
import secrets
import sys
from pathlib import Path

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import verification as v
from hivemind.api import create_app, envelope, hash_token
from hivemind.models import Model
from hivemind.storage import Problem, uid


# ---------------------------------------------------------------------------
# harness
# ---------------------------------------------------------------------------

class SyntheticRunner:
    """Bounded synthetic runner: records commands, returns or raises on demand."""

    def __init__(self, returncode=0, stdout=b"", stderr=b"", raises=None):
        self.returncode, self.stdout, self.stderr, self.raises = returncode, stdout, stderr, raises
        self.commands = []

    def __call__(self, command):
        self.commands.append(command)
        if self.raises is not None:
            raise self.raises
        return self.returncode, self.stdout, self.stderr


def install_on(app, explicit=False, verifier=None):
    """Install verification routes, optionally with explicit host collaborators."""
    service = app.state.service
    if not explicit:
        return v.install(app, verifier=verifier)
    store = service.store

    def principal(request: Request):
        authorization = request.headers.get("authorization", "")
        if not authorization.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hashlib.sha256(authorization[7:].encode()).hexdigest(),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = request.model_dump(exclude_none=True) if isinstance(request, Model) else request
        return envelope(service.mutate(p, operation, key, payload, function))

    return v.install(app, service, principal, command, envelope, verifier=verifier)


def install(world, verifier=None, explicit=False):
    return install_on(world.app, explicit=explicit, verifier=verifier)


def workspace(world):
    root = Path(world.app.state.store.directory) / "workspace"
    root.mkdir(exist_ok=True)
    return root


def setup(world):
    """A completed, self-reported task execution owned by one producing agent."""
    agent = world.agent("Verification worker")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope("tasks/verify", paths=["export"])
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Implement export",
                                        "scope_ids": [scope["id"]]})
    world.claim(agent, run, scope, token)
    active = world.call("POST", f"tasks/{task['id']}:start",
                        {"expected_revision": task["revision"], "run_id": run["id"]}, token)
    return agent, token, run, complete(world, token, run, active)


def complete(world, token, run, task):
    return world.call("POST", f"tasks/{task['id']}:checkpoint", {
        "expected_revision": task["revision"], "run_id": run["id"],
        "execution_id": task["execution"]["id"], "checkpoint_id": uid("checkpoint"),
        "status": "completed", "progress": 100, "summary": "Export implemented"}, token)


def second_task(world, agent, token, run):
    """A second completed execution on the same run, for tests needing a fresh verification."""
    scope = world.scope(f"tasks/verify/{uid('scope')}", paths=["export"])
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Second task",
                                        "scope_ids": [scope["id"]]})
    world.claim(agent, run, scope, token)
    active = world.call("POST", f"tasks/{task['id']}:start",
                        {"expected_revision": task["revision"], "run_id": run["id"]}, token)
    return complete(world, token, run, active)


def artifact(world, token, run, text="export result"):
    return world.call("POST", "artifacts", {"project_id": world.project, "title": "Export artifact",
                                            "content": {"media_type": "text/plain", "text": text},
                                            "run_id": run["id"]}, token)


def payload(task, run, refs=None, **overrides):
    return {"task_id": task["id"], "run_id": run["id"],
            "execution_id": (task.get("execution") or {}).get("id"),
            "completion_claimed": True, "summary": "Export implemented; unit tests reported passing",
            "artifact_refs": refs or [],
            "test_evidence": [{"name": "unit", "outcome": "pass", "detail": "reported by the agent"}],
            **overrides}


def error_code(world, method, path, body=None, token=None, status=200):
    """Assert an exact protocol error code, so a denial cannot pass for the wrong reason."""
    response = world.client.request(method, "/v1/" + path, json=body, headers={
        "Authorization": "Bearer " + (token or world.owner), "Idempotency-Key": uid("key")})
    assert response.status_code == status, response.text
    return response.json()["error"]["code"]


def submit(world, token, task, run, refs=None, key=None, status=200, **overrides):
    return world.call("POST", "verifications", payload(task, run, refs, **overrides), token, key, status)


def assign(world, body, reviewer, token=None, status=200):
    return world.call("POST", f"verifications/{body['id']}:assign-reviewer",
                      {"expected_revision": body["revision"], "reviewer_agent_id": reviewer["id"]},
                      token, status=status)


def check(world, body, name="suite", paths=None, token=None, status=200):
    return world.call("POST", f"verifications/{body['id']}:check",
                      {"expected_revision": body["revision"], "check_name": name,
                       "artifact_paths": paths or []}, token, status=status)


def decide(world, body, token, decision="verified", reason="Independent review", status=200, **overrides):
    return world.call("POST", f"verifications/{body['id']}:decide",
                      {"expected_revision": body["revision"], "decision": decision, "reason": reason,
                       **overrides}, token, status=status)


def passing_verifier(root, runner=None):
    return v.TrustedVerifier(root, {"suite": ["pytest", "-q"], "lint": ["ruff", "check", "."]},
                             runner=runner or SyntheticRunner(returncode=0, stdout=b"3 passed\n"))


# ---------------------------------------------------------------------------
# trusted local verifier helper
# ---------------------------------------------------------------------------

def test_trusted_verifier_runs_only_allowlisted_argv_and_captures_hashes(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "report.txt").write_text("evidence")
    runner = SyntheticRunner(returncode=0, stdout=b"x" * 200)
    trusted = v.TrustedVerifier(root, {"suite": ["pytest", "-q"]}, runner=runner, max_output_bytes=64)

    result = trusted.run("suite", ["report.txt"])

    assert result.ok is True and result.returncode == 0 and result.timed_out is False
    assert result.cwd == str(root.resolve())
    assert runner.commands[0].argv == ("pytest", "-q") and runner.commands[0].timeout == 60.0
    assert result.stdout_truncated is True and result.stdout_bytes == 200 and len(result.stdout) == 64
    assert result.stdout_sha256 == hashlib.sha256(b"x" * 200).hexdigest()
    assert result.artifacts[0]["path"] == "report.txt"
    assert result.artifacts[0]["sha256"] == hashlib.sha256(b"evidence").hexdigest()

    failing = v.TrustedVerifier(root, {"suite": ["pytest"]}, runner=SyntheticRunner(returncode=1, stderr=b"boom"))
    failed = failing.run("suite")
    assert failed.ok is False and failed.returncode == 1
    assert failed.stderr_sha256 == hashlib.sha256(b"boom").hexdigest()

    timed = v.TrustedVerifier(root, {"suite": ["pytest"]}, runner=SyntheticRunner(raises=TimeoutError()))
    expired = timed.run("suite")
    assert expired.timed_out is True and expired.ok is False and expired.returncode is None

    missing = trusted.run("suite", ["absent.txt"])
    assert missing.artifacts[0]["missing"] is True


def test_trusted_verifier_rejects_unknown_checks_and_workspace_escapes(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "report.txt").write_text("evidence")
    trusted = v.TrustedVerifier(root, {"suite": ["pytest"]})

    with pytest.raises(Problem) as unknown:
        trusted.run("not-allowlisted")
    assert unknown.value.code == "check_not_allowlisted"

    with pytest.raises(Problem) as traversal:
        trusted.run("suite", ["../outside.txt"])
    assert traversal.value.code == "invalid_artifact_path"

    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    link = root / "link.txt"
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are unavailable on this platform")
    with pytest.raises(Problem) as escaped:
        trusted.run("suite", ["link.txt"])
    assert escaped.value.code == "artifact_path_escape"


def test_allowlist_refuses_shells_inline_source_and_urls(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    rejected = [
        {"bad": ["sh", "-c", "echo hi"]},
        {"bad": ["bash", "-lc", "ls"]},
        {"bad": [sys.executable, "-c", "print(1)"]},
        {"bad": ["curl", "https://example.test/install.sh"]},
        {"bad": ["../escape", "--version"]},
        {"bad": ["-x"]},
        {"bad": []},
    ]
    for checks in rejected:
        with pytest.raises(ValueError):
            v.TrustedVerifier(root, checks)
    with pytest.raises(ValueError):
        v.TrustedVerifier(tmp_path / "missing", {"ok": ["pytest"]})
    with pytest.raises(ValueError):
        v.TrustedVerifier(root, {"ok": ["pytest"]}, timeout=0)
    with pytest.raises(ValueError):
        v.TrustedVerifier(root, {})


def test_environment_is_sanitized_and_real_runner_is_bounded(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    root.mkdir()
    monkeypatch.setenv("HIVEMIND_TEST_SECRET", "leak")
    trusted = v.TrustedVerifier(root, {"version": [sys.executable, "--version"]},
                                extra_environment={"HIVEMIND_CHECK": "1"})
    environment = trusted.environment()
    assert "HIVEMIND_TEST_SECRET" not in environment
    assert environment["HIVEMIND_CHECK"] == "1"

    result = trusted.run("version")
    assert result.ok is True and result.returncode == 0
    assert "Python" in result.stdout + result.stderr


# ---------------------------------------------------------------------------
# submissions: reported completion is not verification
# ---------------------------------------------------------------------------

def test_submission_binds_execution_and_stays_self_reported(world):
    install(world)
    agent, token, run, task = setup(world)
    proof = artifact(world, token, run)
    refs = [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]}]

    body = submit(world, token, task, run, refs)

    assert body["status"] == "pending" and body["effective_status"] == "pending"
    assert body["reported_completion"] is True and body["independently_verified"] is False
    assert body["reported"]["trust"] == "self_reported" and body["reported"]["objective"] is False
    assert body["test_evidence"][0]["objective"] is False
    assert body["test_evidence"][0]["trust"] == "self_reported"
    assert body["evidence_refs"] == refs
    assert body["evidence_available"] is True
    assert body["task_snapshot"]["status"] == "completed"
    assert body["execution_id"] == task["execution"]["id"] and body["agent_id"] == agent["id"]

    # the task keeps its own self-reported terminal state and is never rewritten
    detail = world.call("GET", f"tasks/{task['id']}", token=token)
    assert detail["status"] == "completed" and detail["revision"] == task["revision"]

    # one open verification per task execution
    assert error_code(world, "POST", "verifications", payload(task, run, refs), token, 409) == "verification_open"
    # a stale, foreign, or unstarted execution cannot submit
    assert error_code(world, "POST", "verifications", payload(task, run, refs, execution_id=uid("execution")),
                      token, 409) == "stale_task_execution"
    other = world.agent("Other worker")
    other_token = world.credential(other)
    other_run = world.run(other, other_token)
    assert error_code(world, "POST", "verifications", payload(task, other_run, refs), other_token,
                      409) == "stale_task_execution"
    unscoped = world.call("POST", "tasks", {"project_id": world.project, "title": "Not started"})
    assert error_code(world, "POST", "verifications", payload(unscoped, run, execution_id=uid("execution")),
                      token, 409) == "task_execution_required"
    world.call("POST", "verifications", {"task_id": uid("task"), "run_id": run["id"],
                                         "execution_id": task["execution"]["id"],
                                         "completion_claimed": True, "summary": "Unknown task"},
               token, status=404)


def test_reported_test_text_never_satisfies_verification(world):
    root = workspace(world)
    (root / "report.txt").write_text("trusted evidence")
    runner = SyntheticRunner(returncode=0, stdout=b"3 passed\n")
    install(world, verifier=passing_verifier(root, runner))
    agent, token, run, task = setup(world)
    reviewer = world.agent("Independent reviewer")
    reviewer_token = world.credential(reviewer)
    proof = artifact(world, token, run)
    refs = [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]}]

    body = submit(world, token, task, run, refs)
    assigned = assign(world, body, reviewer, token)
    assert assigned["reviewer_agent_id"] == reviewer["id"]

    # a producer-reported passing test plus a producer claim is not objective proof
    decide(world, assigned, reviewer_token, status=422)
    assert error_code(world, "POST", f"verifications/{body['id']}:decide",
                      {"expected_revision": assigned["revision"], "decision": "verified", "reason": "reported"},
                      reviewer_token, 422) == "trusted_check_required"

    # a trusted local check records the return code, output digest, and artifact hash
    checked = check(world, assigned, paths=["report.txt"])
    record = checked["checks"][0]
    assert record["returncode"] == 0 and record["ok"] is True and record["objective"] is True
    assert record["trust"] == "trusted_local_check"
    assert record["stdout_sha256"] == hashlib.sha256(b"3 passed\n").hexdigest()
    assert record["artifacts"][0]["sha256"] == hashlib.sha256(b"trusted evidence").hexdigest()

    verified = decide(world, checked, reviewer_token, reason="Allowlisted suite passed locally")
    assert verified["status"] == "verified" and verified["effective_status"] == "verified"
    assert verified["independently_verified"] is True
    assert verified["review"]["reviewer_agent_id"] == reviewer["id"]
    assert verified["review"]["trust"] == "independent_review"

    # no automatic promotion to memory, and the task record is untouched
    assert world.call("GET", f"memories?project_id={world.project}") == []
    detail = world.call("GET", f"tasks/{task['id']}", token=token)
    assert detail["status"] == "completed" and detail["revision"] == task["revision"]

    # a producer-reported failure blocks verification even with a passing trusted check
    second = second_task(world, agent, token, run)
    reported = submit(world, token, second, run, [],
                      test_evidence=[{"name": "integration", "outcome": "fail"}])
    second_assigned = assign(world, reported, reviewer, token)
    second_checked = check(world, second_assigned)
    decide(world, second_checked, reviewer_token, status=422)
    assert error_code(world, "POST", f"verifications/{reported['id']}:decide",
                      {"expected_revision": second_checked["revision"], "decision": "verified",
                       "reason": "trusted check passed"}, reviewer_token, 422) == "reported_test_failure"

    # a failing trusted check cannot be verified either
    failing = SyntheticRunner(returncode=1, stderr=b"1 failed")
    world.app.state.trusted_verifier = passing_verifier(root, failing)
    third = second_task(world, agent, token, run)
    third_body = submit(world, token, third, run, [])
    third_assigned = assign(world, third_body, reviewer, token)
    third_checked = check(world, third_assigned)
    assert third_checked["checks"][0]["ok"] is False
    decide(world, third_checked, reviewer_token, status=422)
    assert error_code(world, "POST", f"verifications/{third_body['id']}:decide",
                      {"expected_revision": third_checked["revision"], "decision": "verified",
                       "reason": "failed check"}, reviewer_token, 422) == "trusted_check_failed"


def test_check_requires_a_trusted_operator_and_a_configured_verifier(world):
    install(world)  # no verifier configured
    agent, token, run, task = setup(world)
    body = submit(world, token, task, run)

    check(world, body, token=token, status=403)
    check(world, body, token=world.credential(role="observer"), status=403)
    check(world, body, status=503)
    world.call("POST", f"verifications/{body['id']}:check",
               {"expected_revision": body["revision"], "check_name": "not-allowlisted"}, status=503)


def test_reviewer_assignment_requires_an_independent_agent(world):
    install(world)
    agent, token, run, task = setup(world)
    reviewer = world.agent("Reviewer")
    outsider_token = world.credential(world.agent("Outsider"))
    observer_token = world.credential(role="observer")
    body = submit(world, token, task, run)

    # the producer may request an independent reviewer, but never itself
    assigned = assign(world, body, reviewer, token)
    assert assigned["assignment"]["assigned_by_agent_id"] == agent["id"]
    assert error_code(world, "POST", f"verifications/{body['id']}:assign-reviewer",
                      {"expected_revision": assigned["revision"], "reviewer_agent_id": agent["id"]},
                      token, 422) == "reviewer_must_differ_from_producer"
    # unrelated agents and observers cannot assign reviewers
    assert error_code(world, "POST", f"verifications/{body['id']}:assign-reviewer",
                      {"expected_revision": assigned["revision"], "reviewer_agent_id": reviewer["id"]},
                      outsider_token, 403) == "reviewer_assignment_forbidden"
    assert error_code(world, "POST", f"verifications/{body['id']}:assign-reviewer",
                      {"expected_revision": assigned["revision"], "reviewer_agent_id": reviewer["id"]},
                      observer_token, 403) == "reviewer_assignment_forbidden"

    # only the assigned reviewer (or an administrator) may decide
    decide(world, assigned, token, decision="rejected", reason="producer", status=403)
    decide(world, assigned, outsider_token, decision="rejected", reason="outsider", status=403)
    decide(world, assigned, observer_token, decision="rejected", reason="observer", status=403)
    assert error_code(world, "POST", f"verifications/{body['id']}:decide",
                      {"expected_revision": assigned["revision"], "decision": "rejected", "reason": "producer"},
                      token, 403) == "forbidden"

    # a decision before any assignment is refused
    second = second_task(world, agent, token, run)
    unassigned = submit(world, token, second, run, [])
    assert error_code(world, "POST", f"verifications/{unassigned['id']}:decide",
                      {"expected_revision": unassigned["revision"], "decision": "rejected", "reason": "no"},
                      world.credential(reviewer), 409) == "reviewer_assignment_required"

    # an administrator may record a decision for the assigned reviewer
    decided = decide(world, assigned, world.owner, decision="rejected", reason="Trusted check missing")
    assert decided["status"] == "rejected" and decided["review"]["reviewer_agent_id"] == reviewer["id"]


def test_corrections_are_explicit_and_bound_to_the_original_execution(world):
    root = workspace(world)
    (root / "report.txt").write_text("first")
    install(world, verifier=passing_verifier(root))
    agent, token, run, task = setup(world)
    reviewer = world.agent("Reviewer")
    reviewer_token = world.credential(reviewer)
    proof = artifact(world, token, run)
    refs = [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]}]

    body = submit(world, token, task, run, refs)
    assigned = assign(world, body, reviewer, token)
    requested = decide(world, assigned, reviewer_token, decision="needs_corrections",
                       reason="The export is incomplete",
                       correction_items=["Cover the empty export", "Add a regression test"])
    assert requested["status"] == "needs_corrections" and requested["corrections"]["cycles"] == 1
    correction_id = requested["corrections"]["open_ids"][0]

    listing = world.call("GET", f"verifications/{body['id']}/corrections", token=token)
    assert [item["state"] for item in listing["corrections"]] == ["open"]
    assert world.call("GET", f"corrections/{correction_id}", token=token)["items"] == [
        "Cover the empty export", "Add a regression test"]

    # trusted checks may still be recorded while corrections are open
    rechecked = check(world, requested)
    assert rechecked["status"] == "needs_corrections" and len(rechecked["checks"]) == 1
    # open corrections block verification, and a second correction cannot be stacked
    decide(world, rechecked, reviewer_token, status=409)
    decide(world, rechecked, reviewer_token, decision="needs_corrections", status=409)
    assert error_code(world, "POST", f"verifications/{body['id']}:decide",
                      {"expected_revision": rechecked["revision"], "decision": "verified", "reason": "ok"},
                      reviewer_token, 409) == "corrections_open"

    # only the producing agent resolves corrections, and only for the original execution
    resolve = {"run_id": run["id"], "execution_id": task["execution"]["id"], "summary": "Fixed"}
    assert error_code(world, "POST", f"corrections/{correction_id}:resolve",
                      {**resolve, "expected_revision": 1}, reviewer_token, 403) == "forbidden"
    assert error_code(world, "POST", f"corrections/{correction_id}:resolve",
                      {**resolve, "expected_revision": 1, "execution_id": uid("execution")}, token,
                      409) == "stale_task_execution"

    resolved = world.call("POST", f"corrections/{correction_id}:resolve",
                          {**resolve, "expected_revision": 1, "artifact_refs": refs}, token)
    assert resolved["state"] == "resolved" and resolved["evidence_available"] is True
    after = world.call("GET", f"verifications/{body['id']}", token=token)
    assert after["status"] == "pending" and after["corrections"]["open_ids"] == []
    assert correction_id in after["corrections"]["resolved_ids"]
    assert after["correction_history"][-1]["event"] == "resolved"
    assert after["evidence_refs"] == refs  # the resolution restated the same pinned evidence

    # the reviewer hands off to a different independent reviewer after the fix
    second_reviewer = world.agent("Second reviewer")
    handoff = assign(world, after, second_reviewer, token)
    assert handoff["reviewer_agent_id"] == second_reviewer["id"]
    decide(world, handoff, reviewer_token, status=403)  # the previous reviewer no longer decides
    checked = check(world, handoff)
    final = decide(world, checked, world.credential(second_reviewer), reason="Corrections verified")
    assert final["status"] == "verified" and final["independently_verified"] is True
    assert final["review"]["reviewer_agent_id"] == second_reviewer["id"]

    world.call("POST", f"corrections/{correction_id}:resolve",
               {**resolve, "expected_revision": resolved["revision"]}, token, status=409)
    assert error_code(world, "POST", f"corrections/{correction_id}:resolve",
                      {**resolve, "expected_revision": resolved["revision"]}, token, 409) == "correction_not_open"


def test_correction_cycles_are_bounded(world):
    domain = install(world)
    domain.max_cycles = 2
    agent, token, run, task = setup(world)
    reviewer = world.agent("Reviewer")
    reviewer_token = world.credential(reviewer)
    body = submit(world, token, task, run)
    current = assign(world, body, reviewer, token)
    resolve = {"run_id": run["id"], "execution_id": task["execution"]["id"], "summary": "Fixed"}
    for _ in range(2):
        requested = decide(world, current, reviewer_token, decision="needs_corrections",
                           reason="Still incomplete", correction_items=["Fix it"])
        assert requested["corrections"]["cycles"] <= 2
        correction_id = requested["corrections"]["open_ids"][0]
        world.call("POST", f"corrections/{correction_id}:resolve",
                   {**resolve, "expected_revision": 1}, token)
        current = world.call("GET", f"verifications/{body['id']}", token=token)
        assert current["status"] == "pending"
    decide(world, current, reviewer_token, decision="needs_corrections",
           reason="Still incomplete", correction_items=["Fix it"], status=409)


def test_tampered_or_deleted_evidence_is_redacted_and_downgraded(world):
    root = workspace(world)
    (root / "report.txt").write_text("trusted evidence")
    install(world, verifier=passing_verifier(root))
    agent, token, run, task = setup(world)
    reviewer = world.agent("Reviewer")
    reviewer_token = world.credential(reviewer)
    proof = artifact(world, token, run)
    post = world.post("Supporting finding")
    refs = [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]},
            {"kind": "board_post", "id": post["id"], "revision": post["revision"]}]

    body = submit(world, token, task, run, refs)
    assigned = assign(world, body, reviewer, token)
    checked = check(world, assigned, paths=["report.txt"])
    verified = decide(world, checked, reviewer_token, reason="Trusted check passed")
    assert verified["independently_verified"] is True

    # tampering with a referenced artifact bumps its revision
    store = world.app.state.store
    row = store.raw(proof["id"])
    with store.transaction():
        store.save("artifact", row["project"], row["owner"],
                   {**json.loads(row["body"]), "title": "Tampered artifact"})
    tampered = world.call("GET", f"verifications/{body['id']}", token=token)
    assert tampered["status"] == "verified"            # the stored decision is preserved
    assert tampered["effective_status"] == "pending"   # but it is no longer usable as proof
    assert tampered["independently_verified"] is False
    assert tampered["evidence_unavailable"] is True
    assert "summary" not in tampered["reported"] and tampered["test_evidence"] == []
    assert tampered["reported_completion"] is True

    # Source-gated permissions hide the verification entirely after deletion.
    # Revision drift above is still readable and downgraded; missing/hidden
    # sources must not leak through verification detail or list endpoints.
    world.call("DELETE", f"posts/{post['id']}", {"expected_revision": post["revision"]})
    world.call("GET", f"verifications/{body['id']}", token=token, status=404)
    assert world.call("GET", f"verifications?project_id={world.project}", token=token)["verifications"] == []


def test_private_evidence_and_cross_project_reads_are_contained(world):
    install(world)
    agent, token, run, task = setup(world)
    other = world.agent("Other agent")
    observer_token = world.credential(role="observer")
    proof = artifact(world, token, run)
    refs = [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]}]

    # evidence private to another agent is simply invisible
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (other["id"], proof["id"]))
    assert error_code(world, "POST", "verifications", payload(task, run, refs), token, 404) == "not_found"

    # private evidence belonging to the producer is still refused as verification evidence
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (agent["id"], proof["id"]))
    assert error_code(world, "POST", "verifications", payload(task, run, refs), token,
                      403) == "evidence_scope_violation"
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=NULL WHERE id=?", (proof["id"],))

    body = submit(world, token, task, run, refs)
    assert body["status"] == "pending"

    # an observer may read but not submit, assign, or decide
    assert world.call("GET", f"verifications/{body['id']}", token=observer_token)["status"] == "pending"
    assert error_code(world, "POST", "verifications", payload(task, run, refs), observer_token,
                      403) == "forbidden"
    assert error_code(world, "POST", f"verifications/{body['id']}:assign-reviewer",
                      {"expected_revision": body["revision"], "reviewer_agent_id": other["id"]},
                      observer_token, 403) == "reviewer_assignment_forbidden"

    # another project cannot read the verification or list this project
    second_project = uid("project")
    second_token = secrets.token_urlsafe(32)
    with store.transaction():
        store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (second_project, "Second project"))
        store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                         (uid("principal"), hash_token(second_token), "agent", second_project, None, 0, 0))
    world.call("GET", f"verifications/{body['id']}", token=second_token, status=404)
    assert world.call("GET", f"verifications?project_id={second_project}", token=second_token)["verifications"] == []
    world.call("GET", f"verifications?project_id={world.project}", token=second_token, status=404)


def test_commands_are_idempotent_and_revision_fenced(world):
    install(world)
    agent, token, run, task = setup(world)
    proof = artifact(world, token, run)
    refs = [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]}]
    reviewer = world.agent("Reviewer")
    reviewer_token = world.credential(reviewer)

    first = submit(world, token, task, run, refs, key="submission")
    assert submit(world, token, task, run, refs, key="submission") == first
    submit(world, token, task, run, [], key="submission", status=409)

    update = {"task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"],
              "completion_claimed": True, "summary": "Amended submission", "artifact_refs": refs}
    amended = world.call("POST", f"verifications/{first['id']}:update",
                         {**update, "expected_revision": first["revision"]}, token)
    assert amended["revision"] == first["revision"] + 1
    world.call("POST", f"verifications/{first['id']}:update",
               {**update, "expected_revision": first["revision"]}, token, status=412)

    assigned = assign(world, amended, reviewer, token)
    world.call("POST", f"verifications/{first['id']}:assign-reviewer",
               {"expected_revision": amended["revision"], "reviewer_agent_id": reviewer["id"]},
               token, status=412)
    decision = {"expected_revision": assigned["revision"], "decision": "rejected",
                "reason": "Reported tests are not objective proof"}
    rejected = world.call("POST", f"verifications/{first['id']}:decide", decision, reviewer_token, key="decision")
    assert rejected["status"] == "rejected"
    assert world.call("POST", f"verifications/{first['id']}:decide", decision, reviewer_token,
                      key="decision") == rejected
    world.call("POST", f"verifications/{first['id']}:decide", decision, reviewer_token, status=409)
    world.call("POST", f"verifications/{first['id']}:update",
               {**update, "expected_revision": rejected["revision"]}, token, status=409)
    # a rejected verification no longer blocks a fresh submission for the same execution
    replacement = submit(world, token, task, run, refs)
    assert replacement["status"] == "pending" and replacement["id"] != first["id"]


def test_install_accepts_explicit_host_collaborators(world):
    agent, token, run, task = setup(world)
    proof = artifact(world, token, run)
    second = create_app(world.app.state.store.directory, workers=False)
    install_on(second, explicit=True)
    try:
        with TestClient(second) as client:
            headers = {"Authorization": "Bearer " + token, "Idempotency-Key": uid("key")}
            response = client.post("/v1/verifications", json={
                "task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"],
                "completion_claimed": True, "summary": "Submitted through an explicitly wired app",
                "artifact_refs": [{"kind": "artifact", "id": proof["id"], "revision": proof["revision"]}]},
                headers=headers)
            assert response.status_code == 200, response.text
            created = response.json()["data"]
            assert created["status"] == "pending" and created["evidence_available"] is True

            detail = client.get(f"/v1/verifications/{created['id']}", headers=headers)
            assert detail.json()["data"]["task_id"] == task["id"]
            assert client.get(f"/v1/verifications?project_id={world.project}").status_code == 401
            assert client.get(f"/v1/verifications/{created['id']}",
                              headers={"Authorization": "Bearer wrong"}).status_code == 401
    finally:
        second.state.store.close()
