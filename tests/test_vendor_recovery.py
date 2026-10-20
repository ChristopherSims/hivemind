import asyncio
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind import vendor_recovery as vr
from hivemind.vendor_runtimes import atomic_state


def workspace(tmp_path, name="workspace"):
    path = tmp_path / name
    path.mkdir()
    return path.resolve()


def state_file(tmp_path):
    private = tmp_path / "private"
    private.mkdir(exist_ok=True)
    return private / "runtime.json"


def base_state(ws):
    return {
        "vendor": "droid",
        "workspace": str(ws),
        "run_id": "run",
        "task_id": "task",
        "execution_id": "execution",
        "session_id": "session",
        "outcome": "paused",
        "phase_id": None,
        "checkpoint_id": None,
    }


# --------------------------------------------------------------- local state fences


def test_fresh_state_is_allowed_as_new_start(tmp_path):
    verdict = vr.validate_private_state({}, vendor="droid", workspace=workspace(tmp_path))
    assert verdict.allowed and verdict.evidence["fresh"] is True


def test_foreign_vendor_or_workspace_fails_closed(tmp_path):
    ws = workspace(tmp_path)
    other = workspace(tmp_path, "other")
    for state in ({**base_state(ws), "vendor": "oh-my-pi"}, {**base_state(ws), "workspace": str(other)}):
        verdict = vr.validate_private_state(state, vendor="droid", workspace=ws)
        assert not verdict.allowed and vr.FOREIGN_STATE in verdict.reasons


def test_uncertain_outcome_holds_and_never_replays(tmp_path):
    ws = workspace(tmp_path)
    for outcome in ("running", "unknown"):
        verdict = vr.validate_private_state(
            {**base_state(ws), "outcome": outcome}, vendor="droid", workspace=ws
        )
        assert not verdict.allowed and vr.OUTCOME_UNCERTAIN in verdict.reasons


def test_malformed_or_missing_outcome_refuses(tmp_path):
    ws = workspace(tmp_path)
    for outcome in ("bogus", None):
        state = base_state(ws)
        if outcome is None:
            del state["outcome"]
        else:
            state["outcome"] = outcome
        verdict = vr.validate_private_state(state, vendor="droid", workspace=ws)
        assert not verdict.allowed and vr.OUTCOME_MALFORMED in verdict.reasons


def test_sealed_outcomes_are_eligible_but_completed_same_position_is_not(tmp_path):
    ws = workspace(tmp_path)
    for outcome in ("paused", "cancelled", "failed", "idle"):
        verdict = vr.validate_private_state(
            {**base_state(ws), "outcome": outcome}, vendor="droid", workspace=ws
        )
        assert verdict.allowed
    verdict = vr.validate_private_state(
        {**base_state(ws), "outcome": "completed"},
        vendor="droid",
        workspace=ws,
        expect_task_id="task",
        expect_phase_id=None,
    )
    assert not verdict.allowed and vr.ALREADY_SETTLED in verdict.reasons


def test_known_session_with_missing_ids_is_incomplete(tmp_path):
    ws = workspace(tmp_path)
    state = base_state(ws)
    del state["run_id"]
    state["run_id"] = None
    verdict = vr.validate_private_state(state, vendor="droid", workspace=ws)
    assert vr.IDENTITY_INCOMPLETE in verdict.reasons


def test_run_task_execution_mismatches_are_distinct_reasons(tmp_path):
    ws = workspace(tmp_path)
    state = base_state(ws)
    cases = (
        {"expect_run_id": "other"},
        vr.SESSION_RUN_MISMATCH,
        {"expect_task_id": "other"},
        vr.SESSION_TASK_MISMATCH,
        {"expect_execution_id": "other"},
        vr.SESSION_EXECUTION_MISMATCH,
    )
    for index in range(0, len(cases), 2):
        kwargs, reason = cases[index], cases[index + 1]
        verdict = vr.validate_private_state(state, vendor="droid", workspace=ws, **kwargs)
        assert not verdict.allowed and reason in verdict.reasons


def test_source_execution_migration_reconciles_without_demanding_old_match(tmp_path):
    ws = workspace(tmp_path)
    state = base_state(ws)  # bound to the prior execution
    verdict = vr.validate_private_state(
        state,
        vendor="droid",
        workspace=ws,
        expect_execution_id="execution-new",
        source_execution_id="execution",
        for_continuation=True,
        expect_run_id="run",
        expect_task_id="task",
    )
    assert verdict.allowed and verdict.evidence["source_migrated"] is True
    refused = vr.validate_private_state(
        state,
        vendor="droid",
        workspace=ws,
        expect_execution_id="execution-new",
        source_execution_id="someone-else",
    )
    assert vr.SESSION_EXECUTION_MISMATCH in refused.reasons


def test_continuation_requires_explicit_expectations(tmp_path):
    ws = workspace(tmp_path)
    with pytest.raises(ValueError):
        vr.validate_private_state(base_state(ws), vendor="droid", workspace=ws, for_continuation=True)


def test_checkpoint_file_must_bind_to_state(tmp_path):
    ws = workspace(tmp_path)
    path = state_file(tmp_path)
    state = {**base_state(ws), "checkpoint_id": "checkpoint"}
    assert (
        vr.CHECKPOINT_MISSING
        in vr.validate_private_state(state, vendor="droid", workspace=ws, state_file=path).reasons
    )
    atomic_state(
        path.with_name(path.name + ".checkpoint.json"),
        {
            "id": "checkpoint",
            "run_id": "run",
            "task_id": "task",
            "execution_id": "execution",
            "quiescent": True,
            "status": "paused",
        },
    )
    verdict = vr.validate_private_state(state, vendor="droid", workspace=ws, state_file=path)
    assert verdict.allowed and verdict.evidence["checkpoint_id"] == "checkpoint"
    atomic_state(
        path.with_name(path.name + ".checkpoint.json"),
        {
            "id": "checkpoint",
            "run_id": "other",
            "task_id": "task",
            "execution_id": "execution",
            "quiescent": True,
        },
    )
    verdict = vr.validate_private_state(state, vendor="droid", workspace=ws, state_file=path)
    assert vr.CHECKPOINT_MISMATCH in verdict.reasons


def test_corrupt_or_oversized_checkpoint_holds_instead_of_failing_open(tmp_path):
    ws = workspace(tmp_path)
    path = state_file(tmp_path)
    state = {**base_state(ws), "checkpoint_id": "checkpoint"}
    checkpoint = path.with_name(path.name + ".checkpoint.json")
    checkpoint.write_text("{corrupt")
    verdict = vr.validate_private_state(state, vendor="droid", workspace=ws, state_file=path)
    assert vr.CHECKPOINT_MISSING in verdict.reasons
    checkpoint.write_text(" " * (vr.MAX_PRIVATE_STATE_BYTES + 1))
    verdict = vr.validate_private_state(state, vendor="droid", workspace=ws, state_file=path)
    assert vr.PRIVATE_FILE_UNTRUSTED in verdict.reasons
    checkpoint.unlink()
    target = tmp_path / "elsewhere.json"
    target.write_text(
        json.dumps(
            {
                "id": "checkpoint",
                "run_id": "run",
                "task_id": "task",
                "execution_id": "execution",
                "quiescent": True,
            }
        )
    )
    try:
        os.symlink(target, checkpoint)
    except OSError:
        pytest.skip("symlink not permitted")
    verdict = vr.validate_private_state(state, vendor="droid", workspace=ws, state_file=path)
    assert vr.PRIVATE_FILE_UNTRUSTED in verdict.reasons


def test_multiple_workspaces_validate_independently(tmp_path):
    first, second = workspace(tmp_path, "one"), workspace(tmp_path, "two")
    assert vr.validate_private_state(base_state(first), vendor="droid", workspace=first).allowed
    assert vr.validate_private_state(base_state(second), vendor="droid", workspace=second).allowed
    crossed = vr.validate_private_state(base_state(first), vendor="droid", workspace=second)
    assert vr.FOREIGN_STATE in crossed.reasons


def test_safe_fingerprint_is_stable_and_free_of_values():
    digest = vr.safe_fingerprint({"model": "secret-model-path", "timeout_seconds": 300})
    assert digest == vr.safe_fingerprint({"model": "secret-model-path", "timeout_seconds": 300})
    assert digest != vr.safe_fingerprint({"model": "other", "timeout_seconds": 300})
    assert "secret-model-path" not in digest


# ------------------------------------------------------------------ durable intent


def test_recovery_intent_lifecycle_and_owner_cas(tmp_path):
    path = state_file(tmp_path)
    ws = workspace(tmp_path)
    assert vr.pending_recovery_intent(path) is None
    intent = vr.record_recovery_intent(
        path,
        vendor="droid",
        workspace=ws,
        session_id="session",
        run_id="run",
        task_id="task",
        execution_id="execution",
        model="model-a",
        options_fingerprint="abc",
    )
    recorded = vr.pending_recovery_intent(path)
    assert recorded["id"] == intent["id"] and recorded["session_id"] == "session"
    assert recorded["model"] == "model-a"
    # A second helper must not overwrite a pending unknown recovery.
    with pytest.raises(vr.RecoveryRefused) as failure:
        vr.record_recovery_intent(path, vendor="droid", workspace=ws, session_id="other")
    assert failure.value.reasons == (vr.INTENT_PENDING,)
    assert vr.pending_recovery_intent(path)["id"] == intent["id"]
    assert not vr.clear_recovery_intent(path, "wrong-id")
    assert vr.pending_recovery_intent(path) is not None
    assert vr.clear_recovery_intent(path, intent["id"])
    assert vr.pending_recovery_intent(path) is None


def test_corrupt_intent_file_holds_fail_closed(tmp_path):
    path = state_file(tmp_path)
    intent_path = path.with_name(path.name + ".recovery-intent.json")
    intent_path.write_text("{corrupt")
    with pytest.raises(vr.RecoveryRefused) as failure:
        vr.pending_recovery_intent(path)
    assert vr.CORRUPT_STATE in failure.value.reasons
    assert not vr.clear_recovery_intent(path, "anything")  # cannot delete corrupt owner
    intent_path.write_text(json.dumps({"kind": "other"}))
    with pytest.raises(vr.RecoveryRefused):
        vr.pending_recovery_intent(path)


def test_intent_file_outside_private_dir_or_linked_is_refused(tmp_path):
    path = state_file(tmp_path)
    link = tmp_path / "private" / "link-intent.json"
    target = tmp_path / "target.json"
    target.write_text("{}")
    try:
        os.symlink(target, link)
    except OSError:
        pytest.skip("symlink not permitted")
    with pytest.raises(vr.RecoveryRefused) as failure:
        vr.trusted_private_file(link, base_dir=path.parent)
    assert failure.value.reasons == (vr.PRIVATE_FILE_UNTRUSTED,)
    with pytest.raises(vr.RecoveryRefused) as failure:
        vr.trusted_private_file(tmp_path / "outside.json", base_dir=path.parent)
    assert failure.value.reasons == (vr.STATE_BOUNDARY_VIOLATION,)


# -------------------------------------------------------------- canonical evidence


class HttpError(Exception):
    def __init__(self, status):
        self.response = SimpleNamespace(status_code=status)


class WorkerClient:
    def __init__(self, task, run, claims, permit=None, permit_error=None):
        self._task, self._run, self._claims = task, run, claims
        self._permit, self._permit_error = permit, permit_error
        self.reads = []

    def task(self, task_id):
        self.reads.append(("task", task_id))
        return self._task

    def request(self, method, path, body=None, key=None):
        self.reads.append((method, path))
        if path.startswith("work-claims"):
            return self._claims
        if path.startswith("runs/") and path.endswith("/permits:check"):
            if self._permit_error is not None:
                raise self._permit_error
            return self._permit
        if path.startswith("runs/"):
            return self._run
        raise AssertionError(path)


def canonical_setup():
    task = {
        "id": "task",
        "project_id": "project",
        "revision": 3,
        "status": "running",
        "scope_ids": ["scope"],
        "execution": {"id": "execution", "run_id": "run", "progress": 40},
    }
    run = {"id": "run", "project_id": "project", "agent_id": "agent", "task_id": "task", "status": "running"}
    claim = {
        "id": "claim",
        "run_id": "run",
        "project_id": "project",
        "agent_id": "agent",
        "task_id": "task",
        "state": "active",
        "mode": "exclusive_owner",
        "scope_ids": ["scope"],
        "expires_at": time.time() + 600,
        "ownership_epoch": 7,
    }
    permit = {"permit_id": "permit", "run_id": "run", "scope_ids": ["scope"], "control_revision": 2}
    return task, run, [claim], permit


BASE = dict(project_id="project", agent_id="agent", run_id="run", task_id="task", execution_id="execution")


def test_canonical_evidence_happy_path_fresh_reads_and_permit():
    task, run, claims, permit = canonical_setup()
    client = WorkerClient(task, run, claims, permit)
    verdict = vr.canonical_recovery_evidence(client, **BASE)
    assert verdict.allowed
    assert verdict.evidence["claim_ids"] == ["claim"]
    assert verdict.evidence["permit_id"] == "permit"
    assert ("task", "task") in client.reads
    assert ("POST", "runs/run/permits:check") in client.reads


def test_canonical_evidence_requires_complete_identity():
    for key in BASE:
        verdict = vr.canonical_recovery_evidence(WorkerClient(None, None, []), **{**BASE, key: None})
        assert verdict.reasons == (vr.IDENTITY_INCOMPLETE,)


def test_canonical_evidence_project_agent_and_run_fences():
    task, run, claims, permit = canonical_setup()
    wrong_project = vr.canonical_recovery_evidence(
        WorkerClient({**task, "project_id": "other"}, run, claims, permit), **BASE
    )
    assert vr.PROJECT_MISMATCH in wrong_project.reasons
    wrong_run_project = vr.canonical_recovery_evidence(
        WorkerClient(task, {**run, "project_id": "other"}, claims, permit), **BASE
    )
    assert vr.PROJECT_MISMATCH in wrong_run_project.reasons
    wrong_agent = vr.canonical_recovery_evidence(
        WorkerClient(task, {**run, "agent_id": "other"}, claims, permit), **BASE
    )
    assert vr.AGENT_MISMATCH in wrong_agent.reasons
    terminal = vr.canonical_recovery_evidence(
        WorkerClient(task, {**run, "status": "completed"}, claims, permit), **BASE
    )
    assert vr.RUN_NOT_LIVE in terminal.reasons
    busy = vr.canonical_recovery_evidence(
        WorkerClient(task, {**run, "task_id": "elsewhere"}, claims, permit), **BASE
    )
    assert vr.RUN_BUSY in busy.reasons


def test_canonical_evidence_refuses_terminal_mismatch_and_unavailable_task():
    task, run, claims, permit = canonical_setup()
    terminal = vr.canonical_recovery_evidence(
        WorkerClient({**task, "status": "completed"}, run, claims, permit), **BASE
    )
    assert vr.TASK_TERMINAL in terminal.reasons
    mismatch = vr.canonical_recovery_evidence(
        WorkerClient({**task, "execution": {"id": "other", "run_id": "run"}}, run, claims, permit), **BASE
    )
    assert vr.TASK_EXECUTION_MISMATCH in mismatch.reasons
    foreign_execution = vr.canonical_recovery_evidence(
        WorkerClient({**task, "execution": {"id": "execution", "run_id": "other"}}, run, claims, permit),
        **BASE,
    )
    assert vr.RUN_MISMATCH in foreign_execution.reasons
    unavailable = vr.canonical_recovery_evidence(WorkerClient(None, run, claims, permit), **BASE)
    assert vr.TASK_UNAVAILABLE in unavailable.reasons


def test_canonical_evidence_claim_fences_including_revoked_and_other_task():
    task, run, claims, permit = canonical_setup()
    missing = vr.canonical_recovery_evidence(WorkerClient(task, run, [], permit), **BASE)
    assert vr.CLAIM_MISSING in missing.reasons
    expired = vr.canonical_recovery_evidence(
        WorkerClient(task, run, [{**claims[0], "expires_at": time.time() - 1}], permit), **BASE
    )
    assert vr.CLAIM_EXPIRED in expired.reasons
    narrow = vr.canonical_recovery_evidence(
        WorkerClient(task, run, [{**claims[0], "scope_ids": ["else"]}], permit), **BASE
    )
    assert vr.CLAIM_SCOPE_MISMATCH in narrow.reasons
    revoked = vr.canonical_recovery_evidence(
        WorkerClient(task, run, [{**claims[0], "state": "released"}], permit), **BASE
    )
    assert vr.GRANT_REVOKED in revoked.reasons
    # A claim for the same run but another task must never count for this task.
    other_task = vr.canonical_recovery_evidence(
        WorkerClient(task, run, [{**claims[0], "task_id": "other-task"}], permit), **BASE
    )
    assert vr.CLAIM_MISSING in other_task.reasons
    other_agent = vr.canonical_recovery_evidence(
        WorkerClient(task, run, [{**claims[0], "agent_id": "other"}], permit), **BASE
    )
    assert vr.CLAIM_MISSING in other_agent.reasons
    no_epoch = vr.canonical_recovery_evidence(
        WorkerClient(
            task, run, [{key: value for key, value in claims[0].items() if key != "ownership_epoch"}], permit
        ),
        **BASE,
    )
    assert vr.CLAIM_MALFORMED in no_epoch.reasons


def test_canonical_evidence_permit_refusal_and_source_unavailable():
    task, run, claims, permit = canonical_setup()
    refused = vr.canonical_recovery_evidence(
        WorkerClient(task, run, claims, permit_error=HttpError(403)), **BASE
    )
    assert vr.PERMIT_REFUSED in refused.reasons
    for status in (404, 405, 501):
        verdict = vr.canonical_recovery_evidence(
            WorkerClient(task, run, claims, permit_error=HttpError(status)), **BASE
        )
        assert vr.PERMIT_SOURCE_UNAVAILABLE in verdict.reasons
    bad_echo = vr.canonical_recovery_evidence(
        WorkerClient(task, run, claims, permit={**permit, "run_id": "other"}), **BASE
    )
    assert vr.PERMIT_REFUSED in bad_echo.reasons


def test_canonical_evidence_handoff_and_checkpoint_fences():
    task, run, claims, permit = canonical_setup()
    stale = {**task, "handoff": {"state": "pending", "accepted_execution_id": "execution"}}
    verdict = vr.canonical_recovery_evidence(WorkerClient(stale, run, claims, permit), **BASE)
    assert vr.HANDOFF_STALE in verdict.reasons
    accepted = {
        **task,
        "handoff": {
            "id": "handoff",
            "state": "accepted",
            "accepted_execution_id": "execution",
            "from_execution_id": "prior",
        },
    }
    verdict = vr.canonical_recovery_evidence(WorkerClient(accepted, run, claims, permit), **BASE)
    assert verdict.allowed and verdict.evidence["handoff_id"] == "handoff"
    evidence_lost = json.loads(json.dumps(task))
    evidence_lost["execution"]["checkpoint"] = {"id": "c", "evidence_unavailable": True}
    verdict = vr.canonical_recovery_evidence(WorkerClient(evidence_lost, run, claims, permit), **BASE)
    assert vr.CHECKPOINT_EVIDENCE_UNAVAILABLE in verdict.reasons


# ------------------------------------------------------------- load result fences


def wire_settings(autonomy="low", restrict=None):
    from droid_sdk.schemas.client import AutonomyLevel, ReasoningEffort
    from droid_sdk.schemas.client import SessionSettings as WireSessionSettings

    return WireSessionSettings(
        modelId="model-a",
        reasoningEffort=ReasoningEffort.Low,
        autonomyLevel=None if autonomy is None else AutonomyLevel(autonomy),
        restrictToolIds=restrict,
    )


def load_result(ws, **overrides):
    settings = wire_settings()
    result = SimpleNamespace(
        cwd=str(ws),
        settings=settings,
        is_agent_loop_in_progress=False,
        pending_permissions=None,
        pending_ask_user_requests=None,
        queued_messages=None,
        calling_session_id=None,
        mission=None,
        model_extra={},
    )
    for key, value in overrides.items():
        setattr(result, key, value)
    return result


ASSESS = dict(session_id="s", expected_model="model-a")


def test_load_result_workspace_and_quiescence_acceptance(tmp_path):
    ws = workspace(tmp_path)
    verdict = vr.assess_load_result(load_result(ws), workspace=ws, **ASSESS)
    assert verdict.allowed and verdict.evidence["workspace_proven"] is True


def test_load_result_missing_workspace_evidence_fails_closed(tmp_path):
    ws = workspace(tmp_path)
    verdict = vr.assess_load_result(load_result(ws, cwd=None), workspace=ws, **ASSESS)
    assert vr.WORKSPACE_NOT_PROVEN in verdict.reasons
    extra = load_result(ws, cwd=None)
    extra.model_extra = {"worktree": {"path": str(ws)}}
    assert vr.assess_load_result(extra, workspace=ws, **ASSESS).allowed


def test_load_result_cwd_and_worktree_must_agree(tmp_path):
    ws, other = workspace(tmp_path), workspace(tmp_path, "other")
    both = load_result(ws)
    both.model_extra = {"worktree": {"path": str(other)}}
    verdict = vr.assess_load_result(both, workspace=ws, **ASSESS)
    assert vr.WORKSPACE_MISMATCH in verdict.reasons
    agree = load_result(ws)
    agree.model_extra = {"worktree": {"path": str(ws)}}
    assert vr.assess_load_result(agree, workspace=ws, **ASSESS).allowed


def test_load_result_workspace_mismatch(tmp_path):
    ws, other = workspace(tmp_path), workspace(tmp_path, "other")
    verdict = vr.assess_load_result(load_result(other), workspace=ws, **ASSESS)
    assert vr.WORKSPACE_MISMATCH in verdict.reasons


def test_load_result_session_id_echo_mismatch(tmp_path):
    ws = workspace(tmp_path)
    echoed = load_result(ws)
    echoed.model_extra = {"sessionId": "other"}
    verdict = vr.assess_load_result(echoed, workspace=ws, **ASSESS)
    assert vr.SESSION_ID_MISMATCH in verdict.reasons


def test_load_result_uncertain_model_turn_holds(tmp_path):
    ws = workspace(tmp_path)
    for flag in (True, None):
        verdict = vr.assess_load_result(
            load_result(ws, is_agent_loop_in_progress=flag), workspace=ws, **ASSESS
        )
        assert vr.MODEL_TURN_UNCERTAIN in verdict.reasons
    for attribute in ("pending_permissions", "pending_ask_user_requests", "queued_messages"):
        verdict = vr.assess_load_result(load_result(ws, **{attribute: [{"id": "x"}]}), workspace=ws, **ASSESS)
        assert vr.PENDING_EFFECTS_UNRESOLVED in verdict.reasons


def test_load_result_model_autonomy_and_role_fences(tmp_path):
    ws = workspace(tmp_path)
    wrong_model = vr.assess_load_result(
        load_result(ws), workspace=ws, session_id="s", expected_model="model-b"
    )
    assert vr.MODEL_MISMATCH in wrong_model.reasons
    no_settings = vr.assess_load_result(load_result(ws, settings=None), workspace=ws, **ASSESS)
    assert vr.AUTH_CONTEXT_MISSING in no_settings.reasons
    # An unpinned model is unknown authorization context unless explicitly allowed.
    unpinned = vr.assess_load_result(load_result(ws), workspace=ws, session_id="s")
    assert vr.AUTH_CONTEXT_MISSING in unpinned.reasons
    assert vr.assess_load_result(
        load_result(ws), workspace=ws, session_id="s", allow_unpinned_model=True
    ).allowed
    # Autonomy None is unknown, not allowed.
    no_autonomy = vr.assess_load_result(load_result(ws, settings=wire_settings(None)), workspace=ws, **ASSESS)
    assert vr.AUTH_CONTEXT_MISSING in no_autonomy.reasons
    elevated = vr.assess_load_result(load_result(ws, settings=wire_settings("high")), workspace=ws, **ASSESS)
    assert vr.GRANT_SCOPE_CHANGED in elevated.reasons
    delegated = vr.assess_load_result(load_result(ws, calling_session_id="parent"), workspace=ws, **ASSESS)
    assert vr.DELEGATED_SESSION in delegated.reasons
    orchestrator = vr.assess_load_result(
        load_result(ws, mission={"state": "running"}), workspace=ws, **ASSESS
    )
    assert vr.SESSION_ROLE_MISMATCH in orchestrator.reasons


# ------------------------------------------------------------- workspace-bound load


class FakeVendorClient:
    """API-compatible double for the connected SDK client (read-only load)."""

    def __init__(self, loaded_factory, fail_load=None):
        self._loaded_factory, self.fail_load = loaded_factory, fail_load
        self.loads, self.closed_sessions, self.settings_updates = [], 0, []
        self.session_id = None
        self.permission_handler = self.question_handler = None
        self._restrict = None

    async def load_session(self, **kwargs):
        self.loads.append(kwargs)
        if self.fail_load is not None:
            raise self.fail_load
        self.session_id = kwargs["session_id"]
        loaded = self._loaded_factory()
        if self._restrict is not None:
            loaded.settings = wire_settings(restrict=list(self._restrict))
        return loaded

    async def close_session(self, **kwargs):
        self.closed_sessions += 1

    async def close(self):
        pass

    async def update_session_settings(self, **kwargs):
        self.settings_updates.append(kwargs)
        self._restrict = kwargs.get("restrict_tool_ids")
        return SimpleNamespace()

    def set_permission_handler(self, handler):
        self.permission_handler = handler

    def set_ask_user_handler(self, handler):
        self.question_handler = handler

    def on_notification(self, callback):
        return lambda: None


def test_resume_rejects_extra_tools_in_verification_echo(tmp_path):
    ws = workspace(tmp_path)

    class WideningClient(FakeVendorClient):
        async def update_session_settings(self, **kwargs):
            await super().update_session_settings(**kwargs)
            self._restrict.append("Execute")

    client = WideningClient(lambda: load_result(ws))
    with pytest.raises(vr.RecoveryRefused) as raised:
        asyncio.run(
            vr.resume_session_workspace_bound(
                client, session_id="session", workspace=ws, allow_unpinned_model=True, restrict_tools=["Read"]
            )
        )
    assert vr.GRANT_SCOPE_CHANGED in raised.value.reasons


def test_reconciled_intent_rejects_changed_options_before_loading(tmp_path):
    ws = workspace(tmp_path)
    path = state_file(tmp_path)
    intent = vr.record_recovery_intent(
        path,
        vendor="droid",
        workspace=ws,
        session_id="session",
        run_id="run",
        task_id="task",
        execution_id="execution",
        model="original",
        options_fingerprint="original-options",
    )
    client = FakeVendorClient(lambda: load_result(ws))
    with pytest.raises(vr.RecoveryRefused) as raised:
        asyncio.run(
            vr.resume_session_workspace_bound(
                client,
                session_id="session",
                workspace=ws,
                expected_model="changed",
                state_file=path,
                resume_intent_id=intent["id"],
                options_fingerprint="changed-options",
                intent_binding={"run_id": "run", "task_id": "task", "execution_id": "execution"},
            )
        )
    assert vr.INTENT_PENDING in raised.value.reasons
    assert client.loads == []


def test_resume_binds_real_sdk_session_without_touching_global_cwd(tmp_path):
    import droid_sdk

    ws = workspace(tmp_path)
    client = FakeVendorClient(lambda: load_result(ws))
    before = os.getcwd()
    session = asyncio.run(
        vr.resume_session_workspace_bound(
            client, session_id="session", workspace=ws, expected_model="model-a", restrict_tools=["Read"]
        )
    )
    assert os.getcwd() == before
    assert isinstance(session, droid_sdk.Session)
    assert session.id == "session" and Path(session.cwd).resolve() == ws
    assert client.loads[0]["disabled_tool_ids"] == ["Execute", "Task"]
    assert client.loads[0]["auto_reject_permission_requests"] is True
    assert client.settings_updates == [{"restrict_tool_ids": ["Read"]}]
    # The governed config survives adoption: no skills, no permissions, tools held.
    config = session._config
    assert config.auto_reject_permission_requests is True
    assert config.disable_builtin_skills is True
    assert set(config.restrict_tools) == {"Read"}
    assert set(config.disabled_tools) == {"Execute", "Task"}
    asyncio.run(session.close())


def test_resume_refusal_never_tears_down_session_and_reports_generic_reasons(tmp_path):
    ws, other = workspace(tmp_path), workspace(tmp_path, "other")
    client = FakeVendorClient(lambda: load_result(other))
    with pytest.raises(vr.RecoveryRefused) as failure:
        asyncio.run(
            vr.resume_session_workspace_bound(client, session_id="s", workspace=ws, expected_model="model-a")
        )
    assert vr.WORKSPACE_MISMATCH in failure.value.reasons
    assert str(ws) not in str(failure.value) and str(other) not in str(failure.value)
    assert client.closed_sessions == 0  # close_session could hit another controller


def test_resume_client_session_identity_must_echo(tmp_path):
    ws = workspace(tmp_path)
    client = FakeVendorClient(lambda: load_result(ws))
    original = client.load_session

    async def wrong_id(**kwargs):
        loaded = await original(**kwargs)
        client.session_id = "someone-else"
        return loaded

    client.load_session = wrong_id
    with pytest.raises(vr.RecoveryRefused) as failure:
        asyncio.run(
            vr.resume_session_workspace_bound(client, session_id="s", workspace=ws, expected_model="model-a")
        )
    assert failure.value.reasons == (vr.SESSION_ID_MISMATCH,)


def test_resume_missing_session_is_unavailable_not_replayed(tmp_path):
    import droid_sdk

    ws = workspace(tmp_path)
    gone = droid_sdk.SessionNotFoundError("session")
    client = FakeVendorClient(None, fail_load=gone)
    with pytest.raises(vr.RecoveryRefused) as failure:
        asyncio.run(
            vr.resume_session_workspace_bound(client, session_id="s", workspace=ws, expected_model="model-a")
        )
    assert failure.value.reasons == (vr.SESSION_UNAVAILABLE,)


def test_resume_restrict_tools_is_verified_by_fresh_read_not_trusted(tmp_path):
    ws = workspace(tmp_path)

    async def run_with(client):
        return await vr.resume_session_workspace_bound(
            client, session_id="s", workspace=ws, expected_model="model-a", restrict_tools=["Read", "Grep"]
        )

    good = FakeVendorClient(lambda: load_result(ws))
    asyncio.run(run_with(good))
    assert len(good.loads) == 2  # verification reload after the settings write

    class SilentDeny(FakeVendorClient):
        async def update_session_settings(self, **kwargs):
            self.settings_updates.append(kwargs)  # accepted but never applied
            return SimpleNamespace()

    false_fence = SilentDeny(lambda: load_result(ws))
    with pytest.raises(vr.RecoveryRefused) as failure:
        asyncio.run(run_with(false_fence))
    assert failure.value.reasons == (vr.GRANT_SCOPE_CHANGED,)


def test_resume_records_durable_intent_and_clears_only_on_success(tmp_path):
    ws, other = workspace(tmp_path), workspace(tmp_path, "other")
    path = state_file(tmp_path)
    binding = {"run_id": "run", "task_id": "task", "execution_id": "execution"}
    refused = FakeVendorClient(lambda: load_result(other))
    with pytest.raises(vr.RecoveryRefused):
        asyncio.run(
            vr.resume_session_workspace_bound(
                refused,
                session_id="s",
                workspace=ws,
                expected_model="model-a",
                state_file=path,
                intent_binding=binding,
            )
        )
    intent = vr.pending_recovery_intent(path)
    assert intent["run_id"] == "run" and intent["session_id"] == "s"  # survives
    # A retry without explicit reconciliation of the pending intent refuses.
    with pytest.raises(vr.RecoveryRefused) as failure:
        asyncio.run(
            vr.resume_session_workspace_bound(
                FakeVendorClient(lambda: load_result(ws)),
                session_id="s",
                workspace=ws,
                expected_model="model-a",
                state_file=path,
                intent_binding=binding,
            )
        )
    assert failure.value.reasons == (vr.INTENT_PENDING,)
    # Explicit reconciled retry with matching identity proceeds and clears.
    accepted = FakeVendorClient(lambda: load_result(ws))
    asyncio.run(
        vr.resume_session_workspace_bound(
            accepted,
            session_id="s",
            workspace=ws,
            expected_model="model-a",
            state_file=path,
            intent_binding=binding,
            resume_intent_id=intent["id"],
        )
    )
    assert vr.pending_recovery_intent(path) is None


def test_resume_fails_closed_when_sdk_contract_changes(tmp_path, monkeypatch):
    import droid_sdk
    from droid_sdk._high_level import session as session_module

    ws = workspace(tmp_path)
    client = FakeVendorClient(lambda: load_result(ws))
    monkeypatch.delattr(session_module, "full_settings_from_wire")
    with pytest.raises(vr.RecoveryRefused) as failure:
        asyncio.run(
            vr.resume_session_workspace_bound(
                client, session_id="s", workspace=ws, expected_model="model-a", sdk=droid_sdk
            )
        )
    assert failure.value.reasons == (vr.SDK_CONTRACT_CHANGED,)
    assert client.closed_sessions == 0


def test_resume_concurrent_workspaces_stay_isolated(tmp_path):
    first, second = workspace(tmp_path, "one"), workspace(tmp_path, "two")
    clients = {
        str(first): FakeVendorClient(lambda: load_result(first)),
        str(second): FakeVendorClient(lambda: load_result(second)),
    }

    async def recover(ws):
        return await vr.resume_session_workspace_bound(
            clients[str(ws)], session_id=f"session-{ws.name}", workspace=ws, expected_model="model-a"
        )

    async def both():
        return await asyncio.gather(recover(first), recover(second))

    before = os.getcwd()
    one, two = asyncio.run(both())
    assert os.getcwd() == before
    assert Path(one.cwd).resolve() == first and Path(two.cwd).resolve() == second
    assert one.id != two.id
    asyncio.run(one.close())
    asyncio.run(two.close())


# ------------------------------------------------------------------- OMP validation


def test_omp_resume_requires_matching_settled_session_inside_state_dir(tmp_path):
    sessions = tmp_path / "private" / "sessions"
    sessions.mkdir(parents=True)
    session_file = sessions / "session.jsonl"
    session_file.write_text("{}")
    saved = {"session_id": "omp-session", "session_file": str(session_file)}
    good = {"sessionId": "omp-session", "isSettled": True, "isStreaming": False}
    verdict = vr.validate_omp_resume(saved, good, sessions_dir=sessions)
    assert verdict.allowed and verdict.evidence["quiescence_observed"] is True
    mismatch = vr.validate_omp_resume(saved, {**good, "sessionId": "other"}, sessions_dir=sessions)
    assert vr.SESSION_ID_MISMATCH in mismatch.reasons
    unsettled = vr.validate_omp_resume(saved, {**good, "isSettled": None}, sessions_dir=sessions)
    assert vr.MODEL_TURN_UNCERTAIN in unsettled.reasons
    streaming = vr.validate_omp_resume(saved, {**good, "isStreaming": True}, sessions_dir=sessions)
    assert vr.MODEL_TURN_UNCERTAIN in streaming.reasons
    escaped = vr.validate_omp_resume(
        {**saved, "session_file": str(tmp_path / "else")}, good, sessions_dir=sessions
    )
    assert vr.STATE_BOUNDARY_VIOLATION in escaped.reasons
    missing = vr.validate_omp_resume(
        {**saved, "session_file": str(sessions / "gone")}, good, sessions_dir=sessions
    )
    assert vr.SESSION_UNAVAILABLE in missing.reasons


def test_require_allowed_raises_with_reasons():
    verdict = vr.RecoveryVerdict(allowed=False, reasons=(vr.MODEL_TURN_UNCERTAIN,))
    with pytest.raises(vr.RecoveryRefused) as failure:
        vr.require_allowed(verdict)
    assert failure.value.reasons == (vr.MODEL_TURN_UNCERTAIN,)
    assert vr.require_allowed(vr.RecoveryVerdict(allowed=True)).allowed
