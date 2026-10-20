"""Evidence-fenced vendor session recovery.

Two deliberately separate layers so callers cannot mistake a live vendor session
for proof that prior work had its intended effects:

1. Local, safe workspace launch and readiness (:func:`open_workspace_client` and
   :func:`resume_session_workspace_bound`). These spawn or bind the vendor client
   rooted at the managed workspace -- never by changing the process-global working
   directory -- then judge only *server-reported* load evidence. A load response
   that omits workspace, identity, quiescence, or authorization evidence fails
   closed. Loading a session is a deterministic read-only probe; it is never
   treated as quiescence by itself, and a refusal never tears the vendor session
   down (``close_session`` is a consequential, possibly cross-controller action --
   the caller may still close its own client/transport).

2. Canonical recovery authorization (:func:`validate_private_state` and
   :func:`canonical_recovery_evidence`). These reconcile the private runtime state
   and its settled-turn checkpoint against fresh server reads of the task, run,
   claim, permit, and handoff records before any continuation turn is sent.

Private state files are bounded, regular, link-free files contained in the trusted
private state directory; a corrupt or unreadable intent/checkpoint file *holds*
recovery instead of failing open. Recovery intents are created by exclusive atomic
create (never overwriting a pending unknown intent) and cleared by
compare-then-delete, so concurrent helpers cannot delete another owner's record.
Verdicts carry stable generic reason codes (no paths, bodies, or credentials) so a
blocked-task surface can consume them safely.
"""

import hashlib
import importlib
import json
import os
import stat
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from .recovery import UnknownOutcome
from .storage import canonical, uid

TERMINAL_TASK_STATUSES = ("completed", "failed")
TERMINAL_RUN_STATUSES = ("completed", "failed", "cancelled")
UNCERTAIN_OUTCOMES = ("running", "unknown")
# Sealed local outcomes eligible for a governed continuation. "completed" is
# handled separately (same task/phase may never be replayed).
ELIGIBLE_OUTCOMES = ("paused", "cancelled", "failed", "idle")

MAX_PRIVATE_STATE_BYTES = 65536

# Stable, generic refusal codes. These are safe to surface to operators and to a
# blocked-task module: they never embed paths, prompt text, or credentials.
CORRUPT_STATE = "corrupt_state"
FOREIGN_STATE = "foreign_state"
OUTCOME_UNCERTAIN = "outcome_uncertain"
OUTCOME_MALFORMED = "outcome_malformed"
ALREADY_SETTLED = "already_settled"
IDENTITY_INCOMPLETE = "identity_incomplete"
SESSION_RUN_MISMATCH = "session_run_mismatch"
SESSION_TASK_MISMATCH = "session_task_mismatch"
SESSION_EXECUTION_MISMATCH = "session_execution_mismatch"
SESSION_ID_MISMATCH = "session_id_mismatch"
CHECKPOINT_MISSING = "checkpoint_missing"
CHECKPOINT_MISMATCH = "checkpoint_mismatch"
CHECKPOINT_EVIDENCE_UNAVAILABLE = "checkpoint_evidence_unavailable"
MODEL_MISMATCH = "model_mismatch"
WORKSPACE_NOT_PROVEN = "workspace_not_proven"
WORKSPACE_MISMATCH = "workspace_mismatch"
MODEL_TURN_UNCERTAIN = "model_turn_uncertain"
PENDING_EFFECTS_UNRESOLVED = "pending_effects_unresolved"
DELEGATED_SESSION = "delegated_session"
SESSION_ROLE_MISMATCH = "session_role_mismatch"
AUTH_CONTEXT_MISSING = "auth_context_missing"
GRANT_SCOPE_CHANGED = "grant_scope_changed"
TASK_UNAVAILABLE = "task_unavailable"
TASK_TERMINAL = "task_terminal"
TASK_EXECUTION_MISMATCH = "task_execution_mismatch"
PROJECT_MISMATCH = "project_mismatch"
AGENT_MISMATCH = "agent_mismatch"
RUN_MISMATCH = "run_mismatch"
RUN_BUSY = "run_busy"
RUN_NOT_LIVE = "run_not_live"
CLAIM_MISSING = "claim_missing"
CLAIM_EXPIRED = "claim_expired"
CLAIM_SCOPE_MISMATCH = "claim_scope_mismatch"
CLAIM_MALFORMED = "claim_malformed"
GRANT_REVOKED = "grant_revoked"
PERMIT_REFUSED = "permit_refused"
PERMIT_SOURCE_UNAVAILABLE = "permit_source_unavailable"
HANDOFF_STALE = "handoff_stale"
SDK_CONTRACT_CHANGED = "sdk_contract_changed"
SESSION_UNAVAILABLE = "session_unavailable"
STATE_BOUNDARY_VIOLATION = "state_boundary_violation"
PRIVATE_FILE_UNTRUSTED = "private_file_untrusted"
INTENT_PENDING = "intent_pending"
TASK_NOT_ACTIVE = "task_not_active"
CONTROL_PENDING = "control_pending"
RECOVERY_AUTHORITY_REFUSED = "recovery_authority_refused"

# Non-secret option fields hashed into the recovery fingerprint. Values are only
# ever stored as a digest, so paths and endpoints never appear in records.
FINGERPRINT_KEYS = (
    "model",
    "tokenizer_id",
    "timeout_seconds",
    "no_tools",
    "read_only_tools",
    "result_contract",
    "vendor_filesystem_sandbox",
    "require_os_isolation",
    "collect_usage",
    "executable",
)


class RecoveryRefused(UnknownOutcome):
    """Evidence-fenced refusal to continue a vendor session. Fail closed."""

    def __init__(self, reasons, message=None):
        self.reasons = tuple(reasons)
        detail = message or ("Vendor session recovery refused: " + ",".join(self.reasons))
        super().__init__(detail)


@dataclass(frozen=True)
class RecoveryVerdict:
    """Structured recovery judgment. ``evidence`` holds generic bookkeeping only."""

    allowed: bool
    reasons: tuple = ()
    evidence: Mapping = field(default_factory=dict)


def require_allowed(verdict):
    """Raise :class:`RecoveryRefused` unless every fence accepted."""
    if not verdict.allowed:
        raise RecoveryRefused(verdict.reasons)
    return verdict


def resolve_workspace(path):
    """Resolve the managed workspace strictly. The directory must already exist."""
    resolved = Path(path).resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("Vendor workspace is not a directory")
    return resolved


def _path_within(root, candidate):
    try:
        Path(candidate).resolve().relative_to(Path(root).resolve())
        return True
    except (OSError, ValueError):
        return False


def _verdict(reasons, evidence=None):
    reasons = tuple(dict.fromkeys(reasons))
    return RecoveryVerdict(allowed=not reasons, reasons=reasons, evidence=evidence or {})


def safe_fingerprint(options, keys=FINGERPRINT_KEYS):
    """Digest of non-secret runtime options. Output is a hash, never the values."""
    selected = {}
    for key in keys:
        value = (options or {}).get(key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            selected[key] = value
        else:
            selected[key] = str(type(value).__name__)
    return hashlib.sha256(canonical(selected).encode()).hexdigest()


# --------------------------------------------------------------- private files


def _is_link(path):
    if os.path.islink(path):
        return True
    try:
        attributes = os.lstat(path).st_file_attributes  # Windows reparse points
    except (AttributeError, OSError):
        return False
    return bool(attributes & 0x400)  # FILE_ATTRIBUTE_REPARSE_POINT


def trusted_private_file(path, *, base_dir):
    """Fail-closed guard: regular, link-free, bounded file inside ``base_dir``."""
    path = Path(path)
    base = Path(base_dir).resolve(strict=True)
    if path.parent != base and path.parent.resolve() != base:
        raise RecoveryRefused((STATE_BOUNDARY_VIOLATION,))
    if _is_link(path):
        raise RecoveryRefused((PRIVATE_FILE_UNTRUSTED,))
    try:
        info = os.lstat(path)
    except OSError:
        raise RecoveryRefused((PRIVATE_FILE_UNTRUSTED,)) from None
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_PRIVATE_STATE_BYTES:
        raise RecoveryRefused((PRIVATE_FILE_UNTRUSTED,))
    return path


def _read_private_json(path, *, base_dir, missing):
    """Read a bounded private JSON object. ``missing`` is the verdict for absence."""
    path = Path(path)
    if not path.exists():
        return None, missing
    try:
        trusted_private_file(path, base_dir=base_dir)
        data = json.loads(path.read_text(encoding="utf-8"))
    except RecoveryRefused as error:
        return None, error.reasons
    except (OSError, ValueError, UnicodeDecodeError):
        return None, (CORRUPT_STATE,)
    if not isinstance(data, dict):
        return None, (CORRUPT_STATE,)
    return data, None


def _write_private_json_exclusive(path, data, *, base_dir):
    """Atomic create-with-exclusion. Never overwrites an existing record."""
    path = Path(path)
    base = Path(base_dir).resolve(strict=True)
    if path.parent != base and path.parent.resolve() != base:
        raise RecoveryRefused((STATE_BOUNDARY_VIOLATION,))
    if path.exists() or _is_link(path):
        raise RecoveryRefused((INTENT_PENDING,))
    text = canonical(data)
    if len(text.encode()) > MAX_PRIVATE_STATE_BYTES:
        raise RecoveryRefused((CORRUPT_STATE,))
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        raise RecoveryRefused((INTENT_PENDING,)) from None
    except OSError:
        raise RecoveryRefused((PRIVATE_FILE_UNTRUSTED,)) from None
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(text)
        output.flush()
        os.fsync(output.fileno())


# --------------------------------------------------------------------- local state


def checkpoint_binding(state_file, state):
    """Read the settled-turn checkpoint file and bind it to the private state.

    The checkpoint is written only after an observed settled turn; here it is
    evidence for identity binding, never proof that the work took effect. A
    corrupt or unreadable checkpoint holds recovery instead of failing open.
    """
    if not state.get("checkpoint_id"):
        return _verdict(())
    if state_file is None:
        return _verdict((CHECKPOINT_MISSING,))
    state_file = Path(state_file)
    path = state_file.with_name(state_file.name + ".checkpoint.json")
    checkpoint, error = _read_private_json(path, base_dir=state_file.parent, missing=(CHECKPOINT_MISSING,))
    if error is not None:
        return _verdict(tuple(CHECKPOINT_MISSING if r == CORRUPT_STATE else r for r in error))
    matched = (
        checkpoint.get("id") == state.get("checkpoint_id")
        and checkpoint.get("run_id") == state.get("run_id")
        and checkpoint.get("task_id") == state.get("task_id")
        and checkpoint.get("execution_id") == state.get("execution_id")
        and checkpoint.get("quiescent") is True
    )
    if not matched:
        return _verdict((CHECKPOINT_MISMATCH,))
    return _verdict((), {"checkpoint_id": checkpoint["id"], "status": checkpoint.get("status")})


def validate_private_state(
    state,
    *,
    vendor,
    workspace,
    expect_run_id=None,
    expect_task_id=None,
    expect_execution_id=None,
    expect_phase_id=None,
    state_file=None,
    source_execution_id=None,
    for_continuation=False,
    allow_task_change=False,
):
    """Validate the private runtime state before any vendor load or continuation.

    An empty state means no prior session and is allowed (a fresh start, not a
    recovery). A non-empty state must carry complete run/task/execution identity
    and a sealed outcome: only ``paused``/``cancelled``/``failed``/``idle`` are
    eligible for continuation; ``running``/``unknown`` hold; anything else is
    malformed and refuses. ``completed`` work for the same task/phase may never
    be replayed. With ``for_continuation`` every expected id is mandatory. When
    the worker supplies ``source_execution_id`` (execution migration), a state
    bound to that source is accepted as migrated rather than a mismatch. With
    ``allow_task_change`` a sealed state bound to a *different* task is session
    reuse (recorded as ``task_changed`` evidence), while a same-task execution
    mismatch still requires the source reconciliation.
    """
    workspace = resolve_workspace(workspace)
    if for_continuation and (expect_run_id is None or expect_task_id is None or expect_execution_id is None):
        raise ValueError("Continuation requires explicit run/task/execution expectations")
    if not isinstance(state, dict):
        return _verdict((CORRUPT_STATE,))
    if not state:
        return _verdict((), {"fresh": True})
    reasons, evidence = [], {}
    if state.get("vendor") != vendor or state.get("workspace") != str(workspace):
        reasons.append(FOREIGN_STATE)
    outcome = state.get("outcome")
    sealed = outcome in ELIGIBLE_OUTCOMES or outcome == "completed"
    if outcome in UNCERTAIN_OUTCOMES:
        reasons.append(OUTCOME_UNCERTAIN)
    elif outcome == "completed":
        pass  # eligibility decided by the same-position fence below
    elif outcome not in ELIGIBLE_OUTCOMES:
        reasons.append(OUTCOME_MALFORMED)
    session_known = bool(state.get("session_id") or state.get("session_file"))
    if session_known and any(state.get(key) is None for key in ("run_id", "task_id", "execution_id")):
        reasons.append(IDENTITY_INCOMPLETE)
    if expect_run_id is not None and state.get("run_id") not in (None, expect_run_id):
        reasons.append(SESSION_RUN_MISMATCH)
    task_changed = (
        expect_task_id is not None
        and state.get("task_id") is not None
        and state.get("task_id") != expect_task_id
    )
    if task_changed:
        if allow_task_change and sealed:
            evidence["task_changed"] = True
        else:
            reasons.append(SESSION_TASK_MISMATCH)
    if expect_execution_id is not None and state.get("execution_id") not in (None, expect_execution_id):
        if source_execution_id is not None and state.get("execution_id") == source_execution_id:
            evidence["source_migrated"] = True  # reconciled via canonical handoff evidence
        elif not (task_changed and evidence.get("task_changed")):
            reasons.append(SESSION_EXECUTION_MISMATCH)
    if (
        outcome == "completed"
        and expect_task_id is not None
        and state.get("task_id") == expect_task_id
        and state.get("phase_id") == expect_phase_id
    ):
        # Settled vendor work for this exact position must be reconciled against
        # canonical task records before any continuation; never replayed.
        reasons.append(ALREADY_SETTLED)
    binding = checkpoint_binding(state_file, state)
    reasons.extend(binding.reasons)
    evidence.update(binding.evidence)
    evidence.update({"session_known": session_known, "outcome": outcome})
    return _verdict(reasons, evidence)


# --------------------------------------------------------------- canonical evidence


def _http_status(error):
    response = getattr(error, "response", None)
    return getattr(response, "status_code", None)


def canonical_recovery_evidence(
    client,
    *,
    project_id,
    agent_id,
    run_id,
    task_id,
    execution_id,
    scope_ids=(),
    require_claim=None,
    check_permit=True,
    now=None,
):
    """Fresh server reads reconciled with the intended continuation.

    ``client`` is the managed worker's API client (duck-typed: ``.task(id)`` and
    ``.request(method, path, body)``). Every record is read fresh and bound to the
    exact project/agent/run/task/execution; claims from another task or agent
    never count. When the task has scopes, the canonical ``permits:check``
    authorization is required: refusal refuses recovery, and a missing permit
    surface is a generic ``permit_source_unavailable``, never a silent pass.
    """
    now = time.time() if now is None else float(now)
    if not all((project_id, agent_id, run_id, task_id, execution_id)):
        return _verdict((IDENTITY_INCOMPLETE,))
    reasons, evidence = [], {}
    task = client.task(task_id)
    if not isinstance(task, dict) or task.get("id") != task_id:
        return _verdict((TASK_UNAVAILABLE,))
    evidence["task_revision"] = task.get("revision")
    if task.get("project_id") != project_id:
        reasons.append(PROJECT_MISMATCH)
    if task.get("status") in TERMINAL_TASK_STATUSES:
        reasons.append(TASK_TERMINAL)
    elif task.get("status") != "running":
        # Continuation requires the task's current active execution.
        reasons.append(TASK_NOT_ACTIVE)
    execution = task.get("execution") or {}
    if execution.get("id") != execution_id:
        reasons.append(TASK_EXECUTION_MISMATCH)
    if execution.get("run_id") != run_id:
        reasons.append(RUN_MISMATCH)
    checkpoint = execution.get("checkpoint")
    if isinstance(checkpoint, dict) and checkpoint.get("evidence_unavailable"):
        reasons.append(CHECKPOINT_EVIDENCE_UNAVAILABLE)
    run = client.request("GET", f"runs/{run_id}")
    if not isinstance(run, dict) or run.get("id") not in (None, run_id):
        reasons.append(RUN_MISMATCH)
    else:
        if run.get("project_id") != project_id:
            reasons.append(PROJECT_MISMATCH)
        if run.get("agent_id") != agent_id:
            reasons.append(AGENT_MISMATCH)
        if run.get("status") not in ("running", "idle"):
            # Exact live statuses only; a paused run is not live.
            reasons.append(RUN_NOT_LIVE)
        if run.get("task_id") not in (None, task_id):
            reasons.append(RUN_BUSY)
    scopes = sorted(scope_ids or task.get("scope_ids") or [])
    need_claim = bool(scopes) if require_claim is None else require_claim
    if need_claim:
        claims = client.request("GET", f"work-claims?project_id={project_id}") or []
        mine = [
            claim
            for claim in claims
            if claim.get("run_id") == run_id
            and claim.get("project_id") == project_id
            and claim.get("agent_id") == agent_id
            and claim.get("task_id") in (None, task_id)
        ]
        active = [
            claim
            for claim in mine
            if claim.get("state") == "active" and claim.get("mode") == "exclusive_owner"
        ]
        live = [claim for claim in active if claim.get("expires_at", 0) > now]
        if any(type(claim.get("ownership_epoch")) is not int for claim in mine):
            reasons.append(CLAIM_MALFORMED)
        if mine and not active:
            reasons.append(GRANT_REVOKED)
        elif active and not live:
            reasons.append(CLAIM_EXPIRED)
        elif not live:
            reasons.append(CLAIM_MISSING)
        else:
            owned = set()
            for claim in live:
                owned.update(claim.get("scope_ids") or [])
            if not set(scopes).issubset(owned):
                reasons.append(CLAIM_SCOPE_MISMATCH)
            evidence["claim_ids"] = sorted(claim.get("id") for claim in live)
        if check_permit and scopes:
            try:
                permit = client.request(
                    "POST", f"runs/{run_id}/permits:check", {"scope_ids": scopes, "paths": []}
                )
            except Exception as error:
                status = _http_status(error)
                if status == 403:
                    reasons.append(PERMIT_REFUSED)
                elif status in (404, 405, 501):
                    reasons.append(PERMIT_SOURCE_UNAVAILABLE)
                else:
                    raise
            else:
                if not isinstance(permit, dict) or permit.get("run_id") != run_id:
                    reasons.append(PERMIT_REFUSED)
                else:
                    evidence["permit_id"] = permit.get("permit_id")
                    evidence["control_revision"] = permit.get("control_revision")
    handoff = task.get("handoff")
    if isinstance(handoff, dict) and handoff:
        if not (
            handoff.get("state") == "accepted"
            and handoff.get("accepted_execution_id") == execution_id
            and handoff.get("from_execution_id") not in (None, execution_id)
        ):
            reasons.append(HANDOFF_STALE)
        else:
            evidence["handoff_id"] = handoff.get("id")
    return _verdict(reasons, evidence)


# ------------------------------------------------------------------ durable intent


def _intent_path(state_file):
    state_file = Path(state_file)
    return state_file.with_name(state_file.name + ".recovery-intent.json")


def record_recovery_intent(
    state_file,
    *,
    vendor,
    workspace,
    session_id=None,
    run_id=None,
    task_id=None,
    execution_id=None,
    model=None,
    options_fingerprint=None,
    reasons=(),
):
    """Persist a durable intent record *before* an uncertain load or continuation.

    The record is created by exclusive atomic create: an existing file -- valid,
    corrupt, or foreign -- refuses with ``intent_pending`` so a concurrent helper
    can never overwrite a pending unknown recovery. The record binds the full
    session identity and a hashed options fingerprint, never secret material.
    """
    intent = {
        "kind": "vendor_recovery_intent",
        "id": uid("recovery_intent"),
        "vendor": vendor,
        "workspace": str(Path(workspace).resolve()),
        "session_id": session_id,
        "run_id": run_id,
        "task_id": task_id,
        "execution_id": execution_id,
        "model": model,
        "options_fingerprint": options_fingerprint,
        "reasons": [str(reason)[:128] for reason in list(reasons)[:16]],
        "recorded_at": time.time(),
    }
    _write_private_json_exclusive(_intent_path(state_file), intent, base_dir=Path(state_file).parent)
    return intent


def pending_recovery_intent(state_file):
    """Return a recorded but unresolved recovery intent, or None when absent.

    Fail closed: a corrupt, unreadable, oversized, or link-bearing intent file
    raises :class:`RecoveryRefused` instead of looking like a clean slate.
    """
    path = _intent_path(state_file)
    data, error = _read_private_json(path, base_dir=Path(state_file).parent, missing=None)
    if error is not None:
        raise RecoveryRefused(error)
    if data is None:
        return None
    if data.get("kind") != "vendor_recovery_intent" or not isinstance(data.get("id"), str):
        raise RecoveryRefused((CORRUPT_STATE,))
    return data


def clear_recovery_intent(state_file, intent_id):
    """Compare-then-delete: only the recorded owner id can clear its intent.

    Returns False (never deletes) when the record is absent, unreadable, corrupt,
    or owned by a different intent id, so one helper cannot drop another owner's
    pending unknown recovery.
    """
    path = _intent_path(state_file)
    try:
        intent = pending_recovery_intent(state_file)
    except RecoveryRefused:
        return False
    if intent is None or intent.get("id") != intent_id:
        return False
    try:
        again = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if again != intent:
        return False
    try:
        path.unlink()
    except OSError:
        return False
    return True


# ----------------------------------------------------- vendor load evidence (Droid)


def _loaded_workspace_evidence(loaded):
    """All workspace evidence from a load result. Absent evidence is not a match."""
    found = []
    cwd = getattr(loaded, "cwd", None)
    if isinstance(cwd, str) and cwd:
        found.append(Path(cwd))
    extra = getattr(loaded, "model_extra", None)
    if isinstance(extra, Mapping):
        extra_cwd = extra.get("cwd")
        if isinstance(extra_cwd, str) and extra_cwd:
            found.append(Path(extra_cwd))
        worktree = extra.get("worktree")
        if isinstance(worktree, Mapping) and isinstance(worktree.get("path"), str):
            found.append(Path(worktree["path"]))
    return found


def _setting_enum_value(value):
    return getattr(value, "value", value)


def assess_load_result(
    loaded,
    *,
    session_id,
    workspace,
    expected_model=None,
    allow_unpinned_model=False,
    require_quiescent=True,
    allowed_autonomy=("low",),
):
    """Judge server-reported load evidence. Silence or omission fails closed.

    Workspace: every reported cwd/worktree path must resolve exactly to the
    managed workspace, and at least one must be reported. Identity: any session
    id echo must match. Authorization: settings must be present with a real model
    id (a pinned ``expected_model`` is required unless explicitly unpinned) and a
    known, non-elevated autonomy -- ``None`` means unknown, not allowed.
    """
    workspace = resolve_workspace(workspace)
    reasons, evidence = [], {}
    proven = _loaded_workspace_evidence(loaded)
    if not proven:
        reasons.append(WORKSPACE_NOT_PROVEN)
    else:
        resolved = []
        for candidate in proven:
            try:
                resolved.append(candidate.resolve(strict=True) if candidate.is_absolute() and candidate.is_dir()
                                else None)
            except (OSError, ValueError):
                resolved.append(None)
        if any(value is None or value != workspace for value in resolved):
            reasons.append(WORKSPACE_MISMATCH)
        else:
            evidence["workspace_proven"] = True
    extra = getattr(loaded, "model_extra", None)
    if isinstance(extra, Mapping):
        echo = extra.get("sessionId") or extra.get("session_id")
        if echo is not None and echo != session_id:
            reasons.append(SESSION_ID_MISMATCH)
    if require_quiescent:
        # A missing or true in-progress flag means a model turn may be live: hold.
        # Loading the session is a read; it never proves quiescence by itself.
        if getattr(loaded, "is_agent_loop_in_progress", None) is not False:
            reasons.append(MODEL_TURN_UNCERTAIN)
        for attribute in ("pending_permissions", "pending_ask_user_requests", "queued_messages"):
            if getattr(loaded, attribute, None):
                reasons.append(PENDING_EFFECTS_UNRESOLVED)
                break
    if getattr(loaded, "calling_session_id", None) is not None:
        reasons.append(DELEGATED_SESSION)
    if getattr(loaded, "mission", None) is not None:
        reasons.append(SESSION_ROLE_MISMATCH)
    settings = getattr(loaded, "settings", None)
    if settings is None:
        reasons.append(AUTH_CONTEXT_MISSING)
    else:
        model = getattr(settings, "model_id", None)
        evidence["model_id"] = model if isinstance(model, str) else None
        if not isinstance(model, str) or not model:
            reasons.append(AUTH_CONTEXT_MISSING)
        elif expected_model is None:
            if not allow_unpinned_model:
                reasons.append(AUTH_CONTEXT_MISSING)
        elif model != expected_model:
            reasons.append(MODEL_MISMATCH)
        autonomy = _setting_enum_value(getattr(settings, "autonomy_level", None))
        if autonomy is None:
            reasons.append(AUTH_CONTEXT_MISSING)
        elif autonomy not in allowed_autonomy:
            reasons.append(GRANT_SCOPE_CHANGED)
    return _verdict(reasons, evidence)


def _real_client_module():
    """Import seam for the installed SDK client bootstrap (tests may stub it)."""
    return importlib.import_module("droid_sdk._high_level._client")


async def open_workspace_client(runtime_config, *, workspace, api_key=None, observability=None, sdk=None):
    """Connect a vendor client with the process rooted at the managed workspace.

    The workspace is passed to the transport as its spawn directory; the global
    process cwd is never changed, so concurrent runtimes stay independent. Any
    SDK contract drift fails closed rather than guessing. ``sdk`` may supply an
    ``open_workspace_client`` connector (API-compatible test doubles); the
    installed ``droid_sdk`` (or None) always uses the real client bootstrap, and
    a foreign SDK without the connector refuses.
    """
    workspace = resolve_workspace(workspace)
    connector = getattr(sdk, "open_workspace_client", None) if sdk is not None else None
    if callable(connector):
        return await connector(runtime_config, workspace=workspace)
    if sdk is not None and getattr(sdk, "__name__", None) != "droid_sdk":
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    module = _real_client_module()
    opener = getattr(module, "open_droid_client", None)
    if not callable(opener):
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    if observability is None:
        from droid_sdk.observability import ObservabilityAdapter

        observability = ObservabilityAdapter(getattr(runtime_config, "observability", None))
    return await opener(runtime_config, api_key=api_key, cwd=workspace, observability=observability)


def _governed_session_config(sdk, *, disabled_tools, restrict_tools):
    """Rebuild the governed config so adoption/replacement never widens grants."""
    config_type = getattr(sdk, "SessionConfig", None)
    autonomy = getattr(getattr(sdk, "Autonomy", None), "LOW", None)
    if config_type is None or autonomy is None:
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    return config_type(
        autonomy=autonomy,
        auto_reject_permission_requests=True,
        disable_builtin_skills=True,
        disabled_tools=tuple(disabled_tools or ()),
        restrict_tools=tuple(restrict_tools) if restrict_tools is not None else None,
    )


def _bind_loaded_session(
    client,
    loaded,
    *,
    session_id,
    workspace,
    sdk=None,
    model=None,
    disabled_tools=("Execute", "Task"),
    restrict_tools=None,
    mcp_servers=None,
):
    """Adopt a loaded client into a high-level SDK Session, fail closed on drift.

    Mirrors the SDK's own successor-binding contract with the governed session
    config (auto-reject permissions, builtin skills disabled, tools restricted)
    so adoption or later replacement cannot re-enable skills or tools the load
    fences disabled; the dispatcher's default handlers cancel any permission or
    question request. Every private hook is probed first, so a changed SDK
    refuses instead of half-binding.
    """
    if sdk is None:
        import droid_sdk as sdk
    session_type = getattr(sdk, "Session", None)
    if session_type is None:
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    import sys

    session_module = sys.modules.get(session_type.__module__)
    open_state = getattr(getattr(session_module, "_State", None), "OPEN", None)
    settings_from_wire = getattr(session_module, "full_settings_from_wire", None)
    if open_state is None or not callable(settings_from_wire):
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    config = _governed_session_config(sdk, disabled_tools=disabled_tools, restrict_tools=restrict_tools)
    if mcp_servers is not None:
        from dataclasses import replace
        try:
            configs = tuple(sdk.StdioMcpServerConfig(
                name=item["name"], command=item["command"], args=item.get("args", ()),
                env=item.get("env", {})) for item in mcp_servers if item["type"] == "stdio")
            if len(configs) != len(mcp_servers):
                raise ValueError("Unsupported managed MCP transport")
            config = replace(config, mcp_servers=configs)
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise RecoveryRefused((SDK_CONTRACT_CHANGED,)) from error
    value = session_type(cwd=workspace, model=model, config=config)
    required = (
        "_client",
        "_id",
        "_settings",
        "_cwd",
        "_state",
        "_subscriptions",
        "_sdk_servers",
        "_dispatcher",
    )
    if any(not hasattr(value, name) for name in required):
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    if not callable(getattr(value, "_bind_metadata", None)):
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    dispatcher = value._dispatcher
    handlers = (getattr(dispatcher, "handle_permission", None), getattr(dispatcher, "handle_question", None))
    if any(not callable(handler) for handler in handlers):
        raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
    client.set_permission_handler(handlers[0])
    client.set_ask_user_handler(handlers[1])
    value._client = client
    value._id = session_id
    value._settings = settings_from_wire(loaded.settings)
    value._cwd = workspace
    value._state = open_state
    if mcp_servers is not None:
        if not hasattr(value, "_load_mcp_configs"):
            raise RecoveryRefused((SDK_CONTRACT_CHANGED,))
        value._load_mcp_configs = list(mcp_servers)
    value._bind_metadata()
    return value


async def _load_once(client, session_id, disabled_tools, mcp_servers=None):
    """Single deterministic read-only session load. Retries are the caller's
    explicit decision; this helper never loops."""
    return await client.load_session(
        session_id=session_id,
        disabled_tool_ids=list(disabled_tools) if disabled_tools else None,
        auto_reject_permission_requests=True,
        disable_builtin_skills=True,
        **({"mcp_servers": mcp_servers} if mcp_servers is not None else {}),
    )


async def resume_session_workspace_bound(
    client,
    *,
    session_id,
    workspace,
    expected_model=None,
    allow_unpinned_model=False,
    disabled_tools=("Execute", "Task"),
    restrict_tools=None,
    mcp_servers=None,
    require_quiescent=True,
    state_file=None,
    vendor="droid",
    intent_binding=None,
    resume_intent_id=None,
    options_fingerprint=None,
    sdk=None,
):
    """Load a saved session behind every evidence fence, or refuse.

    The load is a deterministic read; no prompt is sent and loading alone never
    proves quiescence or prior effects. A durable intent is recorded by exclusive
    create before the uncertain load, and cleared (compare-then-delete) only
    after every fence passes; an already-pending intent refuses unless the caller
    explicitly reconciles it via ``resume_intent_id`` with matching identity. On
    refusal the vendor session is NOT closed -- teardown could hit another
    controller's live session; the caller should close only its own client.
    """
    workspace = resolve_workspace(workspace)
    intent = None
    if state_file is not None:
        binding = dict(intent_binding or {})
        existing = pending_recovery_intent(state_file)  # raises on corrupt: hold
        if existing is not None:
            matches = (
                resume_intent_id is not None
                and existing.get("id") == resume_intent_id
                and existing.get("session_id") == session_id
                and existing.get("run_id") == binding.get("run_id")
                and existing.get("task_id") == binding.get("task_id")
                and existing.get("execution_id") == binding.get("execution_id")
                and existing.get("vendor") == vendor
                and existing.get("workspace") == str(workspace)
                and existing.get("model") == expected_model
                and existing.get("options_fingerprint") == options_fingerprint
            )
            if not matches:
                raise RecoveryRefused((INTENT_PENDING,))
            intent = existing
        else:
            intent = record_recovery_intent(
                state_file,
                vendor=vendor,
                workspace=workspace,
                session_id=session_id,
                run_id=binding.get("run_id"),
                task_id=binding.get("task_id"),
                execution_id=binding.get("execution_id"),
                model=expected_model,
                options_fingerprint=options_fingerprint,
            )
    try:
        try:
            loaded = await _load_once(client, session_id, disabled_tools, mcp_servers)
        except Exception as error:
            if type(error).__name__ == "SessionNotFoundError":
                raise RecoveryRefused((SESSION_UNAVAILABLE,)) from error
            raise  # unknown outcome: the durable intent stays for reconciliation
        if getattr(client, "session_id", None) != session_id:
            raise RecoveryRefused((SESSION_ID_MISMATCH,))
        verdict = assess_load_result(
            loaded,
            session_id=session_id,
            workspace=workspace,
            expected_model=expected_model,
            allow_unpinned_model=allow_unpinned_model,
            require_quiescent=require_quiescent,
        )
        if not verdict.allowed:
            raise RecoveryRefused(verdict.reasons)
        if restrict_tools is not None:
            try:
                await client.update_session_settings(restrict_tool_ids=list(restrict_tools))
            except Exception as error:
                raise RecoveryRefused((GRANT_SCOPE_CHANGED,)) from error
            # Verify the fence took effect with a fresh read; never trust the write.
            reloaded = await _load_once(client, session_id, disabled_tools, mcp_servers)
            applied = getattr(getattr(reloaded, "settings", None), "restrict_tool_ids", None)
            if (
                not isinstance(applied, (list, tuple))
                or any(not isinstance(tool, str) or not tool for tool in applied)
                or set(applied) != set(restrict_tools)
            ):
                raise RecoveryRefused((GRANT_SCOPE_CHANGED,))
            check = assess_load_result(
                reloaded,
                session_id=session_id,
                workspace=workspace,
                expected_model=expected_model,
                allow_unpinned_model=allow_unpinned_model,
                require_quiescent=require_quiescent,
            )
            if not check.allowed:
                raise RecoveryRefused(check.reasons)
            loaded = reloaded
        session = _bind_loaded_session(
            client,
            loaded,
            session_id=session_id,
            workspace=workspace,
            sdk=sdk,
            model=expected_model,
            disabled_tools=disabled_tools,
            restrict_tools=restrict_tools,
            mcp_servers=mcp_servers,
        )
    except RecoveryRefused:
        raise
    if state_file is not None and intent is not None:
        clear_recovery_intent(state_file, intent["id"])
    return session


# --------------------------------------------------------- vendor load evidence (OMP)


def validate_omp_resume(saved_state, vendor_state, *, sessions_dir):
    """Validate an oh-my-pi resume against its fresh ``get_state`` response.

    A matching, settled session id proves only that the vendor process holds that
    session quiescent -- it never proves the prior turn's canonical effects, which
    remain the job of :func:`canonical_recovery_evidence`.
    """
    reasons, evidence = [], {}
    if not isinstance(vendor_state, dict):
        return _verdict((SESSION_UNAVAILABLE,))
    saved_file = saved_state.get("session_file")
    if saved_file:
        if not _path_within(sessions_dir, saved_file):
            reasons.append(STATE_BOUNDARY_VIOLATION)
        elif not Path(saved_file).exists():
            reasons.append(SESSION_UNAVAILABLE)
    if saved_state.get("session_id") and vendor_state.get("sessionId") != saved_state.get("session_id"):
        reasons.append(SESSION_ID_MISMATCH)
    if vendor_state.get("isSettled") is not True:
        reasons.append(MODEL_TURN_UNCERTAIN)
    if vendor_state.get("isStreaming") not in (None, False):
        reasons.append(MODEL_TURN_UNCERTAIN)
    evidence["quiescence_observed"] = not reasons
    return _verdict(reasons, evidence)


__all__ = [name for name in dir() if name.isupper()] + [
    "RecoveryRefused",
    "RecoveryVerdict",
    "require_allowed",
    "resolve_workspace",
    "safe_fingerprint",
    "trusted_private_file",
    "checkpoint_binding",
    "validate_private_state",
    "canonical_recovery_evidence",
    "record_recovery_intent",
    "pending_recovery_intent",
    "clear_recovery_intent",
    "assess_load_result",
    "open_workspace_client",
    "resume_session_workspace_bound",
    "validate_omp_resume",
]
