"""Opt-in runtime lifecycle reconciliation and cooperative safe-pause/cancel boundaries.

This module is additive. It composes the existing capture spool
(:class:`hivemind.sdk.Capture`) and safe-boundary adapter
(:class:`hivemind.sdk.ControlledAdapter`) rather than modifying them, and it never
signals, kills, or infers state from a user process or a vendor runtime. Runtime
facts arrive only through an injected adapter implementing ``inspect``, ``pause``,
``cancel``, and ``resume``; adapters that cannot establish a stop or a checkpoint
produce ``outcome_unknown``, never a fabricated ``applied``.

Two independent grants are required before this module writes a canonical terminal
run transition:

* ``capture_authority`` on the calling principal (the actor grant the service
  already enforces for ``confirmed`` observations), and
* ``terminal_grant`` on the reconciler (the operator opt-in).

Absent either grant, an observed absence is returned as evidence only and is
persisted in a local audit record. Unobserved workspace differences are reported
with ``unattributed`` or ``reported`` attribution and content hashes only; raw file
content, transcripts, and captured reasoning are never read or stored.

Stale presence is never treated as stopped: an old ``last_seen_at`` is recorded as
*derived* evidence and is never an input to a classification or a transition.
"""

import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import parse_qs

import httpx
from fastapi import Depends
from pydantic import Field, ValidationError

from . import models as m
from .sdk import Capture, ControlledAdapter, excluded
from .service import TERMINAL
from .storage import Problem, canonical, now, uid

__all__ = [
    "ABANDONED", "LIVE", "STALE", "TERMINAL_RUN", "CONFIRMED", "REPORTED", "UNATTRIBUTED",
    "RUNTIME_ACTIONS", "TERMINAL_STATUSES",
    "AdapterObservation", "ControlAttempt", "DriftFinding", "ReconciliationReport", "RunDecision",
    "ScanLimits", "FileAdapter", "LifecycleBoundary", "RuntimeReconciler", "ServiceTransport",
    "ReconcileRequest", "install", "scan_workspace", "diff_snapshots", "secret_path",
    "normalize_observation", "normalize_attempt",
]

LIVE = "live"
STALE = "stale"
ABANDONED = "abandoned"
TERMINAL_RUN = "terminal"
UNKNOWN = "unknown"

CONFIRMED = "confirmed"
REPORTED = "reported"
UNATTRIBUTED = "unattributed"

RUNTIME_ACTIONS = ("pause", "cancel", "resume")
# The service requires this observed status before it accepts an applied result.
OBSERVED_STATUS = {"pause": "paused", "cancel": "cancelled", "resume": "running"}
TERMINAL_STATUSES = frozenset(TERMINAL)
# Completion is never inferred from an absence; only these are selectable.
ABANDONED_STATUSES = ("failed", "cancelled")

# Presence threshold defaults mirror the service's derived presence buckets. Presence
# describes evidence only and never drives a lifecycle decision.
PRESENCE_OFFLINE_SECONDS = 30.0

SECRET_NAMES = frozenset({
    ".netrc", ".npmrc", ".pypirc", ".git-credentials", "id_rsa", "id_ed25519", "credentials",
    "secrets.json",
})
SECRET_SUFFIXES = (".pem", ".key", ".p12", ".pfx", ".jks", ".keystore", ".ppk")


def _state_key(value):
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(value))[:128] or "runtime"


def secret_path(relative, extra=()):
    """Secret/ignored path gate. ``sdk.excluded`` is authoritative for standard trees."""
    path = Path(relative)
    if excluded(path):
        return True
    for part in path.parts:
        if part.startswith(".env") or part in SECRET_NAMES or part.lower().endswith(SECRET_SUFFIXES):
            return True
    as_posix = path.as_posix()
    return any(Path(as_posix).match(pattern) for pattern in extra)


@dataclass(frozen=True)
class ScanLimits:
    """Hard bounds for one workspace scan; every bound is enforced independently."""

    max_files: int = 2000
    max_depth: int = 16
    max_file_bytes: int = 262144
    max_total_bytes: int = 33554432
    deadline_seconds: float = 5.0
    follow_symlinks: bool = False


@dataclass(frozen=True)
class AdapterObservation:
    """A runtime fact. ``present is None`` means unknown, which is not absence."""

    present: bool | None = None
    status: str | None = None
    checkpoint_id: str | None = None
    observed_at: float | None = None
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ControlAttempt:
    """A cooperative control attempt. ``applied is None`` means unverifiable."""

    action: str = ""
    applied: bool | None = None
    observed_status: str | None = None
    checkpoint_id: str | None = None
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class DriftFinding:
    """One unobserved workspace difference, expressed as hashes and sizes only."""

    path: str
    change: str
    before: dict | None = None
    after: dict | None = None
    hashed: bool = True


@dataclass
class RunDecision:
    """The reconciled view of a single run plus the action that was taken."""

    run_id: str
    status: str
    classification: str
    reason: str = ""
    action: str = "none"
    outcome: str = "reported"
    terminal_status: str | None = None
    agent_id: str | None = None
    derived_presence: str | None = None
    observation: dict | None = None
    evidence: dict = field(default_factory=dict)


@dataclass
class ReconciliationReport:
    """Serializable result of one reconcile pass; also persisted as the local audit."""

    enabled: bool
    observed_at: float
    workspace: str | None = None
    adapter: str | None = None
    decisions: list = field(default_factory=list)
    drift: list = field(default_factory=list)
    drift_report: dict | None = None
    submitted: dict | None = None
    skipped: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


def normalize_observation(value):
    """Coerce an adapter return value into an observation without inventing facts."""
    if value is None:
        return None
    if isinstance(value, AdapterObservation):
        return value
    if isinstance(value, bool):
        return AdapterObservation(present=value)
    if isinstance(value, dict):
        present = value.get("present")
        status = value.get("status")
        observed_at = value.get("updated_at", value.get("observed_at"))
        return AdapterObservation(
            present=present if isinstance(present, bool) else None,
            status=status if isinstance(status, str) else None,
            checkpoint_id=value.get("checkpoint_id") if isinstance(value.get("checkpoint_id"), str) else None,
            observed_at=float(observed_at) if isinstance(observed_at, (int, float)) else None,
            detail={"state": "reported"},
        )
    return AdapterObservation(detail={"state": "unsupported_observation",
                                      "value_type": type(value).__name__})


def normalize_attempt(value, action):
    """Coerce an adapter control return value into an attempt without inventing facts."""
    if isinstance(value, ControlAttempt):
        return ControlAttempt(action=action, applied=value.applied, observed_status=value.observed_status,
                              checkpoint_id=value.checkpoint_id, detail=value.detail)
    if isinstance(value, bool):
        return ControlAttempt(action=action, applied=value, detail={"state": "reported"})
    if isinstance(value, dict):
        applied = value.get("applied")
        observed = value.get("status", value.get("observed_status"))
        return ControlAttempt(
            action=action,
            applied=applied if isinstance(applied, bool) else None,
            observed_status=observed if isinstance(observed, str) else None,
            checkpoint_id=value.get("checkpoint_id") if isinstance(value.get("checkpoint_id"), str) else None,
            detail={"state": "reported"},
        )
    if value is None:
        return ControlAttempt(action=action, detail={"state": "no_result"})
    return ControlAttempt(action=action, detail={"state": "unsupported_result",
                                                 "value_type": type(value).__name__})


def sha256_file(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def scan_workspace(root, limits=None, exclude=(), clock=time.monotonic):
    """Bounded, workspace-contained scan returning content hashes and sizes only.

    Nothing outside ``root`` is read, symlinks are not followed by default, secret
    and dependency trees are excluded via :func:`hivemind.sdk.excluded`, and every
    limit in :class:`ScanLimits` is enforced. Skipped paths are reported by reason.
    """
    limits = limits or ScanLimits()
    root = Path(root).resolve()
    if not root.is_dir():
        raise ValueError("Workspace root is not a directory")
    deadline = clock() + limits.deadline_seconds
    files, skipped, visited = {}, [], set()
    budget, truncated = limits.max_total_bytes, False
    stack = [(root, 0)]
    while stack and not truncated:
        directory, depth = stack.pop()
        relative_dir = directory.relative_to(root).as_posix()
        if depth >= limits.max_depth:
            skipped.append({"path": relative_dir, "reason": "max_depth"})
            continue
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError:
            skipped.append({"path": relative_dir, "reason": "unreadable"})
            continue
        for entry in entries:
            if clock() > deadline:
                truncated = True
                skipped.append({"path": relative_dir, "reason": "deadline"})
                break
            target = Path(entry.path)
            relative = target.relative_to(root)
            name = relative.as_posix()
            if secret_path(relative, exclude):
                skipped.append({"path": name, "reason": "excluded"})
                continue
            try:
                symlink = entry.is_symlink()
                if symlink and not limits.follow_symlinks:
                    skipped.append({"path": name, "reason": "symlink"})
                    continue
                resolved = target.resolve() if symlink else target
                if not resolved.is_relative_to(root):
                    skipped.append({"path": name, "reason": "outside_workspace"})
                    continue
                if entry.is_dir(follow_symlinks=limits.follow_symlinks):
                    if resolved in visited:
                        skipped.append({"path": name, "reason": "already_visited"})
                        continue
                    visited.add(resolved)
                    stack.append((target, depth + 1))
                    continue
                if not entry.is_file(follow_symlinks=limits.follow_symlinks):
                    skipped.append({"path": name, "reason": "not_a_regular_file"})
                    continue
                if len(files) >= limits.max_files:
                    truncated = True
                    skipped.append({"path": name, "reason": "max_files"})
                    break
                size = entry.stat(follow_symlinks=limits.follow_symlinks).st_size
                if size > limits.max_file_bytes or size > budget:
                    files[name] = {"size": size, "sha256": None, "hashed": False}
                    continue
                files[name] = {"size": size, "sha256": sha256_file(target), "hashed": True}
                budget -= size
            except OSError:
                skipped.append({"path": name, "reason": "unreadable"})
    return {"root": str(root), "files": files, "skipped": skipped, "truncated": truncated}


def diff_snapshots(before, after, max_findings=500):
    """Compare two scans. Oversized, unhashed files are compared by size only."""
    before_files = (before or {}).get("files") or {}
    after_files = (after or {}).get("files") or {}
    findings = []
    for path in sorted(set(before_files) | set(after_files)):
        if len(findings) >= max_findings:
            break
        left, right = before_files.get(path), after_files.get(path)
        if left is None:
            change = "created"
        elif right is None:
            change = "deleted"
        elif left.get("sha256") and right.get("sha256"):
            if left["sha256"] == right["sha256"] and left.get("size") == right.get("size"):
                continue
            change = "modified"
        elif left.get("size") != right.get("size"):
            change = "modified"
        else:
            # Both sides are unhashed and equal in size: the difference is unobserved.
            continue
        findings.append(DriftFinding(path=path, change=change, before=left, after=right,
                                     hashed=bool((right or left).get("sha256"))))
    return findings


class FileAdapter:
    """Observation channel over a JSON state file written by a trusted local runtime wrapper.

    The wrapper writes one object, for example::

        {"run_id": "run_...", "present": true, "status": "running", "updated_at": 1770000000.0}

    Absence is only ever taken from an explicit ``present: false`` (or ``status:
    "stopped"``) observation that names the same run and carries a timestamp.
    A missing, unreadable, or run-mismatched file means *unknown*, never *stopped*.
    Cooperative controls are requested by writing ``<state>.control.json`` and are
    answered by a bounded wait for ``<state>.control-ack.json``. No process is
    signalled, no vendor runtime is inspected, and no inference is performed.
    """

    name = "file"
    # Presence observation does not establish authorship of workspace changes.
    trusted_attribution = False

    def __init__(self, state_file, *, name=None, authoritative_absence=True, freshness_seconds=30.0,
                 timeout=2.0, clock=time.time, poll_interval=0.02):
        self.path = Path(state_file)
        self.name = name or self.name
        self.authoritative_absence = authoritative_absence
        self.freshness_seconds = freshness_seconds
        self.timeout = timeout
        self.clock = clock
        self.poll_interval = poll_interval
        self.control_path = self.path.with_name(self.path.name + ".control.json")
        self.ack_path = self.path.with_name(self.path.name + ".control-ack.json")

    def _read(self, path):
        try:
            return json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def inspect(self, run=None):
        state = self._read(self.path)
        if not isinstance(state, dict):
            return AdapterObservation(detail={"state": "unavailable"})
        run_id = state.get("run_id")
        target = (run or {}).get("id")
        if isinstance(run_id, str) and target and run_id != target:
            return AdapterObservation(detail={"state": "run_mismatch"})
        updated = state.get("updated_at")
        observed_at = float(updated) if isinstance(updated, (int, float)) else None
        present = state.get("present")
        if not isinstance(present, bool) and state.get("status") == "stopped":
            present = False
        status = state.get("status") if isinstance(state.get("status"), str) else None
        detail = {"state": "reported"}
        if present is True and observed_at is not None and self.clock() - observed_at > self.freshness_seconds:
            # A stale "running" claim is unknown, not absence.
            return AdapterObservation(status=status, observed_at=observed_at,
                                      detail={**detail, "stale": True,
                                              "freshness_seconds": self.freshness_seconds})
        return AdapterObservation(present=present if isinstance(present, bool) else None, status=status,
                                  checkpoint_id=state.get("checkpoint_id")
                                  if isinstance(state.get("checkpoint_id"), str) else None,
                                  observed_at=observed_at, detail=detail)

    def _control(self, action, run=None):
        request_id = uid("control")
        target = (run or {}).get("id")
        try:
            self.control_path.write_text(canonical({
                "request_id": request_id, "action": action, "run_id": target,
                "requested_at": self.clock()}), encoding="utf-8")
            deadline = self.clock() + self.timeout
            while self.clock() < deadline:
                ack = self._read(self.ack_path)
                if isinstance(ack, dict) and ack.get("request_id") == request_id:
                    try:
                        self.ack_path.unlink(missing_ok=True)
                    except OSError:
                        pass  # A trusted writer may still hold the acknowledged file open on Windows.
                    applied = ack.get("applied")
                    return ControlAttempt(
                        action=action, applied=applied if isinstance(applied, bool) else None,
                        observed_status=ack.get("status") if isinstance(ack.get("status"), str) else None,
                        checkpoint_id=ack.get("checkpoint_id")
                        if isinstance(ack.get("checkpoint_id"), str) else None,
                        detail={"state": "acknowledged"})
                time.sleep(self.poll_interval)
            return ControlAttempt(action=action, detail={"state": "no_acknowledgement",
                                                         "timeout_seconds": self.timeout})
        finally:
            try:
                self.control_path.unlink(missing_ok=True)
            except OSError:
                pass  # Cleanup failure does not invalidate an already observed checkpoint acknowledgement.

    def pause(self, run=None):
        return self._control("pause", run)

    def cancel(self, run=None):
        return self._control("cancel", run)

    def resume(self, run=None):
        return self._control("resume", run)


class LifecycleBoundary(ControlledAdapter):
    """``ControlledAdapter`` composition that never reports an unverified stop.

    The legacy boundary derives ``applied`` for pause/cancel from the mere delivery
    of the control. This composition settles pause/cancel/resume first: it asks the
    injected runtime adapter, and reports ``applied`` only when the adapter observed
    the required status *and* produced a checkpoint. Otherwise the service records
    ``outcome_unknown`` while the local host still stops mutating work, so a
    requested pause is never silently ignored. Steer/assignment transitions are
    delegated unchanged to the inherited implementation, which is only reached once
    every runtime-observed control has been settled (or retried durably).
    """

    def __init__(self, client, project, agent, run, state_file, *, adapter=None, enabled=True,
                 unobserved_outcome="outcome_unknown"):
        super().__init__(client, project, agent, run, state_file)
        self.adapter = adapter
        self.enabled = enabled
        self.unobserved_outcome = unobserved_outcome
        self.state.setdefault("runtime_pending", {})
        self.state.setdefault("runtime_settled", [])
        self.state.setdefault("runtime_observations", {})

    def boundary(self):
        if not self.enabled:
            return super().boundary()
        self._settle_runtime_controls()
        if self.state["runtime_pending"]:
            # Never let the inherited path re-enter a control we could not settle:
            # it would report an unverified stop as applied.
            return self.state
        return super().boundary()

    def _settle_runtime_controls(self):
        self._replay_runtime_pending()
        controls = self.client.request("GET", f"agents/{self.agent}/controls?project_id={self.project}")
        for control in reversed(controls):
            action, control_id = control["action"], control["id"]
            if action not in RUNTIME_ACTIONS:
                continue
            if control_id in self.state["runtime_pending"] or control_id in self.state["runtime_settled"]:
                continue
            self.client.request("POST", f"controls/{control_id}:ack", {}, key="ack-" + control_id)
            attempt = self._attempt(action, control)
            result = self._result_for(attempt)
            self.state["runtime_observations"][control_id] = {
                "action": action, "applied": attempt.applied, "observed_status": attempt.observed_status,
                "checkpoint_id": attempt.checkpoint_id, "detail": attempt.detail}
            self.state["runtime_pending"][control_id] = result
            self._apply_local_state(action, control_id, result)
            self.persist()
            self._replay_runtime_pending()

    def _attempt(self, action, control):
        target = {"id": control.get("target_run_id") or self.run}
        if self.adapter is None:
            return ControlAttempt(action=action, detail={"state": "no_adapter"})
        method = getattr(self.adapter, action, None)
        if not callable(method):
            return ControlAttempt(action=action, detail={"state": "unsupported_action"})
        try:
            return normalize_attempt(method(target), action)
        except Exception as error:  # Adapter faults must not fabricate an outcome.
            return ControlAttempt(action=action, detail={"state": "adapter_error",
                                                         "error": type(error).__name__})

    def _result_for(self, attempt):
        required = OBSERVED_STATUS.get(attempt.action)
        if attempt.applied is True and attempt.checkpoint_id and attempt.observed_status == required:
            result = {"outcome": "applied", "checkpoint_id": attempt.checkpoint_id,
                      "observed_status": required}
        elif attempt.applied is False:
            result = {"outcome": "failed"}
        else:
            result = {"outcome": self.unobserved_outcome}
        return m.ControlResult.model_validate(result).model_dump(exclude_none=True)

    def _apply_local_state(self, action, control_id, result):
        applied = result["outcome"] == "applied"
        if action == "resume":
            self.state["paused"] = False
            self.state["cancelled"] = False
            if applied:
                self.state["unresolved"] = []
            return
        self.state["paused"] = action == "pause"
        self.state["cancelled"] = action == "cancel"
        if not applied:
            unresolved = self.state.setdefault("unresolved", [])
            if control_id not in unresolved:
                unresolved.append(control_id)

    def _replay_runtime_pending(self):
        for control_id, result in list(self.state["runtime_pending"].items()):
            try:
                self.client.request("POST", f"controls/{control_id}:result", result,
                                    key="runtime-result-" + control_id)
            except (Problem, httpx.HTTPError) as error:
                self.state["runtime_error"] = {"control_id": control_id,
                                               "code": getattr(error, "code", type(error).__name__)}
                self.persist()
                continue
            self.state["runtime_settled"].append(control_id)
            del self.state["runtime_pending"][control_id]
            self.state.pop("runtime_error", None)
            self.persist()


class RuntimeReconciler:
    """Opt-in reconciler for abandoned runs and unobserved workspace file changes.

    ``run_id`` is the *observer* run: it owns the capture spool source and the
    cooperative control boundary. Observations are spooled under that run so an
    abandoned run's presence is never refreshed by this module's own reporting.
    """

    def __init__(self, client, project_id, agent_id, run_id, *, state_dir, workspace_root=None,
                 adapter=None, enabled=False, capture_authority=False, terminal_grant=False,
                 abandoned_status="failed", stale_after=PRESENCE_OFFLINE_SECONDS, absence_grace=300.0,
                 scan_limits=None, exclude=(), capture_interval=5.0, clock=time.time):
        if abandoned_status not in ABANDONED_STATUSES:
            raise ValueError("abandoned_status must be 'failed' or 'cancelled'; completion is never inferred")
        self.client, self.project_id, self.agent_id, self.run_id = client, project_id, agent_id, run_id
        self.adapter = adapter
        self.enabled = enabled
        self.capture_authority = capture_authority
        self.terminal_grant = terminal_grant
        self.abandoned_status = abandoned_status
        self.stale_after = stale_after
        self.absence_grace = absence_grace
        self.scan_limits = scan_limits or ScanLimits()
        self.exclude = tuple(exclude)
        self.capture_interval = capture_interval
        self.clock = clock
        self.workspace_root = Path(workspace_root).resolve() if workspace_root else None
        self.key = _state_key(run_id)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.state_dir / f"{self.key}-lifecycle.json"
        self.state = self._load()
        self._capture = None
        self._boundary = None
        self._observer = None

    # ------------------------------------------------------------------ state

    def _load(self):
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                raise ValueError("Lifecycle state must be an object")
        except (OSError, ValueError):
            state = {}
        state.setdefault("source", uid("source"))
        state.setdefault("sequence", 0)
        state.setdefault("scan_source", uid("source"))
        state.setdefault("scan_sequence", 0)
        state.setdefault("baseline", None)
        state.setdefault("last_report", None)
        return state

    def _save(self):
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(canonical(self.state), encoding="utf-8")
        if os.name != "nt":
            temporary.chmod(0o600)
        os.replace(temporary, self.state_path)

    def _next_sequence(self, scan=False):
        field_name = "scan_sequence" if scan else "sequence"
        self.state[field_name] = int(self.state.get(field_name, 0)) + 1
        self._save()
        return self.state[field_name]

    def audit(self):
        """Local audit view: counters and the last report, never raw file content."""
        return {"run_id": self.run_id, "enabled": self.enabled, "source": self.state["source"],
                "sequence": self.state["sequence"], "scan_source": self.state["scan_source"],
                "scan_sequence": self.state["scan_sequence"], "last_report": self.state["last_report"]}

    # ------------------------------------------------------------ compositions

    def capture(self):
        """The shared capture spool; no background thread is started here."""
        if self._capture is None:
            self._capture = Capture(self.client, self.project_id, self.agent_id, self.run_id,
                                    self.state_dir / f"{self.key}-spool.sqlite",
                                    flush_interval=self.capture_interval, start=False)
        return self._capture

    def boundary(self):
        """Cooperative safe-pause/cancel boundary bound to the observer run."""
        if self._boundary is None:
            self._boundary = LifecycleBoundary(
                self.client, self.project_id, self.agent_id, self.run_id,
                self.state_dir / f"{self.key}-controls.json", adapter=self.adapter, enabled=self.enabled)
        return self._boundary

    def observer_run(self, refresh=False):
        if self._observer is None or refresh:
            self._observer = self.client.request("GET", f"runs/{self.run_id}")
        return self._observer

    # ------------------------------------------------------------------ drift

    def snapshot(self):
        if not self.workspace_root:
            raise ValueError("workspace_root is required for a workspace scan")
        return scan_workspace(self.workspace_root, limits=self.scan_limits, exclude=self.exclude)

    def drift(self, baseline=None, update_baseline=False):
        baseline = baseline or self.state.get("baseline")
        snapshot = self.snapshot()
        findings = diff_snapshots(baseline, snapshot)
        if update_baseline:
            self.state["baseline"] = snapshot
            self._save()
        return findings, snapshot

    def attribution(self, requested=None):
        """Resolve an observation attribution. ``confirmed`` needs both grants.

        Only a trusted adapter (one that itself observed the change) may carry a
        confirmed attribution; a scan-derived workspace difference never can.
        """
        if requested in (None, UNATTRIBUTED, REPORTED):
            return requested or UNATTRIBUTED
        if requested != CONFIRMED:
            raise ValueError("Unsupported attribution")
        if not (self.capture_authority and getattr(self.adapter, "trusted_attribution", False)):
            raise ValueError("Confirmed attribution requires a capture grant and a trusted adapter")
        return CONFIRMED

    def observe_drift(self, findings, target_run=None, attribution=None):
        """Spool file-change observations for scanned differences.

        Paths are validated against the canonical ``file.changed`` schema first and
        findings whose workspace differs from the observer run are skipped rather
        than attributed to the wrong workspace. Scan-derived drift is never
        actor-confirmed: the service rejects ``confirmed`` filesystem observations,
        and this module never guesses an actor.
        """
        reported, skipped = [], []
        if not self.enabled:
            return {"reported": [], "skipped": [{"reason": "disabled"}], "attribution": None}
        workspace = self.observer_run().get("workspace_id") or self.run_id
        target_workspace = (target_run or {}).get("workspace_id") or (target_run or {}).get("id")
        if target_workspace and target_workspace != workspace:
            return {"reported": [], "skipped": [{"reason": "workspace_mismatch",
                                                 "target_run_id": target_run.get("id")}],
                    "attribution": None}
        resolved = self.attribution(attribution)
        if resolved == CONFIRMED:
            raise ValueError("Scan-derived workspace drift cannot carry confirmed attribution")
        capture = self.capture()
        for finding in findings:
            try:
                m.FileData.model_validate({"path": finding.path, "change": finding.change})
            except ValueError:
                skipped.append({"path": finding.path, "reason": "invalid_path"})
                continue
            reported.append(capture.observe("file.changed", {"path": finding.path, "change": finding.change},
                                            attribution=resolved, kind="filesystem"))
        return {"reported": reported, "skipped": skipped, "attribution": resolved,
                "workspace_id": workspace, "kind": "filesystem"}

    def submit(self):
        """Flush the shared spool and report outstanding work."""
        capture = self.capture()
        try:
            capture.flush()
        except (httpx.HTTPError, OSError, ValueError) as error:
            return {"flushed": False, "error": type(error).__name__,
                    "pending": self._spool_count(capture, "pending"),
                    "quarantined": self._spool_count(capture, "quarantined")}
        return {"flushed": True, "pending": self._spool_count(capture, "pending"),
                "quarantined": self._spool_count(capture, "quarantined")}

    @staticmethod
    def _spool_count(capture, state):
        return capture.db.execute("SELECT count(*) FROM spool WHERE state=?", (state,)).fetchone()[0]

    # -------------------------------------------------------------- reconcile

    def inspect(self, run):
        if self.adapter is None or not callable(getattr(self.adapter, "inspect", None)):
            return None
        try:
            return normalize_observation(self.adapter.inspect(run))
        except Exception as error:  # Adapter faults are unknown presence, not absence.
            return AdapterObservation(detail={"state": "adapter_error", "error": type(error).__name__})

    def derived_presence(self, run):
        """Service-equivalent presence bucket, recorded as evidence only."""
        gap = self.clock() - run.get("last_seen_at", 0)
        if gap >= self.stale_after:
            return "offline"
        return "stale" if gap >= self.stale_after / 2 else "online"

    def classify(self, run, observation):
        if run["status"] in TERMINAL_STATUSES:
            return TERMINAL_RUN, "run_already_terminal"
        if observation is None:
            return STALE, "no_adapter_observation"
        if observation.present is None:
            return STALE, "presence_unverified"
        if observation.present:
            return LIVE, "runtime_present"
        if not getattr(self.adapter, "authoritative_absence", False):
            return STALE, "absence_not_authoritative"
        if observation.observed_at is None:
            return STALE, "absence_without_timestamp"
        age = self.clock() - observation.observed_at
        if age > self.absence_grace:
            return STALE, "absence_observation_expired"
        if age < -60:
            return STALE, "absence_observation_from_future"
        return ABANDONED, "runtime_absent"

    def _decide(self, run, observation, classification, reason):
        decision = RunDecision(run_id=run["id"], status=run["status"], classification=classification,
                               reason=reason, agent_id=run.get("agent_id"),
                               derived_presence=self.derived_presence(run),
                               observation=asdict(observation) if observation else None)
        if classification != ABANDONED:
            return decision
        if not self.enabled:
            decision.reason = "disabled"
            return decision
        if not self.terminal_grant:
            decision.reason = "terminal_grant_required"
            return decision
        if not self.capture_authority:
            decision.reason = "capture_authority_required"
            return decision
        return self._end_run(run, decision, observation)

    def _end_run(self, run, decision, observation):
        decision.terminal_status = self.abandoned_status
        decision.evidence = {"run_id": run["id"], "adapter": getattr(self.adapter, "name", None),
                             "observation": asdict(observation) if observation else None,
                             "control_revision": run["control_revision"],
                             "derived_presence": decision.derived_presence}
        decision.action = "ended"
        self._audit_transition("intended", decision)
        try:
            event = m.Event(event_id=uid("evt"), type="hivemind.context.run.ended",
                            agent_id=run["agent_id"], run_id=run["id"], project_id=run["project_id"],
                            occurred_at=now(),
                            source={"source_id": self.state["source"], "kind": "runtime_hook",
                                    "sequence": self._next_sequence(), "attribution": CONFIRMED},
                            data={"status": self.abandoned_status,
                                  "expected_revision": run["control_revision"]})
            results = self.client.request("POST", "events:batch", {"events": [event.model_dump()]})
        except (Problem, httpx.HTTPError) as error:
            self._rewind_sequence()
            decision.action, decision.outcome = "none", "outcome_unknown"
            decision.reason = "transition_transport_error:" + getattr(error, "code", type(error).__name__)
            self._audit_transition("outcome_unknown", decision)
            return decision
        result = results[0] if results else {}
        if result.get("accepted"):
            decision.outcome = "applied"
            decision.evidence["ingestion_seq"] = result.get("ingestion_seq")
            self._audit_transition("applied", decision)
            return decision
        # The event never entered the canonical log, so the sequence is not consumed.
        self._rewind_sequence()
        decision.action = "none"
        error_code = str(result.get("error"))
        if error_code == "terminal_run":
            decision.outcome, decision.reason = "reported", "already_terminal"
        else:
            decision.outcome, decision.reason = "retryable", "transition_rejected:" + error_code
        self._audit_transition(decision.outcome, decision)
        return decision

    def _rewind_sequence(self):
        self.state["sequence"] = max(0, int(self.state.get("sequence", 0)) - 1)
        self._save()

    def _audit_transition(self, state, decision):
        self.state["last_transition"] = {"state": state, "at": self.clock(), "decision": asdict(decision)}
        self._save()

    def reconcile(self, runs=None, scan=False, update_baseline=False):
        """One reconcile pass. Idempotent: re-running re-derives every decision."""
        report = ReconciliationReport(
            enabled=self.enabled, observed_at=self.clock(),
            workspace=str(self.workspace_root) if self.workspace_root else None,
            adapter=getattr(self.adapter, "name", None) if self.adapter is not None else None)
        if scan:
            if not self.workspace_root:
                report.skipped.append({"reason": "workspace_root_required"})
            else:
                findings, _snapshot = self.drift(update_baseline=update_baseline)
                report.drift = [asdict(finding) for finding in findings]
                if findings:
                    report.drift_report = self.observe_drift(findings, target_run=self.observer_run())
                    report.submitted = self.submit()
        targets = list(runs) if runs is not None else [self.observer_run(refresh=True)]
        for run in targets:
            # Always re-read by id: a transition must use the current revision, and a
            # cached body would silently downgrade an authoritative absence to a retry.
            identity = run if isinstance(run, str) else run.get("id")
            if identity:
                run = self.client.request("GET", f"runs/{identity}")
            observation = self.inspect(run)
            classification, reason = self.classify(run, observation)
            report.decisions.append(asdict(self._decide(run, observation, classification, reason)))
        self.state["last_report"] = report.to_dict()
        self._save()
        return report

    def close(self):
        if self._capture is not None:
            try:
                self._capture.close()
            except (httpx.HTTPError, OSError, RuntimeError):
                pass
            self._capture = None


class ServiceTransport:
    """Client-shaped (``.request``) transport over the in-process ``Service``.

    This lets the reconciler run inside the service process (for example from
    :func:`install`) while still using the canonical Service authorization,
    idempotency, and transition rules. It exposes only the routes the reconciler
    uses; every other path is ``not_found``.
    """

    def __init__(self, service, store, principal, command=None):
        self.service, self.store, self.principal = service, store, principal
        self.command = command

    def _mutate(self, operation, key, model, function):
        payload = model.model_dump(by_alias=True, exclude_none=True)
        return self.service.mutate(self.principal, operation, key or uid("cmd"), payload, function)

    def request(self, method, path, body=None, key=None):
        path = path.lstrip("/")
        if path.startswith("./"):
            path = path[2:]
        route, _, query_string = path.partition("?")
        query = parse_qs(query_string)
        segments = route.split("/")
        body = body or {}

        if method == "POST" and route == "events:batch":
            return self._events(body)
        if method == "GET" and route == "runs":
            return self._runs(query)
        if method == "POST" and route == "runs":
            request = m.RunCreate.model_validate(body)
            return self._mutate("runs", key, request, lambda: self.service.start_run(self.principal, request))
        if method == "GET" and segments[0] == "runs" and len(segments) == 2:
            return self.service.run(self.principal, segments[1])
        if method == "POST" and len(segments) == 3 and segments[0] == "runs" and segments[2] == "heartbeat":
            return self._heartbeat(segments[1], body)
        if method == "POST" and len(segments) == 3 and segments[0] == "runs" and segments[2] == "permits:check":
            return self.service.permits(self.principal, segments[1], m.PermitRequest.model_validate(body))
        if method == "GET" and len(segments) == 3 and segments[0] == "agents" and segments[2] == "controls":
            return self._controls(segments[1], query)
        if method == "POST" and segments[0] == "controls" and len(segments) == 2:
            return self._control_command(segments[1], key, body)
        raise Problem("not_found", 404)

    def _events(self, body):
        results = []
        for raw in body.get("events", []):
            identity = raw.get("event_id") if isinstance(raw, dict) else None
            try:
                event = m.Event.model_validate(raw)
                results.append({"accepted": True, **self.service.accept_event(self.principal, event)})
            except ValidationError:
                results.append({"accepted": False, "event_id": identity if isinstance(identity, str) else None,
                                "error": "invalid_request", "retryable": False})
            except Problem as error:
                results.append({"accepted": False, "event_id": identity if isinstance(identity, str) else None,
                                "error": error.code, "retryable": error.status in (429, 503)})
        return results

    def _runs(self, query):
        project = (query.get("project_id") or [""])[0]
        result = []
        for run in self.store.list("run", project, self.principal, 1000):
            gap = time.time() - run["last_seen_at"]
            result.append({**run, "presence": "offline" if gap >= 30 else "stale" if gap >= 15 else "online"})
        return {"runs": result}

    def _heartbeat(self, run_id, body):
        with self.store.transaction():
            run = self.service.run(self.principal, run_id)
            if run["status"] in TERMINAL:
                raise Problem("terminal_run")
            run.update(last_seen_at=time.time(),
                       capture_health=body.get("capture_health", {"state": "healthy"}))
            self.store.db.execute("UPDATE resources SET body=? WHERE id=?", (canonical(run), run_id))
        return {"last_seen_at": run["last_seen_at"]}

    def _controls(self, agent, query):
        project = (query.get("project_id") or [""])[0]
        self.service.represent(self.principal, agent)
        result = []
        for control in self.store.list("control", project, self.principal):
            if control["target_agent_id"] != agent or control["state"] not in (
                    "queued", "acknowledged", "applying"):
                continue
            try:
                self.service.live_control(self.principal, control["id"])
                result.append(control)
            except Problem:
                pass
        return result

    def _control_command(self, control_id, key, body):
        identity, _, action = control_id.partition(":")
        if action == "ack":
            def ack():
                item = self.service.live_control(self.principal, identity)
                item["state"] = "acknowledged"
                row = self.store.raw(identity)
                return self.store.save("control", row["project"], row["owner"], item)
            return self._mutate("control-ack:" + identity, key, _Empty(), ack)
        if action == "result":
            request = m.ControlResult.model_validate(body)
            return self._mutate("control-result:" + identity, key, request,
                                lambda: self.service.control_result(self.principal, identity, request))
        if action == "checkpoint":
            request = m.Checkpoint.model_validate(body)
            return self._mutate("control-checkpoint:" + identity, key, request,
                                lambda: self.service.checkpoint(self.principal, identity, request))
        raise Problem("not_found", 404)


class _Empty(m.Model):
    """Empty body placeholder for commands that take no payload."""


class ReconcileRequest(m.Model):
    project_id: m.ID
    agent_id: m.ID
    run_id: m.ID
    targets: list[m.ID] = Field(default_factory=list, max_length=100)
    workspace_root: str | None = Field(default=None, max_length=4096)
    adapter_state_file: str | None = Field(default=None, max_length=4096)
    scan: bool = False
    enabled: bool = True
    terminal: bool = False
    update_baseline: bool = False


def install(app, service, principal, command=None, envelope=None, *, path="/v0.1/runtime",
            transport_factory=None):
    """Register opt-in runtime lifecycle routes on an existing application.

    The parent wires this; nothing is registered implicitly. ``scan`` and
    ``adapter_state_file`` are administrator-only because they read local files.
    """
    store = getattr(getattr(app, "state", None), "store", None) or service.store
    transport_factory = transport_factory or (
        lambda p: ServiceTransport(service, store, p, command))
    wrapper = envelope or (lambda data: data)

    @app.post(path + ":reconcile")
    async def runtime_reconcile(body: ReconcileRequest, p=Depends(principal)):
        service.project_access(p, body.project_id)
        service.represent(p, body.agent_id)
        administrator = p["role"] == "admin"
        if (body.scan or body.adapter_state_file) and not administrator:
            raise Problem("administrator_required", 403)
        targets = []
        for target in body.targets:
            run = store.get(target, p, "run")
            if run["agent_id"] != body.agent_id and not administrator:
                raise Problem("administrator_required", 403)
            targets.append(run)
        reconciler = RuntimeReconciler(
            transport_factory(p), body.project_id, body.agent_id, body.run_id,
            workspace_root=body.workspace_root, state_dir=store.directory / "runtime",
            adapter=FileAdapter(body.adapter_state_file) if body.adapter_state_file else None,
            enabled=body.enabled, capture_authority=bool(p["capture_authority"]),
            terminal_grant=body.terminal)
        report = reconciler.reconcile(targets or None, scan=body.scan,
                                      update_baseline=body.update_baseline)
        return wrapper(report.to_dict())

    @app.get(path + ":state")
    async def runtime_state(project_id: str, agent_id: str, run_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        service.represent(p, agent_id)
        reconciler = RuntimeReconciler(transport_factory(p), project_id, agent_id, run_id,
                                       state_dir=store.directory / "runtime")
        return wrapper(reconciler.audit())

    return app
