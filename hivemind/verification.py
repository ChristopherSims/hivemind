"""Independent result verification for agent task executions.

This module adds a second, separate resource that records (a) what a producing agent
*reports* about a finished task execution and (b) what an *independent* reviewer
concludes after local trusted checks. It never mutates a task, never promotes a
verification into memory, and never executes agent-supplied text.

Trust model
-----------
- ``TaskProgress``/``task.status == "completed"`` remain self-reported terminal states.
  A verification record is a distinct resource with its own status
  (``pending``, ``needs_corrections``, ``verified``, ``rejected``).
- Producer-supplied test evidence is stored as an *attestation*: every record carries
  ``trust="self_reported"`` and ``objective=False``. Self-reported text can only ever
  obstruct a decision; it can never satisfy ``verified``.
- ``verified`` requires at least one trusted local check whose process exit code was
  zero and no trusted check that failed. Checks come from :class:`TrustedVerifier`,
  which only runs operator-allowlisted argv arrays with ``shell=False``, a sanitized
  environment, a bounded timeout, bounded output capture, workspace-contained working
  directory, and hashed declared artifacts. No shell strings, no downloaded code,
  no privilege escalation, no remote writes, no paid sessions.
- An assigned reviewer agent must differ from the producing agent.
- Corrections are an explicit resource and workflow; nothing is auto-resolved and no
  background scheduler is required (a future scheduler can poll the pending queue).

Scope limits are documented in ``docs/task-verification.md``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Sequence

from fastapi import Depends, Header, Query, Request

from . import models as m
from .storage import Problem, now, uid

VERIFICATION_KIND = "verification"
CORRECTION_KIND = "correction"
STATUSES = ("pending", "needs_corrections", "verified", "rejected")
OPEN_STATUSES = ("pending", "needs_corrections")
DECISIONS = ("verified", "needs_corrections", "rejected")
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
MAX_CORRECTION_CYCLES = 5
MAX_CHECKS = 16
MAX_EVIDENCE_REFS = 64
MAX_ARTIFACT_PATHS = 16
MAX_ARTIFACT_BYTES = 1048576
MAX_OUTPUT_BYTES = 65536
MAX_TIMEOUT_SECONDS = 600.0
ENVIRONMENT_KEYS = ("PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "HOME",
                    "USERPROFILE", "LANG", "LC_ALL", "NUMBER_OF_PROCESSORS")
SHELL_PROGRAMS = frozenset({
    "sh", "bash", "dash", "zsh", "ksh", "csh", "fish", "cmd", "cmd.exe",
    "powershell", "powershell.exe", "pwsh", "pwsh.exe", "wsl", "wsl.exe",
})
# Flags that request inline source, an inline script, or an interactive shell.
BLOCKED_ARGUMENTS = frozenset({
    "-c", "-e", "--eval", "--command", "-command", "/c", "/k", "-encodedcommand",
    "--encodedcommand", "--interactive", "-i",
})
CHECK_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


# ---------------------------------------------------------------------------
# Request models (self-contained; subclasses of the shared model base)
# ---------------------------------------------------------------------------

class VerificationEvidenceRef(m.Ref):
    kind: Literal["artifact", "board_post"]


class ReportedTest(m.Model):
    """Producer-reported test outcome. Never treated as objective proof."""

    name: str = m.Field(min_length=1, max_length=256)
    outcome: Literal["pass", "fail", "unknown"]
    detail: str | None = m.Field(default=None, max_length=4096)


class VerificationSubmit(m.Model):
    task_id: m.ID
    run_id: m.ID
    execution_id: m.ID
    completion_claimed: bool
    summary: str = m.Field(min_length=1, max_length=4096)
    artifact_refs: list[VerificationEvidenceRef] = m.Field(default_factory=list, max_length=32)
    test_evidence: list[ReportedTest] = m.Field(default_factory=list, max_length=32)


class VerificationUpdate(VerificationSubmit):
    expected_revision: int = m.Field(ge=1)


class CorrectionBegin(m.Model):
    expected_revision: int = m.Field(ge=1)
    run_id: m.ID


class ReviewerAssignment(m.Model):
    expected_revision: int = m.Field(ge=1)
    reviewer_agent_id: m.ID


class TrustedCheck(m.Model):
    expected_revision: int = m.Field(ge=1)
    check_name: str = m.Field(min_length=1, max_length=128)
    artifact_paths: list[str] = m.Field(default_factory=list, max_length=MAX_ARTIFACT_PATHS)


class ReviewDecision(m.Model):
    expected_revision: int = m.Field(ge=1)
    decision: Literal["verified", "needs_corrections", "rejected"]
    reason: str = m.Field(min_length=1, max_length=4096)
    correction_items: list[str] = m.Field(default_factory=list, max_length=16)

    @m.model_validator(mode="after")
    def items(self):
        for item in self.correction_items:
            if not item.strip() or len(item) > 1024:
                raise ValueError("Correction items must be non-empty and under 1024 characters")
        return self


class CorrectionResolve(m.Model):
    expected_revision: int = m.Field(ge=1)
    run_id: m.ID
    execution_id: m.ID
    summary: str = m.Field(min_length=1, max_length=4096)
    artifact_refs: list[VerificationEvidenceRef] = m.Field(default_factory=list, max_length=32)


# ---------------------------------------------------------------------------
# Trusted local verification helper
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TrustedCommand:
    """A resolved allowlisted command. Values are operator-supplied, never agent text."""

    name: str
    argv: tuple[str, ...]
    cwd: str
    timeout: float


@dataclass
class TrustedCheckResult:
    name: str
    argv: tuple[str, ...]
    cwd: str
    returncode: int | None
    timed_out: bool
    duration_ms: int
    ok: bool
    stdout: str
    stdout_sha256: str
    stdout_bytes: int
    stdout_truncated: bool
    stderr: str
    stderr_sha256: str
    stderr_bytes: int
    stderr_truncated: bool
    artifacts: list[dict]
    error: str | None = None

    def as_record(self) -> dict:
        return {
            "name": self.name, "argv": list(self.argv), "cwd": self.cwd,
            "returncode": self.returncode, "timed_out": self.timed_out,
            "duration_ms": self.duration_ms, "ok": self.ok, "error": self.error,
            "stdout": self.stdout, "stdout_sha256": self.stdout_sha256,
            "stdout_bytes": self.stdout_bytes, "stdout_truncated": self.stdout_truncated,
            "stderr": self.stderr, "stderr_sha256": self.stderr_sha256,
            "stderr_bytes": self.stderr_bytes, "stderr_truncated": self.stderr_truncated,
            "artifacts": self.artifacts,
        }


def _to_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8", "replace")
    raise TypeError("Runner output must be bytes, str, or None")


def _bounded(data: bytes, limit: int) -> tuple[str, bool]:
    return data[:limit].decode("utf-8", "replace"), len(data) > limit


def _normalize(value: Any) -> tuple[int | None, bytes, bytes]:
    """Accept a 3-tuple or an object with returncode/stdout/stderr from an injected runner."""
    if isinstance(value, (tuple, list)):
        if len(value) != 3:
            raise ValueError("Injected runner must return (returncode, stdout, stderr)")
        return value[0], _to_bytes(value[1]), _to_bytes(value[2])
    return (getattr(value, "returncode", None), _to_bytes(getattr(value, "stdout", None)),
            _to_bytes(getattr(value, "stderr", None)))


def _validate_check(name: str, argv: Iterable[str]) -> tuple[str, ...]:
    if not isinstance(name, str) or not CHECK_NAME.match(name):
        raise ValueError("Check names must be short identifiers")
    tokens = list(argv)
    if not 1 <= len(tokens) <= 16:
        raise ValueError("An allowlisted check must have between 1 and 16 arguments")
    for token in tokens:
        if not isinstance(token, str) or not token or len(token) > 512:
            raise ValueError("Allowlisted arguments must be non-empty strings under 512 characters")
        if any(character in token for character in ("\x00", "\n", "\r")):
            raise ValueError("Allowlisted arguments cannot contain control characters")
        if "://" in token:
            raise ValueError("Allowlisted arguments cannot be URLs; nothing is downloaded or fetched")
        if token.lower() in BLOCKED_ARGUMENTS:
            raise ValueError(f"Allowlisted argument {token!r} requests inline code or an interactive shell")
    program = tokens[0]
    if program.startswith("-"):
        raise ValueError("An allowlisted program cannot start with a dash")
    leaf = Path(program).name.lower()
    if (leaf[:-4] if leaf.endswith(".exe") else leaf) in SHELL_PROGRAMS:
        raise ValueError("Shells are not allowlisted; checks must be direct argv arrays")
    if ".." in Path(program).parts:
        raise ValueError("An allowlisted program cannot traverse directories")
    return tuple(tokens)


class TrustedVerifier:
    """Runs operator-allowlisted local commands inside a contained workspace root.

    ``checks`` maps a check name to an argv array. Payloads from agents never supply a
    command, a flag, an interpreter, or a shell string; they can only name an entry.

    ``runner`` may be injected (tests and embedding applications). It receives the frozen
    :class:`TrustedCommand` and returns ``(returncode, stdout, stderr)`` or an object with
    ``returncode``/``stdout``/``stderr``. Raising :class:`TimeoutError` (or
    ``subprocess.TimeoutExpired``) is recorded as a timeout. When ``runner`` is ``None``
    the command runs through ``subprocess.run(..., shell=False)`` with no stdin, a
    sanitized environment, and a bounded timeout.
    """

    def __init__(self, workspace_root, checks, *, runner: Callable[[TrustedCommand], Any] | None = None,
                 timeout: float = 60.0, max_output_bytes: int = MAX_OUTPUT_BYTES,
                 max_file_bytes: int = MAX_ARTIFACT_BYTES, extra_environment: dict | None = None,
                 require_os_isolation=False):
        self.require_os_isolation = bool(require_os_isolation)
        root = Path(workspace_root).expanduser()
        if not root.is_dir():
            raise ValueError("Workspace root must be an existing directory")
        self.root = root.resolve()
        if not checks:
            raise ValueError("At least one allowlisted check is required")
        if not isinstance(timeout, (int, float)) or not 0 < float(timeout) <= MAX_TIMEOUT_SECONDS:
            raise ValueError(f"Timeout must be between 0 and {MAX_TIMEOUT_SECONDS:g} seconds")
        if int(max_output_bytes) < 0 or int(max_file_bytes) < 0:
            raise ValueError("Output and artifact bounds cannot be negative")
        self.checks = {name: _validate_check(name, argv) for name, argv in checks.items()}
        self.runner = runner
        self.timeout = float(timeout)
        self.max_output_bytes = int(max_output_bytes)
        self.max_file_bytes = int(max_file_bytes)
        self.extra_environment = {}
        for key, value in (extra_environment or {}).items():
            if not isinstance(key, str) or not isinstance(value, str) or "\x00" in value:
                raise ValueError("Extra environment entries must be NUL-free strings")
            self.extra_environment[key] = value

    @property
    def names(self) -> list[str]:
        return sorted(self.checks)

    def environment(self) -> dict:
        """A minimal, sanitized environment: no arbitrary inherited variables."""
        values = {key: os.environ[key] for key in ENVIRONMENT_KEYS if key in os.environ}
        values.update(self.extra_environment)
        return values

    def _contain(self, path: str) -> tuple[str, Path]:
        try:
            relative = m.safe_path(path)
        except ValueError:
            raise Problem("invalid_artifact_path", 422) from None
        resolved = (self.root / relative).resolve()
        try:
            resolved.relative_to(self.root)
        except ValueError:
            raise Problem("artifact_path_escape", 422) from None
        return relative, resolved

    def _execute(self, command: TrustedCommand) -> tuple[int | None, bytes, bytes]:
        if self.runner is not None:
            return _normalize(self.runner(command))
        from .process_boundary import run_bounded
        return run_bounded(command.argv, command.cwd, self.environment(), command.timeout,
                           max_output=self.max_output_bytes * 2,
                           require_os_isolation=self.require_os_isolation)

    def _artifact(self, path: str) -> dict:
        _, resolved = self._contain(path)
        if not resolved.exists():
            return {"path": path, "missing": True}
        if not resolved.is_file():
            return {"path": path, "skipped": "not_a_regular_file"}
        size = resolved.stat().st_size
        if size > self.max_file_bytes:
            return {"path": path, "skipped": "too_large", "byte_size": size}
        digest = hashlib.sha256()
        with resolved.open("rb") as handle:
            for block in iter(lambda: handle.read(65536), b""):
                digest.update(block)
        return {"path": path, "byte_size": size, "sha256": digest.hexdigest()}

    def run(self, name: str, artifact_paths: Sequence[str] = ()) -> TrustedCheckResult:
        argv = self.checks.get(name)
        if argv is None:
            raise Problem("check_not_allowlisted", 422)
        paths = list(artifact_paths)
        if len(paths) > MAX_ARTIFACT_PATHS:
            raise Problem("too_many_artifacts", 422)
        declared = [self._contain(path)[0] for path in paths]
        command = TrustedCommand(name=name, argv=argv, cwd=str(self.root), timeout=self.timeout)
        started = time.monotonic()
        returncode: int | None = None
        stdout = stderr = b""
        timed_out, error = False, None
        try:
            returncode, stdout, stderr = self._execute(command)
        except (TimeoutError, subprocess.TimeoutExpired) as exc:
            timed_out, error = True, "timeout"
            stdout = _to_bytes(getattr(exc, "output", None) or getattr(exc, "stdout", None))
            stderr = _to_bytes(getattr(exc, "stderr", None))
        except Exception as exc:  # A broken allowlisted check is recorded, not raised.
            error = f"{type(exc).__name__}: {exc}"[:512]
        duration_ms = int((time.monotonic() - started) * 1000)
        stdout_text, stdout_cut = _bounded(stdout, self.max_output_bytes)
        stderr_text, stderr_cut = _bounded(stderr, self.max_output_bytes)
        return TrustedCheckResult(
            name=name, argv=argv, cwd=str(self.root), returncode=returncode, timed_out=timed_out,
            duration_ms=duration_ms, ok=error is None and not timed_out and returncode == 0,
            stdout=stdout_text, stdout_sha256=hashlib.sha256(stdout).hexdigest(), stdout_bytes=len(stdout),
            stdout_truncated=stdout_cut, stderr=stderr_text, stderr_sha256=hashlib.sha256(stderr).hexdigest(),
            stderr_bytes=len(stderr), stderr_truncated=stderr_cut,
            artifacts=[self._artifact(path) for path in declared], error=error)


# ---------------------------------------------------------------------------
# Domain
# ---------------------------------------------------------------------------

class Verifications:
    """Result submissions, independent reviewer decisions, and corrections."""

    def __init__(self, service, verifier: TrustedVerifier | None = None, max_cycles: int = MAX_CORRECTION_CYCLES):
        self.service = service
        self.store = service.store
        self.verifier = verifier
        self.max_cycles = int(max_cycles)

    # -- helpers ----------------------------------------------------------
    def _save(self, kind: str, body: dict, expected: int | None) -> dict:
        row = self.store.raw(body["id"])
        return self.store.save(kind, row["project"], row["owner"], body, expected=expected)

    def _pin(self, principal: dict, project: str, refs, task_id: str) -> list[dict]:
        pinned = [ref.model_dump(exclude_none=True) for ref in refs]
        if any(ref["id"] == task_id for ref in pinned):
            raise Problem("self_evidence_forbidden", 422)
        self.service.evidence(principal, project, pinned)
        for ref in pinned:
            ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
        return pinned

    @staticmethod
    def _merge(existing: list[dict], incoming: list[dict]) -> list[dict]:
        merged, seen = [], set()
        for ref in [*existing, *incoming]:
            key = (ref["kind"], ref["id"], ref.get("revision"))
            if key in seen:
                continue
            seen.add(key)
            merged.append(ref)
        if len(merged) > MAX_EVIDENCE_REFS:
            raise Problem("too_many_evidence_refs", 422)
        return merged

    def _evidence_available(self, principal: dict, refs: list[dict]) -> bool:
        for ref in refs:
            try:
                row = self.store.raw(ref["id"])
                if not self.store.visible(row, principal) or row["kind"] != ref["kind"]:
                    raise Problem("not_found", 404)
                if ref.get("revision") != row["revision"]:
                    raise Problem("stale_evidence")
            except Problem:
                return False
        return True

    def _task_execution(self, principal: dict, task: dict, run_id: str, execution_id: str) -> tuple[dict, dict]:
        run = self.service.run(principal, run_id)
        if run["project_id"] != task["project_id"]:
            raise Problem("not_found", 404)
        if run["status"] in TERMINAL_RUN_STATUSES:
            raise Problem("terminal_run")
        execution = task.get("execution")
        if not execution:
            raise Problem("task_execution_required")
        if execution["run_id"] != run["id"] or execution["id"] != execution_id:
            raise Problem("stale_task_execution")
        return run, execution

    def _open_verification(self, project: str, task_id: str, execution_id: str) -> str | None:
        rows = self.store.db.execute(
            "SELECT body FROM resources WHERE kind=? AND project=? AND deleted=0",
            (VERIFICATION_KIND, project)).fetchall()
        for row in rows:
            body = json.loads(row["body"])
            if (body.get("task_id") == task_id and body.get("execution_id") == execution_id
                    and body.get("status") in OPEN_STATUSES):
                return body["id"]
        return None

    @staticmethod
    def _test_evidence(items) -> list[dict]:
        records = []
        for item in items:
            record = item.model_dump(exclude_none=True)
            record.update(trust="self_reported", objective=False)
            records.append(record)
        return records

    # -- views ------------------------------------------------------------
    def view(self, principal: dict, body: dict) -> dict:
        """Return a read-safe verification. Unavailable evidence is redacted and downgraded."""
        result = dict(body)
        available = self._evidence_available(principal, result.get("evidence_refs", []))
        reported = result.get("reported") or {}
        result["evidence_available"] = available
        result["evidence_unavailable"] = not available
        result["reported_completion"] = bool(reported.get("completion_claimed"))
        if not available:
            # Tampered, deleted, or now-invisible evidence cannot support a verified claim.
            result["reported"] = {
                "completion_claimed": bool(reported.get("completion_claimed")),
                "reported_by_agent_id": reported.get("reported_by_agent_id"),
                "reported_at": reported.get("reported_at"),
            }
            result["test_evidence"] = []
            result["effective_status"] = "pending"
        else:
            result["effective_status"] = result.get("status")
        result["independently_verified"] = bool(
            result.get("status") == "verified" and available and result.get("reviewer_agent_id"))
        return result

    def correction_view(self, principal: dict, body: dict) -> dict:
        result = dict(body)
        resolution = result.get("resolution")
        available = self._evidence_available(principal, (resolution or {}).get("evidence_refs", []))
        result["evidence_available"] = available
        if resolution and not available:
            result["resolution"] = {
                "resolved_at": resolution.get("resolved_at"),
                "resolved_by_principal_id": resolution.get("resolved_by_principal_id"),
                "evidence_unavailable": True,
            }
        return result

    # -- reads ------------------------------------------------------------
    def get(self, principal: dict, verification_id: str) -> dict:
        return self.view(principal, self.store.get(verification_id, principal, VERIFICATION_KIND))

    def list(self, principal: dict, project_id: str, task_id: str | None = None,
             status: str | None = None) -> dict:
        self.service.project_access(principal, project_id)
        if status is not None and status not in STATUSES:
            raise Problem("invalid_status", 422)
        items = [self.view(principal, item) for item in self.store.list(VERIFICATION_KIND, project_id, principal, 1000)
                 if (task_id is None or item["task_id"] == task_id)
                 and (status is None or item["status"] == status)]
        return {"project_id": project_id, "verifications": items}

    def correction(self, principal: dict, correction_id: str) -> dict:
        return self.correction_view(principal, self.store.get(correction_id, principal, CORRECTION_KIND))

    def corrections(self, principal: dict, verification_id: str) -> dict:
        body = self.store.get(verification_id, principal, VERIFICATION_KIND)
        summary = body.get("corrections") or {}
        items = []
        for identity in [*summary.get("open_ids", []), *summary.get("resolved_ids", [])]:
            try:
                items.append(self.correction_view(principal, self.store.get(identity, principal, CORRECTION_KIND)))
            except Problem:
                continue
        return {"verification_id": verification_id, "corrections": items}

    # -- producer writes --------------------------------------------------
    def submit(self, principal: dict, request: VerificationSubmit) -> dict:
        task = self.store.get(request.task_id, principal, "task")
        self.service.project_access(principal, task["project_id"])
        run, execution = self._task_execution(principal, task, request.run_id, request.execution_id)
        open_id = self._open_verification(task["project_id"], task["id"], execution["id"])
        if open_id:
            raise Problem("verification_open")
        refs = self._pin(principal, task["project_id"], request.artifact_refs, task["id"])
        body = {
            "id": uid(VERIFICATION_KIND), "project_id": task["project_id"], "task_id": task["id"],
            "title": f"Verification for {task['title'][:400]}",
            "content": {"media_type": "text/plain", "text": request.summary[:65536]},
            "run_id": request.run_id, "execution_id": execution["id"], "agent_id": execution["agent_id"],
            "workspace_id": run.get("workspace_id"),
            "status": "pending",
            "reported": {
                "completion_claimed": request.completion_claimed, "summary": request.summary,
                "reported_by_agent_id": execution["agent_id"],
                "reported_by_principal_id": principal["id"], "reported_at": now(),
                "trust": "self_reported", "objective": False,
            },
            "test_evidence": self._test_evidence(request.test_evidence),
            "evidence_refs": refs,
            "checks": [],
            "reviewer_agent_id": None, "assignment": None, "review": None,
            "corrections": {"cycles": 0, "open_ids": [], "resolved_ids": []},
            "correction_history": [],
            "task_snapshot": {
                "status": task["status"], "progress": execution.get("progress"),
                "checkpoint_id": (execution.get("checkpoint") or {}).get("id"),
                "trust": "self_reported",
            },
        }
        result = self.store.save(VERIFICATION_KIND, task["project_id"], principal["id"], body)
        self.store.relate(task["project_id"], result["id"], task["id"], "VERIFIES")
        self.store.relate(task["project_id"], result["id"], request.run_id, "SUBMITTED_BY")
        return result

    def update_submission(self, principal: dict, verification_id: str, request: VerificationUpdate) -> dict:
        body = self.store.get(verification_id, principal, VERIFICATION_KIND)
        if body["status"] != "pending":
            raise Problem("verification_not_editable")
        if body["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        self.service.represent(principal, body["agent_id"])
        task = self.store.get(body["task_id"], principal, "task")
        _, execution = self._task_execution(principal, task, request.run_id, request.execution_id)
        if execution["id"] != body["execution_id"]:
            raise Problem("stale_task_execution")
        refs = self._pin(principal, body["project_id"], request.artifact_refs, body["task_id"])
        updated = {
            **body,
            "title": f"Verification for {task['title'][:400]}",
            "content": {"media_type": "text/plain", "text": request.summary[:65536]},
            "reported": {
                **body["reported"], "completion_claimed": request.completion_claimed,
                "summary": request.summary, "reported_by_principal_id": principal["id"], "reported_at": now(),
            },
            "test_evidence": self._test_evidence(request.test_evidence),
            "evidence_refs": refs,
            "task_snapshot": {
                "status": task["status"], "progress": execution.get("progress"),
                "checkpoint_id": (execution.get("checkpoint") or {}).get("id"), "trust": "self_reported",
            },
            "checks": [],
            "check_history": [*body.get("check_history", []), body.get("checks", [])][-5:],
        }
        return self._save(VERIFICATION_KIND, updated, request.expected_revision)

    # -- reviewer workflow ------------------------------------------------
    def assign_reviewer(self, principal: dict, verification_id: str, request: ReviewerAssignment) -> dict:
        body = self.store.get(verification_id, principal, VERIFICATION_KIND)
        if body["status"] not in OPEN_STATUSES:
            raise Problem("verification_decided")
        if body["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if principal["role"] not in ("admin", "human") and principal.get("agent") != body["agent_id"]:
            raise Problem("reviewer_assignment_forbidden", 403)
        reviewer = self.store.get(request.reviewer_agent_id, principal, "agent")
        if reviewer["project_id"] != body["project_id"]:
            raise Problem("not_found", 404)
        if reviewer["id"] == body["agent_id"]:
            raise Problem("reviewer_must_differ_from_producer", 422)
        updated = {**body, "reviewer_agent_id": reviewer["id"],
                   "assignment": {"assigned_by_principal_id": principal["id"],
                                  "assigned_by_agent_id": principal.get("agent"), "assigned_at": now()}}
        return self._save(VERIFICATION_KIND, updated, request.expected_revision)

    def record_check(self, principal: dict, verification_id: str, request: TrustedCheck,
                     verifier: TrustedVerifier | None = None) -> dict:
        if principal["role"] not in ("admin", "human"):
            raise Problem("trusted_verifier_required", 403)
        body = self.store.get(verification_id, principal, VERIFICATION_KIND)
        if body["status"] not in OPEN_STATUSES:
            raise Problem("verification_decided")
        if body["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        verifier = verifier or self.verifier
        if verifier is None:
            raise Problem("trusted_verifier_unavailable", 503)
        if len(body.get("checks", [])) >= MAX_CHECKS:
            raise Problem("check_limit_reached", 422)
        from .agent_session import HandlerJournal
        from .recovery import Journal, UnknownOutcome
        signature = hashlib.sha256(json.dumps(
            {"verification_id": body["id"], "revision": body["revision"],
             "check": request.check_name, "paths": request.artifact_paths,
             "argv": getattr(verifier, "checks", {}).get(request.check_name),
             "cwd": str(getattr(verifier, "root", ""))},
            sort_keys=True).encode()).hexdigest()
        try:
            with Journal(self.store.directory / "check-effects.sqlite3") as journal:
                gate = HandlerJournal(journal, body["project_id"], principal["id"])
                def run_check():
                    return verifier.run(request.check_name, request.artifact_paths).as_record()
                receipt = gate.handle("trusted-check", signature, run_check)
                result_record = receipt["result"].get("value")
                if not result_record:
                    raise UnknownOutcome("Reconciled check has no trusted result receipt")
                for artifact in result_record.get("artifacts", []):
                    if verifier._artifact(artifact["path"]) != artifact:
                        raise UnknownOutcome("Recorded check artifacts changed")
        except UnknownOutcome:
            raise Problem("check_effect_unknown", 409) from None
        record = {**result_record, "recorded_at": now(), "recorded_by_principal_id": principal["id"],
                  "trust": "trusted_local_check", "objective": True}
        updated = {**body, "checks": [*body.get("checks", []), record]}
        return self._save(VERIFICATION_KIND, updated, request.expected_revision)

    def decide(self, principal: dict, verification_id: str, request: ReviewDecision) -> dict:
        body = self.store.get(verification_id, principal, VERIFICATION_KIND)
        if body["status"] not in OPEN_STATUSES:
            raise Problem("verification_decided")
        if body["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        reviewer = body.get("reviewer_agent_id")
        if not reviewer:
            raise Problem("reviewer_assignment_required")
        if reviewer == body["agent_id"]:
            raise Problem("reviewer_must_differ_from_producer", 422)
        self.service.represent(principal, reviewer)
        if principal.get("agent") == body["agent_id"]:
            raise Problem("self_review_forbidden", 403)
        corrections = body.get("corrections") or {"cycles": 0, "open_ids": [], "resolved_ids": []}
        if corrections["open_ids"] and request.decision != "rejected":
            raise Problem("corrections_open")
        if request.decision == "verified":
            if not self._evidence_available(principal, body["evidence_refs"]):
                raise Problem("evidence_unavailable", 422)
            if any(item["outcome"] == "fail" for item in body["test_evidence"]):
                raise Problem("reported_test_failure", 422)
            checks = body.get("checks", [])
            if any(not check["ok"] for check in checks):
                raise Problem("trusted_check_failed", 422)
            if not any(check["ok"] for check in checks):
                raise Problem("trusted_check_required", 422)
        if request.decision == "needs_corrections":
            if not request.correction_items:
                raise Problem("correction_items_required", 422)
            if corrections["cycles"] + 1 > self.max_cycles:
                raise Problem("correction_limit_reached")
        review = {"decision": request.decision, "reviewer_agent_id": reviewer,
                  "decided_by_principal_id": principal["id"], "decided_at": now(), "reason": request.reason,
                  "trust": "independent_review"}
        updated = {**body, "status": request.decision, "review": review}
        if request.decision == "needs_corrections":
            correction = self.store.save(CORRECTION_KIND, body["project_id"], principal["id"], {
                "id": uid(CORRECTION_KIND), "project_id": body["project_id"],
                "verification_id": body["id"], "task_id": body["task_id"],
                "title": f"Corrections for {body['task_id']}",
                "content": {"media_type": "text/plain", "text": " ".join(request.correction_items)[:65536]},
                "items": list(request.correction_items), "state": "open", "resolution": None,
                "requested_by_principal_id": principal["id"], "reviewer_agent_id": reviewer,
                "reason": request.reason,
            })
            self.store.relate(body["project_id"], correction["id"], body["id"], "CORRECTS")
            updated["corrections"] = {**corrections, "cycles": corrections["cycles"] + 1,
                                      "open_ids": [*corrections["open_ids"], correction["id"]]}
            updated["correction_history"] = [*body.get("correction_history", []), {
                "correction_id": correction["id"], "event": "requested", "at": now(),
                "reviewer_agent_id": reviewer}][-20:]
        return self._save(VERIFICATION_KIND, updated, request.expected_revision)

    def resolve_correction(self, principal: dict, correction_id: str, request: CorrectionResolve) -> dict:
        correction = self.store.get(correction_id, principal, CORRECTION_KIND)
        if correction["state"] != "open":
            raise Problem("correction_not_open")
        if correction["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        body = self.store.get(correction["verification_id"], principal, VERIFICATION_KIND)
        if body["status"] != "needs_corrections":
            raise Problem("correction_not_open")
        self.service.represent(principal, body["agent_id"])
        task = self.store.get(body["task_id"], principal, "task")
        _, execution = self._task_execution(principal, task, request.run_id, request.execution_id)
        if execution["id"] != body["execution_id"]:
            raise Problem("stale_task_execution")
        refs = self._pin(principal, body["project_id"], request.artifact_refs, body["task_id"])
        resolved = {**correction, "state": "resolved", "resolution": {
            "summary": request.summary, "evidence_refs": refs, "run_id": request.run_id,
            "execution_id": execution["id"], "resolved_by_principal_id": principal["id"],
            "resolved_at": now(), "trust": "self_reported", "objective": False}}
        saved = self.store.save(CORRECTION_KIND, self.store.raw(correction_id)["project"],
                                self.store.raw(correction_id)["owner"], resolved,
                                expected=request.expected_revision)
        corrections = body.get("corrections") or {"cycles": 0, "open_ids": [], "resolved_ids": []}
        updated = {
            **body, "status": "pending", "evidence_refs": self._merge(body["evidence_refs"], refs),
            "checks": [], "check_history": [*body.get("check_history", []), body.get("checks", [])][-5:],
            "corrections": {
                **corrections,
                "open_ids": [value for value in corrections["open_ids"] if value != correction_id],
                "resolved_ids": [*corrections["resolved_ids"], correction_id][-20:],
            },
            "correction_history": [*body.get("correction_history", []), {
                "correction_id": correction_id, "event": "resolved", "at": now(),
                "resolved_by_principal_id": principal["id"]}][-20:],
        }
        self._save(VERIFICATION_KIND, updated, body["revision"])
        return saved

    def begin_correction(self, principal, verification_id, request):
        body = self.store.get(verification_id, principal, VERIFICATION_KIND)
        if body["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if body["status"] != "needs_corrections" or not body["corrections"]["open_ids"]:
            raise Problem("correction_not_open")
        self.service.represent(principal, body["agent_id"])
        task = self.store.get(body["task_id"], principal, "task")
        run, execution = self._task_execution(principal, task, request.run_id, body["execution_id"])
        if run.get("task_id") not in (None, task["id"]):
            raise Problem("run_busy")
        self.service.task_permit(principal, run["id"], task)
        attempts = dict(body.get("automation_attempts") or {})
        identifier = body["corrections"]["open_ids"][0]
        if attempts.get(identifier, 0) >= 3:
            raise Problem("correction_attempt_budget_exhausted")
        attempts[identifier] = attempts.get(identifier, 0) + 1
        self._save(VERIFICATION_KIND, {**body, "automation_attempts": attempts}, body["revision"])
        task["status"] = "running"
        row = self.store.raw(task["id"])
        self.store.save("task", row["project"], row["owner"], task, expected=task["revision"])
        run["task_id"] = task["id"]
        row = self.store.raw(run["id"])
        self.store.save("run", row["project"], row["owner"], run, expected=run["revision"])
        return self.store.get(task["id"], principal, "task")


# ---------------------------------------------------------------------------
# Installation
# ---------------------------------------------------------------------------

def _default_envelope(data):
    return {"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"), "data": data}


def _default_command(service, envelope):
    def command(principal, operation, key, request, function):
        payload = request.model_dump(exclude_none=True) if isinstance(request, m.Model) else request
        return envelope(service.mutate(principal, operation, key, payload, function))
    return command


def _default_principal(store):
    def principal(request: Request):
        authorization = request.headers.get("authorization", "")
        if not authorization.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        token_hash = hashlib.sha256(authorization[7:].encode()).hexdigest()
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?", (token_hash,)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)
    return principal


def _viewed(result, view, principal):
    data = result.get("data") if isinstance(result, dict) else None
    if isinstance(data, dict) and "id" in data:
        result = {**result, "data": view(principal, data)}
    return result


def install(app, service=None, principal=None, command=None, envelope=None, *, verifier=None):
    """Register verification routes on ``app``.

    ``service``, ``principal``, ``command``, and ``envelope`` mirror the collaborators used
    by the API factory, so the host application can pass its own closures. All of them are
    optional: sensible self-contained defaults are created from ``app.state.service``.
    ``verifier`` is an optional :class:`TrustedVerifier`; it is also stored on
    ``app.state.trusted_verifier`` so it can be replaced at runtime. Returns the domain.
    """
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    service = service if service is not None else app.state.service
    envelope = envelope or _default_envelope
    command = command or _default_command(service, envelope)
    principal = principal or _default_principal(service.store)
    app.state.verification = Verifications(service, verifier)
    if verifier is not None:
        app.state.trusted_verifier = verifier
    domain = app.state.verification

    def current_verifier():
        return getattr(app.state, "trusted_verifier", None) or domain.verifier
    @app.post("/v0.1/verifications/{verification_id}:begin-correction")
    async def verification_begin(verification_id: str, body: CorrectionBegin, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return command(p, "verification-begin-correction:" + verification_id, idempotency_key, body,
                       lambda: domain.begin_correction(p, verification_id, body))

    @app.post("/v0.1/verifications")
    async def verification_submit(body: VerificationSubmit, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return _viewed(command(p, "verification-submit", idempotency_key, body,
                               lambda: domain.submit(p, body)), domain.view, p)

    @app.get("/v0.1/verifications")
    async def verification_list(project_id: str, task_id: str | None = Query(None),
                                status: str | None = Query(None), p=Depends(principal)):
        return envelope(domain.list(p, project_id, task_id, status))

    @app.get("/v0.1/verifications/{verification_id}")
    async def verification_get(verification_id: str, p=Depends(principal)):
        return envelope(domain.get(p, verification_id))

    @app.post("/v0.1/verifications/{verification_id}:update")
    async def verification_update(verification_id: str, body: VerificationUpdate, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return _viewed(command(p, "verification-update:" + verification_id, idempotency_key, body,
                               lambda: domain.update_submission(p, verification_id, body)), domain.view, p)

    @app.post("/v0.1/verifications/{verification_id}:assign-reviewer")
    async def verification_assign(verification_id: str, body: ReviewerAssignment, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return _viewed(command(p, "verification-assign:" + verification_id, idempotency_key, body,
                               lambda: domain.assign_reviewer(p, verification_id, body)), domain.view, p)

    @app.post("/v0.1/verifications/{verification_id}:check")
    async def verification_check(verification_id: str, body: TrustedCheck, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return _viewed(command(p, "verification-check:" + verification_id, idempotency_key, body,
                               lambda: domain.record_check(p, verification_id, body, current_verifier())),
                       domain.view, p)

    @app.post("/v0.1/verifications/{verification_id}:decide")
    async def verification_decide(verification_id: str, body: ReviewDecision, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return _viewed(command(p, "verification-decide:" + verification_id, idempotency_key, body,
                               lambda: domain.decide(p, verification_id, body)), domain.view, p)

    @app.get("/v0.1/verifications/{verification_id}/corrections")
    async def verification_corrections(verification_id: str, p=Depends(principal)):
        return envelope(domain.corrections(p, verification_id))

    @app.get("/v0.1/corrections/{correction_id}")
    async def correction_get(correction_id: str, p=Depends(principal)):
        return envelope(domain.correction(p, correction_id))

    @app.post("/v0.1/corrections/{correction_id}:resolve")
    async def correction_resolve(correction_id: str, body: CorrectionResolve, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return _viewed(command(p, "correction-resolve:" + correction_id, idempotency_key, body,
                               lambda: domain.resolve_correction(p, correction_id, body)),
                       domain.correction_view, p)

    return domain
