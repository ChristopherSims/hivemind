"""Opt-in repository-driven memory lifecycle: change review, contradiction candidates,
and reminder/expiry policies.

This extension watches **operator-configured, trusted repository bindings** and turns
observable changes into **review proposals and durable reminders** for the existing
memory lifecycle. It never rewrites semantic facts on generative opinion, never
quarantines authoritative memories on unknown evidence, and never deletes anything:
the only write it can perform on a memory is a **deterministic logical invalidation**
(a new immutable revision carrying `invalidation_reason`) of material pinned to the
*exact* superseded commit, and only when the binding explicitly opts in with
`auto_invalidate=True`. Every prior revision stays in the canonical version history.

Lock and idempotency discipline (every compute operation is two-phase):

1. **Snapshot** — one short `store.lock` section copies a bounded, authorized,
   exact-revision snapshot (principal role/project, binding state, memory
   ids/revisions/pins/texts).
2. **Compute** — encoder, detector, and git work run with **no** canonical store or
   journal lock held. Compute never takes authority from the snapshot.
3. **Validate-publish** — one short transaction re-fetches the operator principal,
   binding state revision, and every memory's current revision/visibility/validity.
   Anything that changed concurrently (revoked principal, edited/hidden evidence, a
   newer binding state) discards the computed result instead of publishing stale
   authority. Durable fingerprints dedup inside the publish transaction.

HTTP compute routes are marked `hivemind_detached_work` so the API middleware skips
its global request/store lock; they authenticate via the app's own principal
dependency (which holds `store.lock` itself) and run through `asyncio.to_thread`.
Idempotency keys are honored without wrapping computation in a transaction: the
intent/checksum is checked under a short lock before compute and persisted with the
result in a short transaction after compute; a replayed key returns the stored
response and a racing key discards the duplicate computation (whose deduped publishes
are no-ops).

Git access is read-only, bounded, and non-executing: commands run through
`process_boundary.run_bounded` (bounded stdout/stderr, wall-clock deadline, process
group cleanup) with `core.fsmonitor=false`, an empty `core.hooksPath`, no external
diff drivers, `GIT_CONFIG_NOSYSTEM=1`, a nulled global config, `GIT_OPTIONAL_LOCKS=0`,
`--literal-pathspecs`, and literal operator-configured arguments only. No fetch, no
hooks, no fsmonitor helper, no remote helpers, and absolute workspace paths are never
persisted. Changed-path evidence redacts configured secret markers to digests.

Contradiction candidates come from the injected encoder plus the deterministic
**lexical** heuristic (always labeled `lexical`, never authoritative), or from an
operator-injected, explicitly opt-in detector callable with a strict output contract.
The default `LocalHashingEncoder` is a lexical hasher, not a semantic model; an
injected semantic encoder is reported distinctly. Candidates trigger review proposals
only. No model or provider is loaded, downloaded, discovered, or paid for here; the
detector/encoder callbacks are trusted operator configuration supplied at install.
"""

import asyncio
import json
import math
import os
import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from fastapi import Depends, Header, Query
from fastapi.responses import JSONResponse
from pydantic import Field

from . import models as m
from .memory_pipeline import (LocalHashingEncoder, contradiction_signals, cosine,
                              currentness, memory_text, repository_identity)
from .models import utc_timestamp
from .process_boundary import run_bounded
from .storage import Problem, canonical, digest, now, uid

EVENT_KIND = "memory_lifecycle_event"
STATE_KIND = "memory_lifecycle_binding"
REMINDER_KIND = "memory_lifecycle_reminder"
VERSION = "memory-lifecycle-v1"
LEXICAL_METHOD = "lexical"
LEXICAL_LIMITATIONS = (
    "Lexical heuristic: token overlap plus negation/polarity/value cues only",
    "Not an authoritative semantic understanding; paraphrases and domain synonyms are missed",
    "Candidate only: a human/admin/observer review decision is required before any action",
)
DETECTOR_LIMITATIONS = (
    "Operator-injected detector output is advisory and unverified",
    "Candidate only: a human/admin/observer review decision is required before any action",
)
DEFAULT_SECRET_PATH_MARKERS = (".env", "secret", "credential", "private_key", ".pem",
                               ".key", "id_rsa", "token")
MAX_DIMENSIONS = 4096
MAX_TRACKED = 500

OPERATION_DESCRIPTORS = (
    {"name": "source_change",
     "purpose": "Record an observed change in a trusted repository binding (new commit, "
                "branch move, rewrite, or working-tree dirtiness) with bounded, "
                "secret-redacted path evidence and the repository-pinned memories it affects.",
     "input": "Bounded read-only git observation (rev-parse/status/diff/merge-base/cat-file "
              "via run_bounded with fsmonitor/hooks/external diff disabled) plus the persisted "
              "binding baseline.",
     "output": "memory_lifecycle_event resource with from/to commit, rewrite flag, bounded "
               "repo-relative changed paths, and per-memory actions.",
     "authority": "Configured operator principal (admin/human), re-fetched and re-validated in "
                  "the publish transaction; events inherit the privacy scope of affected memories.",
     "side_effects": "May write deterministic logical invalidations of exactly commit-bound "
                     "memories only when the binding sets auto_invalidate=True; may notify the "
                     "configured agent. Never deletes data, never runs hooks, never fetches.",
     "limits": "max_events_per_tick, max_diff_paths, max_path_bytes, max_git_output_bytes, "
               "git timeout deadline, process group cleanup.",
     "retry_safety": "Durable fingerprint per observed state transition; concurrent changes "
                     "discard the stale computation; safe to retry."},
    {"name": "contradiction_review",
     "purpose": "Propose human review for semantic memory pairs that lexical signals or the "
                "operator-injected detector flag as potentially contradictory.",
     "input": "Exact-revision snapshots of semantic memories in one project/scope; encoder "
              "vectors and detector verdicts are computed without holding any store lock.",
     "output": "memory_lifecycle_event resource with confidence, similarity, signals, "
               "evidence, encoder/detector identity, and explicit limitations.",
     "authority": "Calling principal's project access, re-validated at publish; pairs whose "
                  "revision, visibility, or activity changed during compute are discarded; "
                  "cross-private-scope pairs are never correlated.",
     "side_effects": "None on memories: no dispute, quarantine, invalidation, or rewrite. "
                     "The lexical method is labeled lexical and never claims authoritative "
                     "semantic understanding.",
     "limits": "contradiction_scan_limit memories (<=64), bounded pair budget, strictly "
               "validated detector output.",
     "retry_safety": "Fingerprint per (left, right, revisions, method); repeats and racing "
                     "publishes dedup."},
    {"name": "expiry_reminder / expired_review / stale_review",
     "purpose": "Schedule durable review reminders before explicit UTC expiry fences, after "
                "expiry, or after a configured review age.",
     "input": "Memory expires_at / validity.valid_until / created_at versus the current clock "
              "and the configured lead/cooldown windows.",
     "output": "memory_lifecycle_reminder resource plus a scoped private notification when a "
               "recipient exists.",
     "authority": "Calling principal's project access, re-validated at publish; notifications "
                  "go only to the memory's private agent owner or the configured notify agent.",
     "side_effects": "Notifications and reminder records only. Logical expiry itself is "
                     "enforced by the store's explicit fences; nothing is physically deleted.",
     "limits": "max_reminders_per_tick, reminder cooldown bucket, notification TTL.",
     "retry_safety": "Fingerprint per (memory, revision, kind, cooldown bucket); expired_review "
                     "is once per revision; repeats within a bucket dedup."},
    {"name": "logical_invalidation",
     "purpose": "Deterministically invalidate a memory pinned to the exact superseded commit "
                "when the binding explicitly allows it.",
     "input": "Repository pin equality (validity.observed_commit == previous HEAD) re-checked "
              "against the current revision inside the publish transaction.",
     "output": "One new immutable memory revision with invalidation_reason; prior revisions "
               "remain in the canonical version history.",
     "authority": "Configured operator principal (admin/human), re-fetched at publish; "
                  "disputed, inactive, rewritten, or concurrently modified memories are "
                  "skipped and remain review proposals.",
     "side_effects": "Sets invalidation_reason on a new revision only. No semantic rewrite, "
                     "no deletion.",
     "limits": "Exactly version/source-bound material only; opt-in per binding.",
     "retry_safety": "Optimistic revision check; already-invalidated memories are skipped."},
)


@dataclass(frozen=True)
class RepositoryBinding:
    """Trusted, operator-configured repository binding. Paths never leave the process."""

    project_id: str
    workspace: str
    branch: str | None = None
    notify_agent_id: str | None = None
    auto_invalidate: bool = False
    label: str | None = None

    def __post_init__(self):
        if not isinstance(self.project_id, str) or not self.project_id.strip():
            raise ValueError("Invalid repository binding: project_id required")
        if not isinstance(self.workspace, str) or not self.workspace.strip():
            raise ValueError("Invalid repository binding: workspace required")
        if self.branch is not None and (not isinstance(self.branch, str) or not self.branch.strip()):
            raise ValueError("Invalid repository binding: branch")
        if not isinstance(self.auto_invalidate, bool):
            raise ValueError("Invalid repository binding: auto_invalidate must be a strict boolean")
        if self.notify_agent_id is not None and not isinstance(self.notify_agent_id, str):
            raise ValueError("Invalid repository binding: notify_agent_id")
        if self.label is not None and (not isinstance(self.label, str)
                                       or len(self.label.encode("utf-8")) > 256):
            raise ValueError("Invalid repository binding: label")


@dataclass(frozen=True)
class LifecyclePolicy:
    """Install-time policy. Disabled by default; every limit is bounded and finite."""

    enabled: bool = False
    operator_principal_id: str | None = None
    notify_agent_id: str | None = None
    reminder_lead_seconds: float = 86400
    reminder_cooldown_seconds: float = 43200
    review_after_seconds: float | None = None
    notification_ttl_seconds: float = 86400
    max_events_per_tick: int = 32
    max_reminders_per_tick: int = 16
    max_diff_paths: int = 64
    max_path_bytes: int = 256
    max_git_output_bytes: int = 262144
    contradiction_scan_limit: int = 32
    contradiction_similarity: float = 0.6
    interval_seconds: float = 60
    git_timeout_seconds: float = 5.0
    secret_path_markers: tuple = DEFAULT_SECRET_PATH_MARKERS

    def __post_init__(self):
        if not isinstance(self.enabled, bool):
            raise ValueError("Invalid memory lifecycle configuration: enabled must be boolean")
        finite = all(isinstance(value, (int, float)) and not isinstance(value, bool)
                     and math.isfinite(value) for value in
                     (self.reminder_lead_seconds, self.reminder_cooldown_seconds,
                      self.notification_ttl_seconds, self.interval_seconds,
                      self.git_timeout_seconds, self.contradiction_similarity))
        if not finite or (self.review_after_seconds is not None and (
                not isinstance(self.review_after_seconds, (int, float))
                or not math.isfinite(self.review_after_seconds))):
            raise ValueError("Invalid memory lifecycle configuration: values must be finite")
        checks = (
            (60 <= self.reminder_lead_seconds <= 30 * 86400, "reminder_lead_seconds"),
            (60 <= self.reminder_cooldown_seconds <= 30 * 86400, "reminder_cooldown_seconds"),
            (self.review_after_seconds is None or 60 <= self.review_after_seconds <= 365 * 86400,
             "review_after_seconds"),
            (60 <= self.notification_ttl_seconds <= 30 * 86400, "notification_ttl_seconds"),
            (1 <= self.max_events_per_tick <= 200, "max_events_per_tick"),
            (1 <= self.max_reminders_per_tick <= 100, "max_reminders_per_tick"),
            (1 <= self.max_diff_paths <= 500, "max_diff_paths"),
            (32 <= self.max_path_bytes <= 1024, "max_path_bytes"),
            (4096 <= self.max_git_output_bytes <= 1048576, "max_git_output_bytes"),
            (2 <= self.contradiction_scan_limit <= 64, "contradiction_scan_limit"),
            (0.0 <= self.contradiction_similarity <= 1.0, "contradiction_similarity"),
            (1 <= self.interval_seconds <= 86400, "interval_seconds"),
            (1 <= self.git_timeout_seconds <= 60, "git_timeout_seconds"),
            (isinstance(self.secret_path_markers, tuple)
             and all(isinstance(item, str) and 1 <= len(item) <= 64
                     for item in self.secret_path_markers)
             and len(self.secret_path_markers) <= 32, "secret_path_markers"),
        )
        for valid, name in checks:
            if not valid:
                raise ValueError(f"Invalid memory lifecycle configuration: {name}")


class DetectorOutputInvalid(Exception):
    """Bounded reason code for rejected detector output. Raw output is never surfaced."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class LifecycleTick(m.Model):
    project_id: m.ID | None = None
    limit: int = Field(default=16, ge=1, le=100)


class ContradictionScan(m.Model):
    project_id: m.ID
    limit: int = Field(default=32, ge=2, le=64)


class ReminderRun(m.Model):
    project_id: m.ID
    limit: int = Field(default=16, ge=1, le=100)


class MemoryLifecycleAutomation:
    """Repository change detection, contradiction candidates, and reminder policies.

    Every public operation follows snapshot -> lock-free compute -> validate-publish.
    """

    def __init__(self, service, *, policy=None, bindings=(), encoder=None, detector=None,
                 time_source=None):
        self.service, self.store = service, service.store
        self.policy = policy or LifecyclePolicy()
        self.encoder = encoder or LocalHashingEncoder()
        self._encoder_injected = encoder is not None
        self.detector = detector
        self._time = time_source or time.time
        self._next_tick = 0
        self._git = None
        self._local = threading.local()
        self.bindings = {}
        for binding in bindings:
            self.bind(binding)
        self._tables()

    def bind(self, binding):
        """Register one trusted repository binding. Install-time configuration surface."""
        if not isinstance(binding, RepositoryBinding):
            raise Problem("invalid_repository_binding", 422)
        resolved = os.path.realpath(str(binding.workspace))
        key = digest([VERSION, "binding", binding.project_id, os.path.normcase(resolved),
                      binding.branch])
        self.bindings[key] = {"key": key, "project_id": binding.project_id,
                              "workspace": resolved, "branch": binding.branch,
                              "notify_agent_id": binding.notify_agent_id,
                              "auto_invalidate": binding.auto_invalidate,
                              "label": binding.label,
                              "state_id": "memlcstate_" + key[:24]}
        return key

    # ------------------------------------------------------------------ persistence

    def _tables(self):
        with self.store.lock:
            self.store.db.executescript("""
            CREATE TABLE IF NOT EXISTS memory_lifecycle_fingerprints(
              fingerprint TEXT PRIMARY KEY, kind TEXT NOT NULL, resource TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS memory_lifecycle_intents(
              principal TEXT NOT NULL, operation TEXT NOT NULL, key TEXT NOT NULL,
              hash TEXT NOT NULL, state TEXT NOT NULL, response TEXT, created_at TEXT NOT NULL,
              PRIMARY KEY(principal, operation, key));
            """)

    # ------------------------------------------------------- external invocation marks

    def _mark_invocation(self):
        """Record that an external callback/process (encoder, detector, git) ran.

        Drives the intent contract: a failure with zero invocations is authoritatively
        pre-invocation and the key is released for retry; any failure after an
        invocation leaves the intent durably held as outcome_unknown.
        """
        tracker = getattr(self._local, "tracker", None)
        if tracker is not None:
            tracker["count"] += 1

    def _save(self, kind, project, owner, body, private=None, expected=None):
        result = self.store.save(kind, project, owner, body, private=private, expected=expected)
        # Lifecycle resources are canonical but not retrieval-indexed.
        self.store.db.execute("DELETE FROM jobs WHERE resource=? AND kind IN ('embedding','graph')",
                              (body["id"],))
        return result

    def _create(self, principal, kind, data, private=None):
        self.service.project_access(principal, data["project_id"])
        return self._save(kind, data["project_id"], principal["id"],
                          {"id": uid(kind), "created_at": now(), **data}, private)

    def _fingerprint(self, kind, fingerprint):
        row = self.store.db.execute(
            "SELECT resource FROM memory_lifecycle_fingerprints WHERE fingerprint=? AND kind=?",
            (fingerprint, kind)).fetchone()
        return row["resource"] if row else None

    def _register(self, kind, fingerprint, resource):
        self.store.db.execute("INSERT OR IGNORE INTO memory_lifecycle_fingerprints VALUES(?,?,?,?)",
                              (fingerprint, kind, resource, now()))

    # ------------------------------------------------------------------------ guards

    def _require_enabled(self):
        if not self.policy.enabled:
            raise Problem("memory_lifecycle_disabled", 409)

    @staticmethod
    def _check_operator(principal):
        if principal["role"] not in ("admin", "human"):
            raise Problem("lifecycle_operator_required", 403)

    def _operator(self):
        """Re-fetch and re-validate the configured operator on every pass."""
        if not self.policy.operator_principal_id:
            raise Problem("lifecycle_operator_not_configured", 409)
        row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                    (self.policy.operator_principal_id,)).fetchone()
        if not row or row["role"] not in ("admin", "human"):
            raise Problem("lifecycle_operator_unavailable", 503)
        return dict(row)

    def _revalidate(self, principal):
        """Re-fetch the acting principal inside a publish transaction.

        A principal whose role/project/agent changed (or was revoked) since the
        snapshot has no authority from the old snapshot: publishing fails closed.
        """
        row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                    (principal["id"],)).fetchone()
        if not row or row["role"] != principal["role"] or row["project"] != principal["project"] \
                or row["agent"] != principal["agent"]:
            raise Problem("principal_authority_changed", 403)
        return dict(row)

    # ------------------------------------------------------- detached idempotent calls

    def detached_call(self, principal, operation, key, payload, compute, authorize=None):
        """Durable intent/checksum/result for long compute. Exactly one invocation owner.

        A short transaction durably reserves (principal, operation, key, payload hash)
        in `memory_lifecycle_intents` **before any** encoder/detector/git invocation:

        * state `reserved` — in progress, or orphaned by a crash. Simultaneous or later
          same-key requests get `compute_intent_in_progress` / `compute_outcome_unknown`
          (409) with **no** compute. An orphaned reservation is never silently
          re-opened: only an explicit new key authorizes new work.
        * state `done` — replays the stored result after re-checking the *current*
          authority (principal row re-fetched: role/project/agent must be unchanged;
          project access re-checked). No callback or process re-runs.
        * A failure raised **before** any external invocation (zero tracked calls) is
          authoritatively pre-invocation: the reservation is deleted and the same key
          may retry. Any failure at/after the first invocation (callback exception,
          timeout, publish revalidation) marks the intent `unknown` — durably held,
          never automatically replayed.
        """
        if not key or len(key) > 128:
            raise Problem("idempotency_key_required", 422)
        checksum = digest(payload)
        with self.store.transaction():
            row = self.store.db.execute(
                "SELECT * FROM memory_lifecycle_intents WHERE principal=? AND operation=? AND key=?",
                (principal["id"], operation, key)).fetchone()
            if row:
                if row["hash"] != checksum:
                    raise Problem("idempotency_conflict")
                if row["state"] == "done":
                    # Replay checks CURRENT authority: the principal row is re-fetched
                    # and the endpoint's own guard runs again, so a revoked role or
                    # project never receives cached output.
                    actor = self._revalidate(principal)
                    self.service.project_access(
                        actor, payload.get("project_id") or actor["project"])
                    if authorize:
                        authorize(actor)
                    return json.loads(row["response"])
                raise Problem("compute_intent_in_progress" if row["state"] == "reserved"
                              else "compute_outcome_unknown", 409)
            actor = self._revalidate(principal)
            if authorize:
                authorize(actor)
            self.store.db.execute(
                "INSERT INTO memory_lifecycle_intents VALUES(?,?,?,?,?,?,?)",
                (principal["id"], operation, key, checksum, "reserved", None, now()))
        tracker = {"count": 0}
        self._local.tracker = tracker
        try:
            result = compute()
        except BaseException:
            with self.store.transaction():
                if tracker["count"] == 0:
                    # Authoritatively pre-invocation: release the key for retry.
                    self.store.db.execute(
                        "DELETE FROM memory_lifecycle_intents WHERE principal=? AND operation=?"
                        " AND key=? AND state='reserved'", (principal["id"], operation, key))
                else:
                    self.store.db.execute(
                        "UPDATE memory_lifecycle_intents SET state='unknown' WHERE principal=?"
                        " AND operation=? AND key=? AND state='reserved'",
                        (principal["id"], operation, key))
            raise
        finally:
            self._local.tracker = None
        with self.store.transaction():
            self.store.db.execute(
                "UPDATE memory_lifecycle_intents SET state='done', response=? WHERE principal=?"
                " AND operation=? AND key=? AND state='reserved'",
                (canonical(result), principal["id"], operation, key))
        return result

    # ------------------------------------------------------------------ git (bounded)

    def _git_executable(self):
        if self._git is None:
            executable = shutil.which("git")
            if not executable:
                raise Problem("git_not_installed", 503)
            self._git = executable
        return self._git

    def _git_read(self, workspace, *arguments, allow=(0,)):
        """One allow-listed read-only git command: bounded output, deadline, group cleanup.

        fsmonitor helpers, hooks, external diff drivers, system/global config, and
        remote helpers are all disabled; argv is literal operator-trusted text only.
        """
        self._mark_invocation()
        environment = os.environ.copy()
        for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_CONFIG",
                     "GIT_CONFIG_PARAMETERS", "GIT_EXTERNAL_DIFF", "GIT_DIFF_OPTS"):
            environment.pop(name, None)
        environment.update({"GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0",
                            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                            "GIT_ALLOW_PROTOCOL": "file", "LC_ALL": "C"})
        argv = [self._git_executable(), "--no-optional-locks", "--literal-pathspecs",
                "-c", "core.fsmonitor=false", "-c", "core.hooksPath=", "-c", "diff.external=",
                "-c", "core.longpaths=true", "-C", str(workspace), *arguments]
        try:
            code, out, _err = run_bounded(argv, workspace, environment,
                                          self.policy.git_timeout_seconds,
                                          max_output=self.policy.max_git_output_bytes)
        except TimeoutError:
            raise Problem("git_observe_timeout", 503) from None
        except ValueError:
            raise Problem("git_observe_output_boundary", 503) from None
        except OSError:
            raise Problem("workspace_unavailable", 503) from None
        if code not in allow:
            raise Problem("workspace_unavailable", 503)
        return out.decode("utf-8", "replace")

    def _observe_workspace(self, workspace):
        """Read-only observation: HEAD snapshot plus a bounded dirty-file count."""
        if not Path(workspace).is_dir():
            raise Problem("workspace_unavailable", 503)
        top_level = self._git_read(workspace, "rev-parse", "--show-toplevel").strip()
        if not top_level:
            raise Problem("workspace_not_a_repository", 503)
        branch = self._git_read(workspace, "rev-parse", "--abbrev-ref", "HEAD").strip()
        commit = self._git_read(workspace, "rev-parse", "--verify", "HEAD", allow=(0, 1)).strip()
        status = self._git_read(workspace, "status", "--porcelain")
        lines = [line for line in status.splitlines() if line.strip()]
        return {"repository_id": repository_identity(top_level),
                "branch": None if branch in ("", "HEAD") else branch,
                "commit": commit or None,
                "dirty_files": min(len(lines), 1000), "dirty_truncated": len(lines) > 1000,
                "source": "git", "available": True, "read_only": True}

    def _commit_exists(self, workspace, commit):
        try:
            self._git_read(workspace, "cat-file", "-e", f"{commit}^{{commit}}")
            return True
        except Problem:
            return False

    def _is_ancestor(self, workspace, old, new):
        try:
            self._git_read(workspace, "merge-base", "--is-ancestor", old, new)
            return True
        except Problem:
            return False

    def _redact(self, path):
        lowered = path.casefold()
        if any(marker in lowered for marker in self.policy.secret_path_markers):
            return "redacted:" + digest([VERSION, "path", path])[:24]
        return path

    def _changed_paths(self, workspace, old, new):
        """Bounded, secret-redacted repo-relative changed path list. Read-only."""
        output = self._git_read(workspace, "diff", "--name-only", old, new)
        paths, redacted, total = [], 0, 0
        for line in output.splitlines():
            path = line.strip()
            if not path:
                continue
            total += 1
            cleaned = self._redact(path)
            if cleaned.startswith("redacted:"):
                redacted += 1
            if len(paths) < self.policy.max_diff_paths and \
                    len(cleaned.encode("utf-8")) <= self.policy.max_path_bytes:
                paths.append(cleaned)
        return paths, total, redacted, total > len(paths)

    # ------------------------------------------------------------------ encoder/detector

    def _encoder_identity(self):
        name = getattr(self.encoder, "name", type(self.encoder).__name__)
        return {"kind": "injected" if self._encoder_injected else "lexical",
                "name": name,
                "semantic": bool(self._encoder_injected),
                "note": None if self._encoder_injected else
                        "Default encoder is a lexical hasher, not a semantic model"}

    def _detector_identity(self):
        if self.detector is None:
            return None
        declared = getattr(self.detector, "name", None)
        return declared if isinstance(declared, str) and declared.strip() else type(self.detector).__name__

    def _encode(self, texts):
        """Lock-free compute. This thread never holds the store lock while encoding."""
        assert not self.store.lock._is_owned()
        self._mark_invocation()
        method = getattr(self.encoder, "vectors", None)
        try:
            vectors = method(list(texts)) if callable(method) else self.encoder(list(texts))
        except Problem:
            raise
        except Exception:
            raise Problem("similarity_encoder_unavailable", 503) from None
        if not isinstance(vectors, (list, tuple)) or len(vectors) != len(texts):
            raise Problem("similarity_encoder_invalid_output", 503)
        normalized = []
        for vector in vectors:
            if not isinstance(vector, (list, tuple)) or not 1 <= len(vector) <= MAX_DIMENSIONS:
                raise Problem("similarity_encoder_invalid_output", 503)
            values = []
            for value in vector:
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise Problem("similarity_encoder_invalid_output", 503)
                values.append(float(value))
            norm = math.sqrt(sum(value * value for value in values))
            if norm == 0.0:
                raise Problem("similarity_encoder_invalid_output", 503)
            normalized.append([value / norm for value in values])
        if len({len(vector) for vector in normalized}) != 1:
            raise Problem("similarity_encoder_invalid_output", 503)
        return normalized

    def _run_detector(self, left_text, right_text):
        """Validate the operator-injected detector contract. Lock-free; never trusts output."""
        assert not self.store.lock._is_owned()
        if self.detector is None:
            return None
        self._mark_invocation()
        try:
            result = self.detector(left_text, right_text)
        except Problem:
            raise
        except Exception:
            raise Problem("contradiction_detector_unavailable", 503) from None
        if not isinstance(result, dict) or not set(result) <= {"verdict", "confidence",
                                                               "evidence", "limitations"}:
            raise DetectorOutputInvalid("invalid_shape")
        if result.get("verdict") not in ("contradiction", "none", "unknown"):
            raise DetectorOutputInvalid("invalid_verdict")
        confidence = result.get("confidence", 0.5)
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) \
                or not math.isfinite(confidence):
            raise DetectorOutputInvalid("invalid_confidence")
        evidence = result.get("evidence", "")
        if not isinstance(evidence, str) or len(evidence.encode("utf-8")) > 1024:
            raise DetectorOutputInvalid("invalid_evidence")
        limitations = result.get("limitations", [])
        if not isinstance(limitations, list) or len(limitations) > 8 or any(
                not isinstance(item, str) or len(item.encode("utf-8")) > 256 for item in limitations):
            raise DetectorOutputInvalid("invalid_limitations")
        return {"verdict": result["verdict"], "confidence": float(confidence),
                "evidence": evidence, "limitations": list(limitations)}

    # ------------------------------------------------------------------ state tracking

    def _state(self, binding):
        row = self.store.db.execute("SELECT * FROM resources WHERE id=? AND kind=? AND deleted=0",
                                    (binding["state_id"], STATE_KIND)).fetchone()
        return (json.loads(row["body"]), row) if row else (None, None)

    def _memory_snapshot(self, memory):
        row = self.store.raw(memory["id"])
        return {"id": memory["id"], "revision": memory["revision"], "title": memory["title"],
                "text": memory_text(memory), "category": memory.get("category"),
                "scope_kind": memory.get("scope_kind"), "scope_owner_id": memory.get("scope_owner_id"),
                "private": bool(memory.get("private")), "private_agent": row["private_agent"],
                "ingestion_seq": memory.get("ingestion_seq", 0),
                "state": self.store.memory_state(memory),
                "created_at": memory.get("created_at"),
                "expires_at": memory.get("expires_at"),
                "valid_until": (memory.get("validity") or {}).get("valid_until")}

    # ------------------------------------------------------------------ repository pass

    def detect_changes(self, principal, binding_key=None, limit=None):
        """Two-phase repository pass: lock-free observe, short-transaction publish."""
        self._require_enabled()
        self._check_operator(principal)
        limit = self.policy.max_events_per_tick if limit is None else min(
            limit, self.policy.max_events_per_tick)
        # Phase 1 — short lock: copy the authorized binding-state snapshot.
        with self.store.lock:
            snapshots = []
            for binding in self.bindings.values():
                if binding_key and binding["key"] != binding_key:
                    continue
                self.service.project_access(principal, binding["project_id"])
                state, row = self._state(binding)
                snapshots.append({"binding": binding, "state": state,
                                  "state_revision": row["revision"] if row else None})
        # Phase 2 — no locks: bounded read-only git observation.
        for snapshot in snapshots:
            binding = snapshot["binding"]
            label = binding["label"] or binding["key"][:12]
            snapshot["label"] = label
            try:
                observed = self._observe_workspace(binding["workspace"])
                if binding["branch"] and observed.get("branch") not in (None, binding["branch"]):
                    snapshot["result"] = {"binding": label, "state": "other_branch_checked_out",
                                          "branch": observed.get("branch"), "events": []}
                    continue
                observation = {"repository_id": observed["repository_id"],
                               "branch": observed["branch"], "commit": observed["commit"],
                               "dirty_files": observed["dirty_files"], "dirty_unknown": False}
                snapshot["observation"] = observation
                previous = (snapshot["state"] or {}).get("observed")
                commit_changed = bool(previous) and previous.get("commit") != observation["commit"]
                rewritten, paths, path_total, redacted, truncated = False, [], 0, 0, False
                if commit_changed and previous.get("commit") and observation["commit"]:
                    rewritten = not self._is_ancestor(binding["workspace"], previous["commit"],
                                                      observation["commit"])
                    if not rewritten:
                        paths, path_total, redacted, truncated = self._changed_paths(
                            binding["workspace"], previous["commit"], observation["commit"])
                snapshot.update(commit_changed=commit_changed, rewritten=rewritten, paths=paths,
                                path_total=path_total, redacted=redacted, truncated=truncated)
            except Problem as error:
                snapshot["result"] = {"binding": label, "state": "unavailable",
                                      "reason": error.code, "events": []}
        # Phase 3 — one short transaction per binding: re-validate, then publish.
        results, produced = [], 0
        for snapshot in snapshots:
            if "result" in snapshot:
                results.append(snapshot["result"])
                continue
            if produced >= limit:
                results.append({"binding": snapshot["label"], "state": "skipped_limit"})
                continue
            with self.store.transaction():
                actor = self._revalidate(principal)
                result = self._publish_observation(actor, snapshot)
            results.append(result)
            produced += len(result.get("events", []))
        return {"results": results, "version": VERSION, "status_effect": "review_only"}

    def _publish_observation(self, principal, snapshot):
        """Publish one observation. Runs inside a short transaction with fresh authority."""
        binding, label = snapshot["binding"], snapshot["label"]
        self.service.project_access(principal, binding["project_id"])
        state, row = self._state(binding)
        # A concurrent pass (or binding removal) discards this computation.
        current_revision = row["revision"] if row else None
        if current_revision != snapshot["state_revision"]:
            return {"binding": label, "state": "superseded", "events": []}
        current_observed = (state or {}).get("observed")
        if current_observed != (snapshot["state"] or {}).get("observed"):
            return {"binding": label, "state": "superseded", "events": []}
        observation = snapshot["observation"]
        if state is None:
            body = {"id": binding["state_id"], "created_at": now(),
                    "project_id": binding["project_id"], "binding_key": binding["key"],
                    "label": binding["label"], "branch": binding["branch"],
                    "observed": observation, "baseline": True, "method": VERSION,
                    "status_effect": "none"}
            self._save(STATE_KIND, binding["project_id"], principal["id"], body)
            return {"binding": label, "state": "baseline", "observed": observation, "events": []}
        previous = state["observed"]
        if previous == observation:
            return {"binding": label, "state": "unchanged", "observed": observation, "events": []}
        fingerprint = digest([VERSION, "source_change", binding["key"], previous.get("commit"),
                              observation["commit"], observation["branch"],
                              observation["dirty_files"]])
        events = []
        if not self._fingerprint("event", fingerprint):
            affected = self._affected(principal, binding, previous, observation,
                                      snapshot["rewritten"])
            groups = {}
            for entry in affected:
                groups.setdefault(entry["private_agent"], []).append(entry)
            if not affected:
                groups[None] = []
            for private, entries in groups.items():
                event = self._create(principal, EVENT_KIND, {
                    "project_id": binding["project_id"], "type": "source_change",
                    "binding": {"repository_id": observation["repository_id"],
                                "branch": binding["branch"], "label": binding["label"]},
                    "from_commit": previous.get("commit"), "to_commit": observation["commit"],
                    "from_branch": previous.get("branch"), "to_branch": observation["branch"],
                    "commit_changed": snapshot["commit_changed"],
                    "branch_changed": previous.get("branch") != observation["branch"],
                    "rewritten": snapshot["rewritten"],
                    "dirty_files": observation["dirty_files"],
                    "dirty_unknown": observation["dirty_unknown"],
                    "changed_paths": snapshot["paths"],
                    "changed_path_count": snapshot["path_total"],
                    "redacted_paths": snapshot["redacted"],
                    "changed_paths_truncated": snapshot["truncated"],
                    "affected": [{key: value for key, value in entry.items()
                                  if key != "private_agent"} for entry in entries],
                    "fingerprint": fingerprint, "method": VERSION,
                    "status_effect": ("logical_invalidation"
                                      if any(entry["action"] == "logically_invalidated"
                                             for entry in entries) else "review_only")},
                    private=private)
                events.append(event)
            self._register("event", fingerprint, events[0]["id"])
            if binding["notify_agent_id"]:
                self._notify(principal, binding["project_id"], binding["notify_agent_id"],
                             "source_change", events[0], dedup=("source_change_notify", fingerprint))
        body = {**state, "observed": observation, "baseline": False, "last_change_at": now(),
                "last_event_fingerprint": fingerprint}
        self._save(STATE_KIND, binding["project_id"], row["owner"], body, expected=row["revision"])
        return {"binding": label, "state": "changed", "observed": observation,
                "rewritten": snapshot["rewritten"], "changed_paths": snapshot["path_total"],
                "redacted_paths": snapshot["redacted"],
                "events": [event["id"] for event in events]}

    def _affected(self, principal, binding, previous, observed, rewritten):
        """Fresh pinned-memory view, computed inside the publish transaction."""
        affected = []
        memories = self.store.list("memory", binding["project_id"], principal, MAX_TRACKED,
                                   review_memory=True)
        for memory in memories:
            validity = memory.get("validity") or {}
            if not validity.get("repository_id") \
                    or validity["repository_id"] != observed["repository_id"]:
                continue
            if binding["branch"] and validity.get("branch") not in (None, binding["branch"]):
                continue
            pinned_commit = validity.get("observed_commit")
            if not pinned_commit or pinned_commit == observed["commit"]:
                continue
            state, reasons = currentness(memory, {**observed, "source": "git"})
            if state != "drifted":
                continue
            row = self.store.raw(memory["id"])
            entry = {"memory_id": memory["id"], "memory_revision": memory["revision"],
                     "observed_commit": pinned_commit, "currentness": state,
                     "currentness_reasons": reasons, "action": "review_proposal",
                     "private_agent": row["private_agent"]}
            exactly_bound = pinned_commit == previous.get("commit")
            if exactly_bound and binding["auto_invalidate"] is True:
                try:
                    invalidated = self._invalidate(principal, memory, {
                        "binding_key": binding["key"], "from_commit": previous.get("commit"),
                        "to_commit": observed["commit"], "rewritten": rewritten,
                        "pinned_commit": pinned_commit, "memory_revision": memory["revision"]})
                except Problem:
                    invalidated = None
                if invalidated:
                    entry.update(action="logically_invalidated",
                                 invalidated_revision=invalidated["revision"])
            affected.append(entry)
        return affected

    def _invalidate(self, principal, memory, evidence):
        """Deterministic logical invalidation of exactly version/source-bound material.

        Runs inside the publish transaction against the freshly re-fetched revision.
        Writes one new immutable revision carrying `invalidation_reason`; history is
        preserved and nothing is deleted. Concurrent modification (revision conflict)
        or lost visibility discards the invalidation instead of publishing stale
        authority.
        """
        current = self.store.get(memory["id"], principal, "memory", review_memory=True)
        if current["revision"] != memory["revision"]:
            return None
        if self.store.memory_state(current) not in ("proposed", "verified"):
            return None
        row = self.store.raw(current["id"])
        body = {key: value for key, value in current.items() if key != "effective_status"}
        body.update(invalidation_reason="repository_commit_superseded", invalidated_at=now(),
                    invalidation=evidence)
        return self._save("memory", row["project"], row["owner"], body,
                          private=row["private_agent"], expected=current["revision"])

    # ------------------------------------------------------------------ contradictions

    @staticmethod
    def _comparable(left, right):
        return (left["id"] != right["id"] and left["scope_kind"] == right["scope_kind"]
                and left["scope_owner_id"] == right["scope_owner_id"]
                and left["private"] == right["private"])

    def scan_contradictions(self, principal, project_id, limit=None):
        """Two-phase pairwise contradiction candidates. Review proposals only."""
        self._require_enabled()
        limit = min(limit or self.policy.contradiction_scan_limit,
                    self.policy.contradiction_scan_limit)
        # Phase 1 — short lock: bounded, authorized, exact-revision snapshot, plus the
        # durable screen registry (deterministic source/revision/model-config identity).
        with self.store.lock:
            self.service.project_access(principal, project_id)
            memories = [memory for memory in
                        self.store.list("memory", project_id, principal, MAX_TRACKED,
                                        review_memory=True)
                        if memory.get("category") == "semantic"
                        and self.store.memory_state(memory) in ("proposed", "verified")]
            memories.sort(key=lambda item: item.get("ingestion_seq", 0), reverse=True)
            snapshots = [self._memory_snapshot(memory) for memory in memories[:limit]]
            screened = {row["fingerprint"] for row in self.store.db.execute(
                "SELECT fingerprint FROM memory_lifecycle_fingerprints WHERE kind='screen'"
            ).fetchall()}
        if len(snapshots) < 2:
            return {"candidates": [], "compared": len(snapshots), "pairs": 0, "screened": 0,
                    "detector": self._detector_identity(), "encoder": self._encoder_identity(),
                    "status_effect": "review_only"}
        # Phase 2 — no locks: encoder + detector compute on the snapshot copies.
        texts = [snapshot["text"] for snapshot in snapshots]
        vectors = self._encode(texts)
        planned, pairs, errors, screened_hits = [], 0, [], 0
        encoder_id = self._encoder_identity()
        for left_index in range(len(snapshots)):
            for right_index in range(left_index + 1, len(snapshots)):
                left, right = snapshots[left_index], snapshots[right_index]
                if not self._comparable(left, right):
                    continue
                if left["private_agent"] and right["private_agent"] and \
                        left["private_agent"] != right["private_agent"]:
                    continue  # never correlate two different private scopes
                similarity = round(cosine(vectors[left_index], vectors[right_index]), 6)
                if similarity < self.policy.contradiction_similarity:
                    continue
                pairs += 1
                signals = contradiction_signals(texts[left_index], texts[right_index])
                detector_result, screen = None, None
                if self.detector is not None:
                    # Durably reserve the pair's deterministic identity BEFORE invoking the
                    # detector: unchanged sources never re-pay, and a crash after the
                    # invocation leaves the pair held (never silently re-opened). The
                    # pure lexical path needs no reservation: it is deterministic,
                    # side-effect-free, and cheap to repeat.
                    screen = digest([VERSION, "screen", left["id"], left["revision"],
                                     right["id"], right["revision"],
                                     self._detector_identity(),
                                     (encoder_id["kind"], encoder_id["name"])])
                    if screen in screened:
                        screened_hits += 1
                        continue
                    with self.store.transaction():
                        if self._fingerprint("screen", screen):
                            screened_hits += 1
                            continue
                        self._register("screen", screen, "reserved")
                    try:
                        detector_result = self._run_detector(texts[left_index],
                                                             texts[right_index])
                    except DetectorOutputInvalid as error:
                        self._screen_outcome(screen, "unknown")
                        errors.append(error.reason)
                        continue
                    except BaseException:
                        self._screen_outcome(screen, "unknown")
                        raise
                lexical_hit = bool(signals)
                detector_hit = bool(detector_result and detector_result["verdict"] == "contradiction")
                if not lexical_hit and not detector_hit:
                    if screen:
                        self._screen_outcome(screen, "clear")
                    continue
                if detector_result and detector_result["verdict"] == "unknown" and not lexical_hit:
                    if screen:
                        self._screen_outcome(screen, "clear")
                    continue
                method = LEXICAL_METHOD if lexical_hit and not detector_hit else (
                    f"injected:{self._detector_identity()}" if detector_hit and not lexical_hit
                    else f"{LEXICAL_METHOD}+injected:{self._detector_identity()}")
                limitations = list(LEXICAL_LIMITATIONS if lexical_hit else DETECTOR_LIMITATIONS)
                if detector_hit:
                    limitations = list(dict.fromkeys(
                        limitations + list(DETECTOR_LIMITATIONS)
                        + (detector_result["limitations"] if detector_result else [])))
                confidence = round(max(similarity if lexical_hit else 0.0,
                                       detector_result["confidence"] if detector_hit else 0.0), 6)
                planned.append({"left": left, "right": right, "similarity": similarity,
                                "signals": signals, "method": method, "limitations": limitations,
                                "confidence": confidence, "screen": screen,
                                "evidence": (detector_result or {}).get("evidence", "")})
        # Phase 3 — one short transaction: re-fetch everything, discard stale, publish.
        candidates = []
        with self.store.transaction():
            actor = self._revalidate(principal)
            self.service.project_access(actor, project_id)
            for plan in planned:
                fresh = []
                for snapshot in (plan["left"], plan["right"]):
                    try:
                        current = self.store.get(snapshot["id"], actor, "memory",
                                                 review_memory=True)
                    except Problem:
                        current = None
                    if current is None or current["revision"] != snapshot["revision"] \
                            or self.store.memory_state(current) not in ("proposed", "verified"):
                        fresh = None
                        break
                    fresh.append(current)
                if fresh is None:
                    continue  # concurrent revoke/edit/source change discards the candidate
                left, right = fresh
                fingerprint = digest([VERSION, "contradiction", plan["left"]["id"],
                                      plan["left"]["revision"], plan["right"]["id"],
                                      plan["right"]["revision"], plan["method"]])
                if self._fingerprint("contradiction", fingerprint):
                    continue
                first, second = sorted(fresh, key=lambda entry: entry["id"])
                event = self._create(actor, EVENT_KIND, {
                    "project_id": project_id, "type": "contradiction_review",
                    "left_memory_id": first["id"], "right_memory_id": second["id"],
                    "left_revision": first["revision"], "right_revision": second["revision"],
                    "left_title": first["title"], "right_title": second["title"],
                    "similarity": plan["similarity"], "confidence": plan["confidence"],
                    "signals": plan["signals"], "evidence": plan["evidence"],
                    "method": plan["method"], "encoder": self._encoder_identity(),
                    "detector": self._detector_identity(), "limitations": plan["limitations"],
                    "fingerprint": fingerprint, "state": "proposed",
                    "status_effect": "review_only"},
                    private=plan["left"]["private_agent"] or plan["right"]["private_agent"])
                self._register("contradiction", fingerprint, event["id"])
                candidates.append(event)
                if plan["screen"]:
                    self.store.db.execute(
                        "UPDATE memory_lifecycle_fingerprints SET resource=? WHERE fingerprint=?"
                        " AND kind='screen'", (event["id"], plan["screen"]))
            # Pairs discarded as stale keep their screen reservation held: the detector
            # invocation happened and its deterministic identity is unchanged.
        return {"candidates": candidates, "compared": len(snapshots), "pairs": pairs,
                "screened": screened_hits,
                "detector_errors": sorted(set(errors)), "detector": self._detector_identity(),
                "encoder": self._encoder_identity(), "status_effect": "review_only",
                "note": "Contradiction candidates are review proposals only. They never dispute, "
                        "quarantine, invalidate, or rewrite a memory, and unknown evidence cannot "
                        "quarantine authoritative facts."}

    def _screen_outcome(self, fingerprint, outcome):
        """Record a screen reservation outcome (`clear`/`unknown`/event id)."""
        with self.store.transaction():
            self.store.db.execute(
                "UPDATE memory_lifecycle_fingerprints SET resource=? WHERE fingerprint=?"
                " AND kind='screen'", (outcome, fingerprint))

    # ---------------------------------------------------------------------- reminders

    def _notify(self, principal, project_id, agent_id, notification_type, subject, *, dedup=None,
                extra=None):
        if dedup and self._fingerprint("notification", dedup[1]):
            return None
        try:
            self.store.get(agent_id, principal, "agent")
        except Problem:
            return None
        notice = self.service.create(principal, "notification", {
            "project_id": project_id, "recipient_agent_id": agent_id, "type": "memory_lifecycle",
            "lifecycle_type": notification_type, "subject_id": subject["id"],
            "subject_kind": self.store.raw(subject["id"])["kind"],
            "subject_revision": subject["revision"], "state": "available",
            "expires_at": time.time() + self.policy.notification_ttl_seconds,
            **(extra or {})}, private=agent_id)
        if dedup:
            self._register("notification", dedup[1], notice["id"])
        return notice

    @staticmethod
    def _deadline(snapshot):
        deadlines = [value for value in (snapshot["expires_at"], snapshot["valid_until"]) if value]
        if not deadlines:
            return None, None
        parsed = [(utc_timestamp(value), value) for value in deadlines]
        return min(parsed)

    def _plan_reminder(self, snapshot, now_ts, bucket):
        """Pure computation over a snapshot copy. No store access, no locks."""
        if snapshot["state"] in ("rejected", "superseded", "invalidated"):
            return None
        deadline_ts, deadline = self._deadline(snapshot)
        kind, once = None, False
        if deadline_ts is not None:
            if deadline_ts <= now_ts:
                kind, once = "expired_review", True
            elif deadline_ts - now_ts <= self.policy.reminder_lead_seconds:
                kind = "expiry_reminder"
        elif (self.policy.review_after_seconds and snapshot["state"] in ("proposed", "verified")
              and now_ts - utc_timestamp(snapshot["created_at"]) >= self.policy.review_after_seconds):
            kind = "stale_review"
        if not kind:
            return None
        parts = [VERSION, "reminder", kind, snapshot["id"], snapshot["revision"]]
        if not once:
            parts.append(bucket)
        return {"snapshot": snapshot, "kind": kind, "deadline": deadline,
                "bucket": None if once else bucket, "fingerprint": digest(parts)}

    def run_reminders(self, principal, project_id, limit=None):
        """Two-phase reminder pass: lock-free planning, short-transaction publish."""
        self._require_enabled()
        limit = self.policy.max_reminders_per_tick if limit is None else min(
            limit, self.policy.max_reminders_per_tick)
        now_ts = self._time()
        bucket = int(now_ts // self.policy.reminder_cooldown_seconds)
        # Phase 1 — short lock: bounded snapshot of memory deadline fields.
        with self.store.lock:
            self.service.project_access(principal, project_id)
            memories = self.store.list("memory", project_id, principal, MAX_TRACKED,
                                       review_memory=True)
            snapshots = [self._memory_snapshot(memory) for memory in memories]
        # Phase 2 — no locks: pure planning.
        planned = []
        for snapshot in snapshots:
            plan = self._plan_reminder(snapshot, now_ts, bucket)
            if plan:
                planned.append(plan)
        # Phase 3 — one short transaction: re-validate freshness, dedup, publish.
        created = []
        with self.store.transaction():
            actor = self._revalidate(principal)
            self.service.project_access(actor, project_id)
            for plan in planned:
                if len(created) >= limit:
                    break
                snapshot = plan["snapshot"]
                try:
                    current = self.store.get(snapshot["id"], actor, "memory", review_memory=True)
                except Problem:
                    continue  # concurrently hidden/revoked: discard
                fresh = self._memory_snapshot(current)
                fresh_plan = self._plan_reminder(fresh, now_ts, bucket)
                if fresh_plan is None or fresh_plan["fingerprint"] != plan["fingerprint"]:
                    continue  # revision/deadline changed during compute: discard
                if self._fingerprint("reminder", plan["fingerprint"]):
                    continue
                row = self.store.raw(current["id"])
                reminder = self._create(actor, REMINDER_KIND, {
                    "project_id": project_id, "memory_id": current["id"],
                    "memory_revision": current["revision"], "memory_title": current["title"],
                    "reminder": plan["kind"], "deadline": plan["deadline"],
                    "memory_state": fresh["state"], "cooldown_bucket": plan["bucket"],
                    "fingerprint": plan["fingerprint"], "method": VERSION,
                    "status_effect": "review_only"}, private=row["private_agent"])
                self._register("reminder", plan["fingerprint"], reminder["id"])
                recipient = row["private_agent"] or self.policy.notify_agent_id
                notified = None
                if recipient:
                    notice = self._notify(actor, project_id, recipient, plan["kind"], reminder,
                                          dedup=("reminder_notify", plan["fingerprint"]),
                                          extra={"memory_id": current["id"],
                                                 "memory_revision": current["revision"],
                                                 "deadline": plan["deadline"]})
                    notified = notice["recipient_agent_id"] if notice else None
                created.append({**reminder, "notified_agent_id": notified})
        return {"reminders": created, "considered": len(snapshots), "cooldown_bucket": bucket,
                "status_effect": "review_only",
                "note": "Reminders schedule human review only. Expiry itself is logical, enforced "
                        "by the store's explicit UTC/version fences; nothing is deleted."}

    # ------------------------------------------------------------------------ tick

    def tick(self, principal=None, request=None):
        """One bounded pass: repository detection, contradiction scan, reminders.

        Each stage is independently two-phase; no stage holds a lock across git,
        encoder, or detector work.
        """
        self._require_enabled()
        operator = self._operator() if principal is None else principal
        self._check_operator(operator)
        request = request or LifecycleTick()
        with self.store.lock:
            projects = sorted({binding["project_id"] for binding in self.bindings.values()})
            if request.project_id:
                self.service.project_access(operator, request.project_id)
                if request.project_id not in projects:
                    projects.append(request.project_id)
        outcome = {"repositories": self.detect_changes(operator, limit=request.limit)["results"],
                   "contradictions": [], "reminders": [], "version": VERSION,
                   "status_effect": "review_only"}
        for project in projects[:8]:
            try:
                outcome["contradictions"].append(
                    self.scan_contradictions(operator, project,
                                             self.policy.contradiction_scan_limit))
            except Problem as error:
                outcome["contradictions"].append({"project_id": project, "error": error.code})
            try:
                outcome["reminders"].append(
                    self.run_reminders(operator, project, self.policy.max_reminders_per_tick))
            except Problem as error:
                outcome["reminders"].append({"project_id": project, "error": error.code})
        return outcome

    def scheduled_tick(self):
        """Host-driven background entry: bounded by interval, operator re-validated."""
        if not self.policy.enabled or not self.policy.operator_principal_id:
            return None
        if time.monotonic() < self._next_tick:
            return None
        self._next_tick = time.monotonic() + self.policy.interval_seconds
        with self.store.lock:
            row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                        (self.policy.operator_principal_id,)).fetchone()
            if not row or row["role"] not in ("admin", "human"):
                return None
            principal = dict(row)
        return self.tick(principal)

    # ------------------------------------------------------------------- read surfaces

    def events(self, principal, project_id, *, event_type=None, limit=50):
        self._require_enabled()
        self.service.project_access(principal, project_id)
        items = self.store.list(EVENT_KIND, project_id, principal, 1000)
        if event_type:
            items = [item for item in items if item.get("type") == event_type]
        counts = {}
        for item in items:
            counts[item.get("type", "unknown")] = counts.get(item.get("type", "unknown"), 0) + 1
        items.sort(key=lambda item: item.get("ingestion_seq", 0), reverse=True)
        return {"events": items[:limit], "counts": counts, "status_effect": "review_only"}

    def reminders(self, principal, project_id, *, limit=50):
        self._require_enabled()
        self.service.project_access(principal, project_id)
        items = self.store.list(REMINDER_KIND, project_id, principal, 1000)
        items.sort(key=lambda item: item.get("ingestion_seq", 0), reverse=True)
        return {"reminders": items[:limit], "count": len(items), "status_effect": "review_only"}

    def status(self, principal, project_id):
        self.service.project_access(principal, project_id)
        events = self.store.list(EVENT_KIND, project_id, principal, 1000)
        reminders = self.store.list(REMINDER_KIND, project_id, principal, 1000)
        return {
            "enabled": bool(self.policy.enabled), "version": VERSION,
            "encoder": self._encoder_identity(), "detector": self._detector_identity(),
            "operator_configured": bool(self.policy.operator_principal_id),
            "bindings": [{"project_id": binding["project_id"], "branch": binding["branch"],
                          "label": binding["label"], "auto_invalidate": binding["auto_invalidate"],
                          "notify": bool(binding["notify_agent_id"])}
                         for binding in self.bindings.values()],
            "policy": {"reminder_lead_seconds": self.policy.reminder_lead_seconds,
                       "reminder_cooldown_seconds": self.policy.reminder_cooldown_seconds,
                       "review_after_seconds": self.policy.review_after_seconds,
                       "max_events_per_tick": self.policy.max_events_per_tick,
                       "max_reminders_per_tick": self.policy.max_reminders_per_tick,
                       "max_git_output_bytes": self.policy.max_git_output_bytes,
                       "contradiction_scan_limit": self.policy.contradiction_scan_limit,
                       "contradiction_similarity": self.policy.contradiction_similarity},
            "events": {kind: sum(1 for item in events if item.get("type") == kind)
                       for kind in ("source_change", "contradiction_review")},
            "reminders": {kind: sum(1 for item in reminders if item.get("reminder") == kind)
                          for kind in ("expiry_reminder", "expired_review", "stale_review")},
            "status_effect": "review_only",
        }

    def descriptors(self):
        """Data-only operation descriptors consumed by the parent tool context."""
        return {"version": VERSION, "descriptors": [dict(item) for item in OPERATION_DESCRIPTORS]}


def install(app, service, principal, command, envelope, *, policy=None, bindings=(), encoder=None,
            detector=None, time_source=None, use_local_semantic=False):
    """Register the opt-in lifecycle routes on an existing service app.

    `principal`, `command`, `envelope` are the app's own dependencies. Compute routes
    (`tick`, `detect`, `scan-contradictions`, `reminders`) are marked
    `hivemind_detached_work` so the API middleware skips its global request/store
    lock: they authenticate through `principal` (which holds `store.lock` itself),
    run the two-phase operation on a worker thread, and persist idempotency
    intent/checksum/result in short transactions outside the computation.
    """
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    if type(use_local_semantic) is not bool:
        raise ValueError("Local semantic selection must be a boolean")
    if use_local_semantic:
        if encoder is not None:
            raise ValueError("Choose the loaded local encoder or an explicit encoder, not both")
        from .local_semantics import LocalSemanticEncoder
        encoder = LocalSemanticEncoder(service.indexes.encoder)
    automation = MemoryLifecycleAutomation(service, policy=policy, bindings=bindings,
                                           encoder=encoder, detector=detector,
                                           time_source=time_source)
    app.state.memory_lifecycle = automation
    if Problem not in getattr(app, "exception_handlers", {}):
        @app.exception_handler(Problem)
        async def lifecycle_problem_handler(request, exc):
            return JSONResponse({"protocol": "hivemind", "protocol_version": "0.1.0",
                                 "error": {"code": exc.code, "message": exc.code.replace("_", " "),
                                           "retryable": exc.status in (429, 503)}}, status_code=exc.status)

    async def run_detached(operation, body, p, key, compute, authorize):
        payload = body.model_dump(by_alias=True, exclude_none=True)
        result = await asyncio.to_thread(automation.detached_call, p, operation, key,
                                         payload, compute, authorize)
        return envelope(result)

    def operator_guard(actor):
        automation._require_enabled()
        automation._check_operator(actor)

    @app.post("/v1/memory-lifecycle:tick")
    async def lifecycle_tick(body: LifecycleTick, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        return await run_detached("memory-lifecycle-tick", body, p, idempotency_key,
                                  lambda: automation.tick(p, body), operator_guard)

    @app.post("/v1/memory-lifecycle:detect")
    async def lifecycle_detect(body: LifecycleTick, p=Depends(principal),
                               idempotency_key: str | None = Header(None)):
        return await run_detached("memory-lifecycle-detect", body, p, idempotency_key,
                                  lambda: automation.detect_changes(p, limit=body.limit),
                                  operator_guard)

    @app.post("/v1/memory-lifecycle:scan-contradictions")
    async def lifecycle_scan(body: ContradictionScan, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        return await run_detached("memory-lifecycle-scan-contradictions", body, p,
                                  idempotency_key,
                                  lambda: automation.scan_contradictions(p, body.project_id,
                                                                         body.limit),
                                  lambda actor: (automation._require_enabled(),
                                                 service.project_access(actor,
                                                                        body.project_id)))

    @app.post("/v1/memory-lifecycle:reminders")
    async def lifecycle_reminders(body: ReminderRun, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return await run_detached("memory-lifecycle-reminders", body, p, idempotency_key,
                                  lambda: automation.run_reminders(p, body.project_id,
                                                                   body.limit),
                                  lambda actor: (automation._require_enabled(),
                                                 service.project_access(actor,
                                                                        body.project_id)))

    # The middleware skips its global request/store lock only for these explicitly
    # two-phase handlers; they manage their own short snapshots/transactions.
    for endpoint in (lifecycle_tick, lifecycle_detect, lifecycle_scan, lifecycle_reminders):
        endpoint.hivemind_detached_work = True

    @app.get("/v1/memory-lifecycle:events")
    async def lifecycle_events(project_id: str, type: str = Query("", max_length=64),
                               limit: int = Query(50, ge=1, le=200), p=Depends(principal)):
        return envelope(automation.events(p, project_id, event_type=type or None, limit=limit))

    @app.get("/v1/memory-lifecycle:reminders")
    async def lifecycle_reminder_list(project_id: str, limit: int = Query(50, ge=1, le=200),
                                      p=Depends(principal)):
        return envelope(automation.reminders(p, project_id, limit=limit))

    @app.get("/v1/memory-lifecycle:status")
    async def lifecycle_status(project_id: str, p=Depends(principal)):
        return envelope(automation.status(p, project_id))

    @app.get("/v1/memory-lifecycle:descriptors")
    async def lifecycle_descriptors(p=Depends(principal)):
        return envelope(automation.descriptors())

    return automation
