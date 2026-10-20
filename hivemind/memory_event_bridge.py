"""Event-driven memory maintenance: canonical commit signals wake targeted lifecycle work.

This module replaces purely periodic memory maintenance with an event-driven bridge on
top of the existing (read-only here) primitives:

* ``Store.subscribe_commits`` delivers **minimal post-commit invalidations**
  (``{project_id: new_seq}``) after the store lock is released. The callback never
  publishes synchronously: it only records a dirty hint in a bounded in-memory queue
  and wakes the bridge worker. It never receives bodies or private content.
* A **single off-thread worker** coalesces dirty hints per project and runs a durable
  **watermark catch-up**: target-kind canonical resources (task progress/completion,
  verification decisions, artifacts, memory revisions, board posts) with
  ``seq > watermark`` are listed under a short lock, then two targeted maintenance
  phases run for exactly that project:
  1. **Repository detection** through the existing
     ``MemoryLifecycleAutomation.detect_changes`` (its own snapshot -> lock-free
     compute -> validate-publish pipeline and durable fingerprints apply unchanged, so
     the periodic ``scheduled_tick`` and this bridge never double-publish).
  2. **Supporting-source review**: memories whose exact pinned ``evidence_refs``
     (``{kind, id, revision}``) point at a changed/deleted canonical resource get a
     deterministic, durable decision: a new immutable revision carrying
     ``invalidation_reason="supporting_source_revision_changed"`` (only when the bridge
     policy opts in with ``auto_invalidate_supporting_refs=True``) or a
     current-authority ``memory_bridge_review`` request. Deterministic only: no
     encoder, contradiction, or model compute ever runs here. Refs are re-read inside
     the publish transaction; a memory whose own revision moved is skipped. Memories
     are found with a **bounded, paged, targeted SQL query** (keyset pagination, cursor
     persisted in the intent's detail) — never an unbounded whole-project scan — and
     the watermark advances only after every targeted decision in the batch is durable.
* **Durable intents + watermark**: every trigger fingerprint is claimed with an atomic
  ``INSERT OR IGNORE`` inside an explicit transaction (safe across Store instances);
  rows carry an owner token (instance id + pid). A live owner is never claimed or
  converted: same-process owners are fenced by an in-process live registry, foreign
  pids by best-effort liveness, and anything unprovable simply stays held. A crashed
  owner becomes a durable **unknown hold** — never replayed (no paid or side-effecting
  repeat) until an operator explicitly releases it. Release only *skips* the held
  trigger; the only compute that may follow is a deterministic git/source re-read —
  release can never authorize an uncertain paid replay, and the bridge never runs
  model compute at all.
* **Acceptance is atomic**: operator re-validation, the intent CAS
  (``state='running' AND owner=us``) and the watermark upsert
  (``seq = MAX(seq, advance)``, never a regression) happen in one transaction.
* **Local repositories**: native/external ref changes produce no canonical commit, so
  the bridge exposes a trusted **notifier seam** (watcher callable whose hints carry
  optional distinct trusted event ids, re-validated by real git observation) plus a
  **hardened bounded ref scan** fallback. The scan validates every raw path component
  *before* reading: the ``.git`` directory, ``HEAD``, ref targets, and ``packed-refs``
  must be plain files/directories — symlinks, junctions/reparse points, alternate data
  streams, absolute refs, and ``..`` traversal fail closed into a stable
  ``untrusted_ref`` marker (a trusted workspace binding never authorizes an
  agent-controlled linked ``.git``). ``.git``-file worktrees and bare repositories
  yield ``None``; watcher hints must then carry distinct event ids or they dedup (the
  scan cannot observe them, and nothing is inferred without a git re-read).
  ``detect_changes`` always re-reads git; the scan only gates.
* **Retry discipline**: a project whose drain hits a transient error is re-hinted with
  a bounded retry count, and every fallback cycle re-hints all known projects for a
  watermark catch-up even when no new commit arrives, so a dropped hint can never
  silently stall a project.

Lifecycle: ``install(...)`` subscribes and starts the worker only when both the bridge
policy and the lifecycle automation are enabled. ``close()`` is an explicit, bounded,
idempotent handle that unsubscribes the commit listener and joins the worker with
``join_timeout_seconds``; call it **before** ``Store.close()``. A repeat ``close()``
re-checks the actual thread so a stale ``drained=False`` result is refreshed once the
thread really exits. When ``drained`` is False the host must keep the store open until
the worker exits (the app's lifespan does exactly that for tracked detached work).
HTTP compute routes run through ``app.state.run_detached`` (shutdown-tracked threads),
never inline on the event loop.
"""

import asyncio
import hashlib
import itertools
import json
import math
import os
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from fastapi import Depends
from fastapi.responses import JSONResponse
from pydantic import Field

from . import models as m
from .storage import Problem, digest, now, uid

VERSION = "memory-event-bridge-v2"
REVIEW_KIND = "memory_bridge_review"
TARGET_KINDS = ("task", "artifact", "verification", "memory", "board_post")
SUPPORT_SOURCE_KINDS = ("task", "artifact", "memory", "board_post")
SUPPORT_REASON = "supporting_source_revision_changed"
MAX_WATCHER_EVENTS = 64
MAX_WATCHER_EVENT_ID = 128
MAX_TRACKED_ERRORS = 64

# In-process live-owner fence: exact evidence for same-process intent ownership.
_OWNERS_LOCK = threading.Lock()
_LIVE_OWNERS = set()

OPERATION_DESCRIPTORS = (
    {"name": "event_catchup",
     "purpose": "Advance a project's durable watermark over target-kind canonical commits "
                "(task progress/completion, verification, artifact, memory revision, board "
                "post) and run targeted repository-change review for that project's bindings.",
     "input": "Minimal post-commit project/seq invalidations from Store.subscribe_commits plus "
              "the durable watermark; never message bodies or private content.",
     "output": "Durable intent rows, watermark advances, and whatever review proposals the "
               "existing lifecycle automation publishes (source_change events, exact-commit "
               "logical invalidations for auto_invalidate bindings).",
     "authority": "Configured operator principal (admin/human), re-fetched on every pass and "
                  "re-validated inside the atomic accept transaction; a revoked operator stops "
                  "work without holds.",
     "side_effects": "Only via the lifecycle automation's own two-phase publish; the bridge "
                     "itself writes only watermarks and intent states.",
     "limits": "batch_limit, max_batches_per_drain, bounded pending queue with gap fallback, "
               "bounded retry re-hints.",
     "retry_safety": "Atomic claim per (project, fingerprint) with owner fencing; accepted "
                     "decisions are durable before the watermark moves (MAX upsert); crashed "
                     "owners leave unknown holds that are never replayed until released."},
    {"name": "supporting_source_review",
     "purpose": "Maintain memories whose exact pinned evidence_refs "
                "(task/artifact/memory/board_post id+revision) changed or were deleted while "
                "the memory revision stayed unchanged.",
     "input": "Bounded paged SQL over the exact changed ref ids from the catch-up batch; refs "
              "and memory rows are re-read inside each publish transaction.",
     "output": "Deterministic durable decision per (memory revision, ref, observed change): a "
               "memory_bridge_review request, or — only with "
               "auto_invalidate_supporting_refs=True — a new immutable memory revision with "
               "invalidation_reason=supporting_source_revision_changed. History is preserved.",
     "authority": "Configured operator, re-validated per decision; the event/revision inherits "
                  "the memory's private namespace; private/private mismatches across namespaces "
                  "are held generically without a derived public signal.",
     "side_effects": "Review resources are canonical but not retrieval-indexed; invalidations "
                     "enqueue normal index revalidation jobs via Store.save.",
     "limits": "support_page_limit, support_pages_per_drain, durable cursor in the intent "
               "detail; no whole-project scans, no model/encoder/contradiction compute.",
     "retry_safety": "Decision fingerprints are durable and idempotent; the watermark advances "
                     "only after every targeted decision in the batch is durable."},
    {"name": "repository_scan",
     "purpose": "Handle local repository ref changes that produce no canonical commit: trusted "
                "watcher hints and a hardened bounded ref scan trigger the same targeted "
                "detection.",
     "input": "Operator-injected watcher events (binding keys plus optional distinct trusted "
              "event ids) and hardened bounded reads of .git/HEAD, its ref target, "
              "refs/heads/<branch>, packed-refs. Every component is validated before reads: "
              "symlinks/junctions/reparse points, alternate data streams, absolute refs, and "
              "traversal fail closed into a stable untrusted marker. No hooks, no code "
              "execution, no inferred effects — detect_changes re-reads git.",
     "output": "The same source_change review proposals as canonical-triggered detection.",
     "authority": "Trusted operator configuration only; watcher hints are never taken as proof.",
     "side_effects": "None beyond the automation's deduped publishes.",
     "limits": "max_ref_bytes per ref file, bounded watcher batch per poll, bounded event-id "
               "length.",
     "retry_safety": "Fingerprint per (binding, observed ref digest, trusted event id); "
                     "duplicate hints dedup; unobservable (.git-file/bare) repositories "
                     "require distinct event ids."},
    {"name": "hold_release",
     "purpose": "Operator release of an unknown-outcome hold after a crashed/failed compute.",
     "input": "Project id and optional fingerprint digest.",
     "output": "Held intents marked skipped; catch-up advances past them without recompute. "
               "Release can never authorize an uncertain paid replay: the only later compute "
               "is a deterministic git/source re-read (or an explicit new operator trigger).",
     "authority": "Configured operator principal only.",
     "side_effects": "Skips recompute; previously accepted automation publishes remain deduped "
                     "by their own fingerprints.",
     "limits": "Operator-only; bounded to held rows of one project.",
     "retry_safety": "Idempotent; releasing twice is a no-op."},
)


@dataclass(frozen=True)
class BridgePolicy:
    """Install-time bridge policy. Disabled by default; every limit bounded and finite."""
    enabled: bool = False
    queue_capacity: int = 256
    batch_limit: int = 32
    max_batches_per_drain: int = 8
    heartbeat_seconds: float = 0.25
    fallback_scan_seconds: float = 300.0
    max_ref_bytes: int = 65536
    join_timeout_seconds: float = 5.0
    retry_limit: int = 3
    support_page_limit: int = 32
    support_pages_per_drain: int = 4
    auto_invalidate_supporting_refs: bool = False

    def __post_init__(self):
        for name in ("queue_capacity", "batch_limit", "max_batches_per_drain",
                     "max_ref_bytes", "retry_limit", "support_page_limit",
                     "support_pages_per_drain"):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"Invalid memory event bridge configuration: {name} must be integer")
        if not isinstance(self.enabled, bool):
            raise ValueError("Invalid memory event bridge configuration: enabled must be boolean")
        if not isinstance(self.auto_invalidate_supporting_refs, bool):
            raise ValueError("Invalid memory event bridge configuration: "
                             "auto_invalidate_supporting_refs must be boolean")
        finite = all(isinstance(value, (int, float)) and not isinstance(value, bool)
                     and math.isfinite(value) for value in
                     (self.heartbeat_seconds, self.fallback_scan_seconds,
                      self.join_timeout_seconds))
        if not finite:
            raise ValueError("Invalid memory event bridge configuration: values must be finite")
        checks = (
            (8 <= self.queue_capacity <= 65536, "queue_capacity"),
            (1 <= self.batch_limit <= 200, "batch_limit"),
            (1 <= self.max_batches_per_drain <= 64, "max_batches_per_drain"),
            (0.05 <= self.heartbeat_seconds <= 60, "heartbeat_seconds"),
            (1 <= self.fallback_scan_seconds <= 86400, "fallback_scan_seconds"),
            (1024 <= self.max_ref_bytes <= 1048576, "max_ref_bytes"),
            (0.0 <= self.join_timeout_seconds <= 60, "join_timeout_seconds"),
            (0 <= self.retry_limit <= 16, "retry_limit"),
            (1 <= self.support_page_limit <= 200, "support_page_limit"),
            (1 <= self.support_pages_per_drain <= 64, "support_pages_per_drain"),
        )
        for valid, name in checks:
            if not valid:
                raise ValueError(f"Invalid memory event bridge configuration: {name}")


class BridgeTrigger(m.Model):
    """Typed operator trigger: names a target only; evidence is always re-fetched."""
    project_id: m.ID
    binding_key: str | None = Field(default=None, max_length=128)


class HoldRelease(m.Model):
    project_id: m.ID
    fingerprint: str | None = Field(default=None, max_length=128)


class MemoryEventBridge:
    """Nonblocking commit subscription -> durable off-thread targeted maintenance."""

    def __init__(self, service, automation, *, policy=None, watcher=None, time_source=None):
        self.service, self.store, self.automation = service, service.store, automation
        self.policy = policy or BridgePolicy()
        if watcher is not None and not callable(watcher):
            raise ValueError("Repository watcher must be callable")
        self.watcher = watcher
        self._time = time_source or time.time
        self._instance = uid("bridge")
        self._state_lock = threading.Lock()
        self._pending = {}
        self._gap = False
        self._gap_count = 0
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = None
        self._unsubscribe = None
        self._closed = False
        self._close_result = None
        self._ref_digests = {}
        self._last_fallback = 0.0
        self._watcher_failures = 0
        self._watcher_rejected = 0
        self._worker_errors = 0
        self._last_errors = {}
        self._retries = {}
        self._tables()

    # ------------------------------------------------------------------ persistence
    def _tables(self):
        with self.store.lock:
            self.store.db.executescript("""
            CREATE TABLE IF NOT EXISTS memory_event_bridge_watermarks(
              project TEXT PRIMARY KEY, seq INTEGER NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS memory_event_bridge_intents(
              project TEXT NOT NULL, fingerprint TEXT NOT NULL, state TEXT NOT NULL,
              detail TEXT, owner TEXT, pid INTEGER, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(project, fingerprint));
            CREATE TABLE IF NOT EXISTS memory_event_bridge_decisions(
              fingerprint TEXT PRIMARY KEY, project TEXT NOT NULL, action TEXT NOT NULL,
              resource TEXT, created_at TEXT NOT NULL);
            """)

    # ------------------------------------------------------------------- lifecycle
    def _require_enabled(self):
        if not self.policy.enabled:
            raise Problem("memory_event_bridge_disabled", 409)

    def start(self):
        """Subscribe and start the worker. No-op (returns False) when not fully enabled."""
        with self._state_lock:
            if self._closed or self._thread is not None:
                return self._thread is not None
            if not (self.policy.enabled and self.automation.policy.enabled
                    and self.automation.policy.operator_principal_id):
                return False
            with _OWNERS_LOCK:
                _LIVE_OWNERS.add(self._instance)
            # Fallback cadence is measured from start; never a startup burst.
            self._last_fallback = self._time()
            self._unsubscribe = self.store.subscribe_commits(self._on_commit)
            self._thread = threading.Thread(target=self._run, name="memory-event-bridge",
                                            daemon=True)
            # Restart reconciliation: re-hint every known project so durable watermarks
            # catch up anything missed while no bridge was running. Cheap no-op when
            # current; crashed owners are discovered per-intent on contact (never
            # blanket-converted: a 'running' row with a live owner is left alone).
            for project in self._known_projects():
                self._pending[project] = 0
            self._thread.start()
            self._wake.set()
            return True

    def close(self):
        """Explicit bounded shutdown handle. Idempotent; call before Store.close().

        A repeat close after ``drained=False`` re-checks the real thread instead of
        serving a stale cached result.
        """
        with self._state_lock:
            if self._closed:
                result = dict(self._close_result)
                thread = self._thread
                if not result.get("drained") and thread is not None \
                        and not thread.is_alive():
                    result["drained"] = True
                    self._close_result = dict(result)
                return result
            self._closed = True
        unsubscribe, self._unsubscribe = self._unsubscribe, None
        if unsubscribe:
            unsubscribe()
        self._stop.set()
        self._wake.set()
        thread = self._thread
        drained = True
        if thread is not None and thread.is_alive():
            thread.join(self.policy.join_timeout_seconds)
            drained = not thread.is_alive()
        with _OWNERS_LOCK:
            _LIVE_OWNERS.discard(self._instance)
        with self._state_lock:
            self._close_result = {"closed": True, "drained": drained}
        return dict(self._close_result)

    # ------------------------------------------------------- commit callback (inline)
    def _on_commit(self, changes):
        """Minimal nonblocking dirty-flag record. Never publishes, never blocks, never raises."""
        try:
            with self._state_lock:
                if self._closed or not isinstance(changes, dict):
                    return
                for project, seq in changes.items():
                    if not isinstance(project, str) or not isinstance(seq, int):
                        continue
                    if project not in self._pending \
                            and len(self._pending) >= self.policy.queue_capacity:
                        # Bounded queue overflow: drop hints, remember the gap, and let
                        # the durable watermarks drive a full catch-up. Nothing is lost.
                        self._pending.clear()
                        self._gap = True
                        self._gap_count += 1
                        break
                    self._pending[project] = max(seq, self._pending.get(project, 0))
            self._wake.set()
        except Exception:
            pass

    def hint(self, project_id):
        """Trusted in-process wake hint for one project."""
        self._on_commit({project_id: 0})

    # ------------------------------------------------------------------ worker loop
    def _run(self):
        while not self._stop.is_set():
            self._wake.wait(self.policy.heartbeat_seconds)
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                self._drain()
                self._poll_watcher()
                self._maybe_fallback_scan()
            except Exception:
                self._worker_errors += 1

    def _operator(self, project=None):
        """Re-fetch the configured operator every pass; revocation stops work, no hold."""
        try:
            with self.store.lock:
                return self.automation._operator()
        except Problem as error:
            if project is not None:
                self._remember_error(project, error.code)
            raise

    def _remember_error(self, project, code):
        if len(self._last_errors) >= MAX_TRACKED_ERRORS:
            self._last_errors.clear()
        self._last_errors[project] = code

    def _known_projects(self):
        projects = {binding["project_id"] for binding in self.automation.bindings.values()}
        with self.store.lock:
            rows = self.store.db.execute(
                "SELECT project FROM memory_event_bridge_watermarks").fetchall()
        projects |= {row["project"] for row in rows}
        return sorted(projects)

    def _drain(self):
        with self._state_lock:
            gap, self._gap = self._gap, False
            pending, self._pending = self._pending, {}
        projects = self._known_projects() if gap else sorted(pending)
        for project in projects:
            if self._stop.is_set():
                with self._state_lock:
                    if not self._closed:
                        self._pending[project] = max(self._pending.get(project, 0), 1)
                break
            try:
                self._catch_up(project)
                self._retries.pop(project, None)
            except Problem as error:
                # Transient failures must not silently drop the project: re-hint with a
                # bounded retry count; the periodic fallback re-hints regardless.
                self._remember_error(project, error.code)
                self._retry(project)
            except Exception:
                self._worker_errors += 1
                self._retry(project)

    def _retry(self, project):
        attempts = self._retries.get(project, 0) + 1
        self._retries[project] = attempts
        if attempts <= self.policy.retry_limit:
            self.hint(project)

    # ------------------------------------------------- owner fencing (crash witness)
    @staticmethod
    def _pid_alive(pid):
        """Best-effort liveness; anything unprovable is treated as alive (hold, never claim).

        Never uses os.kill(pid, 0) on Windows: there it delivers CTRL_C_EVENT to the
        whole console process group (including this process), it is not a null probe.
        """
        if not isinstance(pid, int) or pid <= 0:
            return True
        if pid == os.getpid():
            return True
        if os.name == "nt":
            try:
                import ctypes
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                handle = kernel32.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION
                if handle:
                    kernel32.CloseHandle(handle)
                    return True
                return ctypes.get_last_error() == 5  # access denied: alive, else dead
            except Exception:
                return True  # cannot prove death: hold, never claim
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except (OSError, ValueError, PermissionError):
            # Platform cannot prove death (or refuses): hold, never claim.
            return True

    def _owner_live(self, row):
        with _OWNERS_LOCK:
            if row["owner"] in _LIVE_OWNERS:
                return True
        if row["pid"] == os.getpid():
            # Same process but no live instance: a prior closed/crashed instance.
            return False
        return self._pid_alive(row["pid"])

    # ------------------------------------------------------------- durable intents
    def _intent_begin(self, project, fingerprint, advance_to):
        """Atomically claim or classify a trigger fingerprint.

        Returns (decision, detail) where decision is compute/resume/inflight/hold/advance.
        All transitions run inside one explicit transaction, so separate Store instances
        (and processes) cannot interleave the check and the claim.
        """
        with self.store.transaction():
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO memory_event_bridge_intents"
                "(project, fingerprint, state, detail, owner, pid, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (project, fingerprint, "running", None, self._instance, os.getpid(),
                 now(), now()))
            row = self.store.db.execute(
                "SELECT * FROM memory_event_bridge_intents"
                " WHERE project=? AND fingerprint=?", (project, fingerprint)).fetchone()
            if cursor.rowcount:
                return "compute", None
            state = row["state"]
            if row["owner"] == self._instance and state == "running":
                # Our own live in-flight/paused intent. Only a paused pagination cursor
                # may resume; a plain concurrent claim of the same trigger is inflight.
                detail = self._cursor_of(row["detail"])
                return ("resume", detail) if detail is not None else ("inflight", None)
            if state == "running":
                if self._owner_live(row):
                    # A live owner (this or another process) is on it. Never claim,
                    # never convert, never recompute.
                    return "inflight", None
                # Proven-dead owner: crash witness. CAS to a durable unknown hold.
                self.store.db.execute(
                    "UPDATE memory_event_bridge_intents SET state='unknown', updated_at=?"
                    " WHERE project=? AND fingerprint=? AND state='running' AND owner=?",
                    (now(), project, fingerprint, row["owner"]))
                return "hold", None
            if state == "unknown":
                return "hold", None
            if advance_to is not None:
                self._set_watermark(project, advance_to)
            return "advance", None

    @staticmethod
    def _cursor_of(detail):
        try:
            data = json.loads(detail) if detail else None
        except ValueError:
            return None
        return data if isinstance(data, dict) and "cursor" in data else None

    def _set_watermark(self, project, seq):
        """Monotone upsert: a concurrent accept can never regress the watermark."""
        self.store.db.execute(
            "INSERT INTO memory_event_bridge_watermarks VALUES(?,?,?)"
            " ON CONFLICT(project) DO UPDATE SET seq=MAX(seq, excluded.seq),"
            " updated_at=excluded.updated_at",
            (project, seq, now()))

    def _intent_progress(self, project, fingerprint, cursor):
        """Persist the pagination cursor so a bounded drain can resume deterministically."""
        with self.store.transaction():
            self.store.db.execute(
                "UPDATE memory_event_bridge_intents SET detail=?, updated_at=?"
                " WHERE project=? AND fingerprint=? AND state='running' AND owner=?",
                (json.dumps({"cursor": cursor}), now(), project, fingerprint,
                 self._instance))

    def _intent_accept(self, project, fingerprint, advance_to, detail=None):
        """Atomic accept: fresh operator check + intent CAS + watermark upsert.

        Returns True only when this owner really transitioned the intent; a lost CAS
        (a live concurrent owner took over) does not advance the watermark.
        """
        with self.store.transaction():
            operator = self.automation._operator()  # fresh authority at the decision
            cursor = self.store.db.execute(
                "UPDATE memory_event_bridge_intents SET state='accepted', detail=?,"
                " updated_at=? WHERE project=? AND fingerprint=? AND state='running'"
                " AND owner=?",
                (detail, now(), project, fingerprint, self._instance))
            if not cursor.rowcount:
                return False
            if advance_to is not None:
                self._set_watermark(project, advance_to)
        self._last_operator = operator["id"]
        return True

    def _intent_fail(self, project, fingerprint, code):
        with self.store.transaction():
            self.store.db.execute(
                "UPDATE memory_event_bridge_intents SET state='unknown', detail=?,"
                " updated_at=? WHERE project=? AND fingerprint=? AND state='running'"
                " AND owner=?",
                (str(code)[:128], now(), project, fingerprint, self._instance))
        self._remember_error(project, "outcome_unknown_hold")

    # ------------------------------------------------------------------- catch-up
    def _catch_up(self, project):
        operator = self._operator(project)
        self._last_errors.pop(project, None)
        bindings = [binding for binding in self.automation.bindings.values()
                    if binding["project_id"] == project]
        placeholders = ",".join("?" for _ in TARGET_KINDS)
        for _ in range(self.policy.max_batches_per_drain):
            if self._stop.is_set():
                return
            with self.store.lock:
                row = self.store.db.execute(
                    "SELECT seq FROM memory_event_bridge_watermarks WHERE project=?",
                    (project,)).fetchone()
                watermark = row["seq"] if row else 0
                rows = self.store.db.execute(
                    f"SELECT id, kind, seq FROM resources WHERE project=? AND seq>?"
                    f" AND kind IN ({placeholders}) ORDER BY seq LIMIT ?",
                    (project, watermark, *TARGET_KINDS, self.policy.batch_limit)).fetchall()
            if not rows:
                return
            advance_to = rows[-1]["seq"]
            fingerprint = digest([VERSION, "catchup", project, watermark, advance_to,
                                  [row["id"] for row in rows]])
            decision, detail = self._intent_begin(project, fingerprint, advance_to)
            if decision == "advance":
                continue
            if decision == "inflight":
                # A live owner is on this exact trigger; retry on a later wake.
                return
            if decision == "hold":
                self._remember_error(project, "outcome_unknown_hold")
                return
            cursor = (detail or {}).get("cursor") if decision == "resume" else None
            try:
                published = 0
                for binding in bindings:
                    result = self.automation.detect_changes(operator,
                                                            binding_key=binding["key"])
                    published += sum(1 for item in result.get("results", [])
                                     if item.get("state") == "changed")
                support = self._review_supporting_refs(operator, project, rows, cursor,
                                                       fingerprint)
            except Problem as error:
                self._intent_fail(project, fingerprint, error.code)
                return
            except Exception:
                self._intent_fail(project, fingerprint, "unexpected_error")
                return
            if support == "continue":
                return  # cursor persisted; re-hinted for the next drain
            if not self._intent_accept(project, fingerprint, advance_to,
                                       json.dumps({"events": published})):
                return  # lost the CAS to a live owner; nothing to advance
        self.hint(project)  # bounded per drain for responsiveness; more rows remain

    # --------------------------------------------- supporting-source maintenance
    def _review_supporting_refs(self, operator, project, rows, cursor, fingerprint):
        """Deterministic review of memories exactly pinned to changed/deleted refs.

        Bounded keyset-paged targeted SQL; the durable cursor lives in the intent
        detail so the watermark advances only after every targeted decision is durable.
        """
        refs = sorted({(row["kind"], row["id"]) for row in rows
                       if row["kind"] in SUPPORT_SOURCE_KINDS})
        if not refs:
            return "done"
        index, after = (cursor or {}).get("ref", 0), (cursor or {}).get("after", "")
        pages = 0
        while index < len(refs):
            if self._stop.is_set():
                return "continue"
            kind, ref_id = refs[index]
            while True:
                with self.store.lock:
                    memories = self.store.db.execute(
                        "SELECT id, revision, owner, private_agent, body FROM resources"
                        " WHERE kind='memory' AND project=? AND deleted=0 AND id>?"
                        " AND body LIKE ? ORDER BY id LIMIT ?",
                        (project, after, f"%{ref_id}%",
                         self.policy.support_page_limit)).fetchall()
                    source = self.store.db.execute(
                        "SELECT revision, deleted, private_agent FROM resources"
                        " WHERE id=?", (ref_id,)).fetchone()
                if not memories:
                    break
                for memory in memories:
                    self._review_one_support(operator, project, memory, kind, ref_id,
                                             source)
                after = memories[-1]["id"]
                pages += 1
                if pages >= self.policy.support_pages_per_drain:
                    self._intent_progress(project, fingerprint,
                                          {"ref": index, "after": after})
                    self.hint(project)
                    return "continue"
            index += 1
            after = ""
        return "done"

    def _review_one_support(self, operator, project, memory, ref_kind, ref_id, source):
        """One durable, idempotent, privacy-preserving decision for one memory/ref."""
        try:
            body = json.loads(memory["body"])
        except ValueError:
            return
        pinned = None
        for ref in body.get("evidence_refs") or []:
            if isinstance(ref, dict) and ref.get("kind") == ref_kind \
                    and ref.get("id") == ref_id:
                pinned = ref.get("revision")
                break
        if pinned is None:
            return  # unpinned refs are not exact-bound; nothing deterministic to do
        current = None if source is None or source["deleted"] else source["revision"]
        if current == pinned:
            return  # still exact
        observed = "deleted" if current is None else "changed"
        fingerprint = digest([VERSION, "support", memory["id"], memory["revision"],
                              ref_kind, ref_id, pinned, observed])
        ref_digest = digest([VERSION, "ref", ref_kind, ref_id, pinned, observed])
        memory_private = memory["private_agent"]
        source_private = None if source is None else source["private_agent"]
        # A private source cannot signal into a different (including shared) namespace.
        generic_hold = bool(source_private and memory_private != source_private)
        with self.store.transaction():
            self.automation._operator()  # fresh authority per decision
            if self.store.db.execute(
                    "SELECT 1 FROM memory_event_bridge_decisions WHERE fingerprint=?",
                    (fingerprint,)).fetchone():
                return
            fresh = self.store.db.execute(
                "SELECT id, revision, owner, deleted, private_agent, body FROM resources"
                " WHERE id=?", (memory["id"],)).fetchone()
            if not fresh or fresh["deleted"] or fresh["revision"] != memory["revision"]:
                return  # concurrent change: a later batch re-evaluates the new revision
            latest_source = self.store.db.execute(
                "SELECT revision, deleted, private_agent FROM resources WHERE id=?",
                (ref_id,)).fetchone()
            if (fresh["private_agent"] != memory_private
                    or (tuple(latest_source) if latest_source else None)
                    != (tuple(source) if source else None)):
                self.hint(project)
                return  # Never publish under a stale source namespace snapshot.
            try:
                state = self.store.memory_state(json.loads(fresh["body"]))
            except ValueError:
                return
            if state not in ("proposed", "verified"):
                # Already invalidated/disputed/expired: record the durable decision so
                # the watermark can advance, but never pile on more signals.
                self.store.db.execute(
                    "INSERT OR IGNORE INTO memory_event_bridge_decisions VALUES(?,?,?,?,?)",
                    (fingerprint, project, "skipped_state", None, now()))
                return
            action = resource = None
            if generic_hold:
                action = "held_generic"
            elif self.policy.auto_invalidate_supporting_refs:
                action, resource = self._invalidate_support(
                    project, fresh, ref_digest, pinned, observed)
            if action is None:
                # Invalidation not opted in or guarded out: deterministic review request.
                action, resource = self._review_request(
                    project, operator, fresh, ref_digest, pinned, observed)
            self.store.db.execute(
                "INSERT OR IGNORE INTO memory_event_bridge_decisions VALUES(?,?,?,?,?)",
                (fingerprint, project, action, resource, now()))

    def _invalidate_support(self, project, memory, ref_digest, pinned, observed):
        """Exact deterministic invalidation; None when guards require a review instead."""
        try:
            body = json.loads(memory["body"])
        except ValueError:
            return None, None
        if self.store.memory_state(body) not in ("proposed", "verified"):
            return None, None
        body.update(invalidation_reason=SUPPORT_REASON, invalidated_at=now(),
                    invalidation={"ref_digest": ref_digest, "pinned_revision": pinned,
                                  "observed": observed})
        self.store.save("memory", project, memory["owner"], body,
                        private=memory["private_agent"], expected=memory["revision"])
        return "invalidated", memory["id"]

    def _review_request(self, project, operator, memory, ref_digest, pinned, observed):
        body = {"id": uid(REVIEW_KIND), "created_at": now(), "project_id": project,
                "type": "supporting_source_review", "reason": SUPPORT_REASON,
                "memory_id": memory["id"], "memory_revision": memory["revision"],
                "ref_digest": ref_digest, "pinned_revision": pinned, "observed": observed,
                "trust": "deterministic",
                "limitations": ["Supporting source revision changed or the source was "
                                "deleted; the memory content itself was not re-evaluated",
                                "Review request only: no status change without a reviewer"],
                "status_effect": "review_only"}
        self.store.save(REVIEW_KIND, project, operator["id"], body,
                        private=memory["private_agent"])
        # Review requests are canonical but not retrieval-indexed.
        self.store.db.execute(
            "DELETE FROM jobs WHERE resource=? AND kind IN ('embedding','graph')",
            (body["id"],))
        return "review_requested", body["id"]

    # ------------------------------------------------- local repository observation
    @staticmethod
    def _is_plain_file(path):
        """Fail closed on links/junctions/reparse points; a trusted workspace binding
        never authorizes an agent-controlled linked .git component."""
        try:
            info = os.lstat(path)
        except OSError:
            return None
        if stat.S_ISLNK(info.st_mode):
            return False
        attributes = getattr(info, "st_file_attributes", 0)
        if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            return False
        return stat.S_ISREG(info.st_mode)

    def _read_bounded(self, path):
        if self._is_plain_file(path) is not True:
            return None
        try:
            with open(path, "rb") as handle:
                data = handle.read(self.policy.max_ref_bytes + 1)
        except OSError:
            return None
        if len(data) > self.policy.max_ref_bytes:
            return "oversized"
        return data

    @staticmethod
    def _valid_ref_name(ref):
        """Only plain relative refs inside .git: no traversal, ADS, or absolutes."""
        return isinstance(ref, str) and ref.startswith("refs/") and ".." not in ref \
            and "\\" not in ref and ":" not in ref and "//" not in ref \
            and not ref.endswith("/") and len(ref) <= 256

    def _ref_snapshot(self, binding):
        """Deterministic hardened digest of the binding's ref state. No exec, no hooks.

        Returns None when the repository is not observable through plain ref files
        (.git-file worktrees, bare repositories) — watcher hints then need distinct
        trusted event ids. Returns the stable 'untrusted_ref' marker when any component
        fails validation; the marker dedups but never leaks paths or error detail.
        """
        root = Path(binding["workspace"]) / ".git"
        try:
            info = os.lstat(root)
        except OSError:
            return None
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or (
                getattr(info, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
            return None if not stat.S_ISDIR(info.st_mode) else "untrusted_ref"

        def component(name):
            """Read one .git component, validating it is a plain file BEFORE the read."""
            plain = self._is_plain_file(root / name)
            if plain is False:
                return "untrusted"  # link/junction/reparse/non-regular: fail closed
            if plain is None:
                return None
            return self._read_bounded(root / name)

        head = component("HEAD")
        if head == "untrusted":
            return "untrusted_ref"
        packed = component("packed-refs")
        if packed == "untrusted":
            return "untrusted_ref"
        parts = [head, packed]
        refs = []
        if binding["branch"]:
            refs.append("refs/heads/" + binding["branch"])
        if isinstance(head, bytes) and head.startswith(b"ref: "):
            refs.append(head[5:].decode("utf-8", "replace").strip())
        for ref in dict.fromkeys(refs):
            if not self._valid_ref_name(ref):
                return "untrusted_ref"
            value = component(ref)
            if value == "untrusted":
                return "untrusted_ref"
            parts.append(value)
        return digest([part if isinstance(part, str) or part is None
                       else hashlib.sha256(part).hexdigest() for part in parts])

    def scan_binding(self, operator, binding, snapshot=None, event_id=None):
        """Targeted detection for one binding, deduped by observed refs + trusted event id."""
        if snapshot is None and event_id is None:
            snapshot = self._ref_snapshot(binding)
        fingerprint = digest([VERSION, "scan", binding["key"], snapshot, event_id or ""])
        decision, _ = self._intent_begin(binding["project_id"], fingerprint, None)
        if decision != "compute":
            if decision == "advance":
                self._ref_digests[binding["key"]] = snapshot
            return decision
        try:
            result = self.automation.detect_changes(operator, binding_key=binding["key"])
        except Problem as error:
            self._intent_fail(binding["project_id"], fingerprint, error.code)
            return "failed"
        except Exception:
            self._intent_fail(binding["project_id"], fingerprint, "unexpected_error")
            return "failed"
        published = sum(1 for item in result.get("results", []) if item.get("state") == "changed")
        self._intent_accept(binding["project_id"], fingerprint, None,
                            json.dumps({"events": published}))
        self._ref_digests[binding["key"]] = snapshot
        return "accepted"

    def _poll_watcher(self):
        """Trusted notifier seam: hints only; evidence is re-derived, never caller proof."""
        if self.watcher is None or self._stop.is_set():
            return
        try:
            events = list(itertools.islice(
                self.watcher(tuple(self.automation.bindings.values())), MAX_WATCHER_EVENTS))
        except Exception:
            self._watcher_failures += 1
            return
        for event in events:
            if self._stop.is_set():
                return
            key = event.get("binding_key") if isinstance(event, dict) else None
            binding = self.automation.bindings.get(key) if isinstance(key, str) else None
            event_id = event.get("event_id") if isinstance(event, dict) else None
            if isinstance(event_id, (int,)):
                event_id = str(event_id)
            if binding is None or (event_id is not None and (
                    not isinstance(event_id, str)
                    or not 1 <= len(event_id) <= MAX_WATCHER_EVENT_ID)):
                self._watcher_rejected += 1
                continue
            try:
                operator = self._operator(binding["project_id"])
            except Problem:
                continue
            self.scan_binding(operator, binding, snapshot=self._ref_snapshot(binding),
                              event_id=event_id)

    def _maybe_fallback_scan(self):
        """Backstop for missed notifications; not the sole or primary mechanism."""
        moment = self._time()
        if moment - self._last_fallback < self.policy.fallback_scan_seconds:
            return
        self._last_fallback = moment
        self.fallback_scan()

    def fallback_scan(self):
        try:
            operator = self._operator()
        except Problem:
            return
        for binding in tuple(self.automation.bindings.values()):
            if self._stop.is_set():
                return
            snapshot = self._ref_snapshot(binding)
            if self._ref_digests.get(binding["key"]) == snapshot:
                continue
            self.scan_binding(operator, binding, snapshot)
        # Watermark fallback: re-hint every known project so a dropped hint or a
        # transient error can never stall a project until the next commit arrives.
        for project in self._known_projects():
            self.hint(project)

    # ---------------------------------------------------------------- operator API
    def release_hold(self, principal, project_id, fingerprint=None):
        """Operator decision to skip recompute of held triggers. Idempotent.

        Release never authorizes an uncertain paid replay: held triggers are marked
        skipped, and the only compute that can follow is a deterministic git/source
        re-read via catch-up or an explicit new operator trigger.
        """
        self._require_enabled()
        self.automation._check_operator(principal)
        with self.store.transaction():
            self.service.project_access(principal, project_id)
            if fingerprint:
                cursor = self.store.db.execute(
                    "UPDATE memory_event_bridge_intents SET state='skipped', updated_at=?"
                    " WHERE project=? AND fingerprint=? AND state='unknown'",
                    (now(), project_id, fingerprint))
            else:
                cursor = self.store.db.execute(
                    "UPDATE memory_event_bridge_intents SET state='skipped', updated_at=?"
                    " WHERE project=? AND state='unknown'", (now(), project_id))
            released = cursor.rowcount
        self._last_errors.pop(project_id, None)
        self.hint(project_id)
        return {"released": released, "status_effect": "review_only"}

    def status(self, principal, project_id):
        """Counts, watermarks, and digests only: no bodies, paths, or private ids."""
        self._require_enabled()
        self.automation._check_operator(principal)
        with self.store.lock:
            self.service.project_access(principal, project_id)
            row = self.store.db.execute(
                "SELECT seq, updated_at FROM memory_event_bridge_watermarks WHERE project=?",
                (project_id,)).fetchone()
            holds = self.store.db.execute(
                "SELECT fingerprint, detail, updated_at FROM memory_event_bridge_intents"
                " WHERE project=? AND state='unknown' ORDER BY updated_at LIMIT 32",
                (project_id,)).fetchall()
            decisions = self.store.db.execute(
                "SELECT action, count(*) AS n FROM memory_event_bridge_decisions"
                " WHERE project=? GROUP BY action", (project_id,)).fetchall()
        with self._state_lock:
            pending = len(self._pending)
        return {"enabled": bool(self.policy.enabled), "version": VERSION,
                "watermark": row["seq"] if row else 0,
                "watermark_updated_at": row["updated_at"] if row else None,
                "pending": pending, "gap_count": self._gap_count,
                "worker": bool(self._thread and self._thread.is_alive()),
                "watcher": type(self.watcher).__name__ if self.watcher else None,
                "watcher_failures": self._watcher_failures,
                "watcher_rejected": self._watcher_rejected,
                "worker_errors": self._worker_errors,
                "last_error": self._last_errors.get(project_id),
                "fallback_scan_seconds": self.policy.fallback_scan_seconds,
                "decisions": {row["action"]: row["n"] for row in decisions},
                "holds": [{"fingerprint": hold["fingerprint"], "reason": hold["detail"],
                           "since": hold["updated_at"]} for hold in holds],
                "status_effect": "review_only"}

    def descriptors(self):
        """Data-only operation descriptors consumed by the parent tool context."""
        return {"version": VERSION, "descriptors": [dict(item) for item in OPERATION_DESCRIPTORS]}


def install(app, service, automation, principal, command, envelope, *, policy=None,
            watcher=None, time_source=None):
    """Register the event-driven bridge routes on an existing service app.

    `principal`, `command`, `envelope` are the app's own dependencies. The bridge starts
    only when its policy, the lifecycle automation, and the configured operator are all
    enabled/present. Compute routes run through `app.state.run_detached`
    (shutdown-tracked worker threads) and are marked `hivemind_detached_work` so the
    middleware skips its global request/store lock. `app.state.memory_event_bridge.
    close()` must run before `Store.close()`; the handle is explicit and bounded.
    """
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    bridge = MemoryEventBridge(service, automation, policy=policy, watcher=watcher,
                               time_source=time_source)
    app.state.memory_event_bridge = bridge
    bridge.start()
    if Problem not in getattr(app, "exception_handlers", {}):
        @app.exception_handler(Problem)
        async def bridge_problem_handler(request, exc):
            return JSONResponse({"protocol": "hivemind", "protocol_version": "0.1.0",
                                 "error": {"code": exc.code, "message": exc.code.replace("_", " "),
                                           "retryable": exc.status in (429, 503)}},
                                status_code=exc.status)

    run_detached = getattr(app.state, "run_detached", None) or asyncio.to_thread

    def operator_guard(actor):
        bridge._require_enabled()
        automation._check_operator(actor)

    @app.post("/v1/memory-events:trigger")
    async def bridge_trigger(body: BridgeTrigger, p=Depends(principal)):
        def compute():
            operator_guard(p)
            with bridge.store.lock:
                service.project_access(p, body.project_id)
            if body.binding_key:
                binding = automation.bindings.get(body.binding_key)
                if binding is None or binding["project_id"] != body.project_id:
                    raise Problem("not_found", 404)
                operator = bridge._operator(body.project_id)
                outcome = bridge.scan_binding(operator, binding)
                return {"project_id": body.project_id, "binding": binding["label"],
                        "outcome": outcome, "status_effect": "review_only"}
            bridge.hint(body.project_id)
            return {"project_id": body.project_id, "outcome": "queued",
                    "status_effect": "review_only"}
        return envelope(await run_detached(compute))

    @app.post("/v1/memory-events:release")
    async def bridge_release(body: HoldRelease, p=Depends(principal)):
        return envelope(await run_detached(
            bridge.release_hold, p, body.project_id, body.fingerprint))

    @app.get("/v1/memory-events:status")
    async def bridge_status(project_id: str, p=Depends(principal)):
        return envelope(await run_detached(bridge.status, p, project_id))
    @app.get("/v1/memory-events:reviews")
    async def bridge_reviews(project_id: str, limit: int = 100, p=Depends(principal)):
        def read():
            with bridge.store.lock:
                operator_guard(p)
                current = bridge.store.db.execute(
                    "SELECT * FROM principals WHERE id=?", (p["id"],)).fetchone()
                if not current or any(current[key] != p[key]
                                      for key in ("role", "project", "agent", "token_hash")):
                    raise Problem("authentication_required", 401)
                service.project_access(p, project_id)
                if not 1 <= limit <= 200:
                    raise Problem("invalid_limit", 422)
                entries = bridge.store.list(REVIEW_KIND, project_id, p, limit)
                authorized = []
                for entry in entries:
                    try:
                        memory = bridge.store.raw(entry["memory_id"], "memory")
                        if not bridge.store.visible(memory, p):
                            continue
                    except Problem:
                        continue
                    authorized.append(entry)
                return {"reviews": authorized, "limit": limit,
                        "status_effect": "review_only"}
        return envelope(await run_detached(read))

    @app.get("/v1/memory-events:descriptors")
    async def bridge_descriptors(p=Depends(principal)):
        return envelope(bridge.descriptors())

    # These handlers take store.lock in their own shutdown-tracked worker threads; the
    # middleware must skip its global request/store lock for them (same contract as the
    # lifecycle routes).
    for endpoint in (bridge_trigger, bridge_release, bridge_status, bridge_reviews):
        endpoint.hivemind_detached_work = True

    return bridge
