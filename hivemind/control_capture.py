"""Continuous filesystem-change capture, governed tool gateway, and safe pause/resume.

This module is additive and opt-in. It composes the existing capture spool
(:class:`hivemind.sdk.Capture`), the bounded scanner
(:func:`hivemind.runtime_lifecycle.scan_workspace`), and the cooperative control
boundary (:class:`hivemind.runtime_lifecycle.LifecycleBoundary`) without
modifying them.

Honesty guarantees:

* The watcher is a *polling observer*, not an OS sandbox and not a syscall
  interceptor. It emits hash-only identity (path, change, size, sha256, mtime)
  and never reads or emits excluded secrets, ``.git``, ``.env*``, private keys,
  or symlink/junction/reparse-point escapes. Events land in a durable bounded
  SQLite outbox whose rows are written in the same transaction as the scan
  cursor, so a crash never silently loses accepted observations; transfers into
  the capture spool use stable event IDs and are idempotent across crashes. A
  lost cursor, an over-full outbox, a truncated scan, or a truncated diff
  produces an explicit gap event, never a silent drop and never a fabricated
  "no changes" result.
* The tool gateway classifies tools from a canonical, administrator-controlled
  map. Model- or host-provided tool descriptions are *untrusted metadata* and
  never grant authority. Strict mode is operator-configured only; request
  bodies cannot relax it. Unknown tools and unsupported bypasses are denied
  when the policy is strict.
* Safe pause/resume only reports ``applied`` when a supported authoritative
  adapter (one declaring the action in its ``supported_controls`` capability
  set, or matching the documented vendor runtime contract) observed the
  required status *and* produced a quiescent checkpoint bound to the current
  run. Generic transport failures hold the intent as unknown, never failed.
  A resume consumes an applied-pause checkpoint exactly once; corrupt or
  foreign controller state refuses to load rather than enabling replays. No
  automatic replay of uncertain model output is ever performed.
"""

import json
import math
import os
import re
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import httpx
from fastapi import Depends
from filelock import FileLock
from pydantic import Field

from . import models as m
from .runtime_lifecycle import (FileAdapter, LifecycleBoundary, ScanLimits,
                                ServiceTransport, diff_snapshots, scan_workspace,
                                secret_path)
from .sdk import redact_value
from .storage import Problem, canonical, digest, now, uid

__all__ = [
    "READ", "WRITE", "EXECUTE", "DISCOVER", "UNKNOWN_ACTION",
    "ALLOW", "DENY", "HELD", "APPLIED", "FAILED", "UNKNOWN",
    "OPERATION_DESCRIPTORS",
    "OperationDescriptor",
    "WatcherPolicy", "WatchEvent", "ContinuousWatcher",
    "ToolDescriptor", "GatewayPolicy", "GatewayDecision", "ToolGateway",
    "ControlPolicy", "ControlIntent", "SafePauseController",
    "ToolEvaluateRequest", "install", "describe_tool", "from_hook_payload",
    "runtime_capabilities", "watcher_from_config", "controller_from_config",
]

READ = "read"
WRITE = "write"
EXECUTE = "execute"
DISCOVER = "discover"
UNKNOWN_ACTION = "unknown"

ALLOW = "allow"
DENY = "deny"

HELD = "held"
APPLIED = "applied"
FAILED = "failed"
UNKNOWN = "unknown"

# The service's required observed status before an applied result is accepted.
REQUIRED_STATUS = {"pause": "paused", "cancel": "cancelled", "resume": "running"}
SUPPORTED_ACTIONS = tuple(REQUIRED_STATUS)

MAX_INTENTS = 128
MAX_CONSUMED_CHECKPOINTS = 256
MAX_OUTBOX_ROWS = 10000

# Canonical tool classification. Names are matched case-insensitively; a tool
# absent from this map is UNKNOWN_ACTION and is denied under a strict policy.
# This map is authority: host/model descriptions can never widen it.
_TOOL_MAP = {
    "read": (READ, "filesystem"), "grep": (READ, "filesystem"),
    "glob": (READ, "filesystem"), "ls": (READ, "filesystem"),
    "websearch": (READ, "network"), "fetchurl": (READ, "network"),
    "toolsearch": (DISCOVER, "discovery"),
    "edit": (WRITE, "filesystem"), "write": (WRITE, "filesystem"),
    "create": (WRITE, "filesystem"), "applypatch": (WRITE, "filesystem"),
    "execute": (EXECUTE, "process"), "skill": (EXECUTE, "process"),
}


def _state_key(value):
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(value))[:128] or "control"


def _strict_bool(value, name):
    if type(value) is not bool:
        raise ValueError(f"{name} must be a boolean")
    return value


def _strict_int(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer between {low} and {high}")
    return value


def _strict_float(value, name, low, high):
    if type(value) not in (int, float) or isinstance(value, bool) or not math.isfinite(value) \
            or not low <= float(value) <= high:
        raise ValueError(f"{name} must be a finite number between {low} and {high}")
    return float(value)


def _reject_reparse_ancestors(path, anchor=None):
    """Reject symlink/junction/reparse-point escapes *before* any resolve.

    Every ancestor of ``path`` (up to ``anchor`` or the filesystem root) is
    checked with ``lstat`` semantics: ``os.path.islink`` plus the Windows
    ``FILE_ATTRIBUTE_REPARSE_POINT`` bit, which covers junctions that
    ``islink`` alone misses. Nothing is followed or resolved here.
    """
    target = Path(path)
    anchor = Path(anchor) if anchor is not None else None
    current = target
    checked = 0
    while True:
        try:
            stat = current.lstat()
        except OSError:
            stat = None
        if current.is_symlink():
            raise ValueError(f"Path ancestor is a symbolic link: {current}")
        if stat is not None and os.name == "nt":
            attributes = getattr(stat, "st_file_attributes", 0)
            if attributes & 0x400:  # FILE_ATTRIBUTE_REPARSE_POINT
                raise ValueError(f"Path ancestor is a reparse point: {current}")
        if anchor is not None and current == anchor:
            break
        parent = current.parent
        if parent == current or checked > 64:
            break
        current, checked = parent, checked + 1


# --------------------------------------------------------------------------
# Operation descriptors for the parent CLI / worker wiring
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class OperationDescriptor:
    """Typed, bounded descriptor of one public operation: purpose, input, output,
    authority, side effects, limits, and retry safety. The parent binds trusted
    adapters and config against these contracts."""

    name: str
    purpose: str
    input: str
    output: str
    authority: str
    side_effects: tuple = ()
    limits: dict = field(default_factory=dict)
    retry_safe: bool = True


OPERATION_DESCRIPTORS = {
    "watcher.scan": OperationDescriptor(
        name="watcher.scan",
        purpose="One bounded workspace scan; persists events and cursor in one transaction.",
        input="WatcherPolicy (trusted workspace root, limits, exclusions)",
        output="list[WatchEvent] (hash-only identity; never file content)",
        authority="local workspace read within ScanLimits",
        side_effects=("writes outbox+cursor SQLite rows",),
        limits={"max_files": "ScanLimits", "max_queue": "WatcherPolicy"},
        retry_safe=True),
    "watcher.drain": OperationDescriptor(
        name="watcher.drain",
        purpose="Idempotent transfer of outbox rows into the capture spool, then ack.",
        input="bound Capture spool",
        output="list of transferred outbox rows",
        authority="capture spool of the observer run",
        side_effects=("capture spool inserts with stable event IDs", "outbox deletes"),
        limits={"batch": 256},
        retry_safe=True),
    "gateway.evaluate": OperationDescriptor(
        name="gateway.evaluate",
        purpose="Classify a governed tool request and return an allow/deny verdict.",
        input="tool name, tool input, trusted GatewayPolicy",
        output="GatewayDecision (advisory; enforcement is at the host hook)",
        authority="canonical tool map; never model descriptions",
        side_effects=(),
        limits={"description_bytes": 4096},
        retry_safe=True),
    "control.dispatch": OperationDescriptor(
        name="control.dispatch",
        purpose="Create a durable control intent and dispatch a canonical control command.",
        input="action, expected revision, reason, stable intent key, coordinator epoch",
        output="intent receipt (applied | failed | held | deny) with evidence",
        authority="coordinator lease + run revision, enforced by the service",
        side_effects=("POST controls (idempotent by intent key)", "adapter control call",
                      "durable intent state"),
        limits={"max_intents": MAX_INTENTS},
        retry_safe=True),  # Retries dedup on the stable intent key.
    "control.resume": OperationDescriptor(
        name="control.resume",
        purpose="Resume from an applied-pause quiescent checkpoint, consumed exactly once.",
        input="expected revision, checkpoint id, coordinator epoch",
        output="resume receipt or denial",
        authority="checkpoint bound to this run's applied pause intent",
        side_effects=("durable consumption marker before dispatch",),
        limits={"max_consumed_checkpoints": MAX_CONSUMED_CHECKPOINTS},
        retry_safe=False),  # A consumed checkpoint is never replayed.
}


# --------------------------------------------------------------------------
# Continuous filesystem-change watcher
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class WatcherPolicy:
    """Trusted, administrator-supplied watcher configuration.

    ``enabled`` defaults to ``False``: capture threads are never started
    implicitly. All bounds are strictly typed and enforced independently.
    """

    enabled: bool = False
    workspace_root: str | None = None
    limits: ScanLimits = field(default_factory=ScanLimits)
    exclude: tuple = ()
    poll_interval_seconds: float = 2.0
    max_queue: int = 256            # outbox rows accepted per scan before gap
    max_events_per_scan: int = 500  # diff findings cap; truncation is a declared gap
    state_dir: str | None = None

    def __post_init__(self):
        _strict_bool(self.enabled, "enabled")
        _strict_float(self.poll_interval_seconds, "poll_interval_seconds", 0.1, 60.0)
        _strict_int(self.max_queue, "max_queue", 1, MAX_OUTBOX_ROWS)
        _strict_int(self.max_events_per_scan, "max_events_per_scan", 1, MAX_OUTBOX_ROWS)
        if self.enabled and not self.workspace_root:
            raise ValueError("workspace_root is required when the watcher is enabled")
        if self.workspace_root is not None and type(self.workspace_root) is not str:
            raise ValueError("workspace_root must be a string")
        if not isinstance(self.limits, ScanLimits):
            raise ValueError("limits must be a ScanLimits instance")


@dataclass(frozen=True)
class WatchEvent:
    """One observed filesystem change; hash/size/mtime identity only, no content."""

    path: str
    change: str  # created | modified | deleted
    size: int | None = None
    sha256: str | None = None
    mtime: float | None = None
    sequence: int = 0
    event_id: str = ""
    detail: dict = field(default_factory=dict)


class ContinuousWatcher:
    """Bounded polling watcher with a durable SQLite outbox and cursor.

    Every accepted event row and the post-scan cursor commit in **one
    transaction**, so a crash after ``scan()`` never loses accepted
    observations and never re-emits acknowledged ones. ``drain()`` transfers
    outbox rows into the capture spool with stable event IDs (``INSERT OR
    IGNORE`` semantics under the capture lock) and acknowledges each row only
    after the spool commit; a crash between spool insert and acknowledgement
    dedups on the stable event ID. Missing cursor state after a previous run,
    any truncated scan (including the first), and any truncated diff produce
    explicit ``capture.gap_detected`` rows; a truncated scan never advances the
    baseline and never infers deletions.
    """

    def __init__(self, policy, *, capture=None, clock=time.time, monotonic=time.monotonic,
                 sleep=time.sleep):
        if not isinstance(policy, WatcherPolicy):
            raise ValueError("policy must be a WatcherPolicy")
        self.policy = policy
        self.capture = capture
        self.clock, self.monotonic, self.sleep = clock, monotonic, sleep
        if policy.workspace_root:
            # Reject junction/symlink escapes before any resolve().
            _reject_reparse_ancestors(policy.workspace_root)
            _reject_reparse_ancestors(policy.state_dir or ".")
            if getattr(capture, "path", None) is not None:
                _reject_reparse_ancestors(capture.path)
        self.workspace = Path(policy.workspace_root).resolve() if policy.workspace_root else None
        self.state_dir = Path(policy.state_dir or ".").resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.state_dir / f"{_state_key(self.workspace or 'watcher')}-watch.sqlite"
        self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        if os.name != "nt":
            self.db_path.chmod(0o600)
        self.db.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS cursor(key TEXT PRIMARY KEY, value TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS outbox("
                        "event_id TEXT PRIMARY KEY, seq INTEGER, suffix TEXT, payload TEXT)")
        self.db.commit()
        self._had_source = self._meta_get("source") is not None
        self.source = self._meta_get("source") or self._meta_set("source", uid("watchsource"))
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None

    # --------------------------------------------------------- cursor/meta IO

    def _meta_get(self, key):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_set(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, value))
        self.db.commit()
        return value

    def _cursor_get(self, key, default=None):
        row = self.db.execute("SELECT value FROM cursor WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    @property
    def cursor_sequence(self):
        return int(self._cursor_get("sequence", 0))

    def _insert_outbox(self, suffix, data, detail):
        """Append one durable outbox row. Caller holds the write transaction.

        Gap events are control-plane: they bypass the row cap so an overflow is
        always declared, but a consecutive identical gap coalesces so a stuck
        queue cannot grow the outbox without bound.
        """
        sequence = int(self._cursor_get("sequence", 0)) + 1
        is_gap = suffix == "capture.gap_detected"
        if is_gap:
            last = self.db.execute(
                "SELECT suffix, payload FROM outbox ORDER BY seq DESC LIMIT 1").fetchone()
            if last and last[0] == suffix and json.loads(last[1]).get("data") == data:
                return None  # Same gap already the newest row: coalesced, not lost.
        count = self.db.execute("SELECT count(*) FROM outbox").fetchone()[0]
        if not is_gap and count >= self.policy.max_queue:
            dropped = int(self._cursor_get("dropped", 0)) + 1
            self.db.execute("INSERT OR REPLACE INTO cursor VALUES('dropped', ?)",
                            (json.dumps(dropped),))
            return None
        event_id = f"{self.source}-{sequence}"
        self.db.execute("INSERT INTO outbox VALUES(?,?,?,?)",
                        (event_id, sequence, suffix, canonical({"data": data, "detail": detail})))
        self.db.execute("INSERT OR REPLACE INTO cursor VALUES('sequence', ?)",
                        (json.dumps(sequence),))
        self.db.execute("INSERT OR REPLACE INTO meta VALUES('max_sequence', ?)", (str(sequence),))
        return WatchEvent(path=data.get("path", ""), change=data.get("change", ""),
                          size=detail.get("size"), sha256=detail.get("sha256"),
                          mtime=detail.get("mtime"), sequence=sequence,
                          event_id=event_id, detail=detail)

    # ------------------------------------------------------------------ scan

    @staticmethod
    def _identity(entry):
        if entry is None:
            return (None, None)
        return (entry.get("size"), entry.get("sha256"))

    def _diff(self, before_files, after_files):
        """Scan-to-scan diff with rename pairing. Returns (events_data, truncated)."""
        findings = diff_snapshots({"files": before_files}, {"files": after_files},
                                  max_findings=self.policy.max_events_per_scan + 1)
        truncated = len(findings) > self.policy.max_events_per_scan
        findings = findings[: self.policy.max_events_per_scan]
        # Rename detection: a delete and a create in the same scan with identical
        # size+hash are paired as evidence detail; the canonical event vocabulary
        # (created/modified/deleted) is unchanged.
        deleted = {self._identity(f.before): f.path for f in findings
                   if f.change == "deleted" and self._identity(f.before) != (None, None)}
        renamed = {}
        for finding in findings:
            if finding.change == "created" and self._identity(finding.after) in deleted:
                renamed[finding.path] = deleted[self._identity(finding.after)]
        mtimes = {name: entry.get("mtime") for name, entry in after_files.items()}
        rows = []
        for finding in findings:
            after = finding.after or {}
            detail = {"hashed": finding.hashed, "size": after.get("size"),
                      "sha256": after.get("sha256"), "mtime": mtimes.get(finding.path)}
            if finding.path in renamed:
                detail["renamed_from"] = renamed[finding.path]
            rows.append(({"path": finding.path, "change": finding.change}, detail))
        return rows, truncated

    def scan(self):
        """One bounded scan; events and the new cursor commit atomically."""
        with self._lock:
            if self.workspace is None:
                raise ValueError("workspace_root is required for a watcher scan")
            snapshot = scan_workspace(self.workspace, limits=self.policy.limits,
                                      exclude=self.policy.exclude, clock=self.monotonic)
            files = {}
            for name, entry in snapshot["files"].items():
                try:
                    stat = (self.workspace / name).lstat()
                    files[name] = {**entry, "mtime": stat.st_mtime}
                except OSError:
                    files[name] = {**entry, "mtime": None}
            events = []
            self.db.execute("BEGIN IMMEDIATE")
            try:
                before = self._cursor_get("files")
                if snapshot["truncated"]:
                    # Checked before the baseline branch: even the first scan is a
                    # gap, and a partial snapshot never becomes the baseline and
                    # never infers deletions.
                    gap = self._insert_outbox("capture.gap_detected",
                                              {"reason": "scan_truncated",
                                               "dropped_observation_count":
                                                   int(self._cursor_get("dropped", 0))}, {})
                    if gap:
                        events.append(gap)
                elif before is None:
                    high_water = int(self._meta_get("max_sequence") or 0)
                    if self._had_source and high_water > 0:
                        # Cursor rows are gone but this store emitted before: a
                        # declared gap, never a silent re-baseline.
                        gap = self._insert_outbox("capture.gap_detected",
                                                  {"reason": "cursor_lost",
                                                   "dropped_observation_count": 0}, {})
                        if gap:
                            events.append(gap)
                    self.db.execute("INSERT OR REPLACE INTO cursor VALUES('files', ?)",
                                    (canonical(files),))
                    self._had_source = True
                else:
                    rows, findings_truncated = self._diff(before, files)
                    overflowed = False
                    for data, detail in rows:
                        event = self._insert_outbox("file.changed", data, detail)
                        if event is None:
                            overflowed = True
                        else:
                            events.append(event)
                    if overflowed:
                        gap = self._insert_outbox("capture.gap_detected",
                                                  {"reason": "queue_overflow",
                                                   "dropped_observation_count":
                                                       int(self._cursor_get("dropped", 0))}, {})
                        if gap:
                            events.append(gap)
                    if findings_truncated:
                        # The diff was capped: record the gap; the baseline still
                        # advances (it is complete) but completeness is never claimed
                        # for the event stream of this scan.
                        gap = self._insert_outbox("capture.gap_detected",
                                                  {"reason": "findings_truncated",
                                                   "dropped_observation_count": 0}, {})
                        if gap:
                            events.append(gap)
                    self.db.execute("INSERT OR REPLACE INTO cursor VALUES('files', ?)",
                                    (canonical(files),))
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            return events

    # ----------------------------------------------------------------- drain

    def _transfer(self, capture, event_id, suffix, payload):
        """Idempotent spool transfer: stable event ID, capture lock, dedup check."""
        with capture.lock:
            if capture.path.stat().st_size > 256 * 1024 * 1024:
                raise RuntimeError("Capture spool budget exhausted; capture is incomplete")
            existing = capture.db.execute("SELECT 1 FROM spool WHERE id=?",
                                          (event_id,)).fetchone()
            if existing is None:
                clean = redact_value(payload["data"])
                event = m.Event(
                    event_id=event_id, type="hivemind.context." + suffix,
                    agent_id=capture.agent_id, run_id=capture.run_id,
                    project_id=capture.project_id, occurred_at=now(),
                    source={"source_id": capture.source, "kind": "filesystem",
                            "sequence": capture.sequence, "attribution": "reported"},
                    data=clean)
                capture.sequence += 1
                capture.db.execute("INSERT INTO spool VALUES(?,?,'pending')",
                                   (event.event_id, canonical(event.model_dump())))
                capture.db.commit()

    def drain(self, batch=256):
        """Transfer outbox rows to the capture spool, then acknowledge each row.

        A bound capture is required: without one, rows are retained (use
        :meth:`pending` to inspect them). Any spool failure aborts the drain
        with the remaining rows retained; the next drain retries and dedups on
        stable event IDs.
        """
        if self.capture is None:
            raise RuntimeError("No capture spool bound; outbox rows are retained")
        _strict_int(batch, "batch", 1, MAX_OUTBOX_ROWS)
        with self._lock:
            rows = self.db.execute(
                "SELECT event_id, suffix, payload FROM outbox ORDER BY seq LIMIT ?",
                (batch,)).fetchall()
            transferred = []
            for event_id, suffix, raw in rows:
                payload = json.loads(raw)
                self._transfer(self.capture, event_id, suffix, payload)
                # Acknowledge each row immediately after its spool commit; a
                # failure aborts with the remaining rows retained for retry.
                self.db.execute("DELETE FROM outbox WHERE event_id=?", (event_id,))
                self.db.commit()
                transferred.append((event_id, suffix, payload))
            return transferred

    def pending(self, limit=256):
        """Read retained outbox rows without acknowledging them."""
        _strict_int(limit, "limit", 1, MAX_OUTBOX_ROWS)
        with self._lock:
            return [(event_id, suffix, json.loads(raw)) for event_id, suffix, raw in
                    self.db.execute(
                        "SELECT event_id, suffix, payload FROM outbox ORDER BY seq LIMIT ?",
                        (limit,)).fetchall()]

    # ---------------------------------------------------------------- threads

    def start(self):
        """Explicit opt-in: start the single bounded poll thread."""
        if not self.policy.enabled:
            raise RuntimeError("Watcher is not enabled; set WatcherPolicy(enabled=True)")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("Watcher already running")
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True,
                                            name="hivemind-watch")
            self._thread.start()

    def _loop(self):
        while not self._stop.wait(self.policy.poll_interval_seconds):
            try:
                self.scan()
                self.drain()
            except (OSError, ValueError, sqlite3.Error):
                # Scan faults are gaps, not crashes; the outbox is untouched.
                with self._lock:
                    self.db.execute("BEGIN IMMEDIATE")
                    try:
                        self._insert_outbox("capture.gap_detected",
                                            {"reason": "scan_error",
                                             "dropped_observation_count":
                                                 int(self._cursor_get("dropped", 0))}, {})
                        self.db.commit()
                    except sqlite3.Error:
                        self.db.rollback()

    def stop(self, timeout=10.0):
        """Deterministic shutdown: the thread reference clears only after a
        successful join; a timed-out join raises and retains the reference."""
        with self._lock:
            thread = self._thread
        if thread is None:
            return
        self._stop.set()
        thread.join(timeout=timeout)
        if thread.is_alive():
            raise RuntimeError("Watcher shutdown timed out; cursor and outbox retained")
        with self._lock:
            if self._thread is thread:
                self._thread = None

    @property
    def running(self):
        return self._thread is not None and self._thread.is_alive()

    def audit(self):
        """Counters only: no file content, no secret paths."""
        with self._lock:
            return {"enabled": self.policy.enabled,
                    "workspace": str(self.workspace) if self.workspace else None,
                    "source": self.source, "sequence": self.cursor_sequence,
                    "outbox": self.db.execute("SELECT count(*) FROM outbox").fetchone()[0],
                    "dropped": int(self._cursor_get("dropped", 0)),
                    "baselined": self._cursor_get("files") is not None,
                    "running": self.running}

    def close(self):
        self.stop()
        self.db.close()


def watcher_from_config(config, *, capture=None):
    """Build a watcher from a trusted operator config mapping.

    Recognized keys: ``enabled`` (bool), ``workspace_root``, ``state_dir``,
    ``poll_interval_seconds``, ``max_queue``, ``max_events_per_scan``,
    ``exclude`` (list of extra secret-style patterns). Unknown keys are
    rejected so misconfiguration can never silently widen capture.
    """
    if not isinstance(config, dict):
        raise ValueError("Watcher config must be an object")
    allowed = {"enabled", "workspace_root", "state_dir", "poll_interval_seconds",
               "max_queue", "max_events_per_scan", "exclude", "limits"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError("Unknown watcher config keys: " + ",".join(sorted(unknown)))
    limits = config.get("limits")
    if limits is not None and not isinstance(limits, ScanLimits):
        if not isinstance(limits, dict):
            raise ValueError("limits must be a ScanLimits or an object")
        limits = ScanLimits(**limits)
    return ContinuousWatcher(WatcherPolicy(
        enabled=config.get("enabled", False),
        workspace_root=config.get("workspace_root"),
        limits=limits or ScanLimits(),
        exclude=tuple(config.get("exclude", ())),
        poll_interval_seconds=config.get("poll_interval_seconds", 2.0),
        max_queue=config.get("max_queue", 256),
        max_events_per_scan=config.get("max_events_per_scan", 500),
        state_dir=config.get("state_dir")), capture=capture)


# --------------------------------------------------------------------------
# Governed tool-request gateway
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class ToolDescriptor:
    """Canonical typed descriptor. ``description`` is untrusted host/model metadata
    and is never an input to an authorization decision."""

    name: str
    namespace: str
    action: str  # read | write | execute | discover | unknown
    description: str = ""
    side_effects: tuple = ()
    retry_safe: bool = True
    authority: str = "none"  # none | read_scope | permit
    limits: dict = field(default_factory=dict)


def describe_tool(name, description=""):
    """Resolve the canonical descriptor for a tool name.

    The caller-supplied ``description`` (for example from a model's tool listing)
    is carried as opaque metadata only; classification and required authority
    come exclusively from the administrator-controlled map.
    """
    action, namespace = _TOOL_MAP.get(str(name).lower(), (UNKNOWN_ACTION, "unknown"))
    authority = {"read": "read_scope", "write": "permit"}.get(action, "none")
    side_effects = {WRITE: ("filesystem_mutation",), EXECUTE: ("process_execution",)}.get(action, ())
    return ToolDescriptor(name=str(name), namespace=namespace, action=action,
                          description=str(description or "")[:4096],
                          side_effects=side_effects,
                          retry_safe=action in (READ, DISCOVER), authority=authority)


@dataclass(frozen=True)
class GatewayPolicy:
    """Administrator-controlled gateway policy. Strict mode denies unknown tools
    and any unsupported bypass; there is no implicit allow. Strictness can only
    be set by the trusted operator, never by request bodies or payloads."""

    strict: bool = True
    allowed_tools: frozenset | None = None  # None = all canonical non-unknown tools
    allowed_namespaces: frozenset = frozenset({"filesystem", "network", "discovery"})
    allowed_mcp_tools: tuple = ()  # explicit administrator-configured tool IDs only
    restrict_reads_to_workspace: bool = True
    max_tool_calls: int | None = None

    def __post_init__(self):
        _strict_bool(self.strict, "strict")
        _strict_bool(self.restrict_reads_to_workspace, "restrict_reads_to_workspace")
        if self.max_tool_calls is not None:
            _strict_int(self.max_tool_calls, "max_tool_calls", 1, 1000000)


@dataclass(frozen=True)
class GatewayDecision:
    allowed: bool
    reason: str
    descriptor: ToolDescriptor
    permit_required: bool = False
    paths: tuple = ()
    evidence: dict = field(default_factory=dict)


def from_hook_payload(payload):
    """Normalize a native hook/extension event into ``(event, name, tool_input)``.

    Supported adapters (documented, no others are guessed):

    * Droid stdin hooks: ``hook_event_name`` of ``PreToolUse``/``PostToolUse``/
      ``SessionStart`` with ``tool_name``/``tool_input``.
    * Oh my Pi extension: ``type`` of ``tool_call``/``tool_result``/
      ``session_start`` with ``toolName``/``input``/``toolCallId``.

    An unrecognized payload raises ``ValueError``; under a strict policy the
    caller must deny rather than bypass.
    """
    if not isinstance(payload, dict):
        raise ValueError("Hook payload must be an object")
    event = payload.get("hook_event_name", payload.get("type"))
    if event not in ("PreToolUse", "PostToolUse", "SessionStart",
                     "tool_call", "tool_result", "session_start"):
        raise ValueError("Unsupported hook event")
    name = payload.get("tool_name", payload.get("toolName", ""))
    tool_input = payload.get("tool_input", payload.get("input", {}))
    return event, name, tool_input


_READ_PATH_FIELDS = {"read": ("file_path", "path"), "ls": ("directory_path",),
                     "glob": ("folder",), "grep": ("path",)}


class ToolGateway:
    """Canonical read/write permit gateway for governed tool requests.

    Write tools resolve their target paths and require a canonical permit check
    (``POST runs/{run}/permits:check`` against task/execution/claim scope) via
    :meth:`authorize`. Reads are confined to the workspace and the canonical
    exclusion set. Unknown tools, unsupported input shapes, and out-of-scope
    paths are denied when the policy is strict. Without a trusted workspace
    root, confinement-checked reads and writes are denied rather than allowed
    unbounded.
    """

    def __init__(self, policy=None, *, workspace_root=None):
        self.policy = policy or GatewayPolicy()
        if workspace_root is not None:
            _reject_reparse_ancestors(workspace_root)
        self.workspace = Path(workspace_root).resolve() if workspace_root else None

    def evaluate(self, name, tool_input=None, *, description="", calls_made=0):
        descriptor = describe_tool(name, description)
        evidence = {"action": descriptor.action, "namespace": descriptor.namespace,
                    "authority": descriptor.authority}
        if self.policy.max_tool_calls is not None and calls_made >= self.policy.max_tool_calls:
            return GatewayDecision(False, "tool_call_budget_exhausted", descriptor,
                                   evidence=evidence)
        if descriptor.action == UNKNOWN_ACTION:
            if self.policy.strict:
                return GatewayDecision(False, "unknown_tool", descriptor, evidence=evidence)
            return GatewayDecision(True, "unknown_tool_permitted_by_policy", descriptor,
                                   evidence=evidence)
        if name in self.policy.allowed_mcp_tools:
            # Explicit administrator-configured tool IDs only; identity/policy
            # enforcement still applies upstream.
            return GatewayDecision(True, "administrator_allowlisted", descriptor,
                                   evidence=evidence)
        if self.policy.allowed_tools is not None and descriptor.name.lower() not in {
                tool.lower() for tool in self.policy.allowed_tools}:
            return GatewayDecision(False, "tool_not_allowlisted", descriptor, evidence=evidence)
        if descriptor.namespace not in self.policy.allowed_namespaces:
            return GatewayDecision(False, "namespace_not_allowed", descriptor, evidence=evidence)
        if descriptor.action == WRITE:
            paths = self._write_paths(descriptor, tool_input)
            if paths is None:
                return GatewayDecision(False, "unsupported_write_shape", descriptor,
                                       evidence=evidence)
            return GatewayDecision(True, "permit_required", descriptor, permit_required=True,
                                   paths=tuple(paths), evidence=evidence)
        if descriptor.action == READ and self.policy.restrict_reads_to_workspace:
            if self._read_in_scope(descriptor, tool_input) is not True:
                return GatewayDecision(False, "read_out_of_scope", descriptor, evidence=evidence)
        return GatewayDecision(True, "allowed", descriptor, evidence=evidence)

    def _resolve(self, path):
        if not isinstance(path, str) or not path:
            return None
        if self.workspace is None:
            return None
        target = Path(path)
        if not target.is_absolute():
            _reject_reparse_ancestors(self.workspace / target, anchor=self.workspace)
        else:
            _reject_reparse_ancestors(target)
        target = (self.workspace / target).resolve() if not target.is_absolute() else target.resolve()
        try:
            relative = target.relative_to(self.workspace)
        except ValueError:
            return None
        if secret_path(relative, ()):
            return None
        return relative.as_posix()

    def _write_paths(self, descriptor, tool_input):
        """Canonical write-path extraction; bounded patch headers only."""
        name = descriptor.name.lower()
        if name in ("edit", "write", "create"):
            if not isinstance(tool_input, dict):
                return None
            resolved = self._resolve(tool_input.get("file_path", tool_input.get("path")))
            return [resolved] if resolved else None
        if name == "applypatch":
            patch = tool_input if isinstance(tool_input, str) else (
                tool_input.get("patch", tool_input.get("input"))
                if isinstance(tool_input, dict) else None)
            if not isinstance(patch, str):
                return None
            lines = patch.strip().splitlines()
            if not lines or lines[0] != "*** Begin Patch" or lines[-1] != "*** End Patch":
                return None
            paths = []
            prefixes = ("*** Add File: ", "*** Update File: ", "*** Delete File: ",
                        "*** Move to: ")
            for line in lines[1:-1]:
                prefix = next((item for item in prefixes if line.startswith(item)), None)
                if prefix:
                    resolved = self._resolve(line[len(prefix):].strip())
                    if not resolved:
                        return None
                    paths.append(resolved)
                elif line.startswith("***") and line != "*** End of File":
                    return None
            if not paths or len(paths) > 64:
                return None
            return list(dict.fromkeys(paths))
        return None

    def _read_in_scope(self, descriptor, tool_input):
        if not isinstance(tool_input, dict):
            return False
        fields = _READ_PATH_FIELDS.get(descriptor.name.lower())
        if fields is None:
            return True  # Network reads carry no local path.
        if self.workspace is None:
            # Without a trusted workspace root no local read can be
            # confinement-checked: deny rather than allowing an unbounded read.
            return False
        raw = next((tool_input.get(field) for field in fields
                    if tool_input.get(field) is not None), ".")
        return self._resolve(raw) is not None

    def authorize(self, decision, client, run_id, scope_ids):
        """Canonical permit check for a write decision. Fail-closed: any transport
        or service rejection denies."""
        if not decision.permit_required:
            return decision.allowed
        if not decision.allowed or not scope_ids:
            return False
        try:
            client.request("POST", f"runs/{run_id}/permits:check",
                           {"scope_ids": list(scope_ids), "paths": list(decision.paths)})
            return True
        except (Problem, httpx.HTTPError):
            return False


# --------------------------------------------------------------------------
# Safe pause/resume policy controller
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class ControlPolicy:
    """Thresholds that trigger a safe pause. Every trigger is opt-in (``None``
    disables it). Controls are only dispatched through supported authoritative
    adapters; anything else is held, never falsely applied."""

    enabled: bool = False
    drift_findings_threshold: int | None = None
    quarantined_events_threshold: int | None = None
    conflict_signals: tuple = ()  # e.g. ("claim_conflict", "revision_conflict")

    def __post_init__(self):
        _strict_bool(self.enabled, "enabled")
        for name in ("drift_findings_threshold", "quarantined_events_threshold"):
            value = getattr(self, name)
            if value is not None:
                _strict_int(value, name, 1, 1000000)


@dataclass(frozen=True)
class ControlIntent:
    """A typed, idempotent control intent with revision authority."""

    intent_id: str
    intent_key: str
    action: str
    target_run_id: str
    expected_revision: int
    reason: str
    created_at: float


def runtime_capabilities(adapter):
    """The control actions an adapter can authoritatively settle.

    Authority order: an explicit ``supported_controls`` declaration on the
    adapter (the contract real runtimes implement), then the documented vendor
    runtime contract (:class:`hivemind.vendor_runtimes.VendorRuntime`
    subclasses settle pause/cancel with quiescent checkpoints and resume from a
    paused outcome), then the file-based wrapper contract
    (:class:`hivemind.runtime_lifecycle.FileAdapter`). Anything else has *no*
    control capability: mere callable existence is never sufficient.
    """
    if adapter is None:
        return frozenset()
    declared = getattr(adapter, "supported_controls", None)
    if declared is not None:
        if not isinstance(declared, (frozenset, set, tuple, list)):
            raise ValueError("supported_controls must be a finite collection")
        return frozenset(action for action in declared if action in SUPPORTED_ACTIONS)
    from . import vendor_runtimes
    if isinstance(adapter, vendor_runtimes.VendorRuntime):
        return frozenset(SUPPORTED_ACTIONS)
    if isinstance(adapter, FileAdapter):
        return frozenset(SUPPORTED_ACTIONS)
    return frozenset()


class SafePauseController:
    """Policy-driven safe pause/resume over supported authoritative adapters.

    Durability and fencing rules:

    * Existing state that is corrupt, unreadable, or bound to a different
      project/agent/run refuses to load (raises); it is never silently reset,
      so consumed-checkpoint markers cannot be lost into a replay.
    * State mutations run under a cross-process :class:`filelock.FileLock`.
    * :meth:`dispatch` takes a stable ``intent_key`` (defaults to a digest of
      the exact fence: action + target run + expected revision + reason).
      Re-dispatching a known key returns the recorded receipt, refreshed from
      the canonical control record when available — a crash or timeout never
      creates a second control (the service idempotency key is also derived
      from the intent key).
    * The intent is persisted as ``dispatching`` *before* the canonical command
      is sent (transition durable before dispatch; at-most-once does not mean
      applied). Service rejections (``Problem``) are ``failed``; generic
      transport failures are ``held`` as unknown, never failed and never a
      silent no-effect.
    * A disabled policy denies dispatch and resume outright.
    * Dispatch revalidates the current run: terminal runs and stale expected
      revisions are denied before any adapter call.
    * :meth:`resume` requires an *applied pause* intent whose observed
      checkpoint matches and is bound to this run. Cancel checkpoints are
      terminal and cannot resume (documented contract). Consumption is
      persisted before dispatch; another agent's or a stale checkpoint is
      denied.
    * Intent and consumption history are bounded (``MAX_INTENTS``,
      ``MAX_CONSUMED_CHECKPOINTS``); exceeding a bound raises rather than
      silently dropping history.
    """

    def __init__(self, client, project, agent, run, state_file, *, adapter=None,
                 policy=None, control_client=None, clock=time.time):
        # ``client`` settles controls as the run's own agent; ``control_client``
        # (a coordinator holding the project lease) creates canonical commands.
        self.client, self.project, self.agent, self.run = client, project, agent, run
        self.control_client = control_client or client
        self.adapter = adapter
        self.policy = policy or ControlPolicy()
        if not isinstance(self.policy, ControlPolicy):
            raise ValueError("policy must be a ControlPolicy")
        self.clock = clock
        self.path = Path(state_file)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file_lock = FileLock(str(self.path) + ".lock")
        self._lock = threading.RLock()
        self.state = self._load()
        self._boundary = None

    def _load(self):
        with self._file_lock:
            if self.path.exists():
                try:
                    state = json.loads(self.path.read_text(encoding="utf-8"))
                except (OSError, ValueError) as error:
                    # Corrupt existing state must refuse: silently resetting it
                    # would make checkpoint replays possible.
                    raise ValueError("Control state is unreadable; refusing to continue "
                                     "without the consumed-checkpoint history") from error
                if not isinstance(state, dict):
                    raise ValueError("Control state must be an object")
                bound = state.get("bound_to")
                expected = {"project_id": self.project, "agent_id": self.agent,
                            "run_id": self.run}
                if bound is not None and bound != expected:
                    raise ValueError("Control state belongs to another project/agent/run")
            else:
                state = {}
            state.setdefault("bound_to", {"project_id": self.project, "agent_id": self.agent,
                                          "run_id": self.run})
            state.setdefault("intents", {})
            state.setdefault("consumed_checkpoints", [])
            return state

    def persist(self):
        with self._file_lock:
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(canonical(self.state), encoding="utf-8")
            if os.name != "nt":
                temporary.chmod(0o600)
            os.replace(temporary, self.path)

    def boundary(self):
        if self._boundary is None:
            self._boundary = LifecycleBoundary(
                self.client, self.project, self.agent, self.run,
                self.path.with_name(self.path.stem + "-lifecycle.json"),
                adapter=self.adapter, enabled=True)
        return self._boundary

    # --------------------------------------------------------------- triggers

    def evaluate(self, signals):
        """Map observed signals to a pause trigger, or ``None``.

        ``signals`` is a mapping such as ``{"drift_findings": 3,
        "quarantined_events": 0, "conflicts": ["claim_conflict"]}``. All signals
        are caller-supplied evidence; nothing is inferred here.
        """
        if not self.policy.enabled:
            return None
        reasons = []
        threshold = self.policy.drift_findings_threshold
        if threshold is not None and int(signals.get("drift_findings", 0)) >= threshold:
            reasons.append(f"drift_findings>={threshold}")
        threshold = self.policy.quarantined_events_threshold
        if threshold is not None and int(signals.get("quarantined_events", 0)) >= threshold:
            reasons.append(f"quarantined_events>={threshold}")
        for conflict in signals.get("conflicts", ()):
            if conflict in self.policy.conflict_signals:
                reasons.append(f"conflict:{conflict}")
        if not reasons:
            return None
        return {"action": "pause", "reason": ";".join(reasons)[:512]}

    # --------------------------------------------------------------- dispatch

    def _intent_key(self, action, expected_revision, reason, intent_key):
        if intent_key is not None:
            if not isinstance(intent_key, str) or not 1 <= len(intent_key) <= 256:
                raise ValueError("intent_key must be a string of 1..256 characters")
            return intent_key
        # Derived from the exact fence: stable across crash/timeout retries.
        return digest({"action": action, "target_run_id": self.run,
                       "expected_revision": expected_revision, "reason": reason})

    def _receipt(self, record, deduplicated=False):
        receipt = {"intent_id": record["intent"]["intent_id"],
                   "intent_key": record["intent"]["intent_key"],
                   "status": record["status"], "evidence": record.get("evidence", {})}
        if record.get("control_id"):
            receipt["control_id"] = record["control_id"]
        if deduplicated:
            receipt["deduplicated"] = True
        return receipt

    def _refresh_receipt(self, record):
        """Refresh a deduplicated receipt from the canonical control record."""
        control_id = record.get("control_id")
        if not control_id:
            return
        try:
            control = self.client.request("GET", f"controls/{control_id}")
        except (Problem, httpx.HTTPError, OSError):
            return  # Keep the recorded receipt; canonical state is unchanged by a read.
        state = control.get("state")
        if state in ("applied", "failed", "outcome_unknown"):
            record["status"] = {"applied": APPLIED, "failed": FAILED}.get(state, HELD)
            record["evidence"] = {**record.get("evidence", {}),
                                  "canonical_state": state, "result": control.get("result")}
            self.persist()

    def _check_fences(self, action, expected_revision):
        """Revalidate the current run/revision before any adapter call."""
        try:
            run = self.client.request("GET", f"runs/{self.run}")
        except Problem as error:
            return {"status": DENY, "reason": "fence_check_failed",
                    "evidence": {"code": error.code}}
        except (httpx.HTTPError, OSError) as error:
            return {"status": HELD, "reason": "fence_check_unknown",
                    "evidence": {"state": "transport_unknown", "error": type(error).__name__}}
        if run.get("agent_id") != self.agent or run.get("project_id") != self.project:
            return {"status": DENY, "reason": "run_identity_mismatch"}
        if run.get("status") in ("completed", "failed", "cancelled"):
            # A late control against a terminal run has no effect by construction.
            return {"status": DENY, "reason": "terminal_run"}
        if run.get("control_revision") != expected_revision:
            return {"status": DENY, "reason": "revision_fence"}
        return None

    def dispatch(self, action, expected_revision, reason, *, intent_key=None,
                 coordinator_epoch=None):
        """Create a durable intent and dispatch the canonical control command.

        Idempotent on the stable intent key; see the class contract.
        """
        if action not in SUPPORTED_ACTIONS:
            raise ValueError("Unsupported control action")
        if not self.policy.enabled:
            return {"status": DENY, "reason": "policy_disabled"}
        with self._lock:
            key = self._intent_key(action, expected_revision, reason, intent_key)
            record = self.state["intents"].get(key)
            if record is not None:
                # Crash/timeout retry: never create a second control; refresh from
                # the canonical receipt when one exists.
                self._refresh_receipt(record)
                return self._receipt(record, deduplicated=True)
            if len(self.state["intents"]) >= MAX_INTENTS:
                raise ValueError("Intent history bound reached; refusing to drop history")
            capabilities = runtime_capabilities(self.adapter)
            intent = ControlIntent(intent_id=uid("intent"), intent_key=key, action=action,
                                   target_run_id=self.run, expected_revision=expected_revision,
                                   reason=str(reason)[:512], created_at=self.clock())
            record = {"intent": asdict(intent), "status": "dispatching", "evidence": {}}
            self.state["intents"][key] = record
            self.persist()  # Transition durable before dispatch.
            if action not in capabilities:
                record["status"] = HELD
                record["evidence"] = {"state": "unsupported_adapter",
                                      "adapter": getattr(self.adapter, "name",
                                                         type(self.adapter).__name__
                                                         if self.adapter else None),
                                      "supported_controls": sorted(capabilities)}
                self.persist()
                return self._receipt(record)
            fence = self._check_fences(action, expected_revision)
            if fence is not None:
                record["status"] = fence["status"]
                record["evidence"] = {"state": "fence", "reason": fence["reason"],
                                      **fence.get("evidence", {})}
                self.persist()
                receipt = self._receipt(record)
                receipt["reason"] = fence["reason"]
                return receipt
            body = {"project_id": self.project, "target_run_id": self.run, "action": action,
                    "target_run_revision": expected_revision, "reason": intent.reason}
            if coordinator_epoch is not None:
                body["coordinator_epoch"] = coordinator_epoch
            try:
                control = self.control_client.request("POST", "controls", body,
                                                      key="control-intent-" + key)
            except Problem as error:
                # A canonical service rejection is definitive.
                record["status"] = FAILED
                record["evidence"] = {"state": "dispatch_rejected", "code": error.code}
                self.persist()
                return self._receipt(record)
            except (httpx.HTTPError, OSError) as error:
                # A generic transport failure means unknown: hold, never fail and
                # never pretend nothing happened. The intent key dedups the retry.
                record["status"] = HELD
                record["evidence"] = {"state": "transport_unknown",
                                      "error": type(error).__name__}
                self.persist()
                return self._receipt(record)
            record["control_id"] = control["id"]
            self.persist()
            self.boundary().boundary()  # Settle through the cooperative boundary.
            settled = self.boundary().state["runtime_observations"].get(control["id"], {})
            if (settled.get("applied") is True and settled.get("checkpoint_id")
                    and settled.get("observed_status") == REQUIRED_STATUS[action]):
                record["status"] = APPLIED
            elif settled.get("applied") is False:
                record["status"] = FAILED
            else:
                record["status"] = HELD
            record["evidence"] = {"control_id": control["id"], "observation": settled,
                                  "outcome": record["status"]}
            self.persist()
            return self._receipt(record)

    # ----------------------------------------------------------------- resume

    def resume(self, expected_revision, checkpoint_id, *, intent_key=None,
               coordinator_epoch=None):
        """Resume from an applied-pause quiescent checkpoint, consumed exactly once.

        Only a checkpoint recorded by an *applied pause* intent bound to this
        run qualifies. Checkpoints recorded by cancel intents are terminal and
        can never resume (documented contract); another agent's or run's
        checkpoint is unknown here. The consumption marker is persisted *before*
        dispatch so a crash cannot replay it.
        """
        if not self.policy.enabled:
            return {"status": DENY, "reason": "policy_disabled"}
        if not checkpoint_id:
            return {"status": DENY, "reason": "checkpoint_required"}
        with self._lock:
            holder = None
            for record in self.state["intents"].values():
                intent = record.get("intent", {})
                observation = record.get("evidence", {}).get("observation", {})
                if observation.get("checkpoint_id") != checkpoint_id:
                    continue
                if intent.get("target_run_id") != self.run:
                    return {"status": DENY, "reason": "foreign_checkpoint"}
                if intent.get("action") == "cancel":
                    # Cancel checkpoints are terminal; resume from one is undefined.
                    return {"status": DENY, "reason": "cancel_checkpoint_terminal"}
                if intent.get("action") == "pause" and record.get("status") == APPLIED:
                    holder = record
            if holder is None:
                return {"status": DENY, "reason": "unknown_checkpoint"}
            if checkpoint_id in self.state["consumed_checkpoints"]:
                return {"status": DENY, "reason": "checkpoint_consumed"}
            if len(self.state["consumed_checkpoints"]) >= MAX_CONSUMED_CHECKPOINTS:
                raise ValueError("Consumed-checkpoint history bound reached; refusing "
                                 "to drop history")
            self.state["consumed_checkpoints"].append(checkpoint_id)
            self.persist()
            result = self.dispatch("resume", expected_revision, "resume:" + checkpoint_id,
                                   intent_key=intent_key,
                                   coordinator_epoch=coordinator_epoch)
            result["checkpoint_id"] = checkpoint_id
            return result

    def audit(self):
        """Intent/checkpoint counters only; no raw payloads."""
        statuses = {}
        for record in self.state["intents"].values():
            statuses[record["status"]] = statuses.get(record["status"], 0) + 1
        return {"run_id": self.run, "intents": len(self.state["intents"]),
                "by_status": statuses,
                "consumed_checkpoints": len(self.state["consumed_checkpoints"])}


def controller_from_config(config, client, *, control_client=None, adapter=None):
    """Build a controller from a trusted operator config mapping.

    Recognized keys: ``project_id``, ``agent_id``, ``run_id``, ``state_file``,
    ``policy`` (an object with ControlPolicy fields). Unknown keys are rejected.
    """
    if not isinstance(config, dict):
        raise ValueError("Controller config must be an object")
    allowed = {"project_id", "agent_id", "run_id", "state_file", "policy"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError("Unknown controller config keys: " + ",".join(sorted(unknown)))
    policy_raw = config.get("policy", {})
    if not isinstance(policy_raw, dict):
        raise ValueError("policy must be an object")
    for key in ("project_id", "agent_id", "run_id", "state_file"):
        if not isinstance(config.get(key), str) or not config[key]:
            raise ValueError(f"{key} is required")
    return SafePauseController(client, config["project_id"], config["agent_id"],
                               config["run_id"], config["state_file"], adapter=adapter,
                               control_client=control_client,
                               policy=ControlPolicy(**policy_raw))


# --------------------------------------------------------------------------
# Feature-module install
# --------------------------------------------------------------------------

class ToolEvaluateRequest(m.Model):
    project_id: m.ID
    agent_id: m.ID
    run_id: m.ID
    tool_name: str = Field(min_length=1, max_length=256)
    tool_input: dict | str | None = None
    scope_ids: list[m.ID] = Field(default_factory=list, max_length=32)
    workspace_root: str | None = Field(default=None, max_length=4096)
    description: str = Field(default="", max_length=4096)


def install(app, service, principal, command=None, envelope=None, *,
            path="/v1/control-capture", gateway_policy=None):
    """Register opt-in control-capture routes on an existing application.

    The parent wires this; nothing is registered implicitly. Policy strictness
    comes only from the operator-supplied ``gateway_policy`` (default strict);
    request bodies cannot relax it. ``workspace_root`` confinement reads local
    paths, so supplying one requires an administrator principal; without one,
    confinement-checked reads and writes deny rather than allowing unbounded
    access. The body agent must match the run's actual agent and project.
    """
    policy = gateway_policy or GatewayPolicy()
    if not isinstance(policy, GatewayPolicy):
        raise ValueError("gateway_policy must be a GatewayPolicy")
    wrapper = envelope or (lambda data: data)

    @app.post(path + ":tool-evaluate")
    async def tool_evaluate(body: ToolEvaluateRequest, p=Depends(principal)):
        service.project_access(p, body.project_id)
        service.represent(p, body.agent_id)
        if body.workspace_root and p["role"] != "admin":
            raise Problem("administrator_required", 403)
        run = service.run(p, body.run_id)
        if run["agent_id"] != body.agent_id or run["project_id"] != body.project_id:
            raise Problem("run_identity_mismatch", 422)
        gateway = ToolGateway(policy, workspace_root=body.workspace_root)
        decision = gateway.evaluate(body.tool_name, body.tool_input,
                                    description=body.description)
        permitted = None
        if decision.permit_required and decision.allowed:
            scope_ids = body.scope_ids or run.get("scope_ids", [])
            transport = ServiceTransport(service, service.store, p, command)
            permitted = gateway.authorize(decision, transport, body.run_id, scope_ids)
        return wrapper({"allowed": decision.allowed and permitted is not False,
                        "reason": decision.reason,
                        "action": decision.descriptor.action,
                        "namespace": decision.descriptor.namespace,
                        "permit_required": decision.permit_required,
                        "permitted": permitted,
                        "paths": list(decision.paths),
                        "strict": policy.strict,
                        "evidence": decision.evidence})

    @app.get(path + ":policy")
    async def control_policy(p=Depends(principal)):
        if p["role"] != "admin":
            raise Problem("administrator_required", 403)
        return wrapper({"strict": policy.strict,
                        "allowed_namespaces": sorted(policy.allowed_namespaces),
                        "restrict_reads_to_workspace": policy.restrict_reads_to_workspace,
                        "canonical_tools": sorted(_TOOL_MAP)})

    return app
