"""Authorized, per-project SSE stream of agent activity projections.

What this module is (and is not):

- It streams the **authorized activity roster** (the same projection as
  ``GET /v1/agents``, see :mod:`hivemind.agent_activity`) as typed, structured SSE
  frames — ``snapshot`` baselines and per-agent ``activity`` updates carrying the
  fresh, visibility-filtered roster entry (status, presence, current task summary,
  run context). A client can react to completion/blocking/availability without
  polling. It never carries raw private task bodies, foreign agents' private
  run/task summaries or ids, and never exposes a cursor that counts traffic a
  principal may not see.
- **Volatile presence is not commit-driven.** The canonical heartbeat endpoint
  rewrites a run's SQL body *without* ``Store.tick`` (deliberately — presence must
  not invalidate control revisions), so ``Store.subscribe_commits`` never fires for
  it. The bridge therefore runs a **bounded presence timer** that re-derives
  presence (online/stale/offline) and a volatile checksum under short canonical
  snapshots and publishes only *transitions*; committed changes (task progress,
  completion, control blocks, roster edits, deletions) are driven by the commit
  subscription. The timer is presence maintenance/catch-up only — it never polls
  all bodies every tick.
- Delivery is **at-least-once, never exactly-once**. A durable per-installation
  event log (its own SQLite file, never the canonical store) gives reconnecting
  clients a replay window with definite ``resync`` advisories when a cursor fell
  outside retention, and overflow backpressure is an explicit ``resync`` frame,
  never a silent gap. Cursors are scoped: admins hold a project log cursor
  (``g<n>`` — they are authorized for everything), agent principals hold a
  shared+own-agent pair (``s<n>.a<m>``), and other project members hold a shared
  cursor (``s<n>``). A cursor never counts events invisible to its holder, so
  observing it leaks no foreign traffic.
- Authorization is **continuous**: the endpoint is marked
  ``hivemind_detached_work`` (the parent middleware skips its global request/store
  locks), and the generator re-verifies — under short ``store.lock`` snapshots
  before every snapshot frame, replay frame, live frame, and heartbeat — that the
  principal still exists and still has project access, recomputing the roster
  through the authorized ``activity_roster`` projection each time. Revocation
  closes the stream silently; hidden/deleted/private runs and tasks are dropped by
  the projection itself, so prior summaries/ids are never re-emitted. Subscriber
  admission (429 past the per-project bound) happens before response headers.
- Everything is opt-in and bounded: with no enabled policy the route is registered
  but returns ``feature_disabled`` and no hub, bridge, or thread is created.
  Pending queues, replay windows, stream lifetimes, frame bytes, snapshot chunking,
  and log retention are all capped; ``ActivityStreamHub.close`` and
  ``ActivityBridge.close`` terminate every open stream and thread honestly.

Wiring contract for the parent integrator:

1. Add ``"activity_stream"`` to ``capabilities.INSTALLED_FEATURE_MODULES`` (or call
   :func:`install` explicitly); options may pass
   ``{"activity_stream": {"policy": ActivityStreamPolicy(enabled=True, ...),
   "path": <optional log path>}}``. With no/absent policy the module installs
   disabled (route present, ``feature_disabled``). A reinstall replaces only this
   module's own routes.
2. ``install`` auto-binds the commit/presence bridge when the store supports
   ``subscribe_commits`` (pass ``bridge=False`` to opt out and construct
   :class:`ActivityBridge` yourself). Handles for shutdown:
   ``app.state.activity_stream_hub`` / ``app.state.activity_stream_bridge``
   (both ``None`` when disabled) — call ``close()`` on each from the lifespan.
3. SDK wiring: construct :class:`ActivitySubscription` with an httpx client and
   hand ``subscription.frames()`` to the consumer; ``close()`` is bounded and
   interrupts an active read within its read window. The SDK facade itself is
   owned by the parent.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from pathlib import Path
from queue import Empty, Full, Queue

from fastapi import Depends, Request
from fastapi.responses import StreamingResponse

# Read-only reuse of the push module's strict primitives. Do not edit
# message_push.py from here; these imports are the sanctioned shared surface.
from .message_push import ID_PATTERN, _strict_cursor, _strict_id
from .storage import Problem

EVENT_KINDS = ("snapshot", "activity", "resync", "shutdown")
"""Every SSE event name this stream can emit. Nonempty, bounded schema."""

CHANGE_KINDS = ("status", "task", "presence", "roster", "withdrawal")
"""The only change classes the bridge publishes. Identity/scope only.
``withdrawal`` is a generic shared invalidation (no ids) emitted when a resource
that was previously public becomes private: public caches must recompute, but
the private event itself stays in the agent scope and never touches shared
cursors."""

PRESENCE_STALE_SECONDS = 15.0
PRESENCE_OFFLINE_SECONDS = 30.0
"""Presence thresholds matching agent_activity's reported roster semantics."""

MAX_STREAM_SECONDS = 300.0
MAX_SUBSCRIBERS_PER_PROJECT = 32
MAX_PENDING_EVENTS = 256
MAX_REPLAY_EVENTS = 1000
MAX_SNAPSHOT_CHUNK = 64
MIN_HEARTBEAT_SECONDS = 1.0
MAX_PRESENCE_SECONDS = 60.0
MAX_LINE_BYTES = 16384
MAX_FRAME_BYTES = 16384
MAX_FRAMES_PER_STREAM = 10000
MAX_CURSOR = 2**63 - 1
MAX_SNAPSHOT_AGENTS = 1000
"""Hard cap on roster entries per snapshot baseline; excess is honestly flagged
``truncated`` (clients reread /v1/agents pages) instead of silently dropped."""
MAX_PRESENCE_BATCH = 5000
MAX_CATCHUP_BATCH = 64
MAX_BASELINE_SOURCES = 16384
"""Hard cap on source ids a baseline adopts. Exceeding it fails CLOSED (a resync
advisory, never an untracked baseline that could later lose a withdrawal)."""
MAX_REFS_PER_TASK = 64
"""Bound on checkpoint evidence refs adopted per task; the rest are truncated
(conservative: withdrawals for truncated refs still arrive via task events)."""

CURSOR_PATTERN = re.compile(r"^(g\d+|s\d+(\.a\d+)?)$")
"""Strict SSE cursor shapes: admin g<n>, member s<n>, agent s<n>.a<m>."""

_OVERFLOW = object()  # Backpressure sentinel: the subscriber must resync.
_CLOSE = object()  # Shutdown sentinel: terminate the stream honestly.


class ActivityStreamError(Exception):
    """Definite activity-stream failure. Messages are fixed text only: remote
    bodies and raw frame bytes are never echoed into errors."""


def _scope_for(private_agent):
    """Event visibility scope: a private run/task is scoped to its agent; anything
    else is shared with every principal authorized for the project."""
    return f"agent:{private_agent}" if private_agent else "shared"


def _strict_scope(scope):
    if scope == "shared":
        return scope
    if isinstance(scope, str) and scope.startswith("agent:"):
        _strict_id(scope.split(":", 1)[1], "scope agent")
        return scope
    raise ValueError("Activity scope must be 'shared' or 'agent:<canonical id>'")


def _strict_change(change):
    if change not in CHANGE_KINDS:
        raise ValueError("Activity changes are limited to: " + ",".join(CHANGE_KINDS))
    return change


def _viewer_kind(viewer):
    if viewer["role"] == "admin":
        return "admin"
    if viewer.get("agent"):
        return "agent"
    return "member"


def _render_cursor(viewer, cursor):
    kind = _viewer_kind(viewer)
    if kind == "admin":
        return f"g{cursor['g']}"
    if kind == "agent":
        return f"s{cursor['s']}.a{cursor['a']}"
    return f"s{cursor['s']}"


def _parse_cursor(value, viewer):
    """Parse a reconnect cursor into per-scope components for this viewer class.
    A cursor from a different viewer class is refused (422), never coerced."""
    if not isinstance(value, str) or not CURSOR_PATTERN.match(value):
        raise Problem("invalid_cursor", 422)
    kind = _viewer_kind(viewer)
    try:
        if kind == "admin":
            if not value.startswith("g"):
                raise Problem("invalid_cursor", 422)
            return {"g": _strict_cursor(int(value[1:])), "s": 0, "a": 0}
        if value.startswith("g"):
            raise Problem("invalid_cursor", 422)
        shared, _, own = value.partition(".a")
        cursor = {"g": 0, "s": _strict_cursor(int(shared[1:])), "a": 0}
        if kind == "agent":
            cursor["a"] = _strict_cursor(int(own)) if own else 0
        elif own:
            raise Problem("invalid_cursor", 422)
        return cursor
    except ValueError:
        raise Problem("invalid_cursor", 422) from None


def _visible_to(scope, viewer):
    """Replay/fanout privacy: admins see every scope; members see shared; agent
    principals additionally see their own agent scope. Anything else is never
    delivered and never counted in that principal's cursor."""
    if viewer["role"] == "admin":
        return True
    if scope == "shared":
        return True
    return bool(viewer.get("agent")) and scope == f"agent:{viewer['agent']}"


class ActivityStreamPolicy:
    """Explicit opt-in and bounds for the activity stream. Deny-by-default."""

    def __init__(self, *, enabled=False, max_subscribers_per_project=8,
                 max_pending_events=64, max_stream_seconds=30.0,
                 heartbeat_seconds=5.0, replay_limit=500, presence_interval=2.0,
                 snapshot_chunk=16, presence_batch=512, catchup_batch=8):
        if type(enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        if type(max_subscribers_per_project) is not int or \
                not 1 <= max_subscribers_per_project <= MAX_SUBSCRIBERS_PER_PROJECT:
            raise ValueError("max_subscribers_per_project out of bounds")
        if type(max_pending_events) is not int or \
                not 1 <= max_pending_events <= MAX_PENDING_EVENTS:
            raise ValueError("max_pending_events out of bounds")
        if type(max_stream_seconds) not in (int, float) or \
                not 1.0 <= float(max_stream_seconds) <= MAX_STREAM_SECONDS or \
                float(max_stream_seconds) != float(max_stream_seconds):
            raise ValueError("max_stream_seconds out of bounds")
        if type(heartbeat_seconds) not in (int, float) or \
                not MIN_HEARTBEAT_SECONDS <= float(heartbeat_seconds) <= 60.0:
            raise ValueError("heartbeat_seconds out of bounds")
        if type(replay_limit) is not int or not 1 <= replay_limit <= MAX_REPLAY_EVENTS:
            raise ValueError("replay_limit out of bounds")
        if type(presence_interval) not in (int, float) or \
                not 1.0 <= float(presence_interval) <= MAX_PRESENCE_SECONDS:
            raise ValueError("presence_interval out of bounds")
        if type(snapshot_chunk) is not int or not 1 <= snapshot_chunk <= MAX_SNAPSHOT_CHUNK:
            raise ValueError("snapshot_chunk out of bounds")
        if type(presence_batch) is not int or not 1 <= presence_batch <= MAX_PRESENCE_BATCH:
            raise ValueError("presence_batch out of bounds")
        if type(catchup_batch) is not int or not 1 <= catchup_batch <= MAX_CATCHUP_BATCH:
            raise ValueError("catchup_batch out of bounds")
        self.enabled = enabled
        self.max_subscribers_per_project = max_subscribers_per_project
        self.max_pending_events = max_pending_events
        self.max_stream_seconds = float(max_stream_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        # The replay limit doubles as the per-(project, scope) log retention bound.
        self.replay_limit = replay_limit
        # Bounded presence-maintenance timer: volatile heartbeat presence and
        # online->stale->offline expirations only; commits drive everything else.
        self.presence_interval = float(presence_interval)
        self.snapshot_chunk = snapshot_chunk
        # Bounded presence page per project per tick (keyset-paginated,
        # round-robin) and bounded catch-up projects per loop iteration.
        self.presence_batch = presence_batch
        self.catchup_batch = catchup_batch

    def describe(self):
        return {"enabled": self.enabled,
                "max_subscribers_per_project": self.max_subscribers_per_project,
                "max_pending_events": self.max_pending_events,
                "max_stream_seconds": self.max_stream_seconds,
                "heartbeat_seconds": self.heartbeat_seconds,
                "replay_limit": self.replay_limit,
                "presence_interval": self.presence_interval,
                "snapshot_chunk": self.snapshot_chunk,
                "presence_batch": self.presence_batch,
                "catchup_batch": self.catchup_batch}


class _Subscription:
    __slots__ = ("project_id", "viewer", "queue", "resync", "cursor")

    def __init__(self, project_id, viewer, queue, resync, cursor):
        self.project_id, self.viewer, self.queue = project_id, viewer, queue
        self.resync, self.cursor = resync, cursor


class ActivityStreamHub:
    """Durable scoped activity event log plus in-process fanout, service-owned.

    The log lives in its own SQLite file (WAL), separate from the canonical store.
    Events carry identity and change class only — ``(scope, agent_id, change,
    source_seq)`` — never content. Retention is bounded per (project, scope); a
    replay whose cursor fell outside the retained window is flagged ``resync``
    instead of silently skipping. Fanout is thread-safe and nonblocking: a full
    subscriber queue gets the overflow sentinel (definite backpressure) rather
    than blocking publishers, and events outside a subscriber's authorized scopes
    are never delivered.
    """

    def __init__(self, path, policy=None):
        self.policy = policy or ActivityStreamPolicy(enabled=True)
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()
        self._lock = threading.Lock()
        self._subscribers = {}  # project_id -> set[_Subscription]
        self._closed = False

    def _ensure_schema(self):
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS activity_events("
            "project_id TEXT NOT NULL, scope TEXT NOT NULL,"
            "scope_seq INTEGER NOT NULL, log_seq INTEGER NOT NULL,"
            "agent_id TEXT, change TEXT NOT NULL, source_seq INTEGER,"
            "created_at REAL NOT NULL,"
            "PRIMARY KEY(project_id, scope, scope_seq))")
        self._db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS activity_events_dedup"
            " ON activity_events(project_id, scope, agent_id, change, source_seq)")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS scope_cursors("
            "project_id TEXT NOT NULL, scope TEXT NOT NULL,"
            "next_seq INTEGER NOT NULL, PRIMARY KEY(project_id, scope))")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS project_log("
            "project_id TEXT PRIMARY KEY, next_log INTEGER NOT NULL)")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS catchup_watermarks("
            "project_id TEXT PRIMARY KEY, seq INTEGER NOT NULL)")
        self._db.execute(
            # Durable previous-visibility tracking: detects public->private scope
            # transitions so stale public projections are withdrawn with a
            # generic shared invalidation (never private ids/notes/counts).
            "CREATE TABLE IF NOT EXISTS resource_scopes("
            "project_id TEXT NOT NULL, resource_id TEXT NOT NULL,"
            "scope TEXT NOT NULL, PRIMARY KEY(project_id, resource_id))")
        self._db.commit()

    # ------------------------------------------------------------- publication
    def _append_locked(self, project_id, scope, agent_id, change, source_seq):
        """Insert one event and advance the scope cursor plus project log
        atomically, pruning beyond the retention bound. Returns the event, or None
        when this exact (scope, agent, change, source_seq) was already published
        (outbox dedup)."""
        row = self._db.execute(
            "SELECT next_seq FROM scope_cursors WHERE project_id=? AND scope=?",
            (project_id, scope)).fetchone()
        scope_seq = int(row[0]) if row else 1
        row = self._db.execute("SELECT next_log FROM project_log WHERE project_id=?",
                               (project_id,)).fetchone()
        log_seq = int(row[0]) if row else 1
        cursor = self._db.execute(
            "INSERT OR IGNORE INTO activity_events(project_id, scope, scope_seq,"
            " log_seq, agent_id, change, source_seq, created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (project_id, scope, scope_seq, log_seq, agent_id, change, source_seq,
             time.time()))
        if cursor.rowcount == 0:
            return None  # Already published from this exact canonical source row.
        self._db.execute(
            "INSERT INTO scope_cursors(project_id, scope, next_seq) VALUES(?,?,?)"
            " ON CONFLICT(project_id, scope) DO UPDATE SET next_seq=excluded.next_seq",
            (project_id, scope, scope_seq + 1))
        self._db.execute(
            "INSERT INTO project_log(project_id, next_log) VALUES(?,?)"
            " ON CONFLICT(project_id) DO UPDATE SET next_log=excluded.next_log",
            (project_id, log_seq + 1))
        self._db.execute(
            "DELETE FROM activity_events WHERE project_id=? AND scope=?"
            " AND scope_seq<=?",
            (project_id, scope, scope_seq - self.policy.replay_limit))
        return {"scope": scope, "scope_seq": scope_seq, "log_seq": log_seq,
                "agent_id": agent_id, "change": change}

    def _fanout(self, project_id, events):
        with self._lock:
            subscribers = list(self._subscribers.get(project_id, ()))
        for subscription in subscribers:
            for event in events:
                if not _visible_to(event["scope"], subscription.viewer):
                    continue  # Foreign scope: never delivered, never counted.
                try:
                    subscription.queue.put_nowait(event)
                except Full:
                    self._overflow(subscription)
                    break

    def publish(self, project_id, scope, agent_id, change, *, source_seq=None):
        """Append one identity/change event and fan it out. Returns the event, or
        None when deduplicated. Strict shapes only; events never carry content."""
        _strict_id(project_id, "project")
        _strict_scope(scope)
        if agent_id is not None:
            _strict_id(agent_id, "agent")
        _strict_change(change)
        if source_seq is not None:
            _strict_cursor(source_seq, "source sequence")
        with self._lock:
            self._require_open()
            event = self._append_locked(project_id, scope, agent_id, change,
                                        source_seq)
            self._db.commit()
        if event is not None:
            self._fanout(project_id, [event])
        return event

    def publish_batch(self, project_id, items, *, watermark):
        """Bridge path: append events and advance the durable catch-up watermark in
        ONE transaction, so a crash between canonical commit and publication is
        repaired by the next scan. ``items`` are (resource_id, scope, agent_id,
        change, source_seq) tuples; ``resource_id`` enables durable
        previous-scope tracking — a shared->private transition prepends a
        WITHDRAWAL-only generic shared invalidation (agent_id None, no ids), so
        stale public projections are recomputed away while the private event
        itself stays in the agent scope and never touches shared cursors.
        Returns the published events."""
        _strict_id(project_id, "project")
        _strict_cursor(watermark, "watermark")
        published = []
        with self._lock:
            self._require_open()
            for resource_id, scope, agent_id, change, source_seq in items:
                if resource_id is not None:
                    old = self._db.execute(
                        "SELECT scope FROM resource_scopes WHERE project_id=?"
                        " AND resource_id=?", (project_id, resource_id)).fetchone()
                    if old is not None and old[0] != scope:
                        # Scope transition: the OLD audience must drop its cached
                        # projection. public->private withdraws shared caches;
                        # private A->private B withdraws A's scope only (never
                        # shared — that would leak B's traffic into shared
                        # cursors); private->public needs nothing (the shared
                        # event itself reaches the old audience). Withdrawals are
                        # always generic: no ids, notes, or per-scope counts.
                        target = None
                        if old[0] == "shared":
                            target = "shared"
                        elif scope != "shared":
                            target = old[0]
                        if target is not None:
                            withdrawal = self._append_locked(
                                project_id, target, None, "withdrawal", source_seq)
                            if withdrawal is not None:
                                published.append(withdrawal)
                    self._db.execute(
                        "INSERT INTO resource_scopes(project_id, resource_id,"
                        " scope) VALUES(?,?,?) ON CONFLICT(project_id,"
                        " resource_id) DO UPDATE SET scope=excluded.scope",
                        (project_id, resource_id, scope))
                event = self._append_locked(project_id, scope, agent_id, change,
                                            source_seq)
                if event is not None:
                    published.append(event)
            # Monotonic watermark: a lagging bridge can never regress the
            # catch-up edge another bridge already advanced.
            self._db.execute(
                "INSERT INTO catchup_watermarks(project_id, seq) VALUES(?,?)"
                " ON CONFLICT(project_id) DO UPDATE SET"
                " seq=MAX(seq, excluded.seq)",
                (project_id, watermark))
            self._db.commit()
        if published:
            self._fanout(project_id, published)
        return published

    # ------------------------------------------------------------------ replay
    def _replay_locked(self, project_id, viewer, cursor, limit):
        kind = _viewer_kind(viewer)
        if kind == "admin":
            rows = self._db.execute(
                "SELECT scope, scope_seq, log_seq, agent_id, change FROM"
                " activity_events WHERE project_id=? AND log_seq>?"
                " ORDER BY log_seq LIMIT ?",
                (project_id, cursor["g"], limit)).fetchall()
            oldest = self._db.execute(
                "SELECT MIN(log_seq) FROM activity_events WHERE project_id=?",
                (project_id,)).fetchone()[0]
            resync = oldest is not None and cursor["g"] < oldest - 1
        elif kind == "agent":
            scope = f"agent:{viewer['agent']}"
            rows = self._db.execute(
                "SELECT scope, scope_seq, log_seq, agent_id, change FROM"
                " activity_events WHERE project_id=? AND"
                " ((scope='shared' AND scope_seq>?) OR (scope=? AND scope_seq>?))"
                " ORDER BY created_at, log_seq LIMIT ?",
                (project_id, cursor["s"], scope, cursor["a"], limit)).fetchall()
            oldest_shared = self._db.execute(
                "SELECT MIN(scope_seq) FROM activity_events WHERE project_id=?"
                " AND scope='shared'", (project_id,)).fetchone()[0]
            oldest_own = self._db.execute(
                "SELECT MIN(scope_seq) FROM activity_events WHERE project_id=?"
                " AND scope=?", (project_id, scope)).fetchone()[0]
            resync = (oldest_shared is not None and cursor["s"] < oldest_shared - 1) or \
                     (oldest_own is not None and cursor["a"] < oldest_own - 1)
        else:
            rows = self._db.execute(
                "SELECT scope, scope_seq, log_seq, agent_id, change FROM"
                " activity_events WHERE project_id=? AND scope='shared'"
                " AND scope_seq>? ORDER BY scope_seq LIMIT ?",
                (project_id, cursor["s"], limit)).fetchall()
            oldest = self._db.execute(
                "SELECT MIN(scope_seq) FROM activity_events WHERE project_id=?"
                " AND scope='shared'", (project_id,)).fetchone()[0]
            resync = oldest is not None and cursor["s"] < oldest - 1
        events = [{"scope": row[0], "scope_seq": row[1], "log_seq": row[2],
                   "agent_id": row[3], "change": row[4]} for row in rows]
        # A cursor older than the retained window means events were truncated away:
        # flag an explicit resync instead of pretending the replay is complete.
        return events, resync

    def replay(self, project_id, viewer, after=None, limit=None):
        """Durable scoped replay window. ``after`` is a rendered cursor string for
        this viewer class; ``limit`` must be within 1..policy bound when given —
        out-of-range values are denied, never silently clamped."""
        _strict_id(project_id, "project")
        _validate_viewer(viewer)
        cursor = _parse_cursor(after, viewer) if after else {"g": 0, "s": 0, "a": 0}
        if limit is None:
            limit = self.policy.replay_limit
        elif type(limit) is not int or not 1 <= limit <= self.policy.replay_limit:
            raise ValueError("replay limit out of bounds")
        with self._lock:
            self._require_open()
            events, resync = self._replay_locked(project_id, viewer, cursor, limit)
        return {"events": events, "resync": resync}

    # -------------------------------------------------------------- subscription
    def subscribe(self, project_id, viewer, after=None):
        """One bounded subscription: admission, registration, and the replay
        backlog happen atomically under the hub lock, so live fanout can neither
        overtake nor duplicate the backlog. Raises Problem(429) past the
        per-project subscriber bound and Problem(422) for a malformed or
        foreign-class cursor — callers surface both before any response headers."""
        _strict_id(project_id, "project")
        _validate_viewer(viewer)
        cursor = _parse_cursor(after, viewer) if after else \
            {"g": 0, "s": 0, "a": 0}
        queue = Queue(maxsize=self.policy.max_pending_events)
        with self._lock:
            self._require_open()
            members = self._subscribers.setdefault(project_id, set())
            if len(members) >= self.policy.max_subscribers_per_project:
                raise Problem("activity_subscriber_limit", 429)
            backlog, resync = self._replay_locked(project_id, viewer, cursor,
                                                  self.policy.replay_limit)
            subscription = _Subscription(project_id, viewer, queue, resync, cursor)
            members.add(subscription)
            for event in backlog:
                # Advance the durable-cursor components across replayed events so
                # the first live frame id reflects the whole delivered backlog.
                _advance_cursor(subscription, event)
                try:
                    queue.put_nowait(event)
                except Full:
                    self._overflow(subscription)
                    break
        return subscription

    def _require_open(self):
        if self._closed:
            raise Problem("activity_hub_closed", 503)

    def _overflow(self, subscription):
        try:
            subscription.queue.put_nowait(_OVERFLOW)
        except Full:
            try:
                subscription.queue.get_nowait()  # drop oldest, keep the signal honest
                subscription.queue.put_nowait(_OVERFLOW)
            except (Empty, Full):
                pass

    def unsubscribe(self, subscription):
        with self._lock:
            members = self._subscribers.get(subscription.project_id)
            if members is not None:
                members.discard(subscription)
                if not members:
                    del self._subscribers[subscription.project_id]

    # ------------------------------------------------------------ catchup state
    def watermark(self, project_id):
        with self._lock:
            self._require_open()
            row = self._db.execute(
                "SELECT seq FROM catchup_watermarks WHERE project_id=?",
                (project_id,)).fetchone()
        return int(row[0]) if row else None

    def watermarked_projects(self):
        with self._lock:
            self._require_open()
            rows = self._db.execute(
                "SELECT project_id FROM catchup_watermarks").fetchall()
        return [row[0] for row in rows]

    def interested_projects(self):
        """Projects worth a presence-maintenance pass: durably watermarked or
        currently subscribed. Bounds the timer's work."""
        with self._lock:
            self._require_open()
            rows = self._db.execute(
                "SELECT project_id FROM catchup_watermarks").fetchall()
            return {row[0] for row in rows} | set(self._subscribers)

    def init_watermark(self, project_id, seq):
        """First-bind catch-up baseline: history before install is not signaled."""
        _strict_id(project_id, "project")
        _strict_cursor(seq, "watermark")
        with self._lock:
            self._require_open()
            self._db.execute(
                "INSERT INTO catchup_watermarks(project_id, seq) VALUES(?,?)"
                " ON CONFLICT(project_id) DO NOTHING", (project_id, seq))
            self._db.commit()

    def adopt_sources(self, project_id, pairs):
        """CAS adoption of baseline source visibility scopes.

        ``pairs`` are ``(resource_id, scope)`` observed in the SAME canonical
        snapshot as an about-to-be-emitted baseline. Adoption is insert-only
        (``INSERT OR IGNORE``): it can never overwrite the scanner's recorded
        scope history. If the scanner already moved a resource to a different
        scope before adoption landed, the just-computed baseline is stale for
        that resource, so adoption itself emits the generic withdrawal (into
        the baseline's observed scope) that the scanner skipped — the two
        orders are both correct and withdrawal history is never lost.
        Strictly validated and hard-capped: over the cap this fails closed
        (Problem 503) rather than silently accepting an untracked baseline."""
        _strict_id(project_id, "project")
        pairs = list(pairs)
        if len(pairs) > MAX_BASELINE_SOURCES:
            raise Problem("baseline_sources_exceeded", 503)
        for resource_id, scope in pairs:
            _strict_id(resource_id, "resource")
            _strict_scope(scope)
        withdrawals = []
        with self._lock:
            self._require_open()
            for resource_id, scope in pairs:
                existing = self._db.execute(
                    "SELECT scope FROM resource_scopes WHERE project_id=?"
                    " AND resource_id=?", (project_id, resource_id)).fetchone()
                if existing is None:
                    self._db.execute(
                        "INSERT OR IGNORE INTO resource_scopes(project_id,"
                        " resource_id, scope) VALUES(?,?,?)",
                        (project_id, resource_id, scope))
                elif existing[0] != scope:
                    event = self._append_locked(project_id, scope, None,
                                                "withdrawal", None)
                    if event is not None:
                        withdrawals.append(event)
            self._db.commit()
        if withdrawals:
            self._fanout(project_id, withdrawals)
        return len(pairs)

    # -------------------------------------------------------------- lifecycle
    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = [sub for members in self._subscribers.values()
                           for sub in members]
            self._subscribers.clear()
        for subscription in subscribers:
            try:
                subscription.queue.put_nowait(_CLOSE)
            except Full:
                pass
        self._db.close()

    def stats(self):
        """Safe at any time, including after close; global counts only, so traffic
        stats never reveal per-project or per-scope volume."""
        with self._lock:
            count = sum(len(members) for members in self._subscribers.values())
            closed = self._closed
            db = None if closed else self._db
        signals = None
        if db is not None:
            try:
                signals = int(db.execute(
                    "SELECT COALESCE(SUM(next_log - 1), 0) FROM project_log"
                ).fetchone()[0])
            except Exception:
                signals = None
        return {"closed": closed, "subscribers": count, "signals": signals,
                "path": self.path}


def _validate_viewer(viewer):
    if not isinstance(viewer, dict) or not isinstance(viewer.get("role"), str) \
            or not viewer.get("id"):
        raise ValueError("Activity viewer must be a principal-shaped mapping")
    if viewer.get("agent") is not None:
        _strict_id(viewer["agent"], "viewer agent")


def _advance_cursor(subscription, event):
    """Advance the subscription's per-scope cursor components. Only scopes the
    viewer is authorized for ever advance — a cursor never counts foreign traffic."""
    kind = _viewer_kind(subscription.viewer)
    if kind == "admin":
        subscription.cursor["g"] = max(subscription.cursor["g"], event["log_seq"])
    elif event["scope"] == "shared":
        subscription.cursor["s"] = max(subscription.cursor["s"], event["scope_seq"])
    else:
        subscription.cursor["a"] = max(subscription.cursor["a"], event["scope_seq"])


class ActivityBridge:
    """Converts canonical changes into identity/change-class activity events.

    Two drives, one bounded thread:

    - **Commits** (``Store.subscribe_commits``): the callback receives only
      ``{project: seq}`` under no SQLite lock of ours; it does a nonblocking queue
      put (a full queue just sets the dirty flag — the polling tick covers it).
      The bridge thread scans committed canonical run/task/agent rows newer than
      the durable per-project watermark under short ``store.lock`` snapshots,
      extracting agent identity and change class only (bodies are parsed and
      immediately discarded, never retained), and publishes through
      :meth:`ActivityStreamHub.publish_batch`, which advances the watermark in the
      same transaction as the inserts — a crash between commit and publish is
      repaired by the next scan. Rows whose body was wiped by deletion still carry
      their ``private_agent`` column, so a deleted private run invalidates only its
      own agent scope.
    - **Presence timer** (bounded ``presence_interval``): canonical heartbeats
      rewrite run bodies WITHOUT ``store.tick``, so no commit signal exists for
      them. The timer re-derives per-run presence (online/stale/offline by
      ``last_seen_at`` gap, matching the roster's thresholds) and a volatile
      checksum under one short snapshot, and publishes only *transitions* — this
      covers heartbeat liveness and the purely time-driven online->stale->offline
      expirations. It is presence maintenance/catch-up only: task/run/roster
      bodies are never polled here.
    """

    def __init__(self, store, hub, *, poll_interval=5.0, presence_interval=None,
                 batch_limit=500):
        if not callable(getattr(store, "subscribe_commits", None)):
            raise ValueError("Activity bridge requires Store.subscribe_commits")
        if type(poll_interval) not in (int, float) or \
                not 0.1 <= float(poll_interval) <= 600.0:
            raise ValueError("poll_interval out of bounds")
        if type(batch_limit) is not int or not 1 <= batch_limit <= 5000:
            raise ValueError("batch_limit out of bounds")
        self._store = store
        self._hub = hub
        self._poll_interval = float(poll_interval)
        self._presence_interval = float(
            presence_interval if presence_interval is not None
            else hub.policy.presence_interval)
        if not 1.0 <= self._presence_interval <= MAX_PRESENCE_SECONDS:
            raise ValueError("presence_interval out of bounds")
        self._batch_limit = batch_limit
        self._queue = Queue(maxsize=1024)
        self._dirty = set()
        self._guard = threading.Lock()  # guards _dirty, _presence*, _catchup_index
        self._presence = {}  # run_id -> (project, scope, agent_id, presence)
        self._presence_page = {}  # project -> keyset pagination cursor (run id)
        self._presence_seen = {}  # project -> run ids seen in the current cycle
        self._catchup_index = 0  # round-robin position for periodic catch-up
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._closed = False
        self._started = False
        # Subscribe before baselining so a commit racing the baseline is still
        # queued as a wake and caught by the watermark it lands after.
        self._unsubscribe = store.subscribe_commits(self._on_commit)
        self._baseline()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="hivemind-activity-stream-bridge")

    def _baseline(self):
        """Bind-time baseline under one short snapshot: existing projects get their
        durable watermark at the current committed edge (pre-install history is not
        signaled). On restart the durable watermarks are kept (init is a no-op), so
        catch-up resumes exactly from the crash point."""
        with self._store.lock:
            rows = self._store.db.execute("SELECT id, seq FROM projects").fetchall()
        for project, seq in rows:
            if ID_PATTERN.match(str(project)):
                self._hub.init_watermark(str(project), int(seq))

    # --------------------------------------------------------------- callback
    def _on_commit(self, changes):
        """Nonblocking wake only: never SQLite, never content, never blocking."""
        if not isinstance(changes, dict):
            return
        for project in changes:
            if not isinstance(project, str):
                continue
            try:
                self._queue.put_nowait(project)
            except Full:
                with self._guard:
                    self._dirty.add(project)
        self._wake.set()

    # ------------------------------------------------------------------- scan
    def _scan(self, project):
        hub = self._hub
        watermark = hub.watermark(project)
        if watermark is None:
            # A project first seen after binding: every row is newer than the
            # install baseline, so catch up from zero (bounded pages, deduped).
            watermark = 0
        published = 0
        while not self._stop.is_set():
            with self._store.lock:  # short authoritative snapshot, off the wake path
                rows = self._store.db.execute(
                    "SELECT id, kind, private_agent, seq, body FROM resources"
                    " WHERE project=? AND kind IN"
                    " ('run','task','agent','board_post','memory')"
                    " AND seq>? ORDER BY seq LIMIT ?",
                    (project, watermark, self._batch_limit)).fetchall()
            if not rows:
                return published
            items = [self._extract(row) for row in rows]
            # The parsed body goes out of scope here: identity only survives.
            published += len(hub.publish_batch(project, items,
                                               watermark=int(rows[-1]["seq"])))
            watermark = int(rows[-1]["seq"])
            if len(rows) < self._batch_limit:
                return published
        return published

    @staticmethod
    def _extract(row):
        """Identity/change-class extraction. A wiped body (deletion) still yields
        a scoped roster invalidation so hidden/deleted runs never linger in any
        subscriber's projection."""
        scope = _scope_for(row["private_agent"])
        try:
            body = json.loads(row["body"]) if row["body"] else {}
        except ValueError:
            body = {}
        if row["kind"] == "agent":
            agent_id, change = row["id"], "roster"
        elif row["kind"] == "run":
            agent_id, change = body.get("agent_id"), "status"
        elif row["kind"] == "task":
            # The executing agent's projection is what changes.
            agent_id = (body.get("execution") or {}).get("agent_id")
            change = "task"
        else:
            # board_post/memory: potential checkpoint evidence sources. Their
            # writes/revocations can withdraw a derived summary, so they raise an
            # unattributable task invalidation (subscribers recompute the roster);
            # message traffic is never scanned and never counted anywhere.
            agent_id, change = None, "task"
        if agent_id is not None and not ID_PATTERN.match(str(agent_id)):
            agent_id = None  # Unattributable: subscribers recompute the roster.
        return (row["id"], scope, agent_id, change, int(row["seq"]))

    # ------------------------------------------------------- presence maintenance
    def _presence_pass(self):
        """One bounded presence step per interested project. Never a global
        all-body snapshot: each project is keyset-paginated (round-robin cursor
        per project) with a strict per-tick batch bound, and SQL projects only
        the needed JSON fields (agent_id/status/last_seen_at) — full run bodies
        and foreign projects are never read."""
        try:
            projects = sorted(self._hub.interested_projects())
        except Problem:
            return
        for project in projects:
            if self._stop.is_set():
                return
            try:
                self._presence_page_for(project)
            except Exception:
                pass  # The next bounded tick resumes from the same page cursor.

    def _presence_page_for(self, project):
        batch = self._hub.policy.presence_batch
        with self._guard:
            after = self._presence_page.get(project, "")
        with self._store.lock:  # short snapshot: metadata projection only
            rows = self._store.db.execute(
                "SELECT id, private_agent,"
                " json_extract(body,'$.agent_id'), json_extract(body,'$.status'),"
                " json_extract(body,'$.last_seen_at') FROM resources"
                " WHERE kind='run' AND deleted=0 AND project=? AND id>?"
                " ORDER BY id LIMIT ?", (project, after, batch)).fetchall()
        now = time.time()
        seen = set()
        for row in rows:
            run_id = row["id"]
            agent_id = row[2]
            if agent_id is not None and not ID_PATTERN.match(str(agent_id)):
                agent_id = None
            try:
                last_seen = float(row[4] or 0)
            except (TypeError, ValueError):
                last_seen = 0.0
            gap = now - last_seen
            presence = ("offline" if gap >= PRESENCE_OFFLINE_SECONDS
                        else "stale" if gap >= PRESENCE_STALE_SECONDS else "online")
            if row[3] in ("completed", "failed", "cancelled"):
                presence = "offline"
            state = (project, _scope_for(row["private_agent"]), agent_id,
                     presence)
            seen.add(run_id)
            with self._guard:
                previous = self._presence.get(run_id)
                if previous != state:
                    self._presence[run_id] = state
            if previous is not None and previous == state:
                continue
            if previous is None and presence == "online":
                # Baseline adoption on (re)start: a currently-online run is already
                # covered by commit-driven status events; only transitions matter.
                continue
            try:
                self._hub.publish(state[0], state[1], state[2], "presence")
            except Problem:
                return  # Hub closed mid-pass; the stop flag ends the loop.
        with self._guard:
            cycle = self._presence_seen.setdefault(project, set()) | seen
            if len(rows) < batch:
                # Full cycle completed: forget runs that vanished mid-cycle
                # (their removal is signaled by the commit path, not presence).
                for run_id, known in list(self._presence.items()):
                    if known[0] == project and run_id not in cycle:
                        del self._presence[run_id]
                self._presence_seen[project] = set()
                self._presence_page[project] = ""
            else:
                self._presence_seen[project] = cycle
                self._presence_page[project] = rows[-1]["id"]

    # ------------------------------------------------------------------- loop
    def start(self):
        self._started = True
        self._thread.start()
        return self

    def _run(self):
        # Startup/restart catch-up: scan every project with a durable watermark so
        # a crash between a commit and its publication is repaired from the log.
        for project in self._safe_projects():
            try:
                self._scan(project)
            except Exception:
                pass
        last_presence = 0.0
        while not self._stop.is_set():
            pending = []
            while True:
                try:
                    pending.append(self._queue.get_nowait())
                except Empty:
                    break
            with self._guard:
                pending.extend(self._dirty)
                self._dirty.clear()
            for project in dict.fromkeys(pending):
                try:
                    self._scan(project)
                except Exception:
                    pass  # The periodic catch-up below retries from the durable
                    # watermark even when no further commit ever arrives.
            # Bounded periodic durable-watermark catch-up: round-robin over
            # watermarked + currently-subscribed projects (a first subscription
            # to a project unseen since binding catches up from zero). This is
            # what retries a failed scan — wake-ups alone could lose a commit
            # whose only scan attempt raised.
            for project in self._catchup_projects():
                if self._stop.is_set():
                    break
                try:
                    self._scan(project)
                except Exception:
                    pass  # Watermark unchanged; the next round retries it.
            now = time.time()
            if now - last_presence >= self._presence_interval:
                last_presence = now
                try:
                    self._presence_pass()
                except Exception:
                    pass  # Presence maintenance retries on the next bounded tick.
            self._wake.clear()
            # Bounded wait slices: a stop/wake landing between clear() and wait()
            # must never cost a full poll interval — close() stays promptly
            # joinable and lost wakes are bounded by the slice, not the interval.
            remaining = min(self._poll_interval, self._presence_interval)
            while remaining > 0 and not self._stop.is_set():
                step = min(0.5, remaining)
                if self._wake.wait(step):
                    break
                remaining -= step
        # A wake lost near shutdown is still covered by the durable watermark on
        # the next bridge's startup catch-up; presence is recomputed from scratch.

    def _safe_projects(self):
        try:
            return self._hub.watermarked_projects()
        except Exception:
            return []

    def _catchup_projects(self):
        """Fair round-robin slice of projects due a durable catch-up scan."""
        try:
            projects = sorted(self._hub.interested_projects())
        except Exception:
            return []
        if not projects:
            return []
        batch = min(self._hub.policy.catchup_batch, len(projects))
        with self._guard:
            start = self._catchup_index % len(projects)
            self._catchup_index = (start + batch) % len(projects)
        return (projects[start:] + projects[:start])[:batch]

    def tick(self, project):
        """Explicit scan request (e.g. a test or a parent's polling fallback)."""
        return self._scan(project)

    def presence_tick(self):
        """Explicit presence-maintenance pass (tests; the loop calls this on its
        bounded timer)."""
        self._presence_pass()

    def close(self, timeout=10.0):
        """Honest bounded stop: returns True only when the thread is actually
        not alive. A timed-out close returns False, leaves the handle reusable
        (a later close retries the join), and means the parent lifespan must
        NOT tear down the hub/store this bridge still depends on."""
        self._stop.set()
        self._wake.set()
        if callable(self._unsubscribe):
            self._unsubscribe()
            self._unsubscribe = None
        if self._started and self._thread.is_alive():
            self._thread.join(timeout=float(timeout))
        alive = self._started and self._thread.is_alive()
        self._closed = not alive
        return not alive

    def unsubscribe(self):
        return self.close()


OPERATION_DESCRIPTORS = {
    "activity_stream.stream": {
        "name": "activity_stream.stream",
        "purpose": "Authenticated per-project SSE stream of authorized agent "
                   "activity projections: snapshot baseline plus per-agent "
                   "activity updates (status/presence/current-task context) with "
                   "durable scoped replay cursors.",
        "input": "GET /v1/projects/{project_id}/agents:stream?after "
                 "(or Last-Event-ID); Bearer/session principal with project access.",
        "output": "text/event-stream frames: id=<scoped cursor g<n>|s<n>|s<n>.a<m>>, "
                  "event=snapshot|activity, data={type, agents|agent_id, change, "
                  "agent, removed}; heartbeat comments; resync/shutdown advisories.",
        "authority": "requires the principal to currently have access to the target "
                     "project, re-verified under short canonical snapshots before "
                     "every snapshot frame, replay frame, live frame, and heartbeat, "
                     "including a constant fingerprint (role, agent, project, "
                     "token_hash) so credential rotation or role/agent downgrade "
                     "closes the stream immediately; each projection is recomputed "
                     "through the authorized activity_roster view, and every "
                     "baseline CAS-adopts its visible source scopes under the same "
                     "canonical snapshot so later privatization always produces a "
                     "generic withdrawal — hidden/deleted/private runs and tasks "
                     "are never emitted and revocation closes the stream.",
        "side_effects": "none (read-only stream; one bounded subscription slot).",
        "limits": "per-project subscriber bound admitted before headers, bounded "
                  "pending queue with definite resync backpressure, bounded replay "
                  "window and per-scope log retention, bounded stream lifetime "
                  "(reconnect with cursor), bounded frame bytes and snapshot chunks.",
        "retry_safety": "at-least-once; reconnect with after=<last id> replays the "
                        "durable scoped window; a truncated window yields an "
                        "explicit resync advisory followed by a fresh snapshot. "
                        "Consumers should deduplicate by (agent_id, revision, "
                        "presence). No exactly-once guarantee is made or implied.",
    },
    "activity_stream.commit_bridge": {
        "name": "activity_stream.commit_bridge",
        "purpose": "Converts committed canonical invalidations plus bounded "
                   "volatile-presence maintenance into scoped identity/change "
                   "activity events, off the commit path.",
        "input": "Store.subscribe_commits callbacks ({project: seq}); durable "
                 "per-project catch-up watermarks; short canonical snapshots of "
                 "run identity/last_seen_at/status for presence transitions.",
        "output": "Durable scoped activity events; returns a close()/unsubscribe() "
                  "handle.",
        "authority": "publishes agent identity and change class only, extracted "
                     "from committed canonical rows under short snapshots; "
                     "visibility is enforced again at fanout, replay, and stream "
                     "time. Never publishes uncommitted writes or content.",
        "side_effects": "appends to the durable event log; wakes current "
                        "subscribers; one daemon thread with a polling fallback.",
        "limits": "bounded scan batches; nonblocking wake queue with dirty-flag "
                  "fallback; bounded round-robin periodic catch-up from durable "
                  "watermarks retries failed scans without any new commit; "
                  "bounded presence timer (presence maintenance and expiration "
                  "catch-up only — keyset-paginated per-project metadata "
                  "projection, never all-body polling); watermark/outbox dedup "
                  "makes crashes recoverable.",
        "retry_safety": "a crash between commit and publish is repaired by the next "
                        "scan from the durable watermark; duplicate publications "
                        "are deduplicated by (scope, agent, change, source seq); "
                        "presence republishes at most current state after restart.",
    },
    "activity_stream.client_subscription": {
        "name": "activity_stream.client_subscription",
        "purpose": "Cancellable client-side subscription helper for the Python SDK "
                   "(parent wires the facade): parses bounded SSE frames, maintains "
                   "a durable identity-bound cursor file, closes within a bounded "
                   "read window.",
        "input": "httpx client, stream path, cancel event / close(), optional "
                 "cursor path bound to the exact project identity.",
        "output": "iterator of typed frames {id, event, data}; ActivityStreamError "
                  "with fixed text on malformed input.",
        "authority": "carries only what the server emitted; the durable cursor file "
                     "is strictly validated and refused when foreign or malformed.",
        "side_effects": "atomic cursor file writes (tmp + os.replace).",
        "limits": "bounded line/frame bytes, bounded frames per stream, bounded "
                  "read timeout and close join.",
        "retry_safety": "reconnects resume from the durable cursor; a resync frame "
                        "resets the baseline to the server's fresh snapshot.",
    },
}


def descriptors():
    """Self-describing operation metadata for parent discovery/MCP registration."""
    return {name: dict(descriptor) for name, descriptor in OPERATION_DESCRIPTORS.items()}


# ------------------------------------------------------------------ SSE frames
def _frame(subscription, event, data):
    """Render one bounded SSE frame. Data must be a nonempty JSON object within
    the byte cap; the id reflects this subscription's scoped cursor."""
    if event not in EVENT_KINDS:
        raise ActivityStreamError("Unknown activity stream event")
    payload = json.dumps(data, separators=(",", ":"))
    if not payload or payload == "{}":
        raise ActivityStreamError("Activity frames carry nonempty data")
    cursor = _render_cursor(subscription.viewer, subscription.cursor)
    text = f"id: {cursor}\nevent: {event}\ndata: {payload}\n\n"
    if len(text.encode("utf-8")) > MAX_FRAME_BYTES:
        raise ActivityStreamError("Activity frame budget exceeded")
    return text


def _bounded_entry(entry):
    """Trim one roster entry deterministically until its frame fits: drop the run
    list, then the summary. The minimal projection (identity/status/presence)
    always fits the byte cap."""
    candidate = entry
    if len(json.dumps(candidate)) > MAX_FRAME_BYTES // 2:
        context = {k: v for k, v in (candidate.get("context") or {}).items()
                   if k != "runs"}
        candidate = {**candidate, "context": {**context, "runs_truncated": True}}
    if len(json.dumps(candidate)) > MAX_FRAME_BYTES // 2:
        candidate = {"id": entry["id"], "display_name": entry.get("display_name"),
                     "status": entry.get("status"),
                     "current_task": None, "summary": None,
                     "context": {"presence": (entry.get("context") or {}).get("presence"),
                                 "authority": "canonical_reported_activity"}}
    return candidate


def _baseline_sources(store, project_id, entries):
    """Visible source ids behind a baseline: runs, current tasks, and their
    checkpoint evidence refs (whose revocation/privatization must withdraw a
    derived summary). Metadata projection only — evidence refs are extracted as
    small JSON fragments via SQL, full bodies are never parsed. Scoped to this
    exact project; hard-capped (fail closed) so an untracked baseline is never
    silently accepted."""
    ids = set()
    task_ids = set()
    for entry in entries:
        context = entry.get("context") or {}
        if context.get("run_id"):
            ids.add(context["run_id"])
        for run in context.get("runs") or []:
            if run.get("run_id"):
                ids.add(run["run_id"])
        task = entry.get("current_task")
        if task and task.get("id"):
            ids.add(task["id"])
            task_ids.add(task["id"])
    refs = set()
    for task_id in sorted(task_ids):
        row = store.db.execute(
            "SELECT json_extract(body,'$.execution.checkpoint.evidence_refs'),"
            " json_extract(body,'$.handoff.checkpoint.evidence_refs')"
            " FROM resources WHERE id=? AND kind='task' AND deleted=0",
            (task_id,)).fetchone()
        for fragment in (tuple(row) if row else ()):
            try:
                items = json.loads(fragment) if fragment else []
            except ValueError:
                items = []
            for ref in items[:MAX_REFS_PER_TASK]:
                if isinstance(ref, dict) and isinstance(ref.get("id"), str) \
                        and ID_PATTERN.match(ref["id"]):
                    refs.add(ref["id"])
    ids |= refs
    if len(ids) > MAX_BASELINE_SOURCES:
        raise Problem("baseline_sources_exceeded", 503)
    pairs = []
    ordered = sorted(ids)
    for index in range(0, len(ordered), 500):
        chunk = ordered[index:index + 500]
        marks = ",".join("?" for _ in chunk)
        rows = store.db.execute(
            "SELECT id, private_agent FROM resources WHERE project=?"
            f" AND id IN ({marks})", (project_id, *chunk)).fetchall()
        pairs.extend((row["id"], _scope_for(row["private_agent"]))
                     for row in rows)
    return pairs


def _sse_frames(service, hub, policy, principal_id, project_id, subscription, *,
                principal_fingerprint=None, reauthorize=None):
    """Snapshot baseline, replay backlog, then live events; every emission is
    preceded by a fresh authorization snapshot and every projection is recomputed
    through the authorized roster. All waiting happens on the subscription queue —
    never while holding Request/store locks (detached route)."""
    from .agent_activity import activity_roster

    store = service.store
    last_sent = {}  # agent_id -> canonical digest of the entry we emitted

    def fresh_principal():
        """Fresh short snapshot: principal still exists and still has access to
        this exact project. Revocation closes the stream silently."""
        with store.lock:
            if reauthorize is not None:
                try:
                    authenticated = reauthorize()
                except Problem:
                    return None
                if authenticated["id"] != principal_id:
                    return None
            row = store.db.execute("SELECT * FROM principals WHERE id=?",
                                   (principal_id,)).fetchone()
            if row is None:
                return None
            current = dict(row)
            if principal_fingerprint is not None and any(
                    current[key] != value for key, value in principal_fingerprint.items()):
                return None
            try:
                service.project_access(current, project_id)
            except Problem:
                return None
            return current

    def roster(current):
        with store.lock:
            return activity_roster(service, current, project_id)

    def snapshot_locked(current):
        """Roster AND its visible source scopes under ONE canonical snapshot, so
        adoption below describes exactly the baseline that will be emitted."""
        with store.lock:
            entries = activity_roster(service, current, project_id)
            pairs = _baseline_sources(store, project_id, entries)
        return entries, pairs

    def emit_snapshot(current):
        entries, pairs = snapshot_locked(current)
        # Adopt the baseline's source scopes BEFORE the baseline is emitted
        # (fail-closed on the cap): a later privatization/scope transition of
        # anything this baseline showed is then always attributed and produces
        # a generic withdrawal — even for sources that existed before install
        # and were never scanned by the bridge.
        hub.adopt_sources(project_id, pairs)
        entries = [_bounded_entry(entry) for entry in entries]
        # Honest hard cap: excess agents are flagged, never silently dropped
        # (clients reread paginated /v1/agents for the full roster).
        truncated = len(entries) > MAX_SNAPSHOT_AGENTS
        entries = entries[:MAX_SNAPSHOT_AGENTS]
        last_sent.clear()
        for entry in entries:
            last_sent[entry["id"]] = json.dumps(entry, sort_keys=True)
        chunks, chunk = [], []
        for entry in entries:
            candidate = [*chunk, entry]
            if chunk and (len(candidate) > policy.snapshot_chunk
                          or len(json.dumps(candidate).encode()) > MAX_FRAME_BYTES - 512):
                chunks.append(chunk)
                chunk = []
            chunk.append(entry)
        if chunk or not chunks:
            chunks.append(chunk)
        frames = []
        for index, chunk in enumerate(chunks):
            frames.append(_frame(subscription, "snapshot",
                                 {"type": "snapshot", "index": index,
                                  "count": len(chunks), "agents": chunk,
                                  "truncated": truncated}))
        return frames

    try:
        current = fresh_principal()
        if current is None:
            return  # Never authorized at stream start: close without a frame.
        if subscription.resync:
            yield ("event: resync\n"
                   'data: {"reason": "cursor_outside_window"}\n\n')
        # Baseline snapshot on every (re)connect: a subscriber never sits silent
        # wondering whether it missed history while disconnected. If the
        # baseline's source set exceeds the adoption cap, fail CLOSED: an
        # explicit resync advisory and a clean close (poll /v1/agents instead),
        # never an untracked baseline that could later lose a withdrawal.
        try:
            baseline = emit_snapshot(current)
        except Problem as error:
            if error.code == "baseline_sources_exceeded":
                yield ('event: resync\n'
                       'data: {"reason": "baseline_sources_exceeded"}\n\n')
            return
        for frame in baseline:
            yield frame
        deadline = time.time() + policy.max_stream_seconds
        backlog = True
        while True:
            remaining = deadline - time.time()
            if remaining <= 0 or hub._closed:
                return  # Bounded stream lifetime: reconnect with the cursor.
            try:
                event = subscription.queue.get(
                    timeout=min(policy.heartbeat_seconds, remaining))
            except Empty:
                backlog = False
                if fresh_principal() is None:
                    return  # Authorization lapsed mid-stream: close silently.
                yield ": heartbeat\n\n"
                continue
            if event is _CLOSE:
                yield 'event: shutdown\ndata: {"reason": "hub_closed"}\n\n'
                return
            if event is _OVERFLOW:
                current = fresh_principal()
                if current is None:
                    return
                yield ('event: resync\n'
                       'data: {"reason": "backpressure"}\n\n')
                try:
                    rebaseline = emit_snapshot(current)  # definite re-baseline
                except Problem:
                    return  # Fail closed on the adoption cap; reconnect is futile.
                for frame in rebaseline:
                    yield frame
                continue
            # Drain the pending burst so one roster recompute covers it.
            events = [event]
            while True:
                try:
                    pending = subscription.queue.get_nowait()
                except Empty:
                    break
                if pending in (_CLOSE, _OVERFLOW):
                    try:
                        subscription.queue.put_nowait(pending)  # next iteration
                    except Full:
                        pass  # We just drained at least one slot; never block here.
                    break
                events.append(pending)
            for pending in events:
                _advance_cursor(subscription, pending)
            current = fresh_principal()
            if current is None:
                return
            entries = {entry["id"]: entry for entry in roster(current)}
            dirty = {e["agent_id"] for e in events if e["agent_id"]}
            if any(e["agent_id"] is None for e in events):
                # Unattributable change (e.g. a wiped body): diff the whole roster.
                dirty |= set(entries) | set(last_sent)
            for agent_id in sorted(dirty):
                change = next((e["change"] for e in reversed(events)
                               if e["agent_id"] == agent_id), "roster")
                entry = entries.get(agent_id)
                if entry is None:
                    if agent_id in last_sent:
                        del last_sent[agent_id]
                        yield _frame(subscription, "activity",
                                     {"type": "activity", "agent_id": agent_id,
                                      "change": change, "removed": True,
                                      "agent": None})
                    continue  # Never visible to this principal: never emitted.
                projection = _bounded_entry(entry)
                digest = json.dumps(projection, sort_keys=True)
                if backlog and agent_id in last_sent and \
                        last_sent[agent_id] == digest:
                    continue  # Replay already reflected in the baseline snapshot.
                if last_sent.get(agent_id) == digest:
                    continue  # Content-identical repeat: at-least-once dedup.
                last_sent[agent_id] = digest
                yield _frame(subscription, "activity",
                             {"type": "activity", "agent_id": agent_id,
                              "change": change, "removed": False,
                              "agent": projection})
            backlog = False
    finally:
        hub.unsubscribe(subscription)


# -------------------------------------------------------------------- endpoint
def install(app, service, principal, command, envelope, *, policy=None, hub=None,
            path=None, bridge=True):
    """Conventional capability install. Default-off is safe: without an enabled
    policy the route is registered but returns ``feature_disabled`` and no hub,
    bridge, or thread is created. A reinstall replaces only this module's own
    routes. With an enabled policy, attaches the service-owned hub
    (``app.state.activity_stream_hub`` / ``service.activity_stream_hub``) and,
    when the store supports it, the commit/presence bridge
    (``app.state.activity_stream_bridge``); both handles expose ``close()`` for
    the parent lifespan."""
    from .capabilities import replace_routes
    replace_routes(app, __name__)  # replace our own routes, never another module's

    policy = policy if policy is not None else ActivityStreamPolicy()
    if policy.enabled is not True:
        app.state.activity_stream_hub = None
        app.state.activity_stream_bridge = None

        @app.get("/v1/projects/{project_id}/agents:stream")
        async def agents_stream_disabled(project_id: str, p=Depends(principal)):
            service.project_access(p, project_id)
            raise Problem("feature_disabled", 503)

        agents_stream_disabled.hivemind_detached_work = True
        return None

    if hub is None:
        path = path or (Path(service.store.directory) / "activity-stream.sqlite")
        hub = ActivityStreamHub(path, policy)
    app.state.activity_stream_hub = hub
    service.activity_stream_hub = hub
    store = service.store

    @app.get("/v1/projects/{project_id}/agents:stream")
    async def agents_stream(project_id: str, request: Request, after: str = "",
                            p=Depends(principal)):
        service.project_access(p, project_id)
        viewer = {"id": p["id"], "role": p["role"], "agent": p.get("agent")}
        # Reconnecting EventSource clients send Last-Event-ID; an explicit `after`
        # query parameter wins when both are present. Cursor shape and viewer
        # class are strictly validated inside subscribe (422 before headers).
        cursor = after or request.headers.get("last-event-id") or None
        # Admission (subscriber bound, strict cursor) happens BEFORE headers: a
        # refusal is a clean 429/422, never a mid-stream failure after 200.
        subscription = hub.subscribe(project_id, viewer, after=cursor)
        return StreamingResponse(
            _sse_frames(service, hub, policy, p["id"], project_id, subscription,
                        principal_fingerprint={key: p[key] for key in
                                               ("role", "agent", "project", "token_hash")},
                        reauthorize=(lambda: app.state.authenticate_request(request))
                        if callable(getattr(app.state, "authenticate_request", None)) else None),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    # The parent middleware skips its global request/store locks for marked routes;
    # this generator guards every canonical read with its own short snapshots.
    agents_stream.hivemind_detached_work = True

    if bridge and callable(getattr(store, "subscribe_commits", None)):
        app.state.activity_stream_bridge = ActivityBridge(
            store, hub, presence_interval=policy.presence_interval).start()
    else:
        app.state.activity_stream_bridge = None
    return hub


# ------------------------------------------------------------ client primitives
def parse_activity_sse(lines, *, cancel=None, max_frames=MAX_FRAMES_PER_STREAM,
                       max_line_bytes=MAX_LINE_BYTES,
                       max_frame_bytes=MAX_FRAME_BYTES):
    """Minimal bounded SSE frame parser for the activity stream: yields
    {"id", "event", "data"} frames. Lines and frames are byte-capped, the frame
    count per stream is capped, and malformed content raises
    :class:`ActivityStreamError` whose message never echoes remote bytes.
    ``cancel`` (a threading.Event) is checked between lines so close interrupts a
    live parse. Heartbeat comments are skipped."""
    event, data, last_id = None, None, None
    data_bytes = 0
    frames = 0

    def flush():
        nonlocal event, data, data_bytes, frames
        produced = None
        if event in EVENT_KINDS and data is not None:
            try:
                payload = json.loads(data)
            except ValueError:
                raise ActivityStreamError("Malformed activity frame") from None
            if not isinstance(payload, dict) or not payload:
                raise ActivityStreamError("Malformed activity frame") from None
            if event in ("snapshot", "activity") and payload.get("type") != event:
                raise ActivityStreamError("Malformed activity frame") from None
            if last_id is not None and not CURSOR_PATTERN.match(last_id):
                raise ActivityStreamError("Malformed activity cursor") from None
            produced = {"id": last_id, "event": event, "data": payload}
            frames += 1
        if frames > max_frames:
            raise ActivityStreamError("Activity stream frame budget exceeded")
        event, data, data_bytes = None, None, 0
        return produced

    for raw in lines:
        if cancel is not None and cancel.is_set():
            return
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        line = (line or "").rstrip("\r\n")
        if len(line.encode("utf-8", "replace")) > max_line_bytes:
            raise ActivityStreamError("Activity stream line budget exceeded")
        if not line:
            produced = flush()
            if produced is not None:
                yield produced
            continue
        if line.startswith(":"):
            continue  # heartbeat comment
        field, _, value = line.partition(":")
        value = value.lstrip(" ")
        if field == "event":
            if len(value) > 64:
                raise ActivityStreamError("Malformed activity frame")
            event = value
        elif field == "data":
            data_bytes += len(value.encode("utf-8", "replace")) + 1
            if data_bytes > max_frame_bytes:
                raise ActivityStreamError("Activity stream frame budget exceeded")
            data = value if data is None else data + "\n" + value
        elif field == "id":
            if not CURSOR_PATTERN.match(value):
                raise ActivityStreamError("Malformed activity cursor")
            last_id = value
    produced = flush()
    if produced is not None:
        yield produced


def httpx_activity_subscription(http_client, path, *, cancel, headers=None,
                                read_timeout=10.0,
                                max_frames=MAX_FRAMES_PER_STREAM):
    """Bounded, cancellable SSE subscription over ``http_client.stream``.

    ``cancel`` (threading.Event) interrupts an active read within ``read_timeout``
    seconds, so :meth:`ActivitySubscription.close` never waits on an unbounded
    network read. Status/content-type failures raise :class:`ActivityStreamError`
    with fixed text (no remote bodies). A clean server-side close simply ends the
    iterator — the caller reconnects with its durable cursor.
    """
    import httpx

    if not 1.0 <= float(read_timeout) <= 120.0:
        raise ValueError("read_timeout out of bounds")
    timeout = httpx.Timeout(connect=10.0, read=float(read_timeout),
                            write=10.0, pool=10.0)
    with http_client.stream("GET", path, headers=headers or {},
                            timeout=timeout) as response:
        if response.status_code != 200:
            raise ActivityStreamError(
                f"Activity stream rejected with status {response.status_code}")
        if "text/event-stream" not in response.headers.get("content-type", ""):
            raise ActivityStreamError("Activity stream returned a non-SSE response")
        try:
            yield from parse_activity_sse(response.iter_lines(), cancel=cancel,
                                          max_frames=max_frames)
        except httpx.ReadTimeout:
            return  # Idle read window elapsed; the caller reconnects.


class ActivitySubscription:
    """Cancellable client-side activity subscription for the Python SDK (the
    parent wires the facade; this helper is self-contained).

    ``frames()`` is a blocking generator of typed frames ({"id", "event",
    "data"}); every frame updates the durable cursor. The cursor lives in a
    private cursor file (``cursor_path``): written atomically (tmp + os.replace),
    strictly validated (exact project identity binding — a foreign or malformed
    file is refused, never trusted), so restarts resume instead of replaying or
    skipping. A server ``resync`` advisory resets the local cursor to the fresh
    snapshot baseline that follows it. ``close()`` interrupts an active read
    within its read window; without ``cursor_path`` the subscription is ephemeral
    (cursor kept in memory only).
    """

    def __init__(self, http_client, *, project_id, path_prefix="/v1", headers=None,
                 cursor_path=None, read_timeout=10.0,
                 max_frames=MAX_FRAMES_PER_STREAM):
        _strict_id(project_id, "project")
        if not 1.0 <= float(read_timeout) <= 120.0:
            raise ValueError("read_timeout out of bounds")
        self._http_client = http_client
        self._project_id = project_id
        self._path = f"{path_prefix}/projects/{project_id}/agents:stream"
        self._headers = dict(headers or {})
        self._read_timeout = float(read_timeout)
        self._max_frames = max_frames
        self.cancel = threading.Event()
        self._cursor_path = Path(cursor_path) if cursor_path else None
        self.cursor = self._load_cursor()
        self._closed = False

    def _load_cursor(self):
        if self._cursor_path is None:
            return None
        try:
            raw = self._cursor_path.read_text()
        except FileNotFoundError:
            return None
        except OSError:
            raise ValueError("Activity cursor file is unreadable") from None
        try:
            state = json.loads(raw)
        except ValueError:
            raise ValueError("Activity cursor file is malformed") from None
        if not isinstance(state, dict) or set(state) != {"project_id", "cursor"}:
            raise ValueError("Activity cursor file has an unexpected shape")
        if state["project_id"] != self._project_id:
            raise ValueError("Activity cursor file belongs to another project")
        cursor = state["cursor"]
        if cursor is not None and (not isinstance(cursor, str)
                                   or not CURSOR_PATTERN.match(cursor)):
            raise ValueError("Activity cursor file holds a malformed cursor")
        return cursor

    def _persist_cursor(self):
        if self._cursor_path is None:
            return
        import os
        state = {"project_id": self._project_id, "cursor": self.cursor}
        temporary = self._cursor_path.with_name(self._cursor_path.name + ".tmp")
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(state))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self._cursor_path)

    def frames(self):
        """Blocking generator of typed frames until close/error/server end."""
        while not self.cancel.is_set():
            path = self._path + (f"?after={self.cursor}" if self.cursor else "")
            try:
                for frame in httpx_activity_subscription(
                        self._http_client, path, cancel=self.cancel,
                        headers=self._headers, read_timeout=self._read_timeout,
                        max_frames=self._max_frames):
                    if self.cancel.is_set():
                        return
                    if frame["event"] == "resync":
                        # The server follows a resync with a fresh snapshot; the
                        # local cursor must not skip past events it truncated.
                        self.cursor = None
                        self._persist_cursor()
                    elif frame["id"] is not None:
                        self.cursor = frame["id"]
                        self._persist_cursor()
                    yield frame
                return  # Clean server-side close (shutdown or lifetime bound).
            except ActivityStreamError:
                raise
            except Exception:
                if self.cancel.is_set():
                    return
                # Transient transport failure: reconnect from the durable cursor.
                self.cancel.wait(1.0)

    def close(self, timeout=10.0):
        """Bounded stop: interrupts an active read within its read window and
        persists the final cursor. Safe to call from another thread."""
        if not 0 < float(timeout) <= 120.0:
            raise ValueError("close timeout out of bounds")
        self._closed = True
        self.cancel.set()
        self._persist_cursor()
        return True
