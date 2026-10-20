"""Authenticated, agent-targeted SSE push for message/notification arrival signals.

What this module is (and is not):

- It delivers **minimal invalidation signals** — the receiving agent's own message or
  notification *identity* plus a per-agent monotonic cursor — over an authenticated,
  per-agent SSE stream. It never carries message bodies, sender payloads, other
  agents' data, or private/project contents; receivers re-read canonical state
  (inbox/notifications) under their own authorization after a wake. The cursor is
  per agent, so observing it never leaks other agents' traffic volume.
- Delivery is **at-least-once, never exactly-once**: a durable per-installation
  signal log (its own SQLite file, never the canonical store) gives reconnecting
  clients an ``after=<cursor>``/``Last-Event-ID`` replay window, bounded per agent
  with definite ``resync`` advisories when a cursor falls outside the retained
  window. Consumers must deduplicate by identity (the messaging supervisor already
  does via stable message IDs and the handler journal).
- Publication is **post-commit only** and normally automatic: ``install`` binds a
  :class:`CommitSignalBridge` to ``Store.subscribe_commits`` (minimal
  ``{project: seq}`` invalidations). The bridge never touches SQLite inside the
  commit callback — it enqueues a nonblocking wake, and an off-lock thread scans
  authoritative committed canonical rows under short snapshots, extracting
  identity/recipient only (bodies are never retained). A durable per-project
  watermark plus a unique (agent, kind, item, source_seq) outbox constraint make
  crash-between-commit-and-publish recoverable: the next scan republishes exactly
  the missed window, and a polling fallback tick covers lost wake-ups. A parent may
  instead wire the bridge explicitly via :func:`bind_commit_source`.
- Authorization is **continuous**, not connect-once: the stream endpoint is marked
  ``hivemind_detached_work`` (the parent middleware skips its global request/store
  locks for marked routes), and the generator itself re-verifies, under short
  ``store.lock`` snapshots before every signal, replay frame, and heartbeat, that
  the principal still exists, still matches the project, still represents the exact
  target agent, that the agent still exists in that project, and that each item is
  still canonically visible to that agent. Hidden/deleted or foreign items are
  dropped silently — never emitted, never counted per agent. Subscriber admission
  (429 past the per-agent bound) happens before response headers are sent.
- Everything is opt-in and bounded: with no enabled policy the route is registered
  but returns ``feature_disabled`` and no hub is created. Pending queues, replay
  windows, stream lifetimes, and the per-agent log retention are all capped;
  ``MessagePushHub.close`` terminates every open stream honestly, and ``stats()``
  stays safe after close.
- The worker side (:class:`PushWakeSource`) holds its cursor in a private, atomic,
  identity-bound cursor file; every (re)subscription — even an empty stream — sets
  the wake event so the supervisor reconciles canonically, and the supervisor keeps
  an independent bounded periodic canonical reconcile (``max_reconcile_seconds``)
  plus its heartbeat/lease/control timers, so a lost publication degrades to
  polling, never to silence.

Wiring contract for the parent integrator:

1. ``capabilities.FEATURE_MODULES`` includes ``"message_push"``; options may pass
   ``{"message_push": {"policy": MessagePushPolicy(enabled=True, ...),
   "path": <optional log path>}}``. With no/absent policy the module installs
   disabled (route present, ``feature_disabled``).
2. ``install`` auto-binds the commit bridge when the service store supports
   ``subscribe_commits`` (pass ``bridge=False`` to opt out and call
   ``bind_commit_source(service.store, hub)`` yourself, e.g. from a lifespan).
   The bridge handle exposes ``close()``/``unsubscribe()``.
3. Workers pass ``push_signal=PushWakeSource(...).event`` (or any
   ``threading.Event``) to ``WorkerMessaging``/``AssignedRunMessagingSupervisor``.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from queue import Empty, Full, Queue

from fastapi import Depends, Request
from fastapi.responses import StreamingResponse

from .storage import Problem

SIGNAL_KINDS = ("message", "notification")
"""The only signal kinds the hub publishes. Identity-only, never content."""

ID_PATTERN = re.compile(
    r"^[a-z][a-z0-9]{0,31}_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}"
    r"-[0-9a-f]{12}$")
"""Strict canonical ID shape (``prefix_uuidv7``); anything else is refused."""

MAX_STREAM_SECONDS = 300.0
MAX_SUBSCRIBERS_PER_AGENT = 16
MAX_PENDING_EVENTS = 256
MAX_REPLAY_EVENTS = 1000
MIN_HEARTBEAT_SECONDS = 1.0
MAX_LINE_BYTES = 4096
MAX_FRAME_BYTES = 4096
MAX_FRAMES_PER_STREAM = 10000
MAX_CURSOR = 2**63 - 1

_OVERFLOW = object()  # Backpressure sentinel: the subscriber must resync.
_CLOSE = object()  # Shutdown sentinel: terminate the stream honestly.


class PushStreamError(Exception):
    """Definite push-stream failure. Messages are fixed text only: remote bodies
    and raw frame bytes are never echoed into errors."""


def _strict_id(value, label):
    if not isinstance(value, str) or not ID_PATTERN.match(value):
        raise ValueError(f"Push {label} must be a canonical identifier")
    return value


def _strict_cursor(value, label="cursor"):
    # type() identity, not isinstance: bool is an int subclass and must be denied.
    if type(value) is not int or not 0 <= value <= MAX_CURSOR:
        raise ValueError(f"Push {label} must be a non-negative integer")
    return value


class MessagePushPolicy:
    """Explicit opt-in and bounds for the push stream. Deny-by-default."""

    def __init__(self, *, enabled=False, max_subscribers_per_agent=4,
                 max_pending_events=64, max_stream_seconds=30.0,
                 heartbeat_seconds=5.0, replay_limit=500):
        if type(enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        if type(max_subscribers_per_agent) is not int or \
                not 1 <= max_subscribers_per_agent <= MAX_SUBSCRIBERS_PER_AGENT:
            raise ValueError("max_subscribers_per_agent out of bounds")
        if type(max_pending_events) is not int or \
                not 1 <= max_pending_events <= MAX_PENDING_EVENTS:
            raise ValueError("max_pending_events out of bounds")
        if type(max_stream_seconds) not in (int, float) or \
                not 1.0 <= float(max_stream_seconds) <= MAX_STREAM_SECONDS:
            raise ValueError("max_stream_seconds out of bounds")
        if type(heartbeat_seconds) not in (int, float) or \
                not MIN_HEARTBEAT_SECONDS <= float(heartbeat_seconds) <= 60.0:
            raise ValueError("heartbeat_seconds out of bounds")
        if type(replay_limit) is not int or not 1 <= replay_limit <= MAX_REPLAY_EVENTS:
            raise ValueError("replay_limit out of bounds")
        self.enabled = enabled
        self.max_subscribers_per_agent = max_subscribers_per_agent
        self.max_pending_events = max_pending_events
        self.max_stream_seconds = float(max_stream_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        # The replay limit doubles as the per-agent durable log retention bound.
        self.replay_limit = replay_limit

    def describe(self):
        return {"enabled": self.enabled,
                "max_subscribers_per_agent": self.max_subscribers_per_agent,
                "max_pending_events": self.max_pending_events,
                "max_stream_seconds": self.max_stream_seconds,
                "heartbeat_seconds": self.heartbeat_seconds,
                "replay_limit": self.replay_limit}


class _Subscription:
    __slots__ = ("key", "queue", "resync")

    def __init__(self, key, queue, resync):
        self.key, self.queue, self.resync = key, queue, resync


class MessagePushHub:
    """Durable per-agent signal log plus in-process fanout, owned by the service.

    The log lives in its own SQLite file (WAL), separate from the canonical store.
    Cursors are per (project, agent) so a cursor never reveals other agents'
    traffic; retention is bounded per agent and a replay whose cursor fell outside
    the retained window is flagged ``resync`` instead of silently skipping events.
    Fanout is thread-safe and non-blocking: a full subscriber queue gets the
    overflow sentinel (definite backpressure) rather than blocking publishers.
    """

    def __init__(self, path, policy=None):
        self.policy = policy or MessagePushPolicy(enabled=True)
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()
        self._lock = threading.Lock()
        self._subscribers = {}  # (project_id, agent_id) -> set[_Subscription]
        self._closed = False

    def _ensure_schema(self):
        columns = {row[1] for row in
                   self._db.execute("PRAGMA table_info(push_events)").fetchall()}
        if columns and "agent_seq" not in columns:
            # Pre-per-agent-cursor layout from an earlier development build.
            self._db.execute("DROP TABLE push_events")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS push_events("
            "project_id TEXT NOT NULL, agent_id TEXT NOT NULL,"
            "agent_seq INTEGER NOT NULL, kind TEXT NOT NULL, item_id TEXT NOT NULL,"
            "source_seq INTEGER, created_at REAL NOT NULL,"
            "PRIMARY KEY(project_id, agent_id, agent_seq))")
        self._db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS push_events_dedup"
            " ON push_events(project_id, agent_id, kind, item_id, source_seq)")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS agent_cursors("
            "project_id TEXT NOT NULL, agent_id TEXT NOT NULL,"
            "next_seq INTEGER NOT NULL, PRIMARY KEY(project_id, agent_id))")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS catchup_watermarks("
            "project_id TEXT PRIMARY KEY, seq INTEGER NOT NULL)")
        self._db.commit()

    # ------------------------------------------------------------- publication
    def _append_locked(self, project_id, agent_id, kind, item_id, source_seq):
        """Insert one signal and advance the per-agent cursor atomically, pruning
        beyond the retention bound. Returns the event, or None when the exact
        (agent, kind, item, source_seq) was already published (outbox dedup)."""
        row = self._db.execute(
            "SELECT next_seq FROM agent_cursors WHERE project_id=? AND agent_id=?",
            (project_id, agent_id)).fetchone()
        agent_seq = int(row[0]) if row else 1
        cursor = self._db.execute(
            "INSERT OR IGNORE INTO push_events(project_id, agent_id, agent_seq,"
            " kind, item_id, source_seq, created_at) VALUES(?,?,?,?,?,?,?)",
            (project_id, agent_id, agent_seq, kind, item_id, source_seq,
             time.time()))
        if cursor.rowcount == 0:
            return None  # Already published from this exact canonical source row.
        self._db.execute(
            "INSERT INTO agent_cursors(project_id, agent_id, next_seq) VALUES(?,?,?)"
            " ON CONFLICT(project_id, agent_id) DO UPDATE SET next_seq=excluded.next_seq",
            (project_id, agent_id, agent_seq + 1))
        self._db.execute(
            "DELETE FROM push_events WHERE project_id=? AND agent_id=?"
            " AND agent_seq<=?",
            (project_id, agent_id, agent_seq - self.policy.replay_limit))
        return {"seq": agent_seq, "kind": kind, "item_id": item_id}

    def _fanout(self, project_id, agent_id, events):
        with self._lock:
            subscribers = list(self._subscribers.get((project_id, agent_id), ()))
        for subscription in subscribers:
            for event in events:
                try:
                    subscription.queue.put_nowait(event)
                except Full:
                    self._overflow(subscription)
                    break

    def publish(self, project_id, agent_id, kind, item_id, *, source_seq=None):
        """Append one identity-only signal and fan it out. Returns the per-agent
        cursor, or None when deduplicated. Strict canonical IDs only; kinds are
        limited to signals — publishing never carries content."""
        _strict_id(project_id, "project")
        _strict_id(agent_id, "agent")
        _strict_id(item_id, "item")
        if kind not in SIGNAL_KINDS:
            raise ValueError("Push signals are limited to: " + ",".join(SIGNAL_KINDS))
        if source_seq is not None:
            _strict_cursor(source_seq, "source sequence")
        with self._lock:
            if self._closed:
                raise Problem("push_hub_closed", 503)
            event = self._append_locked(project_id, agent_id, kind, item_id,
                                        source_seq)
            self._db.commit()
        if event is None:
            return None
        self._fanout(project_id, agent_id, [event])
        return event["seq"]

    def publish_batch(self, project_id, items, *, watermark):
        """Bridge path: append identity-only signals and advance the durable
        catch-up watermark in ONE transaction, so a crash between commit and
        publish is repaired by the next scan. ``items`` are
        (agent_id, kind, item_id, source_seq) tuples. Returns fanned-out events."""
        _strict_id(project_id, "project")
        _strict_cursor(watermark, "watermark")
        published = []
        with self._lock:
            if self._closed:
                raise Problem("push_hub_closed", 503)
            for agent_id, kind, item_id, source_seq in items:
                event = self._append_locked(project_id, agent_id, kind, item_id,
                                            source_seq)
                if event is not None:
                    published.append((agent_id, event))
            self._db.execute(
                "INSERT INTO catchup_watermarks(project_id, seq) VALUES(?,?)"
                " ON CONFLICT(project_id) DO UPDATE SET seq=excluded.seq",
                (project_id, watermark))
            self._db.commit()
        for agent_id, event in published:
            self._fanout(project_id, agent_id, [event])
        return len(published)

    def notify_post_commit(self, record=None, *, project_id=None, agent_id=None,
                           kind=None, item_id=None):
        """Explicit post-commit hook for one committed record. Prefer the automatic
        commit bridge; this remains for parents that wire notification points by
        hand. Call ONLY after the canonical commit. Returns the cursor (or list of
        cursors for a multi-recipient record), None when deduplicated."""
        if record is not None:
            project_id = project_id or record.get("project_id")
            item_id = item_id or record.get("id")
            if "to" in record or kind == "message":
                kind = "message"
                recipients = record.get("to")
                recipients = recipients if isinstance(recipients, list) else [recipients]
                if agent_id is not None:
                    recipients = [agent_id]
                cursors = [self.publish(project_id, recipient, kind, item_id)
                           for recipient in recipients if recipient]
                return cursors if isinstance(record.get("to"), list) else cursors[0]
            agent_id = agent_id or record.get("recipient_agent_id")
            kind = kind or "notification"
        return self.publish(project_id, agent_id, kind, item_id)

    def notify_record_post_commit(self, record):
        return self.notify_post_commit(record)

    # ----------------------------------------------------------------- replay
    def latest_cursor(self, project_id, agent_id):
        """The per-agent cursor. Global traffic volume is never exposed."""
        _strict_id(project_id, "project")
        _strict_id(agent_id, "agent")
        with self._lock:
            self._require_open()
            row = self._db.execute(
                "SELECT next_seq FROM agent_cursors WHERE project_id=? AND agent_id=?",
                (project_id, agent_id)).fetchone()
        return int(row[0]) - 1 if row else 0

    def _replay_locked(self, project_id, agent_id, after, limit):
        rows = self._db.execute(
            "SELECT agent_seq, kind, item_id FROM push_events"
            " WHERE project_id=? AND agent_id=? AND agent_seq>?"
            " ORDER BY agent_seq LIMIT ?",
            (project_id, agent_id, after, limit)).fetchall()
        oldest = self._db.execute(
            "SELECT MIN(agent_seq) FROM push_events WHERE project_id=? AND agent_id=?",
            (project_id, agent_id)).fetchone()[0]
        events = [{"seq": row[0], "kind": row[1], "item_id": row[2]}
                  for row in rows]
        # A cursor older than the retained window means events were truncated away:
        # flag an explicit resync instead of pretending the replay is complete.
        resync = oldest is not None and after < oldest - 1
        return events, resync

    def replay(self, project_id, agent_id, after=0, limit=None):
        """Durable per-agent replay window. ``limit`` must be within 1..policy bound
        when given; out-of-range values are denied, never silently clamped."""
        _strict_id(project_id, "project")
        _strict_id(agent_id, "agent")
        _strict_cursor(after, "after")
        if limit is None:
            limit = self.policy.replay_limit
        elif type(limit) is not int or not 1 <= limit <= self.policy.replay_limit:
            raise ValueError("replay limit out of bounds")
        with self._lock:
            self._require_open()
            events, resync = self._replay_locked(project_id, agent_id, after, limit)
        return {"events": events, "resync": resync,
                "latest": self.latest_cursor(project_id, agent_id)}

    # ------------------------------------------------------------- subscription
    def subscribe(self, project_id, agent_id, after=0):
        """One bounded subscription: admission, registration, and the replay
        backlog happen atomically under the hub lock, so live fanout can neither
        overtake nor duplicate the backlog. Raises Problem(429) past the per-agent
        subscriber bound — callers surface that before any response headers."""
        _strict_id(project_id, "project")
        _strict_id(agent_id, "agent")
        _strict_cursor(after, "after")
        key = (project_id, agent_id)
        queue = Queue(maxsize=self.policy.max_pending_events)
        with self._lock:
            self._require_open()
            members = self._subscribers.setdefault(key, set())
            if len(members) >= self.policy.max_subscribers_per_agent:
                self._subscribers[key] = members
                raise Problem("push_subscriber_limit", 429)
            subscription = _Subscription(key, queue, False)
            members.add(subscription)
            backlog, resync = self._replay_locked(project_id, agent_id, after,
                                                  self.policy.replay_limit)
            subscription.resync = resync
            for event in backlog:
                try:
                    queue.put_nowait(event)
                except Full:
                    self._overflow(subscription)
                    break
        return subscription

    def _require_open(self):
        if self._closed:
            raise Problem("push_hub_closed", 503)

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
            members = self._subscribers.get(subscription.key)
            if members is not None:
                members.discard(subscription)
                if not members:
                    del self._subscribers[subscription.key]

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
        stats never reveal per-agent volume."""
        with self._lock:
            count = sum(len(members) for members in self._subscribers.values())
            closed = self._closed
            db = None if closed else self._db
        signals = None
        if db is not None:
            try:
                signals = int(db.execute(
                    "SELECT COALESCE(SUM(next_seq - 1), 0) FROM agent_cursors"
                ).fetchone()[0])
            except Exception:
                signals = None
        return {"closed": closed, "subscribers": count, "signals": signals,
                "path": self.path}


OPERATION_DESCRIPTORS = {
    "message_push.stream": {
        "name": "message_push.stream",
        "purpose": "Authenticated per-agent SSE stream of minimal message/"
                   "notification arrival signals with durable per-agent replay "
                   "cursors.",
        "input": "GET /v1/agents/{agent_id}/message-stream?project_id&after "
                 "(or Last-Event-ID); Bearer/session principal for that agent.",
        "output": "text/event-stream frames: id=<per-agent cursor>, "
                  "event=message|notification, data={id, kind}; heartbeat comments; "
                  "resync/shutdown advisories.",
        "authority": "requires the principal to currently represent the target agent "
                     "in the target project, re-verified under short canonical "
                     "snapshots before every signal, replay frame, and heartbeat; "
                     "items no longer visible are dropped silently.",
        "side_effects": "none (read-only stream; one bounded subscription slot).",
        "limits": "per-agent subscriber bound admitted before headers, bounded "
                  "pending queue with definite resync backpressure, bounded "
                  "per-agent replay window and log retention, bounded stream "
                  "lifetime (reconnect with cursor).",
        "retry_safety": "at-least-once; reconnect with after=<last id> replays the "
                        "durable per-agent window; a truncated window yields an "
                        "explicit resync advisory. Consumers must deduplicate by "
                        "identity. No exactly-once guarantee is made or implied.",
    },
    "message_push.commit_bridge": {
        "name": "message_push.commit_bridge",
        "purpose": "Converts committed canonical project-sequence invalidations "
                   "into per-agent identity-only push signals, off the commit path.",
        "input": "Store.subscribe_commits callbacks ({project: seq}); durable "
                 "per-project catch-up watermarks.",
        "output": "Durable per-agent signals; returns an unsubscribe/close handle.",
        "authority": "publishes identity/recipient only, extracted from committed "
                     "canonical rows under short snapshots; visibility is enforced "
                     "again at stream time. Never publishes uncommitted writes.",
        "side_effects": "appends to the durable signal log; wakes current "
                        "subscribers; one daemon thread with a polling fallback.",
        "limits": "bounded scan batches; nonblocking wake queue with dirty-flag "
                  "fallback; watermark/outbox dedup makes crashes recoverable.",
        "retry_safety": "a crash between commit and publish is repaired by the next "
                        "scan from the durable watermark; duplicate publications "
                        "are deduplicated by (agent, kind, item, source seq).",
    },
    "message_push.notify_post_commit": {
        "name": "message_push.notify_post_commit",
        "purpose": "Explicit service-owned hook to publish one committed record's "
                   "minimal wake signal when the automatic bridge is not wired.",
        "input": "committed canonical record (or explicit project/agent/kind/item "
                 "identity).",
        "output": "per-agent cursor of the appended durable signal (None when "
                  "deduplicated).",
        "authority": "publishes identity only; never content; must never be called "
                     "before the commit lands.",
        "side_effects": "appends to the durable signal log; wakes current "
                        "subscribers.",
        "limits": "signal kinds restricted to message/notification; strict "
                  "canonical ID shapes; hub-closed publication fails definitely "
                  "(503).",
        "retry_safety": "publication is an append with source-sequence dedup; a "
                        "duplicate signal is harmless (consumers deduplicate by "
                        "identity).",
    },
}

# Backward-compatible alias plus the discovery export the parent expects.
PUSH_DESCRIPTORS = OPERATION_DESCRIPTORS


def descriptors():
    """Self-describing operation metadata for parent discovery/MCP registration."""
    return {name: dict(descriptor) for name, descriptor in OPERATION_DESCRIPTORS.items()}


def parse_sse(lines, *, cancel=None, max_frames=MAX_FRAMES_PER_STREAM,
              max_line_bytes=MAX_LINE_BYTES, max_frame_bytes=MAX_FRAME_BYTES):
    """Minimal bounded SSE frame parser: yields {"seq", "kind", "item_id"} signals.

    ``lines`` is any iterable of text/byte lines (e.g. ``response.iter_lines()``).
    Lines and frames are byte-capped, the frame count per stream is capped, and
    malformed content raises :class:`PushStreamError` whose message never echoes
    remote bytes. Advisory events (resync/shutdown) yield {"kind": <name>} with no
    item_id so the follower still wakes a canonical reconcile. ``cancel`` (a
    threading.Event) is checked between lines so close interrupts a live parse.
    """
    event, data, last_id = None, None, None
    data_bytes = 0
    frames = 0

    def flush():
        nonlocal event, data, data_bytes, frames
        produced = None
        if event in SIGNAL_KINDS and data is not None:
            try:
                payload = json.loads(data)
            except ValueError:
                raise PushStreamError("Malformed push signal") from None
            if not isinstance(payload, dict):
                raise PushStreamError("Malformed push signal") from None
            item_id = payload.get("id")
            try:
                if item_id is not None:
                    _strict_id(item_id, "item")
                seq = _strict_cursor(int(last_id or 0))
            except (ValueError, TypeError):
                raise PushStreamError("Malformed push signal") from None
            produced = {"seq": seq, "kind": event, "item_id": item_id}
            frames += 1
        elif event in ("resync", "shutdown"):
            try:
                seq = _strict_cursor(int(last_id or 0))
            except (ValueError, TypeError):
                raise PushStreamError("Malformed push cursor") from None
            produced = {"seq": seq, "kind": event, "item_id": None}
            frames += 1
        if frames > max_frames:
            raise PushStreamError("Push stream frame budget exceeded")
        event, data, data_bytes = None, None, 0
        return produced

    for raw in lines:
        if cancel is not None and cancel.is_set():
            return
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        line = (line or "").rstrip("\r\n")
        if len(line.encode("utf-8", "replace")) > max_line_bytes:
            raise PushStreamError("Push stream line budget exceeded")
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
                raise PushStreamError("Malformed push signal")
            event = value
        elif field == "data":
            data_bytes += len(value.encode("utf-8", "replace")) + 1
            if data_bytes > max_frame_bytes:
                raise PushStreamError("Push stream frame budget exceeded")
            data = value if data is None else data + "\n" + value
        elif field == "id":
            if not value.isdigit():
                raise PushStreamError("Malformed push cursor")
            last_id = value
    produced = flush()
    if produced is not None:
        yield produced


def httpx_sse_subscription(http_client, path, *, cancel, headers=None,
                           read_timeout=10.0, max_frames=MAX_FRAMES_PER_STREAM):
    """Bounded, cancellable SSE subscription over ``http_client.stream``.

    ``cancel`` (threading.Event) interrupts an active read within ``read_timeout``
    seconds, so :meth:`PushWakeSource.close` never waits on an unbounded network
    read. Status/content-type failures raise :class:`PushStreamError` with fixed
    text (no remote bodies). A clean server-side close simply ends the iterator —
    the follower reconnects with its durable cursor.
    """
    import httpx

    if not 1.0 <= float(read_timeout) <= 120.0:
        raise ValueError("read_timeout out of bounds")
    timeout = httpx.Timeout(connect=10.0, read=float(read_timeout),
                            write=10.0, pool=10.0)
    with http_client.stream("GET", path, headers=headers or {},
                            timeout=timeout) as response:
        if response.status_code != 200:
            raise PushStreamError(
                f"Push stream rejected with status {response.status_code}")
        if "text/event-stream" not in response.headers.get("content-type", ""):
            raise PushStreamError("Push stream returned a non-SSE response")
        try:
            yield from parse_sse(response.iter_lines(), cancel=cancel,
                                 max_frames=max_frames)
        except httpx.ReadTimeout:
            return  # Idle read window elapsed; the follower reconnects.


class PushWakeSource:
    """Client-side follower: converts push signals into a ``threading.Event`` wake
    for ``AssignedRunMessagingSupervisor(push_signal=...)``.

    ``subscribe(after)`` must return an iterator of signal mappings carrying
    ``seq`` (e.g. :func:`httpx_sse_subscription`) and honor ``self.cancel`` within
    its own bounded read window. The cursor lives in a private cursor file
    (``cursor_path``): written atomically (tmp + os.replace), strictly validated
    (integer cursor, exact project/agent identity binding — a foreign or malformed
    file is refused, never trusted), so restarts resume instead of replaying or
    skipping. Every (re)subscription — including an empty stream — sets the wake
    event, so a reconnect always triggers a canonical reconcile even when no
    signal arrived. When the stream stays down the supervisor's bounded periodic
    reconcile (``max_reconcile_seconds``) remains as the fallback.
    """

    def __init__(self, subscribe, *, project_id=None, agent_id=None, cursor_path=None,
                 retry_seconds=0.2, max_retry_seconds=5.0):
        if not callable(subscribe):
            raise ValueError("PushWakeSource requires a subscribe(after) callable")
        if type(retry_seconds) not in (int, float) or \
                not 0 < float(retry_seconds) <= float(max_retry_seconds) <= 60.0:
            raise ValueError("retry bounds out of range")
        if (cursor_path is None) != (project_id is None and agent_id is None):
            raise ValueError("cursor_path requires the exact project/agent identity")
        self.event = threading.Event()
        self.cancel = threading.Event()
        self._subscribe = subscribe
        self._retry_seconds = float(retry_seconds)
        self._max_retry_seconds = float(max_retry_seconds)
        self._cursor_path = Path(cursor_path) if cursor_path else None
        self._identity = (project_id, agent_id)
        self.cursor = self._load_cursor()
        self._started = False
        self._thread = threading.Thread(target=self._follow, daemon=True,
                                        name="hivemind-message-push")

    def _load_cursor(self):
        if self._cursor_path is None:
            return 0
        try:
            raw = self._cursor_path.read_text()
        except FileNotFoundError:
            return 0
        except OSError:
            raise ValueError("Push cursor file is unreadable") from None
        try:
            state = json.loads(raw)
        except ValueError:
            raise ValueError("Push cursor file is malformed") from None
        if not isinstance(state, dict) or set(state) != {"project_id", "agent_id",
                                                         "cursor"}:
            raise ValueError("Push cursor file has an unexpected shape")
        if (state["project_id"], state["agent_id"]) != self._identity:
            raise ValueError("Push cursor file belongs to another agent/project")
        return _strict_cursor(state["cursor"])

    def _persist_cursor(self):
        if self._cursor_path is None:
            return
        state = {"project_id": self._identity[0], "agent_id": self._identity[1],
                 "cursor": _strict_cursor(self.cursor)}
        payload = json.dumps(state)
        temporary = self._cursor_path.with_name(self._cursor_path.name + ".tmp")
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self._cursor_path)

    def start(self):
        self._started = True
        self._thread.start()
        return self

    def _follow(self):
        delay = self._retry_seconds
        while not self.cancel.is_set():
            try:
                # A (re)subscription always wakes a canonical reconcile, even when
                # the stream yields nothing: signals may have been lost while down.
                self.event.set()
                for signal in self._subscribe(self.cursor):
                    if self.cancel.is_set():
                        return
                    seq = _strict_cursor(signal.get("seq", 0))  # strict: no coercion
                    if seq > self.cursor:
                        self.cursor = seq
                        self._persist_cursor()
                    self.event.set()
                delay = self._retry_seconds  # a clean close retries quickly
            except Exception:
                delay = min(delay * 2, self._max_retry_seconds)
            self.cancel.wait(delay)

    def close(self, timeout=10.0):
        """Bounded stop: interrupts the follower (and, via ``cancel``, an active
        subscription read within its read window) and joins within ``timeout``.
        Returns True only when the thread actually stopped."""
        if not 0 < float(timeout) <= 120.0:
            raise ValueError("close timeout out of bounds")
        self.cancel.set()
        self.event.set()  # Let any waiter observe shutdown promptly.
        if self._started:
            self._thread.join(timeout=float(timeout))
            if self._thread.is_alive():
                return False
        self._persist_cursor()
        return True


class CommitSignalBridge:
    """Converts committed canonical invalidations into identity-only push signals.

    The store's commit callback receives only ``{project: seq}`` and runs under no
    SQLite lock of ours; it does a nonblocking queue put (a full queue just sets
    the dirty flag — the polling tick covers it). The bridge thread scans
    committed canonical message/notification rows newer than the durable
    per-project watermark under short ``store.lock`` snapshots, extracts identity
    and recipient only (bodies are parsed and immediately discarded, never
    retained), and publishes through :meth:`MessagePushHub.publish_batch`, which
    advances the watermark in the same transaction as the signal inserts — so a
    crash between commit and publish is repaired by the next scan. A polling
    fallback runs even without any wake, and startup always catches up once.
    """

    def __init__(self, store, hub, *, poll_interval=5.0, batch_limit=500):
        if not callable(getattr(store, "subscribe_commits", None)):
            raise ValueError("Commit bridge requires Store.subscribe_commits")
        if type(poll_interval) not in (int, float) or \
                not 0.1 <= float(poll_interval) <= 600.0:
            raise ValueError("poll_interval out of bounds")
        if type(batch_limit) is not int or not 1 <= batch_limit <= 5000:
            raise ValueError("batch_limit out of bounds")
        self._store = store
        self._hub = hub
        self._poll_interval = float(poll_interval)
        self._batch_limit = batch_limit
        self._queue = Queue(maxsize=1024)
        self._dirty = set()
        self._guard = threading.Lock()  # guards _dirty only
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._closed = False
        self._started = False
        # Subscribe before baselining so a commit racing the baseline is still
        # queued as a wake and caught by the watermark it lands after.
        self._unsubscribe = store.subscribe_commits(self._on_commit)
        self._baseline()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="hivemind-message-push-bridge")

    def _baseline(self):
        """Bind-time baseline under one short snapshot: projects that already exist
        get their durable watermark set at the current committed edge (history
        predating install is not signaled). On restart the durable watermarks are
        kept (init is a no-op), so catch-up resumes exactly from the crash point.
        Projects created after binding simply start from watermark 0 — every one
        of their rows is newer than the install."""
        with self._store.lock:
            # Baseline at each existing project's current committed sequence: every
            # pre-install row has a resource seq <= the project seq (save ticks it),
            # and any post-bind commit lands strictly after.
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
            # install baseline, so catch up from zero (bounded by batch pages and
            # source-sequence dedup).
            watermark = 0
        published = 0
        while not self._stop.is_set():
            with self._store.lock:  # short authoritative snapshot, off the wake path
                rows = self._store.db.execute(
                    "SELECT id, kind, seq, body FROM resources"
                    " WHERE project=? AND kind IN ('message','notification')"
                    " AND seq>? ORDER BY seq LIMIT ?",
                    (project, watermark, self._batch_limit)).fetchall()
            if not rows:
                return published
            items = []
            for row in rows:
                try:
                    body = json.loads(row["body"]) if row["body"] else {}
                except ValueError:
                    body = {}
                if row["kind"] == "message":
                    recipients = body.get("to")
                    recipients = (recipients if isinstance(recipients, list)
                                  else [recipients])
                    for recipient in recipients:
                        if isinstance(recipient, str) and ID_PATTERN.match(recipient):
                            items.append((recipient, "message", row["id"],
                                          int(row["seq"])))
                else:
                    recipient = body.get("recipient_agent_id")
                    if isinstance(recipient, str) and ID_PATTERN.match(recipient):
                        items.append((recipient, "notification", row["id"],
                                      int(row["seq"])))
                # The parsed body goes out of scope here: identity only survives.
            published += hub.publish_batch(project, items,
                                           watermark=int(rows[-1]["seq"]))
            watermark = int(rows[-1]["seq"])
            if len(rows) < self._batch_limit:
                return published
        return published

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
                    pass  # The next wake or poll tick retries from the watermark.
            self._wake.clear()
            self._wake.wait(self._poll_interval)
        # Final polling fallback sweep: a wake lost near shutdown is still covered
        # by the watermark on the next bridge's startup catch-up.

    def _safe_projects(self):
        try:
            return self._hub.watermarked_projects()
        except Exception:
            return []

    def tick(self, project):
        """Explicit scan request (e.g. a test or a parent's polling fallback)."""
        return self._scan(project)

    def close(self, timeout=10.0):
        if self._closed:
            return True
        self._closed = True
        self._stop.set()
        self._wake.set()
        if callable(self._unsubscribe):
            self._unsubscribe()
            self._unsubscribe = None
        if self._started:
            self._thread.join(timeout=float(timeout))
            return not self._thread.is_alive()
        return True

    def unsubscribe(self):
        return self.close()


def bind_commit_source(store, hub, *, poll_interval=5.0, batch_limit=500):
    """Explicit parent wiring: bind the commit bridge and return its handle
    (``close()``/``unsubscribe()``). ``install`` already does this unless
    ``bridge=False`` is passed."""
    return CommitSignalBridge(store, hub, poll_interval=poll_interval,
                              batch_limit=batch_limit).start()


def install(app, service, principal, command, envelope, *, policy=None, hub=None,
            path=None, bridge=True):
    """Conventional capability install. Default-off is safe: without an enabled
    policy the route is registered but returns ``feature_disabled`` and no hub,
    bridge, or thread is created. With an enabled policy, attaches the
    service-owned hub (``app.state.message_push_hub`` /
    ``service.message_push_hub``) and, when the store supports it, auto-binds the
    commit bridge (``app.state.message_push_bridge``)."""
    policy = policy if policy is not None else MessagePushPolicy()
    if policy.enabled is not True:
        app.state.message_push_hub = None

        @app.get("/v1/agents/{agent_id}/message-stream")
        async def message_stream_disabled(agent_id: str, project_id: str,
                                          p=Depends(principal)):
            service.project_access(p, project_id)
            raise Problem("feature_disabled", 503)

        message_stream_disabled.hivemind_detached_work = True
        return None

    if hub is None:
        path = path or (Path(service.store.directory) / "message-push.sqlite")
        hub = MessagePushHub(path, policy)
    app.state.message_push_hub = hub
    service.message_push_hub = hub
    store = service.store

    def _reauthorize(principal_id, project_id, agent_id):
        """Fresh short snapshot: principal still exists, matches the project, still
        represents the exact target agent, and that agent still exists here."""
        with store.lock:
            row = store.db.execute("SELECT * FROM principals WHERE id=?",
                                   (principal_id,)).fetchone()
            if row is None:
                return False
            current = dict(row)
            if current["role"] != "admin" and current["project"] != project_id:
                return False
            if current["role"] != "admin" and current["agent"] != agent_id:
                return False
            agent = store.db.execute(
                "SELECT project FROM resources WHERE id=? AND kind='agent'"
                " AND deleted=0", (agent_id,)).fetchone()
            return agent is not None and agent["project"] == project_id

    def _item_visible(principal_id, project_id, agent_id, kind, item_id):
        """Fresh short snapshot: the item still exists, is canonically visible to
        the current principal, and is addressed to this exact agent. Anything else
        is dropped silently — never emitted, never counted per agent."""
        with store.lock:
            row = store.db.execute("SELECT * FROM principals WHERE id=?",
                                   (principal_id,)).fetchone()
            resource = store.db.execute(
                "SELECT * FROM resources WHERE id=? AND deleted=0",
                (item_id,)).fetchone()
            if row is None or resource is None:
                return False
            current = dict(row)
            if resource["kind"] != kind or resource["project"] != project_id:
                return False
            if not store.visible(resource, current):
                return False
            body = json.loads(resource["body"])
            if kind == "message":
                recipients = body.get("to")
                recipients = (recipients if isinstance(recipients, list)
                              else [recipients])
                return agent_id in recipients
            return body.get("recipient_agent_id") == agent_id

    def _sse_frames(principal_id, project_id, agent_id, subscription):
        """Replay backlog then live signals; every emission is preceded by a fresh
        authorization snapshot. All waiting happens on the subscription queue —
        never while holding Request/store locks (detached route)."""
        try:
            if subscription.resync:
                yield ('event: resync\n'
                       'data: {"reason": "cursor_outside_window"}\n\n')
            deadline = time.time() + policy.max_stream_seconds
            while True:
                remaining = deadline - time.time()
                if remaining <= 0 or hub._closed:
                    return  # Bounded stream lifetime: reconnect with the cursor.
                try:
                    event = subscription.queue.get(
                        timeout=min(policy.heartbeat_seconds, remaining))
                except Empty:
                    if not _reauthorize(principal_id, project_id, agent_id):
                        return  # Authorization lapsed mid-stream: close silently.
                    yield ": heartbeat\n\n"
                    continue
                if event is _CLOSE:
                    yield "event: shutdown\ndata: {}\n\n"
                    return
                if event is _OVERFLOW:
                    yield ('event: resync\n'
                           'data: {"reason": "backpressure"}\n\n')
                    return
                if not _item_visible(principal_id, project_id, agent_id,
                                     event["kind"], event["item_id"]):
                    continue  # Hidden/deleted/foreign: dropped, never emitted.
                payload = {"id": event["item_id"], "kind": event["kind"]}
                yield (f"id: {event['seq']}\nevent: {event['kind']}\n"
                       f"data: {json.dumps(payload)}\n\n")
        finally:
            hub.unsubscribe(subscription)

    @app.get("/v1/agents/{agent_id}/message-stream")
    async def message_stream(agent_id: str, request: Request, project_id: str,
                             after: int = 0, p=Depends(principal)):
        service.project_access(p, project_id)
        service.represent(p, agent_id)
        # Reconnecting EventSource clients send Last-Event-ID; an explicit `after`
        # query parameter wins when both are present.
        if after <= 0:
            header = request.headers.get("last-event-id")
            if header and header.isdigit():
                after = int(header)
        # Admission (subscriber bound, strict cursor) happens BEFORE headers: a
        # refusal is a clean 429/422, never a mid-stream failure after 200.
        subscription = hub.subscribe(project_id, agent_id, after=after)
        return StreamingResponse(
            _sse_frames(p["id"], project_id, agent_id, subscription),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    # The parent middleware skips its global request/store locks for marked routes;
    # this generator guards every canonical read with its own short snapshots.
    message_stream.hivemind_detached_work = True

    if bridge and callable(getattr(store, "subscribe_commits", None)):
        app.state.message_push_bridge = bind_commit_source(store, hub)
    return hub
