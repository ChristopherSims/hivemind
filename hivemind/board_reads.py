"""Per-principal, revision-aware unread state for shared board posts.

Why a separate module and a separate table
------------------------------------------
Reading a shared board is not agent delivery. ``conversation_reads`` marks
direct messages as seen for one principal and feeds ``Service.delivery_summary``
and the ``unread`` counts in that summary; agent delivery acknowledgements are
lease-bound execution receipts owned by ``Service.ack_message``. Neither may be
reused for "which shared board posts has this principal already looked at": a
delivery ack is a fact about a run, a conversation read is scoped to a direct
message, and board posts have no lease, no recipient and no delivery state.
This module owns its own marker table (``board_reads``), is its only writer, and
never touches ``conversation_reads``, message bodies, delivery state or any
other row.

Canonical semantics
-------------------
* **Per principal.** A marker row is keyed by ``(principal, post)``. Counts and
  per-post markers are always computed for the authenticated principal only, so
  one reader can never observe or affect another reader's state.
* **Revision aware.** A marker records the *revision* that was read. A post is
  read only while ``board_reads.revision == resources.revision``; an edit (or an
  explicit re-mark request whose revision moved) makes the post unread again for
  every principal except its author.
* **Exact project/board scope.** Marking re-reads every referenced post from the
  canonical store and requires it to be a live ``board_post`` in the requested
  project (and requested board, when the route pins one). Anything else is a
  404 and the whole batch is rejected, like ``Service.validate_read_messages``.
* **Current authorization.** Every scanned or marked post passes
  ``Store.visible`` for the current principal, so deleted, private,
  team-inaccessible and board-restricted posts are excluded and a revoked
  reader stops seeing - or marking - immediately. Nothing is cached across
  principals, projects or visibility changes.
* **Bounded, honest scans.** A scan examines at most ``scan_limit`` candidate
  rows newest-first. When candidates remain beyond that budget the response
  reports ``truncated: True`` and ``counts_complete: False`` instead of
  pretending the count is exact. Marker id lists are separately capped and
  report ``unread_markers_truncated``.
* **Explicit reads only.** ``GET`` never writes. Only the ``:read`` routes -
  batched to ``MAX_MARK_BATCH`` posts per request - create markers.
* **Author behavior (explicit).** ``self_policy`` is
  ``own_posts_never_unread``: a post authored by the requesting principal is
  excluded from unread counts and from ``unread_post_ids``, and the number of
  posts thus excluded is reported as ``own_excluded_count``. Pass
  ``include_own=true`` to count your own posts as unread. The rule is symmetric:
  it only ever applies to the principal that authored the post.
* **No body storage.** The marker table stores ids, revisions and timestamps.
  No post text, no title, no derived copy of content.

Integration contract (owned by the integration layer)
-----------------------------------------------------
Register the module the same way as the other feature modules::

    # hivemind/capabilities.py - FEATURE_MODULES
    "board_reads",

That single name is the whole registration: ``install`` uses the standard
``(app, service, principal, command, envelope)`` seam, creates the marker table
idempotently, and registers four routes without shadowing any existing handler.
``tests/test_board_reads.py::test_capabilities_registration_is_the_only_wiring_the_integrator_needs``
builds a real app through ``create_app`` with that name added and exercises the
routes, so no other wiring change is required.

``install`` also sets ``app.state.board_reads`` to a :class:`BoardReads` for
programmatic use. Two optional seams let the shared routes expose board read
state without this module editing them:

1. ``stream_cursor`` (``hivemind/api.py``) - include the marker rows in the
   canonical payload next to the conversation reads it already folds in::

       from .board_reads import markers_for_cursor
       reads = sorted(...conversation_reads...)
       boards = markers_for_cursor(store, p, project, visible_ids)
       payload = canonical([p["id"], project, revisions, lease_id, reads, boards])

   ``markers_for_cursor`` returns ``[]`` when the table is absent, so the shared
   route stays valid before this module is installed, and it filters to
   currently visible ids exactly like the existing conversation-read filter.

2. ``dashboard_snapshot`` (``hivemind/api.py``) - add board badges to the atomic
   snapshot (same ``store.lock`` block, no extra round trip)::

       from .board_reads import project_unread_summary
       return envelope({..., "snapshot_cursor": stream_cursor(p, project_id),
                        "board_unread": project_unread_summary(store, p, project_id)})

   The returned payload is exactly the ``GET /v1/projects/{id}/board-unread``
   body without ``snapshot_cursor``, and reports ``available: False`` (never a
   false zero) when the module is not installed.

Timeline items gain ``is_read``/``board_unread`` in the dashboard, not here:
``dashboard/src/board-unread.ts`` (re-exported by ``BoardUnread.tsx``) returns
the unread id set and ``decorateBoardItems`` maps
``[{id}] -> [{id, is_read, board_unread}]``. Keeping the join client-side means
the shared timeline handler needs no change and the marker stays per principal.
"""

from __future__ import annotations

import hashlib
import json
import time

from fastapi import Depends, Header, Query, Request
from pydantic import Field

from .models import ID, Model
from .storage import Problem, canonical, now

TABLE = "board_reads"

#: Hard cap on one explicit marking request, mirroring ``ConversationsRead``.
MAX_MARK_BATCH = 100
#: Default/ceiling number of candidate rows one scan may examine.
DEFAULT_SCAN_LIMIT = 2000
MAX_SCAN_LIMIT = 5000
#: Default/ceiling number of unread ids a response may return.
DEFAULT_MARKER_LIMIT = 200
MAX_MARKER_LIMIT = 1000
#: Rows fetched per scan batch / per ``IN`` chunk.
SCAN_WINDOW = 256
#: Explicit, reported author rule.
SELF_POLICY = "own_posts_never_unread"

# Stable creation sequence, matching ``Service.timeline_page`` and
# ``thread_pane``: edits move ``seq`` but never the post's creation order.
CREATION_SEQ = """COALESCE(json_extract(r.body,'$.created_seq'),
    (SELECT json_extract(v.body,'$.ingestion_seq') FROM versions v
     WHERE v.id=r.id ORDER BY v.revision LIMIT 1),r.seq)"""

SCHEMA_STATEMENTS = (
    f"""CREATE TABLE IF NOT EXISTS {TABLE}(
      principal TEXT REFERENCES principals(id),
      project TEXT REFERENCES projects(id),
      post TEXT REFERENCES resources(id),
      revision INTEGER NOT NULL,
      read_at TEXT NOT NULL,
      PRIMARY KEY(principal,post))""",
    f"CREATE INDEX IF NOT EXISTS {TABLE}_principal_project ON {TABLE}(principal,project)",
)


def ensure_schema(store):
    """Create the marker table idempotently.

    Statements run individually: ``executescript`` would implicitly commit an
    enclosing ``Store.transaction``, so this never calls it. Safe to call at
    install time or repeatedly; no row is written.
    """
    with store.lock:
        for statement in SCHEMA_STATEMENTS:
            store.db.execute(statement)
    return True


def table_present(store):
    """True when this module's table exists.

    The shared ``stream_cursor``/snapshot seams may run before installation, and
    a missing table must read as "feature absent", never as "everything unread".
    """
    return store.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone() is not None


def _authored_by(body, principal):
    """True when this principal authored the post (human or agent identity)."""
    author = body.get("author") or {}
    if author.get("kind") == "agent":
        return bool(principal.get("agent")) and author.get("agent_id") == principal["agent"]
    return author.get("principal_id") is not None and author.get("principal_id") == principal["id"]


def _candidate_rows(store, principal, project, board_id, scan_limit):
    """Bounded newest-first scan of live, currently visible board posts.

    Returns ``(visible_rows, examined_count, truncated)``. ``truncated`` is a
    conservative statement: it is True when any candidate row could remain
    beyond the examined budget, even one that would have been filtered as
    invisible. Under-reporting completeness is never allowed; over-reporting
    truncation only makes the caller more careful.
    """
    cursor, examined, exhausted = None, 0, False
    visible = []
    while examined < scan_limit:
        window = min(SCAN_WINDOW, scan_limit - examined)
        sql = (f"SELECT r.*, {CREATION_SEQ} AS creation_seq FROM resources r "
               "WHERE r.project=? AND r.kind='board_post' AND r.deleted=0")
        args = [project]
        if board_id is not None:
            sql += " AND json_extract(r.body,'$.board_id')=?"
            args.append(board_id)
        if cursor is not None:
            sql += f" AND {CREATION_SEQ}<?"
            args.append(cursor)
        sql += " ORDER BY creation_seq DESC LIMIT ?"
        args.append(window)
        rows = store.db.execute(sql, args).fetchall()
        if not rows:
            exhausted = True
            break
        examined += len(rows)
        cursor = rows[-1]["creation_seq"]
        visible.extend(row for row in rows if store.visible(row, principal))
        if len(rows) < window:
            exhausted = True
            break
    truncated = False
    if not exhausted and cursor is not None:
        sql = ("SELECT 1 FROM resources r WHERE r.project=? AND r.kind='board_post' AND r.deleted=0")
        args = [project]
        if board_id is not None:
            sql += " AND json_extract(r.body,'$.board_id')=?"
            args.append(board_id)
        sql += f" AND {CREATION_SEQ}<? LIMIT 1"
        args.append(cursor)
        truncated = store.db.execute(sql, args).fetchone() is not None
    return visible, examined, truncated


def _read_revisions(store, principal, post_ids):
    """Read-marker revisions for this principal, only for the ids in question."""
    markers = {}
    ids = list(post_ids)
    for index in range(0, len(ids), SCAN_WINDOW):
        chunk = ids[index:index + SCAN_WINDOW]
        placeholders = ",".join("?" * len(chunk))
        for row in store.db.execute(
                f"SELECT post, revision FROM {TABLE} WHERE principal=? AND post IN ({placeholders})",
                [principal["id"], *chunk]):
            markers[row["post"]] = row["revision"]
    return markers


def decorate_posts(store, principal, posts):
    """Exact markers for already-authorized page rows, independent of count/ID scan limits."""
    if not table_present(store):
        return posts
    markers = _read_revisions(store, principal, [post["id"] for post in posts])
    return [{**post, "board_unread": not _authored_by(post, principal)
             and markers.get(post["id"]) != post["revision"]} for post in posts]


def read_marker_digest(store, principal, project):
    """Fingerprint of this principal's markers in one project, or None if absent.

    Changes exactly when the principal's read state for that project changes, so
    a host can detect a read-state move without diffing whole payloads. Never
    crosses principals or projects.
    """
    if not table_present(store):
        return None
    rows = store.db.execute(
        f"SELECT post, revision FROM {TABLE} WHERE principal=? AND project=? ORDER BY post",
        (principal["id"], project)).fetchall()
    payload = [[row["post"], row["revision"]] for row in rows]
    return hashlib.sha256(canonical(payload).encode()).hexdigest()


def markers_for_cursor(store, principal, project, visible_ids):
    """Marker rows the shared ``stream_cursor`` should fold into its payload.

    Mirrors how ``stream_cursor`` filters conversation reads to currently visible
    ids. Returns a sorted list of ``[post, revision]`` pairs, or ``[]`` when the
    table is absent so the shared route can call it unconditionally.
    """
    if not table_present(store):
        return []
    ids = set(visible_ids)
    rows = store.db.execute(
        f"SELECT post, revision FROM {TABLE} WHERE principal=? AND project=?", (principal["id"], project))
    return sorted([row["post"], row["revision"]] for row in rows if row["post"] in ids)


def scan_state(store, principal, project, board_id=None, *, scan_limit=DEFAULT_SCAN_LIMIT,
               marker_limit=DEFAULT_MARKER_LIMIT, include_own=False):
    """Unread state for one principal in one exact project (optionally one board).

    Read-only: never creates a marker. Every count is derived from live rows that
    pass ``Store.visible`` for this principal right now.
    """
    scan_limit = max(1, min(int(scan_limit), MAX_SCAN_LIMIT))
    marker_limit = max(1, min(int(marker_limit), MAX_MARKER_LIMIT))
    available = table_present(store)
    visible, examined, truncated = (
        _candidate_rows(store, principal, project, board_id, scan_limit) if available else ([], 0, False))
    markers = _read_revisions(store, principal, [row["id"] for row in visible]) if available else {}

    unread_ids, unread_total, read_total = [], 0, 0
    authored, own_excluded = 0, 0
    per_board = {}
    for row in visible:
        body = json.loads(row["body"])
        identity = body.get("board_id")
        bucket = per_board.setdefault(identity, {"unread_count": 0, "visible_count": 0})
        bucket["visible_count"] += 1
        is_read = markers.get(row["id"]) == row["revision"]
        if is_read:
            read_total += 1
            continue
        own = _authored_by(body, principal)
        if own:
            authored += 1
            if not include_own:
                own_excluded += 1
                continue
        unread_total += 1
        bucket["unread_count"] += 1
        if len(unread_ids) < marker_limit:
            unread_ids.append(row["id"])

    state = {
        "project_id": project,
        "board_id": board_id,
        "board_title": None,
        "available": available,
        "revision_aware": True,
        "self_policy": SELF_POLICY,
        "include_own": bool(include_own),
        "unread_count": unread_total,
        "unread_post_ids": unread_ids,
        # Complete only when the scan was complete *and* every unread id was returned.
        "unread_markers_truncated": bool(truncated or unread_total > len(unread_ids)),
        "visible_count": len(visible),
        "read_count": read_total,
        "authored_count": authored,
        "own_excluded_count": own_excluded,
        "counts_complete": bool(available and not truncated),
        "truncated": bool(truncated),
        "examined_count": examined,
        "scan_limit": scan_limit,
        "marker_limit": marker_limit,
        "read_digest": read_marker_digest(store, principal, project) if available else None,
        "generated_at": time.time(),
    }
    if board_id is not None:
        title = store.db.execute(
            "SELECT * FROM resources WHERE id=? AND kind='board' AND deleted=0", (board_id,)).fetchone()
        if title is not None and store.visible(title, principal):
            state["board_title"] = json.loads(title["body"]).get("title")
    else:
        # Badges are listed only for boards this principal may currently see.
        state["boards"] = [
            {"board_id": board["id"], "title": board.get("title"),
             "unread_count": per_board.get(board["id"], {"unread_count": 0})["unread_count"],
             "visible_count": per_board.get(board["id"], {"visible_count": 0})["visible_count"],
             "counts_complete": bool(available and not truncated)}
            for board in store.list("board", project, principal, 1000)]
    return state


def project_unread_summary(store, principal, project, *, scan_limit=DEFAULT_SCAN_LIMIT,
                           marker_limit=DEFAULT_MARKER_LIMIT, include_own=False):
    """Project-wide board badges, ready to embed in an atomic dashboard snapshot.

    Identical to the ``GET /v1/projects/{id}/board-unread`` body without
    ``snapshot_cursor``. Reports ``available: False`` and ``counts_complete:
    False`` when the module is not installed, never a fabricated zero.
    """
    return scan_state(store, principal, project, None, scan_limit=scan_limit,
                      marker_limit=marker_limit, include_own=include_own)


def mark_posts(store, principal, project, post_ids, *, board_id=None, ignore_unavailable=False,
               max_batch=MAX_MARK_BATCH, post_revisions=None):
    """Explicitly mark live, currently visible posts read for this principal.

    Every id is re-read from the canonical store: it must be a live
    ``board_post`` in ``project`` (and in ``board_id`` when the route pins one)
    and must still be visible to this principal. Any failure rejects the whole
    batch with 404 - the enclosing transaction writes nothing - unless
    ``ignore_unavailable`` is set, in which case unknown/unauthorized ids are
    reported in ``skipped`` and never written.

    The stored revision is the revision that was authorized here, so a later
    edit makes the post unread again.
    """
    if not table_present(store):
        raise Problem("board_reads_unavailable", 409)
    unique = list(dict.fromkeys(post_ids))
    if not unique:
        raise Problem("board_read_batch_empty", 422)
    if len(unique) > max_batch:
        raise Problem("board_read_batch_too_large", 422)
    marked, skipped, already = [], [], 0
    for post_id in unique:
        row = store.db.execute(
            "SELECT * FROM resources WHERE id=? AND kind='board_post' AND deleted=0", (post_id,)).fetchone()
        if row is None or row["project"] != project or not store.visible(row, principal):
            if ignore_unavailable:
                skipped.append({"post_id": post_id, "reason": "not_found"})
                continue
            raise Problem("not_found", 404)
        if board_id is not None and json.loads(row["body"]).get("board_id") != board_id:
            if ignore_unavailable:
                skipped.append({"post_id": post_id, "reason": "not_found"})
                continue
            raise Problem("not_found", 404)
        if post_revisions is not None and post_revisions.get(post_id) != row["revision"]:
            raise Problem("board_post_changed", 409)
        existing = store.db.execute(
            f"SELECT revision FROM {TABLE} WHERE principal=? AND post=?",
            (principal["id"], post_id)).fetchone()
        if existing is not None and existing["revision"] == row["revision"]:
            already += 1
            continue
        store.db.execute(
            f"""INSERT INTO {TABLE}(principal,project,post,revision,read_at) VALUES(?,?,?,?,?)
                ON CONFLICT(principal,post) DO UPDATE SET
                  project=excluded.project, revision=excluded.revision, read_at=excluded.read_at""",
            (principal["id"], row["project"], post_id, row["revision"], now()))
        marked.append({"post_id": post_id, "revision": row["revision"]})
    return {"marked_count": len(marked), "already_read_count": already,
            "marked": marked, "skipped": skipped}


class BoardReads:
    """Programmatic seam for hosts that read or mark without HTTP."""

    def __init__(self, store, *, scan_limit=DEFAULT_SCAN_LIMIT, marker_limit=DEFAULT_MARKER_LIMIT,
                 max_batch=MAX_MARK_BATCH):
        self.store = store
        self.scan_limit = scan_limit
        self.marker_limit = marker_limit
        self.max_batch = max_batch

    def ensure_schema(self):
        return ensure_schema(self.store)

    def state(self, principal, project, board_id=None, *, include_own=False,
              scan_limit=None, marker_limit=None):
        return scan_state(self.store, principal, project, board_id,
                          scan_limit=self.scan_limit if scan_limit is None else scan_limit,
                          marker_limit=self.marker_limit if marker_limit is None else marker_limit,
                          include_own=include_own)

    def mark(self, principal, project, post_ids, *, board_id=None, ignore_unavailable=False, post_revisions=None):
        return mark_posts(self.store, principal, project, post_ids, board_id=board_id,
                          ignore_unavailable=ignore_unavailable, max_batch=self.max_batch,
                          post_revisions=post_revisions)

    def digest(self, principal, project):
        return read_marker_digest(self.store, principal, project)

    def markers_for_cursor(self, principal, project, visible_ids):
        return markers_for_cursor(self.store, principal, project, visible_ids)


class BoardRead(Model):
    """One explicit read-marking batch. ``post_ids`` is capped at 100."""

    post_ids: list[ID] = Field(min_length=1, max_length=MAX_MARK_BATCH)
    snapshot_cursor: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    ignore_unavailable: bool = False
    post_revisions: dict[ID, int] | None = Field(default=None, max_length=MAX_MARK_BATCH)


def install(app, service, principal, command, envelope, **options):
    """Register board unread reads and explicit marking on a ``create_app`` app.

    ``principal``, ``command`` and ``envelope`` are the existing closures from
    ``hivemind.api``; ``command`` is used only by the ``:read`` routes so every
    marker write keeps canonical idempotency and transaction semantics.
    """
    from .capabilities import replace_routes
    replace_routes(app, __name__)

    store = service.store
    default_scan = int(options.get("scan_limit", DEFAULT_SCAN_LIMIT))
    default_marker = int(options.get("marker_limit", DEFAULT_MARKER_LIMIT))
    ensure_schema(store)
    board_reads = BoardReads(store, scan_limit=default_scan, marker_limit=default_marker)
    app.state.board_reads = board_reads

    def current(request, project):
        p = app.state.authenticate_request(request)
        service.project_access(p, project)
        return p

    def scoped_board(p, project, board_id):
        """Resolve and authorize one board inside the requested project."""
        board = store.get(board_id, p, "board")
        if board["project_id"] != project:
            raise Problem("not_found", 404)
        return board

    @app.get("/v1/projects/{project_id}/board-unread")
    async def board_unread_state(request: Request, project_id: str,
                                 board_id: str | None = Query(None, max_length=128),
                                 include_own: bool = Query(False),
                                 scan_limit: int = Query(default_scan, ge=1, le=MAX_SCAN_LIMIT),
                                 marker_limit: int = Query(default_marker, ge=1, le=MAX_MARKER_LIMIT),
                                 snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                                 p=Depends(principal)):
        """Per-board unread badges (and per-post ids) for the current principal."""
        with store.lock:
            p = current(request, project_id)
            if board_id is not None:
                scoped_board(p, project_id, board_id)
            cursor = app.state.checked_snapshot(p, project_id, snapshot_cursor)
            return envelope({**board_reads.state(p, project_id, board_id, include_own=include_own,
                                                 scan_limit=scan_limit, marker_limit=marker_limit),
                             "snapshot_cursor": cursor})

    @app.get("/v1/boards/{board_id}/unread")
    async def board_unread(request: Request, board_id: str,
                           include_own: bool = Query(False),
                           scan_limit: int = Query(default_scan, ge=1, le=MAX_SCAN_LIMIT),
                           marker_limit: int = Query(default_marker, ge=1, le=MAX_MARKER_LIMIT),
                           snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                           p=Depends(principal)):
        """Unread counts and unread post ids for exactly one shared board."""
        with store.lock:
            p = app.state.authenticate_request(request)
            board = store.get(board_id, p, "board")
            service.project_access(p, board["project_id"])
            cursor = app.state.checked_snapshot(p, board["project_id"], snapshot_cursor)
            return envelope({**board_reads.state(p, board["project_id"], board_id,
                                                 include_own=include_own, scan_limit=scan_limit,
                                                 marker_limit=marker_limit),
                             "snapshot_cursor": cursor})

    def apply_marks(p, project, board_id, body):
        """Fence the snapshot, write the batch, and return the fresh state."""
        app.state.checked_snapshot(p, project, body.snapshot_cursor)
        result = board_reads.mark(p, project, body.post_ids, board_id=board_id,
                                  ignore_unavailable=body.ignore_unavailable, post_revisions=body.post_revisions)
        cursor = app.state.checked_snapshot(p, project, None)
        # The echoed state uses the default author policy, matching the read routes.
        return {**result, "project_id": project, "board_id": board_id,
                "state": {**board_reads.state(p, project, board_id), "snapshot_cursor": cursor},
                "snapshot_cursor": cursor}

    @app.post("/v1/boards/{board_id}/unread:read")
    async def board_posts_read(board_id: str, body: BoardRead, p=Depends(principal),
                               idempotency_key: str | None = Header(None)):
        """Mark up to 100 loaded, currently visible posts of one board as read."""
        def apply():
            board = store.get(board_id, p, "board")
            service.project_access(p, board["project_id"])
            return apply_marks(p, board["project_id"], board_id, body)
        return command(p, "board-unread:" + board_id, idempotency_key, body, apply)

    @app.post("/v1/projects/{project_id}/board-unread:read")
    async def project_posts_read(project_id: str, body: BoardRead, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        """Mark up to 100 loaded, currently visible posts of one project as read."""
        def apply():
            service.project_access(p, project_id)
            return apply_marks(p, project_id, None, body)
        return command(p, "board-unread-project:" + project_id, idempotency_key, body, apply)
