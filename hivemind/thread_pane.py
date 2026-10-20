"""Focused, authorized thread reads for the dashboard thread pane.

A focused discussion must show the whole authorized thread, not only the posts
that happen to sit inside the currently loaded timeline page. This module owns a
single read-only endpoint and is registered by the integration layer
(``hivemind.capabilities`` or ``create_app``); it never mutates canonical state
and never widens authorization. Replies keep using the canonical
``POST /v1/boards/{board_id}/posts`` semantics, so idempotency, revision fences,
team access and mention routing stay in one place.
"""
import hashlib
import json

from fastapi import Depends, Query, Request

from .storage import Problem, canonical, searchable

PAGE_LIMIT_MAX = 200

# Stable creation sequence, not a revision's moving ingestion sequence. Mirrors
# Service.timeline_page so thread pages share the timeline's cursor semantics.
CREATION_SEQ = """COALESCE(json_extract(r.body,'$.created_seq'),
    (SELECT json_extract(v.body,'$.ingestion_seq') FROM versions v
     WHERE v.id=r.id ORDER BY v.revision LIMIT 1),r.seq)"""


def thread_page(store, principal, project, board_id, thread_id, before=None, limit=100, query=""):
    """Return the newest page of the authorized thread in oldest-to-newest order.

    Visibility is evaluated per post before the page boundary is applied, so a
    caller can walk every post they are permitted to read with ``next_before``.
    """
    sql = (f"SELECT r.*, {CREATION_SEQ} AS creation_seq FROM resources r "
           "WHERE r.project=? AND r.kind='board_post' AND r.deleted=0 "
           "AND json_extract(r.body,'$.board_id')=? AND json_extract(r.body,'$.thread_id')=?")
    args = [project, board_id, thread_id]
    if before is not None:
        sql += f" AND {CREATION_SEQ}<?"
        args.append(before)
    sql += " ORDER BY creation_seq DESC"
    needle = query.strip().casefold()
    items = []
    for row in store.db.execute(sql, args):
        if not store.visible(row, principal):
            continue
        body = json.loads(row["body"])
        if needle and needle not in searchable(body).casefold():
            continue
        body["created_seq"] = row["creation_seq"]
        body["visibility"] = "Shared board thread"
        items.append(body)
        if len(items) > limit:
            break
    more = len(items) > limit
    items = items[:limit]
    ordered = list(reversed(items))
    if not ordered and before is None:
        # Hidden and missing threads are indistinguishable: both read as not found.
        raise Problem("not_found", 404)
    root = next((item["id"] for item in ordered if not item.get("parent_post_id")), None)
    cursor = hashlib.sha256(canonical(
        [[item["id"], item["revision"]] for item in ordered]).encode()).hexdigest()
    return {"thread_id": thread_id, "board_id": board_id, "project_id": project,
            "root_post_id": root, "items": ordered, "count": len(ordered), "has_more": more,
            "next_before": ordered[0]["created_seq"] if more and ordered else None,
            "snapshot_cursor": cursor}


def install(app, service, principal, command, envelope, policy=None):
    from .capabilities import replace_routes
    replace_routes(app, __name__)

    @app.get("/v1/boards/{board_id}/threads/{thread_id}/posts")
    async def thread_posts(request: Request, board_id: str, thread_id: str,
                           before: int | None = Query(None, ge=1),
                           limit: int = Query(100, ge=1, le=PAGE_LIMIT_MAX),
                           query: str = Query("", max_length=256),
                           snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                           p=Depends(principal)):
        """Read the authorized thread in display order, walking with next_before."""
        with service.store.lock:
            p = app.state.authenticate_request(request)
            board = service.store.get(board_id, p, "board")
            service.project_access(p, board["project_id"])
            cursor = app.state.checked_snapshot(p, board["project_id"], snapshot_cursor)
            page = thread_page(service.store, p, board["project_id"], board_id, thread_id, before, limit, query)
            return envelope({**page, "snapshot_cursor": cursor})
