"""Author-scoped mention receipts so the composer can report queued mentions honestly.

``POST /v1/boards/{board_id}/posts`` already creates one canonical
``notification`` resource per mentioned agent (``type='board_mention'``). Those
records are created with ``private=<agent>``, so the *author* cannot read them
through the ordinary visibility rules, and the canonical post response does not
carry them either. This module owns exactly one read-only endpoint that reports,
for a post the caller authored, whether such a canonical notification record
exists for each recipient, plus the coarsest possible presence hint.

Deliberate limits:

* Only the post's own author may read receipts. Anyone else -- including an
  operator who did not author the post -- sees ``404`` so the read can never
  widen what a post exposes.
* Only agents actually named in *that* post are reported, in mention order.
* Each recipient row is a fixed whitelist of fields: the agent id, the agent's
  display name (only while the caller can still see that agent), the canonical
  notification id, a normalized ``queued``/``closed`` state, the notification's
  own timestamps and a coarse presence band. Never message bodies, run ids,
  run summaries, task titles, or recipient processing state.
* Nothing here mutates canonical state and no command route is installed.
"""
import json
import time

from fastapi import Depends, Request

from .storage import Problem

# Same presence bands as ``GET /v1/ledger`` so a receipt never contradicts the roster.
PRESENCE_ONLINE_SECONDS = 15
PRESENCE_STALE_SECONDS = 30

# Normalized receipt states. "queued" is a claim we only make about a canonical
# notification record that is still available; everything else (acked, expired)
# collapses to the coarse "closed" so recipient behaviour is never disclosed.
QUEUED = "queued"
CLOSED = "closed"


def presence_for(gap: float | None) -> str | None:
    """Coarse presence from the freshest run gap, or ``None`` when the agent has no run."""
    if gap is None:
        return None
    if gap < PRESENCE_ONLINE_SECONDS:
        return "online"
    if gap < PRESENCE_STALE_SECONDS:
        return "stale"
    return "offline"


def freshest_run_gaps(store, principal, project, stamp):
    """``{agent_id: seconds since its most recent run was seen}`` for readable runs."""
    gaps = {}
    for run in store.list("run", project, principal, 1000):
        agent = run.get("agent_id")
        if not agent:
            continue
        try:
            seen = float(run.get("last_seen_at") or 0)
        except (TypeError, ValueError):
            continue
        gap = max(0.0, stamp - seen)
        if agent not in gaps or gap < gaps[agent]:
            gaps[agent] = gap
    return gaps


def mentioned_agents(post):
    """Agent ids named by a post's segments, in mention order, deduplicated."""
    seen = []
    for segment in (post.get("content") or {}).get("segments") or []:
        if isinstance(segment, dict) and segment.get("kind") == "agent_mention":
            agent = segment.get("agent_id")
            if agent and agent not in seen:
                seen.append(agent)
    return seen


def canonical_notifications(store, project, post_id):
    """Canonical ``board_mention`` notification rows for one post, keyed by recipient."""
    rows = store.db.execute(
        "SELECT * FROM resources WHERE kind='notification' AND project=? AND deleted=0 "
        "AND json_extract(body,'$.post_id')=? AND json_extract(body,'$.type')='board_mention'",
        (project, post_id)).fetchall()
    records = {}
    for row in rows:
        try:
            body = json.loads(row["body"])
        except (TypeError, ValueError):
            continue
        recipient = body.get("recipient_agent_id")
        if recipient and recipient not in records:
            records[recipient] = (row["id"], body)
    return records


def mention_receipts(service, principal, post_id, stamp=None):
    """Report the canonical queued-mention receipts for one authored post.

    Raises ``Problem('not_found', 404)`` for a deleted, invisible, or
    not-authored post, so missing and forbidden are indistinguishable.
    """
    store = service.store
    stamp = time.time() if stamp is None else stamp
    post = store.get(post_id, principal, "board_post")
    author = post.get("author") or {}
    owned = (
        (author.get("kind") == "human" and author.get("principal_id") == principal.get("id"))
        or (author.get("kind") == "agent" and principal.get("agent")
            and author.get("agent_id") == principal.get("agent"))
    )
    if not owned:
        raise Problem("not_found", 404)
    project = post["project_id"]
    service.project_access(principal, project)

    records = canonical_notifications(store, project, post_id)
    gaps = freshest_run_gaps(store, principal, project, stamp)
    recipients = []
    for agent_id in mentioned_agents(post):
        record = records.get(agent_id)
        if record is None:
            # No canonical notification exists: disclose nothing rather than guess.
            continue
        notification_id, body = record
        agent_row = store.db.execute(
            "SELECT * FROM resources WHERE id=? AND kind='agent' AND deleted=0", (agent_id,)).fetchone()
        if agent_row is None or not store.visible(agent_row, principal):
            # Recipient is gone or no longer visible to the caller: drop the receipt.
            continue
        try:
            display_name = json.loads(agent_row["body"]).get("display_name")
        except (TypeError, ValueError):
            display_name = None
        expires_at = body.get("expires_at")
        expires_at = expires_at if isinstance(expires_at, (int, float)) else None
        still_waiting = body.get("state") == "available" and (expires_at is None or expires_at > stamp)
        recipients.append({
            "agent_id": agent_id,
            "agent_display_name": display_name,
            "notification_id": notification_id,
            "state": QUEUED if still_waiting else CLOSED,
            "created_at": body.get("created_at"),
            "expires_at": expires_at,
            "presence": presence_for(gaps.get(agent_id)),
        })
    return {
        "post_id": post_id,
        "recipients": recipients,
        "queued_count": sum(1 for item in recipients if item["state"] == QUEUED),
        "generated_at": stamp,
    }


def install(app, service, principal, command, envelope, policy=None):
    from .capabilities import replace_routes
    replace_routes(app, __name__)

    @app.get("/v1/posts/{post_id}/mention-receipts")
    async def post_mention_receipts(request: Request, post_id: str, p=Depends(principal)):
        """Read this authored post's canonical queued-mention receipts."""
        with service.store.lock:
            p = app.state.authenticate_request(request)
            return envelope(mention_receipts(service, p, post_id))
