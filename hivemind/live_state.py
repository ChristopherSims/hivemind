"""Cursor-bound live-state helpers for the dashboard invalidation stream.

Ground truth, read from ``hivemind/api.py`` (do not re-invent):

* ``GET /v1/streams?project_id=`` is an SSE *invalidation* channel. Frames carry
  no content: either ``event: resync`` with ``id: <snapshot_cursor>`` plus
  ``data: {"resync_required":true}``, or a bare ``: keepalive`` comment.
* ``snapshot_cursor`` (returned by ``GET /v1/boards/{id}/posts``,
  ``GET /v1/projects/{id}/conversations`` and
  ``GET /v1/agents/{id}/conversation``) is the opaque 64-hex ``sha256`` digest of
  the principal/project-bound visible snapshot (``api.stream_cursor``). It is
  **not** the activity-stream replay cursor (``s<n>.a<n>`` / ``g<n>``); the two
  are never interchangeable and a client must refuse to bind a foreign cursor.
* The route compares its bound cursor with the freshly computed one: a mismatch
  emits ``resync`` with the fresh ``id``; a match emits only ``keepalive``.
  Because a browser ``EventSource`` cannot set request headers, the snapshot
  cursor a client actually holds can only be bound through an explicit
  ``?cursor=`` query parameter (``Last-Event-ID`` only ever carries the last
  ``id:`` the server itself emitted).

What this module adds without editing ``api.py``:

* :func:`normalize_cursor` / :func:`bound_cursor` - refuse foreign or malformed
  cursors so a client never binds one (today an incompatible cursor silently
  produces a permanent resync loop instead of an error).
* :func:`terminal_frame` / :func:`classify_failure` - a revoked reader currently
  gets an aborted connection (the ``Problem`` raised mid-generator makes
  Starlette raise ``RuntimeError('Caught handled exception, but response already
  started.')``), which a client cannot tell apart from a network blip. A
  terminal ``event: revoked`` frame makes revocation explicit and immediate.
* :func:`feed` - the same loop, with a re-authentication and project-access
  recheck on every heartbeat, ready to be dropped into the route::

      from . import live_state

      def stream_snapshot(request, project_id):
          with store.lock:
              fresh = authenticate(request)
              service.project_access(fresh, project_id)
              lease = service.lease(fresh, project_id)
              return stream_cursor(fresh, project_id), (lease["id"] if lease else "")

      @app.get("/v1/streams")
      async def streams(request: Request, project_id: str,
                        cursor: str | None = Query(None, max_length=128),
                        p=Depends(principal)):
          service.project_access(p, project_id)
          return StreamingResponse(
              live_state.feed(request, lambda: stream_snapshot(request, project_id), cursor=cursor),
              media_type="text/event-stream",
              headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

The patched route keeps today's wire behavior byte-for-byte for authorized
readers (same frames, same cadence, same lease-change resync) and adds only the
optional ``cursor`` binding plus the terminal revocation frame.
"""
import asyncio
import json
import re

CURSOR_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
RESYNC_EVENT = "resync"
KEEPALIVE = ": keepalive\n\n"
HEARTBEAT_SECONDS = 1.0
# Terminal events close the stream for good: the reader's access is gone and no
# reconnect can succeed until it re-authenticates.
TERMINAL_EVENTS = ("revoked",)
# Problem statuses that are terminal for a bound stream: the principal vanished,
# lost the project, or the project itself is gone.
TERMINAL_STATUSES = {401: "session_revoked", 403: "permission_revoked", 404: "project_access_revoked"}


def normalize_cursor(value):
    """Return the canonical 64-hex cursor or ``None``.

    Anything else - including the activity-stream replay cursor format - is
    refused rather than coerced, so a client never binds an incompatible cursor.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip().lower()
    return candidate if CURSOR_PATTERN.match(candidate) else None


def bound_cursor(request, cursor=None):
    """Cursor a subscriber is bound to: explicit binding first, then Last-Event-ID.

    ``cursor`` is the ``?cursor=`` query parameter value the dashboard binds to
    the snapshot it actually displays; it wins over the header because a browser
    ``EventSource`` cannot set headers. An invalid explicit value falls back to
    the header instead of silently binding garbage.
    """
    explicit = normalize_cursor(cursor)
    if explicit is None and cursor is None:
        query = getattr(request, "query_params", None)
        explicit = normalize_cursor(query.get("cursor") if query is not None else None)
    if explicit is not None:
        return explicit
    headers = getattr(request, "headers", None)
    return normalize_cursor(headers.get("last-event-id") if headers is not None else None)


def resync_frame(cursor):
    return f"id: {cursor}\nevent: {RESYNC_EVENT}\ndata: {{\"resync_required\":true}}\n\n"


def classify_failure(error):
    """Map an exception raised by a re-authentication/access recheck to a
    terminal failure, or ``None`` when the stream may simply be retried."""
    status = getattr(error, "status", None)
    code = getattr(error, "code", None)
    if status not in TERMINAL_STATUSES or not isinstance(code, str):
        return None
    return {"event": TERMINAL_EVENTS[0], "status": status, "code": code,
            "reason": TERMINAL_STATUSES[status]}


def terminal_frame(failure):
    """Explicit terminal frame: the client clears all privileged UI at once and
    stops reconnecting instead of guessing from an aborted connection."""
    payload = {"resync_required": True, "reason": failure["reason"],
               "status": failure["status"], "code": failure["code"]}
    return f"event: {failure['event']}\ndata: {json.dumps(payload, sort_keys=True)}\n\n"


def parse_frame(chunk):
    """Parse one SSE frame into ``{"id", "event", "data", "comment"}``.

    Shared by tests and by any non-browser client; the dashboard parses the same
    shape in TypeScript (``dashboard/src/live-state.ts``).
    """
    frame = {"id": None, "event": None, "data": None, "comment": None}
    for line in chunk.splitlines():
        if not line or line.startswith(":"):
            frame["comment"] = line[1:].strip() or frame["comment"]
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field in frame:
            frame[field] = value
    return frame


async def feed(request, snapshot, *, cursor=None, interval=HEARTBEAT_SECONDS, sleep=None):
    """Yield SSE frames for one subscriber until it disconnects or is revoked.

    ``snapshot`` is a zero-argument callable returning ``(cursor, lease_id)``.
    It runs once per heartbeat and must re-authenticate the request and recheck
    project access every time, raising ``Problem`` when access was revoked. Only
    terminal authorization failures end the stream with a terminal frame;
    anything else propagates so a transient failure stays a transient failure.
    """
    sleep = asyncio.sleep if sleep is None else sleep
    current = bound_cursor(request, cursor)
    while not await request.is_disconnected():
        try:
            fresh, lease = snapshot()
        except Exception as failure:  # noqa: BLE001 - re-raised unless terminal
            terminal = classify_failure(failure)
            if terminal is None:
                raise
            yield terminal_frame(terminal)
            return
        fresh = normalize_cursor(fresh)
        if fresh is not None and fresh != current:
            yield resync_frame(fresh)
            current = fresh
        else:
            yield KEEPALIVE
        await sleep(interval)
        # Role expiry changes styling even without a new canonical write.
        try:
            _, active_lease = snapshot()
        except Exception as failure:  # noqa: BLE001 - re-raised unless terminal
            terminal = classify_failure(failure)
            if terminal is None:
                raise
            yield terminal_frame(terminal)
            return
        if active_lease != lease:
            current = None
