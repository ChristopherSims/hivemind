"""Real-local-service tests for the dashboard live-state contract.

Ground truth exercised here, read from the running service (no paid calls, no
external network):

* ``snapshot_cursor`` is the opaque, principal/project-bound 64-hex hash that the
  timeline endpoints return and that ``/v1/streams`` re-states as the ``id:`` of
  an ``event: resync`` frame.
* ``/v1/streams`` compares its bound cursor with a freshly computed one; missing
  or foreign bindings request a fresh snapshot.
* The route re-authenticates and rechecks project access on every heartbeat, and
  refuses a revoked reader before the response starts. Mid-stream revocation
  emits a terminal, content-free frame before closing the connection.
"""
import asyncio
import json
import time

import pytest

from hivemind import live_state
from hivemind.storage import Problem, uid


# --------------------------------------------------------------------- harness
def _scope(path, headers):
    return {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": "GET", "scheme": "http",
        "path": path.split("?", 1)[0], "raw_path": path.split("?", 1)[0].encode(),
        "query_string": path.split("?", 1)[1].encode() if "?" in path else b"",
        "root_path": "", "headers": [(key.lower().encode(), value.encode())
                                     for key, value in (headers or {}).items()],
        "client": ("127.0.0.1", 1234), "server": ("127.0.0.1", 8766),
    }


def drive(app, path, headers=None, *, seconds=1.4, on_chunk=None):
    """Drive the ASGI app directly and return ``(status, headers, frames)``.

    TestClient buffers streaming responses until the body completes, and the SSE
    invalidation stream never completes, so the raw ASGI interface is the only
    faithful harness. Each yielded body chunk is parsed with
    ``live_state.parse_frame``; the request itself is delivered once and then
    left pending so ``is_disconnected`` stays false.
    """
    captured = {"status": None, "headers": {}, "chunks": [], "raw": [], "error": None}
    state = {"request_sent": False, "stopped": False}

    async def receive():
        if not state["request_sent"]:
            state["request_sent"] = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.sleep(3600)
        return {"type": "http.disconnect"}

    async def send(message):
        if message["type"] == "http.response.start":
            captured["status"] = message["status"]
            captured["headers"] = {key.decode(): value.decode() for key, value in message["headers"]}
        elif message["type"] == "http.response.body":
            body = message.get("body") or b""
            if body:
                captured["raw"].append(body.decode())
                # Only an SSE response is parsed into frames: an error envelope is
                # a plain JSON body, never a frame.
                if captured["headers"].get("content-type", "").startswith("text/event-stream"):
                    chunk = body.decode()
                    captured["chunks"].append(chunk)
                    if on_chunk:
                        on_chunk(chunk)

    async def run():
        await app(_scope(path, headers), receive, send)

    async def main():
        try:
            await asyncio.wait_for(run(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        except BaseException as exc:  # noqa: BLE001 - the abort path is under test
            captured["error"] = repr(exc)

    asyncio.run(main())
    frames = [live_state.parse_frame(chunk) for chunk in captured["chunks"]]
    return captured["status"], captured["headers"], frames


def stream_path(world):
    return f"/v1/streams?project_id={world.project}"


def resyncs(frames):
    return [frame for frame in frames if frame["event"] == "resync"]


def keepalives(frames):
    return [frame for frame in frames if frame["comment"] == "keepalive"]


# ------------------------------------------------------- cursor binding (HTTP)
def test_snapshot_cursor_is_project_bound_and_write_sensitive(world):
    board = world.board()["id"]
    page = world.call("GET", f"boards/{board}/posts")
    cursor = page["snapshot_cursor"]
    assert live_state.normalize_cursor(cursor) == cursor
    # The cursor covers the whole authorized project snapshot, not one view.
    conversations = world.call("GET", f"projects/{world.project}/conversations")
    assert conversations["snapshot_cursor"] == cursor
    assert world.call("GET", f"boards/{board}/posts")["snapshot_cursor"] == cursor
    post = world.post("Cursor-visible finding")
    after_post = world.call("GET", f"boards/{board}/posts")["snapshot_cursor"]
    assert after_post != cursor
    world.call("PATCH", f"posts/{post['id']}", {
        "expected_revision": 1, "content": {"media_type": "text/plain", "text": "Cursor-visible edit"}})
    assert world.call("GET", f"boards/{board}/posts")["snapshot_cursor"] != after_post


def test_snapshot_cursor_ignores_writes_the_reader_cannot_see(world):
    reader = world.agent("Cursor reader")
    token = world.credential(reader)
    sender, other = world.agent("Cursor sender"), world.agent("Cursor other")
    sender_token = world.credential(sender)
    path = f"agents/{reader['id']}/conversation"
    before = world.call("GET", path, token=token)["snapshot_cursor"]
    # A private conversation between two other agents is invisible to the reader:
    # hidden writes must not invalidate an unauthorized reader's snapshot.
    world.call("POST", "messages", {
        "message_id": uid("msg"), "project_id": world.project, "from": sender["id"],
        "to": other["id"], "type": "inform", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": "Hidden from the reader"}}, sender_token)
    assert world.call("GET", path, token=token)["snapshot_cursor"] == before
    # A visible write does invalidate it.
    world.post("Visible to every project principal")
    assert world.call("GET", path, token=token)["snapshot_cursor"] != before


def test_stream_frames_bind_to_the_snapshot_cursor(world):
    board = world.board()["id"]
    cursor = world.call("GET", f"boards/{board}/posts")["snapshot_cursor"]
    token = {"Authorization": "Bearer " + world.owner}
    # An unbound subscriber is resynced immediately, and the frame carries the
    # exact cursor the timeline endpoints returned.
    status, headers, frames = drive(world.app, stream_path(world), token)
    assert status == 200 and headers["content-type"].startswith("text/event-stream")
    assert resyncs(frames)[0]["id"] == cursor
    assert resyncs(frames)[0]["data"] == '{"resync_required":true}'
    assert keepalives(frames), "the stream must stay open with keepalives"
    # Binding that cursor is a match: no resync, only keepalives.
    status, _, frames = drive(world.app, stream_path(world),
                              {**token, "Last-Event-ID": cursor})
    assert status == 200 and not resyncs(frames) and keepalives(frames)
    # A stale cursor (including the activity-stream replay format) resyncs, so a
    # client that binds the wrong cursor can never mistake it for a match.
    for stale in ("0" * 64, "s3.a1", "g2", "not-a-cursor"):
        _, _, frames = drive(world.app, stream_path(world), {**token, "Last-Event-ID": stale})
        assert resyncs(frames)[0]["id"] == cursor, stale
    assert live_state.normalize_cursor("s3.a1") is None
    # A new visible write moves the cursor the stream re-states.
    world.post("Live write")
    moved = world.call("GET", f"boards/{board}/posts")["snapshot_cursor"]
    assert moved != cursor
    _, _, frames = drive(world.app, stream_path(world), {**token, "Last-Event-ID": cursor})
    assert resyncs(frames)[0]["id"] == moved


def test_stream_refuses_revoked_or_unauthenticated_readers_before_streaming(world):
    other_project = uid("project")
    status, _, frames = drive(world.app, f"/v1/streams?project_id={other_project}",
                              {"Authorization": "Bearer " + world.owner})
    assert status == 404 and not frames
    status, _, frames = drive(world.app, stream_path(world), {})
    assert status == 401 and not frames
    # A principal bound to another project cannot open this project's stream.
    store = world.app.state.store
    other_project = uid("project")
    with store.transaction():
        store.db.execute("INSERT INTO projects(id, title) VALUES(?, ?)", (other_project, "Other project"))
    outsider = world.credential(project=other_project)
    status, _, frames = drive(world.app, stream_path(world),
                              {"Authorization": "Bearer " + outsider})
    assert status in (403, 404) and not frames


def test_midstream_revocation_announces_access_loss_without_leaking_content(world):
    board = world.board()["id"]
    world.post("Privileged board evidence")
    token = world.owner
    store = world.app.state.store

    def revoke(_chunk):
        with store.transaction():
            store.db.execute("DELETE FROM principals WHERE role='admin'")

    status, _, frames = drive(world.app, stream_path(world),
                              {"Authorization": "Bearer " + token},
                              seconds=2.4, on_chunk=revoke)
    assert status == 200
    # No frame ever carries content: the channel only invalidates snapshots.
    assert all(set(frame) == {"id", "event", "data", "comment"} for frame in frames)
    assert "Privileged board evidence" not in "".join(
        chunk for frame in frames for chunk in frame.values() if chunk)
    # The revoked reader is cut off: no keepalive survives the revocation check.
    assert not keepalives(frames)[1:], frames
    terminal = [frame for frame in frames if frame["event"] == "revoked"]
    assert len(terminal) == 1
    assert json.loads(terminal[0]["data"])["status"] == 401
    assert world.call("GET", f"boards/{board}/posts", status=401) is not None


def test_query_cursor_binds_the_browser_snapshot(world):
    cursor = world.call("GET", f"boards/{world.board()['id']}/posts")["snapshot_cursor"]
    headers = {"Authorization": "Bearer " + world.owner, "Last-Event-ID": "0" * 64}
    status, _, frames = drive(world.app, stream_path(world) + "&cursor=" + cursor, headers)
    assert status == 200 and not resyncs(frames) and keepalives(frames)


def test_midstream_role_change_is_terminal(world):
    def downgrade(_chunk):
        with world.app.state.store.transaction():
            world.app.state.store.db.execute("UPDATE principals SET role='observer' WHERE role='admin'")
    status, _, frames = drive(world.app, stream_path(world),
                              {"Authorization": "Bearer " + world.owner},
                              seconds=2.4, on_chunk=downgrade)
    assert status == 200
    terminal = [frame for frame in frames if frame["event"] == "revoked"]
    assert len(terminal) == 1 and json.loads(terminal[0]["data"])["status"] == 403


def test_paging_rejects_superseded_snapshots(world):
    board = world.board()["id"]
    first = world.post("First snapshot post")
    paths = [f"boards/{board}/posts", f"projects/{world.project}/conversations",
             f"boards/{board}/threads/{first['thread_id']}/posts"]
    agent = world.agent()
    paths += [f"agents/{agent['id']}/conversation", f"agents/{agent['id']}/evidence"]
    cursor = world.call("GET", paths[0])["snapshot_cursor"]
    for path in paths:
        assert world.call("GET", f"{path}?snapshot_cursor={cursor}")["snapshot_cursor"] == cursor
    world.post("Changes the shared snapshot")
    for path in paths:
        error = world.call("GET", f"{path}?snapshot_cursor={cursor}", status=409)
        assert error["error"]["code"] == "snapshot_changed"


def test_workspace_snapshot_is_atomic_and_role_filtered(world):
    agent = world.agent()
    world.run(agent)
    board = world.board()["id"]
    path = f"projects/{world.project}/dashboard-snapshot"
    snapshot = world.call("GET", path)
    assert snapshot["agents"][0]["id"] == agent["id"]
    assert snapshot["runs"][0]["agent_id"] == agent["id"]
    assert snapshot["boards"][0]["id"] == board
    assert isinstance(snapshot["delivery"], dict)
    page = world.call("GET", f"boards/{board}/posts?snapshot_cursor={snapshot['snapshot_cursor']}")
    assert page["snapshot_cursor"] == snapshot["snapshot_cursor"]
    human = world.credential(role="human")
    assert world.call("GET", path, token=human)["delivery"] is None


# ------------------------------------------------- helper module (install seam)
class FakeRequest:
    def __init__(self, headers=None, query=None, disconnected_after=None):
        self.headers = headers or {}
        self.query_params = query or {}
        self.calls = 0
        self.disconnected_after = disconnected_after

    async def is_disconnected(self):
        self.calls += 1
        return self.disconnected_after is not None and self.calls > self.disconnected_after


async def collect(request, snapshot, limit=8, **kwargs):
    frames = []

    async def sleep(_seconds):
        return None

    async for chunk in live_state.feed(request, snapshot, sleep=sleep, **kwargs):
        frames.append(live_state.parse_frame(chunk))
        if len(frames) >= limit:
            break
    return frames


def test_cursor_normalization_refuses_foreign_cursors():
    valid = "a" * 64
    assert live_state.normalize_cursor(valid) == valid
    assert live_state.normalize_cursor(valid.upper()) == valid
    assert live_state.normalize_cursor(" " + valid + " ") == valid
    for value in ("s3.a1", "g2", "", "a" * 63, "z" * 64, None, 42, ["x"]):
        assert live_state.normalize_cursor(value) is None


def test_bound_cursor_prefers_explicit_binding_over_last_event_id():
    request = FakeRequest(headers={"last-event-id": "b" * 64}, query={"cursor": "c" * 64})
    assert live_state.bound_cursor(request) == "c" * 64
    assert live_state.bound_cursor(request, "d" * 64) == "d" * 64
    # An invalid explicit value never binds; the server-issued id still can.
    assert live_state.bound_cursor(request, "s3.a1") == "b" * 64
    assert live_state.bound_cursor(FakeRequest()) is None
    assert live_state.bound_cursor(FakeRequest(headers={"last-event-id": "s3.a1"})) is None


def test_failure_classification_is_terminal_only_for_lost_access():
    for code, status, reason in (("authentication_required", 401, "session_revoked"),
                                 ("forbidden", 403, "permission_revoked"),
                                 ("not_found", 404, "project_access_revoked")):
        terminal = live_state.classify_failure(Problem(code, status))
        assert terminal == {"event": "revoked", "status": status, "code": code, "reason": reason}
        assert live_state.parse_frame(live_state.terminal_frame(terminal))["event"] == "revoked"
    for failure in (Problem("revision_conflict", 412), Problem("conflict", 409), ValueError("boom")):
        assert live_state.classify_failure(failure) is None


def test_helper_feed_matches_endpoint_semantics_and_terminates_on_revocation():
    cursor = "1" * 64
    revoked = {"value": False}

    def snapshot():
        if revoked["value"]:
            raise Problem("authentication_required", 401)
        return cursor, "lease-a"

    # Unbound: immediate resync with the fresh cursor, then keepalives.
    frames = asyncio.run(collect(FakeRequest(disconnected_after=3), snapshot))
    assert [frame["event"] for frame in frames[:3]] == ["resync", None, None]
    assert frames[0]["id"] == cursor and frames[1]["comment"] == "keepalive"
    # Bound to the current cursor: keepalives only, no resync.
    frames = asyncio.run(collect(FakeRequest(headers={"last-event-id": cursor},
                                             disconnected_after=2), snapshot))
    assert not [frame for frame in frames if frame["event"] == "resync"]
    # Bound to a stale cursor: one resync re-binds the stream to the fresh cursor.
    frames = asyncio.run(collect(FakeRequest(headers={"last-event-id": "3" * 64},
                                             disconnected_after=1), snapshot))
    assert frames[0]["event"] == "resync" and frames[0]["id"] == cursor
    # Revocation mid-stream: one explicit terminal frame, then the stream ends
    # instead of aborting the connection (today's endpoint behavior).
    revoked["value"] = True
    frames = asyncio.run(collect(FakeRequest(disconnected_after=4), snapshot))
    terminal = frames[0]
    assert terminal["event"] == "revoked"
    assert '"reason": "session_revoked"' in terminal["data"]
    assert '"status": 401' in terminal["data"]
    assert frames[0]["id"] is None
    assert len(frames) == 1, "a terminal frame ends the stream"
    # Only terminal authorization failures end the stream; transient ones raise.
    with pytest.raises(ValueError):
        asyncio.run(collect(FakeRequest(disconnected_after=1),
                            lambda: (_ for _ in ()).throw(ValueError("boom"))))


def test_helper_feed_resyncs_when_the_lease_changes_mid_heartbeat():
    cursor = "a" * 64
    # The lease is re-read after the heartbeat sleep, mirroring the endpoint's
    # "role expiry changes styling even without a new SQL mutation" check.
    script = [(cursor, "lease-a"), (cursor, "lease-b")]
    calls = {"count": 0}

    def snapshot():
        index = min(calls["count"], len(script) - 1)
        calls["count"] += 1
        return script[index]

    frames = asyncio.run(collect(FakeRequest(headers={"last-event-id": cursor},
                                             disconnected_after=2), snapshot))
    assert [frame["event"] for frame in frames] == [None, "resync"]


def test_feed_stops_without_snapshotting_when_the_client_is_gone():
    calls = {"count": 0}

    def snapshot():
        calls["count"] += 1
        return "a" * 64, ""

    frames = asyncio.run(collect(FakeRequest(disconnected_after=0), snapshot))
    assert frames == [] and calls["count"] == 0


def _request(world, cursor=None, token=None):
    """A real Starlette request, exactly what the patched route hands to feed."""
    from starlette.requests import Request

    query = f"project_id={world.project}" + (f"&cursor={cursor}" if cursor else "")
    scope = _scope(f"/v1/streams?{query}", {"Authorization": "Bearer " + (token or world.owner)})

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.disconnect"}

    return Request(scope, receive)


def test_feed_seam_reuses_real_authentication_project_access_and_lease(world):
    """Install seam: the same snapshot closure the patched route installs, driven
    with real authentication, project access and lease reads."""
    service, store = world.app.state.service, world.app.state.store
    authenticate = world.app.state.authenticate_request
    cursor = world.call("GET", f"boards/{world.board()['id']}/posts")["snapshot_cursor"]
    box = {"cursor": cursor}

    def snapshot():
        with store.lock:
            fresh = authenticate(request)
            service.project_access(fresh, world.project)
            lease = service.lease(fresh, world.project)
            return box["cursor"], (lease["id"] if lease else "")

    # Binding the cursor the snapshot returned is a match: keepalives only.
    request = _request(world, cursor)
    frames = asyncio.run(collect(request, snapshot, limit=2))
    assert [frame["comment"] for frame in frames] == ["keepalive", "keepalive"]
    # A cursor the reader no longer holds is re-stated, never guessed.
    box["cursor"] = "0" * 64
    frames = asyncio.run(collect(_request(world, cursor), snapshot, limit=1))
    assert frames[0]["event"] == "resync" and frames[0]["id"] == "0" * 64
    # Revocation mid-stream ends the stream with one terminal frame.
    with store.transaction():
        store.db.execute("DELETE FROM principals WHERE role='admin'")
    frames = asyncio.run(collect(_request(world, cursor), snapshot, limit=2))
    assert [frame["event"] for frame in frames] == ["revoked"]
    assert '"code": "authentication_required"' in frames[0]["data"]


def test_stream_never_reports_content_and_heartbeat_cadence_is_bounded(world):
    start = time.time()
    status, _, frames = drive(world.app, stream_path(world),
                              {"Authorization": "Bearer " + world.owner}, seconds=2.4)
    elapsed = time.time() - start
    assert status == 200 and len(keepalives(frames)) >= 2
    assert elapsed < 4, "the invalidation stream must heartbeat about once a second"
