"""Real-local-service tests for the agent-targeted SSE message push module.

No paid calls, no external network: the canonical service runs in-process via
``world``/``TestClient``; the push hub keeps its own durable SQLite log.
"""
import threading
import time

import pytest
from fastapi import Request

from hivemind import message_push
from hivemind.api import hash_token
from hivemind.message_push import (OPERATION_DESCRIPTORS, CommitSignalBridge,
                                   MessagePushHub, MessagePushPolicy,
                                   PushStreamError, PushWakeSource,
                                   bind_commit_source, descriptors,
                                   httpx_sse_subscription, install, parse_sse)
from hivemind.storage import Problem, uid


def principal_for(app):
    """Test-local equivalent of api.create_app's principal dependency."""

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            row = app.state.store.db.execute(
                "SELECT * FROM principals WHERE token_hash=?",
                (hash_token(auth[7:]),)).fetchone()
            if row:
                return dict(row)
        raise Problem("authentication_required", 401)

    return principal


def install_on(world, **kwargs):
    app = world.app
    kwargs.setdefault("policy", MessagePushPolicy(enabled=True))
    kwargs.setdefault("bridge", False)  # bridge behavior has dedicated tests
    return install(app, app.state.service, principal_for(app), None,
                   lambda data: {"data": data}, **kwargs)


def fast_policy(**overrides):
    base = {"enabled": True, "max_stream_seconds": 2.0, "heartbeat_seconds": 1.0}
    base.update(overrides)
    return MessagePushPolicy(**base)


def read_frames(response, count, timeout=10.0):
    """Collect SSE frames (blank-line terminated) from a streaming response."""
    frames, current = [], []
    deadline = time.time() + timeout
    for line in response.iter_lines():
        if time.time() > deadline:
            break
        if line == "":
            if current:
                frames.append(current)
                current = []
                if len(frames) >= count:
                    break
        else:
            current.append(line)
    return frames


def open_stream(world, agent_id, token, after=0, headers=None):
    return world.client.stream(
        "GET", f"/v1/agents/{agent_id}/message-stream?project_id={world.project}"
               f"&after={after}",
        headers={"Authorization": "Bearer " + token, **(headers or {})})


P, A, B = uid("project"), uid("agent"), uid("agent")  # strict-shaped fixture IDs


def send_message(world, sender, receiver, token, text="Ping?"):
    return world.call("POST", "messages", {
        "project_id": world.project, "message_id": uid("msg"), "from": sender["id"],
        "to": receiver["id"], "type": "request", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": text}}, token)


# --------------------------------------------------------------------- policy
def test_install_default_disabled_is_safe_and_never_raises(world):
    result = install_on(world, policy=MessagePushPolicy())  # enabled defaults False
    assert result is None and world.app.state.message_push_hub is None
    agent = world.agent("Agent")
    token = world.credential(agent)
    with open_stream(world, agent["id"], token) as response:
        assert response.status_code == 503
        response.read()
        assert response.json()["error"]["code"] == "feature_disabled"


def test_policy_bounds_and_types_are_strict():
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=True, max_pending_events=0)
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=True, max_stream_seconds=9999)
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=True, max_subscribers_per_agent=0)
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=True, replay_limit=0)
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=1)  # bool only, no int coercion
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=True, max_pending_events=True)  # bool is not int
    with pytest.raises(ValueError):
        MessagePushPolicy(enabled=True, max_stream_seconds=float("nan"))


def test_descriptors_are_self_describing():
    described = descriptors()
    assert described == OPERATION_DESCRIPTORS
    assert set(described) == {"message_push.stream", "message_push.commit_bridge",
                              "message_push.notify_post_commit"}
    for descriptor in described.values():
        for field in ("purpose", "input", "output", "authority", "side_effects",
                      "limits", "retry_safety"):
            assert descriptor[field], field


# ------------------------------------------------------------------ hub logic
def test_publish_is_identity_only_strict_and_durable(tmp_path):
    hub = MessagePushHub(tmp_path / "push.sqlite", fast_policy())
    with pytest.raises(ValueError):
        hub.publish(P, A, "content", uid("msg"))  # kinds are signals only
    with pytest.raises(ValueError):
        hub.publish(P, A, "message", "msg-1")  # non-canonical ID shape refused
    with pytest.raises(ValueError):
        hub.publish("project", A, "message", uid("msg"))
    with pytest.raises(ValueError):
        hub.publish(P, A, "message", uid("msg"), source_seq=True)  # bool denied
    item = uid("msg")
    cursor = hub.publish(P, A, "message", item, source_seq=7)
    assert cursor == 1 == hub.latest_cursor(P, A)
    assert hub.publish(P, A, "message", item, source_seq=7) is None  # outbox dedup
    hub.close()
    # Restart over the same log: cursors and the replay window are durable.
    reopened = MessagePushHub(tmp_path / "push.sqlite", fast_policy())
    assert reopened.latest_cursor(P, A) == 1
    window = reopened.replay(P, A)
    assert window == {"events": [{"seq": 1, "kind": "message", "item_id": item}],
                      "resync": False, "latest": 1}
    assert reopened.replay(P, A, after=1)["events"] == []
    # Per-agent cursors: agent B sees none of A's traffic, not even its volume.
    assert reopened.replay(P, B)["events"] == [] and reopened.latest_cursor(P, B) == 0
    assert reopened.replay(uid("project"), A)["events"] == []
    reopened.close()


def test_replay_window_truncation_signals_resync_and_limits_are_denied(tmp_path):
    hub = MessagePushHub(tmp_path / "push.sqlite", fast_policy(replay_limit=2))
    items = [uid("msg") for _ in range(3)]
    for index, item in enumerate(items):
        hub.publish(P, A, "message", item, source_seq=index + 1)
    window = hub.replay(P, A, after=0)  # seq 1 was pruned: cursor outside window
    assert window["resync"] is True
    assert [event["seq"] for event in window["events"]] == [2, 3]
    assert [event["item_id"] for event in window["events"]] == items[1:]
    with pytest.raises(ValueError):
        hub.replay(P, A, limit=-1)  # negative bypass is denied, never clamped
    with pytest.raises(ValueError):
        hub.replay(P, A, limit=0)
    with pytest.raises(ValueError):
        hub.replay(P, A, limit=3)  # beyond the policy bound
    with pytest.raises(ValueError):
        hub.replay(P, A, after=-1)
    with pytest.raises(ValueError):
        hub.replay(P, A, after=True)
    hub.close()


def test_notify_post_commit_handles_recipient_shapes(tmp_path):
    hub = MessagePushHub(tmp_path / "push.sqlite", fast_policy())
    item = uid("msg")
    cursor = hub.notify_record_post_commit({"id": item, "project_id": P, "to": A,
                                            "from": uid("agent"),
                                            "content": {"text": "private body"}})
    event = hub.replay(P, A)["events"][0]
    assert event == {"seq": cursor, "kind": "message", "item_id": item}
    assert "from" not in event and "content" not in event  # identity only
    listed = [uid("agent"), uid("agent")]
    cursors = hub.notify_record_post_commit({"id": uid("msg"), "project_id": P,
                                             "to": listed})
    assert len(cursors) == 2
    assert [hub.latest_cursor(P, recipient) for recipient in listed] == [1, 1]
    note = uid("note")
    hub.notify_record_post_commit({"id": note, "project_id": P,
                                   "recipient_agent_id": A})
    assert [e["kind"] for e in hub.replay(P, A)["events"]] == ["message",
                                                               "notification"]
    hub.close()
    with pytest.raises(Problem):
        hub.notify_record_post_commit({"id": uid("msg"), "project_id": P, "to": A})


def test_subscribe_and_replay_are_atomic_under_a_publication_race(tmp_path):
    """A live publish concurrent with subscribe must neither overtake nor duplicate
    the replay backlog: the subscriber sees one strictly ordered sequence."""
    hub = MessagePushHub(tmp_path / "push.sqlite", fast_policy())
    total = 60
    for index in range(30):  # backlog
        hub.publish(P, A, "message", uid("msg"), source_seq=index + 1)
    observed = []

    def subscriber():
        subscription = hub.subscribe(P, A, after=0)
        while True:
            event = subscription.queue.get(timeout=10)
            if event is message_push._CLOSE:
                return
            observed.append(event["seq"])

    thread = threading.Thread(target=subscriber, daemon=True)
    thread.start()
    for index in range(30, total):  # live publications racing the subscription
        hub.publish(P, A, "message", uid("msg"), source_seq=index + 1)
    deadline = time.time() + 10
    while len(observed) < total and time.time() < deadline:
        time.sleep(0.02)
    hub.close()
    thread.join(timeout=10)
    assert sorted(observed) == list(range(1, total + 1))  # no loss, no duplication
    assert len(set(observed)) == len(observed)
    assert observed == sorted(observed)  # never out of order


def test_backpressure_overflow_signals_resync_instead_of_dropping(tmp_path):
    hub = MessagePushHub(tmp_path / "push.sqlite", fast_policy(max_pending_events=2))
    subscription = hub.subscribe(P, A)
    for _ in range(5):
        hub.publish(P, A, "message", uid("msg"))
    received = []
    while True:
        event = subscription.queue.get(timeout=5)
        received.append(event)
        if event is message_push._OVERFLOW:
            break
    assert len(received) <= 3  # two pending + the definite overflow sentinel
    hub.close()


def test_subscriber_bound_and_shutdown_are_enforced(tmp_path):
    hub = MessagePushHub(tmp_path / "push.sqlite",
                         fast_policy(max_subscribers_per_agent=1))
    subscription = hub.subscribe(P, A)
    with pytest.raises(Problem) as failure:
        hub.subscribe(P, A)
    assert failure.value.status == 429
    hub.close()
    assert subscription.queue.get(timeout=5) is message_push._CLOSE
    with pytest.raises(Problem):
        hub.subscribe(P, A)
    # stats stay safe and non-revealing after close.
    assert hub.stats()["closed"] is True and hub.stats()["subscribers"] == 0
    assert hub.stats()["signals"] is None


# --------------------------------------------------------------- SSE over HTTP
def test_stream_requires_authentication_and_agent_authorization(world):
    hub = install_on(world, policy=fast_policy())
    agent, other = world.agent("Agent"), world.agent("Other")
    token, other_token = world.credential(agent), world.credential(other)
    hub.publish(world.project, agent["id"], "message", uid("msg"))
    # The item does not exist canonically: visibility filtering drops it, so the
    # stream is just heartbeats — identity of a non-visible item never leaks.
    with open_stream(world, agent["id"], token) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        frames = [frame for frame in read_frames(response, 2)
                  if frame[0].startswith("id:")]
    assert frames == []
    with open_stream(world, agent["id"], other_token) as response:
        assert response.status_code == 403  # cannot follow another agent's stream
    with open_stream(world, agent["id"], "bogus") as response:
        assert response.status_code == 401
    hub.close()


def test_stream_replays_live_signals_and_disconnect_reconnects_with_cursor(world):
    hub = install_on(world, policy=fast_policy(max_stream_seconds=5.0))
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = (world.credential(sender),
                                    world.credential(receiver))
    first = send_message(world, sender, receiver, sender_token,
                         text="confidential ping")
    # The post-commit publication is in-process, exactly how the parent hook fires.
    hub.notify_record_post_commit(first)
    with open_stream(world, receiver["id"], receiver_token) as response:
        assert response.status_code == 200
        frames = read_frames(response, 1)
    assert frames == [["id: 1", "event: message",
                       f'data: {{"id": "{first["id"]}", "kind": "message"}}']]
    # Live signal while connected; the canonical write happens before the stream
    # opens (publication order is what matters, not request interleaving).
    second = send_message(world, sender, receiver, sender_token, text="second")
    received = []

    def reader():
        with open_stream(world, receiver["id"], receiver_token, after=1) as response:
            received.extend(read_frames(response, 4))

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    time.sleep(0.3)  # the stream is open server-side and subscribed
    hub.notify_record_post_commit(second)
    thread.join(timeout=15)
    assert not thread.is_alive()
    frames = [frame for frame in received if frame[0].startswith("id:")]
    assert frames[0][0] == "id: 2" and frames[0][1] == "event: message"
    assert frames[0][2].endswith(f'"id": "{second["id"]}", "kind": "message"}}')
    assert "confidential" not in str(frames) and "second" not in frames[0][2]
    # Reconnect via Last-Event-ID replays only what is newer than the cursor.
    with open_stream(world, receiver["id"], receiver_token,
                     headers={"Last-Event-ID": "1"}) as response:
        frames = [frame for frame in read_frames(response, 3)
                  if frame[0].startswith("id:")]
    assert frames[0][0] == "id: 2"
    hub.close()


def test_stream_drops_signal_when_item_is_deleted_before_delivery(world):
    """A signal whose item becomes invisible before the stream reaches it is
    dropped silently: no ID, no content, no per-agent count."""
    hub = install_on(world, policy=fast_policy(max_stream_seconds=3.0))
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = (world.credential(sender),
                                    world.credential(receiver))
    message = send_message(world, sender, receiver, sender_token)
    hub.notify_record_post_commit(message)
    # Delete the message before the subscriber connects: replay must skip it.
    store = world.app.state.store
    current = world.call("GET", f"messages/{message['id']}", token=receiver_token)
    with store.transaction():
        store.delete(message["id"], {"id": uid("principal"), "role": "admin",
                                     "project": world.project, "agent": None},
                     current["revision"])
    with open_stream(world, receiver["id"], receiver_token) as response:
        frames = [frame for frame in read_frames(response, 3)
                  if frame[0].startswith("id:")]
    assert frames == []
    hub.close()


def test_stream_closes_when_authorization_is_revoked_mid_stream(world):
    """Continuous reauthorization: deleting the credential principal ends the
    stream at the next heartbeat instead of leaking further signals."""
    hub = install_on(world, policy=fast_policy(max_stream_seconds=30.0,
                                               heartbeat_seconds=1.0))
    receiver = world.agent("Receiver")
    receiver_token = world.credential(receiver)
    received = []
    finished = threading.Event()

    def reader():
        with open_stream(world, receiver["id"], receiver_token) as response:
            received.extend(read_frames(response, 50, timeout=20))
        finished.set()

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    assert hub.stats()["subscribers"] == 1 or finished.wait(0.1) is False
    deadline = time.time() + 10
    while hub.stats()["subscribers"] != 1 and time.time() < deadline:
        time.sleep(0.02)
    # Revoke the credential: the next heartbeat re-check fails and closes.
    store = world.app.state.store
    with store.transaction():
        store.db.execute("DELETE FROM principals WHERE token_hash=?",
                         (hash_token(receiver_token),))
    assert finished.wait(15)
    frames = [frame for frame in received if frame[0].startswith(("id:", "data:"))]
    assert frames == []  # nothing was emitted before or after revocation
    hub.close()
    thread.join(timeout=5)


def test_subscriber_admission_refuses_before_headers(world):
    hub = install_on(world, policy=fast_policy(max_subscribers_per_agent=1,
                                               max_stream_seconds=5.0))
    agent = world.agent("Agent")
    token = world.credential(agent)
    held = threading.Event()

    def reader():
        with open_stream(world, agent["id"], token) as response:
            assert response.status_code == 200
            held.wait(10)

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while hub.stats()["subscribers"] != 1 and time.time() < deadline:
        time.sleep(0.02)
    # The second subscription is refused with 429 before any 200 headers.
    with open_stream(world, agent["id"], token) as response:
        assert response.status_code == 429
        response.read()
        assert response.json()["error"]["code"] == "push_subscriber_limit"
    held.set()
    thread.join(timeout=10)
    hub.close()


def test_stream_lifetime_is_bounded_and_heartbeats_flow(world):
    hub = install_on(world, policy=fast_policy(max_stream_seconds=1.0,
                                               heartbeat_seconds=1.0))
    agent = world.agent("Agent")
    token = world.credential(agent)
    started = time.monotonic()
    with open_stream(world, agent["id"], token) as response:
        assert response.status_code == 200
        lines = list(response.iter_lines())
    assert time.monotonic() - started < 5  # the stream closed itself promptly
    assert any(line.startswith(":") for line in lines)  # heartbeat comment
    hub.close()


def test_stream_ends_honestly_on_hub_shutdown(world):
    hub = install_on(world, policy=fast_policy(max_stream_seconds=30.0,
                                               heartbeat_seconds=1.0))
    agent = world.agent("Agent")
    token = world.credential(agent)
    received = []

    def reader():
        with open_stream(world, agent["id"], token) as response:
            received.extend(read_frames(response, 1, timeout=15))

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while hub.stats()["subscribers"] != 1 and time.time() < deadline:
        time.sleep(0.02)
    assert hub.stats()["subscribers"] == 1
    hub.close()
    thread.join(timeout=15)
    assert not thread.is_alive()
    assert received == [["event: shutdown", "data: {}"]]


# ------------------------------------------------------------ commit bridge
def make_bridge_world(world, **policy_overrides):
    policy = fast_policy(**policy_overrides)
    hub = MessagePushHub(world.app.state.store.directory / "push.sqlite", policy)
    return hub, policy


def test_commit_bridge_publishes_committed_identity_only(world):
    hub, _ = make_bridge_world(world)
    bridge = bind_commit_source(world.app.state.store, hub, poll_interval=0.2)
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token = world.credential(sender)
    try:
        message = send_message(world, sender, receiver, sender_token)
        deadline = time.time() + 10
        events = []
        while time.time() < deadline:
            events = hub.replay(world.project, receiver["id"])["events"]
            if events:
                break
            time.sleep(0.05)
        assert events == [{"seq": 1, "kind": "message", "item_id": message["id"]}]
        assert "text" not in str(events)  # identity only, never the body
        assert hub.watermark(world.project) is not None
    finally:
        assert bridge.close() is True
        hub.close()


def test_commit_bridge_never_publishes_rolled_back_writes(world):
    hub, _ = make_bridge_world(world)
    store = world.app.state.store
    bridge = bind_commit_source(store, hub, poll_interval=60.0)
    receiver = world.agent("Receiver")
    try:
        rolled_back = uid("msg")
        with pytest.raises(RuntimeError):
            with store.transaction():
                store.save("message", world.project, uid("principal"), {
                    "id": rolled_back, "project_id": world.project,
                    "from": uid("agent"), "to": receiver["id"], "type": "request",
                    "thread_id": uid("thread"),
                    "content": {"media_type": "text/plain", "text": "uncommitted"},
                    "expires_at": time.time() + 60})
                raise RuntimeError("roll it back")
        bridge.tick(world.project)  # explicit scan: the rolled-back row is absent
        assert hub.replay(world.project, receiver["id"])["events"] == []
    finally:
        bridge.close()
        hub.close()


def test_bridge_startup_catchup_repairs_crash_between_commit_and_publish(world):
    """A bridge that never got to scan after a commit loses only the in-memory
    wake: the next bridge over the same durable log catches up from the
    watermark."""
    store = world.app.state.store
    path = store.directory / "push.sqlite"
    hub = MessagePushHub(path, fast_policy())
    first_bridge = CommitSignalBridge(store, hub)  # baseline only, never started
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token = world.credential(sender)
    message = send_message(world, sender, receiver, sender_token)
    # Crash before the wake is processed: the queued wake dies with the bridge.
    first_bridge._queue = type(first_bridge._queue)(maxsize=1024)
    first_bridge.close()
    hub.close()
    # Restart: a fresh bridge over the same durable log republishes the miss.
    reopened = MessagePushHub(path, fast_policy())
    second = bind_commit_source(store, reopened, poll_interval=60.0)
    try:
        deadline = time.time() + 10
        events = []
        while time.time() < deadline:
            events = reopened.replay(world.project, receiver["id"])["events"]
            if events:
                break
            time.sleep(0.05)
        assert events == [{"seq": 1, "kind": "message", "item_id": message["id"]}]
    finally:
        second.close()
        reopened.close()


def test_install_auto_binds_bridge_when_enabled(world):
    hub = install_on(world, policy=fast_policy(), bridge=True)
    assert hub is not None
    bridge = world.app.state.message_push_bridge
    assert isinstance(bridge, CommitSignalBridge)
    assert bridge.close() is True
    hub.close()


# ------------------------------------------------------------- wake follower
def test_push_wake_source_sets_event_reconnects_with_cursor_and_closes(tmp_path):
    attempts = []

    def subscribe(after):
        attempts.append(after)
        if len(attempts) == 1:
            yield {"seq": 1, "kind": "message", "item_id": uid("msg")}
            raise ConnectionError("stream dropped")
        while True:  # an open stream just blocks until cancelled
            if False:
                yield
            return

    cursor_path = tmp_path / "push-cursor.json"
    source = PushWakeSource(subscribe, project_id=P, agent_id=A,
                            cursor_path=cursor_path, retry_seconds=0.05,
                            max_retry_seconds=0.1)
    source.start()
    assert source.event.wait(5)  # reconnect wakes even when the stream is empty
    assert source.cursor == 1
    assert source.close() is True
    # The cursor file is durable, atomic, and identity-bound: a restart resumes.
    resumed = PushWakeSource(subscribe, project_id=P, agent_id=A,
                             cursor_path=cursor_path)
    assert resumed.cursor == 1
    resumed.close()
    with pytest.raises(ValueError, match="another agent"):
        PushWakeSource(subscribe, project_id=P, agent_id=B,
                       cursor_path=cursor_path)
    cursor_path.write_text('{"project_id": 1, "agent_id": "x", "cursor": "a"}')
    with pytest.raises(ValueError):
        PushWakeSource(subscribe, project_id=P, agent_id=A,
                       cursor_path=cursor_path)
    cursor_path.write_text('{"project_id": "%s", "agent_id": "%s", "cursor": true}'
                           % (P, A))
    with pytest.raises(ValueError):  # bool cursor is not an integer
        PushWakeSource(subscribe, project_id=P, agent_id=A,
                       cursor_path=cursor_path)


def test_push_wake_source_close_interrupts_a_blocked_subscription(tmp_path):
    entered = threading.Event()
    cancel_seen = None

    def subscribe(after):
        nonlocal cancel_seen
        entered.set()
        source.cancel.wait(30)  # an in-flight read honoring cancel
        cancel_seen = True
        return iter(())

    source = PushWakeSource(subscribe, retry_seconds=0.05, max_retry_seconds=0.1)
    source.start()
    assert entered.wait(5)
    started = time.monotonic()
    assert source.close(timeout=10) is True
    assert time.monotonic() - started < 10  # never waits out the full read window
    assert cancel_seen is True


def test_push_wake_source_rejects_bad_bounds_and_signals():
    with pytest.raises(ValueError):
        PushWakeSource(None)
    with pytest.raises(ValueError):
        PushWakeSource(lambda after: iter(()), retry_seconds=0)
    with pytest.raises(ValueError):
        PushWakeSource(lambda after: iter(()), cursor_path="x")  # identity required
    seen = []

    def bad_signals(after):
        yield {"seq": 1.5, "kind": "message", "item_id": None}  # float denied
        seen.append("unreachable")

    source = PushWakeSource(bad_signals, retry_seconds=0.05, max_retry_seconds=0.1)
    source.start()
    time.sleep(0.5)
    assert source.cursor == 0 and seen == []  # malformed signal never advances
    assert source.close() is True


def test_httpx_sse_subscription_is_bounded_and_cancellable(world):
    hub = install_on(world, policy=fast_policy(max_stream_seconds=30.0,
                                               heartbeat_seconds=1.0))
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    receiver_token = world.credential(receiver)
    message = send_message(world, sender, receiver, world.credential(sender))
    hub.notify_record_post_commit(message)
    cancel = threading.Event()
    path = (f"/v1/agents/{receiver['id']}/message-stream"
            f"?project_id={world.project}&after=0")
    headers = {"Authorization": "Bearer " + receiver_token}
    received = []
    # TestClient is an httpx.Client: the helper works against the real route.
    for signal in httpx_sse_subscription(world.client, path, cancel=cancel,
                                         headers=headers, read_timeout=5.0):
        received.append(signal)
        cancel.set()  # interrupt mid-stream: bounded, no dangling read
    assert received == [{"seq": 1, "kind": "message", "item_id": message["id"]}]

    def denied():
        for _ in httpx_sse_subscription(world.client, path, cancel=cancel,
                                        headers={"Authorization": "Bearer bogus"}):
            pass

    with pytest.raises(PushStreamError, match="status 401"):
        denied()  # fixed text: the remote error body is never echoed
    hub.close()


def test_parse_sse_bounds_and_malformed_frames():
    good = uid("msg")
    frames = list(parse_sse([
        "id: 7", "event: message", f'data: {{"id": "{good}", "kind": "message"}}',
        "", ": heartbeat", "",
        "event: resync", 'data: {"reason": "backpressure"}', ""]))
    assert frames == [{"seq": 7, "kind": "message", "item_id": good},
                      {"seq": 7, "kind": "resync", "item_id": None}]
    with pytest.raises(PushStreamError) as failure:
        list(parse_sse(["x" * 5000]))
    assert "x" * 100 not in str(failure.value)  # raw remote bytes never echo
    with pytest.raises(PushStreamError):
        list(parse_sse(["id: not-a-number", "event: message", "data: {}", ""]))
    with pytest.raises(PushStreamError):
        list(parse_sse(["id: 1", "event: message",
                        'data: {"id": "not canonical"}', ""]))
    with pytest.raises(PushStreamError):
        list(parse_sse(["id: 1", "event: message", 'data: {"id": "' + good + '"}']
                       * 3, max_frames=1))
