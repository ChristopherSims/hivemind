"""Real-local-service tests for the authorized per-project activity SSE stream.

No paid calls, no external network: the canonical service runs in-process via
``world``/``TestClient`` for mutations, and stream interleaving is exercised over
a real loopback uvicorn server (TestClient buffers streaming bodies until close,
so it cannot observe live frames). The activity hub keeps its own durable SQLite
log.
"""
import contextlib
import json
import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi import Request

from hivemind.activity_stream import (OPERATION_DESCRIPTORS, ActivityBridge,
                                      ActivityStreamError, ActivityStreamHub,
                                      ActivityStreamPolicy, ActivitySubscription,
                                      descriptors, httpx_activity_subscription,
                                      install, parse_activity_sse)
from hivemind.api import hash_token
from hivemind.storage import Problem, canonical, uid
from test_task_execution import checkpoint, setup, start


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


def fast_policy(**overrides):
    base = {"enabled": True, "max_stream_seconds": 30.0, "heartbeat_seconds": 1.0,
            "presence_interval": 60.0}  # tests drive presence_tick() explicitly
    base.update(overrides)
    return ActivityStreamPolicy(**base)


def install_on(world, **kwargs):
    app = world.app
    kwargs.setdefault("policy", fast_policy())
    kwargs.setdefault("bridge", False)  # bridge behavior is explicit per test
    return install(app, app.state.service, principal_for(app), None,
                   lambda data: {"data": data}, **kwargs)


@contextlib.contextmanager
def live_server(app):
    """Serve the already-initialized app over loopback HTTP so streaming
    responses are truly incremental (TestClient buffers them)."""
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error",
                            lifespan="off")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while not server.started and time.time() < deadline:
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=15)


def stream_reader(base, world, token, until=None, params="", timeout=25.0):
    """Open the SSE stream on a thread and collect frames (line groups).
    Returns (received, thread); received["frames"] grows live."""
    received = {"frames": [], "status": None, "matched": None}

    def run():
        try:
            with httpx.Client(base_url=base,
                              timeout=httpx.Timeout(10.0, read=30.0)) as client:
                with client.stream(
                        "GET",
                        f"/v1/projects/{world.project}/agents:stream{params}",
                        headers={"Authorization": "Bearer " + token}) as response:
                    received["status"] = response.status_code
                    if response.status_code != 200:
                        response.read()
                        return
                    current = []
                    deadline = time.time() + timeout
                    for line in response.iter_lines():
                        if time.time() > deadline:
                            return
                        if line == "":
                            if current:
                                received["frames"].append(current)
                                if until and until(current):
                                    received["matched"] = current
                                    return
                                current = []
                        else:
                            current.append(line)
        except (httpx.HTTPError, ValueError):
            return

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return received, thread


def wait_frame(received, predicate, timeout=15.0):
    """Poll collected frames until one satisfies predicate."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for frame in list(received["frames"]):
            if predicate(frame):
                return frame
        time.sleep(0.02)
    return None


def frame_event(lines):
    return next((line[6:].strip() for line in lines
                 if line.startswith("event:")), None)


def frame_data(lines):
    return json.loads(next((line[5:].strip() for line in lines
                            if line.startswith("data:")), "{}"))


def backdate_run(world, run_id, seconds):
    """Move a run's last_seen_at into the past the way elapsed time would —
    a volatile body rewrite WITHOUT store.tick, exactly like the heartbeat route."""
    store = world.app.state.store
    with store.transaction():
        row = store.raw(run_id)
        body = json.loads(row["body"])
        body["last_seen_at"] = time.time() - seconds
        store.db.execute("UPDATE resources SET body=? WHERE id=?",
                         (canonical(body), run_id))


def project_seq(world):
    return world.app.state.store.project(world.project)["seq"]


# --------------------------------------------------------------------- policy
def test_install_default_disabled_is_safe_and_never_raises(world):
    result = install_on(world, policy=ActivityStreamPolicy())
    assert result is None
    assert world.app.state.activity_stream_hub is None
    assert world.app.state.activity_stream_bridge is None
    response = world.client.get(
        f"/v1/projects/{world.project}/agents:stream",
        headers={"Authorization": "Bearer " + world.owner})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "feature_disabled"


def test_policy_bounds_and_types_are_strict():
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, max_pending_events=0)
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, max_stream_seconds=9999)
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, max_stream_seconds=float("nan"))
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, max_subscribers_per_project=0)
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, replay_limit=0)
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, presence_interval=0.1)
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, snapshot_chunk=0)
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=1)  # bool only, no int coercion
    with pytest.raises(ValueError):
        ActivityStreamPolicy(enabled=True, max_pending_events=True)


def test_descriptors_are_self_describing():
    described = descriptors()
    assert described == OPERATION_DESCRIPTORS
    assert set(described) == {"activity_stream.stream",
                              "activity_stream.commit_bridge",
                              "activity_stream.client_subscription"}
    for descriptor in described.values():
        assert set(descriptor) == {"name", "purpose", "input", "output",
                                   "authority", "side_effects", "limits",
                                   "retry_safety"}


def test_reinstall_replaces_own_routes_only(world):
    install_on(world)
    first = [r for r in world.app.routes
             if getattr(r, "path", "").endswith("agents:stream")]
    install_on(world, policy=fast_policy(max_subscribers_per_project=2))
    second = [r for r in world.app.routes
              if getattr(r, "path", "").endswith("agents:stream")]
    assert len(first) == len(second) == 1  # replaced, not duplicated
    assert any(getattr(r, "path", "") == "/v1/agents" for r in world.app.routes)


# ------------------------------------------------------------- live streaming
def test_stream_snapshot_then_task_progress_completion_and_control_block(world):
    install_on(world, bridge=True)
    agent, token, run, _, task = setup(world)
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, world.owner,
            until=lambda f: frame_event(f) == "activity"
            and (frame_data(f).get("agent") or {}).get("status") == "paused")
        snapshot = wait_frame(received, lambda f: frame_event(f) == "snapshot")
        assert snapshot is not None
        data = frame_data(snapshot)
        assert data["type"] == "snapshot" and data["count"] >= 1
        entry = next(a for a in data["agents"] if a["id"] == agent["id"])
        # The run is live but carries no task yet.
        assert entry["current_task"] is None and entry["status"] == "running"

        active = start(world, task, run, token)
        progress = world.call("POST", f"tasks/{task['id']}:checkpoint",
                              checkpoint(active, run), token)
        frame = wait_frame(received, lambda f: frame_event(f) == "activity"
                           and (frame_data(f).get("agent") or {})
                           .get("current_task"))
        assert frame is not None
        data = frame_data(frame)
        assert data["agent_id"] == agent["id"]
        assert data["agent"]["current_task"]["id"] == task["id"]
        assert data["agent"]["current_task"]["progress"] == 40
        assert data["agent"]["summary"] == "Export implementation in progress"

        progressed = world.call("POST", f"tasks/{task['id']}:checkpoint",
                                checkpoint(progress, run, status="completed",
                                           progress=100), token)
        assert progressed["status"] == "completed"
        # Completion clears the current task; the summary derived from it is gone.
        frame = wait_frame(received, lambda f: frame_event(f) == "activity"
                           and frame_data(f).get("agent") is not None
                           and frame_data(f)["agent"].get("current_task") is None
                           and frame_data(f)["agent"].get("summary") is None)
        assert frame is not None

        # A control-applied pause is a committed canonical change: the roster
        # projection flips the reported status without any polling.
        store = world.app.state.store
        with store.transaction():
            row = store.raw(run["id"])
            store.save("run", world.project, row["owner"],
                       {**json.loads(row["body"]), "status": "paused"})
        frame = wait_frame(received, lambda f: frame_event(f) == "activity"
                           and (frame_data(f).get("agent") or {})
                           .get("status") == "paused")
        assert frame is not None
        thread.join(timeout=15)
        assert received["matched"] is not None
    assert world.app.state.activity_stream_bridge.close(timeout=5) is True


def test_heartbeat_presence_transitions_and_endpoint_responsiveness(world):
    install_on(world, bridge=True)
    bridge = world.app.state.activity_stream_bridge
    agent, token, run, _, task = setup(world)
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, world.owner,
            until=lambda f: frame_event(f) == "activity"
            and (frame_data(f).get("agent") or {}).get("context", {})
            .get("presence") == "offline")
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        # Baseline adoption: the first presence pass records, never publishes.
        bridge.presence_tick()
        # Time passes: the run goes stale purely by expiration (no commit, no
        # all-body polling — the bounded presence timer drives this).
        backdate_run(world, run["id"], 20)
        bridge.presence_tick()
        frame = wait_frame(received, lambda f: frame_event(f) == "activity"
                           and (frame_data(f).get("agent") or {})
                           .get("context", {}).get("presence") == "stale")
        assert frame is not None
        assert frame_data(frame)["change"] == "presence"

        # The real heartbeat route must stay responsive while a long stream is
        # open (detached route, no global mutex held while waiting).
        before = project_seq(world)
        started = time.time()
        reply = world.call("POST", f"runs/{run['id']}/heartbeat",
                           {"capture_health": {"state": "healthy"}}, token)
        elapsed = time.time() - started
        assert elapsed < 5.0 and reply["last_seen_at"]
        assert project_seq(world) == before  # volatile: no canonical tick
        bridge.presence_tick()
        frame = wait_frame(received, lambda f: frame_event(f) == "activity"
                           and (frame_data(f).get("agent") or {})
                           .get("context", {}).get("presence") == "online")
        assert frame is not None

        # Offline expiration follows the same bounded timer path.
        backdate_run(world, run["id"], 45)
        bridge.presence_tick()
        thread.join(timeout=15)
        assert received["matched"] is not None
    assert bridge.close(timeout=5) is True


# ------------------------------------------------------------------- privacy
def test_private_current_task_never_leaks_to_other_agents_stream(world):
    install_on(world, bridge=True)
    bridge = world.app.state.activity_stream_bridge
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint",
               checkpoint(active, run, summary="PRIVATE"), token)
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (agent["id"], task["id"]))
        body = store.get(run["id"], {"role": "admin"})
        body["summary"] = {"text": "PRIVATE", "origin": "agent_reported"}
        store.db.execute("UPDATE resources SET body=? WHERE id=?",
                         (json.dumps(body), run["id"]))
    reader_token = world.credential(world.agent("Reader"))
    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, reader_token, timeout=12)
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        assert "PRIVATE" not in json.dumps(frame_data(received["frames"][0]))
        assert task["id"] not in json.dumps(frame_data(received["frames"][0]))
        # Force fresh activity frames for that agent: presence churn replays the
        # projection through the authorized roster each time.
        backdate_run(world, run["id"], 20)
        bridge.presence_tick()
        backdate_run(world, run["id"], 0)
        bridge.presence_tick()
        deadline = time.time() + 8
        while time.time() < deadline and len(received["frames"]) < 3:
            time.sleep(0.05)
        payload = json.dumps([frame_data(f) for f in received["frames"]
                              if frame_event(f) in ("snapshot", "activity")])
        assert "PRIVATE" not in payload
        assert task["id"] not in payload
    bridge.close()


def test_source_revocation_hides_derived_summary_in_stream(world):
    install_on(world, bridge=True)
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    proof = world.post("Checkpoint evidence")
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, world.owner,
            until=lambda f: frame_event(f) == "activity"
            and (frame_data(f).get("agent") or {}).get("summary") is None
            and (frame_data(f).get("agent") or {}).get("current_task"))
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        payload = checkpoint(active, run, evidence_refs=[{"kind": "board_post",
                                                        "id": proof["id"]}])
        world.call("POST", f"tasks/{task['id']}:checkpoint", payload, token)
        frame = wait_frame(received, lambda f: frame_event(f) == "activity"
                           and (frame_data(f).get("agent") or {}).get("summary"))
        assert frame is not None
        # Revoke the evidence source: the projection recomputes and the summary
        # derived from hidden evidence is withdrawn, never re-emitted.
        world.call("DELETE", f"posts/{proof['id']}",
                   {"expected_revision": proof["revision"]})
        thread.join(timeout=15)
        assert received["matched"] is not None
    world.app.state.activity_stream_bridge.close()


def test_principal_revocation_closes_stream_silently(world):
    install_on(world)
    agent = world.agent("Revoked")
    token = world.credential(agent)
    store = world.app.state.store
    principal_id = store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?",
        (hash_token(token),)).fetchone()["id"]
    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, token, timeout=20)
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        with store.transaction():
            store.db.execute("DELETE FROM principals WHERE id=?", (principal_id,))
        # The next heartbeat re-verifies authorization and closes the stream.
        thread.join(timeout=15)
        assert not thread.is_alive()
        assert received["status"] == 200  # closed honestly, no error frame


def test_foreign_project_principal_gets_404(world):
    install_on(world)
    store = world.app.state.store
    other_project = uid("project")
    with store.transaction():
        store.db.execute("INSERT INTO projects(id, title) VALUES(?,?)",
                         (other_project, "Other"))
    outsider = world.credential(role="agent", project=other_project)
    response = world.client.get(
        f"/v1/projects/{world.project}/agents:stream",
        headers={"Authorization": "Bearer " + outsider})
    assert response.status_code == 404


# ----------------------------------------------------- admission and cursors
def test_admission_limit_is_enforced_before_headers(world):
    hub = install_on(world, policy=fast_policy(max_subscribers_per_project=1))
    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, world.owner, timeout=20)
        deadline = time.time() + 10
        while hub.stats()["subscribers"] != 1 and time.time() < deadline:
            time.sleep(0.02)
        assert hub.stats()["subscribers"] == 1
        holder = world.credential(role="human")
        with httpx.Client(base_url=base) as client:
            response = client.get(
                f"/v1/projects/{world.project}/agents:stream",
                headers={"Authorization": "Bearer " + holder})
        assert response.status_code == 429  # refused before any SSE headers
        assert response.json()["error"]["code"] == "activity_subscriber_limit"
        hub.close()
        thread.join(timeout=10)


def test_cursor_class_mismatch_and_malformed_cursor_rejected(world):
    install_on(world)
    owner = {"Authorization": "Bearer " + world.owner}
    bad = world.client.get(f"/v1/projects/{world.project}/agents:stream",
                           params={"after": "notacursor"}, headers=owner)
    assert bad.status_code == 422
    agent_token = world.credential(world.agent("Cursor agent"))
    # An admin-class cursor is refused for an agent-class viewer (and vice versa).
    bad = world.client.get(f"/v1/projects/{world.project}/agents:stream",
                           params={"after": "g5"},
                           headers={"Authorization": "Bearer " + agent_token})
    assert bad.status_code == 422
    bad = world.client.get(f"/v1/projects/{world.project}/agents:stream",
                           params={"after": "s5"}, headers=owner)
    assert bad.status_code == 422


def test_replay_cursor_privacy_and_retention_resync(tmp_path):
    hub = ActivityStreamHub(tmp_path / "activity.sqlite",
                            ActivityStreamPolicy(enabled=True, replay_limit=3))
    project = uid("project")
    agent_a, agent_b = uid("agent"), uid("agent")
    admin = {"id": uid("principal"), "role": "admin", "agent": None}
    member = {"id": uid("principal"), "role": "agent", "agent": agent_a}
    other = {"id": uid("principal"), "role": "agent", "agent": agent_b}
    hub.publish(project, "shared", agent_a, "status", source_seq=1)
    hub.publish(project, f"agent:{agent_a}", agent_a, "status", source_seq=2)
    hub.publish(project, f"agent:{agent_b}", agent_b, "status", source_seq=3)
    # The agent's own replay sees shared + own scope only; foreign private
    # traffic is neither delivered nor counted in its cursor.
    replay = hub.replay(project, member, after="s0.a0")
    assert {event["scope"] for event in replay["events"]} == {
        "shared", f"agent:{agent_a}"}
    assert not replay["resync"]
    replay = hub.replay(project, other, after="s0.a0")
    # agent_b sees shared + its OWN scope, never agent_a's private scope.
    assert {event["scope"] for event in replay["events"]} == {
        "shared", f"agent:{agent_b}"}
    replay = hub.replay(project, admin, after="g0")
    assert len(replay["events"]) == 3  # admin is authorized for every scope
    # Retention overflow is an explicit resync, never a silent gap.
    for seq in range(4, 12):
        hub.publish(project, "shared", agent_a, "task", source_seq=seq)
    assert hub.replay(project, other, after="s0")["resync"] is True
    assert hub.replay(project, admin, after="g0")["resync"] is True
    assert hub.replay(project, other, after="s6")["resync"] is False
    hub.close()
    # Durable across reopen: the log survives a hub restart.
    reopened = ActivityStreamHub(tmp_path / "activity.sqlite",
                                 ActivityStreamPolicy(enabled=True, replay_limit=3))
    assert reopened.replay(project, other, after="s0")["resync"] is True
    assert reopened.replay(project, admin, after="g1")["events"]
    reopened.close()


# --------------------------------------------------------- bridge durability
def test_rollback_publishes_nothing_and_restart_catches_up(world, tmp_path):
    install_on(world, bridge=True, path=tmp_path / "activity.sqlite")
    bridge = world.app.state.activity_stream_bridge
    hub = world.app.state.activity_stream_hub
    store = world.app.state.store
    admin = {"id": uid("principal"), "role": "admin", "agent": None}
    assert hub.watermark(world.project) is not None
    agent, token, run, _, task = setup(world)
    bridge.tick(world.project)
    watermark = hub.watermark(world.project)
    log_edge = max((e["log_seq"] for e in
                    hub.replay(world.project, admin, after="g0")["events"]),
                   default=0)
    # A rolled-back transaction is not committed: no wake, no watermark drift,
    # no event — subscribers never see phantom activity.
    try:
        with store.transaction():
            row = store.raw(run["id"])
            store.save("run", world.project, row["owner"],
                       {**json.loads(row["body"]), "status": "paused"})
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    time.sleep(0.5)  # allow any (impossible) phantom publication to land
    assert hub.watermark(world.project) == watermark
    events = hub.replay(world.project, admin, after="g0")["events"]
    assert max((e["log_seq"] for e in events), default=0) == log_edge
    assert store.get(run["id"], {"role": "admin"})["status"] == "running"

    # Crash the bridge between commit and publish: stop it, commit, restart a
    # fresh bridge on the same durable log; startup catch-up republishes.
    assert bridge.close(timeout=5) is True
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint", checkpoint(active, run),
               token)
    restarted = ActivityBridge(store, hub, presence_interval=60.0).start()
    try:
        deadline = time.time() + 20
        events = []
        while time.time() < deadline:
            events = hub.replay(world.project, admin, after="g0")["events"]
            if any(e["change"] == "task" and e["agent_id"] == agent["id"]
                   for e in events):
                break
            time.sleep(0.05)
        assert any(e["change"] == "task" and e["agent_id"] == agent["id"]
                   for e in events)
        assert hub.watermark(world.project) >= project_seq(world)
    finally:
        assert restarted.close(timeout=5) is True


def test_overflow_backpressure_is_definite_and_rebaselines(world):
    install_on(world, policy=fast_policy(max_pending_events=1))
    hub = world.app.state.activity_stream_hub
    agent = world.agent("Overflowed")
    # A durable backlog larger than the single-slot pending queue overflows at
    # subscribe time: the stream must emit an explicit resync advisory followed
    # by a fresh snapshot baseline, never a silent gap.
    for seq in range(1, 6):
        hub.publish(world.project, "shared", agent["id"], "status",
                    source_seq=seq)
    state = {"resync": None}

    def until(frame):
        if frame_event(frame) == "resync":
            state["resync"] = frame
        # The stream is only healthy once a fresh snapshot follows the advisory.
        return state["resync"] is not None and frame_event(frame) == "snapshot"

    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, world.owner, until=until,
                                         params="?after=g0", timeout=20)
        thread.join(timeout=25)
        assert state["resync"] is not None
        assert frame_data(state["resync"])["reason"] == "backpressure"
        assert received["matched"] is not None  # re-baselined, no silent gap


def test_hub_close_terminates_long_stream_and_bridge_children(world):
    install_on(world, bridge=True)
    hub = world.app.state.activity_stream_hub
    bridge = world.app.state.activity_stream_bridge
    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, world.owner, timeout=20)
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        hub.close()
        thread.join(timeout=15)
        assert not thread.is_alive()  # the shutdown advisory ends the stream
        shutdown = wait_frame(received, lambda f: frame_event(f) == "shutdown",
                              timeout=1)
        assert shutdown is not None
    assert bridge.close(timeout=5) is True
    assert not bridge._thread.is_alive()  # no leaked child after close
    # stats() stays safe after close and never reveals per-scope volume.
    stats = hub.stats()
    assert stats["closed"] is True and stats["subscribers"] == 0


def test_privatization_withdraws_public_cache_without_leaking_private(world):
    """A canonical public->private transition (no new shared-scope content)
    promptly withdraws the cached PUBLIC summary from public streams via a
    generic withdrawal invalidation — no private ids, notes, or counts ever
    enter shared cursors."""
    install_on(world, bridge=True)
    hub = world.app.state.activity_stream_hub
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint",
               checkpoint(active, run, summary="PUBLIC-SUMMARY"), token)
    bridge = world.app.state.activity_stream_bridge
    bridge.tick(world.project)  # the task's shared scope is durably recorded
    reader = {"id": uid("principal"), "role": "human", "agent": None}
    shared_before = hub.replay(world.project, reader, after="s0")["events"]
    reader_token = world.credential(role="human")
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, reader_token,
            until=lambda f: frame_event(f) == "activity"
            and frame_data(f).get("agent_id") == agent["id"]
            and (frame_data(f).get("agent") or {}).get("current_task") is None
            and (frame_data(f).get("agent") or {}).get("summary") is None
            and frame_data(f).get("removed") is False)
        snapshot = wait_frame(received, lambda f: frame_event(f) == "snapshot")
        assert snapshot is not None
        assert "PUBLIC-SUMMARY" in json.dumps(frame_data(snapshot))

        # Privatize: the canonical store keeps private_agent fixed on conflict
        # updates, so the scope change is a column update plus a real ticked
        # commit (a new seq the bridge must attribute to the agent scope only).
        store = world.app.state.store
        with store.transaction():
            store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                             (agent["id"], task["id"]))
            row = store.raw(task["id"])
            store.save("task", world.project, row["owner"],
                       json.loads(row["body"]))
        thread.join(timeout=15)
        assert received["matched"] is not None  # withdrawal reached the public stream
        after_frames = received["frames"][received["frames"].index(snapshot) + 1:]
        for frame in after_frames:
            payload = json.dumps(frame_data(frame))
            assert "PUBLIC-SUMMARY" not in payload and task["id"] not in payload
    # The private event itself never touched the shared cursor: exactly one
    # generic withdrawal (agent_id None, no ids) was added to shared scope.
    shared_after = hub.replay(world.project, reader, after="s0")["events"]
    new_shared = shared_after[len(shared_before):]
    assert [e["change"] for e in new_shared].count("withdrawal") >= 1
    assert all(e["agent_id"] is None for e in new_shared
               if e["change"] == "withdrawal")
    assert not any(e["scope"] != "shared" for e in new_shared)
    # The content-bearing private event exists only in the agent's own scope.
    owner_viewer = {"id": uid("principal"), "role": "agent", "agent": agent["id"]}
    own = hub.replay(world.project, owner_viewer, after="s0.a0")["events"]
    assert any(e["scope"] == f"agent:{agent['id']}" and e["change"] == "task"
               for e in own)
    bridge.close()


def test_failed_scan_retries_via_periodic_catchup_without_new_commit(world):
    """One failing scan with NO second commit must still catch up: the bounded
    periodic durable-watermark catch-up retries, and the live stream receives
    the frame."""
    install_on(world)  # bridge wired manually for a fast poll interval
    hub = world.app.state.activity_stream_hub
    store = world.app.state.store
    bridge = ActivityBridge(store, hub, poll_interval=0.2,
                            presence_interval=60.0).start()
    agent, token, run, _, task = setup(world)
    original = hub.publish_batch
    state = {"failed": False}

    def fail_once(project_id, items, *, watermark):
        if not state["failed"]:
            state["failed"] = True
            raise RuntimeError("injected scan failure")
        return original(project_id, items, watermark=watermark)

    hub.publish_batch = fail_once
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, world.owner,
            until=lambda f: frame_event(f) == "activity"
            and (frame_data(f).get("agent") or {}).get("current_task"))
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        active = start(world, task, run, token)
        world.call("POST", f"tasks/{task['id']}:checkpoint",
                   checkpoint(active, run), token)
        thread.join(timeout=20)
        assert state["failed"] is True
        assert received["matched"] is not None  # caught up with no second commit
        assert hub.watermark(world.project) >= project_seq(world)
    assert bridge.close(timeout=5) is True


def test_foreign_project_runs_are_never_read_or_tracked(world):
    """2000 foreign-project runs must not force a global body parse: presence
    maintenance touches only interested projects' metadata projection."""
    install_on(world, bridge=True)
    bridge = world.app.state.activity_stream_bridge
    store = world.app.state.store
    foreign = uid("project")
    junk = "x" * 2048
    with store.transaction():
        store.db.execute("INSERT INTO projects(id, title) VALUES(?,?)",
                         (foreign, "Foreign"))
        for _ in range(2000):
            store.db.execute(
                "INSERT INTO resources(id,kind,project,owner,private_agent,"
                "revision,seq,body) VALUES(?,?,?,?,NULL,1,0,?)",
                (uid("run"), "run", foreign, "bulk",
                 canonical({"id": "x", "agent_id": uid("agent"),
                            "status": "running", "last_seen_at": 1,
                            "payload": junk})))
    agent = world.agent("Local")
    token = world.credential(agent)
    run = world.run(agent, token)
    bridge.tick(world.project)
    bridge.presence_tick()
    bridge.presence_tick()  # complete the cycle for the local project
    # Only the local project's runs are tracked; the foreign bulk is untouched.
    assert set(bridge._presence) == {run["id"]}
    backdate_run(world, run["id"], 20)
    bridge.presence_tick()
    assert bridge._presence[run["id"]][3] == "stale"
    admin = {"id": uid("principal"), "role": "admin", "agent": None}
    events = world.app.state.activity_stream_hub.replay(
        world.project, admin, after="g0")["events"]
    assert any(e["change"] == "presence" for e in events)
    assert bridge.close(timeout=5) is True


def test_presence_pagination_is_bounded_and_fair(world):
    install_on(world, bridge=True,
               policy=fast_policy(presence_batch=2, presence_interval=60.0))
    bridge = world.app.state.activity_stream_bridge
    hub = world.app.state.activity_stream_hub
    agent = world.agent("Paged")
    token = world.credential(agent)
    runs = [world.run(agent, token) for _ in range(5)]
    store = world.app.state.store
    with store.transaction():
        for run in runs:
            row = store.raw(run["id"])
            body = json.loads(row["body"])
            body["last_seen_at"] = time.time() - 20  # stale everywhere
            store.db.execute("UPDATE resources SET body=? WHERE id=?",
                             (canonical(body), run["id"]))
    bridge.presence_tick()
    assert len(bridge._presence) == 2  # strict per-tick page bound
    bridge.presence_tick()
    assert len(bridge._presence) == 4
    bridge.presence_tick()
    assert len(bridge._presence) == 5  # round-robin reached the final page
    admin = {"id": uid("principal"), "role": "admin", "agent": None}
    presence_events = [e for e in hub.replay(world.project, admin, after="g0")
                       ["events"] if e["change"] == "presence"]
    assert len(presence_events) == 5  # every run's stale transition published
    assert bridge.close(timeout=5) is True


def test_close_reports_real_liveness_and_retries(world):
    install_on(world)
    hub = world.app.state.activity_stream_hub
    store = world.app.state.store
    bridge = ActivityBridge(store, hub, poll_interval=0.2,
                            presence_interval=60.0).start()
    entered = threading.Event()
    release = threading.Event()

    def stuck(project):
        entered.set()
        release.wait(10)
        return 0

    bridge._scan = stuck
    assert entered.wait(10)  # the periodic catch-up walks into the blocked scan
    assert bridge.close(timeout=0.2) is False  # honestly incomplete
    assert bridge._thread.is_alive()
    assert bridge.close(timeout=0.2) is False  # repeated close stays honest
    release.set()
    assert bridge.close(timeout=5) is True
    assert not bridge._thread.is_alive()
    assert bridge.close(timeout=5) is True  # idempotent once really dead


def privatize(world, resource_id, agent_id):
    """Scope change as the canonical store represents it: a column update plus
    a real ticked commit (conflict updates never rewrite private_agent)."""
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (agent_id, resource_id))
        row = store.raw(resource_id)
        store.save("task", world.project, row["owner"], json.loads(row["body"]))


def test_preexisting_public_task_privatized_before_first_scan_withdraws(world):
    """Regression: a task/run that existed BEFORE install, baselined publicly,
    then privatized before the bridge ever scanned it must still produce a
    prompt generic shared withdrawal — the baseline's CAS scope adoption (same
    canonical snapshot as the emitted roster) supplies the prior scope."""
    install_on(world, bridge=False)  # hub only; scanner paused (never started)
    hub = world.app.state.activity_stream_hub
    store = world.app.state.store
    agent, token, run, _, task = setup(world)
    active = start(world, task, run, token)
    world.call("POST", f"tasks/{task['id']}:checkpoint",
               checkpoint(active, run, summary="PRE-EXISTING"), token)
    # Scanner exists (subscribed, watermark baselined AFTER the setup commits)
    # but its thread is not running: it cannot adopt anything yet.
    bridge = ActivityBridge(store, hub, presence_interval=60.0)
    assert hub.watermark(world.project) >= project_seq(world)
    reader_token = world.credential(role="human")
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, reader_token,
            until=lambda f: frame_event(f) == "activity"
            and frame_data(f).get("agent_id") == agent["id"]
            and (frame_data(f).get("agent") or {}).get("summary") is None
            and frame_data(f).get("removed") is False)
        snapshot = wait_frame(received, lambda f: frame_event(f) == "snapshot")
        assert snapshot is not None
        assert "PRE-EXISTING" in json.dumps(frame_data(snapshot))
        # The scanner has not run: only baseline adoption could know the task
        # was public. Privatize, then release the scanner.
        privatize(world, task["id"], agent["id"])
        bridge.start()
        thread.join(timeout=15)
        assert received["matched"] is not None  # withdrawal recomputed the cache
        for frame in received["frames"][1:]:
            payload = json.dumps(frame_data(frame))
            assert "PRE-EXISTING" not in payload and task["id"] not in payload
    # The shared scope gained a generic withdrawal, never the private event.
    reader = {"id": uid("principal"), "role": "human", "agent": None}
    shared = hub.replay(world.project, reader, after="s0")["events"]
    assert any(e["change"] == "withdrawal" and e["agent_id"] is None
               for e in shared)
    assert not any(e["change"] == "task" for e in shared)
    assert bridge.close(timeout=5) is True


def test_private_owner_change_withdraws_old_agent_scope_only(world):
    """private A -> private B: A gets a generic withdrawal in ITS scope (never
    shared), so A drops the cached summary while B's traffic stays invisible
    to shared cursors."""
    install_on(world, bridge=True)
    hub = world.app.state.activity_stream_hub
    bridge = world.app.state.activity_stream_bridge
    agent_a = world.agent("Owner A")
    token_a = world.credential(agent_a)
    agent_b = world.agent("Owner B")
    run = world.run(agent_a, token_a)
    scope = world.scope("tasks/private")
    task = world.call("POST", "tasks", {
        "project_id": world.project, "title": "Private task",
        "scope_ids": [scope["id"]]})
    world.claim(agent_a, run, scope, token_a)
    active = start(world, task, run, token_a)
    world.call("POST", f"tasks/{task['id']}:checkpoint",
               checkpoint(active, run, summary="A-ONLY"), token_a)
    privatize(world, task["id"], agent_a["id"])
    bridge.tick(world.project)  # scanner records scope agent:A
    with live_server(world.app) as base:
        received, thread = stream_reader(
            base, world, token_a,
            until=lambda f: frame_event(f) == "activity"
            and frame_data(f).get("agent_id") == agent_a["id"]
            and (frame_data(f).get("agent") or {}).get("summary") is None
            and frame_data(f).get("removed") is False)
        snapshot = wait_frame(received, lambda f: frame_event(f) == "snapshot")
        assert snapshot is not None
        assert "A-ONLY" in json.dumps(frame_data(snapshot))  # A is authorized
        member = {"id": uid("principal"), "role": "human", "agent": None}
        shared_before = len(hub.replay(world.project, member,
                                       after="s0")["events"])
        privatize(world, task["id"], agent_b["id"])  # ownership moves A -> B
        thread.join(timeout=15)
        assert received["matched"] is not None  # A's cache withdrawn promptly
        for frame in received["frames"][1:]:
            assert "A-ONLY" not in json.dumps(frame_data(frame))
    # The withdrawal lived in A's scope; the shared cursor did not move, and
    # admin-visible withdrawals carry no ids.
    viewer_a = {"id": uid("principal"), "role": "agent", "agent": agent_a["id"]}
    own = hub.replay(world.project, viewer_a, after="s0.a0")["events"]
    withdrawals = [e for e in own if e["change"] == "withdrawal"]
    assert withdrawals and all(e["agent_id"] is None for e in withdrawals)
    assert any(e["scope"] == f"agent:{agent_a['id']}" for e in withdrawals)
    shared_after = len(hub.replay(world.project, member, after="s0")["events"])
    assert shared_after == shared_before  # B's private traffic never surfaced
    bridge.close()


def test_credential_rotation_and_downgrade_close_stream(world):
    """The principal fingerprint (role/agent/project/token_hash) is re-verified
    before every heartbeat: rotating the credential or downgrading the role
    closes the stream immediately, and a stale admin-class cursor is refused."""
    install_on(world)
    store = world.app.state.store
    agent = world.agent("Rotating")
    token = world.credential(agent)
    principal_id = store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?",
        (hash_token(token),)).fetchone()["id"]
    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, token, timeout=20)
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        with store.transaction():  # credential rotation: token_hash changes
            store.db.execute("UPDATE principals SET token_hash=? WHERE id=?",
                             (hash_token(uid("token")), principal_id))
        thread.join(timeout=15)
        assert not thread.is_alive()  # closed at the next heartbeat
    # Role/agent downgrade mid-stream also closes immediately.
    with live_server(world.app) as base:
        received, thread = stream_reader(base, world, world.owner, timeout=20)
        assert wait_frame(received, lambda f: frame_event(f) == "snapshot")
        with store.transaction():
            store.db.execute(
                "UPDATE principals SET role='agent', agent=? WHERE token_hash=?",
                (agent["id"], hash_token(world.owner)))
        thread.join(timeout=15)
        assert not thread.is_alive()
    # After downgrade the admin-class cursor is unusable (no g-cursor private
    # counting as a lesser principal), and the agent class gets scoped cursors.
    response = world.client.get(
        f"/v1/projects/{world.project}/agents:stream", params={"after": "g3"},
        headers={"Authorization": "Bearer " + world.owner})
    assert response.status_code == 422


def test_watermark_is_monotonic_across_publishers(tmp_path):
    hub = ActivityStreamHub(tmp_path / "a.sqlite",
                            ActivityStreamPolicy(enabled=True))
    project, agent = uid("project"), uid("agent")
    hub.publish_batch(project, [(None, "shared", agent, "status", 10)],
                      watermark=10)
    assert hub.watermark(project) == 10
    # A lagging bridge's older receipt can never regress the durable edge.
    hub.publish_batch(project, [(None, "shared", agent, "status", 5)],
                      watermark=5)
    assert hub.watermark(project) == 10
    hub.close()


def test_adopt_sources_cas_orders_and_cap(tmp_path):
    """Both race orders are safe: adopt-then-scan and scan-then-adopt both end
    with exactly one withdrawal; adoption never overwrites scanner history."""
    hub = ActivityStreamHub(tmp_path / "a.sqlite",
                            ActivityStreamPolicy(enabled=True))
    project, task, agent = uid("project"), uid("task"), uid("agent")
    # adopt-before-scan: scanner observes shared -> agent and withdraws.
    hub.adopt_sources(project, [(task, "shared")])
    hub.publish_batch(project, [(task, f"agent:{agent}", agent, "task", 7)],
                      watermark=7)
    member = {"id": uid("principal"), "role": "human", "agent": None}
    events = hub.replay(project, member, after="s0")["events"]
    assert [e["change"] for e in events] == ["withdrawal"]
    # scan-before-adopt (stale baseline racing a privatization): adoption
    # detects the mismatch and emits the withdrawal the scanner skipped.
    other_task = uid("task")
    hub.publish_batch(project, [(other_task, f"agent:{agent}", agent, "task", 8)],
                      watermark=8)
    hub.adopt_sources(project, [(other_task, "shared")])  # stale baseline view
    events = hub.replay(project, member, after="s0")["events"]
    assert [e["change"] for e in events] == ["withdrawal", "withdrawal"]
    # Re-adopting the now-current view is a no-op (no withdrawal flood).
    hub.adopt_sources(project, [(other_task, f"agent:{agent}")])
    events = hub.replay(project, member, after="s0")["events"]
    assert [e["change"] for e in events] == ["withdrawal", "withdrawal"]
    # Fail closed beyond the cap.
    with pytest.raises(Problem) as failure:
        hub.adopt_sources(project, [(uid("task"), "shared")] * 16385)
    assert failure.value.code == "baseline_sources_exceeded"
    hub.close()


# ------------------------------------------------------------ client helper
def test_parse_activity_sse_bounds_and_fixed_errors():
    lines = ["id: s1", "event: snapshot",
             'data: {"type":"snapshot","index":0,"count":1,"agents":[]}', "",
             ": heartbeat", "", "id: s2.a1", "event: activity",
             'data: {"type":"activity","agent_id":"a","change":"task",'
             '"removed":false,"agent":{}}', ""]
    frames = list(parse_activity_sse(iter(lines)))
    assert [f["event"] for f in frames] == ["snapshot", "activity"]
    assert frames[1]["id"] == "s2.a1"
    with pytest.raises(ActivityStreamError):
        list(parse_activity_sse(iter(["id: 5", "event: activity",
                                      'data: {"type":"activity"}', ""])))
    with pytest.raises(ActivityStreamError):
        list(parse_activity_sse(iter(["event: activity", 'data: {"type":"x"}',
                                      ""])))
    with pytest.raises(ActivityStreamError):
        list(parse_activity_sse(iter(["event: activity",
                                      'data: ' + "x" * 90000, ""])))
    cancel = threading.Event()
    cancel.set()
    assert list(parse_activity_sse(iter(lines), cancel=cancel)) == []


def test_client_subscription_frames_cursor_and_close(world, tmp_path):
    install_on(world, bridge=True)
    agent, token, run, _, task = setup(world)
    cursor_path = tmp_path / "cursor.json"
    with live_server(world.app) as base:
        with httpx.Client(base_url=base,
                          timeout=httpx.Timeout(10.0, read=30.0)) as client:
            subscription = ActivitySubscription(
                client, project_id=world.project,
                headers={"Authorization": "Bearer " + world.owner},
                cursor_path=cursor_path)
            received = []

            def consume():
                for frame in subscription.frames():
                    received.append(frame)
                    if frame["event"] == "activity":
                        return

            consumer = threading.Thread(target=consume, daemon=True)
            consumer.start()
            deadline = time.time() + 10
            while not received and time.time() < deadline:
                time.sleep(0.05)
            assert received and received[0]["event"] == "snapshot"
            active = start(world, task, run, token)
            world.call("POST", f"tasks/{task['id']}:checkpoint",
                       checkpoint(active, run), token)
            consumer.join(timeout=15)
            assert any(f["event"] == "activity" for f in received)
            assert subscription.close(timeout=5) is True
    # The durable cursor file is identity-bound and strictly shaped.
    state = json.loads(cursor_path.read_text())
    assert state["project_id"] == world.project
    assert state["cursor"].startswith("g")
    cursor_path.write_text(json.dumps({"project_id": uid("project"),
                                       "cursor": "g1"}))
    with pytest.raises(ValueError):
        ActivitySubscription(world.client, project_id=world.project,
                             cursor_path=cursor_path)
    cursor_path.write_text("not json")
    with pytest.raises(ValueError):
        ActivitySubscription(world.client, project_id=world.project,
                             cursor_path=cursor_path)


def test_httpx_subscription_helper_against_real_route(world):
    install_on(world)
    cancel = threading.Event()
    frames = []

    with live_server(world.app) as base:
        with httpx.Client(base_url=base,
                          timeout=httpx.Timeout(10.0, read=30.0)) as client:
            def read():
                path = f"/v1/projects/{world.project}/agents:stream"
                for frame in httpx_activity_subscription(
                        client, path, cancel=cancel,
                        headers={"Authorization": "Bearer " + world.owner}):
                    frames.append(frame)
                    if frames:
                        cancel.set()

            thread = threading.Thread(target=read, daemon=True)
            thread.start()
            thread.join(timeout=15)
            assert frames and frames[0]["event"] == "snapshot"
            cancel.set()
            with pytest.raises(ActivityStreamError) as failure:
                list(httpx_activity_subscription(
                    client, f"/v1/projects/{uid('project')}/agents:stream",
                    cancel=threading.Event(),
                    headers={"Authorization": "Bearer " + world.owner}))
            assert "404" in str(failure.value)
