"""Registered activity routes, SDK paths, byte budgets and credential rechecks."""
import json

import httpx
from fastapi.testclient import TestClient

from hivemind.activity_stream import (ActivityStreamHub, ActivityStreamPolicy,
                                      _sse_frames, parse_activity_sse)
from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.sdk import Client
from hivemind.storage import Problem, uid


def test_python_sdk_uses_single_v1_prefix_and_authentication():
    seen = []

    def transport(request):
        seen.append(request)
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
                              content='id: g0\nevent: snapshot\ndata: {"type":"snapshot","agents":[]}\n\n')

    client = Client("http://testserver", "test-only-token", transport=httpx.MockTransport(transport))
    project = uid("project")
    subscription = client.subscribe_agent_activity(project)
    try:
        assert next(subscription.frames())["event"] == "snapshot"
        assert seen[0].url.path == f"/v1/projects/{project}/agents:stream"
        assert seen[0].headers["authorization"] == "Bearer test-only-token"
    finally:
        subscription.close()
        client.close()


def test_registered_default_off_activity_route(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    with TestClient(create_app(tmp_path, workers=False)) as http:
        response = http.get(f"/v1/projects/{config['project_id']}/agents:stream",
                            headers={"Authorization": "Bearer " + config["token"]})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "feature_disabled"


def test_large_snapshot_chunks_fit_wire_and_parser_and_session_revocation(tmp_path):
    config = json.loads(initialize(tmp_path / "canonical").read_text())
    app = create_app(tmp_path / "canonical", workers=False)
    service, store = app.state.service, app.state.store
    p = dict(store.db.execute("SELECT * FROM principals WHERE token_hash IS NOT NULL").fetchone())
    project = config["project_id"]
    policy = ActivityStreamPolicy(enabled=True, snapshot_chunk=16, heartbeat_seconds=1.0)
    hub = ActivityStreamHub(tmp_path / "activity.sqlite", policy)
    for number in range(20):
        service.create(p, "agent", {"project_id": project, "display_name": str(number),
                                    "description": "Context " * 200})
    subscription = hub.subscribe(project, p)
    authorized = True

    def current_auth():
        if not authorized:
            raise Problem("authentication_required", 401)
        return p

    frames = _sse_frames(service, hub, policy, p["id"], project, subscription,
                         reauthorize=current_auth)
    try:
        first = next(frames)
        count = json.loads(first.split("data: ", 1)[1])["count"]
        all_frames = [first, *(next(frames) for _ in range(count - 1))]
        assert all(len(frame.encode()) <= 16384 for frame in all_frames)
        parsed = list(parse_activity_sse("".join(all_frames).splitlines()))
        assert len(parsed) == count
        assert sum(len(frame["data"]["agents"]) for frame in parsed) == 20
        authorized = False
        assert list(frames) == []  # session revocation closes at the next read window
    finally:
        frames.close()
        hub.close()
        app.state.indexes.close()
        store.close()
