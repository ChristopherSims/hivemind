"""MCP resource-template surface: authorized reads over the same authenticated API."""
import asyncio
import json
import os

import pytest

from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.storage import uid

from hivemind.mcp import server as mcp_server
from mcp.server.mcpserver.exceptions import ResourceNotFoundError, UnexpectedResourceError


def write_config(path, tmp_path, base_url, token, project_id, agent_id):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"base_url": base_url, "token": token,
                                "project_id": project_id, "agent_id": agent_id,
                                "state_dir": str(tmp_path / "state")}))


def credential_for(app, project_id, agent_id):
    """Issue an agent credential as the owner (same as World.credential)."""
    from hivemind.api import hash_token
    from hivemind.storage import uid as make_id
    import secrets
    with app.state.store.transaction():
        app.state.store.db.execute(
            "INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
            (make_id("principal"), hash_token(secrets.token_urlsafe(32)),
             "agent", project_id, agent_id, 0, 0))


@pytest.fixture
def mcp_world(tmp_path):
    """Real loopback service plus the MCP façade reading over HTTP.

    Uses a real uvicorn thread (not TestClient) because the façade opens its own
    httpx connection per resource read, exactly as it does in production stdio.
    """
    import threading
    import time as time_module

    import httpx
    import uvicorn

    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app = create_app(tmp_path, workers=False)
    # Pick a free loopback port; parallel/ordered tests must not fight over one.
    import socket
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    server_thread = threading.Thread(
        target=uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                             log_level="error")).run, daemon=True)
    server_thread.start()
    for _ in range(200):
        try:
            httpx.get(base + "/.well-known/hivemind", timeout=0.5)
            break
        except Exception:
            time_module.sleep(0.05)
    else:
        pytest.skip("loopback service did not start")
    yield OwnerWorld(app, base, config, tmp_path)
    # A daemon thread; the process fixture teardown ends it.


class OwnerWorld:
    def __init__(self, app, base, config, tmp_path):
        self.app = app
        self.base = base
        self.project = config["project_id"]
        self.token = config["token"]
        self.tmp_path = tmp_path

    def call(self, method, path, body=None, token=None, status=200):
        import httpx
        from hivemind.storage import uid
        headers = {"Authorization": "Bearer " + (token or self.token),
                   "Idempotency-Key": uid("key")}
        response = httpx.request(method, self.base + "/v1/" + path, json=body,
                                 headers=headers, timeout=10)
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())


def test_resource_templates_registered():
    import asyncio
    templates = asyncio.run(mcp_server.list_resource_templates())
    uri_templates = {t.uri_template for t in templates}
    assert len(uri_templates) == 7
    assert "hivemind://ledger/{project_id}" in uri_templates
    assert "hivemind://boards/{board_id}/posts" in uri_templates
    assert "hivemind://boards/{board_id}/threads/{thread_id}/posts" in uri_templates
    assert "hivemind://memories/{project_id}" in uri_templates
    assert "hivemind://memories/{project_id}/{memory_id}" in uri_templates
    assert "hivemind://tasks/{project_id}" in uri_templates
    assert "hivemind://tasks/{project_id}/{task_id}" in uri_templates
    # The tool catalog is unchanged by resource registration.
    tools = asyncio.run(mcp_server.list_tools())
    assert len(tools) >= 10
    assert "hivemind_query_context" in {t.name for t in tools}


def test_resource_reads_return_authorized_content(mcp_world):
    world = mcp_world
    agent = world.call("POST", "agents", {"project_id": world.project,
                                          "display_name": "Probe"})
    board = world.call("GET", f"boards?project_id={world.project}")[0]
    world.call("POST", "runs", {"project_id": world.project, "agent_id": agent["id"]})
    post = world.call("POST", f"boards/{board['id']}/posts", {
        "client_post_id": uid("client"), "kind": "finding",
        "content": {"media_type": "text/plain", "text": "Header owns nav links"}})
    task = world.call("POST", "tasks", {"project_id": world.project,
                                        "title": "Probe task"})
    created = world.call("POST", "memories", {
        "project_id": world.project, "title": "Header fact",
        "content": {"media_type": "text/plain", "text": "Nav links are shared"},
        "evidence_refs": [{"kind": "board_post", "id": post["id"]}],
        "scope_kind": "project", "scope_owner_id": world.project})
    write_config(world.tmp_path / "mcp-private" / "owner.json", world.tmp_path,
                 world.base, world.token, world.project, agent["id"])
    os.environ["HIVEMIND_CONFIG"] = str(world.tmp_path / "mcp-private" / "owner.json")
    try:
        async def reads():
            ledger = json.loads((await mcp_server.read_resource(
                f"hivemind://ledger/{world.project}"))[0].content)
            assert len(ledger["runs"]) == 1
            assert ledger["runs"][0]["agent_id"] == agent["id"]

            board_page = json.loads((await mcp_server.read_resource(
                f"hivemind://boards/{board['id']}/posts"))[0].content)
            assert [item["id"] for item in board_page["items"]] == [post["id"]]

            memories = json.loads((await mcp_server.read_resource(
                f"hivemind://memories/{world.project}"))[0].content)
            assert [m["id"] for m in memories] == [created["id"]]

            one_memory = json.loads((await mcp_server.read_resource(
                f"hivemind://memories/{world.project}/{created['id']}"))[0].content)
            assert one_memory["id"] == created["id"]
            assert one_memory["effective_status"] in ("proposed", "verified")

            tasks = json.loads((await mcp_server.read_resource(
                f"hivemind://tasks/{world.project}"))[0].content)
            assert [t["id"] for t in tasks] == [task["id"]]

            one_task = json.loads((await mcp_server.read_resource(
                f"hivemind://tasks/{world.project}/{task['id']}"))[0].content)
            assert one_task["title"] == "Probe task"

        asyncio.run(reads())
    finally:
        os.environ.pop("HIVEMIND_CONFIG", None)


def test_resource_reads_respect_private_and_project_visibility(mcp_world):
    world = mcp_world
    import secrets as secrets_module
    agent = world.call("POST", "agents", {"project_id": world.project,
                                          "display_name": "Probe"})
    other = world.call("POST", "agents", {"project_id": world.project,
                                          "display_name": "Other"})
    board = world.call("GET", f"boards?project_id={world.project}")[0]
    post = world.call("POST", f"boards/{board['id']}/posts", {
        "client_post_id": uid("client"), "kind": "finding",
        "content": {"media_type": "text/plain", "text": "Public finding"}})

    agent_token = secrets_module.token_urlsafe(32)
    from hivemind.api import hash_token
    from hivemind.storage import uid as make_id
    with world.app.state.store.transaction():
        world.app.state.store.db.execute(
            "INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
            (make_id("principal"), hash_token(agent_token), "agent", world.project,
             agent["id"], 0, 0))
    other_token = secrets_module.token_urlsafe(32)
    with world.app.state.store.transaction():
        world.app.state.store.db.execute(
            "INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
            (make_id("principal"), hash_token(other_token), "agent", world.project,
             other["id"], 0, 0))

    private = world.call("POST", "memories", {
        "project_id": world.project, "title": "Private finding",
        "content": {"media_type": "text/plain", "text": "private only"},
        "evidence_refs": [{"kind": "board_post", "id": post["id"]}],
        "scope_kind": "project", "scope_owner_id": world.project,
        "private": True}, token=agent_token)

    private_dir = world.tmp_path / "mcp-private"
    write_config(private_dir / "agent.json", world.tmp_path, world.base,
                 agent_token, world.project, agent["id"])
    write_config(private_dir / "other.json", world.tmp_path, world.base,
                 other_token, world.project, other["id"])
    os.environ["HIVEMIND_CONFIG"] = str(private_dir / "agent.json")
    try:
        async def reads():
            own = json.loads((await mcp_server.read_resource(
                f"hivemind://memories/{world.project}/{private['id']}"))[0].content)
            assert own["id"] == private["id"]

            # The other agent's credential gets the API's not-found (404), which
            # the MCP library wraps; no content leaks either way.
            os.environ["HIVEMIND_CONFIG"] = str(private_dir / "other.json")
            with pytest.raises((ResourceNotFoundError, UnexpectedResourceError)):
                await mcp_server.read_resource(
                    f"hivemind://memories/{world.project}/{private['id']}")

            # A nonmember project fails closed the same way.
            os.environ["HIVEMIND_CONFIG"] = str(private_dir / "foreign.json")
            (private_dir / "foreign.json").write_text(json.dumps({
                "base_url": world.base, "token": other_token,
                "project_id": "project_other", "agent_id": other["id"],
                "state_dir": str(private_dir)}))
            with pytest.raises((ResourceNotFoundError, UnexpectedResourceError)):
                await mcp_server.read_resource("hivemind://ledger/project_other")

        asyncio.run(reads())
    finally:
        os.environ.pop("HIVEMIND_CONFIG", None)