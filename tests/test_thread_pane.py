import pytest
from fastapi import Request

from hivemind.storage import uid
from hivemind.thread_pane import install, thread_page


@pytest.fixture
def threads(world):
    """Install the thread read route the way the integration layer will."""
    app = world.app

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    def command(*args, **kwargs):  # the read-only pane installs no mutation route
        raise AssertionError("thread pane must not expose a command route")

    install(app, app.state.service, principal, command, envelope)
    return world


def thread_fixture(world, replies=3):
    board = world.board()["id"]
    root = world.post("Root question about the header")
    posts = [root]
    for index in range(replies):
        posts.append(world.call("POST", f"boards/{board}/posts", {
            "client_post_id": uid("client"), "kind": "answer", "parent_post_id": root["id"],
            "thread_id": root["thread_id"],
            "content": {"media_type": "text/plain", "text": f"Reply {index}"}}))
    return board, root, posts


def test_thread_route_installed_by_default(world):
    board, root, posts = thread_fixture(world, replies=1)
    page = world.call("GET", f"boards/{board}/threads/{root['thread_id']}/posts")
    assert [post["id"] for post in page["items"]] == [post["id"] for post in posts]


def test_thread_reads_full_history_beyond_loaded_timeline_pages(threads):
    board, root, posts = thread_fixture(threads, replies=4)
    for index in range(150):
        threads.post(f"Unrelated board noise {index}")
    page = threads.call("GET", f"boards/{board}/posts?limit=100")
    assert root["id"] not in [item["id"] for item in page["items"]]
    result = threads.call("GET", f"boards/{board}/threads/{root['thread_id']}/posts")
    assert [item["id"] for item in result["items"]] == [item["id"] for item in posts]
    assert result["root_post_id"] == root["id"]
    assert result["count"] == 5
    assert result["has_more"] is False and result["next_before"] is None
    assert result["project_id"] == threads.project and result["board_id"] == board
    assert all(item["visibility"] == "Shared board thread" for item in result["items"])
    assert len(result["snapshot_cursor"]) == 64


def test_thread_pages_walk_newest_page_first_without_gaps_or_duplicates(threads):
    board, root, posts = thread_fixture(threads, replies=5)
    path = f"boards/{board}/threads/{root['thread_id']}/posts"
    page = threads.call("GET", f"{path}?limit=2")
    assert page["has_more"] is True
    assert [item["id"] for item in page["items"]] == [posts[-2]["id"], posts[-1]["id"]]
    collected = page["items"]
    before = page["next_before"]
    while before is not None:
        page = threads.call("GET", f"{path}?limit=2&before={before}")
        collected = page["items"] + collected
        before = page["next_before"]
    assert [item["id"] for item in collected] == [item["id"] for item in posts]
    assert len({item["id"] for item in collected}) == len(posts)
    # A page past the start is empty, not an error.
    beyond = threads.call("GET", f"{path}?before={posts[0]['created_seq']}")
    assert beyond["items"] == [] and beyond["next_before"] is None and beyond["root_post_id"] is None


def test_thread_reply_via_canonical_board_posting_joins_the_same_thread(threads):
    board, root, posts = thread_fixture(threads, replies=1)
    agent = threads.agent()
    token = threads.credential(agent)
    reply = threads.call("POST", f"boards/{board}/posts", {
        "client_post_id": uid("client"), "kind": "answer", "parent_post_id": posts[1]["id"],
        "thread_id": root["thread_id"],
        "content": {"media_type": "application/vnd.hivemind.chat+json", "segments": [
            {"kind": "text", "text": "Agent follow-up"}, {"kind": "agent_mention", "agent_id": agent["id"]}]}},
        token)
    result = threads.call("GET", f"boards/{board}/threads/{root['thread_id']}/posts")
    assert result["count"] == 3
    assert result["items"][-1]["id"] == reply["id"]
    assert result["items"][-1]["parent_post_id"] == posts[1]["id"]
    assert result["items"][-1]["author"] == {"kind": "agent", "agent_id": agent["id"]}
    assert result["items"][0]["id"] == root["id"]


def test_thread_hides_restricted_posts_and_missing_threads(threads):
    board, root, posts = thread_fixture(threads, replies=2)
    path = f"boards/{board}/threads/{root['thread_id']}/posts"
    reader = threads.agent("Reader")
    reader_token = threads.credential(reader)
    outsider = threads.agent("Outsider")
    outsider_token = threads.credential(outsider)
    assert threads.call("GET", path, token=reader_token)["count"] == 3
    assert threads.call("GET", path, token=outsider_token)["count"] == 3
    threads.call("PUT", f"resources/{root['id']}/access", {
        "public_read": False, "reader_agent_ids": [reader["id"]]})
    # The root restriction also hides the replies that inherit it.
    assert threads.call("GET", path, token=reader_token)["count"] == 3
    threads.call("GET", path, token=outsider_token, status=404)
    threads.call("GET", f"boards/{board}/threads/{uid('thread')}/posts", status=404)


def test_thread_read_scope_and_input_limits(threads):
    board, root, posts = thread_fixture(threads, replies=1)
    path = f"boards/{board}/threads/{root['thread_id']}/posts"
    assert threads.client.get(f"/v1/{path}").status_code == 401
    filtered = threads.call("GET", f"{path}?query=Reply%200")
    assert [item["id"] for item in filtered["items"]] == [posts[1]["id"]]
    # A filtered reply whose parent is filtered out is still returned, rooted at the top level.
    assert filtered["root_post_id"] is None
    for suffix in ("limit=0", "limit=201", "before=-1", "query=" + "x" * 257):
        threads.call("GET", f"{path}?{suffix}", status=422)
    # Board visibility is required before any thread content is considered.
    hidden = threads.call("POST", "boards", {"project_id": threads.project, "title": "Hidden board",
                                             "access": {"public_read": False}})
    boardless = threads.agent("Boardless")
    boardless_token = threads.credential(boardless)
    threads.call("GET", f"boards/{hidden['id']}/threads/{root['thread_id']}/posts",
                 token=boardless_token, status=404)
    # A thread id only reads under the board that owns it.
    threads.call("GET", f"boards/{hidden['id']}/threads/{root['thread_id']}/posts", status=404)


def test_thread_page_helper_bounds_and_visibility(threads):
    board, root, posts = thread_fixture(threads, replies=3)
    store = threads.app.state.store
    principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    page = thread_page(store, principal, threads.project, board, root["thread_id"], None, 2)
    assert [item["id"] for item in page["items"]] == [posts[-2]["id"], posts[-1]["id"]]
    assert page["next_before"] == posts[-2]["created_seq"] and page["has_more"] is True
