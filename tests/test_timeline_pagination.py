from hivemind import models as m
from hivemind.storage import uid


def seed_posts(world, count):
    service, store = world.app.state.service, world.app.state.store
    board = world.board()["id"]
    principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    posts = []
    with store.transaction():
        for index in range(count):
            posts.append(service.board_post(principal, board, m.PostCreate(
                client_post_id=uid("post"), content=m.Content(media_type="text/plain", text=f"Archive item {index}"))))
    return board, posts


def test_board_pagination_and_search_reach_beyond_old_recent_limit(world):
    board, posts = seed_posts(world, 1005)
    newest = world.call("GET", f"boards/{board}/posts?limit=3")
    assert [item["id"] for item in newest["items"]] == [item["id"] for item in posts[-3:]]
    oldest = world.call("GET", f"boards/{board}/posts?before={posts[4]['created_seq']}&limit=4")
    assert [item["id"] for item in oldest["items"]] == [item["id"] for item in posts[:4]]
    assert oldest["next_before"] is None
    found = world.call("GET", f"boards/{board}/posts?query=Archive%20item%200")
    assert [item["id"] for item in found["items"]] == [posts[0]["id"]]


def test_edits_and_new_posts_do_not_move_or_duplicate_page_boundaries(world):
    board, posts = seed_posts(world, 7)
    first = world.call("GET", f"boards/{board}/posts?limit=3")
    world.call("PATCH", f"posts/{posts[0]['id']}", {
        "expected_revision": 1, "content": {"media_type": "text/plain", "text": "Edited old evidence"}})
    world.post("A new live post")
    second = world.call("GET", f"boards/{board}/posts?limit=3&before={first['next_before']}")
    third = world.call("GET", f"boards/{board}/posts?limit=3&before={second['next_before']}")
    assert not {item["id"] for item in first["items"]} & {item["id"] for item in second["items"]}
    assert third["items"][0]["id"] == posts[0]["id"]
    assert third["items"][0]["content"]["text"] == "Edited old evidence"
    assert third["items"][0]["created_seq"] == posts[0]["created_seq"]


def test_literal_unicode_search_and_input_limits(world):
    board = world.board()["id"]
    post = world.post("Straße %_ literal ' OR 1=1 --")
    for query in ("STRASSE", "%25_", "%27%20OR%201%3D1%20--"):
        result = world.call("GET", f"boards/{board}/posts?query={query}")
        assert [item["id"] for item in result["items"]] == [post["id"]]
    for suffix in ("limit=0", "limit=101", "before=-1", "query=" + "x" * 257):
        world.call("GET", f"boards/{board}/posts?{suffix}", status=422)


def test_conversation_pages_filter_private_rows_before_paging(world):
    from test_message_delivery import setup
    sender, recipient, token, run, first, _ = setup(world)
    other = world.agent("Other")
    for index in range(6):
        world.call("POST", "messages", {
            "message_id": uid("msg"), "project_id": world.project, "from": sender["id"],
            "to": other["id"] if index % 2 else recipient["id"], "type": "inform",
            "thread_id": uid("thread"), "content": {"media_type": "text/plain", "text": f"Paged private {index}"}})
    one = world.call("GET", f"agents/{recipient['id']}/conversation?limit=2", token=token)
    two = world.call("GET", f"agents/{recipient['id']}/conversation?limit=2&before={one['next_before']}", token=token)
    assert len(one["items"]) == len(two["items"]) == 2 and two["next_before"] is None
    assert all(item["to"] == recipient["id"] for item in [*one["items"], *two["items"]])
    receipt = world.call("POST", f"messages/{first['id']}:claim", {"run_id": run["id"]}, token)
    world.call("POST", f"messages/{first['id']}:ack",
               {"run_id": run["id"], "delivery_id": receipt["delivery_id"]}, token)
    updated = world.call("GET", f"agents/{recipient['id']}/conversation?before={one['next_before']}&limit=2", token=token)
    assert updated["items"][0]["id"] == first["id"] and updated["items"][0]["delivery_status"] == "acked"
    hidden = world.call("GET", f"agents/{recipient['id']}/conversation?query=Paged%20private%201", token=token)
    assert not hidden["items"] and hidden["next_before"] is None
    human = world.credential(role="human")
    world.call("GET", f"projects/{world.project}/conversations?query=private", token=human, status=403)


def test_agent_board_pages_include_authorized_thread_replies(world):
    agent = world.agent()
    token = world.credential(agent)
    question = world.post("Agent finding", token)
    reply = world.call("POST", f"boards/{world.board()['id']}/posts", {
        "client_post_id": uid("reply"), "parent_post_id": question["id"], "thread_id": question["thread_id"],
        "content": {"media_type": "text/plain", "text": "Human follow-up"}})
    world.post("Unrelated conversation")
    result = world.call("GET", f"boards/{world.board()['id']}/posts?agent_id={agent['id']}&limit=1")
    assert result["items"][0]["id"] == reply["id"]
    earlier = world.call("GET", f"boards/{world.board()['id']}/posts?agent_id={agent['id']}&before={result['next_before']}")
    assert [item["id"] for item in earlier["items"]] == [question["id"]]


def test_legacy_revision_history_supplies_stable_creation_cursor(world):
    import json
    board, posts = seed_posts(world, 3)
    store = world.app.state.store
    with store.transaction():
        body = dict(posts[0])
        body.pop("created_seq")
        encoded = json.dumps(body)
        store.db.execute("UPDATE resources SET body=? WHERE id=?", (encoded, body["id"]))
        store.db.execute("UPDATE versions SET body=? WHERE id=? AND revision=1", (encoded, body["id"]))
    world.call("PATCH", f"posts/{posts[0]['id']}", {
        "expected_revision": 1, "content": {"media_type": "text/plain", "text": "Legacy edit"}})
    result = world.call("GET", f"boards/{board}/posts?before={posts[1]['created_seq']}")
    assert result["items"][0]["id"] == posts[0]["id"]
    assert result["items"][0]["created_seq"] == posts[0]["created_seq"]
