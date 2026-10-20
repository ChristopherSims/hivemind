"""Canonical tests for per-principal, revision-aware shared-board unread state.

The module is registered exactly the way the integration layer will register it
(``hivemind.capabilities``), so these tests exercise the real seam, the real
``command``/idempotency path for marking, and the real snapshot fence.
"""
import json

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import models as m
from hivemind.board_reads import (
    MAX_MARK_BATCH, install, markers_for_cursor, mark_posts, project_unread_summary, read_marker_digest,
    scan_state,
)
from hivemind.storage import Problem, uid


def install_board_reads(world, **options):
    """Install the module the way ``hivemind.capabilities`` will."""
    app = world.app

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) if isinstance(request, m.Model) else request
        return envelope(app.state.service.mutate(p, operation, key, payload, function))

    install(app, app.state.service, principal, command, envelope, **options)
    return world


@pytest.fixture
def reads(world):
    return install_board_reads(world)


def store_of(world):
    return world.app.state.store


def marker_rows(world):
    store = store_of(world)
    return store.db.execute("SELECT principal, project, post, revision FROM board_reads ORDER BY post").fetchall()


def conversation_read_rows(world):
    return store_of(world).db.execute("SELECT principal, message FROM conversation_reads").fetchall()


def principal_dict(world, role="admin"):
    store = store_of(world)
    return dict(store.db.execute("SELECT * FROM principals WHERE role=?", (role,)).fetchone())


def agent_post(world, token, text, board_id=None):
    board_id = board_id or world.board()["id"]
    return world.call("POST", f"boards/{board_id}/posts", {
        "client_post_id": uid("client"), "kind": "message",
        "content": {"media_type": "text/plain", "text": text}}, token)


def post_on(world, board_id, text, token=None):
    """Post to an explicit board; ``world.post`` targets the newest board."""
    return world.call("POST", f"boards/{board_id}/posts", {
        "client_post_id": uid("client"), "kind": "message",
        "content": {"media_type": "text/plain", "text": text}}, token)


def unread(world, board_id, token=None, query=""):
    return world.call("GET", f"boards/{board_id}/unread{query}", token=token)


def project_unread(world, token=None, query=""):
    return world.call("GET", f"projects/{world.project}/board-unread{query}", token=token)


def mark(world, board_id, post_ids, token=None, cursor=None, ignore_unavailable=None):
    body = {"post_ids": post_ids}
    if cursor is not None:
        body["snapshot_cursor"] = cursor
    if ignore_unavailable is not None:
        body["ignore_unavailable"] = ignore_unavailable
    return world.call("POST", f"boards/{board_id}/unread:read", body, token)


# ---------------------------------------------------------------------------
# Installation and honest "not installed" behavior
# ---------------------------------------------------------------------------

def test_capabilities_registration_is_the_only_wiring_the_integrator_needs(tmp_path, monkeypatch):
    """Adding the module name to FEATURE_MODULES installs it on a real app."""
    from hivemind import capabilities
    from hivemind.api import create_app
    from hivemind.cli import initialize

    monkeypatch.setattr(capabilities, "INSTALLED_FEATURE_MODULES",
                        (*capabilities.FEATURE_MODULES, "board_reads"))
    owner = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False)
    headers = {"Authorization": "Bearer " + owner["token"], "Idempotency-Key": uid("key")}
    with TestClient(app) as client:
        assert app.state.board_reads is not None
        boards = client.get("/v1/boards?project_id=" + owner["project_id"], headers=headers).json()["data"]
        board = boards[0]["id"]
        post = client.post(f"/v1/boards/{board}/posts", headers=headers, json={
            "client_post_id": uid("client"), "kind": "message",
            "content": {"media_type": "text/plain", "text": "Registered post"}}).json()["data"]
        state = client.get(f"/v1/boards/{board}/unread", headers=headers).json()["data"]
        assert state["available"] is True and state["revision_aware"] is True
        # The owner authored it, so the explicit author rule excludes it by default.
        assert state["unread_count"] == 0 and state["own_excluded_count"] == 1
        included = client.get(f"/v1/boards/{board}/unread?include_own=true",
                              headers=headers).json()["data"]
        assert included["unread_count"] == 1 and included["unread_post_ids"] == [post["id"]]
        # Registration must not shadow any existing canonical route.
        assert client.get("/v1/boards?project_id=" + owner["project_id"],
                          headers=headers).json()["data"] == boards


def test_install_creates_the_marker_table_idempotently_and_writes_nothing(reads):
    store = store_of(reads)
    assert store.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='board_reads'").fetchone() is not None
    install_board_reads(reads)  # reinstall must be safe and must not duplicate routes
    reads.post("First post on the board")
    assert marker_rows(reads) == []
    state = unread(reads, reads.board()["id"])
    assert state["available"] is True and state["counts_complete"] is True
    assert state["revision_aware"] is True and state["self_policy"] == "own_posts_never_unread"
    assert marker_rows(reads) == []


def test_helpers_are_safe_and_honest_before_installation(world):
    store = store_of(world)
    # Default applications now install the feature. Remove only this test's
    # empty, temporary table to exercise an older/uninstalled store.
    store.db.execute("DROP TABLE board_reads")
    owner = principal_dict(world)
    # No table yet: the shared stream_cursor/snapshot seams must not see a false zero.
    assert markers_for_cursor(store, owner, world.project, []) == []
    assert read_marker_digest(store, owner, world.project) is None
    state = scan_state(store, owner, world.project)
    assert state["available"] is False and state["counts_complete"] is False
    assert state["unread_count"] == 0 and state["read_digest"] is None
    summary = project_unread_summary(store, owner, world.project)
    assert summary["available"] is False and "snapshot_cursor" not in summary
    # Marking without the table fails closed rather than inventing markers.
    world.post("A post nobody can mark yet")
    with pytest.raises(Problem):
        mark_posts(store, owner, world.project, [uid("board_post")])


# ---------------------------------------------------------------------------
# Per-principal counts and the explicit author policy
# ---------------------------------------------------------------------------

def test_unread_counts_and_ids_are_per_principal(reads):
    board = reads.board()["id"]
    author = reads.agent("Author")
    author_token = reads.credential(author)
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    posts = [agent_post(reads, author_token, f"Author post {index}") for index in range(3)]

    author_state = unread(reads, board, author_token)
    assert author_state["unread_count"] == 0
    assert author_state["authored_count"] == 3 and author_state["own_excluded_count"] == 3

    reader_state = unread(reads, board, reader_token)
    assert reader_state["unread_count"] == 3
    assert reader_state["unread_post_ids"] == [post["id"] for post in reversed(posts)]
    assert reader_state["visible_count"] == 3 and reader_state["read_count"] == 0
    assert reader_state["authored_count"] == 0
    assert reader_state["counts_complete"] is True and reader_state["truncated"] is False
    assert reader_state["board_id"] == board and reader_state["board_title"] == "Main board"
    assert len(reader_state["snapshot_cursor"]) == 64 and len(reader_state["read_digest"]) == 64

    mark(reads, board, [posts[0]["id"]], reader_token)
    assert unread(reads, board, reader_token)["unread_count"] == 2
    # One principal's markers never leak into another principal's state.
    assert unread(reads, board, author_token)["unread_count"] == 0
    owner_state = unread(reads, board)
    assert owner_state["unread_count"] == 3 and owner_state["own_excluded_count"] == 0
    reader_principal = dict(store_of(reads).db.execute(
        "SELECT * FROM principals WHERE agent=?", (reader["id"],)).fetchone())
    assert {row["principal"] for row in marker_rows(reads)} == {reader_principal["id"]}


def test_author_policy_is_explicit_and_configurable(reads):
    board = reads.board()["id"]
    author = reads.agent("Author")
    author_token = reads.credential(author)
    agent_post(reads, author_token, "Self-authored post")
    agent_post(reads, reads.owner, "Owner post")

    default = unread(reads, board, author_token)
    assert default["self_policy"] == "own_posts_never_unread"
    assert default["include_own"] is False
    assert default["authored_count"] == 1 and default["own_excluded_count"] == 1
    assert default["unread_count"] == 1  # only the other principal's post

    included = unread(reads, board, author_token, "?include_own=true")
    assert included["include_own"] is True and included["unread_count"] == 2
    assert included["own_excluded_count"] == 0 and included["authored_count"] == 1


# ---------------------------------------------------------------------------
# Explicit, batched marking
# ---------------------------------------------------------------------------

def test_viewing_never_marks_read_but_explicit_batches_do(reads):
    board = reads.board()["id"]
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    posts = [reads.post(f"Post {index}") for index in range(2)]

    # Reading the timeline and the unread state is not a read receipt.
    reads.call("GET", f"boards/{board}/posts?limit=100")
    reads.call("GET", f"boards/{board}/posts?limit=100", token=reader_token)
    assert unread(reads, board, reader_token)["unread_count"] == 2
    assert marker_rows(reads) == []

    result = mark(reads, board, [post["id"] for post in posts], reader_token)
    assert result["marked_count"] == 2 and result["already_read_count"] == 0
    assert result["state"]["unread_count"] == 0
    assert [item["post_id"] for item in result["marked"]] == [post["id"] for post in posts]
    assert result["skipped"] == []
    # Idempotent re-marking is reported, not double-counted.
    again = mark(reads, board, [posts[0]["id"]], reader_token)
    assert again["marked_count"] == 0 and again["already_read_count"] == 1
    assert unread(reads, board, reader_token)["unread_count"] == 0


def test_marking_is_batched_to_one_hundred_posts(reads):
    board = reads.board()["id"]
    posts = [reads.post(f"Boundary post {index}") for index in range(MAX_MARK_BATCH)]
    assert mark(reads, board, [post["id"] for post in posts])["marked_count"] == MAX_MARK_BATCH
    assert unread(reads, board)["unread_count"] == 0
    # The batch cap is a schema limit, enforced before any canonical write.
    reads.call("POST", f"boards/{board}/unread:read",
               {"post_ids": [post["id"] for post in posts] + [uid("board_post")]}, status=422)
    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": []}, status=422)
    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": posts[0]["id"]}, status=422)


def test_edits_make_a_post_unread_again(reads):
    board = reads.board()["id"]
    author = reads.agent("Author")
    author_token = reads.credential(author)
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    post = agent_post(reads, author_token, "First revision")

    mark(reads, board, [post["id"]], reader_token)
    assert unread(reads, board, reader_token)["unread_count"] == 0

    edited = reads.call("PATCH", f"posts/{post['id']}", {
        "content": {"media_type": "text/plain", "text": "Second revision"},
        "expected_revision": post["revision"]}, author_token)
    assert edited["revision"] > post["revision"]
    state = unread(reads, board, reader_token)
    assert state["unread_count"] == 1 and state["unread_post_ids"] == [post["id"]]
    # The author's own post stays excluded either way; the marker revision moved only on re-mark.
    assert unread(reads, board, author_token)["unread_count"] == 0
    assert unread(reads, board, author_token, "?include_own=true")["unread_count"] == 1
    mark(reads, board, [post["id"]], reader_token)
    assert unread(reads, board, reader_token)["unread_count"] == 0
    assert {row["revision"] for row in marker_rows(reads)} == {edited["revision"]}


def test_default_page_markers_are_exact_beyond_summary_marker_limit(world):
    reader = world.credential(world.agent("Page reader"))
    board = world.board()["id"]
    posts = [world.post(f"Exact page marker {index}") for index in range(3)]
    state = unread(world, board, reader, "?marker_limit=1")
    assert len(state["unread_post_ids"]) == 1 and state["unread_markers_truncated"]
    page = world.call("GET", f"boards/{board}/posts", token=reader)
    assert all(post["board_unread"] for post in page["items"])
    result = world.call("POST", f"boards/{board}/unread:read", {
        "post_ids": [posts[0]["id"]], "post_revisions": {posts[0]["id"]: posts[0]["revision"]},
        "snapshot_cursor": page["snapshot_cursor"]}, reader)
    assert result["snapshot_cursor"] != page["snapshot_cursor"]
    assert result["state"]["snapshot_cursor"] == result["snapshot_cursor"]
    page = world.call("GET", f"boards/{board}/posts?snapshot_cursor={result['snapshot_cursor']}", token=reader)
    assert {post["id"]: post["board_unread"] for post in page["items"]} == {
        post["id"]: post["id"] != posts[0]["id"] for post in posts}


def test_displayed_revision_fence_rejects_unseen_edits_without_partial_markers(world):
    reader = world.credential(world.agent("Revision reader"))
    board = world.board()["id"]
    first, second = world.post("First displayed revision"), world.post("Second displayed revision")
    world.call("PATCH", f"posts/{second['id']}", {
        "expected_revision": second["revision"],
        "content": {"media_type": "text/plain", "text": "An unseen newer revision"}})
    current = unread(world, board, reader)
    world.call("POST", f"boards/{board}/unread:read", {
        "post_ids": [first["id"], second["id"]], "snapshot_cursor": current["snapshot_cursor"],
        "post_revisions": {first["id"]: first["revision"], second["id"]: second["revision"]}},
        reader, status=409)
    assert marker_rows(world) == []

# ---------------------------------------------------------------------------
# Deletion, hiding and access revocation
# ---------------------------------------------------------------------------

def test_deleted_and_hidden_posts_are_excluded_from_counts_and_marking(reads):
    board = reads.board()["id"]
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    posts = [reads.post(f"Post {index}") for index in range(3)]
    mark(reads, board, [post["id"] for post in posts], reader_token)

    reads.call("DELETE", f"posts/{posts[0]['id']}", {"expected_revision": posts[0]["revision"]})
    state = unread(reads, board, reader_token)
    assert state["visible_count"] == 2 and state["unread_count"] == 0
    assert posts[0]["id"] not in state["unread_post_ids"]
    # A deleted post can never be marked again, and no marker row is invented for it.
    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": [posts[0]["id"]]},
               reader_token, status=404)

    # A private (hidden) post is invisible to this reader: not counted, not markable.
    hidden = reads.post("Private note")
    reads.call("PUT", f"resources/{hidden['id']}/access", {"public_read": False, "reader_agent_ids": []})
    state = unread(reads, board, reader_token)
    assert state["visible_count"] == 2 and hidden["id"] not in state["unread_post_ids"]
    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": [hidden["id"]]},
               reader_token, status=404)


def test_team_and_board_access_is_reauthorized_on_every_read_and_mark(reads):
    board = reads.board()["id"]
    author = reads.agent("Author")
    author_token = reads.credential(author)
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    outsider = reads.agent("Outsider")
    outsider_token = reads.credential(outsider)
    posts = [agent_post(reads, author_token, f"Shared post {index}", board) for index in range(2)]

    assert unread(reads, board, reader_token)["unread_count"] == 2
    assert unread(reads, board, outsider_token)["unread_count"] == 2

    # A board-level restriction is re-checked per request: the outsider loses both
    # the state read and the ability to mark, while the reader keeps both.
    hidden_board = reads.call("POST", "boards", {"project_id": reads.project, "title": "Restricted",
                                                 "access": {"public_read": False,
                                                            "reader_agent_ids": [reader["id"]],
                                                            "contributor_agent_ids": [author["id"]]}})
    restricted = agent_post(reads, author_token, "Restricted post", hidden_board["id"])
    assert unread(reads, hidden_board["id"], reader_token)["unread_count"] == 1
    reads.call("GET", f"boards/{hidden_board['id']}/unread", token=outsider_token, status=404)
    reads.call("POST", f"boards/{hidden_board['id']}/unread:read", {"post_ids": [restricted["id"]]},
               outsider_token, status=404)
    assert unread(reads, hidden_board["id"], reader_token)["unread_count"] == 1
    assert unread(reads, hidden_board["id"])["unread_count"] == 1  # admin still sees it

    # A per-post restriction is re-checked the same way.
    reads.call("PUT", f"resources/{posts[0]['id']}/access", {"public_read": False})
    assert unread(reads, board, reader_token)["unread_count"] == 1
    assert unread(reads, board, outsider_token)["unread_count"] == 1
    for token in (reader_token, outsider_token):
        reads.call("POST", f"boards/{board}/unread:read", {"post_ids": [posts[0]["id"]]},
                   token, status=404)
    # The reader can still mark what it is allowed to see, for itself alone.
    assert mark(reads, board, [posts[1]["id"]], reader_token)["marked_count"] == 1
    assert unread(reads, board, reader_token)["unread_count"] == 0
    assert unread(reads, board, outsider_token)["unread_count"] == 1


# ---------------------------------------------------------------------------
# Snapshot drift, exact scope and fail-closed batches
# ---------------------------------------------------------------------------

def test_snapshot_drift_is_detected_before_reading_or_marking(reads):
    board = reads.board()["id"]
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    post_on(reads, board, "Post zero")
    state = unread(reads, board, reader_token)
    cursor = state["snapshot_cursor"]

    post_on(reads, board, "Post one")  # moves the principal/project-bound snapshot
    reads.call("GET", f"boards/{board}/unread?snapshot_cursor={cursor}", token=reader_token, status=409)
    reads.call("POST", f"boards/{board}/unread:read",
               {"post_ids": [uid("board_post")], "snapshot_cursor": cursor}, reader_token, status=409)
    assert marker_rows(reads) == []

    # A fresh cursor succeeds and is echoed back for the next binding.
    fresh = unread(reads, board, reader_token)
    result = mark(reads, board, [fresh["unread_post_ids"][0]], reader_token,
                  cursor=fresh["snapshot_cursor"])
    assert result["marked_count"] == 1 and len(result["snapshot_cursor"]) == 64
    reads.call("GET", f"boards/{board}/unread?snapshot_cursor={'0' * 64}", token=reader_token, status=409)


def test_marking_is_exactly_scoped_and_fails_closed_without_partial_writes(reads):
    board = reads.board()["id"]
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    other = reads.call("POST", "boards", {"project_id": reads.project, "title": "Other board"})
    on_board = post_on(reads, board, "On the first board")
    on_other = post_on(reads, other["id"], "On the other board")

    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": [on_board["id"], on_other["id"]]},
               reader_token, status=404)
    assert marker_rows(reads) == []  # the valid id was not written either
    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": [on_board["id"], uid("board_post")]},
               reader_token, status=404)
    assert marker_rows(reads) == []

    # Explicit opt-in reports the unavailable ids instead of failing the batch.
    result = mark(reads, board, [on_board["id"], on_other["id"], uid("board_post")],
                  reader_token, ignore_unavailable=True)
    assert result["marked_count"] == 1
    assert [item["post_id"] for item in result["marked"]] == [on_board["id"]]
    assert [item["reason"] for item in result["skipped"]] == ["not_found", "not_found"]
    assert [row["post"] for row in marker_rows(reads)] == [on_board["id"]]
    # The project-scoped route resolves each post against the project, not one board.
    assert mark(reads, other["id"], [on_other["id"]], reader_token)["marked_count"] == 1
    assert unread(reads, other["id"], reader_token)["unread_count"] == 0


def test_scope_and_authentication_failures(reads):
    board = reads.board()["id"]
    reads.post("A post")
    assert reads.client.get(f"/v1/boards/{board}/unread").status_code == 401
    assert reads.client.get(f"/v1/projects/{reads.project}/board-unread").status_code == 401
    assert reads.client.post(f"/v1/boards/{board}/unread:read", json={"post_ids": [uid("board_post")]}
                             ).status_code == 401

    outsider_project = uid("project")
    store = store_of(reads)
    with store.transaction():
        store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (outsider_project, "Elsewhere"))
    foreign_token = reads.credential(None, project=outsider_project)
    # Project scope is enforced by the canonical access primitive, not by this module.
    reads.call("GET", f"projects/{reads.project}/board-unread", token=foreign_token, status=404)
    reads.call("GET", f"boards/{board}/unread", token=foreign_token, status=404)
    reads.call("POST", f"boards/{board}/unread:read", {"post_ids": [uid("board_post")]},
               foreign_token, status=404)
    # A board from another project cannot be pinned onto this project's read.
    reads.call("GET", f"projects/{reads.project}/board-unread?board_id={uid('board')}", status=404)
    other = reads.call("POST", "boards", {"project_id": reads.project, "title": "Elsewhere board"})
    assert unread(reads, other["id"])["board_id"] == other["id"]


# ---------------------------------------------------------------------------
# Independence from delivery acknowledgements and conversation reads
# ---------------------------------------------------------------------------

def test_read_markers_are_independent_of_delivery_and_conversation_reads(reads):
    board = reads.board()["id"]
    sender = reads.agent("Sender")
    sender_token = reads.credential(sender)
    recipient = reads.agent("Recipient")
    recipient_token = reads.credential(recipient)
    post = agent_post(reads, sender_token, "Board post")

    message = reads.call("POST", "messages", {
        "message_id": uid("message"), "project_id": reads.project,
        "from": sender["id"], "to": recipient["id"], "type": "inform",
        "thread_id": uid("thread"), "content": {"media_type": "text/plain", "text": "Direct"}},
        sender_token)
    delivery_before = reads.call("GET", f"messages/{message['id']}")

    assert mark(reads, board, [post["id"]], recipient_token)["marked_count"] == 1
    # Board reading is not a delivery fact.
    assert conversation_read_rows(reads) == []
    assert reads.call("GET", f"messages/{message['id']}") == delivery_before
    assert reads.call("GET", f"agents/{recipient['id']}/inbox?project_id={reads.project}",
                      recipient_token)[0]["state"] == delivery_before["state"]

    # A conversation read is not a board read.
    reads.call("POST", f"projects/{reads.project}/conversations:read",
               {"message_ids": [message["id"]]}, recipient_token)
    assert [row["message"] for row in conversation_read_rows(reads)] == [message["id"]]
    assert unread(reads, board, recipient_token)["unread_count"] == 0
    assert [row["post"] for row in marker_rows(reads)] == [post["id"]]
    # And the unread helper never consults delivery state.
    assert unread(reads, board, recipient_token)["read_count"] == 1


# ---------------------------------------------------------------------------
# Bounded, honest scans and the shared-route seams
# ---------------------------------------------------------------------------

def test_bounded_scans_report_truncation_and_marker_caps(world):
    reads = install_board_reads(world, scan_limit=2, marker_limit=1)
    board = reads.board()["id"]
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    posts = [post_on(reads, board, f"Post {index}") for index in range(5)]

    state = unread(reads, board, reader_token)
    assert state["truncated"] is True and state["counts_complete"] is False
    assert state["examined_count"] == 2 and state["scan_limit"] == 2
    # A truncated count is a lower bound, and the marker list is separately capped.
    assert state["unread_count"] == 2 and state["unread_post_ids"] == [posts[-1]["id"]]
    assert state["unread_markers_truncated"] is True
    assert state["visible_count"] == 2

    # A full scan reports completeness and every marker.
    install_board_reads(world, scan_limit=2000, marker_limit=200)
    full = unread(reads, board, reader_token)
    assert full["truncated"] is False and full["counts_complete"] is True
    assert full["unread_count"] == 5 and len(full["unread_post_ids"]) == 5
    assert full["unread_markers_truncated"] is False
    reads.call("GET", f"boards/{board}/unread?scan_limit=0", status=422)
    reads.call("GET", f"boards/{board}/unread?marker_limit=5000", status=422)
    # A per-request bound is honored, and the default cap still applies unasked.
    bounded = unread(reads, board, reader_token, "?scan_limit=1&marker_limit=1")
    assert bounded["truncated"] is True and bounded["unread_count"] == 1
    assert bounded["unread_markers_truncated"] is True


def test_project_badges_group_by_board_and_hide_unreadable_boards(reads):
    board = reads.board()["id"]
    second = reads.call("POST", "boards", {"project_id": reads.project, "title": "Second board"})
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    posts = [post_on(reads, board, f"Main {index}") for index in range(2)]
    other = post_on(reads, second["id"], "Second board post")

    state = project_unread(reads, reader_token)
    assert state["board_id"] is None and state["unread_count"] == 3
    badges = {item["board_id"]: item for item in state["boards"]}
    assert badges[board]["unread_count"] == 2 and badges[board]["title"] == "Main board"
    assert badges[board]["visible_count"] == 2 and badges[board]["counts_complete"] is True
    assert badges[second["id"]]["unread_count"] == 1
    assert state["unread_post_ids"][0] == other["id"]
    assert "snapshot_cursor" in state and len(state["snapshot_cursor"]) == 64

    hidden = reads.call("POST", "boards", {"project_id": reads.project, "title": "Hidden",
                                           "access": {"public_read": False}})
    hidden_state = project_unread(reads, reader_token)
    assert hidden["id"] not in {item["board_id"] for item in hidden_state["boards"]}
    reads.call("GET", f"projects/{reads.project}/board-unread?board_id={hidden['id']}",
               token=reader_token, status=404)

    # Project-scoped marking re-authorizes each post and returns the fresh badges.
    result = reads.call("POST", f"projects/{reads.project}/board-unread:read",
                        {"post_ids": [post["id"] for post in posts]}, reader_token)
    assert result["marked_count"] == 2 and result["board_id"] is None
    assert {item["board_id"]: item["unread_count"] for item in result["state"]["boards"]}[board] == 0
    assert unread(reads, board, reader_token)["unread_count"] == 0
    assert unread(reads, second["id"], reader_token)["unread_count"] == 1


def test_cursor_seams_fold_only_currently_visible_markers(reads):
    store = store_of(reads)
    board = reads.board()["id"]
    reader = reads.agent("Reader")
    reader_token = reads.credential(reader)
    reader_principal = dict(store.db.execute(
        "SELECT * FROM principals WHERE agent=?", (reader["id"],)).fetchone())
    posts = [post_on(reads, board, f"Post {index}") for index in range(3)]
    mark(reads, board, [post["id"] for post in posts], reader_token)

    visible_ids = [row["id"] for row in store.db.execute(
        "SELECT * FROM resources WHERE kind='board_post' AND deleted=0")]
    folded = markers_for_cursor(store, reader_principal, reads.project, visible_ids)
    assert folded == sorted([[post["id"], post["revision"]] for post in posts])
    assert markers_for_cursor(store, reader_principal, reads.project, []) == []
    assert markers_for_cursor(store, principal_dict(reads), reads.project, visible_ids) == []

    digest = read_marker_digest(store, reader_principal, reads.project)
    mark(reads, board, [posts[1]["id"]], reader_token)  # already read: digest is stable
    assert read_marker_digest(store, reader_principal, reads.project) == digest
    assert read_marker_digest(store, principal_dict(reads), reads.project) != digest

    # A marker for a post this principal can no longer see is not folded into the cursor.
    reads.call("PUT", f"resources/{posts[0]['id']}/access", {"public_read": False})
    still_visible = [row["id"] for row in store.db.execute(
        "SELECT * FROM resources WHERE kind='board_post' AND deleted=0")
        if store.visible(row, reader_principal)]
    hidden_aware = markers_for_cursor(store, reader_principal, reads.project, still_visible)
    assert hidden_aware == sorted([[post["id"], post["revision"]] for post in posts[1:]])
    assert unread(reads, board, reader_token)["visible_count"] == 2

    # The embedded dashboard-snapshot summary matches the dedicated route body.
    summary = project_unread_summary(store, reader_principal, reads.project)
    route = project_unread(reads, reader_token)
    dropped = {"snapshot_cursor", "generated_at"}
    assert {key: value for key, value in summary.items() if key not in dropped} == {
        key: value for key, value in route.items() if key not in dropped}
    assert summary["board_id"] is None and summary["available"] is True
