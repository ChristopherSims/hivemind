"""Author-scoped mention receipts: canonical queued records, minimal disclosure."""
import json
import time

import pytest
from fastapi import Request

from hivemind.mention_feedback import install, mention_receipts
from hivemind.storage import uid


@pytest.fixture
def receipts(world):
    """Install the read-only receipt route the way the integration layer will."""
    app = world.app

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    def command(*args, **kwargs):
        raise AssertionError("mention receipts must not expose a command route")

    install(app, app.state.service, principal, command, envelope)
    return world


def mention_segments(agent_id, text="Please confirm"):
    return [{"kind": "text", "text": text}, {"kind": "agent_mention", "agent_id": agent_id}]


def stored_notification(world, post_id):
    row = world.app.state.store.db.execute(
        "SELECT * FROM resources WHERE kind='notification' AND deleted=0 "
        "AND json_extract(body,'$.post_id')=?", (post_id,)).fetchone()
    assert row is not None, "the canonical mention notification must exist"
    return row


def age_run(world, run, seconds):
    """Backdate a run so the coarse presence band is deterministic."""
    row = world.app.state.store.db.execute("SELECT * FROM resources WHERE id=?", (run["id"],)).fetchone()
    body = {**json.loads(row["body"]), "last_seen_at": time.time() - seconds}
    world.app.state.store.db.execute(
        "UPDATE resources SET body=? WHERE id=?", (json.dumps(body), run["id"]))
    return body


def test_default_app_installs_author_scoped_receipts(world):
    agent = world.agent("Default receipt recipient")
    post = world.post(segments=mention_segments(agent["id"]))
    result = world.call("GET", f"posts/{post['id']}/mention-receipts")
    assert result["queued_count"] == 1
    assert result["recipients"][0]["agent_id"] == agent["id"]


def test_receipts_report_the_canonical_queued_record_for_an_offline_agent(receipts):
    agent = receipts.agent("Droid worker")
    run = receipts.run(agent)
    age_run(receipts, run, 120)  # offline band, mirrors /v1/ledger
    post = receipts.post(segments=mention_segments(agent["id"]))

    result = receipts.call("GET", f"posts/{post['id']}/mention-receipts")
    assert result["post_id"] == post["id"]
    assert result["queued_count"] == 1
    assert isinstance(result["generated_at"], (int, float))
    recipient = result["recipients"][0]
    assert recipient["agent_id"] == agent["id"]
    assert recipient["agent_display_name"] == "Droid worker"
    assert recipient["state"] == "queued"
    assert recipient["presence"] == "offline"
    # The receipt names the real canonical notification, not a presence-based guess.
    canonical = stored_notification(receipts, post["id"])
    assert recipient["notification_id"] == canonical["id"]
    assert recipient["expires_at"] is not None and recipient["expires_at"] > time.time()


def test_receipts_are_author_scoped_and_limited_to_the_one_post(receipts):
    author = receipts.credential(None, role="human")
    other = receipts.credential(None, role="observer")
    operator = receipts.credential(None, role="admin")
    first, second = receipts.agent("First worker"), receipts.agent("Second worker")
    post = receipts.post(token=author, segments=mention_segments(first["id"], "Ping first"))
    other_post = receipts.post(token=author, segments=mention_segments(second["id"], "Ping second"))

    result = receipts.call("GET", f"posts/{post['id']}/mention-receipts", token=author)
    assert [item["agent_id"] for item in result["recipients"]] == [first["id"]]
    # A reader, and even an operator who did not author the post, reads not_found.
    receipts.call("GET", f"posts/{post['id']}/mention-receipts", token=other, status=404)
    receipts.call("GET", f"posts/{post['id']}/mention-receipts", token=operator, status=404)
    # Another author's own post still resolves; receipts never cross posts.
    assert receipts.call("GET", f"posts/{other_post['id']}/mention-receipts",
                         token=author)["recipients"][0]["agent_id"] == second["id"]


def test_receipts_are_minimal_and_never_mirror_the_recipient_private_record(receipts):
    author = receipts.credential(None, role="human")
    agent = receipts.agent("Private worker")
    post = receipts.post(token=author, segments=mention_segments(agent["id"]))
    canonical = stored_notification(receipts, post["id"])
    assert canonical["private_agent"] == agent["id"], "the notification is recipient-private"

    recipient = receipts.call("GET", f"posts/{post['id']}/mention-receipts",
                              token=author)["recipients"][0]
    # Fixed whitelist: no body, run id, run summary, task or processing state leaks.
    assert set(recipient) == {"agent_id", "agent_display_name", "notification_id", "state",
                              "created_at", "expires_at", "presence"}
    assert "body" not in json.dumps(recipient)
    # The author still cannot read the underlying private notification itself.
    receipts.call("GET", f"agents/{agent['id']}/notifications?project_id={receipts.project}",
                  token=author, status=403)


def test_receipts_stop_reporting_queued_once_the_notification_is_no_longer_available(receipts):
    agent = receipts.agent("Droid worker")
    agent_token = receipts.credential(agent)
    post = receipts.post(segments=mention_segments(agent["id"]))
    notification_id = stored_notification(receipts, post["id"])["id"]

    assert receipts.call("GET", f"posts/{post['id']}/mention-receipts")["queued_count"] == 1
    receipts.call("POST", f"notifications/{notification_id}:ack", {}, token=agent_token)
    closed = receipts.call("GET", f"posts/{post['id']}/mention-receipts")
    assert closed["queued_count"] == 0
    assert closed["recipients"][0]["state"] == "closed"


def test_receipts_handle_deleted_posts_and_posts_without_mentions(receipts):
    agent = receipts.agent("Droid worker")
    plain = receipts.post("No mentions here")  # text/plain content carries no segments
    result = receipts.call("GET", f"posts/{plain['id']}/mention-receipts")
    assert result["recipients"] == [] and result["queued_count"] == 0

    post = receipts.post(segments=mention_segments(agent["id"]))
    receipts.call("DELETE", f"posts/{post['id']}", {"expected_revision": post["revision"]})
    receipts.call("GET", f"posts/{post['id']}/mention-receipts", status=404)


def test_receipt_helper_rejects_an_unauthenticated_store_read(receipts):
    agent = receipts.agent("Droid worker")
    post = receipts.post(segments=mention_segments(agent["id"]))
    store = receipts.app.state.store
    principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    service = receipts.app.state.service
    # The helper is usable directly, and still enforces author scoping.
    assert mention_receipts(service, principal, post["id"])["queued_count"] == 1
    stranger = {**principal, "id": uid("principal")}
    from hivemind.storage import Problem
    with pytest.raises(Problem) as failure:
        mention_receipts(service, stranger, post["id"])
    assert failure.value.status == 404
