import json
import secrets
import time

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import conversations
from hivemind import models as m
from hivemind.api import create_app, envelope, hash_token
from hivemind.cli import initialize
from hivemind.storage import Problem, Store, uid


class Harness:
    """HTTP harness for the conversation workflow routes.

    ``create_app`` owns the production ``principal``/``command`` wiring; this file owns only
    the module routes, so the harness supplies equivalent callables and installs them the
    same way ``api.py`` will.
    """

    def __init__(self, app, client, config):
        self.app, self.client = app, client
        self.project, self.owner = config["project_id"], config["token"]

    def raw(self, method, path, body=None, token=None, key=None):
        return self.client.request(method, "/v0.1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner), "Idempotency-Key": key or uid("key")})

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.raw(method, path, body, token, key)
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def failure(self, method, path, body=None, token=None, key=None, status=409):
        response = self.raw(method, path, body, token, key)
        assert response.status_code == status, response.text
        return response.json()["error"]["code"]

    def agent(self, name="Worker"):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name})

    def credential(self, agent=None, role="agent"):
        token = secrets.token_urlsafe(32)
        store = self.app.state.store
        with store.transaction():
            store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                             (uid("principal"), hash_token(token), role, self.project,
                              agent["id"] if agent else None, 0, 1))
        return token

    def run(self, agent, token=None):
        return self.call("POST", "runs", {"project_id": self.project, "agent_id": agent["id"]}, token)

    def request(self, sender, recipient, message_id=None, text="Does the session response require expires_at?",
                deadline=60, ttl=3600, thread_id=None, type="request", **kwargs):
        return self.call("POST", "conversation-requests", {
            "project_id": self.project, "message_id": message_id or uid("msg"),
            "from_agent": sender["id"], "to": recipient["id"], "thread_id": thread_id or uid("thread"),
            "type": type, "ttl_seconds": ttl, "response_deadline_seconds": deadline,
            "content": {"media_type": "text/markdown", "text": text}}, **kwargs)

    def message(self, sender, recipient, ttl=3600, type="inform", text="Work item", thread_id=None):
        return self.call("POST", "messages", {
            "message_id": uid("msg"), "project_id": self.project, "from": sender["id"],
            "to": recipient["id"], "type": type, "thread_id": thread_id or uid("thread"),
            "ttl_seconds": ttl, "content": {"media_type": "text/plain", "text": text}})

    def reply_payload(self, workflow, sender, recipient, type="response", correlation=None,
                      in_reply_to=None, text="Yes, keep expires_at"):
        return {"message_id": uid("msg"), "project_id": self.project, "from": sender["id"],
                "to": recipient["id"], "type": type, "thread_id": workflow["thread_id"],
                "correlation_id": correlation if correlation is not None else workflow["correlation_id"],
                "in_reply_to": in_reply_to or workflow["message_id"],
                "content": {"media_type": "text/plain", "text": text}}

    def conversation(self, token=None):
        return self.call("GET", f"projects/{self.project}/conversations", token=token)["items"]

    def claim(self, message, run, token, **kwargs):
        return self.call("POST", f"messages/{message['id']}:claim", {"run_id": run["id"]}, token, **kwargs)

    def renew(self, message, run, token, delivery_id, ttl=conversations.LEASE_TTL_SECONDS, **kwargs):
        return self.call("POST", f"messages/{message['id']}:renew", {
            "run_id": run["id"], "delivery_id": delivery_id, "ttl_seconds": ttl}, token, **kwargs)


@pytest.fixture
def world(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app = create_app(tmp_path, workers=False)
    store, service = app.state.store, app.state.service

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        row = (store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                                (hash_token(auth[7:]),)).fetchone()
               if auth.startswith("Bearer ") else None)
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, request, function):
        payload = (request.model_dump(by_alias=True, exclude_none=True)
                   if isinstance(request, m.Model) else request)
        return envelope(service.mutate(p, operation, key, payload, function))

    conversations.install(app, service, principal, command, envelope)
    with TestClient(app) as client:
        yield Harness(app, client, config)


def test_tracked_request_records_correlation_deadline_and_never_acks_delivery(world):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    sender_token, recipient_token = world.credential(sender), world.credential(recipient)
    payload = {"project_id": world.project, "message_id": uid("msg"), "from_agent": sender["id"],
               "to": recipient["id"], "thread_id": uid("thread"), "type": "request", "ttl_seconds": 3600,
               "response_deadline_seconds": 30,
               "content": {"media_type": "text/markdown", "text": "Does the frontend need expires_at?"}}
    workflow = world.call("POST", "conversation-requests", payload, sender_token, key="open")
    assert world.call("POST", "conversation-requests", payload, sender_token, key="open") == workflow
    replay = world.call("POST", "conversation-requests", payload, sender_token)
    assert replay["id"] == workflow["id"] and replay["revision"] == workflow["revision"]
    assert workflow["state"] == "open" and workflow["workflow_state"] == "open"
    assert workflow["unanswered"] is True and workflow["response_overdue"] is False
    assert workflow["correlation_id"] == workflow["thread_id"]
    assert workflow["response_deadline"] <= workflow["message"]["expires_at"]
    assert workflow["message"]["delivery_status"] == "available"
    assert workflow["message"]["from"] == sender["id"] and workflow["message"]["to"] == recipient["id"]

    # Replies must preserve thread, correlation, and participants through the existing validation.
    wrong_correlation = world.reply_payload(workflow, recipient, sender, correlation=uid("corr"))
    assert world.failure("POST", "messages", wrong_correlation, recipient_token, status=422) == "thread_mismatch"
    swapped = world.reply_payload(workflow, sender, sender, in_reply_to=workflow["message_id"])
    assert world.failure("POST", "messages", swapped, sender_token, status=422) == "thread_mismatch"

    reply = world.call("POST", "messages", world.reply_payload(workflow, recipient, sender), recipient_token)
    reconciled = world.call("POST", f"conversation-requests/{workflow['id']}:reconcile", {}, sender_token)
    assert reconciled["state"] == "answered" and reconciled["reconciled"] is True
    assert reconciled["answer_message_id"] == reply["id"] and reconciled["answered_at"]
    assert reconciled["unanswered"] is False
    again = world.call("POST", f"conversation-requests/{workflow['id']}:reconcile", {}, sender_token)
    assert again["state"] == "answered" and again["reconciled"] is False
    # Workflow state is separate from delivery state: nothing acknowledged the request envelope.
    detail = world.call("GET", f"messages/{workflow['message_id']}", token=recipient_token)
    assert detail["delivery_status"] == "available" and detail["state"] == "available"
    assert world.call("GET", f"agents/{recipient['id']}/inbox?project_id={world.project}",
                      token=recipient_token)[0]["id"] == workflow["message_id"]


def test_respond_helper_is_addressee_only_and_keeps_delivery_state_untouched(world):
    sender, recipient, bystander = world.agent("Sender"), world.agent("Recipient"), world.agent("Bystander")
    sender_token, recipient_token = world.credential(sender), world.credential(recipient)
    bystander_token = world.credential(bystander)
    run = world.run(recipient, recipient_token)
    observer = world.credential(role="observer")
    workflow = world.request(sender, recipient, deadline=60)
    body = {"content": {"media_type": "text/plain", "text": "Confirmed: keep expires_at"},
            "type": "response", "run_id": run["id"]}
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:respond", body, sender_token,
                         status=403) == "forbidden"
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:respond", body, bystander_token,
                         status=404) == "not_found"
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:respond", body, observer,
                         status=403) == "forbidden"
    answered = world.call("POST", f"conversation-requests/{workflow['id']}:respond", body, recipient_token,
                          key="answer")
    assert answered["state"] == "answered" and answered["answer_message_id"]
    assert answered["message"]["delivery_status"] == "available"
    assert world.call("POST", f"conversation-requests/{workflow['id']}:respond", body, recipient_token,
                      key="answer") == answered
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:respond", body, recipient_token,
                         status=409) == "request_closed"
    answered_reply = [item for item in world.conversation(observer) if item["id"] == answered["answer_message_id"]][0]
    assert answered_reply["in_reply_to"] == workflow["message_id"]
    assert answered_reply["correlation_id"] == workflow["correlation_id"]
    assert answered_reply["from"] == recipient["id"] and answered_reply["to"] == sender["id"]


def test_lease_renewal_is_fenced_by_receipt_and_bounded_by_original_ttl(world, monkeypatch):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    token = world.credential(recipient)
    run = world.run(recipient, token)
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    message = world.message(sender, recipient)
    receipt = world.claim(message, run, token)
    assert receipt["claim_until"] == clock[0] + conversations.LEASE_TTL_SECONDS
    renewed = world.renew(message, run, token, receipt["delivery_id"], ttl=120, key="renew")
    assert renewed["claim_until"] == clock[0] + 120
    assert renewed["delivery_id"] == receipt["delivery_id"] and renewed["attempt_count"] == 1
    assert renewed["delivery_status"] == "leased" and renewed["state"] == "delivered"
    assert renewed["delivery_history"][-1]["action"] == "lease_renewed"
    assert renewed["delivery_history"][-1]["ttl_seconds"] == 120
    assert world.renew(message, run, token, receipt["delivery_id"], ttl=120, key="renew") == renewed
    assert world.failure("POST", f"messages/{message['id']}:renew",
                         {"run_id": run["id"], "delivery_id": receipt["delivery_id"], "ttl_seconds": 7200},
                         token, status=422) == "invalid_request"
    assert world.failure("POST", f"messages/{message['id']}:renew",
                         {"run_id": run["id"], "delivery_id": uid("delivery")},
                         token, status=409) == "delivery_receipt_stale"

    # A renewal can never extend delivery past the original message expiry.
    short = world.message(sender, recipient, ttl=60)
    short_receipt = world.claim(short, run, token)
    extended = world.renew(short, run, token, short_receipt["delivery_id"], ttl=120)
    assert extended["claim_until"] == short["expires_at"]

    # Another agent's run cannot renew this delivery.
    other = world.agent("Other")
    other_token = world.credential(other)
    other_run = world.run(other, other_token)
    assert world.failure("POST", f"messages/{message['id']}:renew",
                         {"run_id": other_run["id"], "delivery_id": renewed["delivery_id"]},
                         other_token, status=404) == "not_found"

    # An acknowledged delivery is not a lease and is never renewed or re-acked.
    acked = world.call("POST", f"messages/{message['id']}:ack",
                       {"run_id": run["id"], "delivery_id": renewed["delivery_id"]}, token)
    assert acked["delivery_status"] == "acked"
    assert world.failure("POST", f"messages/{message['id']}:renew",
                         {"run_id": run["id"], "delivery_id": renewed["delivery_id"]},
                         token, status=409) == "delivery_lease_expired"
    assert world.call("GET", f"messages/{message['id']}", token=token)["state"] == "acked"


def test_renewal_never_revives_an_expired_or_replaced_lease(world, monkeypatch):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    token = world.credential(recipient)
    first_run = world.run(recipient, token)
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    message = world.message(sender, recipient)
    first = world.claim(message, first_run, token)
    clock[0] += 31
    assert world.failure("POST", f"messages/{message['id']}:renew",
                         {"run_id": first_run["id"], "delivery_id": first["delivery_id"]},
                         token, status=409) == "delivery_lease_expired"
    clock[0] += 3
    second_run = world.run(recipient, token)
    second = world.claim(message, second_run, token)
    assert second["delivery_id"] != first["delivery_id"]
    assert world.failure("POST", f"messages/{message['id']}:renew",
                         {"run_id": second_run["id"], "delivery_id": first["delivery_id"]},
                         token, status=409) == "delivery_receipt_stale"
    current = world.renew(message, second_run, token, second["delivery_id"], ttl=60)
    assert current["claim_until"] > second["claim_until"]
    assert current["delivery_id"] == second["delivery_id"] and current["attempt_count"] == 2

    # A terminal run cannot renew the lease it still holds.
    terminal = world.message(sender, recipient)
    terminal_run = world.run(recipient, token)
    held = world.claim(terminal, terminal_run, token)
    store = world.app.state.store
    with store.transaction():
        admin = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        body = store.get(terminal_run["id"], admin)
        body["status"] = "completed"
        store.save("run", world.project, admin["id"], body)
    assert world.failure("POST", f"messages/{terminal['id']}:renew",
                         {"run_id": terminal_run["id"], "delivery_id": held["delivery_id"]},
                         token, status=409) == "terminal_run"


def test_cancellation_requires_the_sender_and_never_impersonates_or_acks(world):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    sender_token, recipient_token = world.credential(sender), world.credential(recipient)
    bystander_token = world.credential(world.agent("Bystander"))
    observer = world.credential(role="observer")
    workflow = world.request(sender, recipient, deadline=60)
    body = {"expected_revision": workflow["revision"], "reason": "requirement withdrawn"}
    for token in (recipient_token, observer):
        assert world.failure("POST", f"conversation-requests/{workflow['id']}:cancel", body, token,
                             status=403) == "forbidden"
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:cancel", body, bystander_token,
                         status=404) == "not_found"
    stale = {"expected_revision": workflow["revision"] + 4, "reason": "stale"}
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:cancel", stale, sender_token,
                         status=412) == "revision_conflict"
    cancelled = world.call("POST", f"conversation-requests/{workflow['id']}:cancel", body, sender_token,
                           key="cancel")
    assert cancelled["state"] == "cancelled" and cancelled["unanswered"] is False
    assert cancelled["cancel"]["requested_by"] == sender["id"]
    assert cancelled["cancel"]["reason"] == "requirement withdrawn"
    assert cancelled["cancel"]["notified"] is True and cancelled["cancel"]["message_id"]
    # Cancellation is a workflow record, not an acknowledgment of recipient processing.
    assert cancelled["message"]["delivery_status"] == "available"
    notices = [item for item in world.conversation(observer) if item["type"] == "cancel"]
    assert len(notices) == 1 and notices[0]["in_reply_to"] == workflow["message_id"]
    assert notices[0]["from"] == sender["id"] and notices[0]["to"] == recipient["id"]
    repeat = world.call("POST", f"conversation-requests/{workflow['id']}:cancel",
                        {**body, "expected_revision": cancelled["revision"]}, sender_token, key="cancel-2")
    assert repeat["cancel"]["message_id"] == cancelled["cancel"]["message_id"]
    assert len([item for item in world.conversation(observer) if item["type"] == "cancel"]) == 1

    # An answered request cannot be withdrawn, and a durable cancel message is observed by reconcile.
    answered = world.request(sender, recipient, deadline=60)
    closed = world.call("POST", f"conversation-requests/{answered['id']}:respond",
                        {"content": {"media_type": "text/plain", "text": "Already answered"}}, recipient_token)
    assert world.failure("POST", f"conversation-requests/{answered['id']}:cancel",
                         {"expected_revision": closed["revision"], "reason": "too late"}, sender_token,
                         status=409) == "request_already_answered"
    observed = world.request(sender, recipient, deadline=60)
    world.call("POST", "messages", world.reply_payload(observed, sender, recipient, type="cancel",
                                                       text="stop work"), sender_token)
    reconciled = world.call("POST", f"conversation-requests/{observed['id']}:reconcile", {}, sender_token)
    assert reconciled["state"] == "cancelled" and reconciled["reconciled"] is True
    assert reconciled["cancel"]["message_id"] and reconciled["cancel"]["notified"] is True


def test_one_time_escalation_of_overdue_requests_and_late_resolution(world, monkeypatch):
    sender, recipient, bystander = world.agent("Sender"), world.agent("Recipient"), world.agent("Bystander")
    sender_token, recipient_token = world.credential(sender), world.credential(recipient)
    bystander_token = world.credential(bystander)
    observer = world.credential(role="observer")
    run = world.run(recipient, recipient_token)
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    workflow = world.request(sender, recipient, deadline=5, ttl=3600)
    escalate = {"expected_revision": workflow["revision"], "reason": "no response in 5s"}
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:escalate", escalate, sender_token,
                         status=409) == "response_deadline_pending"
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:escalate", escalate,
                         recipient_token, status=403) == "forbidden"
    clock[0] += 6
    overdue = world.call("GET", f"conversation-requests/{workflow['id']}", token=sender_token)
    assert overdue["response_overdue"] is True and overdue["deadline_reached"] is True
    assert overdue["state"] == "open"
    escalated = world.call("POST", f"conversation-requests/{workflow['id']}:escalate",
                           {**escalate, "expected_revision": overdue["revision"]}, sender_token, key="escalate")
    assert escalated["state"] == "escalated" and escalated["unanswered"] is True
    record = escalated["escalation"]
    assert record["one_time"] is True and record["state"] == "open"
    assert record["reason"] == "no response in 5s" and record["escalate_to"] == recipient["id"]
    assert record["message_id"] and record["escalated_by"] == sender["id"]
    assert record["request_message_id"] == workflow["message_id"]
    listed = world.call("GET", f"conversation-escalations?project_id={world.project}&request_id={workflow['id']}",
                        token=sender_token)
    assert [item["id"] for item in listed] == [record["id"]]
    repeat = world.call("POST", f"conversation-requests/{workflow['id']}:escalate",
                        {**escalate, "expected_revision": escalated["revision"], "reason": "again"},
                        sender_token, key="escalate-2")
    assert repeat["escalation"]["id"] == record["id"] and repeat["escalation"]["reason"] == "no response in 5s"
    assert len(world.call("GET", f"conversation-escalations?project_id={world.project}&request_id="
                                 f"{workflow['id']}", token=observer)) == 1

    # A late answer resolves the workflow and the recorded escalation, still without an ack.
    answered = world.call("POST", f"conversation-requests/{workflow['id']}:respond",
                          {"content": {"media_type": "text/plain", "text": "Late answer"},
                           "type": "response", "run_id": run["id"]}, recipient_token)
    assert answered["state"] == "answered" and answered["message"]["delivery_status"] == "available"
    resolved = world.call("GET", f"conversation-escalations?project_id={world.project}&request_id="
                                 f"{workflow['id']}", token=sender_token)[0]
    assert resolved["state"] == "resolved" and resolved["answer_message_id"] == answered["answer_message_id"]

    # Escalating to a third party records the notice without widening workflow visibility.
    third = world.request(sender, recipient, deadline=5, ttl=3600)
    clock[0] += 6
    notice = world.call("POST", f"conversation-requests/{third['id']}:escalate",
                        {"expected_revision": third["revision"], "reason": "needs an owner",
                         "escalate_to": bystander["id"]}, sender_token)
    assert notice["escalation"]["escalate_to"] == bystander["id"]
    assert world.call("GET", f"messages/{notice['escalation']['message_id']}",
                      token=bystander_token)["to"] == bystander["id"]
    assert world.failure("GET", f"conversation-requests/{third['id']}", None, bystander_token,
                         status=404) == "not_found"
    assert [item["id"] for item in world.call("GET", f"conversation-escalations?project_id={world.project}",
                                              token=bystander_token)] == [notice["escalation"]["id"]]

    # Unknown and cross-project escalation targets reuse the existing agent reference check.
    invalid = world.request(sender, recipient, deadline=5, ttl=3600)
    clock[0] += 6
    assert world.failure("POST", f"conversation-requests/{invalid['id']}:escalate",
                         {"expected_revision": invalid["revision"], "reason": "owner",
                          "escalate_to": uid("agent")}, sender_token, status=404) == "not_found"
    assert world.failure("POST", f"conversation-requests/{invalid['id']}:escalate",
                         {"expected_revision": invalid["revision"], "reason": "self",
                          "escalate_to": sender["id"]}, sender_token,
                         status=422) == "invalid_escalation_target"

    # Withdrawing an escalated request resolves its record as cancelled, not answered.
    withdraw = world.request(sender, recipient, deadline=5, ttl=3600)
    clock[0] += 6
    escalated_withdraw = world.call("POST", f"conversation-requests/{withdraw['id']}:escalate",
                                    {"expected_revision": withdraw["revision"], "reason": "no owner"},
                                    sender_token)
    closed = world.call("POST", f"conversation-requests/{withdraw['id']}:cancel",
                        {"expected_revision": escalated_withdraw["revision"], "reason": "withdrawn"},
                        sender_token)
    assert closed["state"] == "cancelled"
    assert closed["escalation"]["state"] == "resolved"
    assert closed["escalation"]["resolution"] == "cancelled"
    assert closed["escalation"]["answer_message_id"] is None


def test_workflow_reads_reuse_participant_visibility_and_reconcile_ignores_noise(world):
    sender, recipient, bystander = world.agent("Sender"), world.agent("Recipient"), world.agent("Bystander")
    sender_token, recipient_token = world.credential(sender), world.credential(recipient)
    bystander_token = world.credential(bystander)
    observer, human = world.credential(role="observer"), world.credential(role="human")
    workflow = world.request(sender, recipient, deadline=60)
    detail = f"conversation-requests/{workflow['id']}"
    for token in (sender_token, recipient_token, observer):
        assert world.call("GET", detail, token=token)["id"] == workflow["id"]
    assert world.failure("GET", detail, None, bystander_token, status=404) == "not_found"
    assert world.failure("GET", detail, None, human, status=404) == "not_found"
    assert world.failure("GET", f"conversation-requests?project_id={world.project}", None, human,
                         status=403) == "conversation_observer_grant_required"
    listing = f"conversation-requests?project_id={world.project}"
    assert [item["id"] for item in world.call("GET", listing, token=bystander_token)] == []
    assert workflow["id"] in [item["id"] for item in world.call("GET", listing, token=observer)]
    assert [item["id"] for item in world.call("GET", f"{listing}&state=answered")] == []
    assert [item["id"] for item in world.call("GET", f"{listing}&state=open")] == [workflow["id"]]
    assert world.failure("GET", f"{listing}&state=stale", None, status=422) == "invalid_request"

    # Same-thread chatter that is not a participant reply is not an answer.
    world.call("POST", "messages", world.reply_payload(workflow, recipient, sender, type="inform"),
               recipient_token)
    world.call("POST", "messages", {
        "message_id": uid("msg"), "project_id": world.project, "from": recipient["id"],
        "to": sender["id"], "type": "inform", "thread_id": workflow["thread_id"],
        "correlation_id": workflow["correlation_id"],
        "content": {"media_type": "text/plain", "text": "unrelated chatter"}}, recipient_token)
    world.call("POST", "messages", world.reply_payload(workflow, recipient, sender, correlation=uid("corr")),
               recipient_token, status=422)
    scanned = world.call("POST", f"conversation-requests/{workflow['id']}:reconcile", {}, sender_token)
    assert scanned["state"] == "open" and scanned["reconciled"] is False
    assert world.failure("POST", f"conversation-requests/{workflow['id']}:reconcile",
                         {"expected_revision": workflow["revision"] + 2}, sender_token,
                         status=412) == "revision_conflict"
    answered = world.call("POST", f"conversation-requests/{workflow['id']}:reconcile",
                          {"expected_revision": scanned["revision"]}, sender_token)
    assert answered["state"] == "open"
    world.call("POST", "messages", world.reply_payload(workflow, recipient, sender, type="ack"),
               recipient_token)
    acked_reply = world.call("POST", f"conversation-requests/{workflow['id']}:reconcile", {}, sender_token)
    assert acked_reply["state"] == "answered" and acked_reply["reconciled"] is True


def test_request_creation_reuses_existing_message_validation(world):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    sender_token = world.credential(sender)
    bystander_token = world.credential(world.agent("Bystander"))
    base = {"project_id": world.project, "message_id": uid("msg"), "from_agent": sender["id"],
            "to": recipient["id"], "thread_id": uid("thread"), "ttl_seconds": 3600,
            "response_deadline_seconds": 60,
            "content": {"media_type": "text/plain", "text": "Status?"}}
    assert world.failure("POST", "conversation-requests", {**base, "type": "inform"}, sender_token,
                         status=422) == "invalid_request"
    assert world.failure("POST", "conversation-requests", {**base, "response_deadline_seconds": 0},
                         sender_token, status=422) == "invalid_request"
    assert world.failure("POST", "conversation-requests", {**base, "extra": 1}, sender_token,
                         status=422) == "invalid_request"
    assert world.failure("POST", "conversation-requests", {**base, "content": {
        "media_type": "application/vnd.hivemind.chat+json",
        "segments": [{"kind": "text", "text": "Status?"}]}}, sender_token, status=422) == "invalid_request"
    assert world.failure("POST", "conversation-requests", base, bystander_token, status=403) == "forbidden"
    assert world.failure("POST", "conversation-requests", {**base, "to": uid("agent")}, sender_token,
                         status=404) == "not_found"
    clamped = world.call("POST", "conversation-requests",
                         {**base, "ttl_seconds": 10, "response_deadline_seconds": 60}, sender_token)
    assert clamped["response_deadline"] == clamped["message"]["expires_at"]
    repeat = world.call("POST", "conversation-requests",
                        {**base, "ttl_seconds": 10, "response_deadline_seconds": 60}, sender_token)
    assert repeat["id"] == clamped["id"] and repeat["revision"] == clamped["revision"]
    assert world.failure("POST", "conversation-requests",
                         {**base, "ttl_seconds": 20, "response_deadline_seconds": 60}, sender_token,
                         status=409) == "idempotency_conflict"


def test_workflow_and_escalation_records_survive_a_fresh_canonical_connection(world, monkeypatch):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    sender_token = world.credential(sender)
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    workflow = world.request(sender, recipient, deadline=5)
    clock[0] += 6
    escalated = world.call("POST", f"conversation-requests/{workflow['id']}:escalate",
                           {"expected_revision": workflow["revision"], "reason": "unanswered"}, sender_token)
    restored = Store(world.app.state.store.directory)
    try:
        tracked = json.loads(restored.raw(workflow["id"], conversations.REQUEST_KIND)["body"])
        assert tracked["state"] == "escalated"
        assert tracked["escalation_id"] == escalated["escalation"]["id"]
        assert tracked["message_id"] == workflow["message_id"]
        record = json.loads(restored.raw(escalated["escalation"]["id"], conversations.ESCALATION_KIND)["body"])
        assert record["one_time"] is True and record["request_id"] == workflow["id"]
        assert record["state"] == "open" and record["message_id"]
    finally:
        restored.close()


def test_python_helper_bounds_polling_reply_and_one_time_escalation(world, monkeypatch):
    import httpx

    from hivemind.sdk import Client

    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    sender_token, recipient_token = world.credential(sender), world.credential(recipient)
    run = world.run(recipient, recipient_token)

    def forward(request):
        response = world.client.request(request.method, request.url.raw_path.decode(),
                                        content=request.content, headers=dict(request.headers))
        return httpx.Response(response.status_code, headers=dict(response.headers), content=response.content)

    def client_for(token):
        client = Client("http://testserver", token)
        client.http.close()
        client.http = httpx.Client(base_url="http://testserver/v0.1/",
                                   headers={"Authorization": f"Bearer {token}"},
                                   transport=httpx.MockTransport(forward))
        return client

    virtual = [time.time()]
    monkeypatch.setattr(time, "time", lambda: virtual[0])

    def sleep(seconds):
        virtual[0] += seconds

    sender_client, recipient_client = client_for(sender_token), client_for(recipient_token)
    sender_helper = conversations.ConversationWorkflow(sender_client, world.project, sender["id"],
                                                       sleep=sleep, clock=lambda: virtual[0])
    recipient_helper = conversations.ConversationWorkflow(recipient_client, world.project, recipient["id"],
                                                          run_id=run["id"], sleep=sleep,
                                                          clock=lambda: virtual[0])
    try:
        opened = sender_helper.open_request(recipient["id"], "Confirm the response schema",
                                           deadline_seconds=30)
        assert opened["state"] == "open" and "sender_run_id" not in opened["message"]
        replies = []

        def sleep_then_reply(seconds):
            sleep(seconds)
            if not replies:
                replies.append(recipient_helper.reply(opened["id"], "Confirmed: keep expires_at"))

        sender_helper.sleep = sleep_then_reply
        answered = sender_helper.wait_for_reply(opened["id"], timeout_seconds=30, interval_seconds=5)
        assert answered["wait_result"] == "answered" and answered["state"] == "answered"
        assert answered["polls"] == 2 and len(replies) == 1
        assert answered["answer_message_id"]
        assert sender_helper.read_request(opened["id"])["state"] == "answered"

        # A bounded wait never blocks past its own timeout, and escalation happens at most once.
        waiting = sender_helper.open_request(recipient["id"], "Still waiting", deadline_seconds=5)
        virtual[0] += 6
        timed_out = sender_helper.wait_for_reply(waiting["id"], timeout_seconds=10, interval_seconds=5)
        assert timed_out["wait_result"] == "timeout" and timed_out["polls"] == 3
        assert timed_out["state"] == "open"
        escalated = sender_helper.escalate_if_overdue(waiting["id"], reason="no reply in 10s")
        assert escalated["escalated"] is True and escalated["state"] == "escalated"
        assert escalated["escalation"]["reason"] == "no reply in 10s"
        assert sender_helper.escalate_if_overdue(waiting["id"])["escalated"] is False
        assert len(sender_helper.client.request(
            "GET", f"conversation-escalations?project_id={world.project}&request_id={waiting['id']}")) == 1

        message = world.message(sender, recipient)
        receipt = world.claim(message, run, recipient_token)
        renewed = recipient_helper.renew_lease(message["id"], receipt["delivery_id"], ttl_seconds=60)
        assert renewed["delivery_id"] == receipt["delivery_id"]
        assert renewed["claim_until"] == virtual[0] + 60
        with pytest.raises(ValueError):
            sender_helper.renew_lease(message["id"], receipt["delivery_id"])
    finally:
        sender_client.close()
        recipient_client.close()
