"""Tracked agent conversation workflows.

This module adds a durable *workflow* layer on top of the existing direct-message
delivery domain: tracked requests with response deadlines, reply helpers, cancellation
records, fenced delivery lease renewal, and one-time escalation of unanswered requests.

Delivery state (``claim``/``ack``/``fail``, the immutable ``delivery_id``/``claim_until``,
and the original ``ttl_seconds``/``expires_at``) stays owned by
:class:`hivemind.service.Service`. Workflow state is persisted separately and never
acknowledges, retries, or impersonates a recipient. Message content is evidence, never
instructions. Route wiring is registered by :func:`install`; ``api.py`` owns that call.

Bounded by design: no scheduler, no paid agent session, no network egress. Response
deadlines and escalations are evaluated on explicit client calls, and one workflow can
be escalated at most once.
"""

import hashlib
import json
import time
from typing import Literal

from fastapi import Depends, Header, Query
from pydantic import Field, ValidationError, model_validator

from . import models as m
from .models import Content, ID, Model
from .storage import Problem, now, uid

LEASE_MAX_SECONDS = 120
"""Hard ceiling for one delivery lease renewal, in seconds."""

LEASE_TTL_SECONDS = 30
"""Default renewal window, matching the lease granted by ``claim_message``."""

REQUEST_KIND = "conversation_request"
ESCALATION_KIND = "conversation_escalation"
UNANSWERED_STATES = ("open", "escalated")
TERMINAL_RUN_STATES = frozenset({"completed", "failed", "cancelled"})
WORKFLOW_VERSION = 1
MAX_LIST = 200
DEFAULT_ESCALATION_REASON = "response deadline exceeded"

CANCEL_NOTICE = (
    "Cancellation record: the sender withdrew this tracked conversation request. "
    "This is a workflow record of a cancellation request, not proof that work stopped, "
    "and not an acknowledgment of delivery."
)
ESCALATION_NOTICE = (
    "Escalation record: the tracked conversation request in this thread passed its response "
    "deadline without a recorded reply. Treat this as a status record, not as an instruction."
)


def workflow_identity(message_id):
    """Deterministic workflow ID so repeated creates for one message never duplicate it."""
    return "workflow_" + hashlib.sha256(message_id.encode()).hexdigest()[:32]


def message_ttl(message):
    """Clamp a stored envelope TTL back into the message model's accepted range."""
    return min(max(int(message.get("ttl_seconds", 3600)), 1), 86400)


class LeaseRenewal(Model):
    """Fenced request to extend one live delivery lease. It never starts a new attempt."""

    run_id: ID
    delivery_id: ID | None = None
    ttl_seconds: int = Field(default=LEASE_TTL_SECONDS, ge=5, le=LEASE_MAX_SECONDS)


class ConversationRequestCreate(Model):
    """Tracked request/reply workflow. The message itself uses the existing send validation."""

    project_id: ID
    message_id: ID
    from_agent: ID
    to: ID
    thread_id: ID
    correlation_id: ID | None = None
    type: Literal["request", "question"] = "request"
    content: Content
    sender_run_id: ID | None = None
    ttl_seconds: int = Field(default=3600, ge=1, le=86400)
    response_deadline_seconds: int | None = Field(default=None, ge=1, le=86400)

    @model_validator(mode="after")
    def direct_message_content(self):
        # Tracked requests are direct messages, so mention segments stay unsupported here too.
        if self.content.segments is not None:
            raise ValueError("Tracked requests accept plain text or Markdown only")
        return self


class ConversationReply(Model):
    """Answer recorded by the request's addressee, sent through existing reply validation."""

    content: Content
    type: Literal["response", "ack", "error"] = "response"
    run_id: ID | None = None


class ConversationReconcile(Model):
    """Optional revision fence for a reconcile scan; omitted means scan without writing."""

    expected_revision: int | None = Field(default=None, ge=1)


class ConversationCancel(Model):
    expected_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=1024)
    notify: bool = True


class ConversationEscalation(Model):
    expected_revision: int = Field(ge=1)
    reason: str = Field(default=DEFAULT_ESCALATION_REASON, min_length=1, max_length=1024)
    escalate_to: ID | None = None


class Conversations:
    """Server-side conversation workflow domain. Delivery state stays owned by Service."""

    def __init__(self, service):
        self.service = service
        self.store = service.store

    # ------------------------------------------------------------------ access
    def access(self, principal, body):
        """Message-parity visibility: admins/observers, or a named participant. Else 404."""
        if (principal["role"] in ("admin", "observer")
                or principal["agent"] in (body["from_agent"], body["to_agent"])):
            return body
        raise Problem("not_found", 404)

    def workflow(self, principal, request_id):
        return self.access(principal, self.store.get(request_id, principal, REQUEST_KIND))

    def deadline_reached(self, workflow, at=None):
        deadline = workflow.get("response_deadline")
        if deadline is None:
            return False
        return (time.time() if at is None else at) >= deadline

    def view(self, principal, body):
        """Workflow state plus read-time derived deadline flags and separate delivery state."""
        message = None
        try:
            message = self.service.delivery_state(self.store.get(body["message_id"], principal, "message"))
        except Problem:
            message = None
        escalation = None
        if body.get("escalation_id"):
            try:
                escalation = self.store.get(body["escalation_id"], principal, ESCALATION_KIND)
            except Problem:
                escalation = None
        overdue = self.deadline_reached(body)
        unanswered = body["state"] in UNANSWERED_STATES
        return {**body, "workflow_state": body["state"], "unanswered": unanswered,
                "deadline_reached": overdue, "response_overdue": overdue and unanswered,
                "message": message, "message_unavailable": message is None,
                "escalation": escalation}

    # ---------------------------------------------------------------- lifecycle
    def create(self, principal, request):
        """Persist the durable request message and its tracked workflow record."""
        self.service.project_access(principal, request.project_id)
        try:
            message_request = m.MessageCreate(
                project_id=request.project_id, message_id=request.message_id, to=request.to,
                sender_run_id=request.sender_run_id, type=request.type, thread_id=request.thread_id,
                correlation_id=request.correlation_id, content=request.content,
                ttl_seconds=request.ttl_seconds, **{"from": request.from_agent})
        except ValidationError:
            # Direct-message rules stay authoritative. Never echo rejected field values.
            raise Problem("invalid_request", 422) from None
        message = self.service.message(principal, message_request)
        identity = workflow_identity(message["id"])
        existing = self.store.db.execute(
            "SELECT id FROM resources WHERE id=? AND deleted=0", (identity,)).fetchone()
        if existing:
            return self.view(principal, self.workflow(principal, identity))
        deadline = (min(time.time() + request.response_deadline_seconds, message["expires_at"])
                    if request.response_deadline_seconds is not None else None)
        body = self.store.save(REQUEST_KIND, request.project_id, principal["id"], {
            "id": identity, "protocol": "hivemind", "protocol_version": "0.1.0",
            "created_at": now(), "workflow_version": WORKFLOW_VERSION,
            "project_id": request.project_id, "message_id": message["id"],
            "from_agent": message["from"], "to_agent": message["to"],
            "thread_id": message["thread_id"], "correlation_id": message["correlation_id"],
            "type": message["type"], "state": "open",
            "response_deadline": deadline,
            "response_deadline_seconds": request.response_deadline_seconds,
            "answer_message_id": None, "answered_at": None, "cancel": None,
            "escalation_id": None, "escalated_at": None, "reconciled_at": None,
        })
        self.store.relate(request.project_id, identity, message["id"], "TRACKS_REQUEST")
        return self.view(principal, body)

    def read(self, principal, request_id):
        return self.view(principal, self.workflow(principal, request_id))

    def list_requests(self, principal, project, state=None, limit=MAX_LIST):
        self.service.conversation_access(principal, project)
        items = []
        for body in self.store.list(REQUEST_KIND, project, principal, limit):
            if (principal["role"] not in ("admin", "observer")
                    and principal["agent"] not in (body["from_agent"], body["to_agent"])):
                continue
            if state and body["state"] != state:
                continue
            items.append(self.view(principal, body))
        return items

    def list_escalations(self, principal, project, request_id=None, limit=MAX_LIST):
        self.service.conversation_access(principal, project)
        items = []
        for body in self.store.list(ESCALATION_KIND, project, principal, limit):
            if request_id and body["request_id"] != request_id:
                continue
            if (principal["role"] not in ("admin", "observer")
                    and principal["agent"] not in (body["from_agent"], body["to_agent"], body["escalate_to"])):
                continue
            items.append(body)
        return items

    def respond(self, principal, request_id, request):
        """Reply helper: only the addressee answers, and existing thread rules still apply."""
        workflow = self.workflow(principal, request_id)
        self.service.represent(principal, workflow["to_agent"])
        if workflow["state"] not in UNANSWERED_STATES:
            raise Problem("request_closed")
        message = self.store.get(workflow["message_id"], principal, "message")
        created = self.service.message(principal, m.MessageCreate(
            project_id=workflow["project_id"], message_id=uid("msg"), to=workflow["from_agent"],
            sender_run_id=request.run_id, type=request.type, thread_id=workflow["thread_id"],
            correlation_id=workflow["correlation_id"], in_reply_to=workflow["message_id"],
            content=request.content, ttl_seconds=message_ttl(message),
            **{"from": workflow["to_agent"]}))
        workflow.update(state="answered", answer_message_id=created["id"], answered_at=now(),
                        reconciled_at=now())
        row = self.store.raw(request_id)
        saved = self.store.save(REQUEST_KIND, row["project"], row["owner"], workflow,
                                expected=workflow["revision"])
        self.resolve_escalation(saved)
        self.store.relate(saved["project_id"], request_id, created["id"], "ANSWERED_BY")
        return self.view(principal, saved)

    def reconcile(self, principal, request_id, request=None):
        """Scan already-visible thread traffic and persist the first answer or cancel observed.

        Matching requires the same thread, the same correlation, the workflow's participants,
        and a reply to the tracked message. Reconciling never widens message visibility.
        """
        workflow = self.workflow(principal, request_id)
        expected = getattr(request, "expected_revision", None)
        if expected is not None and expected != workflow["revision"]:
            raise Problem("revision_conflict", 412)
        if workflow["state"] in ("answered", "cancelled"):
            return {**self.view(principal, workflow), "reconciled": False}
        replies, cancels = [], []
        for candidate in self.store.list("message", workflow["project_id"], principal, 1000):
            if (candidate["thread_id"] != workflow["thread_id"]
                    or candidate["correlation_id"] != workflow["correlation_id"]
                    or candidate.get("in_reply_to") != workflow["message_id"]
                    or {candidate["from"], candidate["to"]} != {workflow["from_agent"], workflow["to_agent"]}):
                continue
            if candidate["from"] == workflow["to_agent"] and candidate["type"] in ("response", "ack", "error"):
                replies.append(candidate)
            elif candidate["from"] == workflow["from_agent"] and candidate["type"] == "cancel":
                cancels.append(candidate)
        if not replies and not cancels:
            return {**self.view(principal, workflow), "reconciled": False}
        if replies:
            answer = min(replies, key=lambda item: item["ingestion_seq"])
            workflow.update(state="answered", answer_message_id=answer["id"], answered_at=now(),
                            reconciled_at=now())
            relation = "ANSWERED_BY"
        else:
            notice = min(cancels, key=lambda item: item["ingestion_seq"])
            workflow.update(state="cancelled", reconciled_at=now(), cancel={
                "requested_at": notice["created_at"], "requested_by": notice["from"],
                "reason": "observed cancel message", "message_id": notice["id"], "notified": True})
            relation = "CANCELLED_BY"
        row = self.store.raw(request_id)
        saved = self.store.save(REQUEST_KIND, row["project"], row["owner"], workflow,
                                expected=workflow["revision"])
        if replies:
            self.resolve_escalation(saved)
        else:
            self.resolve_escalation(saved, "cancelled")
        self.store.relate(saved["project_id"], request_id,
                          saved["answer_message_id"] or saved["cancel"]["message_id"], relation)
        return {**self.view(principal, saved), "reconciled": True}

    def cancel(self, principal, request_id, request):
        """Record the sender withdrawing its own request. Never sent in the target's name."""
        workflow = self.workflow(principal, request_id)
        self.service.represent(principal, workflow["from_agent"])
        if workflow["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if workflow["state"] == "answered":
            raise Problem("request_already_answered")
        if workflow["state"] == "cancelled":
            return self.view(principal, workflow)
        notice_id = None
        if request.notify:
            message = self.store.get(workflow["message_id"], principal, "message")
            notice_id = self.service.message(principal, m.MessageCreate(
                project_id=workflow["project_id"], message_id=uid("msg"), to=workflow["to_agent"],
                type="cancel", thread_id=workflow["thread_id"],
                correlation_id=workflow["correlation_id"], in_reply_to=workflow["message_id"],
                content=Content(media_type="text/plain", text=CANCEL_NOTICE),
                ttl_seconds=message_ttl(message), **{"from": workflow["from_agent"]}))["id"]
        workflow.update(state="cancelled", reconciled_at=now(), cancel={
            "requested_at": now(), "requested_by": workflow["from_agent"],
            "requested_by_principal": principal["id"], "reason": request.reason,
            "message_id": notice_id, "notified": notice_id is not None})
        row = self.store.raw(request_id)
        saved = self.store.save(REQUEST_KIND, row["project"], row["owner"], workflow,
                                expected=request.expected_revision)
        self.resolve_escalation(saved, "cancelled")
        if notice_id:
            self.store.relate(saved["project_id"], request_id, notice_id, "CANCELLED_BY")
        return self.view(principal, saved)

    def escalate(self, principal, request_id, request):
        """Persist one escalation record for an overdue request. Repeat calls never duplicate it."""
        workflow = self.workflow(principal, request_id)
        self.service.represent(principal, workflow["from_agent"])
        if workflow["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if workflow.get("escalation_id"):
            return self.view(principal, workflow)
        if workflow["state"] not in UNANSWERED_STATES:
            raise Problem("request_closed")
        if not self.deadline_reached(workflow):
            raise Problem("response_deadline_pending")
        target = request.escalate_to or workflow["to_agent"]
        if target == workflow["from_agent"]:
            raise Problem("invalid_escalation_target", 422)
        self.service.reference(principal, workflow["project_id"], target, "agent")
        same_thread = target in (workflow["from_agent"], workflow["to_agent"])
        notice = self.service.message(principal, m.MessageCreate(
            project_id=workflow["project_id"], message_id=uid("msg"), to=target, type="inform",
            thread_id=workflow["thread_id"], correlation_id=workflow["correlation_id"],
            in_reply_to=workflow["message_id"] if same_thread else None,
            content=Content(media_type="text/plain", text=ESCALATION_NOTICE),
            ttl_seconds=3600, **{"from": workflow["from_agent"]}))
        escalation = self.store.save(ESCALATION_KIND, workflow["project_id"], principal["id"], {
            "id": uid("escalation"), "protocol": "hivemind", "protocol_version": "0.1.0",
            "created_at": now(), "project_id": workflow["project_id"], "request_id": request_id,
            "request_message_id": workflow["message_id"], "thread_id": workflow["thread_id"],
            "correlation_id": workflow["correlation_id"], "from_agent": workflow["from_agent"],
            "to_agent": workflow["to_agent"], "escalate_to": target, "reason": request.reason,
            "escalated_by": workflow["from_agent"], "escalated_by_principal": principal["id"],
            "escalated_at": now(), "response_deadline": workflow["response_deadline"],
            "state": "open", "message_id": notice["id"], "resolved_at": None,
            "answer_message_id": None, "one_time": True,
        })
        workflow.update(state="escalated", escalation_id=escalation["id"],
                        escalated_at=escalation["escalated_at"])
        row = self.store.raw(request_id)
        saved = self.store.save(REQUEST_KIND, row["project"], row["owner"], workflow,
                                expected=request.expected_revision)
        self.store.relate(saved["project_id"], request_id, escalation["id"], "ESCALATED_AS")
        return self.view(principal, saved)

    def resolve_escalation(self, workflow, resolution="answered"):
        """Mark a recorded one-time escalation resolved by a later recorded answer or cancellation."""
        escalation_id = workflow.get("escalation_id")
        if not escalation_id:
            return None
        try:
            row = self.store.raw(escalation_id, ESCALATION_KIND)
        except Problem:
            return None
        body = json.loads(row["body"])
        if body.get("state") == "resolved":
            return body
        body.update(state="resolved", resolved_at=now(), resolution=resolution,
                    answer_message_id=workflow.get("answer_message_id"))
        return self.store.save(ESCALATION_KIND, row["project"], row["owner"], body)

    # ------------------------------------------------------------- lease renewal
    def renew_lease(self, principal, message_id, request):
        """Extend one live delivery lease in place, fenced by run and immutable receipt.

        A renewal never changes ``state``, ``attempt_count``, or ``delivery_id``, never
        acknowledges, never revives an expired or replaced lease, and never extends past the
        original message expiry. Each grant adds at most ``LEASE_MAX_SECONDS``.
        """
        message = self.store.get(message_id, principal, "message")
        run = self.service.run(principal, request.run_id)
        if run["agent_id"] != message["to"] or run["project_id"] != message["project_id"]:
            raise Problem("not_found", 404)
        if run["status"] in TERMINAL_RUN_STATES:
            raise Problem("terminal_run")
        if message["claim_run_id"] != request.run_id or (
                request.delivery_id is not None and request.delivery_id != message.get("delivery_id")):
            raise Problem("delivery_receipt_stale")
        current = time.time()
        if (message["state"] != "delivered" or message["claim_until"] <= current
                or self.service.delivery_state(message)["delivery_status"] != "leased"):
            raise Problem("delivery_lease_expired")
        granted = min(request.ttl_seconds, LEASE_MAX_SECONDS)
        ceiling = min(message["expires_at"], current + granted)
        if ceiling <= message["claim_until"]:
            # Renewal never shrinks or replaces a live lease; the recorded receipt is unchanged.
            return self.service.delivery_state(message)
        message["claim_until"] = ceiling
        return self.service.delivery_save(message, "lease_renewed",
                                          ttl_seconds=round(ceiling - current, 3))


def install(app, service, principal, command, envelope):
    """Register conversation workflow routes on an app built by ``create_app``.

    ``principal``, ``command``, and ``envelope`` are the existing closures from
    ``hivemind.api``. Returns the domain object for programmatic use.
    """
    workflows = Conversations(service)

    @app.post("/v1/conversation-requests")
    async def conversation_request_create(body: ConversationRequestCreate, p=Depends(principal),
                                         idempotency_key: str | None = Header(None)):
        return command(p, "conversation-requests", idempotency_key, body,
                       lambda: workflows.create(p, body))

    @app.get("/v1/conversation-requests")
    async def conversation_requests(project_id: str,
                                    state: str | None = Query(None, pattern="^(open|answered|cancelled|escalated)$"),
                                    limit: int = Query(MAX_LIST, ge=1, le=MAX_LIST),
                                    p=Depends(principal)):
        return envelope(workflows.list_requests(p, project_id, state, limit))

    @app.get("/v1/conversation-requests/{request_id}")
    async def conversation_request(request_id: str, p=Depends(principal)):
        return envelope(workflows.read(p, request_id))

    @app.post("/v1/conversation-requests/{request_id}:respond")
    async def conversation_respond(request_id: str, body: ConversationReply, p=Depends(principal),
                                   idempotency_key: str | None = Header(None)):
        return command(p, "conversation-respond:" + request_id, idempotency_key, body,
                       lambda: workflows.respond(p, request_id, body))

    @app.post("/v1/conversation-requests/{request_id}:reconcile")
    async def conversation_reconcile(request_id: str, body: ConversationReconcile, p=Depends(principal),
                                     idempotency_key: str | None = Header(None)):
        return command(p, "conversation-reconcile:" + request_id, idempotency_key, body,
                       lambda: workflows.reconcile(p, request_id, body))

    @app.post("/v1/conversation-requests/{request_id}:cancel")
    async def conversation_cancel(request_id: str, body: ConversationCancel, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return command(p, "conversation-cancel:" + request_id, idempotency_key, body,
                       lambda: workflows.cancel(p, request_id, body))

    @app.post("/v1/conversation-requests/{request_id}:escalate")
    async def conversation_escalate(request_id: str, body: ConversationEscalation, p=Depends(principal),
                                    idempotency_key: str | None = Header(None)):
        return command(p, "conversation-escalate:" + request_id, idempotency_key, body,
                       lambda: workflows.escalate(p, request_id, body))

    @app.get("/v1/conversation-escalations")
    async def conversation_escalations(project_id: str, request_id: str | None = None,
                                       limit: int = Query(MAX_LIST, ge=1, le=MAX_LIST),
                                       p=Depends(principal)):
        return envelope(workflows.list_escalations(p, project_id, request_id, limit))

    @app.post("/v1/messages/{message_id}:renew")
    async def message_renew(message_id: str, body: LeaseRenewal, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "message-renew:" + message_id, idempotency_key, body,
                       lambda: workflows.renew_lease(p, message_id, body))

    return workflows


class ConversationWorkflow:
    """Bounded request/wait/reply helper for any client exposing ``request(method, path, body, key)``.

    ``sleep`` and ``clock`` are injected so polling is deterministic and never blocks on wall
    time. The helper never acknowledges delivery, never renews a lease it does not hold, and
    never escalates twice for one request.
    """

    def __init__(self, client, project_id, agent_id, run_id=None, sleep=time.sleep, clock=time.time):
        self.client = client
        self.project_id = project_id
        self.agent_id = agent_id
        self.run_id = run_id
        self.sleep = sleep
        self.clock = clock

    def open_request(self, to, text, thread_id=None, deadline_seconds=120, kind="request",
                     media_type="text/plain", ttl_seconds=3600, key=None):
        body = {"project_id": self.project_id, "message_id": uid("msg"), "from_agent": self.agent_id,
                "to": to, "thread_id": thread_id or uid("thread"), "type": kind,
                "content": {"media_type": media_type, "text": text}, "ttl_seconds": ttl_seconds}
        if deadline_seconds is not None:
            body["response_deadline_seconds"] = deadline_seconds
        if self.run_id:
            body["sender_run_id"] = self.run_id
        return self.client.request("POST", "conversation-requests", body, key=key)

    def read_request(self, request_id):
        return self.client.request("GET", f"conversation-requests/{request_id}")

    def reconcile(self, request_id, expected_revision=None, key=None):
        body = {} if expected_revision is None else {"expected_revision": expected_revision}
        return self.client.request("POST", f"conversation-requests/{request_id}:reconcile", body, key=key)

    def reply(self, request_id, text, kind="response", media_type="text/plain", key=None):
        body = {"content": {"media_type": media_type, "text": text}, "type": kind}
        if self.run_id:
            body["run_id"] = self.run_id
        return self.client.request("POST", f"conversation-requests/{request_id}:respond", body, key=key)

    def cancel(self, request_id, reason, expected_revision=None, notify=True, key=None):
        body = {"expected_revision": self._revision(request_id, expected_revision),
                "reason": reason, "notify": notify}
        return self.client.request("POST", f"conversation-requests/{request_id}:cancel", body, key=key)

    def escalate(self, request_id, reason=DEFAULT_ESCALATION_REASON, expected_revision=None,
                 escalate_to=None, key=None):
        body = {"expected_revision": self._revision(request_id, expected_revision), "reason": reason}
        if escalate_to:
            body["escalate_to"] = escalate_to
        return self.client.request("POST", f"conversation-requests/{request_id}:escalate", body, key=key)

    def escalate_if_overdue(self, request_id, reason=DEFAULT_ESCALATION_REASON, key=None):
        """Escalate once when the recorded deadline passed; otherwise report no escalation."""
        state = self.read_request(request_id)
        if state["state"] != "open" or not state.get("response_overdue"):
            return {**state, "escalated": False}
        result = self.escalate(request_id, reason=reason, expected_revision=state["revision"], key=key)
        return {**result, "escalated": True}

    def renew_lease(self, message_id, delivery_id, ttl_seconds=LEASE_TTL_SECONDS, key=None):
        if not self.run_id:
            raise ValueError("A run_id is required to renew a delivery lease")
        return self.client.request("POST", f"messages/{message_id}:renew", {
            "run_id": self.run_id, "delivery_id": delivery_id, "ttl_seconds": ttl_seconds}, key=key)

    def wait_for_reply(self, request_id, timeout_seconds=60, interval_seconds=5):
        """Reconcile on a bounded poll loop; returns ``wait_result`` of answered/cancelled/timeout."""
        polls_max = max(1, min(1000, int(timeout_seconds // max(interval_seconds, 1)) + 1))
        deadline = self.clock() + timeout_seconds
        state = None
        for index in range(polls_max):
            state = self.reconcile(request_id, key=uid("poll"))
            if state["state"] in ("answered", "cancelled"):
                return {**state, "wait_result": state["state"], "polls": index + 1}
            if self.clock() >= deadline:
                break
            self.sleep(interval_seconds)
        return {**state, "wait_result": "timeout", "polls": index + 1}

    def _revision(self, request_id, expected_revision):
        return expected_revision if expected_revision is not None else self.read_request(request_id)["revision"]
