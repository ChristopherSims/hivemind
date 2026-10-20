"""Recipient-accepted structured handoffs between two agents on canonical state.

This module adds a durable ``structured_handoff`` resource alongside the existing
``Scheduling.handoff`` semantics (``hivemind.scheduling``). The two are distinct:

* ``Scheduling.handoff`` is an administrator/human ownership action that requeues a
  task and archives its current execution. It is unchanged.
* A *structured handoff* here is a canonical proposal FROM the agent currently
  executing a task TO another agent. An offer never transfers ownership, never
  releases claims, and never declares a runtime quiescent. Acceptance is an
  explicit, fenced, durable recipient decision recorded on the offer itself.

Hardening contract:

* **Exact canonical binding.** An offer pins the exact sender
  project/agent/run/task/execution plus the task revision at offer time. Context
  references, deliverables, and checkpoint evidence refs carry exact
  kind/id/revision pins that are re-authorized (visible, same project, exact
  current revision) at offer against the OFFERING principal, at every read against
  the READING principal, at accept against the RECIPIENT principal, and at every
  consumption against the CONSUMING run's principal. Checkpoint evidence refs
  without an exact revision refuse the offer; they are never silently dropped.
* **Visibility is restricted, not project-wide.** Although the record is a
  standard project resource, views expose context payload (summary detail,
  checkpoint, context refs, deliverables, unresolved questions, decision reason)
  ONLY to the exact sender agent, the exact recipient agent, or an administrator,
  and ONLY while every pinned source is still visible to that exact viewer. Any
  stale or invisible source suppresses the context payload in the view; other
  project members receive a blind identity/state view, and freshness reasons are
  generic codes that never name or enumerate private resources.
* **Sanitized checkpoint, never raw reasoning.** The transferred checkpoint is
  derived server-side from the canonical task execution (summary, progress,
  pinned evidence refs), never client-supplied. Credential-shaped content (per
  ``hivemind.recovery.looks_sensitive``) in the checkpoint, summary, notes,
  questions, or decision reasons refuses the operation. Facts and refs are
  transferred; hidden reasoning, credentials, and vendor context are not stored.
* **Acceptance is new durable state, not a mutation of the task.** Accept/reject
  require the exact current recipient run fence (live run, same project, owned by
  the recipient agent, matching any offer-pinned recipient run), the exact offer
  revision, a fresh capability check, and a fresh source-snapshot check. Accepting
  NEVER changes task status, run assignment, work claims, or executions.
* **Settled means canonically settled, never heartbeat-settled.** For
  ``transfer="ownership"`` offers the accept additionally requires an explicit
  opt-in policy (default OFF) AND verified settled-execution evidence: the exact
  source execution must have ENDED with a canonical terminal checkpoint (task
  ``completed``/``failed`` with the execution's recorded checkpoint still
  available, or an archived execution with a canonical terminal outcome and
  checkpoint). A stale/absent heartbeat, a terminal run row, a paused run, a
  missing/deleted/unknown source, or a still-executing source is NEVER external
  quiescence proof and HOLDS the accept (``source_effects_active``). A pending
  ``Scheduling.handoff`` record is administrator INTENT, not proof of settled
  source effects: it authorizes accepting the context across the moved task
  snapshot, but the ownership request is labeled ``intent_only_unverified`` and
  remains unsupported unless settled-execution evidence also exists. Even with
  evidence, acceptance only records recipient intent; ownership moves exclusively
  through the existing ``tasks/{id}:handoff`` + dispatch machinery.
* **Stable idempotency, reauthorization on every replay.** All mutations go
  through ``Service.mutate`` with operation-scoped idempotency keys; the routes
  then re-read/re-run the domain logic so EVERY response — including command
  replays — is reauthorized against current visibility. Revocation (pending, or
  accepted-but-unconsumed, by the sender or an administrator) durably scrubs ALL
  context payload including ref/deliverable identities and checkpoint metadata;
  late readers and replays see the revoked, context-free record.
* **Replayable consume-once delivery.** Consumption binds the accepted context to
  the EXACT recipient run and the EXACT current task execution. A retry with the
  same Idempotency-Key replays the canonical command receipt, and a retry with a
  new key re-runs the fenced delivery idempotently (same run + same execution
  returns the same context again, reauthorized fresh; a different run or execution
  fails with ``handoff_consumed_elsewhere``). A lost response therefore never
  loses the context, and consumption is never untracked.

Nothing here starts paid work, calls a model or host, touches remotes, or commits
anything outside the canonical store.
"""

from __future__ import annotations

import copy
import json
from typing import Annotated, Literal

from fastapi import Depends, Header
from pydantic import Field, field_validator, model_validator

from . import models as m
from .models import ID
from .recovery import looks_sensitive
from .scheduling import Capability, Scheduling, strip_list, strip_text
from .storage import Problem, now

HANDOFF_KIND = "structured_handoff"
MAX_CONTEXT_REFS = 16
MAX_DELIVERABLES = 16
MAX_QUESTIONS = 8
QUESTION_MAX_LENGTH = 1024
DEFAULT_MAX_OPEN_OFFERS_PER_TASK = 8
LIVE_RUN_STATES = ("running", "idle")
STATES = ("pending", "accepted", "rejected", "revoked")
TERMINAL_TASK_STATES = ("completed", "failed", "cancelled")
SETTLED_TASK_STATES = ("completed", "failed")
SETTLED_OUTCOMES = ("succeeded", "failed")
CONTEXT_FIELDS = ("checkpoint", "context_refs", "deliverables", "unresolved_questions")
BLIND_FIELDS = (
    "id", "project_id", "task_id", "task_revision", "sender_agent_id", "source_run_id",
    "source_execution_id", "recipient_agent_id", "recipient_run_id", "transfer", "state",
    "created_at", "revision", "updated_at", "ingestion_seq",
)

MODELS = ("HandoffOffer", "HandoffAccept", "HandoffReject", "HandoffRevoke", "RunTaskContext")

__all__ = [
    "HANDOFF_KIND",
    "MODELS",
    "OPERATION_DESCRIPTORS",
    "STATES",
    "HandoffAccept",
    "HandoffDeliverable",
    "HandoffOffer",
    "HandoffReject",
    "HandoffRevoke",
    "PinnedRef",
    "RunTaskContext",
    "StructuredHandoffPolicy",
    "StructuredHandoffs",
    "consume_accepted_context",
    "descriptors",
    "install",
]

Question = Annotated[str, Field(min_length=1, max_length=QUESTION_MAX_LENGTH)]


class StructuredHandoffPolicy:
    """Operator opt-in for effectful handoff behavior. Transfer is default-off."""

    def __init__(self, *, allow_ownership_transfer=False,
                 max_open_offers_per_task=DEFAULT_MAX_OPEN_OFFERS_PER_TASK):
        if type(allow_ownership_transfer) is not bool:
            raise ValueError("allow_ownership_transfer must be a boolean")
        if type(max_open_offers_per_task) is not int or not 1 <= max_open_offers_per_task <= 64:
            raise ValueError("max_open_offers_per_task must be an integer in 1..64")
        self.allow_ownership_transfer = allow_ownership_transfer
        self.max_open_offers_per_task = max_open_offers_per_task


class PinnedRef(m.Ref):
    """A source context reference pinned to an exact current revision."""

    revision: int = Field(ge=1)


class HandoffDeliverable(m.Model):
    """A typed deliverable reference at an exact revision; content is never inlined."""

    kind: Literal["artifact", "memory", "task", "board_post"]
    id: ID
    revision: int = Field(ge=1)
    note: str = Field(default="", max_length=1024)

    _clean_note = field_validator("note", mode="before")(strip_text)


class HandoffOffer(m.Model):
    """Canonical handoff proposal bound to the exact sender execution snapshot."""

    project_id: ID
    task_id: ID
    expected_task_revision: int = Field(ge=1)
    source_run_id: ID
    source_execution_id: ID
    recipient_agent_id: ID
    recipient_run_id: ID | None = None
    summary: str = Field(min_length=1, max_length=4096)
    required_capabilities: list[Capability] = Field(default_factory=list, max_length=16)
    context_refs: list[PinnedRef] = Field(default_factory=list, max_length=MAX_CONTEXT_REFS)
    deliverables: list[HandoffDeliverable] = Field(default_factory=list, max_length=MAX_DELIVERABLES)
    unresolved_questions: list[Question] = Field(default_factory=list, max_length=MAX_QUESTIONS)
    transfer: Literal["context", "ownership"] = "context"

    _clean_summary = field_validator("summary", mode="before")(strip_text)
    _clean_capabilities = field_validator("required_capabilities", mode="before")(strip_list)
    _clean_questions = field_validator("unresolved_questions", mode="before")(strip_list)

    @model_validator(mode="after")
    def consistent(self):
        keys = [(ref.kind, ref.id) for ref in self.context_refs]
        if len(set(keys)) != len(keys):
            raise ValueError("Context references must be unique")
        deliverable_keys = [(item.kind, item.id) for item in self.deliverables]
        if len(set(deliverable_keys)) != len(deliverable_keys):
            raise ValueError("Deliverables must be unique")
        if len(set(self.required_capabilities)) != len(self.required_capabilities):
            raise ValueError("Required capabilities must be unique")
        if any(key in keys for key in deliverable_keys):
            raise ValueError("A deliverable cannot also be a context reference")
        if self.transfer == "ownership" and self.recipient_run_id is None:
            raise ValueError("Ownership-transfer offers must pin the recipient run")
        return self


class HandoffAccept(m.Model):
    """Recipient decision fence: exact offer revision plus the exact current run."""

    expected_revision: int = Field(ge=1)
    recipient_run_id: ID


class HandoffReject(m.Model):
    expected_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=4096)

    _clean_reason = field_validator("reason", mode="before")(strip_text)


class HandoffRevoke(m.Model):
    expected_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=4096)

    _clean_reason = field_validator("reason", mode="before")(strip_text)


class RunTaskContext(m.Model):
    """Worker consumption fence: the exact current run, task, execution, revision."""

    run_id: ID
    task_id: ID
    execution_id: ID
    expected_task_revision: int = Field(ge=1)


def _refuse_sensitive(value, code="sensitive_content_refused"):
    if looks_sensitive(value):
        raise Problem(code, 422)


class StructuredHandoffs:
    """Domain logic for recipient-accepted handoffs; mutate inside ``Service.mutate``."""

    def __init__(self, service, policy=None):
        self.service = service
        self.store = service.store
        self.policy = policy if policy is not None else StructuredHandoffPolicy()

    # ---- helpers ----------------------------------------------------------

    def _body(self, handoff_id):
        row = self.store.raw(handoff_id, HANDOFF_KIND)
        return json.loads(row["body"])

    def _save(self, body, expected):
        row = self.store.raw(body["id"], HANDOFF_KIND)
        return self.store.save(HANDOFF_KIND, row["project"], row["owner"], body, expected=expected)

    def _pin_ref(self, principal, project_id, kind, ref_id, revision):
        """Authorize a reference for THIS exact principal, right now, at an exact revision."""
        body = self.service.reference(principal, project_id, ref_id, kind)
        if body["revision"] != revision:
            raise Problem("stale_evidence")
        return {"kind": kind, "id": ref_id, "revision": revision}

    def _pinned_refs(self, offer):
        """Every pinned reference carried by the offer, including checkpoint evidence."""
        refs = [dict(ref) for ref in offer.get("context_refs") or []]
        refs += [{"kind": item["kind"], "id": item["id"], "revision": item["revision"]}
                 for item in offer.get("deliverables") or []]
        checkpoint = offer.get("checkpoint") or {}
        refs += [dict(ref) for ref in checkpoint.get("evidence_refs") or []]
        return refs

    def _authorize_refs(self, principal, offer):
        """Re-authorize every pinned ref against THIS exact principal, right now."""
        for ref in self._pinned_refs(offer):
            self._pin_ref(principal, offer["project_id"], ref["kind"], ref["id"], ref["revision"])

    @staticmethod
    def _cleared(principal, body):
        """Context payload clearance: the exact sender, the exact recipient, or an admin."""
        if principal["role"] == "admin":
            return True
        return principal.get("agent") in (body.get("sender_agent_id"), body.get("recipient_agent_id"))

    def _live_recipient_run(self, principal, offer, run_id):
        run = self.service.run(principal, run_id)
        if run["agent_id"] != offer["recipient_agent_id"] or run["project_id"] != offer["project_id"]:
            raise Problem("not_found", 404)
        if run["status"] not in LIVE_RUN_STATES:
            raise Problem("run_not_ready")
        if offer.get("recipient_run_id") and offer["recipient_run_id"] != run["id"]:
            raise Problem("recipient_run_mismatch", 409)
        return run

    @staticmethod
    def _settled_execution(task, execution_id):
        """Verified settled-execution evidence: the exact execution ENDED with a
        canonical terminal checkpoint. Heartbeat absence, a terminal run row, a
        paused run, or an unknown source NEVER count as settled."""
        candidates = []
        execution = task.get("execution") or {}
        if execution.get("id") == execution_id:
            candidates.append((execution, task.get("status") in SETTLED_TASK_STATES))
        for item in task.get("execution_history") or []:
            if isinstance(item, dict) and item.get("id") == execution_id:
                outcome = (item.get("result") or {}).get("outcome")
                candidates.append((item, outcome in SETTLED_OUTCOMES))
        for item, terminal in candidates:
            checkpoint = item.get("checkpoint") or {}
            if terminal and checkpoint.get("id") and not checkpoint.get("evidence_unavailable"):
                return True
        return False

    def _source_snapshot(self, principal, offer):
        """Current source task, explicit-handoff intent, and settled evidence."""
        task = self.store.get(offer["task_id"], principal, "task")
        if task["status"] == "cancelled":
            raise Problem("terminal_task")
        pending = task.get("handoff") or {}
        explicit = (pending.get("state") == "pending"
                    and pending.get("from_execution_id") == offer["source_execution_id"]
                    and pending.get("target_agent_id") == offer["recipient_agent_id"])
        settled = self._settled_execution(task, offer["source_execution_id"])
        return task, explicit, settled

    def _freshness(self, principal, body):
        """Current staleness signal for authorized viewers; generic reason codes only."""
        if body.get("state") in ("rejected", "revoked"):
            return {"checked": False, "stale": None, "reasons": []}
        reasons = []
        try:
            task = self.store.get(body["task_id"], principal, "task")
        except Problem:
            task = None
            reasons.append("task_unavailable")
        if task is not None and body.get("state") == "pending":
            pending = task.get("handoff") or {}
            explicit = (pending.get("state") == "pending"
                        and pending.get("from_execution_id") == body["source_execution_id"])
            settled = self._settled_execution(task, body["source_execution_id"])
            if not explicit and not settled:
                if task["revision"] != body["task_revision"]:
                    reasons.append("task_revision_changed")
                execution = task.get("execution") or {}
                if execution.get("id") != body["source_execution_id"]:
                    reasons.append("source_execution_changed")
                if body.get("checkpoint") and (execution.get("checkpoint") or {}).get(
                        "evidence_unavailable"):
                    reasons.append("checkpoint_evidence_unavailable")
        if task is not None:
            executions = [task.get("execution") or {}, *(task.get("execution_history") or [])]
            source = next((item for item in executions if isinstance(item, dict)
                           and item.get("id") == body.get("source_execution_id")), None)
            if source is None:
                reasons.append("source_execution_unavailable")
            elif body.get("checkpoint") and (source.get("checkpoint") or {}).get(
                    "evidence_unavailable"):
                reasons.append("checkpoint_evidence_unavailable")
        try:
            self._authorize_refs(principal, body)
        except Problem as error:
            reasons.append("context_ref_unavailable" if error.status == 404 else "context_ref_changed")
        return {"checked": True, "stale": bool(reasons), "reasons": sorted(set(reasons))}

    def _view(self, principal, body):
        """Authorized view: full context only for the exact sender/recipient/admin,
        and only while every pinned source is still visible to that viewer."""
        if not self._cleared(principal, body):
            # Blind view for any other project member: identity and state only,
            # never context-derived payload or per-resource freshness detail.
            view = {key: copy.deepcopy(body[key]) for key in BLIND_FIELDS if key in body}
            view["context_available"] = False
            view["freshness"] = {"checked": False, "stale": None, "reasons": []}
            view["authority"] = "canonical_handoff_record"
            return view
        view = copy.deepcopy(body)
        freshness = self._freshness(principal, body)
        if body.get("state") == "revoked":
            # No hidden cached context survives revocation.
            view.update(summary=None, checkpoint=None, context_refs=[], deliverables=[],
                        unresolved_questions=[], context_note="revoked_context_scrubbed",
                        context_available=False)
        elif freshness["stale"]:
            for field in CONTEXT_FIELDS:
                view[field] = None if field == "checkpoint" else []
            view.update(context_note="context_suppressed_source_stale_or_invisible",
                        context_available=False)
        else:
            view["context_available"] = True
        view["freshness"] = freshness
        view["authority"] = "canonical_handoff_record"
        return view

    # ---- offer ------------------------------------------------------------

    def offer(self, principal, request):
        self.service.project_access(principal, request.project_id)
        run = self.service.run(principal, request.source_run_id)
        if run["project_id"] != request.project_id:
            raise Problem("not_found", 404)
        task = self.service.reference(principal, request.project_id, request.task_id, "task")
        if task["revision"] != request.expected_task_revision:
            raise Problem("revision_conflict", 412)
        if task["status"] in TERMINAL_TASK_STATES:
            raise Problem("terminal_task")
        execution = task.get("execution") or {}
        if (execution.get("id") != request.source_execution_id
                or execution.get("run_id") != run["id"]
                or execution.get("agent_id") != run["agent_id"]):
            raise Problem("stale_task_execution", 409)
        recipient = self.service.reference(principal, request.project_id,
                                           request.recipient_agent_id, "agent")
        if recipient["id"] == run["agent_id"]:
            raise Problem("handoff_requires_different_agent", 422)
        if request.recipient_run_id is not None:
            target = self.service.reference(principal, request.project_id,
                                            request.recipient_run_id, "run")
            if (target["agent_id"] != recipient["id"]
                    or target["status"] not in LIVE_RUN_STATES
                    or target.get("task_id") not in (None, task["id"])):
                raise Problem("target_run_not_ready", 409)
        scheduling = Scheduling(self.service)
        if not set(request.required_capabilities).issubset(
                scheduling.capability_set(principal, recipient["id"])):
            raise Problem("capability_mismatch")
        if request.transfer == "ownership" and not self.policy.allow_ownership_transfer:
            # Transfer actions are default-off; an explicit operator opt-in is required.
            raise Problem("transfer_policy_disabled", 403)
        if any(ref.kind == "task" and ref.id == task["id"] for ref in request.context_refs):
            raise Problem("self_evidence_forbidden", 422)
        open_offers = [item for item in self.store.list(HANDOFF_KIND, request.project_id,
                                                        principal, 1000)
                       if item.get("task_id") == task["id"] and item.get("state") == "pending"]
        if len(open_offers) >= self.policy.max_open_offers_per_task:
            raise Problem("too_many_pending_offers", 409)
        pinned = [self._pin_ref(principal, request.project_id, ref.kind, ref.id, ref.revision)
                  for ref in request.context_refs]
        deliverables = []
        for item in request.deliverables:
            self._pin_ref(principal, request.project_id, item.kind, item.id, item.revision)
            deliverables.append(item.model_dump())
        # Sanitized server-side checkpoint: facts and pinned refs, never raw reasoning.
        checkpoint, source_checkpoint = None, execution.get("checkpoint")
        if source_checkpoint and not source_checkpoint.get("evidence_unavailable"):
            evidence = []
            for ref in source_checkpoint.get("evidence_refs") or []:
                if not isinstance(ref, dict) or not ref.get("revision"):
                    # Unsupported source evidence refuses the offer; never silently dropped.
                    raise Problem("checkpoint_evidence_unsupported", 422)
                evidence.append(self._pin_ref(principal, request.project_id,
                                              ref["kind"], ref["id"], ref["revision"]))
            checkpoint = {"id": source_checkpoint["id"],
                          "summary": source_checkpoint.get("summary"),
                          "progress": execution.get("progress", 0),
                          "evidence_refs": evidence,
                          "recorded_at": source_checkpoint.get("recorded_at")}
            _refuse_sensitive(checkpoint, "checkpoint_not_transferable")
        _refuse_sensitive(request.summary)
        _refuse_sensitive(request.unresolved_questions)
        _refuse_sensitive([item["note"] for item in deliverables])
        record = self.service.create(principal, HANDOFF_KIND, {
            "project_id": request.project_id,
            "task_id": task["id"],
            "task_revision": task["revision"],
            "sender_agent_id": run["agent_id"],
            "source_run_id": run["id"],
            "source_execution_id": request.source_execution_id,
            "recipient_agent_id": recipient["id"],
            "recipient_run_id": request.recipient_run_id,
            "summary": request.summary,
            "required_capabilities": list(request.required_capabilities),
            "context_refs": pinned,
            "deliverables": deliverables,
            "checkpoint": checkpoint,
            "unresolved_questions": list(request.unresolved_questions),
            "transfer": request.transfer,
            "state": "pending",
            "decision": None,
            "consumed": None,
            "offered_by": principal["id"],
        })
        return self._view(principal, record)

    # ---- reads --------------------------------------------------------------

    def get(self, principal, handoff_id):
        return self._view(principal, self.store.get(handoff_id, principal, HANDOFF_KIND))

    def list(self, principal, project_id, task_id=None, recipient_agent_id=None, state=None):
        self.service.project_access(principal, project_id)
        if state is not None and state not in STATES:
            raise Problem("invalid_state", 422)
        items = []
        for item in self.store.list(HANDOFF_KIND, project_id, principal, 200):
            if task_id is not None and item.get("task_id") != task_id:
                continue
            if recipient_agent_id is not None and item.get("recipient_agent_id") != recipient_agent_id:
                continue
            if state is not None and item.get("state") != state:
                continue
            items.append(self._view(principal, item))
        return items

    # ---- decisions ----------------------------------------------------------

    def _fenced_offer(self, principal, handoff_id, expected_revision):
        offer = self.store.get(handoff_id, principal, HANDOFF_KIND)
        if offer["revision"] != expected_revision:
            raise Problem("revision_conflict", 412)
        return self._body(handoff_id)

    def accept(self, principal, handoff_id, request):
        offer = self._fenced_offer(principal, handoff_id, request.expected_revision)
        if offer["state"] != "pending":
            raise Problem("handoff_not_pending", 409)
        self.service.represent(principal, offer["recipient_agent_id"])
        run = self._live_recipient_run(principal, offer, request.recipient_run_id)
        scheduling = Scheduling(self.service)
        if not set(offer.get("required_capabilities") or []).issubset(
                scheduling.capability_set(principal, offer["recipient_agent_id"])):
            raise Problem("capability_mismatch")
        task, explicit, settled = self._source_snapshot(principal, offer)
        if not explicit and not settled:
            # Exact snapshot: the task revision and source execution must be unchanged.
            if task["revision"] != offer["task_revision"]:
                raise Problem("handoff_source_stale", 409)
            execution = task.get("execution") or {}
            if execution.get("id") != offer["source_execution_id"]:
                raise Problem("handoff_source_stale", 409)
            checkpoint = offer.get("checkpoint")
            if checkpoint:
                current = execution.get("checkpoint") or {}
                if current.get("evidence_unavailable"):
                    raise Problem("checkpoint_evidence_unavailable", 409)
                if current.get("id") != checkpoint["id"]:
                    raise Problem("handoff_source_stale", 409)
        # Re-authorize every pinned ref against the RECIPIENT principal, right now.
        self._authorize_refs(principal, offer)
        ownership = {"requested": offer["transfer"] == "ownership", "effected": False}
        if offer["transfer"] == "ownership":
            if not self.policy.allow_ownership_transfer:
                raise Problem("transfer_policy_disabled", 403)
            if settled:
                ownership["support"] = "settled_execution_evidence"
                ownership["mechanism"] = "existing_task_handoff_or_dispatch"
            elif explicit:
                # Administrator intent is NOT proof that source effects settled.
                ownership["support"] = "intent_only_unverified"
                ownership["mechanism"] = "unsupported_without_settled_evidence"
            else:
                # Active/executing/paused/unknown sources hold; heartbeat absence or
                # a terminal run row alone is never quiescence proof.
                raise Problem("source_effects_active", 409)
            for other in self.store.list(HANDOFF_KIND, offer["project_id"], principal, 1000):
                if (other["id"] != offer["id"] and other.get("task_id") == offer["task_id"]
                        and other.get("state") == "accepted"):
                    raise Problem("handoff_already_accepted", 409)
            ownership["note"] = ("Acceptance records recipient intent only; ownership moves "
                                 "exclusively through tasks/:handoff and dispatch.")
        offer["state"] = "accepted"
        offer["decision"] = {
            "state": "accepted", "decided_by": principal["id"],
            "recipient_agent_id": offer["recipient_agent_id"],
            "recipient_run_id": run["id"], "decided_at": now(),
            "ownership": ownership,
        }
        saved = self._save(offer, expected=request.expected_revision)
        return self._view(principal, saved)

    def reject(self, principal, handoff_id, request):
        offer = self._fenced_offer(principal, handoff_id, request.expected_revision)
        if offer["state"] != "pending":
            raise Problem("handoff_not_pending", 409)
        self.service.represent(principal, offer["recipient_agent_id"])
        _refuse_sensitive(request.reason)
        offer["state"] = "rejected"
        offer["decision"] = {"state": "rejected", "decided_by": principal["id"],
                             "reason": request.reason, "decided_at": now()}
        saved = self._save(offer, expected=request.expected_revision)
        return self._view(principal, saved)

    def revoke(self, principal, handoff_id, request):
        offer = self._fenced_offer(principal, handoff_id, request.expected_revision)
        self.service.represent(principal, offer["sender_agent_id"])
        revocable = offer["state"] == "pending" or (
            offer["state"] == "accepted" and not offer.get("consumed"))
        if not revocable:
            raise Problem("handoff_not_revocable", 409)
        _refuse_sensitive(request.reason)
        previous_state = offer["state"]
        offer["state"] = "revoked"
        offer["decision"] = {"state": "revoked", "decided_by": principal["id"],
                             "previous_state": previous_state, "reason": request.reason,
                             "decided_at": now()}
        # No hidden cached context after revocation: scrub ALL context payload,
        # including reference/deliverable identities and checkpoint metadata.
        offer.update(summary="", checkpoint=None, unresolved_questions=[],
                     context_refs=[], deliverables=[], context_scrubbed=True)
        saved = self._save(offer, expected=request.expected_revision)
        return self._view(principal, saved)

    # ---- worker context consumption ------------------------------------------

    def consume_context(self, principal, request, *, require_consumed=False):
        """Replayable consume-once delivery bound to the exact run and execution.

        Fences (all rechecked on EVERY call, including command replays): the
        principal must currently represent the run's agent; the run must be live,
        hold the task, and match the accepted decision's exact recipient run (and
        any offer-pinned run); the task revision and current execution must match
        the request exactly; every pinned ref and checkpoint evidence ref is
        re-authorized against the consuming principal right now. Delivery to the
        same run + same execution is idempotent (returns the same context again,
        reauthorized); delivery to a different run or execution fails with
        ``handoff_consumed_elsewhere``. Never touches task/run/claim ownership.
        """
        run = self.service.run(principal, request.run_id)
        task = self.service.reference(principal, run["project_id"], request.task_id, "task")
        if task["revision"] != request.expected_task_revision:
            raise Problem("revision_conflict", 412)
        if run["status"] not in LIVE_RUN_STATES:
            raise Problem("run_not_ready")
        if run.get("task_id") != request.task_id:
            raise Problem("run_task_mismatch", 409)
        execution = task.get("execution") or {}
        if execution.get("id") != request.execution_id or execution.get("run_id") != run["id"]:
            raise Problem("stale_task_execution", 409)
        candidates = [item for item in self.store.list(HANDOFF_KIND, run["project_id"],
                                                       principal, 1000)
                      if item.get("task_id") == request.task_id
                      and item.get("recipient_agent_id") == run["agent_id"]
                      and item.get("state") == "accepted"]
        if not candidates:
            return {"id": None, "context": None}
        offer = candidates[-1]  # store.list is newest-first; deliver the oldest first
        decision = offer.get("decision") or {}
        if (decision.get("recipient_run_id") != run["id"]
                or (offer.get("recipient_run_id") and offer["recipient_run_id"] != run["id"])):
            raise Problem("recipient_run_mismatch", 409)
        body = self._body(offer["id"])
        consumed = body.get("consumed") or {}
        if require_consumed and not consumed:
            raise Problem("handoff_delivery_uncommitted", 409)
        if consumed and (consumed.get("run_id") != run["id"]
                         or consumed.get("execution_id") != execution["id"]):
            raise Problem("handoff_consumed_elsewhere", 409)
        # Fresh reauthorization of every pinned source ref against THIS principal.
        self._authorize_refs(principal, body)
        if self._freshness(principal, body)["stale"]:
            raise Problem("handoff_source_stale", 409)
        if not consumed:
            body["consumed"] = {"run_id": run["id"], "task_id": request.task_id,
                                "execution_id": execution["id"], "consumed_at": now()}
            body = self._save(body, expected=body["revision"])
        return {"id": body["id"], "context": {
            "handoff_id": body["id"], "task_id": request.task_id,
            "sender_agent_id": body["sender_agent_id"],
            "source_run_id": body["source_run_id"],
            "source_execution_id": body["source_execution_id"],
            "recipient_run_id": run["id"],
            "recipient_execution_id": execution["id"],
            "summary": body["summary"], "checkpoint": copy.deepcopy(body.get("checkpoint")),
            "context_refs": copy.deepcopy(body.get("context_refs") or []),
            "deliverables": copy.deepcopy(body.get("deliverables") or []),
            "unresolved_questions": list(body.get("unresolved_questions") or []),
            "transfer": body["transfer"],
            "ownership": copy.deepcopy(decision.get("ownership")),
            "authority": "canonical_accepted_handoff"}}


# ---- worker context hook (parent wires the call site) ----------------------

def consume_accepted_context(service, principal, request):
    """In-process equivalent of ``POST /v1/structured-handoffs:consume``.

    Exact contract for the parent worker wiring (no shared edits required):

    * ``request`` is a ``RunTaskContext`` (run_id, task_id, execution_id,
      expected_task_revision). All fences are rechecked on every call.
    * MUST be called inside ``Service.mutate`` (canonical transaction + idempotent
      command record) when it can mark delivery; the helper performs durable saves
      and assumes that context. Read-only replays (already delivered to this exact
      run and execution) are safe inside any canonical transaction.
    * Returns ``{"id", "context"}`` (context ``None`` when no accepted handoff
      exists). Delivery is idempotent per exact (run, execution); a lost response
      is recovered by retrying with the same Idempotency-Key (canonical command
      receipt replay) or a new key (fenced re-delivery), both reauthorized fresh.
    * Raises ``Problem`` on any fence violation; the caller must treat that as a
      hook failure, never as task evidence. NEVER changes task status, run
      assignment, claims, executions, or ownership.
    """
    return StructuredHandoffs(service).consume_context(principal, request)


OPERATION_DESCRIPTORS = {
    "structured_handoffs.offer": {
        "name": "structured_handoffs.offer",
        "purpose": "Canonical handoff proposal from the agent currently executing a "
                   "task to another agent, bound to the exact sender "
                   "project/agent/run/task/execution/revision snapshot.",
        "input": "POST /v1/structured-handoffs (HandoffOffer); Bearer principal that "
                 "currently represents the source agent (or an administrator).",
        "output": "The durable structured_handoff record (state=pending) with a "
                  "sanitized server-side checkpoint and pinned context refs.",
        "authority": "Current principal must represent the source agent; the task "
                     "revision, source execution, recipient agent/run, capabilities, "
                     "checkpoint evidence refs, and every pinned ref are re-verified "
                     "canonically at offer time; unsupported (unpinned) checkpoint "
                     "evidence refuses the offer.",
        "side_effects": "Creates one durable record. Never changes task status, run "
                        "assignment, claims, executions, or ownership.",
        "limits": "Bounded refs/deliverables/questions; bounded open offers per task; "
                  "credential-shaped content is refused; ownership-transfer offers "
                  "require the default-off policy opt-in and a pinned recipient run.",
        "retry_safety": "Idempotency-Key scoped to the offering principal; every "
                        "response (including replays) is a fresh reauthorized view.",
    },
    "structured_handoffs.read": {
        "name": "structured_handoffs.read",
        "purpose": "Read one handoff or list project handoffs with a current "
                   "freshness signal over the pinned snapshot.",
        "input": "GET /v1/structured-handoffs/{id} or "
                 "GET /v1/structured-handoffs?project_id[&task_id][&recipient_agent_id][&state].",
        "output": "Handoff views. Context payload is exposed only to the exact "
                  "sender agent, recipient agent, or an administrator, and only "
                  "while every pinned source remains visible to that viewer; all "
                  "other project members receive a blind identity/state view. "
                  "Revoked records are context-free.",
        "authority": "Standard canonical visibility for the record; per-viewer "
                     "re-authorization of every pinned ref gates context payload.",
        "side_effects": "None (read-only).",
        "limits": "Bounded list window (200); freshness reasons are generic codes "
                  "that never enumerate private resources.",
        "retry_safety": "Recheck on every read; freshness is informational and "
                        "never substitutes for the accept-time fences.",
    },
    "structured_handoffs.accept": {
        "name": "structured_handoffs.accept",
        "purpose": "Explicit recipient acceptance with exact current run, capability, "
                   "revision and source-snapshot fences. Acceptance is new durable "
                   "state on the offer, never a task/claim/ownership mutation.",
        "input": "POST /v1/structured-handoffs/{id}:accept (HandoffAccept).",
        "output": "The accepted record, including the ownership disposition "
                  "(requested/effected/support/mechanism).",
        "authority": "Principal must currently represent the recipient agent with a "
                     "live fenced run; refs re-authorized against the recipient; "
                     "ownership transfers additionally require the policy opt-in AND "
                     "verified settled-execution evidence (a canonical terminal "
                     "checkpoint for the exact source execution). Heartbeat absence, "
                     "terminal run rows, paused runs, and unknown sources NEVER "
                     "settle; a pending task handoff is administrator intent only "
                     "and is labeled intent_only_unverified.",
        "side_effects": "Records the durable acceptance decision only. Ownership "
                        "moves exclusively through tasks/:handoff and dispatch.",
        "limits": "One accepted ownership handoff per task; stale source snapshots, "
                  "revoked checkpoint evidence, and hidden refs fail closed.",
        "retry_safety": "Idempotency-Key scoped to structured-handoff-accept:{id}; a "
                        "second accept under a different key fails definitely; "
                        "replays re-read and reauthorize the current record.",
    },
    "structured_handoffs.reject": {
        "name": "structured_handoffs.reject",
        "purpose": "Explicit recipient rejection with an exact revision fence.",
        "input": "POST /v1/structured-handoffs/{id}:reject (HandoffReject).",
        "output": "The rejected record.",
        "authority": "Principal must currently represent the recipient agent.",
        "side_effects": "Records the durable rejection only.",
        "limits": "Only pending offers can be rejected.",
        "retry_safety": "Idempotency-Key scoped to structured-handoff-reject:{id}.",
    },
    "structured_handoffs.revoke": {
        "name": "structured_handoffs.revoke",
        "purpose": "Sender/administrator revocation of a pending or "
                   "accepted-but-unconsumed offer with durable scrubbing of ALL "
                   "context payload (summary, checkpoint, questions, ref and "
                   "deliverable identities).",
        "input": "POST /v1/structured-handoffs/{id}:revoke (HandoffRevoke).",
        "output": "The revoked, context-free record.",
        "authority": "Principal must currently represent the sender agent (or an "
                     "administrator).",
        "side_effects": "Records the revocation and durably scrubs the context "
                        "payload; no hidden cached context survives revocation.",
        "limits": "Consumed deliveries and rejected/revoked offers are final; the "
                  "reason is screened for credential-shaped content.",
        "retry_safety": "Idempotency-Key scoped to structured-handoff-revoke:{id}.",
    },
    "structured_handoffs.consume_context": {
        "name": "structured_handoffs.consume_context",
        "purpose": "Replayable consume-once delivery of an accepted handoff context "
                   "to the recipient worker, bound to the exact recipient run and "
                   "the exact current task execution.",
        "input": "POST /v1/structured-handoffs:consume (RunTaskContext: run_id, "
                 "task_id, execution_id, expected_task_revision).",
        "output": "The accepted context (summary, sanitized checkpoint, pinned refs, "
                  "deliverables, questions, ownership disposition), or context=null.",
        "authority": "Exact current run/task/execution/revision fences plus the "
                     "accepted decision's exact recipient run; every pinned ref and "
                     "checkpoint evidence ref is re-authorized against the consuming "
                     "principal on every call, including command replays.",
        "side_effects": "Durably records the delivery binding (run + execution); "
                        "never touches task status, runs, claims, or ownership.",
        "limits": "Oldest accepted handoff per (task, agent); delivery to a "
                  "different run or execution fails with handoff_consumed_elsewhere.",
        "retry_safety": "Retry with the same Idempotency-Key replays the canonical "
                        "command receipt; a new key re-runs fenced delivery "
                        "idempotently. A lost response never loses the context.",
    },
}


def descriptors():
    """Self-describing operation metadata for parent discovery/MCP registration."""
    return {name: dict(descriptor) for name, descriptor in OPERATION_DESCRIPTORS.items()}


def install(app, service, principal, command, envelope, *, policy=None):
    """Register structured-handoff endpoints on the application built by ``create_app``.

    Attaches the handler as ``app.state.structured_handoffs`` /
    ``service.structured_handoffs`` so the parent worker wiring has a stable hook;
    when the module is not installed the attribute is simply absent (callers use
    ``getattr(service, "structured_handoffs", None)`` — default empty).
    """
    from .capabilities import replace_routes
    from .agent_feature_gates import set_resource_projection
    replace_routes(app, __name__)
    handoffs = StructuredHandoffs(service, policy)
    app.state.structured_handoffs = handoffs
    service.structured_handoffs = handoffs
    set_resource_projection(service, HANDOFF_KIND, handoffs.get)
    endpoints = []

    @app.post("/v1/structured-handoffs")
    async def handoff_offer(body: HandoffOffer, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        result = command(p, "structured-handoff-offer:" + body.task_id, idempotency_key, body,
                         lambda: handoffs.offer(p, body))
        # Every response, including command replays, is a fresh reauthorized view.
        return envelope(handoffs.get(p, result["data"]["id"]))

    @app.post("/v1/structured-handoffs:consume")
    async def handoff_context_consume(body: RunTaskContext, p=Depends(principal),
                                      idempotency_key: str | None = Header(None)):
        result = command(p, "structured-handoff-consume:" + body.task_id, idempotency_key, body,
                         lambda: handoffs.consume_context(p, body))
        # Reauthorize fresh on every call, including canonical receipt replays.
        if not result["data"].get("id"):
            return envelope({"id": None, "context": None})
        return envelope(handoffs.consume_context(p, body, require_consumed=True))

    @app.get("/v1/structured-handoffs")
    async def handoff_list(project_id: str, task_id: str | None = None,
                           recipient_agent_id: str | None = None, state: str | None = None,
                           p=Depends(principal)):
        return envelope(handoffs.list(p, project_id, task_id, recipient_agent_id, state))

    @app.get("/v1/structured-handoffs/{handoff_id}")
    async def handoff_get(handoff_id: str, p=Depends(principal)):
        return envelope(handoffs.get(p, handoff_id))

    @app.post("/v1/structured-handoffs/{handoff_id}:accept")
    async def handoff_accept(handoff_id: str, body: HandoffAccept, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        command(p, "structured-handoff-accept:" + handoff_id, idempotency_key, body,
                lambda: handoffs.accept(p, handoff_id, body))
        return envelope(handoffs.get(p, handoff_id))

    @app.post("/v1/structured-handoffs/{handoff_id}:reject")
    async def handoff_reject(handoff_id: str, body: HandoffReject, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        command(p, "structured-handoff-reject:" + handoff_id, idempotency_key, body,
                lambda: handoffs.reject(p, handoff_id, body))
        return envelope(handoffs.get(p, handoff_id))

    @app.post("/v1/structured-handoffs/{handoff_id}:revoke")
    async def handoff_revoke(handoff_id: str, body: HandoffRevoke, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        command(p, "structured-handoff-revoke:" + handoff_id, idempotency_key, body,
                lambda: handoffs.revoke(p, handoff_id, body))
        return envelope(handoffs.get(p, handoff_id))

    endpoints.append("POST /v1/structured-handoffs")
    endpoints.append("POST /v1/structured-handoffs:consume")
    endpoints.append("GET /v1/structured-handoffs")
    endpoints.append("GET /v1/structured-handoffs/{handoff_id}")
    endpoints.append("POST /v1/structured-handoffs/{handoff_id}:accept")
    endpoints.append("POST /v1/structured-handoffs/{handoff_id}:reject")
    endpoints.append("POST /v1/structured-handoffs/{handoff_id}:revoke")
    return {"endpoints": endpoints, "models": list(MODELS)}
