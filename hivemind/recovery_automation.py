"""Bounded, opt-in, evidence-driven recovery automation on top of the durable journal.

This module reduces the manual reconciliation burden for the three hard cases that
today require an operator to inspect state by hand:

* **Unknown effects after a crash** — a journaled operation whose outcome is
  ``unknown`` is classified from *evidence* (the durable local record plus read-only
  canonical queries), never from the absence of an error and never from a generic GET
  that happened to succeed. Four buckets only: ``proved_applied``,
  ``proved_not_attempted``, ``ambiguous`` (still unknown; left for an operator), and
  ``stale_replaced`` (the canonical record moved to a newer execution/revision, so the
  old attempt must never be retried).
* **Interrupted vendor turns** — a turn operation is resolvable only from the vendor
  runtime's own *observed quiescent checkpoint* bound to the current run/task/execution
  identity **plus** authoritative pre-dispatch non-invocation proof. A paused/cancelled
  quiescent checkpoint alone is never proof of zero effect (a real model can write a
  file or fire a tool effect and only then be interrupted), so it stays ``ambiguous``
  for replay purposes; "the vendor stopped" is reported separately from "the vendor did
  nothing". A completed-but-unrecorded turn also stays ``ambiguous`` (the validated
  contract is unrecoverable; only a hash survives, and a result is never invented).
  The Factory live SDK 0.5.0 resume path stays fail-closed on workspace mismatch;
  nothing here resumes a vendor session or weakens that guard.
* **Reply sent but task not settled** — when the turn record holds a completed,
  validated decision, :meth:`Reconciler.recover_message_effect` finishes the canonical
  effect phase *without* calling a model or runtime: the deterministic reply is
  re-posted only after a canonical query proves it missing (the server idempotency on
  ``message_id`` plus the preserved command key make the re-post a replay, never a
  duplicate), and the task is settled under the server-side ``expected_revision`` /
  ``execution_id`` fence. A stale fence means ``stale_replaced`` and stops recovery.

Every classification and resolution appends a durable record to the journal's
``recovery_audit`` table, so automated recovery is itself auditable after a restart.
Everything is off unless a :class:`RecoveryPolicy` explicitly enables it; the existing
operator paths (``resolve_unknown``/``resolve_operation``) are untouched and remain
available for anything that stays ``ambiguous``.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .agent_session import SessionLock
from .recovery import (COMPLETED, PENDING, UNKNOWN, Journal, JournalError,
                       UnknownOutcome, command_key, derive_operation_id,
                       payload_checksum)
from .storage import digest, now, uid
from .runtime_messaging import (MESSAGE_EFFECT_KIND, NOTIFICATION_EFFECT_KIND,
                                NOTIFICATION_TASK_KIND, NOTIFICATION_TURN_KIND,
                                REPLY_PREFIX, TASK_OPERATION_KIND, TURN_OPERATION_KIND,
                                MessagingError, post_reply)

__all__ = [
    "AMBIGUOUS",
    "PROVED_APPLIED",
    "PROVED_NOT_ATTEMPTED",
    "STALE_REPLACED",
    "COMMAND_RECEIPTS_WIRING_NEED",
    "OPERATION_DESCRIPTORS",
    "RECOVERY_ACTIONS",
    "RecoveryAutomationHandle",
    "RecoveryError",
    "RecoveryPolicy",
    "RecoveryProposal",
    "RecoveryStatus",
    "Reconciler",
    "TASK_CREATE_COMMAND_OPERATION",
    "command_receipts",
    "handler_operation_ids",
    "install",
    "recover_task_creates",
    "reply_message_id",
    "settle_checkpoint_identity",
    "task_create_normalize",
    "vendor_quiescence_evidence",
]

PROVED_APPLIED = "proved_applied"
PROVED_NOT_ATTEMPTED = "proved_not_attempted"
AMBIGUOUS = "ambiguous"
STALE_REPLACED = "stale_replaced"

TERMINAL_TASK_STATES = frozenset({"completed", "failed", "cancelled"})
"""Canonical task states in which a settle checkpoint is a confirmed no-op."""

MAX_RECOVERY_OPERATIONS = 50
"""Hard bound on operations one reconciliation pass may inspect."""


class RecoveryError(RuntimeError):
    """Recovery automation refused: disabled policy, missing proof, or bad wiring."""


class _GateRefused(Exception):
    """Internal: the caller's effect_gate refused a phase. Carries the phase name."""

    def __init__(self, phase):
        super().__init__("effect_gate refused " + str(phase))
        self.phase = str(phase)


class RecoveryPolicy:
    """Explicit opt-in gates for automated recovery. Deny-by-default in every dimension.

    ``enabled`` unlocks read-only classification and auditing. ``allow_idempotent_replay``
    additionally permits re-issuing a journaled *retryable* command with its exact
    durable payload and preserved command key. ``allow_reply_replay`` permits re-posting
    a deterministic canonical reply that was proved missing. ``allow_quiescence_resolution``
    permits resolving an interrupted vendor turn to ``not_applied`` from an observed
    authoritative quiescent checkpoint that additionally proves pre-dispatch
    non-invocation (a deliberate retry then remains an explicit follow-up, never
    automatic); a mere paused/cancelled checkpoint never qualifies.
    """

    def __init__(self, *, enabled=False, allow_idempotent_replay=False,
                 allow_reply_replay=False, allow_quiescence_resolution=False,
                 max_operations=MAX_RECOVERY_OPERATIONS):
        self.enabled = enabled is True
        self.allow_idempotent_replay = allow_idempotent_replay is True
        self.allow_reply_replay = allow_reply_replay is True
        self.allow_quiescence_resolution = allow_quiescence_resolution is True
        self.max_operations = max(1, min(int(max_operations), 1000))

    def require_enabled(self):
        if not self.enabled:
            raise RecoveryError("Recovery automation requires an explicit opt-in policy")


# --------------------------------------------------------------------- identity
# These derivations must stay byte-identical to VendorMessagingExecutor's. They are
# recomputed from the same public primitives rather than imported so this module never
# reaches into private executor state. (Integration note for the messaging owner:
# exporting these three helpers from runtime_messaging would remove the duplication.)

def handler_operation_ids(project_id, agent_id, item_id, *, notification=False):
    """The (task, turn, effect) journal operation IDs for one handled item."""
    kinds = (NOTIFICATION_TASK_KIND, NOTIFICATION_TURN_KIND, NOTIFICATION_EFFECT_KIND) \
        if notification else (TASK_OPERATION_KIND, TURN_OPERATION_KIND, MESSAGE_EFFECT_KIND)
    return tuple(derive_operation_id("handler", kind, f"{project_id}:{agent_id}:{item_id}")
                 for kind in kinds)


def reply_message_id(project_id, agent_id, message_id, kind="response"):
    """The deterministic canonical reply ID used by ``post_reply``."""
    return ("msg-" + derive_operation_id(
        REPLY_PREFIX, project_id, agent_id, message_id, kind)[3:])[:128]


def settle_checkpoint_identity(project_id, agent_id, item_id, status, progress):
    """The deterministic (checkpoint_id, command key) for one settle checkpoint."""
    checkpoint_id = ("ckpt-" + derive_operation_id(
        "runtime-message", "checkpoint", project_id, agent_id, item_id,
        status, progress)[3:])[:128]
    key = command_key(derive_operation_id(
        "runtime-message", f"checkpoint-{status}-{progress}", project_id, agent_id, item_id))
    return checkpoint_id, key


def _status_of(error):
    """The HTTP status carried by an httpx-style error, or ``None``."""
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    return status if isinstance(status, int) else None


def _fetch(client, method, path):
    """One canonical read. Returns ``(value, status)``; errors never become evidence."""
    try:
        return client.request(method, path), 200
    except Exception as error:  # noqa: BLE001 - classification, never suppression
        return None, _status_of(error)


class Reconciler:
    """Evidence-driven reconciler for one journal, bound to one agent identity.

    ``client`` only needs ``request(method, path, body=None, key=None)`` (the canonical
    service client). It is used for read-only proof queries and, only under the
    explicit policy gates, for re-issuing idempotent canonical commands. No model,
    runtime, or vendor session is ever invoked from here.
    """

    def __init__(self, journal, client=None, *, project_id=None, agent_id=None,
                 run_id=None, policy=None):
        if not isinstance(journal, Journal):
            raise TypeError("Reconciler requires a hivemind.recovery.Journal")
        self.journal = journal
        self.client = client
        self.project_id = project_id
        self.agent_id = agent_id
        self.run_id = run_id
        self.policy = policy or RecoveryPolicy()

    # ------------------------------------------------------------ classification
    def classify(self, operation_id, probe=None):
        """Classify one operation from durable state plus an optional evidence probe.

        ``probe`` is ``probe(record, client) -> {"outcome", "evidence", "result"?}``;
        a probe that raises or returns an unknown bucket leaves the operation
        ``ambiguous``. The classification is audited durably; the operation record is
        never mutated here.
        """
        self.policy.require_enabled()
        record = self.journal.operation(operation_id)
        if record is None:
            raise KeyError(str(operation_id))
        result = None
        if record["state"] == COMPLETED:
            outcome, evidence = PROVED_APPLIED, {"source": "durable_record"}
        elif record["state"] == PENDING and not record["attempts"]:
            # Durably never dispatched: provably nothing was attempted.
            outcome = PROVED_NOT_ATTEMPTED
            evidence = {"source": "durable_record", "attempts": 0}
        elif record["state"] == PENDING and probe is None:
            # An attempt happened and was definitely rejected (4xx). That is NOT proof
            # of zero effect: a native/external side effect (e.g. a Git write) can
            # precede a fenced canonical failure. Only an explicit trusted
            # operation-specific probe may prove non-application.
            outcome, evidence = AMBIGUOUS, {"source": "durable_record",
                                            "attempts": record["attempts"],
                                            "reason": "attempted_then_rejected"}
        elif probe is None:
            outcome, evidence = AMBIGUOUS, {"source": "no_probe"}
        else:
            try:
                found = probe(record, self.client)
            except Exception as error:  # noqa: BLE001 - a failing probe proves nothing
                outcome = AMBIGUOUS
                evidence = {"source": "probe_error", "error_type": type(error).__name__}
            else:
                outcome = str((found or {}).get("outcome") or AMBIGUOUS)
                evidence = dict((found or {}).get("evidence") or {"source": "probe"})
                result = (found or {}).get("result")
                if outcome not in (PROVED_APPLIED, PROVED_NOT_ATTEMPTED, STALE_REPLACED):
                    outcome = AMBIGUOUS
        audit_evidence = {**evidence, "operation_id": record["operation_id"]}
        self.journal.record_recovery_audit(record["operation_id"], outcome, audit_evidence)
        return {"operation_id": record["operation_id"], "state": record["state"],
                "outcome": outcome, "evidence": evidence, "result": result}

    def resolve(self, operation_id, finding):
        """Apply a *proved* classification. ``ambiguous``/``stale_replaced`` never resolve.

        ``proved_applied`` requires the finding's independently verified result and
        completes the operation with exactly that canonical record (never an invented
        one). ``proved_not_attempted`` reopens the operation to ``pending``; any retry
        afterwards is a separate deliberate act under the preserved command key.
        """
        self.policy.require_enabled()
        record = self.journal.operation(operation_id)
        if record is None:
            raise KeyError(str(operation_id))
        # Binding: the finding must name this exact operation, and the operation must
        # still be unknown — a finding is never applied to an arbitrary or already
        # resolved record.
        if str(finding.get("operation_id")) != record["operation_id"]:
            raise RecoveryError("Finding does not identify this exact operation")
        if record["state"] != UNKNOWN:
            raise RecoveryError("Only unknown operations resolve; state is "
                                + str(record["state"]))
        outcome = finding.get("outcome")
        if outcome == PROVED_APPLIED:
            if "result" not in finding or finding["result"] is None:
                raise RecoveryError("A proved-applied resolution requires the verified result")
            record = self.journal.resolve_operation(operation_id, COMPLETED, finding["result"])
        elif outcome == PROVED_NOT_ATTEMPTED:
            record = self.journal.resolve_operation(operation_id, PENDING)
        else:
            raise UnknownOutcome(
                f"Operation {operation_id} is {outcome}; only proved outcomes resolve")
        decision = "completed" if outcome == PROVED_APPLIED else "not_applied"
        self.journal.record_recovery_audit(
            record["operation_id"], outcome,
            {"operation_id": record["operation_id"],
             **(finding.get("evidence") or {})}, decision=decision)
        return record

    def reconcile(self, probes=None, *, limit=None):
        """One bounded pass over ``unknown`` operations.

        ``probes`` is an iterable of ``(match, probe)`` pairs where ``match(record)``
        selects the operations a probe can prove. Operations without a matching probe,
        and anything a probe cannot prove, stay ``unknown``. Returns a report; resolves
        only proved outcomes.
        """
        self.policy.require_enabled()
        budget = min(self.policy.max_operations, int(limit) if limit else self.policy.max_operations)
        probes = list(probes or ())
        report = {"classified": 0, "resolved": [], "unresolved": [], "stale": []}
        for record in self.journal.operations(state=UNKNOWN, limit=budget):
            probe = next((p for match, p in probes if match(record)), None)
            finding = self.classify(record["operation_id"], probe)
            report["classified"] += 1
            if finding["outcome"] in (PROVED_APPLIED, PROVED_NOT_ATTEMPTED):
                self.resolve(record["operation_id"], finding)
                report["resolved"].append(record["operation_id"])
            elif finding["outcome"] == STALE_REPLACED:
                report["stale"].append(record["operation_id"])
            else:
                report["unresolved"].append(record["operation_id"])
        return report

    # ------------------------------------------------------------------- retrying
    def retry_operation(self, operation_id, *, fence=None):
        """Re-issue one journaled idempotent command with its exact durable payload.

        The payload and command key come from the durable record, never from the
        caller; a changed payload is impossible by construction (and would raise
        ``JournalConflict`` at the journal layer anyway). Requires the operation to be
        retry-safe *as recorded at begin time* (read-only method or an explicit
        ``safe_to_retry=True`` then) and its exact original body to be persisted.
        ``fence`` is an optional ``fence(record, client) -> "current"|"stale"|"unknown"``
        proof run immediately before the retry; anything but ``"current"`` aborts.
        """
        self.policy.require_enabled()
        if not self.policy.allow_idempotent_replay:
            raise RecoveryError("Retrying journaled commands requires allow_idempotent_replay")
        if self.client is None:
            raise RecoveryError("Retrying requires the canonical client")
        record = self.journal.operation(operation_id)
        if record is None:
            raise KeyError(str(operation_id))
        if record["state"] == COMPLETED:
            return record["result"]  # replay of the stored result; no network call
        if not record["retryable"]:
            raise UnknownOutcome(
                f"Operation {operation_id} was not recorded as retry-safe; reconcile explicitly")
        if record["method"] not in ("GET", "HEAD", "OPTIONS") and not record["body_persisted"]:
            raise RecoveryError(
                f"Operation {operation_id} has no persisted exact payload; never retried")
        # The durable payload must still hash to the recorded request hash: a replay
        # uses the exact persisted, unredacted original or nothing at all.
        if payload_checksum(record["method"], record["path"], record["body"]) \
                != record["request_hash"]:
            raise RecoveryError(
                f"Operation {operation_id} durable payload no longer matches its "
                "recorded request hash; never retried")
        if record["attempts"] > 0 and fence is None:
            # An omitted fence never authorizes replaying a previously attempted
            # operation; only a never-dispatched record may retry fence-free.
            raise RecoveryError(
                f"Operation {operation_id} was already attempted; an explicit "
                "idempotency/fence proof is required before any replay")
        if fence is not None:
            verdict = fence(record, self.client)
            if verdict == "stale":
                self.journal.record_recovery_audit(
                    record["operation_id"], STALE_REPLACED,
                    {"operation_id": record["operation_id"], "source": "fence"})
                raise UnknownOutcome(f"Operation {operation_id} is stale/replaced; never retried")
            if verdict != "current":
                raise UnknownOutcome(f"Operation {operation_id} fence is unproven; not retried")
        return self.journal.request(self.client, record["method"], record["path"],
                                    record["body"], operation_id=record["operation_id"])

    # -------------------------------------------------------- message settlement
    def _reply_evidence(self, message, decision, *, kind):
        """Prove from canonical state whether the deterministic reply was posted."""
        reply_id = reply_message_id(self.project_id, self.agent_id, message["id"], kind)
        value, status = _fetch(self.client, "GET", f"messages/{reply_id}")
        evidence = {"reply_message_id": reply_id, "query": "messages/" + reply_id,
                    "query_status": status}
        if status == 404:
            return PROVED_NOT_ATTEMPTED, evidence
        if status != 200 or not isinstance(value, dict):
            return AMBIGUOUS, evidence
        # Exact identity match; anything conflicting is proof of nothing.
        expected = {"id": reply_id, "in_reply_to": message["id"], "from": self.agent_id,
                    "project_id": self.project_id, "to": message.get("from"),
                    "thread_id": message.get("thread_id"), "type": kind,
                    "correlation_id": message.get("correlation_id") or message.get("thread_id"),
                    "sender_run_id": self.run_id}
        conflicts = [key for key, wanted in expected.items() if value.get(key) != wanted]
        content = value.get("content")
        if not isinstance(content, dict) or content.get("text") != decision.get("reply_text"):
            conflicts.append("content.text")
        if not isinstance(content, dict) or content.get("media_type") != "text/plain":
            conflicts.append("content.media_type")
        if conflicts:
            return AMBIGUOUS, {**evidence, "conflict": sorted(conflicts)}
        return PROVED_APPLIED, evidence

    def _settle_task(self, task, decision, item_id, label):
        """One fenced settle checkpoint, or a no-op when the task already settled."""
        if task.get("status") in TERMINAL_TASK_STATES:
            return True
        progress = 100 if decision["handled"] is True else \
            int((task.get("execution") or {}).get("progress", 0))
        status = "completed" if decision["handled"] is True else "failed"
        summary = (label + " handled" + ("; reply sent" if decision.get("reply_text")
                                         else "; no reply owed")) if decision["handled"] is True \
            else "Vendor runtime confirmed the " + label.lower() + " was not handled"
        checkpoint_id, key = settle_checkpoint_identity(
            self.project_id, self.agent_id, item_id, status, progress)
        self.client.request("POST", f"tasks/{task['id']}:checkpoint", {
            "expected_revision": task["revision"], "run_id": self.run_id,
            "execution_id": decision["execution_id"], "checkpoint_id": checkpoint_id,
            "status": status, "progress": progress, "summary": summary[:4096],
            "evidence_refs": []}, key=key)
        return True

    def recover_message_effect(self, message, *, notification=False, kind="response",
                               ttl_seconds=3600, label=None, effect_gate=None):
        """Finish the canonical effect phase of one message/notification from evidence.

        Precondition: the turn phase holds a *completed, validated* durable decision.
        This function never invokes a runtime or model, never fabricates a reply, and
        never acks a delivery. It re-posts the exact deterministic reply only when a
        canonical query proves it absent (and ``allow_reply_replay`` is opted in), then
        settles the canonical task under the server-side revision/execution fence.
        Raises ``UnknownOutcome`` whenever any link in the evidence chain is missing.

        ``effect_gate`` is an optional callback ``gate(phase, context) -> True``
        invoked immediately *before* the reply re-post (``phase="reply"``) and again
        *before* the settle checkpoint (``phase="settle"``), so a concurrent messaging
        primary's pause/cancel/steer/shutdown stays fenced during recovery. Anything
        but ``True`` is a definite refusal: recovery stops, and the effect record is
        left ``pending`` when this run applied no canonical effect yet or ``unknown``
        when one already landed. Every mutation is preceded by a strict durable audit
        write — if auditing fails, the mutation does not happen.
        """
        self.policy.require_enabled()
        if self.client is None or not self.project_id or not self.agent_id or not self.run_id:
            raise RecoveryError("Effect recovery requires the canonical client and identity")
        if notification:
            if message.get("recipient_agent_id") not in (None, self.agent_id):
                raise MessagingError("Recovery only settles this agent's own notifications")
        elif message.get("to") != self.agent_id or message.get("project_id") != self.project_id:
            raise MessagingError("Recovery only settles this agent's own project messages")
        item_id = message["id"]
        _, turn_op, effect_op = handler_operation_ids(
            self.project_id, self.agent_id, item_id, notification=notification)
        label = label or ("Notification" if notification else "Message")
        lock_path = self.journal.path.with_name(
            self.journal.path.name + "." + effect_op + ".exec")
        with SessionLock(lock_path):
            turn = self.journal.operation(turn_op)
            if turn is None or turn["state"] != COMPLETED:
                self._audit(effect_op, AMBIGUOUS, {"source": "turn_record",
                                                   "turn_state": (turn or {}).get("state")})
                raise UnknownOutcome(
                    "No durable validated decision for " + str(item_id)
                    + "; reconcile the turn first (a model is never re-invoked here)")
            decision = dict(turn.get("result") or {})
            if decision.get("operator_reconciled") or not isinstance(decision.get("handled"), bool) \
                    or not decision.get("task_id") or not decision.get("execution_id"):
                self._audit(effect_op, AMBIGUOUS, {"source": "turn_record", "valid": False})
                raise UnknownOutcome(
                    "The durable turn record carries no usable validated decision for "
                    + str(item_id))
            if decision.get("reply_text") is not None and not isinstance(decision["reply_text"], str):
                raise UnknownOutcome("Invalid durable reply_text; reconcile explicitly")
            effect = self.journal.operation(effect_op)
            if effect is not None and effect["state"] == COMPLETED:
                return {**dict(effect.get("result") or {}), "deduplicated": True}
            if effect is None:
                self.journal.begin_operation(effect_op, "HANDLER",
                                             NOTIFICATION_EFFECT_KIND if notification
                                             else MESSAGE_EFFECT_KIND,
                                             {"project_id": self.project_id,
                                              "agent_id": self.agent_id, "run_id": self.run_id,
                                              "item_id": item_id}, safe_to_retry=False)
            else:
                current = self.journal.operation(effect_op)
                if current["attempting"]:
                    # The exclusion lock is held, so this is a crashed holder's record.
                    self.journal.fail_operation(effect_op, RuntimeError("recovery_interrupted"))
            if decision["handled"] is True and decision.get("reply_text") and not notification \
                    and not self.policy.allow_reply_replay:
                raise RecoveryError("Re-posting a proved-missing reply requires allow_reply_replay")
            self.journal.mark_attempt(effect_op)
            applied = {"value": False}
            try:
                value = self._finish_effect(message, decision, item_id, label,
                                            notification=notification, kind=kind,
                                            ttl_seconds=ttl_seconds,
                                            effect_gate=effect_gate,
                                            effect_op=effect_op, applied=applied)
            except _GateRefused as refused:
                # A definite local control refusal, before or between mutations.
                if applied["value"]:
                    self.journal.fail_operation(effect_op, RuntimeError("gate_refused_after_effect"))
                    self._audit_strict(effect_op, AMBIGUOUS,
                                       {"source": "effect_gate", "phase": refused.phase,
                                        "applied_before_refusal": True})
                    raise UnknownOutcome(
                        "Control gate refused after an effect landed for " + str(item_id)
                        + "; reconcile explicitly") from None
                self._audit_strict(effect_op, PROVED_NOT_ATTEMPTED,
                                   {"source": "effect_gate", "phase": refused.phase})
                self.journal.fail_operation(effect_op, RuntimeError("gate_refused"), status=409)
                raise RecoveryError(
                    "Control gate refused the " + refused.phase + " phase for "
                    + str(item_id) + "; recovery stopped before any new effect") from None
            except MessagingError:
                self.journal.fail_operation(effect_op, RuntimeError("stale_or_replaced"))
                self._audit(effect_op, STALE_REPLACED, {"source": "canonical_fence"})
                raise UnknownOutcome(
                    "Canonical task or run for " + str(item_id) + " is stale/replaced") from None
            except Exception as error:  # noqa: BLE001 - transport/5xx stays unknown
                self.journal.fail_operation(effect_op, error)
                self._audit(effect_op, AMBIGUOUS,
                            {"source": "effect_recovery", "error_type": type(error).__name__})
                raise UnknownOutcome(
                    "Effect recovery for " + str(item_id) + " is ambiguous") from None
            # Audit strictly BEFORE the final journal mutation; a failed audit means
            # the completion is not recorded and the operation stays recoverable.
            self._audit_strict(effect_op, PROVED_APPLIED,
                               {"source": "effect_recovery", "task_id": decision["task_id"],
                                "execution_id": decision["execution_id"]}, decision="completed")
            record = self.journal.complete_operation(effect_op, value)
            return dict(record["result"] or {})

    def _gate(self, effect_gate, phase, context):
        """Invoke the caller's control gate; anything but ``True`` is a refusal."""
        if effect_gate is None:
            return
        if effect_gate(phase, context) is not True:
            raise _GateRefused(phase)

    def _finish_effect(self, message, decision, item_id, label, *, notification, kind,
                       ttl_seconds, effect_gate, effect_op, applied):
        """Apply exactly the durable decision: at most one reply, then the settle."""
        task, status = _fetch(self.client, "GET", f"tasks/{decision['task_id']}")
        if status != 200 or not isinstance(task, dict):
            raise RuntimeError("canonical_task_unreadable")
        if task.get("project_id") not in (None, self.project_id):
            raise MessagingError("Canonical task belongs to another project")
        execution = task.get("execution") or {}
        # Exact execution/run binding ALWAYS — including for already-terminal tasks,
        # so a replaced-then-completed execution can never launder a late old reply
        # or a false settlement.
        if execution.get("id") != decision["execution_id"] \
                or execution.get("run_id") != self.run_id:
            raise MessagingError("Canonical handling task execution is stale")
        run, run_status = _fetch(self.client, "GET", f"runs/{self.run_id}")
        if run_status != 200 or not isinstance(run, dict):
            raise RuntimeError("canonical_run_unreadable")
        if run.get("agent_id") != self.agent_id:
            raise MessagingError("Canonical run belongs to another agent")
        if run.get("status") in TERMINAL_TASK_STATES:
            raise MessagingError("Canonical run is terminal; recovery under it is stale")
        gate_context = {"operation_id": effect_op, "item_id": item_id,
                        "task_id": decision["task_id"], "execution_id": decision["execution_id"],
                        "run_id": self.run_id}
        value = {"handled": decision["handled"], "task_id": decision["task_id"],
                 "execution_id": decision["execution_id"], "settled": False,
                 "recovered": True}
        if not notification:
            value.update(replied=False, reply_message_id=None)
            if decision["handled"] is True and decision.get("reply_text"):
                outcome, _ = self._reply_evidence(message, decision, kind=kind)
                if outcome == PROVED_APPLIED:
                    value.update(replied=True, reply_replayed=True, reply_message_id=reply_message_id(
                        self.project_id, self.agent_id, message["id"], kind))
                elif outcome == PROVED_NOT_ATTEMPTED:
                    self._gate(effect_gate, "reply", {**gate_context, "phase": "reply"})
                    # Strict audit before the mutation: no audit, no reply.
                    self._audit_strict(effect_op, PROVED_NOT_ATTEMPTED,
                                       {"source": "reply_evidence", "action": "reply_repost",
                                        "reply_message_id": reply_message_id(
                                            self.project_id, self.agent_id, message["id"], kind)})
                    posted = post_reply(self.client, self.project_id, self.agent_id,
                                        self.run_id, message, decision["reply_text"],
                                        kind=kind, ttl_seconds=ttl_seconds)
                    applied["value"] = True
                    value.update(replied=True, reply_message_id=posted["id"])
                else:
                    raise RuntimeError("reply_state_ambiguous")
        if task.get("status") in TERMINAL_TASK_STATES:
            # Already settled under THIS exact execution/run binding (proved above).
            value["settled"] = True
            return value
        self._gate(effect_gate, "settle", {**gate_context, "phase": "settle"})
        # Strict audit before the mutation: no audit, no checkpoint.
        self._audit_strict(effect_op, PROVED_APPLIED,
                           {"source": "canonical_fence", "action": "settle",
                            "task_id": decision["task_id"],
                            "execution_id": decision["execution_id"],
                            "expected_revision": task["revision"]})
        value["settled"] = self._settle_task(task, decision, item_id, label)
        applied["value"] = True
        return value

    # -------------------------------------------------------- vendor interruption
    def resolve_interrupted_turn(self, operation_id, evidence):
        """Resolve an interrupted vendor turn from *pre-dispatch* non-invocation proof.

        ``evidence`` is the result of :func:`vendor_quiescence_evidence`. Resolution
        requires ALL of:

        * the explicit ``allow_quiescence_resolution`` opt-in;
        * a ``proved_not_attempted`` verdict, which the evidence function only issues
          for an authoritative quiescent checkpoint proving the turn was interrupted
          *before dispatch* (``dispatched is False``) — a mere paused/cancelled
          checkpoint is never enough, because a real model may have written a file or
          fired a tool effect and only then been interrupted;
        * exact binding: the journal record exists, is still ``unknown`` and not
          attempting, and its durable body (project/agent/run) plus the evidence
          run/task/execution identity match this reconciler's identity exactly.

        The reopened operation is then a deliberate retry decision for the caller;
        nothing re-invokes the vendor here. Anything short of that proof raises
        ``UnknownOutcome`` and leaves the record ``unknown``.
        """
        self.policy.require_enabled()
        if not self.policy.allow_quiescence_resolution:
            raise RecoveryError(
                "Resolving interrupted turns requires allow_quiescence_resolution")
        record = self.journal.operation(operation_id)
        if record is None:
            raise KeyError(str(operation_id))
        if record["state"] != UNKNOWN or record["attempting"]:
            raise RecoveryError("Only a durable unknown, non-attempting turn resolves")
        ev = dict((evidence or {}).get("evidence") or {})
        body = dict(record.get("body") or {})
        bound = (
            (evidence or {}).get("outcome") == PROVED_NOT_ATTEMPTED
            and ev.get("run_id") == self.run_id
            and (body.get("run_id") in (None, self.run_id))
            and (body.get("project_id") in (None, self.project_id))
            and (body.get("agent_id") in (None, self.agent_id))
            and ev.get("pre_dispatch") is True)
        if not bound:
            outcome = (evidence or {}).get("outcome")
            self._audit(operation_id,
                        outcome if outcome in (STALE_REPLACED, PROVED_APPLIED) else AMBIGUOUS,
                        {"source": "vendor_quiescence", "bound": False})
            raise UnknownOutcome(
                "Vendor turn is not proved pre-dispatch non-invoked for this exact "
                "operation/run/task/execution binding; reconcile explicitly")
        self._audit_strict(operation_id, PROVED_NOT_ATTEMPTED,
                           {"source": "vendor_quiescence", "run_id": ev.get("run_id"),
                            "task_id": ev.get("task_id"), "execution_id": ev.get("execution_id"),
                            "pre_dispatch": True}, decision="not_applied")
        return self.journal.resolve_operation(operation_id, PENDING)

    # --------------------------------------------------------- task-create recovery
    def recover_task_create(self, operation_id, *, receipts=None, normalize=None):
        """Recover an interrupted canonical task creation from authoritative receipts.

        ``receipts`` is a callable ``receipts(operation, key, payload_sha256) ->
        mapping`` backed by the *authoritative* server-side command ledger; use
        :func:`command_receipts` to adapt the canonical
        ``GET /v1/commands/receipt?operation=...&key=...`` endpoint. Decisions:

        * ``found/authoritative`` with a ``payload_sha256`` matching the durable
          payload AND an exact ``resource_id``: proof of application; the operation
          completes with exactly the receipt's current authorized projection
          (``response``), whose ``id`` must equal ``resource_id``. The receipt proves
          the canonical CREATION only — it says nothing about external/model effects
          (``external_effects_proved`` is recorded as evidence, never assumed).
        * ``{"found": False, "authoritative": True}`` — only a ledger that can
          authoritatively assert absence may prove non-commit. The canonical
          endpoint's 404 is NOT such proof (the cached resource may be hidden or
          deleted), and the adapter never emits it.
        * anything else — not evidence; the operation stays as recorded.

        ``normalize`` maps the durable journaled payload to the canonicalized
        request model (the server stores the command checksum over the Pydantic
        model WITH defaults applied — the parent wires
        ``lambda p: TaskCreate(**p).model_dump()``); the receipt hash is compared
        against ``digest(normalize(payload))``. A generic 4xx, quiescence, or a
        missing task in a listing is NEVER proof of no effect. IDs are never
        fabricated.
        """
        self.policy.require_enabled()
        record = self.journal.operation(operation_id)
        if record is None:
            raise KeyError(str(operation_id))
        operation_id = record["operation_id"]
        if record["state"] == COMPLETED:
            return {"operation_id": operation_id, "outcome": PROVED_APPLIED,
                    "result": record["result"], "record": record}
        if record["method"] != "POST" or record["path"].strip("/").split("?")[0] != "tasks":
            raise RecoveryError(
                f"Operation {operation_id} is not a canonical task-create command")
        if record["attempts"] == 0 and record["state"] == PENDING:
            self._audit(operation_id, PROVED_NOT_ATTEMPTED,
                        {"source": "durable_record", "attempts": 0})
            return {"operation_id": operation_id, "outcome": PROVED_NOT_ATTEMPTED,
                    "result": None, "record": record}
        if not record["body_persisted"]:
            # A redacted payload can never be hash-bound to a receipt; ambiguous.
            self._audit(operation_id, AMBIGUOUS, {"source": "durable_record",
                                                  "reason": "payload_not_persisted"})
            raise UnknownOutcome(
                f"Operation {operation_id} has no persisted exact payload; "
                "a task-create receipt cannot be bound to it")
        payload = json.loads(record["body"]) if isinstance(record["body"], str) \
            else record["body"]
        try:
            normalized = normalize(payload) if normalize is not None else payload
        except Exception as error:  # noqa: BLE001 - a failing model binding proves nothing
            self._audit(operation_id, AMBIGUOUS,
                        {"source": "durable_record", "reason": "normalize_failed",
                         "error_type": type(error).__name__})
            raise UnknownOutcome(
                f"Operation {operation_id} payload failed canonical normalization; "
                "reconcile explicitly") from None
        payload_sha256 = digest(normalized)
        if payload_checksum(record["method"], record["path"], payload) \
                != record["request_hash"]:
            self._audit(operation_id, AMBIGUOUS, {"source": "durable_record",
                                                  "reason": "payload_hash_mismatch"})
            raise UnknownOutcome(
                f"Operation {operation_id} durable payload no longer matches its "
                "recorded request hash; reconcile explicitly")
        if receipts is None:
            self._audit(operation_id, AMBIGUOUS,
                        {"source": "command_receipts", "reason": "receipts_unavailable"})
            raise UnknownOutcome(
                "Authoritative command receipts are not wired; the attempted "
                "task-create stays unknown (see COMMAND_RECEIPTS_WIRING_NEED)")
        try:
            receipt = receipts("task-create", record["command_key"], payload_sha256)
        except Exception as error:  # noqa: BLE001 - a failing lookup proves nothing
            self._audit(operation_id, AMBIGUOUS,
                        {"source": "command_receipts", "reason": "receipt_lookup_failed",
                         "error_type": type(error).__name__})
            raise UnknownOutcome(
                f"Command-receipt lookup for {operation_id} failed; not evidence") from None
        receipt = dict(receipt or {})
        if receipt.get("found") is True and receipt.get("authoritative") is True:
            response = receipt.get("response")
            if not isinstance(response, dict) or not response.get("id"):
                self._audit(operation_id, AMBIGUOUS,
                            {"source": "command_receipts", "reason": "receipt_without_task"})
                raise UnknownOutcome(
                    "The command receipt carries no canonical task record; "
                    "reconcile explicitly (an ID is never invented)")
            if receipt.get("payload_sha256") != payload_sha256:
                self._audit(operation_id, AMBIGUOUS,
                            {"source": "command_receipts", "reason": "payload_hash_conflict"})
                raise UnknownOutcome(
                    "The command receipt's payload hash conflicts with the durable "
                    "payload; reconcile explicitly")
            # Exact current resource binding: the recorded projection must BE the
            # receipt's resource_id. Anything else is ambiguity, not proof.
            resource_id = receipt.get("resource_id")
            if not resource_id or response.get("id") != resource_id:
                self._audit(operation_id, AMBIGUOUS,
                            {"source": "command_receipts", "reason": "resource_mismatch"})
                raise UnknownOutcome(
                    "The command receipt's resource_id does not exactly match the "
                    "task projection; reconcile explicitly")
            if receipt.get("external_effects_proved") is not False:
                # The endpoint proves canonical creation only; a receipt claiming
                # more is not this adapter's contract and must be reconciled.
                if receipt.get("external_effects_proved") not in (None, False):
                    self._audit(operation_id, AMBIGUOUS,
                                {"source": "command_receipts",
                                 "reason": "external_effects_claimed"})
                    raise UnknownOutcome(
                        "The receipt claims external-effect proof, which creation "
                        "receipts never provide; reconcile explicitly")
            decision = {"task_id": response["id"]}
            if record["state"] == UNKNOWN:
                resolved = self.resolve(operation_id, {
                    "operation_id": operation_id, "outcome": PROVED_APPLIED,
                    "result": response,
                    "evidence": {"operation_id": operation_id,
                                 "source": "command_receipts",
                                 "command_key": record["command_key"],
                                 "payload_sha256": payload_sha256}})
            else:
                # A definite 4xx left the record pending, yet the authoritative
                # ledger proves the command committed; complete with exactly the
                # recorded canonical response.
                self._audit_strict(operation_id, PROVED_APPLIED,
                                   {"source": "command_receipts",
                                    "command_key": record["command_key"],
                                    "payload_sha256": payload_sha256,
                                    "task_id": response["id"]},
                                   decision="completed")
                resolved = self.journal.complete_operation(operation_id, response)
            return {"operation_id": operation_id, "outcome": PROVED_APPLIED,
                    "result": resolved.get("result"), "task_id": decision["task_id"],
                    "record": resolved}
        if receipt.get("found") is False and receipt.get("authoritative") is True:
            # The authoritative ledger provably never committed this exact key:
            # zero canonical effect by construction. The operation reopens to
            # pending; any retry is a deliberate act under the preserved key.
            self._audit_strict(operation_id, PROVED_NOT_ATTEMPTED,
                               {"source": "command_receipts",
                                "command_key": record["command_key"],
                                "payload_sha256": payload_sha256},
                               decision="not_applied")
            if record["state"] == UNKNOWN:
                resolved = self.journal.resolve_operation(operation_id, PENDING)
            else:
                resolved = record
            return {"operation_id": operation_id, "outcome": PROVED_NOT_ATTEMPTED,
                    "result": None, "record": resolved}
        self._audit(operation_id, AMBIGUOUS,
                    {"source": "command_receipts", "reason": "not_authoritative"})
        raise UnknownOutcome(
            f"Command receipts could not authoritatively prove {operation_id}; "
            "the task-create stays as recorded for an operator")

    # -------------------------------------------------- wedged auxiliary recovery
    def resolve_auxiliary(self, operation_id, finding, *, gate=None):
        """Resolve a wedged auxiliary task from proved evidence under current controls.

        An auxiliary task (reviewer/worker sub-task) wedges when its journaled
        invocation crashed between dispatch and durable completion. Resolution is
        allowed ONLY from a finding that is fully proved and exactly bound:

        * ``finding["outcome"]`` is ``proved_applied`` (with the independently
          verified ``result``) or ``proved_not_attempted`` — an ``ambiguous`` or
          ``stale_replaced`` finding remains a hold and is never resolved here;
        * the journal record exists, is still ``unknown`` and not attempting;
        * the finding's evidence carries this reconciler's exact
          project/agent/run identity, and when the durable body names a
          ``task_id``/``execution_id`` the evidence must match them exactly;
        * the evidence carries a DURABLE proof object — an operator's proposal or
          a caller's bare claim is never authority. ``evidence["proof"]`` must be
          a mapping with ``kind`` one of:

          - ``"command_receipt"``: an authoritative server command receipt
            carrying the exact ``resource_id`` and ``payload_sha256``;
          - ``"durable_result"``: an independently recorded durable result whose
            ``result_sha256`` equals ``digest(finding["result"])`` (required for
            ``proved_applied``);
          - ``"pre_dispatch_non_invocation"``: authoritative quiescent proof the
            invocation never dispatched (for ``proved_not_attempted``);
        * ``gate`` is REQUIRED: ``gate(phase, context) -> True`` must prove the
          current identity/controls/delivery-lease are still held at the moment
          of mutation (phase ``"resolve"``). It is invoked immediately before
          the state transition, after all evidence checks; anything but ``True``
          is a definite refusal and nothing resolves.

        Unknown remains hold: every shortfall raises ``UnknownOutcome`` (or
        ``RecoveryError`` for a control refusal) and leaves the record untouched.
        """
        self.policy.require_enabled()
        record = self.journal.operation(operation_id)
        if record is None:
            raise KeyError(str(operation_id))
        operation_id = record["operation_id"]
        finding = dict(finding or {})
        evidence = dict(finding.get("evidence") or {})
        outcome = finding.get("outcome")
        if outcome not in (PROVED_APPLIED, PROVED_NOT_ATTEMPTED):
            self._audit(operation_id,
                        outcome if outcome == STALE_REPLACED else AMBIGUOUS,
                        {"source": "auxiliary_evidence", "bound": False})
            raise UnknownOutcome(
                f"Auxiliary task {operation_id} is not proved; it remains on hold")
        if record["state"] != UNKNOWN or record["attempting"]:
            raise RecoveryError("Only a durable unknown, non-attempting auxiliary "
                                "operation resolves")
        body = dict(record.get("body") or {})
        bound = (
            str(finding.get("operation_id")) == operation_id
            and evidence.get("project_id") == self.project_id
            and evidence.get("agent_id") == self.agent_id
            and evidence.get("run_id") == self.run_id
            and (body.get("task_id") in (None, evidence.get("task_id")))
            and (body.get("execution_id") in (None, evidence.get("execution_id")))
        )
        if not bound:
            self._audit(operation_id, AMBIGUOUS,
                        {"source": "auxiliary_evidence", "bound": False})
            raise UnknownOutcome(
                "Auxiliary evidence is not bound to this exact "
                "operation/project/agent/run/task/execution; remains on hold")
        # Durable proof requirement: a bare caller claim, however confident, is
        # never authority. The proof must reference durable, checkable evidence.
        proof = evidence.get("proof")
        proof_ok = isinstance(proof, dict) and (
            (proof.get("kind") == "command_receipt"
             and bool(proof.get("resource_id")) and bool(proof.get("payload_sha256")))
            or (proof.get("kind") == "durable_result"
                and outcome == PROVED_APPLIED and finding.get("result") is not None
                and proof.get("result_sha256") == digest(finding["result"]))
            or (proof.get("kind") == "pre_dispatch_non_invocation"
                and outcome == PROVED_NOT_ATTEMPTED
                and proof.get("quiescent") is True))
        if not proof_ok:
            self._audit(operation_id, AMBIGUOUS,
                        {"source": "auxiliary_evidence", "proof": "missing_or_invalid"})
            raise UnknownOutcome(
                "Auxiliary resolution requires durable fenced proof "
                "(command_receipt / durable_result / pre_dispatch_non_invocation); "
                "an operator proposal alone is not proof")
        if gate is None:
            raise RecoveryError(
                "Auxiliary resolution requires the current identity/controls/lease "
                "gate; refusing to resolve ungated")
        if gate("resolve", {
                "operation_id": operation_id, "phase": "resolve",
                "project_id": self.project_id, "agent_id": self.agent_id,
                "run_id": self.run_id,
                # The durable body wins; when it does not name the task/execution
                # (e.g. a wedged task-create phase), the binding-checked evidence
                # supplies the exact identity the gate must fence.
                "task_id": body.get("task_id") or evidence.get("task_id"),
                "execution_id": (body.get("execution_id")
                                 or evidence.get("execution_id"))}) is not True:
            self._audit(operation_id, AMBIGUOUS,
                        {"source": "controls_gate", "phase": "resolve"})
            raise RecoveryError(
                "Current identity/controls/lease gate refused auxiliary "
                "resolution; nothing was resolved")
        evidence = {**evidence, "operation_id": operation_id}
        return self.resolve(operation_id, {**finding, "evidence": evidence})

    def _audit_strict(self, operation_id, classification, detail, *, decision=None):
        """Mandatory audit write before a mutation. Failure propagates; nothing mutates."""
        self.journal.record_recovery_audit(
            operation_id, classification, {"operation_id": operation_id},
            decision=decision, detail=detail)

    def _audit(self, operation_id, classification, detail, *, decision=None):
        """Best-effort audit that never breaks the recovery flow itself."""
        try:
            self.journal.record_recovery_audit(
                operation_id, classification, {"operation_id": operation_id},
                decision=decision, detail=detail)
        except (JournalError, ValueError):
            pass


def vendor_quiescence_evidence(state_path, *, run_id, task_id=None, execution_id=None):
    """Evidence from the vendor runtime's own quiescent checkpoint files. Read-only.

    Reads ``<state_path>`` and its ``.checkpoint.json`` sibling (the files
    ``hivemind.vendor_runtimes.VendorRuntime`` writes only after an *observed* settled
    turn). Returns ``{"outcome", "evidence"}``.

    Crucially, a paused/cancelled quiescent checkpoint is **not** proof that nothing
    was attempted: a real model can write a file or fire a tool effect and only then
    be interrupted. ``proved_not_attempted`` is therefore issued *only* when the
    checkpoint additionally carries authoritative pre-dispatch non-invocation proof
    (``dispatched is False``: the prompt never reached the model, so no model effect
    was possible). Otherwise:

    * ``ambiguous`` (``interrupted_after_dispatch``) — a quiescent paused/cancelled
      checkpoint without pre-dispatch proof: the turn observably stopped, but its
      effects are unproven and must be reconciled by an operator;
    * ``ambiguous`` (``result_lost``) — a ``completed``/``failed`` checkpoint: only a
      result hash survives, the validated contract is unrecoverable, and a result is
      never invented;
    * ``ambiguous`` — missing/non-quiescent files or any identity mismatch.

    The evidence mapping always reports the *stopped/quiescent* facts separately from
    the effect outcome, so callers never confuse "the vendor stopped" with "the vendor
    did nothing". This never resumes a vendor session, never inspects transcripts, and
    never weakens the Factory live SDK fail-closed workspace guard.
    """
    path = Path(state_path)
    checkpoint_path = path.with_name(path.name + ".checkpoint.json")
    evidence = {"state_file": path.name, "checkpoint_file": checkpoint_path.name,
                "run_id": run_id, "task_id": task_id, "execution_id": execution_id}

    def read(target):
        try:
            return json.loads(target.read_text()) if target.exists() else None
        except (OSError, ValueError):
            return None

    state, checkpoint = read(path), read(checkpoint_path)
    if not isinstance(state, dict) or not isinstance(checkpoint, dict):
        return {"outcome": AMBIGUOUS, "evidence": {**evidence, "reason": "files_unreadable"}}
    pre_dispatch = checkpoint.get("dispatched") is False
    evidence.update({
        "quiescent": checkpoint.get("quiescent") is True,
        "stopped": checkpoint.get("status") in ("paused", "cancelled"),
        "status": checkpoint.get("status"),
        "session_id": checkpoint.get("session_id"),
        "result_sha256": checkpoint.get("result_sha256"),
        "recorded_at": checkpoint.get("recorded_at"),
        "state_outcome": state.get("outcome"),
        "pre_dispatch": pre_dispatch,
    })
    identity_ok = (
        checkpoint.get("quiescent") is True
        and checkpoint.get("run_id") == run_id
        and checkpoint.get("session_id") == state.get("session_id")
        and (task_id is None or checkpoint.get("task_id") == task_id)
        and (execution_id is None or checkpoint.get("execution_id") == execution_id)
        and state.get("outcome") not in (None, "running", "unknown")
        and checkpoint.get("status") in ("paused", "cancelled", "completed", "failed"))
    if not identity_ok:
        return {"outcome": AMBIGUOUS,
                "evidence": {**evidence, "reason": "identity_or_quiescence_unproven"}}
    if checkpoint["status"] in ("paused", "cancelled"):
        if pre_dispatch:
            # Authoritative: the turn never reached the model, so no model effect
            # was possible. Zero-effect by construction, not by inference.
            return {"outcome": PROVED_NOT_ATTEMPTED,
                    "evidence": {**evidence, "reason": "pre_dispatch_non_invocation"}}
        # Stopped, yes — but a dispatched model may already have written a file or
        # fired a tool effect before the interrupt. This is never replay permission.
        return {"outcome": AMBIGUOUS,
                "evidence": {**evidence, "reason": "interrupted_after_dispatch"}}
    # completed/failed: the turn finished but only a result hash survives. The
    # validated decision is unrecoverable; an operator must reconcile.
    return {"outcome": AMBIGUOUS, "evidence": {**evidence, "reason": "result_lost"}}


# ------------------------------------------------------------------ public API
COMMAND_RECEIPTS_WIRING_NEED = {
    "endpoint": "GET /v1/commands/receipt?operation=...&key=...",
    "auth": "authenticated, own-principal only",
    "success": "200 {'state': 'committed', 'operation', 'key', 'payload_sha256', "
               "'response_sha256', 'resource_id', 'resource', 'current' "
               "(current authorized projection), 'external_effects_proved': false}",
    "absence": "404 is NOT authoritative absence (the cached resource may be hidden "
               "or deleted); never infer not-attempted from a 404",
    "non_evidence": "any 5xx/transport error or foreign-principal response is never evidence",
    "normalization": "payload_sha256 is stored over the canonicalized request model "
                     "(Pydantic defaults applied); pass normalize=TaskCreate-model_dump "
                     "to recover_task_create (the parent wires it)",
    "note": "Endpoint provided by the parent; command_receipts() adapts it. The "
            "receipt proves canonical creation only, never external/model effects.",
}
"""Contract of the parent's command-receipt endpoint and how this module consumes it."""


TASK_CREATE_COMMAND_OPERATION = "tasks"
"""The server-side ``mutate()`` operation label recorded for ``POST /v1/tasks``.

``Reconciler.recover_task_create`` addresses the ledger with the logical label
``"task-create"``; the canonical service (``hivemind.api.tasks_create`` via
``command(p, "tasks", ...)``) stores the command under ``"tasks"``. The
:func:`command_receipts` adapter bridges the two via ``operation_map``.
"""


def task_create_normalize(payload):
    """Canonicalize a durable ``POST tasks`` body to the server's exact checksum shape.

    The canonical service checksums commands over the Pydantic request model WITH
    defaults applied (``TaskCreate.model_dump(by_alias=True, exclude_none=True)``),
    so a receipt's ``payload_sha256`` is only comparable after the exact same
    normalization. Extra keys raise (the model forbids them), which the caller
    treats as ambiguity, never as proof.
    """
    from .models import TaskCreate
    return TaskCreate(**dict(payload or {})).model_dump(by_alias=True, exclude_none=True)


def command_receipts(client, *, base_path="commands/receipt", operation_map=None):
    """Adapt the canonical command-receipt endpoint to the ``receipts`` callable.

    ``client`` needs ``request(method, path)`` against the authenticated canonical
    API. ``operation_map`` translates the logical operation labels used by the
    reconciler (e.g. ``"task-create"``) into the operation names the server actually
    recorded (``TASK_CREATE_COMMAND_OPERATION`` for task creation); the default is
    identity. The returned callable maps the real response shape
    (``state/operation/key/payload_sha256/response_sha256/resource_id/resource/
    current/external_effects_proved``) onto the receipt mapping
    :meth:`Reconciler.recover_task_create` consumes:

    * a 200 ``committed`` receipt whose operation/key echo the query becomes
      ``{"found": True, "authoritative": True, ...}`` with ``response`` set to the
      CURRENT authorized projection (``current``, falling back to ``resource``);
    * a 404 becomes ``{"found": False, "authoritative": False}`` — explicitly NOT
      evidence of non-application;
    * anything unexpected raises (the caller treats that as ambiguity, never proof).
    """
    from urllib.parse import urlencode
    mapping = dict(operation_map or {})

    def lookup(operation, key, payload_sha256):
        served = mapping.get(operation, operation)
        path = f"{base_path}?{urlencode({'operation': served, 'key': key})}"
        try:
            value = client.request("GET", path)
        except Exception as error:  # noqa: BLE001 - 404 is data; the rest is not
            status = getattr(getattr(error, "response", None), "status_code", None)
            if status == 404:
                return {"found": False, "authoritative": False,
                        "reason": "receipt_absent_not_proof"}
            raise
        receipt = dict(value or {})
        if receipt.get("state") != "committed" \
                or receipt.get("operation") != served or receipt.get("key") != key:
            raise RecoveryError("Command receipt does not echo the exact queried "
                                "operation/key; not evidence")
        response = receipt.get("current") if isinstance(receipt.get("current"), dict) \
            else receipt.get("resource")
        return {"found": True, "authoritative": True,
                "payload_sha256": receipt.get("payload_sha256"),
                "response_sha256": receipt.get("response_sha256"),
                "resource_id": receipt.get("resource_id"),
                "response": response,
                "external_effects_proved": receipt.get("external_effects_proved", False)}

    return lookup


def recover_task_creates(journal, client, *, policy, receipts=None, normalize=None,
                         limit=None):
    """Resolve every interrupted canonical task-create in ``journal`` from receipts.

    This is the startup/recovery sweep a parent (CLI/API) invokes explicitly — it is
    never wired into any automatic path and never re-issues a command. Each durable
    ``unknown`` ``POST tasks`` record is resolved by
    :meth:`Reconciler.recover_task_create` against the authoritative server command
    ledger (:func:`command_receipts`, with the logical ``"task-create"`` label mapped
    to the server's recorded ``"tasks"`` operation) and the exact service checksum
    normalization (:func:`task_create_normalize`, Pydantic defaults applied). A
    missing/hidden/deleted-receipt 404 is never proof of absence; redacted payloads,
    hash conflicts, lookup failures and stale bindings stay held exactly as
    recorded. Nothing is fabricated and no model/runtime is invoked.

    Requires an explicitly enabled :class:`RecoveryPolicy` (default-off). The whole
    sweep is serialized under the journal's shared mutex; every per-operation
    mutation is preceded by its own strict durable audit record. Returns
    ``{"recovered": [...], "held": [...], "skipped": int}`` with per-operation
    context (operation_id, command_key, method, path, attempts).
    """
    if not isinstance(journal, Journal):
        raise TypeError("recover_task_creates requires a hivemind.recovery.Journal")
    if policy is None or not policy.enabled:
        raise RecoveryError("Task-create recovery requires an explicit opt-in policy")
    if client is None and receipts is None:
        raise RecoveryError("Task-create recovery requires the canonical client")
    if receipts is None:
        receipts = command_receipts(
            client, operation_map={"task-create": TASK_CREATE_COMMAND_OPERATION})
    normalize = normalize or task_create_normalize
    reconciler = Reconciler(journal, client, policy=policy)
    budget = min(policy.max_operations, int(limit) if limit else policy.max_operations)
    report = {"recovered": [], "held": [], "skipped": 0}
    with journal.lock:  # one serialized sweep; per-op audits/mutations stay atomic
        records = journal.operations(state=UNKNOWN, limit=budget)
        for record in records:
            operation_id = record["operation_id"]
            context = {"operation_id": operation_id,
                       "command_key": record["command_key"],
                       "method": record["method"], "path": record["path"],
                       "attempts": record["attempts"]}
            if record["method"] != "POST" \
                    or record["path"].strip("/").split("?")[0] != "tasks":
                report["skipped"] += 1
                continue
            try:
                outcome = reconciler.recover_task_create(
                    operation_id, receipts=receipts, normalize=normalize)
            except (UnknownOutcome, RecoveryError) as error:
                report["held"].append({**context, "reason": type(error).__name__})
            else:
                report["recovered"].append({**context, "outcome": outcome["outcome"],
                                            "task_id": outcome.get("task_id")})
    return report


RECOVERY_ACTIONS = [
    {"action": "recovery.classify",
     "purpose": "Classify one journaled operation into a proved/ambiguous/stale bucket from evidence only",
     "input": "operation_id, optional trusted probe",
     "output": "classification finding; durable recovery_audit record",
     "authority": "requires enabled RecoveryPolicy; read-only against canonical state",
     "side_effects": "appends recovery_audit; never mutates the operation record",
     "limits": "bounded by policy.max_operations per pass; probes that fail prove nothing",
     "retry_safety": "idempotent; safe to repeat"},
    {"action": "recovery.reconcile",
     "purpose": "One bounded pass resolving only proved classifications",
     "input": "(match, probe) pairs, optional limit",
     "output": "report {classified, resolved, unresolved, stale}",
     "authority": "enabled policy; resolves proved outcomes only",
     "side_effects": "recovery_audit per operation; state transitions for proved outcomes",
     "limits": "at most policy.max_operations operations",
     "retry_safety": "idempotent; ambiguous operations stay unknown across passes"},
    {"action": "recovery.recover_task_create",
     "purpose": "Resolve an interrupted canonical task-create from authoritative command receipts",
     "input": "operation_id + receipts callable bound to the server-side command ledger",
     "output": "{outcome, result?, task_id?, record}",
     "authority": "enabled policy; receipt must be authoritative and hash-bound to the durable payload",
     "side_effects": "strict pre-mutation recovery_audit; completes or reopens the operation",
     "limits": "POST tasks operations with a persisted payload only; never fabricates IDs; "
               "generic 4xx/quiescence is never proof of no effect",
     "retry_safety": "idempotent; replay uses the preserved durable command key"},
    {"action": "recovery.recover_task_creates",
     "purpose": "Startup/recovery sweep: resolve every interrupted canonical task-create in a journal from authoritative receipts",
     "input": "journal, canonical client, enabled policy; optional receipts/normalize overrides",
     "output": "{recovered: [...], held: [...], skipped: n} with per-operation context",
     "authority": "explicitly enabled policy; receipts authoritative and hash-bound to the durable payload (TaskCreate defaults applied)",
     "side_effects": "strict pre-mutation recovery_audit per operation; completes or reopens records; never re-issues a command",
     "limits": "bounded by policy.max_operations; 404/deleted/hidden receipts are never proof of absence",
     "retry_safety": "idempotent; safe to invoke on every startup"},
    {"action": "recovery.resolve_auxiliary",
     "purpose": "Resolve a wedged auxiliary task from proved, identity-bound evidence",
     "input": "operation_id, finding, optional controls/lease gate",
     "output": "resolved record",
     "authority": "enabled policy; evidence must bind exact project/agent/run/task/execution; "
                  "gate must confirm current controls",
     "side_effects": "recovery_audit; state transition for proved outcomes only",
     "limits": "unknown non-attempting records only; ambiguous findings remain on hold",
     "retry_safety": "idempotent; a refused gate leaves the record untouched"},
    {"action": "recovery.tick",
     "purpose": "Operator/scheduler entry: one bounded reconcile pass plus a typed status snapshot",
     "input": "optional probes and limit",
     "output": "TickReport dict {report, status}",
     "authority": "enabled policy",
     "side_effects": "as reconcile(); no model/runtime invocation ever",
     "limits": "bounded; never blocks on vendor state",
     "retry_safety": "idempotent"},
]

# Self-describing exported descriptor data for the parent's registry. Every action
# is an explicit two-phase long handler: hivemind_detached_work=True (never inside
# a Service.mutate transaction or whole-process request path), default-off, and
# admin/trusted-paths only.
OPERATION_DESCRIPTORS = [
    {**descriptor, "hivemind_detached_work": True, "default_enabled": False,
     "execution": "two_phase_long_handler", "trusted_paths_only": True}
    for descriptor in RECOVERY_ACTIONS
]


@dataclass(frozen=True)
class RecoveryProposal:
    """One typed, self-describing recovery proposal for an unresolved operation."""

    proposal_id: str
    operation_id: str
    kind: str
    state: str
    outcome: str
    action: str               # "recover_task_create" | "resolve" | "hold"
    summary: str
    requires: tuple = ()      # policy flags / wiring needed before acting
    evidence: dict = field(default_factory=dict)
    created_at: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class RecoveryStatus:
    """Typed point-in-time recovery status for one reconciler identity."""

    project_id: str
    agent_id: str
    run_id: str
    pending: int
    unknown: int
    completed: int
    attempting: int
    policy: dict
    generated_at: str

    def to_dict(self):
        return asdict(self)


class RecoveryAutomationHandle:
    """Public handle returned by :func:`install`. Owns no wiring; the parent does."""

    def __init__(self, reconciler, *, context=None):
        self.reconciler = reconciler
        self.context = dict(context or {})

    def proposals(self, *, limit=None):
        """Typed proposals for every currently-unknown operation (bounded, audited)."""
        gate = self.reconciler
        gate.policy.require_enabled()
        budget = min(gate.policy.max_operations,
                     int(limit) if limit else gate.policy.max_operations)
        proposals = []
        for record in gate.journal.operations(state=UNKNOWN, limit=budget):
            finding = gate.classify(record["operation_id"])
            body = dict(record.get("body") or {})
            requires, action, summary = (), "hold", "Requires operator reconciliation"
            if record["method"] == "POST" and str(record["path"]).strip("/") == "tasks":
                action = "recover_task_create"
                summary = "Interrupted task-create; resolve from authoritative command receipts"
                requires = ("command_receipts_endpoint",)
            elif body.get("run_id") in (None, gate.run_id):
                action = "resolve"
                summary = "Unknown effect; supply proved evidence via resolve/resolve_auxiliary"
            if finding["outcome"] in (PROVED_APPLIED, PROVED_NOT_ATTEMPTED):
                action, summary = "resolve", "Proved " + finding["outcome"] + "; safe to resolve"
            proposals.append(RecoveryProposal(
                proposal_id=uid("proposal"), operation_id=record["operation_id"],
                kind=str(record.get("path") or ""), state=record["state"],
                outcome=finding["outcome"], action=action, summary=summary,
                requires=requires, evidence=dict(finding.get("evidence") or {}),
                created_at=now()))
        return proposals

    def status(self):
        """Typed counts by durable state for this reconciler's journal."""
        gate = self.reconciler
        journal = gate.journal
        counts = {COMPLETED: 0, PENDING: 0, UNKNOWN: 0}
        attempting = 0
        for state in counts:
            rows = journal.operations(state=state, limit=1000)
            counts[state] = len(rows)
            attempting += sum(1 for row in rows if row["attempting"])
        return RecoveryStatus(
            project_id=gate.project_id or "", agent_id=gate.agent_id or "",
            run_id=gate.run_id or "", pending=counts[PENDING],
            unknown=counts[UNKNOWN], completed=counts[COMPLETED],
            attempting=attempting,
            policy={"enabled": gate.policy.enabled,
                    "allow_idempotent_replay": gate.policy.allow_idempotent_replay,
                    "allow_reply_replay": gate.policy.allow_reply_replay,
                    "allow_quiescence_resolution": gate.policy.allow_quiescence_resolution},
            generated_at=now())

    def tick(self, probes=(), *, limit=None):
        """One bounded reconcile pass plus a typed status snapshot. Never invokes models."""
        return {"tick_id": uid("tick"),
                "report": self.reconciler.reconcile(probes, limit=limit),
                "status": self.status().to_dict()}

    def describe(self):
        """Self-describing action descriptors plus unmet wiring needs."""
        return {"actions": [dict(item) for item in OPERATION_DESCRIPTORS],
                "wiring_needs": [dict(COMMAND_RECEIPTS_WIRING_NEED)],
                "context": dict(self.context)}


def install(app=None, service=None, principal=None, command=None, envelope=None, *,
            journal, client=None, project_id=None, agent_id=None, run_id=None,
            policy=None):
    """Build the public recovery-automation handle.

    The parent owns registry/config/CLI/endpoint wiring; ``app``, ``service``,
    ``principal``, ``command`` and ``envelope`` are accepted and recorded as
    self-describing context only — this function never mutates them. Returns a
    :class:`RecoveryAutomationHandle` exposing ``proposals()``, ``status()``,
    ``tick()`` and ``describe()``. The exact endpoint this module still needs is
    reported in :data:`COMMAND_RECEIPTS_WIRING_NEED` and via ``describe()``.
    """
    reconciler = Reconciler(journal, client, project_id=project_id, agent_id=agent_id,
                            run_id=run_id, policy=policy)
    return RecoveryAutomationHandle(reconciler, context={
        "app": type(app).__name__ if app is not None else None,
        "service": type(service).__name__ if service is not None else None,
        "principal": (principal or {}).get("id") if isinstance(principal, dict) else None,
        "command": command, "envelope": envelope,
        # Explicit two-phase long handlers: never inside a Service.mutate
        # transaction or a whole-process request path; default-off, admin/
        # trusted-paths only. The parent wires the offline/CLI surfaces.
        "hivemind_detached_work": True, "default_enabled": False,
        "trusted_paths_only": True,
    })
