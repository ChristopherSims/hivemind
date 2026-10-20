"""Adversarial tests for evidence-driven recovery automation.

No paid inference, no network beyond the in-process service, no workspace writes.
Process-kill tests use a real child process that dies mid-attempt.
"""
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind.recovery import (COMPLETED, PENDING, UNKNOWN, Journal, JournalConflict,
                               UnknownOutcome, command_key)
from hivemind.recovery_automation import (AMBIGUOUS, PROVED_APPLIED, PROVED_NOT_ATTEMPTED,
                                          STALE_REPLACED, Reconciler, RecoveryError,
                                          RecoveryPolicy, handler_operation_ids,
                                          reply_message_id, vendor_quiescence_evidence)
from hivemind.runtime_messaging import MessagingError
from test_agent_recovery import status_error
from test_runtime_messaging import (AGENT, PEER, PROJECT, RUN, FakeCanonicalClient,
                                    make_executor, make_message,
                                    make_notice)


class HttpError(Exception):
    """httpx-shaped error: only the status code is evidence."""

    def __init__(self, status):
        super().__init__(f"status {status}")
        self.response = SimpleNamespace(status_code=status)


class ReplyAwareClient(FakeCanonicalClient):
    """Canonical GET for replies and runs too; 404 (never a bare error) when absent."""

    def __init__(self, messages=()):
        super().__init__(messages)
        self.runs = {RUN: {"id": RUN, "agent_id": AGENT, "project_id": PROJECT,
                           "status": "running"}}

    def request(self, method, path, body=None, key=None):
        if method == "GET" and path.startswith("messages/"):
            message_id = path[len("messages/"):]
            if message_id in self.posts:
                return dict(self.posts[message_id])
            if message_id in self.messages:
                return dict(self.messages[message_id])
            raise HttpError(404)
        if method == "GET" and path.startswith("runs/"):
            run = self.runs.get(path[len("runs/"):])
            if run is None:
                raise HttpError(404)
            return dict(run)
        return super().request(method, path, body, key)


def enabled(**overrides):
    base = {"enabled": True}
    base.update(overrides)
    return RecoveryPolicy(**base)


def reconciler(journal, client=None, **kwargs):
    policy = kwargs.pop("policy", None) or enabled(**kwargs)
    return Reconciler(journal, client, project_id=PROJECT, agent_id=AGENT, run_id=RUN,
                      policy=policy)


# --------------------------------------------------------------------- classify
def test_policy_disabled_denies_everything(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("op", "POST", "tasks", {"x": 1})
        gate = Reconciler(journal, policy=RecoveryPolicy())
        with pytest.raises(RecoveryError):
            gate.classify("op")
        with pytest.raises(RecoveryError):
            gate.reconcile()
        with pytest.raises(RecoveryError):
            gate.retry_operation("op")


def test_classification_buckets_are_durable_and_audited(tmp_path):
    path = tmp_path / "j.sqlite"
    with Journal(path) as journal:
        journal.begin_operation("op-done", "POST", "tasks", {"x": 1})
        journal.complete_operation("op-done", {"id": "t-1"})
        journal.begin_operation("op-fresh", "POST", "tasks", {"x": 2})
        journal.begin_operation("op-4xx", "POST", "tasks", {"x": 3})
        journal.mark_attempt("op-4xx")
        journal.fail_operation("op-4xx", status_error(409))
        journal.begin_operation("op-unknown", "POST", "tasks", {"x": 4})
        journal.mark_attempt("op-unknown")
        journal.fail_operation("op-unknown", TimeoutError("lost"))
        gate = reconciler(journal)
        assert gate.classify("op-done")["outcome"] == PROVED_APPLIED
        fresh = gate.classify("op-fresh")
        assert fresh["outcome"] == PROVED_NOT_ATTEMPTED
        assert fresh["evidence"]["attempts"] == 0
        # A definite 4xx after an attempt is NOT proof of zero effect: a native or
        # external side effect can precede a fenced canonical failure. Only an
        # explicit trusted operation-specific probe may prove non-application.
        rejected = gate.classify("op-4xx")
        assert rejected["outcome"] == AMBIGUOUS
        assert rejected["evidence"]["reason"] == "attempted_then_rejected"
        def trusted(record, client):
            return {"outcome": PROVED_NOT_ATTEMPTED,
                    "evidence": {"canonical": "commit_absent"}}

        assert gate.classify("op-4xx", trusted)["outcome"] == PROVED_NOT_ATTEMPTED
        assert gate.classify("op-unknown")["outcome"] == AMBIGUOUS
        # Unknown is never resolved by classification alone.
        assert journal.operation("op-unknown")["state"] == UNKNOWN
    with Journal(path) as reopened:
        audits = reopened.recovery_audits()
        assert {a["operation_id"] for a in audits} == {"op-done", "op-fresh", "op-4xx",
                                                       "op-unknown"}
        by_op = {a["operation_id"]: a["classification"] for a in audits}
        assert by_op["op-unknown"] == "ambiguous" and by_op["op-done"] == "proved_applied"


def test_probe_proves_applied_and_resolves_with_verified_result(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("op", "POST", "messages", {"text": "hi"})
        journal.mark_attempt("op")
        journal.fail_operation("op", TimeoutError("lost"))
        calls = []

        def probe(record, client):
            calls.append(record["operation_id"])
            return {"outcome": PROVED_APPLIED,
                    "evidence": {"server_id": "msg-9"},
                    "result": {"id": "msg-9", "status": "recorded"}}

        gate = reconciler(journal, ReplyAwareClient(), allow_idempotent_replay=True)
        finding = gate.classify("op", probe)
        gate.resolve("op", finding)
        record = journal.operation("op")
        assert record["state"] == COMPLETED and record["result"] == {"id": "msg-9",
                                                                     "status": "recorded"}
        decisions = [a for a in journal.recovery_audits("op") if a["decision"]]
        assert decisions[0]["decision"] == "completed"
        # A proved-applied resolution replays the verified result without a network call.
        assert gate.retry_operation("op") == {"id": "msg-9", "status": "recorded"}
        assert calls == ["op"]


def test_probe_error_or_conflict_stays_ambiguous(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("op", "POST", "tasks", {"x": 1})
        journal.mark_attempt("op")
        journal.fail_operation("op", TimeoutError("lost"))
        gate = reconciler(journal)
        assert gate.classify("op", lambda record, client: 1 / 0)["outcome"] == AMBIGUOUS
        conflict = lambda record, client: {"outcome": "definitely fine trust me"}  # noqa: E731
        assert gate.classify("op", conflict)["outcome"] == AMBIGUOUS
        with pytest.raises(UnknownOutcome):
            gate.resolve("op", {"operation_id": "op", "outcome": AMBIGUOUS})
        with pytest.raises(UnknownOutcome):
            gate.resolve("op", {"operation_id": "op", "outcome": STALE_REPLACED})
        # A finding bound to a different operation is never applied here.
        with pytest.raises(RecoveryError):
            gate.resolve("op", {"operation_id": "op-other", "outcome": PROVED_NOT_ATTEMPTED})
        assert journal.operation("op")["state"] == UNKNOWN


def test_proved_applied_requires_the_verified_result_never_an_invented_one(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("op", "POST", "tasks", {"x": 1})
        journal.mark_attempt("op")
        journal.fail_operation("op", TimeoutError("lost"))
        gate = reconciler(journal)
        with pytest.raises(RecoveryError):
            gate.resolve("op", {"outcome": PROVED_APPLIED})  # no result: refuse to invent
        assert journal.operation("op")["state"] == UNKNOWN


# ---------------------------------------------------------------------- retrying
def test_retry_uses_exact_durable_payload_and_preserved_key(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        client = ReplyAwareClient()
        # Non-idempotent-looking but explicitly retry-safe command, interrupted mid-flight.
        journal.begin_operation("op", "POST", "custom:command", {"exact": "payload"},
                                safe_to_retry=True)
        journal.mark_attempt("op")
        journal.fail_operation("op", TimeoutError("lost"))
        seen = []

        def replaying(method, path, body=None, key=None):
            seen.append({"method": method, "path": path, "body": body, "key": key})
            return {"ok": True}

        client.request = replaying
        gate = reconciler(journal, client, allow_idempotent_replay=True)
        # A previously attempted operation requires an explicit fence proof.
        with pytest.raises(RecoveryError):
            gate.retry_operation("op")
        assert seen == []
        assert gate.retry_operation("op", fence=lambda record, c: "current") == {"ok": True}
        assert seen == [{"method": "POST", "path": "custom:command",
                         "body": {"exact": "payload"}, "key": command_key("op")}]
        assert journal.operation("op")["state"] == COMPLETED


def test_retry_refuses_non_retryable_unpersisted_and_stale(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        gate = reconciler(journal, ReplyAwareClient(), allow_idempotent_replay=True)
        journal.begin_operation("op-unsafe", "POST", "tasks", {"x": 1}, safe_to_retry=False)
        journal.mark_attempt("op-unsafe")
        journal.fail_operation("op-unsafe", TimeoutError("lost"))
        with pytest.raises(UnknownOutcome):
            gate.retry_operation("op-unsafe")
        secret = {"token": "sk-live-abcdefghijklmnop"}
        journal.begin_operation("op-secret", "POST", "tasks", secret,
                                safe_to_retry=True, allow_sensitive_payload=True)
        journal.mark_attempt("op-secret")
        journal.fail_operation("op-secret", TimeoutError("lost"))
        with pytest.raises(RecoveryError):  # exact payload was never persisted
            gate.retry_operation("op-secret")
        journal.begin_operation("op-stale", "POST", "tasks", {"x": 1}, safe_to_retry=True)
        journal.mark_attempt("op-stale")
        journal.fail_operation("op-stale", TimeoutError("lost"))
        with pytest.raises(UnknownOutcome):
            gate.retry_operation("op-stale", fence=lambda record, client: "stale")
        audits = journal.recovery_audits("op-stale")
        assert audits[0]["classification"] == "stale_replaced"
        with pytest.raises(UnknownOutcome):
            gate.retry_operation("op-stale", fence=lambda record, client: "unknown")
        # Nothing was re-issued: every record above stays unknown.
        assert len(journal.operations(state=UNKNOWN)) == 3


def test_retry_refuses_a_tampered_durable_payload(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("op", "POST", "tasks", {"exact": "payload"},
                                safe_to_retry=True)
        journal.mark_attempt("op")
        journal.fail_operation("op", TimeoutError("lost"))
        # Direct durable tampering: the stored body no longer hashes to the recorded
        # request hash, so replay is refused even with a fence.
        journal.db.execute("UPDATE operations SET body=? WHERE operation_id=?",
                           ('{"exact": "tampered"}', "op"))
        gate = reconciler(journal, ReplyAwareClient(), allow_idempotent_replay=True)
        with pytest.raises(RecoveryError):
            gate.retry_operation("op", fence=lambda record, client: "current")
        assert journal.operation("op")["state"] == UNKNOWN


def test_changed_payload_after_crash_conflicts_and_is_never_retried(tmp_path):
    path = tmp_path / "j.sqlite"
    with Journal(path) as journal:
        journal.begin_operation("op", "POST", "tasks", {"title": "original"})
        journal.mark_attempt("op")
    with Journal(path) as journal:  # the child "crashed": reopening marks it unknown
        assert journal.operation("op")["state"] == UNKNOWN
        with pytest.raises(JournalConflict):
            journal.begin_operation("op", "POST", "tasks", {"title": "changed"})
        gate = reconciler(journal, ReplyAwareClient(), allow_idempotent_replay=True)
        with pytest.raises(UnknownOutcome):  # non-retryable: automation cannot touch it
            gate.retry_operation("op")


def test_real_process_kill_leaves_unknown_until_evidence_resolves(tmp_path):
    path = tmp_path / "j.sqlite"
    repo_root = Path(__file__).resolve().parents[1]
    child = (
        "import os, sys;"
        f"sys.path.insert(0, r'{repo_root}');"
        "from hivemind.recovery import Journal;"
        f"j = Journal(r'{path}');"
        "j.begin_operation('op-killed', 'POST', 'tasks', {'x': 1}, safe_to_retry=True);"
        "j.mark_attempt('op-killed'); os._exit(42)")
    result = subprocess.run([sys.executable, "-c", child], capture_output=True, timeout=60)
    assert result.returncode == 42
    with Journal(path) as journal:  # reopening resolves the in-flight attempt to unknown
        record = journal.operation("op-killed")
        assert record["state"] == UNKNOWN and record["error"] == {"kind": "interrupted"}
        gate = reconciler(journal)
        assert gate.classify("op-killed")["outcome"] == AMBIGUOUS
        gate.resolve("op-killed", {"operation_id": "op-killed",
                                   "outcome": PROVED_NOT_ATTEMPTED})
        assert journal.operation("op-killed")["state"] == PENDING


# --------------------------------------------------------- message effect recovery
def crashed_after_reply(tmp_path):
    class CrashAfterReply(ReplyAwareClient):
        armed = True

        def request(self, method, path, body=None, key=None):
            result = super().request(method, path, body, key)
            if self.armed and method == "POST" and path == "messages":
                self.armed = False
                raise ConnectionError("lost after the reply receipt")
            return result

    client = CrashAfterReply()
    handle, journal = make_executor(tmp_path, client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert len(client.posts) == 1 and len(client.tasks) == 1
    return client, handle, journal


def test_reply_sent_task_unsettled_recovers_without_model_or_second_reply(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    recovered = gate.recover_message_effect(make_message())
    task = next(iter(client.tasks.values()))
    assert recovered["handled"] and recovered["settled"] and recovered["recovered"]
    assert recovered["replied"] and recovered["reply_replayed"] is True
    assert recovered["reply_message_id"] == reply_message_id(PROJECT, AGENT, "msg-1")
    assert len(client.posts) == 1  # the reply was proved present, never re-posted
    assert task["status"] == "completed" and task["execution"]["progress"] == 100
    assert len(handle._runtime().calls) == 1  # the vendor turn never ran again
    checkpoints = [c for c in client.checkpoints if c["status"] == "completed"]
    assert len(checkpoints) == 1  # the settle checkpoint landed exactly once
    effect = journal.journal.operation(gate_effect_op())
    assert effect["state"] == COMPLETED and effect["result"]["settled"] is True
    audits = journal.journal.recovery_audits(gate_effect_op())
    assert audits[0]["classification"] == "proved_applied"
    assert audits[0]["decision"] == "completed"
    journal.journal.close()


def gate_effect_op():
    return handler_operation_ids(PROJECT, AGENT, "msg-1")[2]


def test_repeated_recovery_and_restart_are_deduplicated(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    first = gate.recover_message_effect(make_message())
    posts, checkpoints = len(client.posts), len(client.checkpoints)
    again = gate.recover_message_effect(make_message())
    assert again["deduplicated"] and again["settled"]
    assert len(client.posts) == posts and len(client.checkpoints) == checkpoints
    journal.journal.close()
    # Restart: a fresh journal + reconciler over the same files changes nothing.
    fresh = Journal(tmp_path / "handler.sqlite")
    gate2 = reconciler(fresh, client, allow_reply_replay=True)
    third = gate2.recover_message_effect(make_message())
    assert third["deduplicated"] and third["task_id"] == first["task_id"]
    assert len(client.posts) == posts and len(client.checkpoints) == checkpoints
    assert len(handle._runtime().calls) == 1
    fresh.close()


def test_proved_missing_reply_is_reposted_once_under_its_deterministic_id(tmp_path):
    class CrashBeforeReply(ReplyAwareClient):
        armed = True

        def request(self, method, path, body=None, key=None):
            if self.armed and method == "POST" and path == "messages":
                self.armed = False
                raise ConnectionError("lost before the reply reached the server")
            return super().request(method, path, body, key)

    client = CrashBeforeReply()
    handle, journal = make_executor(tmp_path, client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert client.posts == {} and len(client.tasks) == 1
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    recovered = gate.recover_message_effect(make_message())
    assert recovered["replied"] and "reply_replayed" not in recovered
    reply = next(iter(client.posts.values()))
    assert reply["id"] == reply_message_id(PROJECT, AGENT, "msg-1")
    assert reply["in_reply_to"] == "msg-1" and reply["to"] == PEER
    assert reply["content"]["text"] == "Pong"  # the durable decision text, never invented
    assert next(iter(client.tasks.values()))["status"] == "completed"
    assert len(handle._runtime().calls) == 1
    journal.journal.close()


def test_reply_replay_requires_explicit_opt_in(tmp_path):
    class CrashBeforeReply(ReplyAwareClient):
        def request(self, method, path, body=None, key=None):
            if method == "POST" and path == "messages":
                raise ConnectionError("lost")
            return super().request(method, path, body, key)

    client = CrashBeforeReply()
    handle, journal = make_executor(tmp_path, client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    gate = reconciler(journal.journal, client)  # allow_reply_replay defaults to False
    with pytest.raises(RecoveryError):
        gate.recover_message_effect(make_message())
    assert client.posts == {}  # no reply without the opt-in
    journal.journal.close()


def test_stale_execution_is_replaced_never_settled(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    task = next(iter(client.tasks.values()))
    task["execution"] = {"id": "exec-other", "run_id": RUN, "progress": 0}  # replaced
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message())
    assert task["status"] == "running"  # no checkpoint reached the replaced execution
    assert len([c for c in client.checkpoints if c["status"] == "completed"]) == 0
    effect = journal.journal.operation(gate_effect_op())
    assert effect["state"] == UNKNOWN  # never hidden: stays for the operator
    audits = journal.journal.recovery_audits(gate_effect_op())
    assert audits[0]["classification"] == "stale_replaced"
    journal.journal.close()


def test_conflicting_reply_evidence_stays_ambiguous(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    # The canonical reply ID now resolves to a message that answers something else.
    reply_id = reply_message_id(PROJECT, AGENT, "msg-1")
    client.posts[reply_id]["in_reply_to"] = "msg-unrelated"
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message())
    assert journal.journal.operation(gate_effect_op())["state"] == UNKNOWN
    audits = journal.journal.recovery_audits(gate_effect_op())
    assert audits[0]["classification"] == "ambiguous"
    journal.journal.close()


@pytest.mark.parametrize("field", ["project_id", "type", "correlation_id", "sender_run_id",
                                  "content.text", "content.media_type"])
def test_reply_proof_requires_complete_exact_envelope(tmp_path, field):
    client, handle, journal = crashed_after_reply(tmp_path)
    reply = client.posts[reply_message_id(PROJECT, AGENT, "msg-1")]
    if field.startswith("content."):
        reply["content"].pop(field.split(".")[1], None)
    else:
        reply[field] = "foreign"
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    try:
        with pytest.raises(UnknownOutcome):
            gate.recover_message_effect(make_message())
        assert len(client.posts) == 1 and len(handle._runtime().calls) == 1
        assert next(iter(client.tasks.values()))["status"] == "running"
        assert journal.journal.operation(gate_effect_op())["state"] == UNKNOWN
    finally:
        journal.journal.close()


def test_unknown_turn_is_never_recovered_and_runtime_never_invoked(tmp_path):
    client = ReplyAwareClient()
    handle, journal = make_executor(tmp_path, behavior="timeout", client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    calls_before = len(handle._runtime().calls)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message())  # the turn decision is unknown
    assert client.posts == {} and len(handle._runtime().calls) == calls_before
    effect = journal.journal.operation(gate_effect_op())
    assert effect is None  # no effect record was even begun
    journal.journal.close()


def test_recovery_refuses_foreign_or_cross_project_items(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(MessagingError):
        gate.recover_message_effect(make_message(to="agent-other"))
    with pytest.raises(MessagingError):
        gate.recover_message_effect(make_message(project_id="other"))
    journal.journal.close()


def test_notification_settlement_recovery_has_no_reply_channel(tmp_path):
    client = ReplyAwareClient()

    class CrashAtSettle(ReplyAwareClient):
        armed = True

        def request(self, method, path, body=None, key=None):
            if self.armed and method == "POST" and path.endswith(":checkpoint") \
                    and body["status"] in ("completed", "failed"):
                self.armed = False
                raise ConnectionError("lost after the decision")
            return super().request(method, path, body, key)

    client = CrashAtSettle()
    handle, journal = make_executor(tmp_path, client=client,
                                    behavior={"handled": True, "reply_text": None},
                                    allowed_notification_types=["board_mention"])
    with pytest.raises(UnknownOutcome):
        handle.review_notification(make_notice())
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    recovered = gate.recover_message_effect(make_notice(), notification=True)
    assert recovered["handled"] and recovered["settled"]
    assert "replied" not in recovered and client.posts == {}  # notifications never reply
    task = next(iter(client.tasks.values()))
    assert task["status"] == "completed"
    journal.journal.close()


def test_handled_false_decision_settles_failed_without_reply(tmp_path):
    class CrashAtSettle(ReplyAwareClient):
        armed = True

        def request(self, method, path, body=None, key=None):
            if self.armed and method == "POST" and path.endswith(":checkpoint") \
                    and body["status"] == "failed":
                self.armed = False
                raise ConnectionError("lost")
            return super().request(method, path, body, key)

    client = CrashAtSettle()
    handle, journal = make_executor(tmp_path, client=client,
                                    behavior={"handled": False, "reply_text": None})
    from hivemind.runtime_messaging import MessageNotHandled
    with pytest.raises((UnknownOutcome, MessageNotHandled)):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    gate = reconciler(journal.journal, client)
    recovered = gate.recover_message_effect(make_message())
    assert recovered["handled"] is False and recovered["settled"]
    assert client.posts == {}
    assert next(iter(client.tasks.values()))["status"] == "failed"
    journal.journal.close()


def crashed_before_reply(tmp_path):
    class CrashBeforeReply(ReplyAwareClient):
        armed = True

        def request(self, method, path, body=None, key=None):
            if self.armed and method == "POST" and path == "messages":
                self.armed = False
                raise ConnectionError("lost before the reply reached the server")
            return super().request(method, path, body, key)

    client = CrashBeforeReply()
    handle, journal = make_executor(tmp_path, client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert client.posts == {} and len(client.tasks) == 1
    return client, handle, journal


def test_effect_gate_fences_reply_and_settle_phases(tmp_path):
    phases = []
    client, handle, journal = crashed_before_reply(tmp_path)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    recovered = gate.recover_message_effect(
        make_message(), effect_gate=lambda phase, context: phases.append(
            (phase, context["operation_id"], context["task_id"])) or True)
    assert recovered["replied"] and recovered["settled"]
    assert [phase for phase, *_ in phases] == ["reply", "settle"]
    assert all(op == gate_effect_op() and task == recovered["task_id"]
               for _, op, task in phases)
    journal.journal.close()


def test_effect_gate_refusal_before_reply_is_definite_and_applies_nothing(tmp_path):
    client, handle, journal = crashed_before_reply(tmp_path)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(RecoveryError):
        gate.recover_message_effect(make_message(), effect_gate=lambda phase, ctx: False)
    assert client.posts == {}  # no reply left the process
    task = next(iter(client.tasks.values()))
    assert task["status"] == "running"  # and no settle
    effect = journal.journal.operation(gate_effect_op())
    assert effect["state"] == PENDING  # definite local refusal, safely re-runnable later
    audits = journal.journal.recovery_audits(gate_effect_op())
    assert audits[0]["detail"]["source"] == "effect_gate"
    # Once the primary lifts the gate, the same recovery completes exactly once.
    recovered = gate.recover_message_effect(make_message(),
                                            effect_gate=lambda phase, ctx: True)
    assert recovered["replied"] and recovered["settled"]
    assert len(client.posts) == 1
    journal.journal.close()


def test_effect_gate_refusal_at_settle_after_reply_is_honest_unknown(tmp_path):
    client, handle, journal = crashed_before_reply(tmp_path)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    allow_reply_only = lambda phase, ctx: phase == "reply"  # noqa: E731
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message(), effect_gate=allow_reply_only)
    assert len(client.posts) == 1  # the reply landed; the settle was refused
    assert next(iter(client.tasks.values()))["status"] == "running"
    effect = journal.journal.operation(gate_effect_op())
    assert effect["state"] == UNKNOWN  # honestly unknown: an effect was applied
    audits = journal.journal.recovery_audits(gate_effect_op())
    assert audits[0]["detail"]["applied_before_refusal"] is True
    # A later gated recovery does not re-post the reply and settles exactly once.
    recovered = gate.recover_message_effect(make_message(),
                                            effect_gate=lambda phase, ctx: True)
    assert recovered["reply_replayed"] is True and recovered["settled"]
    assert len(client.posts) == 1
    journal.journal.close()


def test_terminal_task_with_replaced_execution_is_stale_never_settled(tmp_path):
    """Adversarial: task replaced by a new execution which then completed."""
    client, handle, journal = crashed_after_reply(tmp_path)
    task = next(iter(client.tasks.values()))
    task.update(status="completed", execution={"id": "exec-new", "run_id": RUN,
                                               "progress": 100})
    posts_before = len(client.posts)
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message())
    assert len(client.posts) == posts_before  # no late old reply
    assert journal.journal.operation(gate_effect_op())["state"] == UNKNOWN
    assert journal.journal.recovery_audits(gate_effect_op())[0]["classification"] == \
        "stale_replaced"
    journal.journal.close()


def test_terminal_run_stops_recovery_even_with_current_task(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    client.runs[RUN]["status"] = "completed"  # the run is over; control moved on
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message())
    assert next(iter(client.tasks.values()))["status"] == "running"
    assert journal.journal.operation(gate_effect_op())["state"] == UNKNOWN
    journal.journal.close()


def test_recovery_refuses_a_task_from_another_project(tmp_path):
    client, handle, journal = crashed_after_reply(tmp_path)
    next(iter(client.tasks.values()))["project_id"] = "project-other"
    gate = reconciler(journal.journal, client, allow_reply_replay=True)
    with pytest.raises(UnknownOutcome):
        gate.recover_message_effect(make_message())
    assert journal.journal.operation(gate_effect_op())["state"] == UNKNOWN
    journal.journal.close()


def test_identities_match_the_executor_derivation(tmp_path):
    # The recovery module recomputes the executor's IDs; a drift here would recover the
    # wrong records, so pin them against the executor's own private derivation.
    handle, journal = make_executor(tmp_path, client=ReplyAwareClient())
    expected = tuple(handle._operation_id(kind, make_message()) for kind in (
        "runtime-message-task", "runtime-message-turn", "runtime-message-effect"))
    assert handler_operation_ids(PROJECT, AGENT, "msg-1") == expected
    lock = journal.journal.path.with_name(journal.journal.path.name + "." + expected[2] + ".exec")
    assert lock.name.endswith(".exec")
    journal.journal.close()


# ------------------------------------------------------------- bounded reconcile
def test_reconcile_pass_is_bounded_and_probes_opted(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        for index in range(5):
            journal.begin_operation(f"op-{index}", "POST", "tasks", {"i": index})
            journal.mark_attempt(f"op-{index}")
            journal.fail_operation(f"op-{index}", TimeoutError("lost"))
        gate = reconciler(journal, policy=enabled(max_operations=2))
        report = gate.reconcile(probes=[(
            lambda record: True,
            lambda record, client: {"outcome": PROVED_NOT_ATTEMPTED, "evidence": {}})])
        assert report["classified"] == 2 and len(report["resolved"]) == 2  # the bound held
        assert len(journal.operations(state=UNKNOWN)) == 3


# --------------------------------------------------------- vendor interruption
def write_vendor_state(tmp_path, checkpoint=None, **state):
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "vendor.json"
    path.write_text(json.dumps({"session_id": "sess-1", "run_id": RUN, "task_id": "t-1",
                                "execution_id": "e-1", "outcome": "paused", **state}))
    if checkpoint is not None:
        path.with_name(path.name + ".checkpoint.json").write_text(json.dumps(checkpoint))
    return path


def test_vendor_quiescence_buckets(tmp_path):
    base = {"id": "rc-1", "session_id": "sess-1", "run_id": RUN, "task_id": "t-1",
            "execution_id": "e-1", "result_sha256": "0" * 64, "recorded_at": 1.0}
    # A paused quiescent checkpoint alone is NOT proof of zero effect: a dispatched
    # model may have written a file or fired a tool effect before the interrupt.
    paused = write_vendor_state(tmp_path / "a", {**base, "status": "paused",
                                                 "quiescent": True})
    found = vendor_quiescence_evidence(paused, run_id=RUN, task_id="t-1", execution_id="e-1")
    assert found["outcome"] == AMBIGUOUS
    assert found["evidence"]["reason"] == "interrupted_after_dispatch"
    assert found["evidence"]["stopped"] is True and found["evidence"]["quiescent"] is True
    # Only authoritative pre-dispatch non-invocation (dispatched is False) proves that
    # no model effect was possible.
    pre = write_vendor_state(tmp_path / "pd", {**base, "status": "cancelled",
                                               "quiescent": True, "dispatched": False},
                             outcome="cancelled")
    found = vendor_quiescence_evidence(pre, run_id=RUN, task_id="t-1", execution_id="e-1")
    assert found["outcome"] == PROVED_NOT_ATTEMPTED
    assert found["evidence"]["reason"] == "pre_dispatch_non_invocation"
    # An explicit dispatched=True (a write-then-pause) is ambiguous even pre-settle.
    write_then_pause = write_vendor_state(tmp_path / "wp", {**base, "status": "paused",
                                                            "quiescent": True,
                                                            "dispatched": True})
    found = vendor_quiescence_evidence(write_then_pause, run_id=RUN)
    assert found["outcome"] == AMBIGUOUS
    wrong_run = vendor_quiescence_evidence(paused, run_id="run-other")
    assert wrong_run["outcome"] == AMBIGUOUS
    wrong_exec = vendor_quiescence_evidence(paused, run_id=RUN, execution_id="e-other")
    assert wrong_exec["outcome"] == AMBIGUOUS
    noisy = write_vendor_state(tmp_path / "b", {**base, "status": "paused",
                                                "quiescent": False})
    assert vendor_quiescence_evidence(noisy, run_id=RUN)["outcome"] == AMBIGUOUS
    done = write_vendor_state(tmp_path / "c", {**base, "status": "completed",
                                               "quiescent": True}, outcome="completed")
    lost = vendor_quiescence_evidence(done, run_id=RUN)
    assert lost["outcome"] == AMBIGUOUS and lost["evidence"]["reason"] == "result_lost"
    running = write_vendor_state(tmp_path / "d", {**base, "status": "paused",
                                                  "quiescent": True}, outcome="running")
    assert vendor_quiescence_evidence(running, run_id=RUN)["outcome"] == AMBIGUOUS
    missing = vendor_quiescence_evidence(tmp_path / "e" / "ghost.json", run_id=RUN)
    assert missing["outcome"] == AMBIGUOUS


def test_interrupted_turn_resolution_is_opted_in_bound_and_audited(tmp_path):
    checkpoint = {"id": "rc-1", "session_id": "sess-1", "run_id": RUN, "task_id": "t-1",
                  "execution_id": "e-1", "status": "cancelled", "quiescent": True,
                  "dispatched": False, "result_sha256": "0" * 64, "recorded_at": 1.0}
    path = write_vendor_state(tmp_path / "v", checkpoint, outcome="cancelled")
    evidence = vendor_quiescence_evidence(path, run_id=RUN, task_id="t-1", execution_id="e-1")
    assert evidence["outcome"] == PROVED_NOT_ATTEMPTED
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("turn-op", "HANDLER", "runtime-message-turn",
                                {"project_id": PROJECT, "agent_id": AGENT, "run_id": RUN,
                                 "item_id": "m"}, safe_to_retry=False)
        journal.mark_attempt("turn-op")
        journal.fail_operation("turn-op", TimeoutError("interrupted"))
        gate = reconciler(journal)  # allow_quiescence_resolution defaults to False
        with pytest.raises(RecoveryError):
            gate.resolve_interrupted_turn("turn-op", evidence)
        assert journal.operation("turn-op")["state"] == UNKNOWN
        opted = reconciler(journal, policy=enabled(allow_quiescence_resolution=True))
        record = opted.resolve_interrupted_turn("turn-op", evidence)
        assert record["state"] == PENDING  # reopened; any retry is a deliberate act
        audits = journal.recovery_audits("turn-op")
        assert audits[0]["classification"] == "proved_not_attempted"
        assert audits[0]["decision"] == "not_applied"
        # A completed-turn evidence (result lost) never resolves automatically.
        journal.begin_operation("turn-op-2", "HANDLER", "runtime-message-turn", {"id": "m2"},
                                safe_to_retry=False)
        journal.mark_attempt("turn-op-2")
        journal.fail_operation("turn-op-2", TimeoutError("interrupted"))
        done = write_vendor_state(tmp_path / "w", {**checkpoint, "status": "completed"},
                                  outcome="completed")
        lost = vendor_quiescence_evidence(done, run_id=RUN)
        with pytest.raises(UnknownOutcome):
            opted.resolve_interrupted_turn("turn-op-2", lost)
        assert journal.operation("turn-op-2")["state"] == UNKNOWN


def test_write_then_pause_never_grants_replay_permission(tmp_path):
    """Adversarial: model wrote a file, THEN the turn was paused at quiescence."""
    checkpoint = {"id": "rc-9", "session_id": "sess-1", "run_id": RUN, "task_id": "t-1",
                  "execution_id": "e-1", "status": "paused", "quiescent": True,
                  "dispatched": True, "result_sha256": "f" * 64, "recorded_at": 1.0}
    path = write_vendor_state(tmp_path / "v", checkpoint, outcome="paused")
    evidence = vendor_quiescence_evidence(path, run_id=RUN)
    assert evidence["outcome"] == AMBIGUOUS
    assert evidence["evidence"]["stopped"] is True  # "stopped" is not "did nothing"
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("turn-op", "HANDLER", "runtime-message-turn",
                                {"project_id": PROJECT, "agent_id": AGENT, "run_id": RUN,
                                 "item_id": "m"}, safe_to_retry=False)
        journal.mark_attempt("turn-op")
        journal.fail_operation("turn-op", TimeoutError("interrupted"))
        opted = reconciler(journal, policy=enabled(allow_quiescence_resolution=True))
        with pytest.raises(UnknownOutcome):
            opted.resolve_interrupted_turn("turn-op", evidence)
        assert journal.operation("turn-op")["state"] == UNKNOWN  # never reset to pending


def test_interrupted_turn_resolution_rejects_foreign_binding(tmp_path):
    """Adversarial: pre-dispatch proof for a DIFFERENT run/operation never resolves."""
    checkpoint = {"id": "rc-1", "session_id": "sess-1", "run_id": "run-other",
                  "task_id": "t-1", "execution_id": "e-1", "status": "cancelled",
                  "quiescent": True, "dispatched": False, "result_sha256": "0" * 64,
                  "recorded_at": 1.0}
    path = write_vendor_state(tmp_path / "v", checkpoint, outcome="cancelled")
    foreign = {"outcome": PROVED_NOT_ATTEMPTED,
               "evidence": {"run_id": "run-other", "pre_dispatch": True}}
    assert vendor_quiescence_evidence(path, run_id=RUN)["outcome"] == AMBIGUOUS
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("turn-op", "HANDLER", "runtime-message-turn",
                                {"project_id": PROJECT, "agent_id": AGENT, "run_id": RUN,
                                 "item_id": "m"}, safe_to_retry=False)
        journal.mark_attempt("turn-op")
        journal.fail_operation("turn-op", TimeoutError("interrupted"))
        opted = reconciler(journal, policy=enabled(allow_quiescence_resolution=True))
        with pytest.raises(UnknownOutcome):  # run binding mismatch
            opted.resolve_interrupted_turn("turn-op", foreign)
        # A trusted-looking outcome string on a non-unknown record is also refused.
        journal.resolve_operation("turn-op", PENDING)  # operator reopens deliberately
        with pytest.raises(RecoveryError):
            opted.resolve_interrupted_turn("turn-op", {
                "outcome": PROVED_NOT_ATTEMPTED,
                "evidence": {"run_id": RUN, "pre_dispatch": True}})
        assert journal.operation("turn-op")["state"] == PENDING  # unchanged


# ------------------------------------------------------- real in-process service
def test_reply_evidence_against_the_real_service(world, tmp_path):
    from hivemind.runtime_messaging import post_reply
    from test_agent_recovery import bound_client

    client = bound_client(world)
    sender = world.agent("Peer")
    worker = world.agent("Worker")
    run = world.run(worker)
    message = world.call("POST", "messages", {
        "project_id": world.project, "message_id": "msg-real-1", "from": sender["id"],
        "to": worker["id"], "thread_id": "thread-1", "type": "request",
        "content": {"media_type": "text/plain", "text": "Ping?"}, "ttl_seconds": 3600})
    gate = Reconciler(Journal(tmp_path / "j.sqlite"), client,
                      project_id=world.project, agent_id=worker["id"], run_id=run["id"],
                      policy=enabled())
    decision = {"reply_text": "Pong"}
    outcome, _ = gate._reply_evidence(message, decision, kind="response")
    assert outcome == PROVED_NOT_ATTEMPTED  # canonical 404, not an inferred absence
    posted = post_reply(client, world.project, worker["id"], run["id"], message, "Pong")
    outcome, evidence = gate._reply_evidence(message, decision, kind="response")
    assert outcome == PROVED_APPLIED and evidence["reply_message_id"] == posted["id"]
    outcome, evidence = gate._reply_evidence(message, {"reply_text": "different"}, kind="response")
    assert outcome == AMBIGUOUS and "content.text" in evidence["conflict"]
    gate.journal.close()


# ---------------------------------------------------------- task-create recovery
from hivemind.recovery_automation import (COMMAND_RECEIPTS_WIRING_NEED,  # noqa: E402
                                          install)
from hivemind.storage import digest as payload_digest  # noqa: E402


def make_task_create(journal, *, crash="timeout", payload=None):
    payload = payload or {"project_id": PROJECT, "title": "Recovered task"}
    journal.begin_operation("task-op", "POST", "tasks", payload, safe_to_retry=True)
    if crash == "timeout":
        journal.mark_attempt("task-op")
        journal.fail_operation("task-op", TimeoutError("lost after dispatch"))
    elif crash == "4xx":
        journal.mark_attempt("task-op")
        journal.fail_operation("task-op", status_error(409))
    return payload


class Receipts:
    """Authoritative server-side command ledger stub."""

    def __init__(self, entries=(), authoritative=True):
        self.entries = dict(entries)
        self.authoritative = authoritative
        self.calls = []

    def __call__(self, operation, key, payload_sha256):
        self.calls.append((operation, key, payload_sha256))
        entry = self.entries.get((operation, key))
        if entry is None:
            return {"found": False, "authoritative": self.authoritative}
        response = entry["response"]
        return {"found": True, "authoritative": self.authoritative,
                "payload_sha256": entry.get("payload_sha256", payload_sha256),
                "resource_id": entry.get("resource_id", response.get("id")),
                "external_effects_proved": entry.get("external_effects_proved", False),
                "response": response}


def test_task_create_recovered_from_authoritative_receipt(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        payload = make_task_create(journal)
        record = journal.operation("task-op")
        assert record["state"] == UNKNOWN
        response = {"id": "task-9", "project_id": PROJECT, "title": payload["title"]}
        receipts = Receipts({("task-create", record["command_key"]):
                             {"response": response}})
        gate = reconciler(journal)
        recovered = gate.recover_task_create("task-op", receipts=receipts)
        assert recovered["outcome"] == PROVED_APPLIED
        assert recovered["task_id"] == "task-9"
        # The completed result is EXACTLY the recorded canonical response.
        assert journal.operation("task-op")["result"] == response
        # The lookup was bound to the exact durable key and payload hash.
        assert receipts.calls == [("task-create", record["command_key"],
                                   payload_digest(payload))]
        audits = journal.recovery_audits("task-op")
        assert audits[0]["classification"] == "proved_applied"
        assert audits[0]["decision"] == "completed"


def test_task_create_receipt_proves_never_applied(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        make_task_create(journal)
        record = journal.operation("task-op")
        gate = reconciler(journal)
        recovered = gate.recover_task_create(
            "task-op", receipts=Receipts())  # authoritative absence
        assert recovered["outcome"] == PROVED_NOT_ATTEMPTED
        reopened = journal.operation("task-op")
        assert reopened["state"] == PENDING  # deliberate retry under the SAME key
        assert reopened["command_key"] == record["command_key"]
        audits = journal.recovery_audits("task-op")
        assert audits[0]["classification"] == "proved_not_attempted"
        assert audits[0]["decision"] == "not_applied"


def test_task_create_4xx_is_not_no_effect_without_receipts(tmp_path):
    """A generic 4xx or quiescence is never interpreted as 'no effect'."""
    with Journal(tmp_path / "j.sqlite") as journal:
        make_task_create(journal, crash="4xx")
        assert journal.operation("task-op")["state"] == PENDING
        gate = reconciler(journal)
        with pytest.raises(UnknownOutcome):  # receipts not wired: stays as recorded
            gate.recover_task_create("task-op")
        # An authoritative receipt still completes a 4xx-pending operation.
        response = {"id": "task-committed", "project_id": PROJECT, "title": "t"}
        record = journal.operation("task-op")
        recovered = gate.recover_task_create("task-op", receipts=Receipts(
            {("task-create", record["command_key"]): {"response": response}}))
        assert recovered["outcome"] == PROVED_APPLIED
        assert journal.operation("task-op")["state"] == COMPLETED


def test_task_create_never_fabricates_ids_or_trusts_nonauthoritative(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        make_task_create(journal)
        gate = reconciler(journal)
        # Non-authoritative absence is not evidence.
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op",
                                     receipts=Receipts(authoritative=False))
        # A receipt without a canonical task record never invents one.
        record = journal.operation("task-op")
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op", receipts=Receipts(
                {("task-create", record["command_key"]): {"response": {"note": "?"}}}))
        # A payload-hash conflict is ambiguity, not proof.
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op", receipts=Receipts(
                {("task-create", record["command_key"]):
                 {"response": {"id": "task-x"}, "payload_sha256": "0" * 64}}))
        # A resource_id mismatch (receipt for a different resource) is ambiguity.
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op", receipts=Receipts(
                {("task-create", record["command_key"]):
                 {"response": {"id": "task-x"}, "resource_id": "task-other"}}))
        assert journal.operation("task-op")["state"] == UNKNOWN
        # A raising lookup proves nothing either.
        def boom(operation, key, payload_sha256):
            raise ConnectionError("ledger unreachable")
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op", receipts=boom)
        assert journal.operation("task-op")["state"] == UNKNOWN


def test_task_create_404_receipt_is_never_not_attempted(tmp_path):
    """Absent-not-proof: the endpoint's 404 must not infer non-application."""
    from hivemind.recovery_automation import command_receipts

    class Endpoint:
        def __init__(self, receipts=()):
            self.receipts = dict(receipts)
            self.paths = []

        def request(self, method, path, body=None, key=None):
            self.paths.append(path)
            if path.startswith("commands/receipt?"):
                from urllib.parse import parse_qs, urlparse
                query = parse_qs(urlparse(path).query)
                receipt = self.receipts.get((query["operation"][0], query["key"][0]))
                if receipt is None:
                    raise HttpError(404)
                return receipt
            raise HttpError(404)

    with Journal(tmp_path / "j.sqlite") as journal:
        payload = make_task_create(journal)
        record = journal.operation("task-op")
        gate = reconciler(journal)
        # 404: stays unknown, never reopened, never completed.
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create(
                "task-op", receipts=command_receipts(Endpoint()))
        assert journal.operation("task-op")["state"] == UNKNOWN
        # A committed receipt through the real adapter shape completes exactly.
        receipt = {"state": "committed", "operation": "task-create",
                   "key": record["command_key"],
                   "payload_sha256": payload_digest(payload),
                   "response_sha256": "f" * 64, "resource_id": "task-real",
                   "resource": {"id": "task-real", "title": payload["title"]},
                   "current": {"id": "task-real", "title": payload["title"],
                               "status": "running"},
                   "external_effects_proved": False}
        endpoint = Endpoint({("task-create", record["command_key"]): receipt})
        recovered = gate.recover_task_create("task-op",
                                             receipts=command_receipts(endpoint))
        assert recovered["outcome"] == PROVED_APPLIED
        assert recovered["task_id"] == "task-real"
        # The current authorized projection is the result, exactly as served.
        assert journal.operation("task-op")["result"]["status"] == "running"
        assert endpoint.paths[0].startswith("commands/receipt?")
        # A receipt that echoes a different key is not evidence.
        bad = Endpoint({("task-create", record["command_key"]):
                        {**receipt, "key": "other-key"}})
        journal.fail_operation("task-op", TimeoutError("again"))  # back to unknown
        journal.resolve_operation("task-op", PENDING)
        journal.mark_attempt("task-op")
        journal.fail_operation("task-op", TimeoutError("again"))
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op", receipts=command_receipts(bad))


def test_task_create_payload_normalization_matches_server_checksum(tmp_path):
    """The server checksums the Pydantic-canonicalized payload (defaults applied)."""
    with Journal(tmp_path / "j.sqlite") as journal:
        payload = make_task_create(journal)  # durable body lacks server defaults
        record = journal.operation("task-op")

        def normalize(body):
            return {"project_id": body["project_id"], "title": body["title"],
                    "description": "", "scope_ids": []}  # TaskCreate.model_dump shape

        canonical_payload = normalize(payload)
        response = {"id": "task-norm", "title": payload["title"]}
        receipts = Receipts({("task-create", record["command_key"]):
                             {"response": response,
                              "payload_sha256": payload_digest(canonical_payload)}})
        gate = reconciler(journal)
        # Without normalization the hashes differ: ambiguity, never a guess.
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op", receipts=receipts)
        assert journal.operation("task-op")["state"] == UNKNOWN
        recovered = gate.recover_task_create("task-op", receipts=receipts,
                                             normalize=normalize)
        assert recovered["outcome"] == PROVED_APPLIED
        # The adapter was consulted with the NORMALIZED payload hash.
        assert receipts.calls[-1][2] == payload_digest(canonical_payload)
        # A failing model binding proves nothing.
        journal2 = journal
        journal2.begin_operation("task-op-2", "POST", "tasks", payload,
                                 safe_to_retry=True)
        journal2.mark_attempt("task-op-2")
        journal2.fail_operation("task-op-2", TimeoutError("lost"))
        with pytest.raises(UnknownOutcome):
            gate.recover_task_create("task-op-2", receipts=receipts,
                                     normalize=lambda body: 1 / 0)
        assert journal2.operation("task-op-2")["state"] == UNKNOWN


def test_task_create_recovery_refusals(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("get-op", "GET", "tasks")
        journal.begin_operation("redacted", "POST", "tasks",
                                {"title": "x", "token": "secret-token-value-123"},
                                allow_sensitive_payload=True)
        journal.mark_attempt("redacted")
        journal.fail_operation("redacted", TimeoutError("lost"))
        gate = reconciler(journal)
        with pytest.raises(RecoveryError):
            gate.recover_task_create("get-op", receipts=Receipts())  # not task-create
        with pytest.raises(UnknownOutcome):  # redacted payload cannot be hash-bound
            gate.recover_task_create("redacted", receipts=Receipts())
        disabled = Reconciler(journal, project_id=PROJECT, agent_id=AGENT, run_id=RUN,
                              policy=RecoveryPolicy())
        with pytest.raises(RecoveryError):
            disabled.recover_task_create("redacted", receipts=Receipts())
        # A never-dispatched task-create is proved not attempted from the record.
        journal.begin_operation("fresh-op", "POST", "tasks", {"title": "y"})
        assert gate.recover_task_create("fresh-op")["outcome"] == PROVED_NOT_ATTEMPTED


# ----------------------------------------------------- wedged auxiliary recovery
def make_wedged_auxiliary(journal, *, task_id="t-1", execution_id="e-1"):
    journal.begin_operation("aux-op", "HANDLER", "auxiliary-task",
                            {"project_id": PROJECT, "agent_id": AGENT, "run_id": RUN,
                             "task_id": task_id, "execution_id": execution_id},
                            safe_to_retry=False)
    journal.mark_attempt("aux-op")
    journal.fail_operation("aux-op", TimeoutError("wedged"))


def bound_finding(outcome, **overrides):
    evidence = {"project_id": PROJECT, "agent_id": AGENT, "run_id": RUN,
                "task_id": "t-1", "execution_id": "e-1"}
    evidence.update(overrides.pop("evidence", {}))
    if "proof" not in evidence and outcome == PROVED_NOT_ATTEMPTED:
        evidence["proof"] = {"kind": "pre_dispatch_non_invocation", "quiescent": True}
    if "proof" not in evidence and outcome == PROVED_APPLIED \
            and overrides.get("result") is not None:
        evidence["proof"] = {"kind": "durable_result",
                             "result_sha256": payload_digest(overrides["result"])}
    finding = {"operation_id": "aux-op", "outcome": outcome, "evidence": evidence}
    finding.update(overrides)
    return finding


def allow_gate(calls=None):
    def gate(phase, context):
        if calls is not None:
            calls.append((phase, context))
        return True
    return gate


def test_auxiliary_proved_not_invoked_resolves_under_gate(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        make_wedged_auxiliary(journal)
        gate_calls = []
        gate = reconciler(journal)
        record = gate.resolve_auxiliary(
            "aux-op", bound_finding(PROVED_NOT_ATTEMPTED), gate=allow_gate(gate_calls))
        assert record["state"] == PENDING
        assert gate_calls[0][0] == "resolve"
        assert gate_calls[0][1]["task_id"] == "t-1"
        audits = journal.recovery_audits("aux-op")
        assert audits[0]["classification"] == "proved_not_attempted"
        assert audits[0]["decision"] == "not_applied"


def test_auxiliary_proved_applied_requires_verified_result(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        make_wedged_auxiliary(journal)
        gate = reconciler(journal)
        # Applied without a durable result proof never resolves.
        with pytest.raises(UnknownOutcome):
            gate.resolve_auxiliary("aux-op", bound_finding(PROVED_APPLIED),
                                   gate=allow_gate())
        # A durable_result proof without the verified result also never resolves.
        with pytest.raises((UnknownOutcome, RecoveryError)):
            gate.resolve_auxiliary("aux-op", bound_finding(
                PROVED_APPLIED, evidence={"proof": {"kind": "durable_result",
                                                    "result_sha256": "0" * 64}}),
                gate=allow_gate())
        result = {"id": "t-1", "status": "completed"}
        record = gate.resolve_auxiliary(
            "aux-op", bound_finding(PROVED_APPLIED, result=result), gate=allow_gate())
        assert record["state"] == COMPLETED
        assert record["result"]["status"] == "completed"


def test_auxiliary_requires_durable_proof_and_current_gate(tmp_path):
    """An operator proposal or a bare caller claim is never authority."""
    with Journal(tmp_path / "j.sqlite") as journal:
        make_wedged_auxiliary(journal)
        gate = reconciler(journal)
        # No gate at all: refused outright.
        with pytest.raises(RecoveryError):
            gate.resolve_auxiliary(
                "aux-op", bound_finding(PROVED_NOT_ATTEMPTED), gate=None)
        # No durable proof object: stays on hold even with a passing gate.
        finding = bound_finding(PROVED_NOT_ATTEMPTED)
        finding["evidence"].pop("proof")
        with pytest.raises(UnknownOutcome):
            gate.resolve_auxiliary("aux-op", finding, gate=allow_gate())
        # A proof whose durable result hash does not match the claimed result.
        mismatched = bound_finding(PROVED_APPLIED, result={"id": "t-1"})
        mismatched["evidence"]["proof"] = {"kind": "durable_result",
                                           "result_sha256": "0" * 64}
        with pytest.raises(UnknownOutcome):
            gate.resolve_auxiliary("aux-op", mismatched, gate=allow_gate())
        # A wrong proof kind for the claimed outcome is not proof.
        wrong_kind = bound_finding(PROVED_NOT_ATTEMPTED)
        wrong_kind["evidence"]["proof"] = {"kind": "durable_result",
                                           "result_sha256": "0" * 64}
        with pytest.raises(UnknownOutcome):
            gate.resolve_auxiliary("aux-op", wrong_kind, gate=allow_gate())
        # The gate must be consulted at mutation time and must affirm.
        calls = []
        record = gate.resolve_auxiliary(
            "aux-op", bound_finding(PROVED_NOT_ATTEMPTED,
                                    evidence={"proof": {
                                        "kind": "command_receipt",
                                        "resource_id": "t-1",
                                        "payload_sha256": "a" * 64}}),
            gate=allow_gate(calls))
        assert record["state"] == PENDING and calls[0][0] == "resolve"


def test_auxiliary_gate_refusal_and_unknown_remains_hold(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        make_wedged_auxiliary(journal)
        gate = reconciler(journal)
        with pytest.raises(RecoveryError):  # lease/controls no longer held
            gate.resolve_auxiliary("aux-op", bound_finding(PROVED_NOT_ATTEMPTED),
                                   gate=lambda phase, context: False)
        assert journal.operation("aux-op")["state"] == UNKNOWN
        # Ambiguous and stale findings are holds, never resolutions.
        for outcome in (AMBIGUOUS, STALE_REPLACED):
            with pytest.raises(UnknownOutcome):
                gate.resolve_auxiliary("aux-op", bound_finding(outcome))
        # Foreign identity binding never resolves, even with a proved outcome.
        with pytest.raises(UnknownOutcome):
            gate.resolve_auxiliary("aux-op", bound_finding(
                PROVED_NOT_ATTEMPTED, evidence={"run_id": "run-other"}))
        with pytest.raises(UnknownOutcome):
            gate.resolve_auxiliary("aux-op", bound_finding(
                PROVED_NOT_ATTEMPTED, evidence={"execution_id": "e-other"}))
        assert journal.operation("aux-op")["state"] == UNKNOWN


def test_auxiliary_resolution_requires_opt_in_and_unknown_state(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        journal.begin_operation("aux-op", "HANDLER", "auxiliary-task", {"run_id": RUN},
                                safe_to_retry=False)
        journal.complete_operation("aux-op", {"id": "t-1"})
        gate = reconciler(journal)
        with pytest.raises(RecoveryError):  # not unknown
            gate.resolve_auxiliary("aux-op", bound_finding(PROVED_NOT_ATTEMPTED))
        disabled = Reconciler(journal, project_id=PROJECT, agent_id=AGENT, run_id=RUN,
                              policy=RecoveryPolicy())
        with pytest.raises(RecoveryError):
            disabled.resolve_auxiliary("aux-op", bound_finding(PROVED_NOT_ATTEMPTED))


# ------------------------------------------------------------- public handle
def test_install_handle_proposals_status_tick(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        make_task_create(journal)
        journal.begin_operation("other-op", "POST", "messages", {"to": AGENT})
        journal.mark_attempt("other-op")
        journal.fail_operation("other-op", TimeoutError("lost"))
        handle = install(journal=journal, project_id=PROJECT, agent_id=AGENT,
                         run_id=RUN, policy=enabled(), principal={"id": "p-1"},
                         command="recovery")
        proposals = handle.proposals()
        by_op = {p.operation_id: p for p in proposals}
        assert by_op["task-op"].action == "recover_task_create"
        assert by_op["task-op"].requires == ("command_receipts_endpoint",)
        assert by_op["other-op"].action == "resolve"
        assert all(p.outcome == AMBIGUOUS for p in proposals)
        assert all(p.proposal_id and p.created_at for p in proposals)
        status = handle.status()
        assert status.unknown == 2 and status.attempting == 0
        assert status.policy["enabled"] is True
        tick = handle.tick()
        assert tick["report"]["classified"] == 2 and tick["report"]["resolved"] == []
        assert tick["status"]["unknown"] == 2
        description = handle.describe()
        assert {a["action"] for a in description["actions"]} >= {
            "recovery.classify", "recovery.reconcile", "recovery.recover_task_create",
            "recovery.resolve_auxiliary", "recovery.tick"}
        assert description["wiring_needs"] == [COMMAND_RECEIPTS_WIRING_NEED]
        # A probe that proves the task-create resolves it via tick.
        tick = handle.tick(probes=[(
            lambda record: record["operation_id"] == "other-op",
            lambda record, client: {"outcome": PROVED_NOT_ATTEMPTED,
                                    "evidence": {"source": "test"}})])
        assert tick["report"]["resolved"] == ["other-op"]
        assert handle.status().unknown == 1


def test_install_handle_requires_enabled_policy(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        handle = install(journal=journal, project_id=PROJECT, agent_id=AGENT,
                         run_id=RUN, policy=RecoveryPolicy())
        with pytest.raises(RecoveryError):
            handle.proposals()
        with pytest.raises(RecoveryError):
            handle.tick()
