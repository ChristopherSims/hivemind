"""Offline tests for opt-in runtime messaging: durable dedup, confirmed vs unknown
effects, no ack on unknown outcomes, private recipient visibility, safe replies, and
bounded conversation workflows. No vendor session, paid call, or network is used; one
test exercises the real in-process service via the ``world`` fixture."""

import json
import re
from pathlib import Path

import pytest

from hivemind.agent_session import HandlerJournal, SessionLock
from hivemind.recovery import Journal, UnknownOutcome, derive_operation_id
from hivemind.runtime_messaging import (
    MESSAGE_EFFECT_KIND,
    MESSAGE_PHASE_ID,
    MESSAGE_RESULT_INSTRUCTION,
    NOTIFICATION_PHASE_ID,
    NOTIFICATION_RESULT_INSTRUCTION,
    ConversationBridge,
    MessagingError,
    MessagingPolicy,
    MessageNotHandled,
    RuntimeMessageGateway,
    UnauthorizedNotification,
    UnauthorizedRecipient,
    UnauthorizedSender,
    VendorMessagingExecutor,
)
from hivemind.storage import uid

PROJECT = "project-1"
AGENT = "agent-1"
PEER = "agent-2"
RUN = "run-1"


def make_message(message_id="msg-1", sender=PEER, to=AGENT, **overrides):
    message = {"id": message_id, "project_id": PROJECT, "from": sender, "to": to,
               "type": "request", "thread_id": "thread-1", "correlation_id": "corr-1",
               "content": {"media_type": "text/plain", "text": "Ping?"}, "ttl_seconds": 3600}
    message.update(overrides)
    return message


def make_notice(notice_id="note-1", kind="board_mention", recipient=AGENT, **overrides):
    notice = {"id": notice_id, "project_id": PROJECT, "type": kind,
              "recipient_agent_id": recipient, "state": "available"}
    notice.update(overrides)
    return notice


class RecordingAdapter:
    """Fake-contract adapter: records invocations, can crash after a side effect."""

    def __init__(self, crash=False, reply_text="composed reply"):
        self.messages = []
        self.notices = []
        self.composed = []
        self.effects = []
        self.crash = crash
        self.reply_text = reply_text

    def handle_message(self, receipt, message):
        self.messages.append((receipt, message))
        self.effects.append(message["id"])
        if self.crash:
            raise RuntimeError("crash after side effect")
        return {"echo": message["content"]["text"]}

    def review_notification(self, notice):
        self.notices.append(notice)
        self.effects.append(notice["id"])
        if self.crash:
            raise RuntimeError("crash after side effect")
        return {"reviewed": notice["type"]}

    def compose_reply(self, request_view):
        self.composed.append(request_view["id"])
        return self.reply_text


class FakeMessageClient:
    """Minimal message surface: GET one message, idempotent POST replies."""

    def __init__(self, messages=()):
        self.messages = {message["id"]: dict(message) for message in messages}
        self.posts = {}

    def request(self, method, path, body=None, key=None):
        if method == "GET" and path.startswith("messages/"):
            return dict(self.messages[path[len("messages/"):]])
        if method == "POST" and path == "messages":
            existing = self.posts.get(body["message_id"])
            if existing is not None:
                assert existing == {**body, "id": body["message_id"]}, (
                    "same reply ID must replay the identical body")
                return dict(existing)
            self.posts[body["message_id"]] = {**body, "id": body["message_id"]}
            return dict(self.posts[body["message_id"]])
        raise AssertionError(f"unexpected request {method} {path}")


def policy(**overrides):
    base = {"enabled": True, "allowed_senders": [PEER],
            "allowed_notification_types": ["board_mention"], "allow_replies": True}
    base.update(overrides)
    return MessagingPolicy(**base)


def gateway(client=None, adapter=None, journal=None, pol=None, tmp_path=None):
    handler_journal = None
    if journal == "real":
        handler_journal = HandlerJournal(Journal(tmp_path / "journal.sqlite"), PROJECT, AGENT)
    return RuntimeMessageGateway(client or FakeMessageClient(), PROJECT, AGENT, RUN,
                                 adapter or RecordingAdapter(), policy=pol or policy(),
                                 handler_journal=handler_journal)


def deliver_loop(receipts, handler):
    """Mirror of sdk.Client.deliver_inbox ack/fail/unknown semantics."""
    acked, failed = [], []
    for receipt in receipts:
        try:
            handler(receipt)
        except UnknownOutcome:
            raise
        except Exception:
            failed.append(receipt.get("message_id") or receipt.get("id"))
        else:
            acked.append(receipt.get("message_id") or receipt.get("id"))
    return acked, failed


def test_disabled_policy_registers_no_handlers_and_rejects_work():
    handle = gateway(pol=MessagingPolicy())
    assert handle.message_handler is None and handle.notification_handler is None
    with pytest.raises(MessagingError):
        handle.handle_message(make_message())
    with pytest.raises(MessagingError):
        handle.review_notification(make_notice())


def test_missing_adapter_hook_registers_no_handler():
    class Bare:
        pass

    handle = gateway(adapter=Bare())
    assert handle.message_handler is None and handle.notification_handler is None
    with pytest.raises(MessagingError):
        handle.handle_message(make_message())


def test_private_recipient_visibility_and_sender_authorization():
    handle = gateway()
    with pytest.raises(UnauthorizedRecipient):
        handle.handle_message(make_message(to="agent-other"))
    with pytest.raises(UnauthorizedSender):
        handle.handle_message(make_message(sender="intruder"))
    with pytest.raises(UnauthorizedRecipient):
        handle.handle_message(make_message(project_id="project-other"))
    with pytest.raises(UnauthorizedRecipient):
        handle.review_notification(make_notice(recipient="agent-other"))
    with pytest.raises(UnauthorizedNotification):
        handle.review_notification(make_notice(kind="memory_review"))
    assert handle.adapter.messages == [] and handle.adapter.notices == []


def test_minimal_receipt_fetches_full_message_and_confirms():
    client = FakeMessageClient([make_message()])
    handle = gateway(client=client)
    outcome = handle.handle_message({"message_id": "msg-1", "delivery_id": "delivery-1"})
    assert outcome["handled"] and outcome["result"]["echo"] == "Ping?"
    assert outcome["message_id"] == "msg-1" and outcome["delivery_id"] == "delivery-1"
    receipt, message = handle.adapter.messages[0]
    assert receipt["delivery_id"] == "delivery-1" and message["from"] == PEER


def test_stable_id_dedup_survives_redelivery_and_restart(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    handle = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN,
                                   RecordingAdapter(), policy=policy(),
                                   handler_journal=HandlerJournal(journal, PROJECT, AGENT))
    first = handle.handle_message({"message_id": "msg-1", "delivery_id": "delivery-1",
                                   **make_message()})
    assert first["handled"] and not first["deduplicated"] and first["durable"]
    journal.close()
    # Redelivery under a new delivery ID after a restart replays the record, not the effect.
    journal = Journal(tmp_path / "journal.sqlite")
    adapter = RecordingAdapter()
    resumed = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN, adapter,
                                    policy=policy(),
                                    handler_journal=HandlerJournal(journal, PROJECT, AGENT))
    second = resumed.handle_message({"message_id": "msg-1", "delivery_id": "delivery-2",
                                     **make_message()})
    assert second["deduplicated"] and adapter.effects == []
    journal.close()


def test_unknown_outcome_is_never_acked_failed_or_reapplied(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    adapter = RecordingAdapter(crash=True)
    handle = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN, adapter,
                                   policy=policy(),
                                   handler_journal=HandlerJournal(journal, PROJECT, AGENT))
    receipt = {"message_id": "msg-1", "delivery_id": "delivery-1", **make_message()}
    with pytest.raises(UnknownOutcome):
        deliver_loop([receipt], handle.handle_message)
    assert adapter.effects == ["msg-1"]  # The effect happened; the outcome is unknown.
    journal.close()

    journal = Journal(path)
    handle = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN,
                                   RecordingAdapter(), policy=policy(),
                                   handler_journal=HandlerJournal(journal, PROJECT, AGENT))
    with pytest.raises(UnknownOutcome):
        handle.handle_message(receipt)  # Redelivery stays blocked, never double-applied.
    operation = journal.operations(state="unknown")[0]
    journal.resolve_unknown(operation["operation_id"], "completed",
                            "Inspected: the effect was applied",
                            {"operation_id": operation["operation_id"], "observed": "applied"})
    settled = handle.handle_message(receipt)
    assert settled["deduplicated"] and handle.adapter.effects == []
    journal.close()


def test_unknown_without_journal_still_refuses_confirmation():
    handle = gateway(adapter=RecordingAdapter(crash=True))  # no handler journal
    acked, failed = [], []
    with pytest.raises(UnknownOutcome):
        deliver_loop([{"message_id": "msg-1", "delivery_id": "d-1", **make_message()}],
                     handle.handle_message)
    assert acked == [] and failed == []


def test_definite_rejection_marks_failure_not_ack():
    handle = gateway()
    acked, failed = deliver_loop([{"message_id": "msg-1", "delivery_id": "d-1",
                                   **make_message(sender="intruder")}], handle.handle_message)
    assert acked == [] and failed == ["msg-1"]


def test_notification_confirmed_and_deduplicated(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    handle = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN,
                                   RecordingAdapter(), policy=policy(),
                                   handler_journal=HandlerJournal(journal, PROJECT, AGENT))
    outcome = handle.review_notification(make_notice())
    assert outcome["handled"] and outcome["result"]["reviewed"] == "board_mention"
    again = handle.review_notification(make_notice())
    assert again["deduplicated"]
    assert len(handle.adapter.notices) == 1
    journal.close()


def test_reply_requires_opt_in_and_private_recipient():
    source = make_message()
    with pytest.raises(MessagingError):
        gateway(pol=policy(allow_replies=False)).reply(source, "Pong")
    with pytest.raises(UnauthorizedRecipient):
        gateway().reply(make_message(to="agent-other"), "Pong")
    with pytest.raises(MessagingError):
        gateway().reply(source, "   ")


def test_reply_is_correlated_bounded_and_replays_under_a_stable_id():
    client = FakeMessageClient()
    handle = gateway(client=client)
    source = make_message()
    reply = handle.reply(source, "Pong" * 10000)
    assert reply["to"] == PEER and reply["from"] == AGENT
    assert reply["thread_id"] == "thread-1" and reply["correlation_id"] == "corr-1"
    assert reply["in_reply_to"] == "msg-1" and reply["sender_run_id"] == RUN
    assert len(reply["content"]["text"]) == handle.policy.max_reply_chars
    replay = handle.reply(source, "Pong" * 10000)
    assert replay["id"] == reply["id"] and len(client.posts) == 1


class FakeConversationClient:
    """Stateful stand-in for the conversation-requests routes."""

    def __init__(self):
        self.store = {}
        self.posts = 0

    def request(self, method, path, body=None, key=None):
        if method == "POST" and path == "conversation-requests":
            self.posts += 1
            request_id = "workflow_" + body["message_id"]
            record = {"id": request_id, "state": "open", "revision": 1,
                      "from_agent": body["from_agent"], "to_agent": body["to"],
                      "response_overdue": False, "response_deadline_seconds":
                      body.get("response_deadline_seconds"), **{}}
            record.update({k: body[k] for k in ("thread_id", "content")})
            self.store[request_id] = record
            return dict(record)
        match = re.fullmatch(r"conversation-requests/([^:]+)(?::(\w+))?", path)
        assert match, f"unexpected request {method} {path}"
        record = self.store[match.group(1)]
        action = match.group(2)
        if method == "GET":
            return dict(record)
        if action == "respond":
            assert record["state"] in ("open", "escalated")
            record.update(state="answered", revision=record["revision"] + 1,
                          answer=body["content"]["text"])
            return dict(record)
        if action == "reconcile":
            return {**record, "reconciled": False}
        if action == "escalate":
            assert body["expected_revision"] == record["revision"]
            assert record.get("response_overdue"), "escalation requires an overdue deadline"
            record.update(state="escalated", revision=record["revision"] + 1)
            return dict(record)
        raise AssertionError(f"unexpected action {action}")


def bridge(client, adapter=None, pol=None, **overrides):
    base = {"sleep": lambda seconds: None, "max_wait_seconds": 60, "poll_interval": 1}
    base.update(overrides)
    return ConversationBridge(client, PROJECT, AGENT, RUN, policy=pol or policy(),
                              adapter=adapter or RecordingAdapter(), **base)


def test_bridge_open_answer_and_compose_hook():
    client = FakeConversationClient()
    adapter = RecordingAdapter()
    handle = bridge(client, adapter)
    opened = handle.open_request(PEER, "Question?")
    assert opened["state"] == "open" and opened["response_deadline_seconds"] == 60
    # The addressee side answers; a closed request answers again as a no-op.
    peer = ConversationBridge(client, PROJECT, PEER, None, policy=policy(), adapter=adapter,
                              sleep=lambda s: None, max_wait_seconds=60, poll_interval=1)
    answered = peer.answer(opened["id"])
    assert answered["answered"] and answered["state"] == "answered"
    assert adapter.composed == [opened["id"]]
    assert answered["answer"] == "composed reply"
    assert peer.answer(opened["id"])["answered"] is False
    outcome = handle.await_answer(opened["id"])
    assert outcome["wait_result"] == "answered"


def test_bridge_answer_requires_addressee_opt_in_and_text():
    client = FakeConversationClient()
    handle = bridge(client)
    opened = handle.open_request(PEER, "Question?")
    with pytest.raises(UnauthorizedRecipient):
        handle.answer(opened["id"], "self answer")
    peer = bridge(client, pol=policy(allow_replies=False))
    with pytest.raises(MessagingError):
        peer.answer(opened["id"], "not allowed")
    peer = bridge(client)
    with pytest.raises(MessagingError):
        peer.answer(opened["id"], "")  # bound_text rejects empty replies
    no_hook = ConversationBridge(client, PROJECT, PEER, None, policy=policy(), adapter=None,
                                 sleep=lambda s: None, max_wait_seconds=60, poll_interval=1)
    with pytest.raises(MessagingError):
        no_hook.answer(opened["id"])  # no text and no compose_reply hook


def test_bridge_await_is_bounded_and_escalation_is_one_time():
    client = FakeConversationClient()
    handle = bridge(client, max_wait_seconds=3, poll_interval=1)
    opened = handle.open_request(PEER, "Question?")
    clock = [0.0]

    def tick(seconds):
        clock[0] += seconds
        record = client.store[opened["id"]]
        record["response_overdue"] = True

    waiting = ConversationBridge(client, PROJECT, AGENT, RUN, policy=policy(),
                                 adapter=RecordingAdapter(), sleep=tick,
                                 clock=lambda: clock[0], max_wait_seconds=3, poll_interval=1)
    outcome = waiting.await_answer(opened["id"])
    assert outcome["wait_result"] == "timeout" and outcome["polls"] <= 4
    escalated = handle.escalate_overdue(opened["id"])
    assert escalated["escalated"] and escalated["state"] == "escalated"
    repeat = handle.escalate_overdue(opened["id"])
    assert repeat["escalated"] is False  # one-time: no second escalation record


class FakeCanonicalClient(FakeMessageClient):
    """Adds the canonical task APIs (create/start/checkpoint) with real fencing.

    Command keys replay identically, mirroring the server's idempotency, so a retried
    create/start returns the same task instead of duplicating it.
    """

    def __init__(self, messages=()):
        super().__init__(messages)
        self.tasks = {}
        self.checkpoints = []
        self.command_cache = {}
        self.task_seq = 0
        self.exec_seq = 0

    def request(self, method, path, body=None, key=None):
        if key and key in self.command_cache:
            return self.command_cache[key]
        result = self._dispatch(method, path, body)
        if key and method == "POST":
            self.command_cache[key] = result
        return result

    def _dispatch(self, method, path, body):
        if method == "POST" and path == "tasks":
            self.task_seq += 1
            task = {"id": f"task-{self.task_seq}", "project_id": body["project_id"],
                    "title": body["title"], "description": body["description"],
                    "scope_ids": list(body["scope_ids"]), "status": "queued", "revision": 1,
                    "execution": None}
            self.tasks[task["id"]] = task
            return dict(task)
        match = re.fullmatch(r"tasks/([^:]+)(?::(\w+))?", path)
        if match:
            task = self.tasks[match.group(1)]
            if method == "GET":
                return dict(task)
            if match.group(2) == "start":
                assert body["expected_revision"] == task["revision"], "start fencing"
                assert task["status"] not in ("completed", "failed"), "terminal task"
                self.exec_seq += 1
                task.update(status="running", revision=task["revision"] + 1,
                            execution={"id": f"exec-{self.exec_seq}", "run_id": body["run_id"],
                                       "progress": 0})
                return dict(task)
            if match.group(2) == "checkpoint":
                execution = task["execution"]
                assert execution and execution["id"] == body["execution_id"], "stale execution"
                assert execution["run_id"] == body["run_id"], "stale run"
                assert body["expected_revision"] == task["revision"], "revision fencing"
                assert body["progress"] >= execution["progress"], "monotonic progress"
                if body["status"] == "completed":
                    assert body["progress"] == 100, "completed requires 100"
                execution["progress"] = body["progress"]
                task.update(status=body["status"], revision=task["revision"] + 1)
                self.checkpoints.append(dict(body))
                return dict(task)
        return super().request(method, path, body)


class FakeVendorRuntime:
    """Faithful fake of the official vendor adapter contract. No paid calls, no process.

    Mirrors ``hivemind.vendor_runtimes.VendorRuntime`` state discipline: one quiescent
    JSON state file outside any workspace, ``running``/``unknown`` block new turns, and a
    completed canonical task refuses re-execution pending reconciliation. The real
    checkpoint callback passed by the executor is exercised mid-turn.
    """

    tool_names = ["Read", "Grep", "Glob", "LS"]

    def __init__(self, state_dir, behavior=None, during_turn=None,
                 exec_lock_path=None, handler_lock_path=None):
        self.state_path = Path(state_dir) / "vendor-messaging.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.behavior = behavior if behavior is not None else {"handled": True,
                                                               "reply_text": "Pong"}
        self.during_turn = during_turn
        self.exec_lock_path = exec_lock_path
        self.handler_lock_path = handler_lock_path
        self.exec_lock_held = None
        self.handler_lock_free = None
        self.calls = []

    def _state(self):
        return json.loads(self.state_path.read_text()) if self.state_path.exists() else {}

    def _save(self, **fields):
        self.state_path.write_text(json.dumps({**self._state(), **fields}))

    def execute(self, task, context, checkpoint):
        state = self._state()
        if state.get("outcome") in ("running", "unknown"):
            raise UnknownOutcome("Reconcile interrupted vendor work before another turn")
        if state.get("outcome") == "completed" and state.get("task_id") == task["id"]:
            raise UnknownOutcome("Completed vendor work requires reconciliation")
        self._save(outcome="running", task_id=task["id"], run_id=context["run_id"],
                   execution_id=context["execution_id"], phase_id=context.get("phase_id"))
        self.calls.append({"task": task, "context": context})
        if self.exec_lock_path:
            try:
                with SessionLock(self.exec_lock_path):
                    self.exec_lock_held = False
            except RuntimeError:
                self.exec_lock_held = True  # the executor's per-op exclusion is active
        if self.handler_lock_path:
            try:
                with SessionLock(self.handler_lock_path):
                    self.handler_lock_free = True
            except RuntimeError:
                self.handler_lock_free = False  # no journal lock may be held mid-turn
        behavior = self.behavior
        if behavior == "timeout":
            self._save(outcome="unknown")
            raise TimeoutError("Vendor turn timed out")
        if behavior == "crash":
            self._save(outcome="unknown")
            raise ConnectionError("Vendor transport closed")
        if self.during_turn:
            self.during_turn(task, context)
        checkpoint("Vendor turn in progress", progress=50)  # the real canonical callback
        self._save(outcome="completed")
        result = {"status": "completed", "summary": "vendor turn completed"}
        if behavior != "hash-only":  # default VendorRuntime contract is hash-only
            result["message_result"] = behavior
        return result


def executor_journal(tmp_path):
    return HandlerJournal(Journal(tmp_path / "handler.sqlite"), PROJECT, AGENT)


def make_executor(tmp_path, behavior=None, client=None, lock_probe=False, **overrides):
    journal = executor_journal(tmp_path)
    base = {"allowed_senders": [PEER], "allow_replies": True}
    base.update(overrides)
    factory = base.pop("runtime_factory", None)
    if factory is None and "runtime" not in base:
        state_dir = tmp_path / "vendor-state"
        probe = {}
        if lock_probe:
            op_id = derive_operation_id("handler", MESSAGE_EFFECT_KIND, f"{PROJECT}:{AGENT}:msg-1")
            name = journal.journal.path.name + "." + op_id
            probe = {"exec_lock_path": journal.journal.path.with_name(name + ".exec"),
                     "handler_lock_path": journal.journal.path.with_name(name + ".handler")}
        base["runtime"] = FakeVendorRuntime(state_dir, behavior, **probe)
    elif factory is not None:
        base["runtime_factory"] = factory
    handle = VendorMessagingExecutor(client or FakeCanonicalClient(), PROJECT, AGENT, RUN,
                                     handler_journal=journal, **base)
    return handle, journal


def test_executor_requires_explicit_wiring(tmp_path):
    client = FakeMessageClient()
    journal = executor_journal(tmp_path)
    with pytest.raises(ValueError):
        VendorMessagingExecutor(client, PROJECT, AGENT, RUN)  # no runtime at all
    with pytest.raises(ValueError):
        VendorMessagingExecutor(client, PROJECT, AGENT, RUN, runtime=FakeVendorRuntime(tmp_path),
                                runtime_factory=lambda: None)  # both
    with pytest.raises(ValueError):
        VendorMessagingExecutor(client, PROJECT, AGENT, RUN,
                                runtime=FakeVendorRuntime(tmp_path))  # no journal
    with pytest.raises(ValueError):
        VendorMessagingExecutor(client, PROJECT, AGENT, None,
                                runtime=FakeVendorRuntime(tmp_path),
                                handler_journal=journal)  # no real run
    journal.journal.close()


def test_executor_turn_uses_canonical_task_records_and_replies_once(tmp_path):
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path, client=client, lock_probe=True)
    outcome = handle.handle_message({"message_id": "msg-1", "delivery_id": "d-1"},
                                    make_message())
    assert outcome["handled"] and not outcome["deduplicated"]
    runtime = handle._runtime()
    call = runtime.calls[0]
    # Canonical authority: the IDs are the server-issued records, not derived strings.
    task = client.tasks[outcome["task_id"]]
    assert outcome["task_id"] == task["id"] == call["task"]["id"]
    assert outcome["execution_id"] == task["execution"]["id"] == call["context"]["execution_id"]
    assert call["context"]["run_id"] == RUN and call["context"]["phase_id"] == MESSAGE_PHASE_ID
    assert call["task"]["scope_ids"] == []  # no-tools runtime: no file scopes
    assert MESSAGE_RESULT_INSTRUCTION in call["task"]["description"]
    assert "Ping?" in call["task"]["description"] and call["context"]["packet"]["message"]
    # Real checkpoint callback ran mid-turn; the task settled only after the reply.
    assert [c["status"] for c in client.checkpoints] == ["running", "completed"]
    assert client.checkpoints[0]["execution_id"] == outcome["execution_id"]
    assert task["status"] == "completed" and task["execution"]["progress"] == 100
    # The exclusion lock covers the vendor turn; no journal lock is held during it.
    assert runtime.exec_lock_held is True and runtime.handler_lock_free is True
    assert len(client.posts) == 1
    reply = next(iter(client.posts.values()))
    assert reply["to"] == PEER and reply["in_reply_to"] == "msg-1"

    # Redelivery: deduped end to end — no new task, no vendor re-run, no repost.
    again = handle.handle_message({"message_id": "msg-1", "delivery_id": "d-2"},
                                  make_message())
    assert again["deduplicated"] and again["task_id"] == task["id"]
    assert len(runtime.calls) == 1 and len(client.tasks) == 1 and len(client.posts) == 1
    journal.journal.close()


def test_executor_restart_after_completion_never_duplicates_reply(tmp_path):
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path, client=client)
    first = handle.handle_message({"message_id": "msg-1"}, make_message())
    assert len(client.posts) == 1 and len(client.tasks) == 1
    journal.journal.close()
    # Fresh journal object over the same file plus a fresh runtime over the same state dir.
    handle2, journal2 = make_executor(tmp_path, client=client)
    outcome = handle2.handle_message({"message_id": "msg-1"}, make_message())
    assert outcome["deduplicated"] and outcome["task_id"] == first["task_id"]
    assert len(handle2._runtime().calls) == 0 and len(client.tasks) == 1
    assert len(client.posts) == 1
    journal2.journal.close()


def test_executor_handled_false_is_never_acked_and_not_reexecuted(tmp_path):
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path, behavior={"handled": False, "reply_text": None},
                                    client=client)
    with pytest.raises(MessageNotHandled):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert client.posts == {}  # a false handled flag produces no reply
    task = next(iter(client.tasks.values()))
    assert task["status"] == "failed"  # settled canonically after the confirmed decision
    with pytest.raises(MessageNotHandled):
        handle.handle_message({"message_id": "msg-1"}, make_message())  # durable decision
    assert len(handle._runtime().calls) == 1 and len(client.tasks) == 1
    journal.journal.close()


def test_executor_hash_only_default_and_invalid_contracts_stay_unknown(tmp_path):
    for index, behavior in enumerate(("hash-only", {"handled": "yes", "reply_text": None},
                                      {"handled": True, "reply_text": 42})):
        client = FakeCanonicalClient()
        handle, journal = make_executor(tmp_path / f"case-{index}", behavior=behavior,
                                        client=client)
        with pytest.raises(UnknownOutcome):
            handle.handle_message({"message_id": "msg-1"}, make_message())
        assert client.posts == {}  # invalid/missing contract never confirms or replies
        # The turn intent was journaled BEFORE the model ran: a resolvable unknown record.
        unknown = journal.journal.operations(state="unknown")
        assert len(unknown) == 1 and unknown[0]["path"] == "runtime-message-turn"
        # A fresh executor instance is suppressed before ever reaching the runtime.
        fresh, _ = make_executor(tmp_path / f"case-{index}", behavior=behavior, client=client)
        # make_executor opens a second Journal on the same file; close the first handle's.
        with pytest.raises(UnknownOutcome):
            fresh.handle_message({"message_id": "msg-1"}, make_message())
        assert fresh._runtime().calls == []  # redelivery never re-executes the vendor turn
        fresh.handler_journal.journal.close()
        journal.journal.close()


def test_executor_timeout_is_unknown_resolvable_and_never_duplicates(tmp_path):
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path, behavior="timeout", client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert handle._runtime()._state()["outcome"] == "unknown"
    assert client.posts == {}
    # Restart: a fresh adapter instance sees the durable unknown and does not re-execute.
    fresh, fresh_journal = make_executor(tmp_path, behavior="timeout", client=client)
    with pytest.raises(UnknownOutcome):
        fresh.handle_message({"message_id": "msg-1"}, make_message())
    assert fresh._runtime().calls == []
    # Operator adjudication: nothing was applied, so a retry under the same canonical
    # records is safe; the reconciled runtime (fresh state dir) completes once.
    operation = fresh_journal.journal.operations(state="unknown")[0]
    fresh_journal.journal.resolve_unknown(operation["operation_id"], "not_applied",
                                          "Vendor process timed out before any effect",
                                          {"operation_id": operation["operation_id"],
                                           "observed": "not_applied"})
    fresh_journal.journal.close()
    # The operator also reconciled the runtime itself, modeled as a fresh state file.
    handle2, journal2 = make_executor(tmp_path, client=client,
                                      runtime=FakeVendorRuntime(tmp_path / "vendor-state-2"))
    outcome = handle2.handle_message({"message_id": "msg-1"}, make_message())
    assert outcome["handled"] and not outcome["deduplicated"]
    assert len(handle2._runtime().calls) == 1 and len(client.posts) == 1
    assert len(client.tasks) == 1  # the task create/start commands replayed canonically
    journal2.journal.close()
    journal.journal.close()


def test_executor_readonly_tools_unless_mutation_opted_in(tmp_path):
    class MutatingRuntime(FakeVendorRuntime):
        tool_names = ["Read", "Execute"]

    journal = executor_journal(tmp_path / "ja")
    with pytest.raises(PermissionError):
        VendorMessagingExecutor(FakeMessageClient(), PROJECT, AGENT, RUN,
                                runtime=MutatingRuntime(tmp_path / "a"),
                                handler_journal=journal)
    journal.journal.close()
    handle, journal = make_executor(tmp_path / "jb",
                                    runtime_factory=lambda: MutatingRuntime(tmp_path / "b"))
    with pytest.raises(PermissionError):
        handle._runtime()  # lazy factory: no runtime (and no session) until first use
    journal.journal.close()
    allowed, journal = make_executor(tmp_path / "jc", runtime=MutatingRuntime(tmp_path / "c"),
                                     allow_tool_mutation=True)
    assert allowed._runtime().tool_names == ["Read", "Execute"]
    journal.journal.close()


def test_executor_binds_sender_project_and_recipient(tmp_path):
    handle, journal = make_executor(tmp_path)
    with pytest.raises(UnauthorizedSender):
        handle.handle_message({"message_id": "m-x"}, make_message(message_id="m-x",
                                                                   sender="intruder"))
    with pytest.raises(UnauthorizedRecipient):
        handle.handle_message({"message_id": "m-y"}, make_message(message_id="m-y",
                                                                   to="agent-other"))
    with pytest.raises(UnauthorizedRecipient):
        handle.handle_message({"message_id": "m-z"}, make_message(message_id="m-z",
                                                                   project_id="other"))
    assert handle._runtime().calls == []
    journal.journal.close()


def test_executor_crash_after_reply_stays_unknown_and_never_duplicates(tmp_path):
    class CrashAfterReply(FakeCanonicalClient):
        def request(self, method, path, body=None, key=None):
            result = super().request(method, path, body, key)
            if method == "POST" and path == "messages":
                raise ConnectionError("lost after the reply receipt")
            return result

    client = CrashAfterReply()
    handle, journal = make_executor(tmp_path, client=client)
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert len(client.posts) == 1  # the reply landed, but the effect stays unknown
    assert len(client.tasks) == 1  # the canonical task exists from the durable turn
    with pytest.raises(UnknownOutcome):
        handle.handle_message({"message_id": "msg-1"}, make_message())
    assert len(client.posts) == 1 and len(handle._runtime().calls) == 1
    operation = next(op for op in journal.journal.operations(state="unknown")
                     if op["path"] == "runtime-message-effect")
    journal.journal.resolve_unknown(operation["operation_id"], "completed",
                                    "Reply observed in the sender thread",
                                    {"operation_id": operation["operation_id"],
                                     "observed": "applied"})
    settled = handle.handle_message({"message_id": "msg-1"}, make_message())
    assert settled["deduplicated"] and len(client.posts) == 1
    journal.journal.close()


def test_gateway_with_executor_registers_only_message_handler(tmp_path):
    handle, journal = make_executor(tmp_path)
    gateway_handle = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN,
                                           handle, policy=policy())
    assert callable(gateway_handle.message_handler)
    assert gateway_handle.notification_handler is None  # unsupported stays None/unacked
    with pytest.raises(MessagingError):
        gateway_handle.review_notification(make_notice())
    outcome = gateway_handle.handle_message({"message_id": "msg-1", **make_message()})
    assert outcome["handled"] and outcome["message_id"] == "msg-1"
    assert gateway_handle.handle_message({"message_id": "msg-1", **make_message()})[
        "deduplicated"]
    journal.journal.close()


def test_executor_notifications_unregister_without_allowed_types(tmp_path):
    handle, journal = make_executor(tmp_path)  # no allowed_notification_types
    assert handle.review_notification is None
    gateway_handle = RuntimeMessageGateway(FakeMessageClient(), PROJECT, AGENT, RUN,
                                           handle, policy=policy())
    assert gateway_handle.notification_handler is None
    assert callable(gateway_handle.message_handler)  # messages stay registered
    journal.journal.close()


NOTICE_BEHAVIOR = {"handled": True, "reply_text": None}


def test_executor_notification_canonical_turn_no_reply_and_restart_dedup(tmp_path):
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path, behavior=NOTICE_BEHAVIOR, client=client,
                                    allowed_notification_types=["board_mention"])
    notice = make_notice()
    outcome = handle.review_notification(notice)
    assert outcome["handled"] and not outcome["deduplicated"]
    runtime = handle._runtime()
    call = runtime.calls[0]
    task = client.tasks[outcome["task_id"]]
    assert task["id"] == call["task"]["id"] and task["execution"]["id"] == outcome["execution_id"]
    assert call["context"]["phase_id"] == NOTIFICATION_PHASE_ID
    assert call["context"]["packet"]["notification"]["type"] == "board_mention"
    assert NOTIFICATION_RESULT_INSTRUCTION in call["task"]["description"]
    assert task["status"] == "completed"
    assert [c["status"] for c in client.checkpoints] == ["running", "completed"]
    # No reply channel and no synthetic sender: nothing is ever posted.
    assert client.posts == {} and "reply_message_id" not in outcome["result"]
    assert handle.review_notification(notice)["deduplicated"]
    assert len(runtime.calls) == 1 and len(client.tasks) == 1
    journal.journal.close()
    # Full restart: fresh journal object and fresh runtime; dedup still holds.
    handle2, journal2 = make_executor(tmp_path, behavior=NOTICE_BEHAVIOR, client=client,
                                      allowed_notification_types=["board_mention"])
    assert handle2.review_notification(notice)["deduplicated"]
    assert len(handle2._runtime().calls) == 0 and len(client.tasks) == 1
    journal2.journal.close()


def test_executor_notification_reply_field_is_invalid_and_handled_false_unacked(tmp_path):
    # A reply_text on a notification contract impersonates a reply channel: unknown.
    handle, journal = make_executor(tmp_path / "reply-field", client=FakeCanonicalClient(),
                                    behavior={"handled": True, "reply_text": "sneaky"},
                                    allowed_notification_types=["board_mention"])
    with pytest.raises(UnknownOutcome):
        handle.review_notification(make_notice())
    assert journal.journal.operations(state="unknown")[0]["path"] == "runtime-notification-turn"
    journal.journal.close()
    # A confirmed handled=false is a definite refusal: never acked, never re-executed.
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path / "declined", client=client,
                                    behavior={"handled": False, "reply_text": None},
                                    allowed_notification_types=["board_mention"])
    with pytest.raises(MessageNotHandled):
        handle.review_notification(make_notice())
    assert next(iter(client.tasks.values()))["status"] == "failed"
    with pytest.raises(MessageNotHandled):
        handle.review_notification(make_notice())
    assert len(handle._runtime().calls) == 1 and client.posts == {}
    journal.journal.close()


def test_executor_notification_binds_recipient_project_and_type(tmp_path):
    handle, journal = make_executor(tmp_path, behavior=NOTICE_BEHAVIOR,
                                    allowed_notification_types=["board_mention"])
    with pytest.raises(UnauthorizedRecipient):
        handle.review_notification(make_notice(recipient="agent-other"))
    with pytest.raises(UnauthorizedRecipient):
        handle.review_notification(make_notice(project_id="project-other"))
    with pytest.raises(UnauthorizedNotification):
        handle.review_notification(make_notice(kind="memory_review"))
    assert handle._runtime().calls == []
    journal.journal.close()


def test_executor_notification_timeout_is_unknown_and_fresh_instance_suppressed(tmp_path):
    client = FakeCanonicalClient()
    handle, journal = make_executor(tmp_path, behavior="timeout", client=client,
                                    allowed_notification_types=["board_mention"])
    with pytest.raises(UnknownOutcome):
        handle.review_notification(make_notice())
    fresh, fresh_journal = make_executor(tmp_path, behavior="timeout", client=client,
                                         allowed_notification_types=["board_mention"])
    with pytest.raises(UnknownOutcome):
        fresh.review_notification(make_notice())
    assert fresh._runtime().calls == []  # suppressed before reaching the runtime
    fresh_journal.journal.close()
    journal.journal.close()


def test_executor_notification_against_real_service(world, tmp_path):
    """Real service: canonical notification visibility, binding, and restart dedup."""
    from test_agent_session import client_for

    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    run = world.run(receiver, receiver_token)
    world.post("Looping in our agent", token=sender_token,
               segments=[{"kind": "text", "text": "cc "},
                         {"kind": "agent_mention", "agent_id": receiver["id"]}])
    receiver_client = client_for(world, receiver_token)
    notices = receiver_client.notifications(receiver["id"], world.project)
    assert len(notices) == 1 and notices[0]["type"] == "board_mention"
    assert notices[0]["recipient_agent_id"] == receiver["id"]  # private to the recipient
    sender_client = client_for(world, sender_token)
    assert sender_client.notifications(sender["id"], world.project) == []  # not visible

    notice = notices[0]
    with Journal(tmp_path / "handler.sqlite") as journal:
        runtime = FakeVendorRuntime(tmp_path / "vendor-state", behavior=NOTICE_BEHAVIOR)
        handle = VendorMessagingExecutor(
            receiver_client, world.project, receiver["id"], run["id"], runtime=runtime,
            handler_journal=HandlerJournal(journal, world.project, receiver["id"]),
            allowed_senders=[sender["id"]], allowed_notification_types=["board_mention"])
        outcome = handle.review_notification(notice)
        assert outcome["handled"] and not outcome["deduplicated"]
    # Duplicate restart: fresh journal and executor over the same state dedup.
    with Journal(tmp_path / "handler.sqlite") as journal:
        runtime2 = FakeVendorRuntime(tmp_path / "vendor-state", behavior=NOTICE_BEHAVIOR)
        handle2 = VendorMessagingExecutor(
            receiver_client, world.project, receiver["id"], run["id"], runtime=runtime2,
            handler_journal=HandlerJournal(journal, world.project, receiver["id"]),
            allowed_senders=[sender["id"]], allowed_notification_types=["board_mention"])
        again = handle2.review_notification(notice)
        assert again["deduplicated"] and again["task_id"] == outcome["task_id"]
        assert runtime2.calls == []

    task = world.call("GET", f"tasks/{outcome['task_id']}", token=receiver_token)
    assert task["status"] == "completed" and task["execution"]["id"] == outcome["execution_id"]
    assert task["execution"]["run_id"] == run["id"] and task["execution"]["progress"] == 100
    assert task["execution"]["checkpoint"]["summary"] == "Notification handled; no reply owed"
    assert len(world.call("GET", f"tasks?project_id={world.project}", token=receiver_token)) == 1
    # Handling never acks and never replies: the notice stays visible until acked here.
    assert len(receiver_client.notifications(receiver["id"], world.project)) == 1
    receiver_client.ack_notification(notice["id"])
    assert receiver_client.notifications(receiver["id"], world.project) == []
    receiver_client.close()
    sender_client.close()


def test_executor_against_real_service_canonical_task_and_managed_hooks(world, tmp_path,
                                                                        monkeypatch):
    """Real service: canonical task/execution IDs, real checkpoint fencing, hook binding."""
    from hivemind import hooks
    from test_agent_session import client_for

    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    run = world.run(receiver, receiver_token)
    message = world.call("POST", "messages", {
        "project_id": world.project, "message_id": uid("msg"), "from": sender["id"],
        "to": receiver["id"], "type": "request", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": "Ping?"}}, sender_token)

    client = client_for(world, receiver_token)
    seen = {}

    def during_turn(task, context):
        seen.update(task_id=task["id"], execution_id=context["execution_id"])
        # Managed hooks bind to the real store records while the task is running.
        config = {"base_url": "http://testserver", "token": receiver_token,
                  "project_id": world.project, "agent_id": receiver["id"],
                  "runtime": "droid", "workspace_root": str(tmp_path),
                  "state_dir": str(tmp_path / "hook-state"), "scope_ids": [],
                  "managed_run_id": run["id"], "managed_task_id": task["id"],
                  "managed_execution_id": context["execution_id"]}
        hooks.invoke(config, {"hook_event_name": "SessionStart", "session_id": "messaging"})
        with pytest.raises(RuntimeError):  # a stale execution fence fails closed
            hooks.invoke({**config, "managed_execution_id": "stale-execution"},
                         {"hook_event_name": "SessionStart", "session_id": "messaging"})

    monkeypatch.setattr(hooks, "Client", lambda *args, **kwargs: client_for(world, receiver_token))
    with Journal(tmp_path / "handler.sqlite") as journal:
        runtime = FakeVendorRuntime(tmp_path / "vendor-state", during_turn=during_turn)
        handle = VendorMessagingExecutor(
            client, world.project, receiver["id"], run["id"], runtime=runtime,
            handler_journal=HandlerJournal(journal, world.project, receiver["id"]),
            allowed_senders=[sender["id"]], allow_replies=True)
        receipt = client.claim_message(message["id"], run["id"])
        outcome = handle.handle_message(receipt)
        assert outcome["handled"] and not outcome["deduplicated"]
        assert handle.handle_message(receipt)["deduplicated"]
        client.ack_message(message["id"], run["id"], receipt["delivery_id"])
    client.close()

    # The IDs are the canonical store records, and the task settled after the reply.
    assert outcome["task_id"] == seen["task_id"]
    assert outcome["execution_id"] == seen["execution_id"]
    task = world.call("GET", f"tasks/{outcome['task_id']}", token=receiver_token)
    assert task["status"] == "completed" and task["execution"]["id"] == outcome["execution_id"]
    assert task["execution"]["run_id"] == run["id"]
    assert task["execution"]["progress"] == 100
    assert task["execution"]["checkpoint"]["summary"] == "Message handled; reply sent"
    assert task["scope_ids"] == []  # no-tools runtime: no file scopes
    assert len(world.call("GET", f"tasks?project_id={world.project}", token=receiver_token)) == 1

    sender_client = client_for(world, sender_token)
    inbox = sender_client.message_inbox(sender["id"], world.project)
    assert len(inbox) == 1 and inbox[0]["in_reply_to"] == message["id"]
    assert inbox[0]["content"]["text"] == "Pong"
    sender_client.close()


def test_gateway_against_real_service_end_to_end(world, tmp_path):
    from test_agent_session import client_for

    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    run = world.run(receiver, receiver_token)
    message = world.call("POST", "messages", {
        "project_id": world.project, "message_id": uid("msg"), "from": sender["id"],
        "to": receiver["id"], "type": "request", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": "Ping?"}}, sender_token)

    client = client_for(world, receiver_token)
    adapter = RecordingAdapter()
    with Journal(tmp_path / "journal.sqlite") as journal:
        handle = RuntimeMessageGateway(
            client, world.project, receiver["id"], run["id"], adapter,
            policy=policy(allowed_senders=[sender["id"]]),
            handler_journal=HandlerJournal(journal, world.project, receiver["id"]))
        receipt = client.claim_message(message["id"], run["id"])
        outcome = handle.handle_message(receipt)
        assert outcome["handled"] and not outcome["deduplicated"]
        assert handle.handle_message(receipt)["deduplicated"]
        assert adapter.effects == [message["id"]]
        reply = handle.reply(message, "Pong")
        client.ack_message(message["id"], run["id"], receipt["delivery_id"])
    client.close()

    sender_client = client_for(world, sender_token)
    inbox = sender_client.message_inbox(sender["id"], world.project)
    assert [item["id"] for item in inbox] == [reply["id"]]
    assert inbox[0]["in_reply_to"] == message["id"]
    assert inbox[0]["thread_id"] == message["thread_id"]
    assert inbox[0]["correlation_id"] == message["correlation_id"]
    state = world.call("GET", f"messages/{message['id']}", token=receiver_token)
    assert state["delivery_status"] == "acked"
    sender_client.close()
