"""Integration wiring: interrupted task-create recovery and wedged auxiliary recovery.

Task-create tests run against the REAL canonical service (TestClient) so the
command-receipt evidence, the service checksum (Pydantic TaskCreate defaults
applied), and the visibility rules (404 for hidden/deleted resources) are the
actual ones. Wedged-auxiliary tests use the real durable Journal and the real
WorkerMessaging wiring with a stub canonical client and a stub recovery gate; no
model, vendor session, or paid call is ever invoked.
"""
import json

import httpx
import pytest

from hivemind.agent_session import JournalClient
from hivemind.messaging_concurrency import EffectsBlocked
from hivemind.recovery import Journal, command_key, derive_operation_id
from hivemind.recovery_automation import (RecoveryError, handler_operation_ids,
                                          task_create_normalize)
from hivemind.storage import digest
from hivemind.worker_messaging import WorkerMessaging
from test_agent_session import client_for

PROJECT = "project"
AGENT = "agent"
RUN_AUX = "run-aux-0"
ITEM = "msg-1"


# ------------------------------------------------------------- service fixtures
class DropFirst:
    """Forward everything, but lose the first POST tasks response after it lands."""

    def __init__(self, request):
        self.request, self.dropped = request, False

    def __call__(self, method, path, body=None, key=None):
        result = self.request(method, path, body, key=key)
        if method == "POST" and path == "tasks" and not self.dropped:
            self.dropped = True
            raise httpx.ReadError("response lost after the command committed")
        return result


class NeverSends:
    """Transport-level failure BEFORE anything reaches the canonical service."""

    def request(self, method, path, body=None, key=None):
        raise httpx.ConnectError("connection refused before dispatch")

    def close(self):
        pass


def interrupted_create(world, tmp_path, body=None):
    """Crash a journaled POST tasks after the server committed it. Returns context."""
    body = body or {"project_id": world.project, "title": "Recovered task"}
    client = client_for(world, world.owner)
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        wrapped = JournalClient(client, journal)
        wrapped.request = lambda *a, **k: JournalClient.request(wrapped, *a, **k)
        original = client.request
        client.request = DropFirst(original)
        with pytest.raises(httpx.ReadError):
            wrapped.request("POST", "tasks", body, key="stable-create")
    client.request = original
    return client, path, body


def test_interrupted_task_create_recovers_from_real_receipt(world, tmp_path):
    client, path, body = interrupted_create(world, tmp_path)
    try:
        with Journal(path) as journal:
            wrapped = JournalClient(client, journal)
            report = wrapped.recover_task_creates(enabled=True)
            assert report["held"] == [] and report["skipped"] == 0
            (entry,) = report["recovered"]
            assert entry["outcome"] == "proved_applied"
            record = journal.operation(entry["operation_id"])
            assert record["state"] == "completed"
            # The completed result is EXACTLY the receipt's current projection.
            task = world.call("GET", f"tasks/{entry['task_id']}")
            assert record["result"]["id"] == task["id"]
            assert record["result"]["status"] == task["status"]
            # Exactly one canonical task exists; recovery never re-issued a command.
            tasks = world.call("GET", f"tasks?project_id={world.project}")
            assert [t["title"] for t in tasks] == ["Recovered task"]
            # The receipt hash matches the REAL service checksum: the durable body
            # lacks TaskCreate defaults, so normalization must apply them.
            receipt = world.call(
                "GET", f"commands/receipt?operation=tasks&key={record['command_key']}")
            assert receipt["payload_sha256"] == digest(task_create_normalize(body))
            assert receipt["payload_sha256"] != digest(body)  # defaults are part of it
            assert receipt["external_effects_proved"] is False
            audits = journal.recovery_audits(record["operation_id"])
            assert audits[0]["classification"] == "proved_applied"
            assert audits[0]["decision"] == "completed"
            # The stable command key survived the restart; a second sweep is a no-op.
            assert wrapped.recover_task_creates(enabled=True)["recovered"] == []
    finally:
        client.close()


def test_undispatched_create_and_404_receipt_never_prove_absence(world, tmp_path):
    """A receipt 404 (missing/hidden/deleted) is never evidence of non-application."""
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        wrapped = JournalClient(NeverSends(), journal)
        with pytest.raises(httpx.ConnectError):
            wrapped.request("POST", "tasks",
                            {"project_id": world.project, "title": "Ghost"},
                            key="stable-ghost")
    with Journal(path) as journal:
        wrapped = JournalClient(NeverSends(), journal)
        # A failing receipt lookup proves nothing: held, never an exception escape.
        assert len(wrapped.recover_task_creates(enabled=True)["held"]) == 1
        # Even with a reachable service, the receipt 404 is NOT authoritative
        # absence: the operation stays unknown for an operator.
        client = client_for(world, world.owner)
        try:
            report = JournalClient(client, journal).recover_task_creates(enabled=True)
            assert len(report["held"]) == 1
            assert journal.operations(state="unknown")
            assert world.call("GET", f"tasks?project_id={world.project}") == []
        finally:
            client.close()


def test_deleted_resource_receipt_404_stays_held(world, tmp_path):
    client, path, _ = interrupted_create(world, tmp_path)
    try:
        with Journal(path) as journal:
            # The command committed, but the cached resource was deleted afterwards:
            # the endpoint's 404 hides the receipt, and hiding is never proof.
            task = world.call("GET", f"tasks?project_id={world.project}")[0]
            with world.app.state.store.transaction():
                world.app.state.store.db.execute(
                    "UPDATE resources SET deleted=1 WHERE id=?", (task["id"],))
            report = JournalClient(client, journal).recover_task_creates(enabled=True)
            assert len(report["held"]) == 1
            assert journal.operations(state="unknown")[0]["path"] == "tasks"
    finally:
        client.close()


def test_task_create_recovery_is_opt_in_bounded_and_refuses_redacted(world, tmp_path):
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        journal.begin_operation("read-op", "GET", "tasks")
        journal.mark_attempt("read-op")
        journal.fail_operation("read-op", TimeoutError("lost"))
        journal.begin_operation("redacted-op", "POST", "tasks",
                                {"title": "x", "token": "secret-token-value-123"},
                                allow_sensitive_payload=True)
        journal.mark_attempt("redacted-op")
        journal.fail_operation("redacted-op", TimeoutError("lost"))
        client = JournalClient(NeverSends(), journal)
        with pytest.raises(RecoveryError):  # default-off: explicit opt-in required
            client.recover_task_creates()
        report = client.recover_task_creates(enabled=True)
        # A read is skipped (not a task-create); a redacted payload can never be
        # hash-bound to a receipt, so it is held, never guessed.
        assert report["skipped"] == 1
        assert [held["operation_id"] for held in report["held"]] == ["redacted-op"]
        assert journal.operation("redacted-op")["state"] == "unknown"


# --------------------------------------------------------- wedged auxiliary wiring
class StubClient:
    """Canonical client stub: serves only the routes it is configured with."""

    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.calls = []

    def request(self, method, path, body=None, key=None):
        self.calls.append((method, path))
        value = self.routes.get((method, path.split("?")[0]))
        if callable(value):
            return value(method, path, body, key)
        if value is not None:
            return value
        raise RuntimeError("unexpected canonical read: " + path)


def make_messaging(tmp_path, client=None, *, recover=True):
    journal = Journal(tmp_path / "journal.sqlite")
    messaging = WorkerMessaging(
        client or StubClient(), {"project_id": PROJECT, "agent_id": AGENT,
                                 "run_id": "run-primary"},
        journal, lambda options: object(), {"state_file": str(tmp_path / "slot.json")},
        enabled=False, recover_message_effects=recover, allowed_senders={"user"})
    messaging.supervisor.auxiliary_run_ids = [RUN_AUX]
    return messaging, journal


def wedge(journal, kind, item=ITEM, run=RUN_AUX, **extra_body):
    operation_id = derive_operation_id("handler", kind, f"{PROJECT}:{AGENT}:{item}")
    journal.begin_operation(operation_id, "HANDLER", kind,
                            {"project_id": PROJECT, "agent_id": AGENT, "run_id": run,
                             "item_id": item, **extra_body}, safe_to_retry=False)
    journal.mark_attempt(operation_id)
    journal.fail_operation(operation_id, TimeoutError("wedged mid-phase"))
    return operation_id


def allow_gate(captured):
    def authorize(phase, context):
        captured.append((phase, context))
        return True
    return authorize


def quiescent_checkpoint(tmp_path, *, dispatched=False, task_id="t-1",
                         execution_id="e-1"):
    (tmp_path / "slot.json").write_text(json.dumps(
        {"session_id": "s-1", "outcome": "paused"}))
    (tmp_path / "slot.json.checkpoint.json").write_text(json.dumps(
        {"quiescent": True, "status": "paused", "dispatched": dispatched,
         "run_id": RUN_AUX, "session_id": "s-1", "task_id": task_id,
         "execution_id": execution_id}))


def test_wedged_turn_pre_dispatch_non_invocation_resolves(tmp_path):
    """Actual non-invocation (dispatched is False) is provable; a timeout is not."""
    messaging, journal = make_messaging(tmp_path)
    task_op, turn_op, _ = handler_operation_ids(PROJECT, AGENT, ITEM)
    journal.begin_operation(task_op, "HANDLER", "runtime-message-task",
                            {"project_id": PROJECT, "agent_id": AGENT,
                             "run_id": RUN_AUX, "item_id": ITEM}, safe_to_retry=False)
    journal.complete_operation(task_op, {"task_id": "t-1", "execution_id": "e-1",
                                         "revision": 2})
    wedge(journal, "runtime-message-turn")
    quiescent_checkpoint(tmp_path, dispatched=False)
    captured = []
    messaging.supervisor.authorize_recovery_effect = allow_gate(captured)
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["resolved"]] == [turn_op]
    assert journal.operation(turn_op)["state"] == "pending"  # deliberate retry later
    # The gate proved the current identity/lease at the moment of mutation.
    phase, context = captured[0]
    assert phase == "resolve"
    assert context["run_id"] == RUN_AUX and context["item_id"] == ITEM
    audits = journal.recovery_audits(turn_op)
    assert audits[0]["classification"] == "proved_not_attempted"
    journal.close()


def test_wedged_turn_interrupted_after_dispatch_stays_held(tmp_path):
    """A paused checkpoint without pre-dispatch proof is a timeout unknown, held."""
    messaging, journal = make_messaging(tmp_path)
    turn_op = wedge(journal, "runtime-message-turn")
    quiescent_checkpoint(tmp_path, dispatched=True)  # model may have acted first
    messaging.supervisor.authorize_recovery_effect = allow_gate([])
    report = messaging.recover_wedged_auxiliaries()
    assert report["resolved"] == []
    assert [entry["operation_id"] for entry in report["held"]] == [turn_op]
    assert journal.operation(turn_op)["state"] == "unknown"
    journal.close()


def durable_task_commands(journal, *, item=ITEM, run=RUN_AUX, family="message"):
    create_key = command_key(derive_operation_id(
        "runtime-message", family + "-task-create", PROJECT, AGENT, item))
    create_op = derive_operation_id("POST", "tasks", create_key)
    journal.begin_operation(create_op, "POST", "tasks",
                            {"project_id": PROJECT, "title": "Handle " + item},
                            safe_to_retry=True)
    journal.complete_operation(create_op, {"id": "t-1", "project_id": PROJECT,
                                           "title": "Handle " + item, "revision": 1,
                                           "status": "queued"})
    start_key = command_key(derive_operation_id(
        "runtime-message", family + "-task-start", PROJECT, AGENT, item))
    start_op = derive_operation_id("POST", "tasks/t-1:start", start_key)
    journal.begin_operation(start_op, "POST", "tasks/t-1:start",
                            {"run_id": run, "expected_revision": 1, "recover": False},
                            safe_to_retry=True)
    journal.complete_operation(start_op, {"id": "t-1", "revision": 2,
                                          "execution": {"id": "e-1", "run_id": run}})
    return create_op, create_key


def test_wedged_task_phase_resolves_from_durable_command_records(tmp_path):
    messaging, journal = make_messaging(tmp_path, StubClient({
        ("GET", "tasks/t-1"): {"id": "t-1", "revision": 2,
                               "execution": {"id": "e-1", "run_id": RUN_AUX}}}))
    task_op = wedge(journal, "runtime-message-task")
    durable_task_commands(journal)
    captured = []
    messaging.supervisor.authorize_recovery_effect = allow_gate(captured)
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["resolved"]] == [task_op]
    # The result is EXACTLY what the journaled commands durably recorded.
    assert journal.operation(task_op)["result"] == {
        "task_id": "t-1", "execution_id": "e-1", "revision": 2}
    assert captured[0][1]["task_id"] == "t-1"
    journal.close()


def test_wedged_task_phase_stale_current_execution_stays_held(tmp_path):
    messaging, journal = make_messaging(tmp_path, StubClient({
        ("GET", "tasks/t-1"): {"id": "t-1", "revision": 5,
                               "execution": {"id": "e-NEW", "run_id": RUN_AUX}}}))
    task_op = wedge(journal, "runtime-message-task")
    durable_task_commands(journal)
    messaging.supervisor.authorize_recovery_effect = allow_gate([])
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["held"]] == [task_op]
    assert journal.operation(task_op)["state"] == "unknown"
    journal.close()


def test_wedged_task_phase_unknown_create_uses_real_receipt_shape(tmp_path):
    """The durable POST tasks record is first resolved from command receipts."""
    messaging, journal = make_messaging(tmp_path)
    task_op = wedge(journal, "runtime-message-task")
    create_op, create_key = durable_task_commands(journal)
    # Rewind the create command to unknown, as a crash-after-dispatch left it.
    journal.resolve_operation(create_op, "pending")
    journal.mark_attempt(create_op)
    journal.fail_operation(create_op, TimeoutError("response lost"))
    payload = {"project_id": PROJECT, "title": "Handle " + ITEM}
    receipt = {"state": "committed", "operation": "tasks",
               # The server-side idempotency key is the journal's command key.
               "key": journal.operation(create_op)["command_key"],
               "payload_sha256": digest(task_create_normalize(payload)),
               "response_sha256": "0" * 64, "resource_id": "t-1",
               "resource": {"id": "t-1", "project_id": PROJECT, "revision": 1},
               "external_effects_proved": False}
    def routes(method, path, body=None, key=None):
        if path.startswith("commands/receipt?"):
            return receipt
        return {"id": "t-1", "revision": 2,
                "execution": {"id": "e-1", "run_id": RUN_AUX}}
    messaging.client = StubClient({("GET", "commands/receipt"): routes,
                                   ("GET", "tasks/t-1"): routes})
    messaging.supervisor.authorize_recovery_effect = allow_gate([])
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["resolved"]] == [task_op]
    assert journal.operation(create_op)["state"] == "completed"
    assert journal.operation(task_op)["result"]["task_id"] == "t-1"
    journal.close()


def test_wedged_auxiliary_cross_slot_run_stays_held(tmp_path):
    messaging, journal = make_messaging(tmp_path)
    turn_op = wedge(journal, "runtime-message-turn", run="run-foreign")
    gate_calls = []
    messaging.supervisor.authorize_recovery_effect = allow_gate(gate_calls)
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["held"]] == [turn_op]
    assert report["held"][0]["reason"] == "cross_slot_or_unbound"
    assert gate_calls == []  # a foreign slot's evidence never reaches the gate
    assert journal.operation(turn_op)["state"] == "unknown"
    journal.close()


def test_wedged_auxiliary_control_fence_refusal_stays_held(tmp_path):
    """A pause/shutdown control fence at settlement is a definite refusal."""
    messaging, journal = make_messaging(tmp_path)
    turn_op = wedge(journal, "runtime-message-turn")
    quiescent_checkpoint(tmp_path, dispatched=False)
    def refuse(phase, context):
        raise EffectsBlocked("Supervisor is paused")
    messaging.supervisor.authorize_recovery_effect = refuse
    report = messaging.recover_wedged_auxiliaries()
    assert report["resolved"] == []
    assert [entry["operation_id"] for entry in report["held"]] == [turn_op]
    assert journal.operation(turn_op)["state"] == "unknown"
    journal.close()


def test_wedged_auxiliary_recovery_requires_opt_in(tmp_path):
    messaging, journal = make_messaging(tmp_path, recover=False)
    wedge(journal, "runtime-message-turn")
    with pytest.raises(RecoveryError):
        messaging.recover_wedged_auxiliaries()
    journal.close()


def test_wedged_turn_requires_durable_task_provenance(tmp_path):
    """A pre-dispatch checkpoint without the durable task/execution is incidental."""
    messaging, journal = make_messaging(tmp_path)
    turn_op = wedge(journal, "runtime-message-turn")  # no completed task op
    quiescent_checkpoint(tmp_path, dispatched=False)
    messaging.supervisor.authorize_recovery_effect = allow_gate([])
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["held"]] == [turn_op]
    assert journal.operation(turn_op)["state"] == "unknown"
    journal.close()


def test_wedged_turn_checkpoint_identity_mismatch_stays_held(tmp_path):
    """A checkpoint for another task/execution is never this operation's proof."""
    messaging, journal = make_messaging(tmp_path)
    task_op, turn_op, _ = handler_operation_ids(PROJECT, AGENT, ITEM)
    journal.begin_operation(task_op, "HANDLER", "runtime-message-task",
                            {"project_id": PROJECT, "agent_id": AGENT,
                             "run_id": RUN_AUX, "item_id": ITEM}, safe_to_retry=False)
    journal.complete_operation(task_op, {"task_id": "t-1", "execution_id": "e-1",
                                         "revision": 2})
    wedge(journal, "runtime-message-turn")
    quiescent_checkpoint(tmp_path, dispatched=False, task_id="t-OTHER",
                         execution_id="e-OTHER")
    messaging.supervisor.authorize_recovery_effect = allow_gate([])
    report = messaging.recover_wedged_auxiliaries()
    assert [entry["operation_id"] for entry in report["held"]] == [turn_op]
    assert journal.operation(turn_op)["state"] == "unknown"
    journal.close()


# ------------------------------------------------------- start-integrated wiring
RUNS = {"run-aux-0": {"id": "run-aux-0", "project_id": PROJECT, "agent_id": AGENT,
                      "status": "running"},
        "run-aux-1": {"id": "run-aux-1", "project_id": PROJECT, "agent_id": AGENT,
                      "status": "running"}}


def two_slot_messaging(tmp_path, journal):
    created = []

    def create_run(method, path, body=None, key=None):
        run = RUNS[f"run-aux-{len(created)}"]
        created.append(run["id"])
        return dict(run)

    client = StubClient({("POST", "runs"): create_run})
    messaging = WorkerMessaging(
        client, {"project_id": PROJECT, "agent_id": AGENT, "run_id": "run-primary"},
        journal, lambda options: object(), {"state_file": str(tmp_path / "slot.json")},
        enabled=True, recover_message_effects=True, allowed_senders={"user"},
        slots=2, poll_interval=600, shutdown_timeout=5)
    return messaging, created


def complete_task_phase(journal, item, run):
    task_op, _, _ = handler_operation_ids(PROJECT, AGENT, item)
    journal.begin_operation(task_op, "HANDLER", "runtime-message-task",
                            {"project_id": PROJECT, "agent_id": AGENT,
                             "run_id": run, "item_id": item}, safe_to_retry=False)
    journal.complete_operation(task_op, {"task_id": "t-" + item,
                                         "execution_id": "e-" + item, "revision": 2})


def test_start_runs_recovery_after_bindings_before_consumers(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    messaging, created = two_slot_messaging(tmp_path, journal)
    # Slot 1: provable pre-dispatch non-invocation. Slot 0: interrupted after
    # dispatch (held). Both records exist before start().
    complete_task_phase(journal, "msg-a", "run-aux-1")
    turn_ok = wedge(journal, "runtime-message-turn", item="msg-a", run="run-aux-1")
    complete_task_phase(journal, "msg-b", "run-aux-0")
    turn_held = wedge(journal, "runtime-message-turn", item="msg-b", run="run-aux-0")
    slot1 = json.loads(json.dumps({"session_id": "s-1", "outcome": "paused"}))
    (tmp_path / "slot-slot1.json").write_text(json.dumps(slot1))
    (tmp_path / "slot-slot1.json.checkpoint.json").write_text(json.dumps(
        {"quiescent": True, "status": "paused", "dispatched": False,
         "run_id": "run-aux-1", "session_id": "s-1", "task_id": "t-msg-a",
         "execution_id": "e-msg-a"}))
    (tmp_path / "slot.json").write_text(json.dumps(slot1))
    (tmp_path / "slot.json.checkpoint.json").write_text(json.dumps(
        {"quiescent": True, "status": "paused", "dispatched": True,
         "run_id": "run-aux-0", "session_id": "s-1", "task_id": "t-msg-b",
         "execution_id": "e-msg-b"}))
    calls = []
    messaging.supervisor.authorize_recovery_effect = allow_gate([])
    original = messaging.recover_wedged_auxiliaries

    def recording(*args, **kwargs):
        import threading
        calls.append({
            # Canonical bindings exist already...
            "runs": list(messaging.supervisor.auxiliary_run_ids),
            # ...and no consumer/producer thread is dispatching yet.
            "consumers": list(messaging.supervisor._consumers),
            "threads": [t.name for t in threading.enumerate()
                        if t.name.startswith("hivemind-messaging")],
        })
        return original(*args, **kwargs)

    messaging.recover_wedged_auxiliaries = recording
    try:
        status = messaging.start()
        assert created == ["run-aux-0", "run-aux-1"]
        assert calls == [{"runs": ["run-aux-0", "run-aux-1"], "consumers": [],
                          "threads": []}]
        assert status["bindings"]["auxiliary_run_ids"] == ["run-aux-0", "run-aux-1"]
        assert len(status["bindings"]["slot_state_files"]) == 2
        recovery = status["recovery"]
        assert [e["operation_id"] for e in recovery["resolved"]] == [turn_ok]
        assert [e["operation_id"] for e in recovery["held"]] == [turn_held]
        assert journal.operation(turn_ok)["state"] == "pending"
        assert journal.operation(turn_held)["state"] == "unknown"
        # A concurrent/duplicate start is blocked; recovery ran exactly once.
        from hivemind.runtime_messaging import MessagingError
        with pytest.raises(MessagingError, match="already started"):
            messaging.start()
        assert len(calls) == 1
    finally:
        messaging.close()
        journal.close()


def test_start_without_opt_in_reports_no_recovery(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    messaging = WorkerMessaging(
        StubClient({("POST", "runs"): lambda *a, **k: dict(RUNS["run-aux-0"])}),
        {"project_id": PROJECT, "agent_id": AGENT, "run_id": "run-primary"},
        journal, lambda options: object(), {"state_file": str(tmp_path / "s.json")},
        enabled=True, recover_message_effects=False, allowed_senders={"user"},
        poll_interval=600, shutdown_timeout=5)
    try:
        status = messaging.start()
        assert status["recovery"] is None
        assert status["bindings"]["auxiliary_run_ids"] == ["run-aux-0"]
    finally:
        messaging.close()
        journal.close()
