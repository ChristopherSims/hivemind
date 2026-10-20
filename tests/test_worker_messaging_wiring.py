"""Parent lifecycle/CLI regression checks, local canonical service only."""
import threading
import time

import pytest

from hivemind import cli
from hivemind.agent_session import JournalClient
from hivemind.recovery import Journal, UnknownOutcome
from hivemind.storage import uid
from hivemind.worker_messaging import WorkerMessaging
from test_agent_session import client_for


@pytest.mark.parametrize("flags,reason", [
    (["--concurrent-messaging"], "requires --messaging"),
    (["--message-queue-capacity", "2"], "require --concurrent-messaging"),
    (["--recover-message-effects"], "requires --concurrent-messaging"),
])
def test_concurrent_worker_cli_rejects_missing_opt_ins(monkeypatch, capsys, tmp_path, flags, reason):
    monkeypatch.setattr("sys.argv", ["hivemind", "worker", "--enable",
        "--config", str(tmp_path / "absent.json"), "--journal", str(tmp_path / "journal.sqlite"),
        "--workspace", str(tmp_path), "--adapter", "hivemind.vendor_runtimes:factory_runtime", *flags])
    assert cli.main() == 1
    assert reason in capsys.readouterr().err
    assert not (tmp_path / "journal.sqlite").exists()


def test_shutdown_cleanup_failure_is_unknown_and_still_stops_supervisor(monkeypatch):
    actions = []

    class Supervisor:
        def __init__(self, *args, **kwargs):
            self.stops = 0

        def stop(self, **kwargs):
            self.stops += 1
            return {"stopped": self.stops == 2}

    class Runtime:
        state = {"run_id": "auxiliary"}

        def cancel(self, run):
            actions.append(("cancel", run["id"]))
            return {"applied": None}

        def close(self):
            actions.append(("close",))
            raise RuntimeError("Fixture cleanup is unconfirmed")

    monkeypatch.setattr("hivemind.worker_messaging.AssignedRunMessagingSupervisor", Supervisor)
    wiring = WorkerMessaging(None, {"project_id": "project", "agent_id": "agent", "run_id": "primary"},
                             None, lambda _: None, {})
    wiring.runtimes.append(Runtime())
    with pytest.raises(UnknownOutcome, match="cleanup"):
        wiring.close()
    assert actions == [("cancel", "auxiliary"), ("close",)]
    assert wiring.cleanup_report == {"stopped": True, "cleanup_errors": ["RuntimeError"],
                                     "slots": 1, "recovery": {"recovered": 0, "held": 0}}
    wiring.close()
    assert wiring.supervisor.stops == 2


def test_parent_wiring_preserves_assigned_primary_and_stops_before_journal(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    run = world.run(receiver, receiver_token)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Primary", "scope_ids": []})
    task = world.call("POST", f"tasks/{task['id']}:start", {
        "expected_revision": task["revision"], "run_id": run["id"], "recover": False}, receiver_token)
    world.call("POST", "messages", {
        "project_id": world.project, "message_id": uid("msg"),
        "from": sender["id"], "to": receiver["id"], "type": "request", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": "Two plus two?"}}, sender_token)
    client = client_for(world, receiver_token)
    called, closed = threading.Event(), []
    options = {"state_file": str(tmp_path / "message-runtime.json")}

    class Runtime:
        tool_names = []

        def execute(self, task, context, checkpoint):
            assert task["scope_ids"] == [] and context["run_id"] != run["id"]
            called.set()
            return {"status": "completed", "message_result": {"handled": True, "reply_text": "4"}}

        def close(self):
            closed.append(True)

    def factory(actual):
        assert actual["no_tools"] is True and actual["result_contract"] == "message"
        assert actual["state_file"] == options["state_file"]
        return Runtime()

    with Journal(tmp_path / "journal.sqlite") as journal:
        wrapped = JournalClient(client, journal)
        wiring = WorkerMessaging(wrapped, {
            "project_id": world.project, "agent_id": receiver["id"], "run_id": run["id"]},
            journal, factory, options, enabled=True, allowed_senders=[sender["id"]],
            allow_replies=True, poll_interval=.1)
        try:
            wiring.start()
            assert called.wait(5)
            deadline = time.monotonic() + 5
            while wiring.supervisor.status()["handled"] != 1 and time.monotonic() < deadline:
                time.sleep(.02)
            assert wiring.supervisor.status()["handled"] == 1
            current = world.call("GET", f"runs/{run['id']}", token=receiver_token)
            assert current["task_id"] == task["id"]
            current_task = world.call("GET", f"tasks/{task['id']}", token=receiver_token)
            assert current_task["execution"]["id"] == task["execution"]["id"]
        finally:
            wiring.close()
            wiring.close()
        assert closed == [True]
        assert journal.operations()  # Cleanup completed while the journal was open.
    client.close()


def test_slots_bounds_are_validated(tmp_path):
    config = {"project_id": "project", "agent_id": "agent", "run_id": "primary"}
    for slots in (0, 9):
        with pytest.raises(ValueError, match="slots"):
            WorkerMessaging(None, config, None, lambda _: None, {}, slots=slots)


def test_multi_slot_allocates_private_paths_and_cleans_up_all(world, tmp_path):
    """Two slots: stable distinct per-slot state files, two distinct runtimes, all
    cleaned up before the journal closes, exact per-slot run bindings."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    run = world.run(receiver, receiver_token)
    world.call("POST", "messages", {
        "project_id": world.project, "message_id": uid("msg"),
        "from": sender["id"], "to": receiver["id"], "type": "request",
        "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": "Ping?"}}, sender_token)
    client = client_for(world, receiver_token)
    seen_options, closed = [], []

    class Runtime:
        tool_names = []

        def execute(self, task, context, checkpoint):
            return {"status": "completed",
                    "message_result": {"handled": True, "reply_text": "Pong"}}

        def close(self):
            closed.append(self)

    def factory(options):
        seen_options.append(dict(options))
        return Runtime()

    options = {"state_file": str(tmp_path / "message-runtime.json")}
    with Journal(tmp_path / "journal.sqlite") as journal:
        wiring = WorkerMessaging(JournalClient(client, journal),
                                 {"project_id": world.project,
                                  "agent_id": receiver["id"], "run_id": run["id"]},
                                 journal, factory, options, enabled=True,
                                 allowed_senders=[sender["id"]], allow_replies=True,
                                 poll_interval=.1, slots=2)
        # Slot 0 keeps the exact configured path; slot 1 gets its own stable path.
        assert wiring.slot_state_files[0] == options["state_file"]
        assert wiring.slot_state_files[1].endswith("message-runtime-slot1.json")
        assert len(set(wiring.slot_state_files)) == 2
        try:
            status = wiring.start()
            assert status["slots"] == 2
            assert len(set(status["auxiliary_run_ids"])) == 2
            deadline = time.monotonic() + 10
            while wiring.supervisor.status()["handled"] != 1 \
                    and time.monotonic() < deadline:
                time.sleep(.02)
            assert wiring.supervisor.status()["handled"] == 1
        finally:
            wiring.close()
        assert wiring.cleanup_report["slots"] == 2
        assert wiring.cleanup_report["stopped"] is True
    assert len(wiring.runtimes) == len(closed) >= 1
    assert all(runtime in wiring.runtimes for runtime in closed)
    for actual in seen_options:
        assert actual["no_tools"] is True and actual["result_contract"] == "message"
    assert {actual["state_file"] for actual in seen_options} <= set(
        wiring.slot_state_files)
    client.close()


def test_recovery_chooses_the_owning_slot_run_from_evidence(monkeypatch, tmp_path):
    """The recovery callback derives the run from the item's journaled task
    execution, never from a shared slot-0 auxiliary_run_id."""
    from test_recovery_automation import AGENT, PROJECT, crashed_after_reply, make_message
    client, handle, journal = crashed_after_reply(tmp_path)
    source = make_message()
    client.messages[source["id"]] = source
    # The evidence says slot 1's run owns this item's task.
    task = next(iter(client.tasks.values()))
    task["execution"]["run_id"] = "aux-slot-1"
    next(iter(client.posts.values()))["sender_run_id"] = "aux-slot-1"
    client.runs["aux-slot-1"] = {"id": "aux-slot-1", "agent_id": AGENT,
                                 "project_id": PROJECT, "status": "running"}
    checked = []

    class Supervisor:
        auxiliary_run_id = "aux-slot-0"
        auxiliary_run_ids = ["aux-slot-0", "aux-slot-1"]

        def __init__(self, *args, **kwargs):
            pass

        def authorize_recovery_effect(self, phase, context):
            checked.append(context["run_id"])
            return True

    monkeypatch.setattr("hivemind.worker_messaging.AssignedRunMessagingSupervisor",
                        Supervisor)
    wiring = WorkerMessaging(client, {"project_id": PROJECT, "agent_id": AGENT,
                                      "run_id": "primary"},
                             journal, lambda _: pytest.fail("no runtime"), {},
                             allowed_senders=[source["from"]], allow_replies=True,
                             recover_message_effects=True, slots=2)
    try:
        wiring._recover_unknown("message", source["id"], UnknownOutcome("unknown"))
        assert checked and set(checked) == {"aux-slot-1"}
        assert wiring.recovery_report == {"recovered": 1, "held": 0}
        # An item whose evidence points outside the owned slots stays held.
        task["execution"]["run_id"] = "foreign-run"
        wiring._recover_unknown("message", source["id"], UnknownOutcome("unknown"))
        assert wiring.recovery_report == {"recovered": 1, "held": 1}
    finally:
        journal.journal.close()


@pytest.mark.parametrize("gate_allowed", [True, False])
def test_opted_in_unknown_callback_recovers_effects_not_model(monkeypatch, tmp_path, gate_allowed):
    from test_recovery_automation import AGENT, PROJECT, RUN, crashed_after_reply, make_message
    client, handle, journal = crashed_after_reply(tmp_path)
    source = make_message()
    client.messages[source["id"]] = source
    checked = []

    class Supervisor:
        auxiliary_run_id = RUN

        def __init__(self, *args, **kwargs):
            assert callable(kwargs["on_unknown"])

        def authorize_recovery_effect(self, phase, context):
            checked.append(phase)
            assert context["run_id"] == RUN
            return gate_allowed

    monkeypatch.setattr("hivemind.worker_messaging.AssignedRunMessagingSupervisor", Supervisor)
    wiring = WorkerMessaging(client, {"project_id": PROJECT, "agent_id": AGENT, "run_id": "primary"},
                             journal, lambda _: pytest.fail("Recovery invoked a runtime"), {},
                             allowed_senders=[source["from"]], allow_replies=True,
                             recover_message_effects=True)
    try:
        wiring._recover_unknown("message", source["id"], UnknownOutcome("unknown"))
        assert checked == ["settle"]  # Existing exact reply is proved, never posted twice.
        assert len(handle._runtime().calls) == 1 and len(client.posts) == 1
        assert wiring.recovery_report == {"recovered": int(gate_allowed), "held": int(not gate_allowed)}
        task = next(iter(client.tasks.values()))
        assert task["status"] == ("completed" if gate_allowed else "running")
    finally:
        journal.journal.close()
