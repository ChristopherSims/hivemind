"""Offline real-service/thread tests for assigned-run messaging concurrency.

A dedicated auxiliary run handles an agent's messages/notifications while the primary
run's task execution is in flight. No vendor session, paid call, or network: the vendor
runtime is the faithful in-process fake, and the service runs in-process via ``world``.
"""

import threading
import time

import pytest

from hivemind.agent_worker import AgentWorker
from hivemind.messaging_concurrency import (AssignedRunMessagingSupervisor,
                                            EffectsBlocked)
from hivemind.recovery import Journal
from hivemind.runtime_messaging import (MessagingDisabled, MessagingError,
                                        MessagingPolicy)
from hivemind.storage import uid
from test_agent_session import client_for
from test_agent_worker import AGENT, PROJECT, RUN, FakeClient, SyntheticRuntime
from test_runtime_messaging import FakeVendorRuntime


def wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def send(world, sender, receiver, text="Ping?", token=None):
    return world.call("POST", "messages", {
        "project_id": world.project, "message_id": uid("msg"), "from": sender["id"],
        "to": receiver["id"], "type": "request", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": text}}, token)


def message_state(world, message, token):
    return world.call("GET", f"messages/{message['id']}", token=token)["delivery_status"]


def replies_to(world, sender, token):
    inbox = world.call("GET", f"agents/{sender['id']}/inbox?project_id={world.project}",
                       token=token)
    return [m for m in inbox if m.get("type") == "response"]


def coordinator(world):
    nexus = world.agent("Nexus")
    token = world.credential(nexus, coordinator=True)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": world.run(nexus, token)["id"]},
                       token)
    return token, lease


def control(world, lease_token, lease, run_id, action="pause"):
    current = world.call("GET", f"runs/{run_id}")
    return world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": run_id, "action": action,
        "target_run_revision": current["control_revision"],
        "coordinator_epoch": lease["coordinator_epoch"], "reason": "Boundary"}, lease_token)


def make_supervisor(world, receiver, primary, journal, tmp_path, behavior=None,
                    during_turn=None, on_unknown=None, **overrides):
    client = client_for(world, overrides.pop("token"))
    base = {"primary_run_id": primary["id"], "journal": journal, "enabled": True,
            "runtime_factory": lambda: FakeVendorRuntime(tmp_path / "vendor",
                                                         behavior=behavior,
                                                         during_turn=during_turn),
            "on_unknown": on_unknown, "poll_interval": 0.1}
    base.update(overrides)
    return AssignedRunMessagingSupervisor(client, world.project, receiver["id"], **base)


def test_bounds_and_opt_in_are_enforced(tmp_path):
    with Journal(tmp_path / "j.sqlite") as journal:
        with pytest.raises(MessagingDisabled):
            AssignedRunMessagingSupervisor(
                object(), PROJECT, AGENT, primary_run_id=RUN, journal=journal,
                runtime=object(), enabled=False).start()
        with pytest.raises(ValueError):
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal, runtime=object(),
                                           max_in_flight=0)
        with pytest.raises(ValueError, match="single prebuilt runtime cannot serve"):
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal, runtime=object(),
                                           max_in_flight=2)  # shared session forbidden
        with pytest.raises(ValueError, match="distinct instances"):
            shared = object()
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal,
                                           runtime={0: shared, 1: shared},
                                           max_in_flight=2)  # mapped but not independent
        with pytest.raises(ValueError, match="exactly 0..max_in_flight-1"):
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal,
                                           runtime={0: object()}, max_in_flight=2)
        with pytest.raises(ValueError, match="one factory per slot"):
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal,
                                           runtime_factory=[lambda: object()],
                                           max_in_flight=2)
        # A factory serving several slots is accepted (invoked once per slot).
        AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                       journal=journal,
                                       runtime_factory=lambda: object(),
                                       max_in_flight=2)
        AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                       journal=journal,
                                       runtime={0: object(), 1: object()},
                                       max_in_flight=2)
        with pytest.raises(ValueError):
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal,
                                           runtime_factory=lambda: object(),
                                           queue_capacity=0)
        with pytest.raises(ValueError):
            AssignedRunMessagingSupervisor(object(), PROJECT, AGENT, primary_run_id=RUN,
                                           journal=journal,
                                           runtime_factory=lambda: object(),
                                           shutdown_timeout=9999)


def test_worker_delegate_skips_message_and_notification_polls():
    client = FakeClient(inbox=[{"id": "msg-1"}], notices=[{"id": "note-1"}])
    runtime = SyntheticRuntime()
    runtime.handle_message = lambda receipt: {"handled": True}
    runtime.review_notification = lambda notice: {"reviewed": True}
    worker = AgentWorker(client, PROJECT, AGENT, RUN, runtime=runtime, enabled=True,
                         messaging_delegate=object())
    assert worker._poll_messages() == 0 and client.delivered == []
    assert worker._poll_notifications() == 0 and client.acked == []
    # Default behavior is unchanged without the delegate.
    plain = AgentWorker(FakeClient(inbox=[{"id": "msg-1"}]), PROJECT, AGENT, RUN,
                        runtime=runtime, enabled=True)
    assert plain._poll_messages() == 1


def test_assigned_task_stays_running_while_reply_completes(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    client = client_for(world, receiver_token)
    task = world.call("POST", "tasks", {"project_id": world.project,
                                        "title": "Primary work",
                                        "description": "Long running", "scope_ids": []},
                      receiver_token)
    started = client.start_task(task["id"], primary["id"], task["revision"])
    execution_id = started["execution"]["id"]
    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True)
        status = supervisor.start()
        assert status["auxiliary_run_id"] and status["auxiliary_run_id"] != primary["id"]
        assert wait_for(lambda: message_state(world, message, receiver_token) == "acked")
        report = supervisor.stop()
    assert report["stopped"] and report["handled"] == 1 and report["unknown"] == 0
    # The primary assignment was never stolen, replaced, or fenced off.
    current = world.call("GET", f"tasks/{task['id']}", token=receiver_token)
    assert current["status"] == "running"
    assert current["execution"]["id"] == execution_id
    assert current["execution"]["run_id"] == primary["id"]
    assert world.call("GET", f"runs/{primary['id']}",
                      token=receiver_token)["task_id"] == task["id"]
    # One deterministic reply to the original sender, sent under the auxiliary run.
    replies = replies_to(world, sender, sender_token)
    assert len(replies) == 1
    reply = world.call("GET", f"messages/{replies[0]['id']}", token=sender_token)
    assert reply["to"] == sender["id"] and reply["from"] == receiver["id"]
    assert reply["in_reply_to"] == message["id"]
    assert reply["thread_id"] == message["thread_id"]
    assert reply.get("sender_run_id") == status["auxiliary_run_id"]
    # The handling task is a real canonical record under the auxiliary run, scope-less.
    tasks = world.call("GET", f"tasks?project_id={world.project}", token=receiver_token)
    handling = next(t for t in tasks if t["id"] != task["id"])
    assert handling["scope_ids"] == [] and handling["status"] == "completed"
    assert handling["execution"]["run_id"] == status["auxiliary_run_id"]
    client.close()


def test_heartbeat_and_poll_loop_continue_during_long_turn(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    first = send(world, sender, receiver, text="first", token=sender_token)
    second = send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True,
                                     during_turn=during_turn)
        status = supervisor.start()
        aux = status["auxiliary_run_id"]
        assert entered.wait(10)
        seen = world.call("GET", f"runs/{aux}", token=receiver_token)["last_seen_at"]
        time.sleep(0.4)
        # The producer keeps heartbeating the auxiliary run through the long turn...
        later = world.call("GET", f"runs/{aux}", token=receiver_token)["last_seen_at"]
        assert later > seen
        # ...and keeps polling: the second delivery is claimed and queued, not stuck.
        assert wait_for(lambda: supervisor.status()["queued"] >= 1, timeout=10)
        # Exactly one vendor turn in flight: the queue serializes deliveries.
        runtime = supervisor._gateways[0].adapter._runtime()
        assert len(runtime.calls) == 1
        release.set()
        assert wait_for(lambda: message_state(world, first, receiver_token) == "acked"
                        and message_state(world, second, receiver_token) == "acked")
        report = supervisor.stop()
    assert report["handled"] == 2 and report["unknown"] == 0
    assert len(runtime.calls) == 2
    assert len(replies_to(world, sender, sender_token)) == 2


def test_pause_control_blocks_claims_and_gates_reply_then_resume_recovers(world,
                                                                          tmp_path):
    sender = world.agent("Sender")
    receiver = world.agent("Receiver", ["pause", "cancel", "steer"])
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    lease_token, lease = coordinator(world)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True,
                                     during_turn=during_turn)
        supervisor.start()
        assert entered.wait(10)
        # Pause the primary run mid-turn: the in-flight turn finishes, but the
        # reply/settle effect is gated and the receipt is failed, never acked.
        pause = control(world, lease_token, lease, primary["id"], "pause")
        release.set()
        assert wait_for(lambda: message_state(world, message, receiver_token)
                        == "available")
        assert replies_to(world, sender, sender_token) == []
        # While the control is pending, no new delivery is claimed.
        second = send(world, sender, receiver, text="second", token=sender_token)
        time.sleep(0.6)
        assert message_state(world, second, receiver_token) == "available"
        runtime = supervisor._gateways[0].adapter._runtime()
        assert len(runtime.calls) == 1  # the queued pause blocks the second turn too
        # Clearing the control unblocks redelivery: release the lease, then a fresh
        # acquisition retires (supersedes) the stale queued control.
        world.call("POST", f"projects/{world.project}/nexus:release", {}, lease_token)
        coordinator(world)
        assert wait_for(lambda: message_state(world, message, receiver_token) == "acked"
                        and message_state(world, second, receiver_token) == "acked")
        report = supervisor.stop()
    assert report["blocked_turns"] >= 1 and report["unknown"] == 0
    assert len(runtime.calls) == 2  # one vendor turn per message, never re-run
    replies = replies_to(world, sender, sender_token)
    assert len(replies) == 2
    assert {r["in_reply_to"] for r in replies} == {message["id"], second["id"]}
    handling = [t for t in world.call("GET", f"tasks?project_id={world.project}",
                                      token=receiver_token)]
    assert {t["status"] for t in handling} == {"completed"}
    assert pause["action"] == "pause"


def test_cancel_control_halts_supervisor_without_claiming(world, tmp_path):
    sender = world.agent("Sender")
    receiver = world.agent("Receiver", ["pause", "cancel", "steer"])
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    lease_token, lease = coordinator(world)
    # The cancel control is pending before the supervisor starts, so no delivery is
    # ever claimed and no vendor turn ever runs.
    control(world, lease_token, lease, primary["id"], "cancel")
    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]])
        supervisor.start()
        assert wait_for(lambda: supervisor.stopped, timeout=10)
        report = supervisor.stop()
    assert report["halt_reason"] == "control:cancel"
    assert message_state(world, message, receiver_token) == "available"
    # The runtime factory was never even invoked.
    assert supervisor._gateways[0].adapter._cached_runtime is None


def test_restart_reuses_auxiliary_run_and_dedups_without_replay(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        first = make_supervisor(world, receiver, primary, journal, tmp_path,
                                token=receiver_token,
                                allowed_senders=[sender["id"]], allow_replies=True)
        first.start()
        assert wait_for(lambda: message_state(world, message, receiver_token) == "acked")
        aux_run_id = first.auxiliary_run_id
        assert first.stop()["stopped"]
        tasks_before = world.call("GET", f"tasks?project_id={world.project}",
                                  token=receiver_token)
        replies_before = replies_to(world, sender, sender_token)
        # A fresh supervisor over the same journal: the deterministic run key replays
        # to the same canonical auxiliary run; re-handling the same message is a
        # journaled no-op with no vendor turn, no new task, and no duplicate reply.
        second = make_supervisor(world, receiver, primary, journal, tmp_path,
                                 token=receiver_token,
                                 allowed_senders=[sender["id"]], allow_replies=True)
        second.start()
        assert second.auxiliary_run_id == aux_run_id
        outcome = second._gateways[0].handle_message({"id": message["id"]})
        assert outcome["deduplicated"] and outcome["handled"]
        assert second._gateways[0].adapter._cached_runtime is None  # never invoked
        assert second.stop()["stopped"]
    assert world.call("GET", f"tasks?project_id={world.project}",
                      token=receiver_token) == tasks_before
    assert len(replies_to(world, sender, sender_token)) == len(replies_before)


def test_unknown_outcome_is_never_acked_and_surfaces_for_recovery(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    unknowns = []
    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token, behavior="crash",
                                     allowed_senders=[sender["id"]],
                                     on_unknown=lambda kind, ident, err:
                                     unknowns.append((kind, ident)))
        supervisor.start()
        assert wait_for(lambda: bool(unknowns), timeout=10)
        # No ack, no fail: the lease stays until expiry; redelivery stays blocked by
        # the durable unknown record.
        assert message_state(world, message, receiver_token) in ("leased", "delivered")
        evidence = supervisor.unknown_operations()
        assert evidence and all(op["method"] == "HANDLER" for op in evidence)
        assert unknowns[0][0] == "message"
        report = supervisor.stop()
    assert report["unknown"] >= 1 and report["handled"] == 0
    assert replies_to(world, sender, sender_token) == []


def test_bounded_shutdown_blocks_effects_and_releases_queued_receipts(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    first = send(world, sender, receiver, text="first", token=sender_token)
    second = send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True,
                                     during_turn=during_turn, shutdown_timeout=0.5)
        supervisor.start()
        assert entered.wait(10)
        assert wait_for(lambda: supervisor.status()["queued"] >= 1, timeout=10)
        # Bounded: the in-flight vendor call is not interruptible within the budget.
        report = supervisor.stop(timeout=0.5)
        assert report["stopped"] is False and report["alive_threads"]
        release.set()
        # The finishing turn's reply/settle is gated off after shutdown; the queued
        # receipt is failed back to available without any effect.
        assert wait_for(lambda: supervisor.stop(timeout=15)["stopped"], timeout=20)
    assert wait_for(lambda: message_state(world, first, receiver_token) == "available"
                    and message_state(world, second, receiver_token) == "available")
    assert replies_to(world, sender, sender_token) == []
    # No effects after shutdown: a later poll with a fresh supervisor owns redelivery.
    with Journal(tmp_path / "journal.sqlite") as journal:
        successor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                    token=receiver_token,
                                    allowed_senders=[sender["id"]], allow_replies=True)
        successor.start()
        assert wait_for(lambda: message_state(world, first, receiver_token) == "acked"
                        and message_state(world, second, receiver_token) == "acked")
        done = successor.stop()
    assert done["handled"] == 2 and done["unknown"] == 0
    assert len(replies_to(world, sender, sender_token)) == 2


def test_notifications_are_isolated_and_acknowledged_separately(world, tmp_path):
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    world.post("Looping in the agent", token=sender_token,
               segments=[{"kind": "text", "text": "cc "},
                         {"kind": "agent_mention", "agent_id": receiver["id"]}])
    stranger = world.agent("Stranger")
    stranger_token = world.credential(stranger)
    unwanted = send(world, stranger, receiver, text="not allowed", token=stranger_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(
            world, receiver, primary, journal, tmp_path, token=receiver_token,
            behavior={"handled": True, "reply_text": None}, poll_interval=0.3,
            allowed_senders=[], allowed_notification_types=["board_mention"])
        supervisor.start()
        client = client_for(world, receiver_token)
        assert wait_for(lambda: client.notifications(receiver["id"],
                                                     world.project) == [])
        time.sleep(0.8)  # the unauthorized sender's mail is never acked or replied
        report = supervisor.stop()
    assert report["notices"] == 1 and report["handled"] == 0
    assert message_state(world, unwanted, receiver_token) != "acked"
    assert replies_to(world, stranger, stranger_token) == []
    # The notice produced a real canonical completed task and no reply channel.
    handling = world.call("GET", f"tasks?project_id={world.project}",
                          token=receiver_token)
    assert len(handling) == 1 and handling[0]["status"] == "completed"
    client.close()


def test_effect_gate_definite_block_leaves_no_effect_record(tmp_path):
    """Unit seam: the gate raises before the effect phase, so redelivery resumes it."""
    from test_runtime_messaging import FakeCanonicalClient, make_executor

    client = FakeCanonicalClient()

    def gate(item, contract):
        raise EffectsBlocked("blocked by test")

    handle, journal = make_executor(tmp_path / "gated", client=client, effect_gate=gate)
    receipt = {"id": "msg-1", "delivery_id": "d-1"}
    message = {"id": "msg-1", "project_id": PROJECT, "from": "agent-2", "to": AGENT,
               "type": "request", "thread_id": "thread-1",
               "content": {"media_type": "text/plain", "text": "Ping?"}}
    with pytest.raises(EffectsBlocked):
        handle.handle_message(receipt, message)
    kinds = [op["path"] for op in journal.journal.operations()]
    assert "runtime-message-effect" not in kinds  # no poisoned effect record
    journal.journal.close()
    # Ungated, same canonical client and journal: resumes at the effect phase.
    handle2, journal2 = make_executor(tmp_path / "gated", client=client)
    outcome = handle2.handle_message(receipt, message)
    assert outcome["handled"] and outcome["result"]["replied"]
    assert len(handle2._runtime().calls) == 0  # the vendor turn was deduplicated
    journal2.journal.close()


def apply_control(world, lease_token, lease, run_id, action, observed, target_token):
    """Drive a control through ack + applied result, changing the target run status."""
    item = control(world, lease_token, lease, run_id, action)
    world.call("POST", f"controls/{item['id']}:ack", {}, target_token)
    world.call("POST", f"controls/{item['id']}:result",
               {"outcome": "applied", "observed_status": observed,
                "checkpoint_id": f"checkpoint-{item['id']}"}, target_token)
    return item


def test_paused_auxiliary_run_blocks_and_terminal_auxiliary_halts(world, tmp_path):
    """An APPLIED pause of the auxiliary run (status paused, no pending control) blocks
    exactly like a primary pause: no claims, no turns, no effects. An applied cancel
    (terminal status) halts the supervisor permanently."""
    sender = world.agent("Sender")
    receiver = world.agent("Receiver", ["pause", "resume", "cancel", "steer"])
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    lease_token, lease = coordinator(world)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True)
        supervisor.start()
        aux = supervisor.auxiliary_run_id
        apply_control(world, lease_token, lease, aux, "pause", "paused", receiver_token)
        assert world.call("GET", f"runs/{aux}", token=receiver_token)["status"] == "paused"
        message = send(world, sender, receiver, token=sender_token)
        assert wait_for(lambda: (supervisor.status()["blocked"] or {}).get("run_status")
                        == "paused")
        time.sleep(0.5)
        # Fenced off behind an applied pause: never claimed, no vendor session opened.
        assert message_state(world, message, receiver_token) == "available"
        assert supervisor._gateways[0].adapter._cached_runtime is None
        # A terminal auxiliary run halts the supervisor rather than degrading.
        apply_control(world, lease_token, lease, aux, "cancel", "cancelled",
                      receiver_token)
        assert world.call("GET", f"runs/{aux}", token=receiver_token)["status"] == "cancelled"
        assert wait_for(lambda: supervisor._halt_reason == "auxiliary_run_cancelled")
        time.sleep(0.3)
        assert message_state(world, message, receiver_token) == "available"
        report = supervisor.stop()
    assert report["stopped"] and report["halt_reason"] == "auxiliary_run_cancelled"
    assert report["handled"] == 0 and report["unknown"] == 0


def test_renewal_failure_blocks_effects_and_resume_needs_no_new_model(world, tmp_path):
    """A definite lease-renewal failure drops the outstanding lease; the finishing
    turn's reply/settle is gated off, and later redelivery reuses the journaled
    decision without any new vendor turn."""
    from test_agent_worker import http_error

    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True,
                                     during_turn=during_turn)
        supervisor.start()
        assert entered.wait(10)
        client = supervisor.client
        healthy_renew = client.renew_message_lease

        def broken_renew(message_id, run_id, delivery_id, ttl_seconds=30, key=None):
            raise http_error(409, "lease_conflict")

        client.renew_message_lease = broken_renew
        # The definite 409 drops the outstanding lease while the turn is in flight.
        assert wait_for(lambda: supervisor._outstanding_snapshot() == {})
        release.set()
        runtime = supervisor._gateways[0].adapter._runtime()
        # The reply/settle is gated off: no reply, no ack, no new turn.
        assert wait_for(lambda: message_state(world, message, receiver_token)
                        in ("available", "retry_wait"))
        time.sleep(0.5)
        assert replies_to(world, sender, sender_token) == []
        assert len(runtime.calls) == 1
        # Renewal healthy again: redelivery dedups the journaled task+turn decision
        # and runs only the effect phase — no new model call.
        client.renew_message_lease = healthy_renew
        assert wait_for(lambda: message_state(world, message, receiver_token) == "acked")
        report = supervisor.stop()
    assert len(runtime.calls) == 1 and report["handled"] == 1 and report["unknown"] == 0
    assert len(replies_to(world, sender, sender_token)) == 1
    handling = world.call("GET", f"tasks?project_id={world.project}", token=receiver_token)
    assert len(handling) == 1 and handling[0]["status"] == "completed"


def test_stale_queued_lease_is_discarded_without_a_turn(world, tmp_path):
    """A queued receipt whose lease read no longer shows this supervisor's exact
    delivery (expired or stolen) is discarded: no vendor turn, no ack, no fail."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    first = send(world, sender, receiver, text="first", token=sender_token)
    second = send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True,
                                     during_turn=during_turn)
        supervisor.start()
        assert entered.wait(10)
        assert wait_for(lambda: supervisor.status()["queued"] == 1)
        # The queued receipt is the one not yet in a turn; steal exactly its lease.
        queued = supervisor._queue.queue[0][1]
        stolen_id = queued["id"]
        in_flight_id = second["id"] if stolen_id == first["id"] else first["id"]
        client = supervisor.client
        original_request = client.request

        def spoofed(method, path, body=None, key=None):
            result = original_request(method, path, body, key=key)
            if method == "GET" and path == f"messages/{stolen_id}":
                result = {**result, "claim_run_id": "run-thief",
                          "delivery_id": "delivery-stolen"}
            return result

        client.request = spoofed
        release.set()
        assert wait_for(lambda: supervisor.status()["stale"] == 1)
        assert wait_for(lambda: message_state(world, {"id": in_flight_id},
                                              receiver_token) == "acked")
        runtime = supervisor._gateways[0].adapter._runtime()
        assert len(runtime.calls) == 1  # the stale receipt never reached the runtime
        assert stolen_id not in supervisor._outstanding_snapshot()
        detail = world.call("GET", f"messages/{stolen_id}", token=receiver_token)
        assert detail["state"] != "acked" and detail["attempt_count"] == 1
        client.request = original_request
        report = supervisor.stop()
    assert report["handled"] == 1 and report["failed"] == 0 and report["unknown"] == 0


def test_duplicate_periodic_claim_is_suppressed(world, tmp_path):
    """The same message is never claimed twice while outstanding (queued/in-flight)."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    first = send(world, sender, receiver, text="first", token=sender_token)
    second = send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]],
                                     during_turn=during_turn)
        supervisor.start()
        assert entered.wait(10)
        assert wait_for(lambda: supervisor.status()["queued"] == 1)
        for _ in range(3):  # adversarial extra polls while one is queued
            supervisor._poll_messages()
        assert supervisor._queue.qsize() == 1
        assert len(supervisor._outstanding_snapshot()) == 2  # one in-flight, one queued
        detail = world.call("GET", f"messages/{second['id']}", token=receiver_token)
        assert detail["attempt_count"] == 1 and detail["total_attempts"] == 1
        release.set()
        assert wait_for(lambda: message_state(world, first, receiver_token) == "acked"
                        and message_state(world, second, receiver_token) == "acked")
        report = supervisor.stop()
    assert report["handled"] == 2 and report["failed"] == 0 and report["unknown"] == 0
    assert supervisor._outstanding_snapshot() == {}  # bounded map drains fully


def test_pre_reply_gate_block_is_retryable_and_resume_needs_no_new_turn(tmp_path):
    """Executor unit seam: a gate refusal before the reply reopens the effect record
    as pending (never poisoned unknown); redelivery resumes without a new turn."""
    from hivemind.recovery import derive_operation_id
    from hivemind.runtime_messaging import MESSAGE_EFFECT_KIND
    from test_runtime_messaging import FakeCanonicalClient, make_executor

    client = FakeCanonicalClient()
    calls = []

    def gate(item, contract):
        calls.append(item["id"])
        if len(calls) == 2:  # the pre-reply boundary (pre-effect passed)
            raise EffectsBlocked("fenced before reply")

    handle, journal = make_executor(tmp_path / "pre-reply", client=client,
                                    effect_gate=gate)
    receipt = {"id": "msg-1", "delivery_id": "d-1"}
    message = {"id": "msg-1", "project_id": PROJECT, "from": "agent-2", "to": AGENT,
               "type": "request", "thread_id": "thread-1",
               "content": {"media_type": "text/plain", "text": "Ping?"}}
    with pytest.raises(EffectsBlocked):
        handle.handle_message(receipt, message)
    operation_id = derive_operation_id("handler", MESSAGE_EFFECT_KIND,
                                       f"{PROJECT}:{AGENT}:msg-1")
    record = journal.journal.operation(operation_id)
    assert record["state"] == "pending" and not record["attempting"]  # retryable
    assert journal.journal.operations(state="unknown") == []
    assert client.posts == {}  # no reply ran
    journal.journal.close()
    handle2, journal2 = make_executor(tmp_path / "pre-reply", client=client)
    outcome = handle2.handle_message(receipt, message)
    assert outcome["handled"] and outcome["result"]["replied"]
    assert len(handle2._runtime().calls) == 0  # journaled decision reused, no new turn
    assert len(client.posts) == 1
    journal2.journal.close()


def test_pre_settle_gate_after_reply_is_honest_unknown(tmp_path):
    """A gate refusal after the reply was posted is genuinely uncertain: the effect
    record is unknown for the reconciler, never acked, and redelivery cannot
    duplicate the deterministic reply."""
    from hivemind.runtime_messaging import MESSAGE_EFFECT_KIND
    from hivemind.recovery import UnknownOutcome, derive_operation_id
    from test_runtime_messaging import FakeCanonicalClient, make_executor

    client = FakeCanonicalClient()
    calls = []

    def gate(item, contract):
        calls.append(item["id"])
        if len(calls) == 3:  # the pre-settle boundary (reply already posted)
            raise EffectsBlocked("fenced before settle")

    handle, journal = make_executor(tmp_path / "pre-settle", client=client,
                                    effect_gate=gate)
    receipt = {"id": "msg-1", "delivery_id": "d-1"}
    message = {"id": "msg-1", "project_id": PROJECT, "from": "agent-2", "to": AGENT,
               "type": "request", "thread_id": "thread-1",
               "content": {"media_type": "text/plain", "text": "Ping?"}}
    with pytest.raises(UnknownOutcome):
        handle.handle_message(receipt, message)
    assert len(client.posts) == 1  # the reply is on the wire
    operation_id = derive_operation_id("handler", MESSAGE_EFFECT_KIND,
                                       f"{PROJECT}:{AGENT}:msg-1")
    assert journal.journal.operation(operation_id)["state"] == "unknown"
    journal.journal.close()
    # Redelivery stays blocked on the durable unknown record; the reply is not
    # duplicated and the vendor turn is not re-run.
    handle2, journal2 = make_executor(tmp_path / "pre-settle", client=client)
    with pytest.raises(UnknownOutcome):
        handle2.handle_message(receipt, message)
    assert len(client.posts) == 1 and handle2._runtime().calls == []
    journal2.journal.close()


def test_authorize_recovery_effect_verifies_identity_fence_and_liveness(world,
                                                                        tmp_path):
    """The public recovery gate: fresh identity/liveness/fence checks, True only for
    this supervisor's own live auxiliary run and current task/execution."""
    from hivemind.recovery import derive_operation_id
    from hivemind.runtime_messaging import MESSAGE_EFFECT_KIND, MessagingError

    sender = world.agent("Sender")
    receiver = world.agent("Receiver", ["pause", "cancel", "steer"])
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    lease_token, lease = coordinator(world)
    release = threading.Event()
    entered = threading.Event()

    def during_turn(task, context):
        entered.set()
        release.wait(20)

    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]], allow_replies=True,
                                     during_turn=during_turn)
        supervisor.start()
        assert entered.wait(10)
        assert wait_for(lambda: any(op["path"] == "runtime-message-task"
                                    and op["state"] == "completed"
                                    for op in journal.operations()))
        task_op = next(op for op in journal.operations()
                       if op["path"] == "runtime-message-task")
        context = {"run_id": supervisor.auxiliary_run_id,
                   "item_id": message["id"],
                   "task_id": task_op["result"]["task_id"],
                   "execution_id": task_op["result"]["execution_id"],
                   "operation_id": derive_operation_id(
                       "handler", MESSAGE_EFFECT_KIND,
                       f"{world.project}:{receiver['id']}:{message['id']}")}
        # Live auxiliary run owning its current task: authorized.
        assert supervisor.authorize_recovery_effect("runtime-message-effect",
                                                    context) is True
        with pytest.raises(EffectsBlocked, match="delivery lease"):
            supervisor.authorize_recovery_effect("reply", {**context, "item_id": "foreign-message"})
        original_request = supervisor.client.request

        def stolen(method, path, body=None, key=None):
            result = original_request(method, path, body, key=key)
            if method == "GET" and path == f"messages/{message['id']}":
                result = {**result, "delivery_id": "stolen"}
            return result

        supervisor.client.request = stolen
        try:
            with pytest.raises(EffectsBlocked, match="delivery lease"):
                supervisor.authorize_recovery_effect("settle", context)
        finally:
            supervisor.client.request = original_request
        # Foreign run / task / execution identities are refused definitely.
        with pytest.raises(MessagingError):
            supervisor.authorize_recovery_effect("runtime-message-effect",
                                                 {**context, "run_id": primary["id"]})
        with pytest.raises(MessagingError):
            supervisor.authorize_recovery_effect("runtime-message-effect",
                                                 {**context, "task_id": "task-other"})
        with pytest.raises(MessagingError):
            supervisor.authorize_recovery_effect(
                "runtime-message-effect", {**context, "execution_id": "exec-other"})
        with pytest.raises(MessagingError):
            supervisor.authorize_recovery_effect("runtime-message-effect", {})
        # A closed control fence blocks recovery effects too.
        control(world, lease_token, lease, primary["id"], "pause")
        with pytest.raises(EffectsBlocked):
            supervisor.authorize_recovery_effect("runtime-message-effect", context)
        # After shutdown, nothing is authorized.
        release.set()
        assert supervisor.stop(timeout=15)["stopped"]
        with pytest.raises(EffectsBlocked):
            supervisor.authorize_recovery_effect("runtime-message-effect", context)


def make_slot_supervisor(world, receiver, primary, journal, tmp_path, slots=2,
                         entered=None, release=None, **overrides):
    """Two genuinely independent slots: per-slot state dirs, per-slot runtimes."""
    client = client_for(world, overrides.pop("token"))
    made = []
    counter = {"index": 0}

    def factory():
        index = counter["index"]
        counter["index"] += 1

        def during_turn(task, context):
            if entered is not None:
                entered[index].set()
            if release is not None:
                release.wait(20)

        runtime = FakeVendorRuntime(tmp_path / f"vendor-slot{index}",
                                    during_turn=during_turn)
        made.append(runtime)
        return runtime

    base = {"primary_run_id": primary["id"], "journal": journal, "enabled": True,
            "runtime_factory": factory, "max_in_flight": slots, "poll_interval": 0.1}
    base.update(overrides)
    supervisor = AssignedRunMessagingSupervisor(
        client, world.project, receiver["id"], **base)
    supervisor._made_runtimes = made
    return supervisor


def claiming_run(world, message, token):
    return world.call("GET", f"messages/{message['id']}", token=token).get(
        "claim_run_id")


def test_two_slots_run_concurrently_with_exact_slot_bindings(world, tmp_path):
    """Two independent slots: distinct canonical runs, distinct private runtimes,
    and each delivery's claim/task/ack bound to the SAME slot's run."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = [threading.Event(), threading.Event()]
    first = send(world, sender, receiver, text="first", token=sender_token)
    second = send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_slot_supervisor(world, receiver, primary, journal, tmp_path,
                                          token=receiver_token, entered=entered,
                                          release=release,
                                          allowed_senders=[sender["id"]],
                                          allow_replies=True)
        status = supervisor.start()
        assert status["slots"] == 2
        runs = status["auxiliary_run_ids"]
        assert len(set(runs)) == 2 and primary["id"] not in runs
        assert status["auxiliary_run_id"] == runs[0]
        # Both slots run a turn at the same time (real concurrency, never shared).
        assert all(event.wait(10) for event in entered)
        runtimes = supervisor._made_runtimes
        assert len(runtimes) == 2 and runtimes[0] is not runtimes[1]
        assert runtimes[0].state_path != runtimes[1].state_path
        # Each delivery is claimed by exactly one slot's run, and the two deliveries
        # landed on different slots.
        claim_runs = {claiming_run(world, first, receiver_token),
                      claiming_run(world, second, receiver_token)}
        assert claim_runs == set(runs)
        release.set()
        assert wait_for(lambda: message_state(world, first, receiver_token) == "acked"
                        and message_state(world, second, receiver_token) == "acked")
        report = supervisor.stop()
    assert report["handled"] == 2 and report["unknown"] == 0
    # Exact slot binding end to end: each handling task's execution ran under the
    # same run that claimed the delivery.
    messages = {first["id"]: first, second["id"]: second}
    tasks = world.call("GET", f"tasks?project_id={world.project}", token=receiver_token)
    handling = [t for t in tasks if t["scope_ids"] == []]
    assert len(handling) == 2
    replies = replies_to(world, sender, sender_token)
    assert len(replies) == 2
    for message_id, message in messages.items():
        claim = claiming_run(world, message, receiver_token)
        # The ack used the claiming run: delivery state is acked under that run.
        detail = world.call("GET", f"messages/{message_id}", token=receiver_token)
        assert detail["delivery_status"] == "acked"
        assert claim in runs


def test_factory_identity_collision_across_slots_is_rejected(tmp_path):
    """A factory that hands two slots the same instance is refused at
    materialization: slots never share a mutable vendor session."""
    shared = object()
    with Journal(tmp_path / "j.sqlite") as journal:
        supervisor = AssignedRunMessagingSupervisor(
            object(), PROJECT, AGENT, primary_run_id=RUN, journal=journal,
            runtime_factory=lambda: shared, max_in_flight=2)
        first = supervisor._slot_factories[0]()
        assert first is shared
        with pytest.raises(MessagingError, match="never share a vendor session"):
            supervisor._slot_factories[1]()


def test_prebuilt_mapping_runs_two_independent_slots(world, tmp_path):
    """An explicit validated {slot: runtime} mapping is accepted for >1 slots."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    client = client_for(world, receiver_token)
    runtimes = {index: FakeVendorRuntime(tmp_path / f"mapped-slot{index}")
                for index in range(2)}
    first = send(world, sender, receiver, text="first", token=sender_token)
    second = send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = AssignedRunMessagingSupervisor(
            client, world.project, receiver["id"], primary_run_id=primary["id"],
            journal=journal, enabled=True, runtime=runtimes, max_in_flight=2,
            allowed_senders=[sender["id"]], allow_replies=True, poll_interval=0.1)
        supervisor.start()
        assert wait_for(lambda: message_state(world, first, receiver_token) == "acked"
                        and message_state(world, second, receiver_token) == "acked")
        report = supervisor.stop()
    assert report["handled"] == 2
    assert sum(len(runtime.calls) for runtime in runtimes.values()) == 2
    client.close()


def test_lease_theft_in_one_slot_does_not_disturb_the_other(world, tmp_path):
    """Stealing slot 1's lease gates only that delivery: the other slot's turn,
    reply, and ack proceed; the stolen delivery is never acked by this supervisor."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = [threading.Event(), threading.Event()]
    send(world, sender, receiver, text="first", token=sender_token)
    send(world, sender, receiver, text="second", token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_slot_supervisor(world, receiver, primary, journal, tmp_path,
                                          token=receiver_token, entered=entered,
                                          release=release,
                                          allowed_senders=[sender["id"]],
                                          allow_replies=True)
        supervisor.start()
        assert all(event.wait(10) for event in entered)
        snapshot = supervisor._outstanding_snapshot()
        assert len(snapshot) == 2
        # Identify the slot-1 delivery and steal exactly its lease.
        stolen_id = next(mid for mid, entry in snapshot.items()
                         if entry["slot"] == 1)
        safe_id = next(mid for mid, entry in snapshot.items()
                       if entry["slot"] == 0)
        client = supervisor.client
        original_request = client.request

        def spoofed(method, path, body=None, key=None):
            result = original_request(method, path, body, key=key)
            if method == "GET" and path == f"messages/{stolen_id}":
                result = {**result, "claim_run_id": "run-thief",
                          "delivery_id": "delivery-stolen"}
            return result

        client.request = spoofed
        release.set()
        try:
            assert wait_for(lambda: message_state(world, {"id": safe_id},
                                                  receiver_token) == "acked")
            report = supervisor.stop(timeout=15)
        finally:
            client.request = original_request
    assert report["stopped"] and report["handled"] == 1
    detail = world.call("GET", f"messages/{stolen_id}", token=receiver_token)
    assert detail["delivery_status"] != "acked"
    replies = replies_to(world, sender, sender_token)
    assert len(replies) == 1 and replies[0]["in_reply_to"] == safe_id


def test_recovery_authorization_binds_item_to_its_own_slot_run(world, tmp_path):
    """authorize_recovery_effect accepts any OWN slot run, but the item's exact
    lease must be held by THAT run: slot 0's lease never authorizes slot 1's
    recovery, and vice versa."""
    from hivemind.recovery import derive_operation_id
    from hivemind.runtime_messaging import MESSAGE_EFFECT_KIND, MessagingError

    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    release = threading.Event()
    entered = [threading.Event(), threading.Event()]
    message = send(world, sender, receiver, token=sender_token)
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_slot_supervisor(world, receiver, primary, journal, tmp_path,
                                          token=receiver_token, entered=entered,
                                          release=release,
                                          allowed_senders=[sender["id"]],
                                          allow_replies=True)
        supervisor.start()
        assert wait_for(lambda: any(op["path"] == "runtime-message-task"
                                    and op["state"] == "completed"
                                    for op in journal.operations()))
        runs = supervisor.auxiliary_run_ids
        entry = supervisor._outstanding_snapshot()[message["id"]]
        owner, other = entry["run_id"], next(r for r in runs
                                             if r != entry["run_id"])
        task_op = next(op for op in journal.operations()
                       if op["path"] == "runtime-message-task")
        context = {"run_id": owner, "item_id": message["id"],
                   "task_id": task_op["result"]["task_id"],
                   "execution_id": task_op["result"]["execution_id"],
                   "operation_id": derive_operation_id(
                       "handler", MESSAGE_EFFECT_KIND,
                       f"{world.project}:{receiver['id']}:{message['id']}")}
        # The owning slot's run, live, holding the exact lease: authorized.
        assert supervisor.authorize_recovery_effect("runtime-message-effect",
                                                    context) is True
        # The lease pin is per slot: the same item under the other slot's run fails
        # the exact-lease check even before task identity is considered.
        assert supervisor._lease_held(message["id"], run_id=owner) is True
        assert supervisor._lease_held(message["id"], run_id=other) is False
        with pytest.raises(MessagingError):
            supervisor.authorize_recovery_effect("runtime-message-effect",
                                                 {**context, "run_id": other})
        with pytest.raises(MessagingError):
            supervisor.authorize_recovery_effect("runtime-message-effect",
                                                 {**context,
                                                  "run_id": primary["id"]})
        release.set()
        assert wait_for(lambda: message_state(world, message, receiver_token)
                        == "acked")
        report = supervisor.stop()
    assert report["handled"] == 1 and report["unknown"] == 0


def test_push_signal_wakes_dispatch_and_startup_always_reconciles(world, tmp_path):
    """With a push signal, dispatch does not poll on the timer: a message sent after
    startup waits for the signal, while a pre-existing inbox entry is reconciled at
    startup without any signal. Heartbeats/lease renewal stay timer-driven."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    early = send(world, sender, receiver, text="before start", token=sender_token)
    signal = threading.Event()
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]],
                                     allow_replies=True,
                                     poll_interval=30.0, push_signal=signal)
        status = supervisor.start()
        assert status["push"] is True
        # Startup reconcile: handled without any push signal despite the long tick.
        assert wait_for(lambda: message_state(world, early, receiver_token)
                        == "acked", timeout=10)
        # No timer polling now: a new message sits unclaimed until the push arrives.
        late = send(world, sender, receiver, text="after start", token=sender_token)
        time.sleep(1.0)
        assert message_state(world, late, receiver_token) == "available"
        assert supervisor.status()["handled"] == 1
        signal.set()
        assert wait_for(lambda: message_state(world, late, receiver_token)
                        == "acked", timeout=10)
        report = supervisor.stop()
    assert report["handled"] == 2 and report["unknown"] == 0
    # Exactly one deterministic reply per message, even woken by push.
    assert len(replies_to(world, sender, sender_token)) == 2


def test_push_signal_during_fence_reconciles_after_reopen(world, tmp_path):
    """A push consumed while the control fence is closed is not lost: the supervisor
    re-reconciles the canonical inbox when the fence reopens."""
    sender = world.agent("Sender")
    receiver = world.agent("Receiver", ["pause", "cancel", "steer"])
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    lease_token, lease = coordinator(world)
    signal = threading.Event()
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]],
                                     allow_replies=True,
                                     poll_interval=0.2, push_signal=signal)
        supervisor.start()
        control(world, lease_token, lease, primary["id"], "pause")
        assert wait_for(lambda: supervisor.status()["blocked"] is not None)
        message = send(world, sender, receiver, token=sender_token)
        signal.set()  # consumed while fenced: must not be lost
        time.sleep(0.5)
        assert message_state(world, message, receiver_token) == "available"
        world.call("POST", f"projects/{world.project}/nexus:release", {}, lease_token)
        coordinator(world)
        assert wait_for(lambda: message_state(world, message, receiver_token)
                        == "acked")
        report = supervisor.stop()
    assert report["handled"] == 1


def test_push_outage_falls_back_to_bounded_canonical_reconcile(world, tmp_path):
    """A completely silent push signal (publication lost) cannot silence dispatch:
    the bounded periodic canonical reconcile still delivers within its bound."""
    sender, receiver = world.agent("Sender"), world.agent("Receiver")
    sender_token, receiver_token = world.credential(sender), world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    silent = threading.Event()  # never set: the push publication was lost
    with Journal(tmp_path / "journal.sqlite") as journal:
        supervisor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                     token=receiver_token,
                                     allowed_senders=[sender["id"]],
                                     allow_replies=True, poll_interval=0.2,
                                     push_signal=silent, max_reconcile_seconds=1.0)
        supervisor.start()
        message = send(world, sender, receiver, token=sender_token)
        assert wait_for(lambda: message_state(world, message, receiver_token)
                        == "acked", timeout=15)
        report = supervisor.stop()
    assert report["handled"] == 1
    assert len(replies_to(world, sender, sender_token)) == 1
    with pytest.raises(ValueError, match="max_reconcile_seconds"):
        make_supervisor(world, receiver, primary, journal, tmp_path,
                        token=receiver_token, max_reconcile_seconds=0)


def test_reused_run_with_active_control_is_refused(world, tmp_path):
    """Resuming an explicitly reused auxiliary run that has an active control
    would silently override an operator fence: refuse it instead."""
    sender = world.agent("Sender")
    receiver = world.agent("Receiver", ["pause", "cancel", "steer"])
    receiver_token = world.credential(receiver)
    primary = world.run(receiver, receiver_token)
    lease_token, lease = coordinator(world)
    with Journal(tmp_path / "journal.sqlite") as journal:
        first = make_supervisor(world, receiver, primary, journal, tmp_path,
                                token=receiver_token,
                                allowed_senders=[sender["id"]])
        first.start()
        auxiliary = first.auxiliary_run_id
        assert first.stop()["stopped"]
        control(world, lease_token, lease, auxiliary, "pause")  # queued, active
        successor = make_supervisor(world, receiver, primary, journal, tmp_path,
                                    token=receiver_token,
                                    allowed_senders=[sender["id"]],
                                    auxiliary_run_id=auxiliary)
        with pytest.raises(MessagingError, match="active control"):
            successor.start()
        world.call("POST", f"projects/{world.project}/nexus:release", {},
                   lease_token)
        coordinator(world)  # supersedes the stale control
        clean = make_supervisor(world, receiver, primary, journal, tmp_path,
                                token=receiver_token,
                                allowed_senders=[sender["id"]],
                                auxiliary_run_id=auxiliary)
        assert clean.start()["auxiliary_run_id"] == auxiliary
        assert clean.stop()["stopped"]


def test_policy_still_required_for_gateways(tmp_path):
    """A supervisor-built gateway enforces the sender allowlist on redelivery paths."""
    from hivemind.runtime_messaging import UnauthorizedSender

    gateway_policy = MessagingPolicy(enabled=True, allowed_senders=["agent-9"])
    assert gateway_policy.allowed_senders == frozenset({"agent-9"})
    with pytest.raises(UnauthorizedSender):
        gateway_policy.authorize_message(
            {"id": "m", "from": "agent-2", "to": AGENT}, AGENT)
