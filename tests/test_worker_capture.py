"""Tests for the worker capture lifecycle and automatic safe-pause controls.

Real loopback tests run the in-process service (``world`` fixture) with local
principals only: no network, no paid/vendor sessions. The background watcher
thread, durable pending-signal ledger, restart, foreign-binding, corrupt-state,
capture-failure, watcher-constructor-failure, watchdog-stop-failure, timeout,
disabled, unsupported-adapter, and worker-boundary paths are covered.
"""

import json
import threading
import time

import httpx
import pytest

from hivemind import runtime_lifecycle as rl
from hivemind import worker_capture as wc
from hivemind.agent_worker import AgentWorker, WorkerStopped
from hivemind.sdk import Capture
from hivemind.storage import canonical, uid


class LocalClient:
    """HTTP-shaped client over the in-process test application."""

    def __init__(self, world, token):
        self.world, self.token = world, token

    def request(self, method, path, body=None, key=None):
        response = self.world.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + self.token, "Idempotency-Key": key or uid("key")})
        assert response.status_code < 400, response.text
        return response.json()["data"]

    def notifications(self, agent_id, project_id):
        return self.request("GET", f"agents/{agent_id}/notifications?project_id={project_id}")

    def close(self):
        pass


class LenientClient(LocalClient):
    """Raises a typed Problem on rejection, as the production SDK client would."""

    def request(self, method, path, body=None, key=None):
        from hivemind.storage import Problem
        response = self.world.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + self.token, "Idempotency-Key": key or uid("key")})
        if response.status_code >= 400:
            raise Problem(response.json()["error"]["code"], response.status_code)
        return response.json()["data"]


class Journal:
    def __init__(self):
        self.entries = []

    def append(self, entry):
        self.entries.append(entry)

    def kinds(self):
        return [entry["kind"] for entry in self.entries]


class AckingAdapter:
    """Declares its controls and establishes a stop with a quiescent checkpoint."""

    name = "acking"
    authoritative_absence = True
    trusted_attribution = False
    supported_controls = ("pause", "cancel", "resume")

    def __init__(self, *, applied=True, checkpoint="checkpoint-quiescent"):
        self.applied, self.checkpoint = applied, checkpoint

    def inspect(self, run=None):
        return rl.AdapterObservation(present=True, status="running", observed_at=time.time())

    def pause(self, run=None):
        return {"applied": self.applied, "status": "paused", "checkpoint_id": self.checkpoint}

    def cancel(self, run=None):
        return {"applied": self.applied, "status": "cancelled", "checkpoint_id": self.checkpoint}

    def resume(self, run=None):
        return {"applied": self.applied, "status": "running", "checkpoint_id": self.checkpoint}


class ObserveOnlyAdapter:
    """Presence observation, no declared controls: an unsupported runtime."""

    name = "observe-only"
    authoritative_absence = False
    trusted_attribution = False

    def inspect(self, run=None):
        return rl.AdapterObservation(present=True, status="running", observed_at=time.time())


def coordinator(world):
    nexus = world.agent("Nexus")
    token = world.credential(nexus, coordinator=True)
    run = world.run(nexus, token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": run["id"]}, token)
    return token, lease


def make_worker(world, name="CaptureWorker", actions=None):
    agent = world.agent(name, actions=actions)
    token = world.credential(agent)
    run = world.run(agent, token)
    return agent, token, run


def config(tmp_path, **overrides):
    values = {"enabled": True, "workspace_root": str(tmp_path / "workspace"),
              "state_dir": str(tmp_path / "state"), "poll_interval_seconds": 60.0,
              "flush_interval": 60.0}  # Threads never fire mid-test; scans/drains are explicit.
    (tmp_path / "workspace").mkdir(parents=True, exist_ok=True)
    values.update(overrides)
    return wc.WorkerCaptureConfig(**values)


def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


# --------------------------------------------------------------------------
# Config strictness
# --------------------------------------------------------------------------

def test_capture_config_is_strict_and_default_off(tmp_path):
    assert wc.WorkerCaptureConfig().enabled is False
    for bad in ({"enabled": 1}, {"poll_interval_seconds": 0.01},
                {"flush_interval": float("inf")}, {"max_queue": "8"},
                {"workspace_root": 3}, {"limits": {"max_files": 1}},
                {"exclude": "*.pem"},              # A bare string is not a pattern list.
                {"exclude": ["ok", 5]}, {"exclude": [""]}, {"exclude": ["x" * 513]}):
        with pytest.raises(ValueError):
            wc.WorkerCaptureConfig(**bad)
    with pytest.raises(ValueError):
        wc.WorkerCaptureConfig(enabled=True, workspace_root=None,
                               state_dir=str(tmp_path))
    # Secret/ignored state paths are rejected after resolve.
    with pytest.raises(ValueError, match="secret"):
        wc.WorkerCaptureConfig(enabled=True, workspace_root=str(tmp_path),
                               state_dir=str(tmp_path / ".env"))
    with pytest.raises(ValueError, match="secret"):
        wc.WorkerCaptureConfig(workspace_root=str(tmp_path / "id_rsa"))
    with pytest.raises(ValueError):
        wc.capture_config_from_mapping({"enabled": True, "bogus": 1})
    with pytest.raises(ValueError):
        wc.capture_config_from_mapping({"exclude": "*.pem"})
    built = wc.capture_config_from_mapping(
        {"enabled": True, "workspace_root": str(tmp_path), "state_dir": str(tmp_path / "s"),
         "limits": {"max_files": 5}, "exclude": ["*.pem"]})
    assert built.enabled and built.limits.max_files == 5 and built.exclude == ("*.pem",)


def test_automatic_policy_is_strict_and_default_off():
    assert wc.AutomaticPolicy().enabled is False
    for bad in ({"enabled": 1}, {"auto_resume": "yes"}, {"capture_gap_threshold": 0},
                {"observed_changes_threshold": "2"},
                {"conflict_signals": ["claim_conflict", 7]},
                {"conflict_signals": "claim_conflict"}):  # Never a tuple of characters.
        with pytest.raises(ValueError):
            wc.AutomaticPolicy(**bad)
    with pytest.raises(ValueError):
        wc.AutomaticPolicy(enabled=False, auto_resume=True)
    with pytest.raises(ValueError):
        wc.automatic_policy_from_mapping({"enabled": True, "unknown": 1})
    with pytest.raises(ValueError):
        wc.automatic_policy_from_mapping({"conflict_signals": "claim_conflict"})
    policy = wc.automatic_policy_from_mapping(
        {"enabled": True, "observed_changes_threshold": 2, "auto_resume": True,
         "conflict_signals": ["claim_conflict"]})
    assert policy.auto_resume and policy.conflict_signals == ("claim_conflict",)


def test_operation_descriptors_describe_purpose_authority_effects_retry_limits():
    for name in ("worker_capture.start", "worker_capture.drain", "worker_capture.signals",
                 "worker_capture.ack_signals", "worker_capture.close",
                 "controls.tick", "controls.resume"):
        descriptor = wc.WORKER_CAPTURE_DESCRIPTORS[name]
        assert descriptor.purpose and descriptor.authority and descriptor.side_effects
        assert isinstance(descriptor.retry_safe, bool)
    assert wc.WORKER_CAPTURE_DESCRIPTORS["controls.resume"].retry_safe is False
    assert wc.WORKER_CAPTURE_DESCRIPTORS["worker_capture.ack_signals"].retry_safe is False


# --------------------------------------------------------------------------
# Lifecycle against the real loopback service
# --------------------------------------------------------------------------

def test_lifecycle_is_default_off_and_constructing_performs_no_requests(tmp_path):
    lifecycle = wc.WorkerCaptureLifecycle(None, "p", "a", "r", wc.WorkerCaptureConfig())
    assert not lifecycle.running
    with pytest.raises(RuntimeError, match="not enabled"):
        lifecycle.start()
    assert lifecycle.close()["closed"] is True


def test_lifecycle_binds_observer_run_captures_and_restarts_dedup(world, tmp_path):
    agent, token, run = make_worker(world)
    client = LocalClient(world, token)
    journal = Journal()
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path), journal=journal)
    receipt = lifecycle.start()
    observer_id = receipt["observer_run_id"]
    assert receipt["status"] == "started" and observer_id != run["id"]
    observer = world.call("GET", f"runs/{observer_id}", token)
    assert observer["project_id"] == world.project and observer["agent_id"] == agent["id"]
    # Startup dedup: a second start on the live lifecycle is a receipt, not a run.
    assert lifecycle.start()["status"] == "already_running"
    # Observed changes flow: watcher scan -> drain -> spool; signals peek, then ack.
    root = tmp_path / "workspace"
    lifecycle._watcher.scan()
    (root / "a.ts").write_text("a")
    lifecycle._watcher.scan()
    drained = lifecycle.drain()
    assert drained["transferred"] == 1
    signals = lifecycle.pending_signals()
    assert signals == {"observed_changes": 1, "capture_gaps": 0}
    # Peek, not consume: the signal survives until a durable policy decision acks it.
    assert lifecycle.pending_signals()["observed_changes"] == 1
    lifecycle.ack_signals()
    assert lifecycle.pending_signals() == {"observed_changes": 0, "capture_gaps": 0}
    assert lifecycle.capture_health()["state"] == "healthy"
    report = lifecycle.close()
    assert report["closed"] is True and report["errors"] == []
    # Shutdown order: start intent first, final stop entry only once fully closed.
    assert journal.kinds()[0] == "capture_start_intent"
    assert journal.kinds()[-1] == "capture_stop"
    assert "capture_stop_pending" not in journal.kinds()

    # Restart over the same state reuses the observer run; no duplicate is minted.
    restarted = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path), journal=journal)
    receipt = restarted.start()
    assert receipt["observer_run_id"] == observer_id and receipt["starts"] == 2
    runs = world.call("GET", f"ledger?project_id={world.project}", token)["runs"]
    assert sum(1 for item in runs if item["id"] == observer_id) == 1
    assert restarted.close()["closed"] is True


def test_pending_signal_survives_restart_before_intent(world, tmp_path):
    agent, token, run = make_worker(world)
    client = LocalClient(world, token)
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    lifecycle.start()
    root = tmp_path / "workspace"
    lifecycle._watcher.scan()
    (root / "a.ts").write_text("a")
    lifecycle._watcher.scan()
    lifecycle.drain()
    assert lifecycle.pending_signals()["observed_changes"] == 1
    # Crash/restart before any policy tick: the pending signal is durable.
    assert lifecycle.close()["closed"] is True
    restarted = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    assert restarted.pending_signals()["observed_changes"] == 1  # Survives pre-start.
    restarted.start()
    assert restarted.pending_signals()["observed_changes"] == 1  # Not double-counted.
    restarted.ack_signals()
    assert restarted.pending_signals() == {"observed_changes": 0, "capture_gaps": 0}
    assert restarted.close()["closed"] is True


def test_lifecycle_state_refuses_foreign_binding_and_corruption(world, tmp_path):
    agent, token, run = make_worker(world)
    client = LocalClient(world, token)
    before = threading.active_count()
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    lifecycle.start()
    lifecycle.close()
    # A state file rebound to another project/agent/run refuses to load.
    state = json.loads(lifecycle.state_path.read_text(encoding="utf-8"))
    state["bound_to"]["run_id"] = "run_other"
    lifecycle.state_path.write_text(canonical(state), encoding="utf-8")
    with pytest.raises(ValueError, match="another project/agent/run"):
        wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                  config(tmp_path))
    # Corrupt state refuses rather than silently re-baselining.
    lifecycle.state_path.write_text("{broken")
    with pytest.raises(ValueError, match="unreadable"):
        wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                  config(tmp_path))
    # Startup refusal leaked nothing: no threads, no new resources.
    assert threading.active_count() == before


def test_watcher_constructor_failure_cleans_up_capture(world, tmp_path, monkeypatch):
    agent, token, run = make_worker(world)
    journal = Journal()
    closed = []

    class SpyCapture(Capture):
        def close(self):
            closed.append(True)
            super().close()

    def exploding(*args, **kwargs):
        raise ValueError("watcher constructor exploded")

    monkeypatch.setattr(wc, "Capture", SpyCapture)
    monkeypatch.setattr(wc, "_AccountingWatcher", exploding)
    lifecycle = wc.WorkerCaptureLifecycle(LocalClient(world, token), world.project,
                                          agent["id"], run["id"], config(tmp_path),
                                          journal=journal)
    with pytest.raises(ValueError, match="exploded"):
        lifecycle.start()
    assert closed == [True]  # The spool's flush thread was stopped and DB closed.
    assert lifecycle._capture is None and lifecycle._watcher is None
    # The lifecycle is startable again after the failed stage.
    monkeypatch.undo()
    assert lifecycle.start()["status"] == "started"
    assert lifecycle.close()["closed"] is True


def test_lifecycle_capture_failure_keeps_resources_safe(world, tmp_path, monkeypatch):
    agent, token, run = make_worker(world)
    journal = Journal()
    lifecycle = wc.WorkerCaptureLifecycle(LocalClient(world, token), world.project,
                                          agent["id"], run["id"], config(tmp_path),
                                          journal=journal)
    lifecycle.start()
    (tmp_path / "workspace" / "a.ts").write_text("a")
    lifecycle._watcher.scan()

    def failing_flush():
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(lifecycle._capture, "flush", failing_flush)
    report = lifecycle.close()
    assert report["closed"] is False and report["shutdown_pending"] is True
    assert any(error.startswith("capture_flush:") for error in report["errors"])
    assert lifecycle._watcher is None  # Released after a successful stop.
    assert lifecycle._capture is not None  # Retained for a shutdown retry.
    # The journal stays open (a pending note, not the final stop entry).
    assert journal.kinds()[-1] == "capture_stop_pending"
    monkeypatch.undo()
    retry = lifecycle.close()
    assert retry["closed"] is True and retry["errors"] == []
    assert lifecycle._capture is None
    assert journal.kinds()[-1] == "capture_stop"


def test_watcher_stop_failure_retains_spool_and_exit_raises(world, tmp_path, monkeypatch):
    agent, token, run = make_worker(world)
    journal = Journal()
    lifecycle = wc.WorkerCaptureLifecycle(LocalClient(world, token), world.project,
                                          agent["id"], run["id"], config(tmp_path),
                                          journal=journal)

    def stuck(timeout=10.0):
        raise RuntimeError("Watcher shutdown timed out; cursor and outbox retained")

    with pytest.raises(wc.CleanupError) as caught:
        with lifecycle:
            monkeypatch.setattr(lifecycle._watcher, "stop", stuck)
    report = caught.value.report
    assert report["shutdown_pending"] is True
    assert any(error.startswith("watcher_stop:") for error in report["errors"])
    assert "capture_retained:watcher_alive" in report["errors"]
    # The spool was never closed under the live writer thread; both are retained.
    assert lifecycle._watcher is not None and lifecycle._capture is not None
    lifecycle._capture.db.execute("SELECT count(*) FROM spool").fetchone()  # Still open.
    # The journal was never finalized and remains usable for the parent.
    assert "capture_stop" not in journal.kinds()
    journal.append({"kind": "parent_note"})
    monkeypatch.undo()
    assert lifecycle.close()["closed"] is True
    assert journal.kinds()[-1] == "capture_stop"


# --------------------------------------------------------------------------
# Background watcher thread drives policy (no manual scans/drains)
# --------------------------------------------------------------------------

def test_background_thread_gap_drives_automatic_pause(world, tmp_path):
    lease = coordinator(world)
    agent, token, run = make_worker(world, name="ThreadDriven",
                                    actions=["pause", "resume", "cancel"])
    root = tmp_path / "workspace"
    root.mkdir(parents=True, exist_ok=True)
    (root / "a.ts").write_text("a")
    (root / "b.ts").write_text("b")
    # A tight scan limit makes every background scan truncated: an explicit gap.
    cfg = config(tmp_path, poll_interval_seconds=0.1,
                 limits=rl.ScanLimits(max_files=1))
    lifecycle = wc.WorkerCaptureLifecycle(LocalClient(world, token), world.project,
                                          agent["id"], run["id"], cfg)
    lifecycle.start()
    # The watcher's own loop scans AND drains; accounting still observes the gap.
    assert wait_for(lambda: lifecycle.pending_signals().get("capture_gaps", 0) >= 1)
    controls = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run["id"],
        tmp_path / "thread-controls.json", adapter=AckingAdapter(),
        control_client=LenientClient(world, lease[0]),
        coordinator_epoch=lambda: lease[1]["coordinator_epoch"],
        policy=wc.AutomaticPolicy(enabled=True, capture_gap_threshold=1))
    worker = AgentWorker(LocalClient(world, token), world.project, agent["id"], run["id"],
                         runtime=None, enabled=True, poll_interval=0.1,
                         capture_lifecycle=lifecycle, automatic_controls=controls)
    report = worker.tick()
    assert report["automatic_controls"]["status"] == "applied"
    assert report["control"] == "pause" and report["task_id"] is None
    assert controls.hold()["state"] == "pause_applied"
    # The durable decision acknowledged the pending gap signals.
    assert wait_for(lambda: lifecycle.pending_signals() == {
        "observed_changes": 0, "capture_gaps": 0})
    assert lifecycle.close()["closed"] is True


# --------------------------------------------------------------------------
# Automatic controls against the real loopback service
# --------------------------------------------------------------------------

def build_controls(world, tmp_path, lease, *, name="Auto", adapter=None, policy=None):
    lease_token, lease_body = lease
    agent, token, run = make_worker(world, name, actions=["pause", "resume", "cancel"])
    scope = world.scope(key=f"scope/{name.lower()}-{uid('s')}", paths=["src/" + name.lower()])
    world.claim(agent, run, scope)
    controls = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run["id"],
        tmp_path / f"{name}-controls.json", adapter=adapter or AckingAdapter(),
        control_client=LenientClient(world, lease_token),
        coordinator_epoch=lambda: lease_body["coordinator_epoch"],
        policy=policy or wc.AutomaticPolicy(enabled=True, observed_changes_threshold=2))
    return controls, run


def test_automatic_pause_applies_with_supported_adapter(world, tmp_path):
    lease = coordinator(world)
    controls, run = build_controls(world, tmp_path, lease)
    assert controls.tick({"observed_changes": 1, "capture_gaps": 0})["status"] == "clear"
    outcome = controls.tick({"observed_changes": 2, "capture_gaps": 0})
    assert outcome["status"] == "applied" and outcome["hold"] == "pause_applied"
    assert controls.held and controls.hold()["checkpoint_id"] == "checkpoint-quiescent"
    control = world.call("GET", f"controls/{outcome['receipt']['control_id']}")
    assert control["state"] == "applied"
    # A tick while paused without auto_resume stays held; nothing is replayed.
    assert controls.tick({"observed_changes": 9})["status"] == "held"
    # Absent signals can never imply "problem cleared" for auto-resume.
    controls.policy = wc.AutomaticPolicy(enabled=True, observed_changes_threshold=2,
                                         auto_resume=True)
    assert controls.tick(None)["status"] == "held"
    assert controls.tick({"reset": True, "observed_changes": 0})["status"] == "held"
    assert controls.tick({})["status"] == "held"  # Missing threshold value: incomplete.
    # An explicit, complete, clear signal set resumes from the exact checkpoint once.
    resumed = controls.tick({"observed_changes": 0})
    assert resumed["status"] == "resume_applied" and resumed["hold"] == "clear"
    assert not controls.held
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    assert controls.controller.resume(revision, "checkpoint-quiescent")["reason"] == (
        "checkpoint_consumed")


def test_observed_changes_are_not_drift(world, tmp_path):
    lease = coordinator(world)
    # A drift-only policy never triggers on watcher-observed filesystem events:
    # known tool changes are observed changes, not unobserved drift.
    controls, _run = build_controls(
        world, tmp_path, lease, name="DriftOnly",
        policy=wc.AutomaticPolicy(enabled=True, drift_findings_threshold=1))
    outcome = controls.tick({"observed_changes": 50, "capture_gaps": 0})
    assert outcome["status"] == "clear" and not controls.held
    # Drift arrives only through the explicit trusted signal seam.
    triggered = controls.tick({"drift_findings": 1, "observed_changes": 0})
    assert triggered["status"] == "applied" and controls.held


def test_automatic_pause_unsupported_adapter_is_held_without_control(world, tmp_path):
    lease = coordinator(world)
    controls, run = build_controls(world, tmp_path, lease, name="Unsupported",
                                   adapter=ObserveOnlyAdapter())
    outcome = controls.tick({"observed_changes": 5})
    assert outcome["status"] == "held"
    assert outcome["receipt"]["evidence"]["state"] == "unsupported_adapter"
    assert controls.hold()["state"] == "pause_held"
    controls_list = world.call("GET",
                               f"agents/{controls.controller.agent}/controls"
                               f"?project_id={world.project}", lease[0])
    assert controls_list == []  # No canonical control exists for an unsupported runtime.
    # The hold persists across process restart: durable intents are authoritative.
    reloaded = wc.AutomaticControls(
        controls.controller.client, world.project, controls.controller.agent, run["id"],
        controls.controller.path, adapter=ObserveOnlyAdapter(),
        policy=wc.AutomaticPolicy(enabled=True, observed_changes_threshold=2))
    assert reloaded.held and reloaded.tick({"observed_changes": 0})["status"] == "held"


def test_automatic_controls_disabled_and_conflict_signals(world, tmp_path):
    lease = coordinator(world)
    controls, _run = build_controls(
        world, tmp_path, lease, name="Disabled", policy=wc.AutomaticPolicy(enabled=False))
    assert controls.tick({"observed_changes": 99})["status"] == "disabled"
    assert not controls.held
    conflicted, _run2 = build_controls(
        world, tmp_path, lease, name="Conflicted",
        policy=wc.AutomaticPolicy(enabled=True, conflict_signals=("claim_conflict",),
                                  capture_gap_threshold=3))
    assert conflicted.tick({"conflicts": ["some_other"]})["status"] == "clear"
    assert conflicted.tick({"capture_gaps": 2})["status"] == "clear"
    assert conflicted.tick({"conflicts": ["claim_conflict"]})["status"] == "applied"
    assert conflicted.held


def test_automatic_controls_timeout_holds_unknown_never_fails(world, tmp_path, monkeypatch):
    lease = coordinator(world)
    controls, run = build_controls(world, tmp_path, lease, name="Timeout")

    def timeout(method, path, body=None, key=None):
        raise httpx.ReadTimeout("simulated timeout")

    monkeypatch.setattr(controls.controller.control_client, "request", timeout)
    outcome = controls.tick({"observed_changes": 2})
    assert outcome["status"] == "held"
    assert outcome["receipt"]["evidence"]["state"] == "transport_unknown"
    assert controls.held  # Unknown outcome blocks model work until resolution.
    monkeypatch.undo()
    # A re-tick does not create a second control or fabricate an outcome.
    again = controls.tick({"observed_changes": 2})
    assert again["status"] == "held"
    assert world.call("GET", f"agents/{controls.controller.agent}/controls"
                            f"?project_id={world.project}", lease[0]) == []


def test_automatic_controls_corrupt_state_refuses(world, tmp_path):
    lease = coordinator(world)
    controls, run = build_controls(world, tmp_path, lease, name="Corrupt")
    controls.controller.persist()
    controls.controller.path.write_text(canonical({
        "bound_to": {"project_id": world.project, "agent_id": controls.controller.agent,
                     "run_id": "run_foreign"}, "intents": {}, "consumed_checkpoints": []}))
    with pytest.raises(ValueError, match="another project/agent/run"):
        wc.AutomaticControls(controls.controller.client, world.project,
                             controls.controller.agent, run["id"],
                             controls.controller.path,
                             policy=wc.AutomaticPolicy(enabled=True))


# --------------------------------------------------------------------------
# AgentWorker boundary seam
# --------------------------------------------------------------------------

def static_signals():
    """An explicit installed signal source: quiet, no capture lifecycle."""
    return {"observed_changes": 0, "capture_gaps": 0}


def test_worker_seam_blocks_dispatch_while_policy_held(world, tmp_path):
    lease = coordinator(world)
    controls, run = build_controls(world, tmp_path, lease, name="Seamed")
    # LenientClient is request-compatible with the worker's client contract.
    worker = AgentWorker(controls.controller.client, world.project,
                         controls.controller.agent, run["id"], runtime=None,
                         enabled=True, poll_interval=0.1, automatic_controls=controls,
                         control_signals=static_signals)
    assert worker._pending_control() is None  # No hold yet.
    controls.tick({"observed_changes": 2})
    assert controls.held
    control = worker._pending_control()
    assert control == {"action": "pause", "policy_hold": "pause_applied"}
    report = worker.tick()
    assert report["control"] == "pause" and report["task_id"] is None
    assert report["automatic_controls"]["status"] == "held"


def test_worker_seam_blocks_long_turn_at_supported_boundary(world, tmp_path):
    lease = coordinator(world)
    controls, run = build_controls(world, tmp_path, lease, name="LongTurn")
    worker = AgentWorker(controls.controller.client, world.project,
                         controls.controller.agent, run["id"],
                         runtime=None, enabled=True, poll_interval=0.1,
                         automatic_controls=controls, control_signals=static_signals)
    # A policy hold raised mid-turn surfaces at the checkpoint boundary.
    controls.tick({"observed_changes": 2})
    boundary = worker._make_checkpoint("task-x", "execution-x")
    with pytest.raises(WorkerStopped, match="control:pause"):
        boundary("progress", progress=10)


def test_worker_signals_flow_from_capture_lifecycle(world, tmp_path):
    lease = coordinator(world)
    agent, token, run = make_worker(world, name="SignalFlow",
                                    actions=["pause", "resume", "cancel"])
    client = LocalClient(world, token)
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    lifecycle.start()
    controls = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run["id"],
        tmp_path / "signal-controls.json", adapter=AckingAdapter(),
        control_client=LenientClient(world, lease[0]),
        coordinator_epoch=lambda: lease[1]["coordinator_epoch"],
        policy=wc.AutomaticPolicy(enabled=True, observed_changes_threshold=1))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime=None,
                         enabled=True, poll_interval=0.1,
                         capture_lifecycle=lifecycle, automatic_controls=controls)
    root = tmp_path / "workspace"
    lifecycle._watcher.scan()
    (root / "change.ts").write_text("x")
    lifecycle._watcher.scan()
    lifecycle.drain()  # Account the observed change as a pending signal.
    assert lifecycle.pending_signals() == {"observed_changes": 1, "capture_gaps": 0}
    report = worker.tick()
    # The lifecycle's observed change triggered the policy pause at the boundary.
    assert report["automatic_controls"]["status"] == "applied"
    assert report["control"] == "pause" and report["task_id"] is None
    assert worker.capture_health["state"] == "healthy"
    assert worker._pending_control()["policy_hold"] == "pause_applied"
    # The durable pause intent acknowledged the pending signals exactly once.
    assert lifecycle.pending_signals() == {"observed_changes": 0, "capture_gaps": 0}
    assert lifecycle.close()["closed"] is True


def test_worker_without_seams_is_unchanged(world, tmp_path):
    agent, token, run = make_worker(world, name="Plain")
    worker = AgentWorker(LocalClient(world, token), world.project, agent["id"],
                         run["id"], runtime=None, enabled=False)
    assert worker.automatic_controls is None and worker.capture_lifecycle is None
    report = worker.tick()
    assert report["enabled"] is False and report["automatic_controls"] is None


def test_long_turn_background_gap_pauses_at_checkpoint_boundary(world, tmp_path):
    """A gap emitted mid-turn creates its durable pause intent at the next
    supported boundary, not after the turn ends; no dispatch follows."""
    lease = coordinator(world)
    agent, token, run = make_worker(world, name="MidTurn",
                                    actions=["pause", "resume", "cancel"])
    root = tmp_path / "workspace"
    root.mkdir(parents=True, exist_ok=True)
    (root / "a.ts").write_text("a")
    (root / "b.ts").write_text("b")
    cfg = config(tmp_path, poll_interval_seconds=0.1, limits=rl.ScanLimits(max_files=1))
    lifecycle = wc.WorkerCaptureLifecycle(LocalClient(world, token), world.project,
                                          agent["id"], run["id"], cfg)
    lifecycle.start()
    controls = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run["id"],
        tmp_path / "midturn-controls.json", adapter=AckingAdapter(),
        control_client=LenientClient(world, lease[0]),
        coordinator_epoch=lambda: lease[1]["coordinator_epoch"],
        policy=wc.AutomaticPolicy(enabled=True, capture_gap_threshold=1))
    worker = AgentWorker(LocalClient(world, token), world.project, agent["id"],
                         run["id"], runtime=None, enabled=True, poll_interval=0.1,
                         capture_lifecycle=lifecycle, automatic_controls=controls)
    # Before any background gap, the boundary is clear.
    assert worker._pending_control(check_policy=True) is None
    # The watcher's own thread emits and drains the truncated-scan gap.
    assert wait_for(lambda: lifecycle.pending_signals().get("capture_gaps", 0) >= 1)
    # The long-turn boundary runs the policy check itself: durable pause intent.
    control = worker._pending_control(check_policy=True)
    assert control == {"action": "pause", "policy_hold": "pause_applied"}
    intents = controls.controller.state["intents"]
    assert len(intents) == 1
    control_id = next(iter(intents.values()))["control_id"]
    canonical = world.call("GET", f"controls/{control_id}", lease[0])
    assert canonical["action"] == "pause" and canonical["state"] == "applied"
    # The durable decision consumed exactly the snapshotted gap signals.
    assert wait_for(lambda: lifecycle.pending_signals() == {
        "observed_changes": 0, "capture_gaps": 0})
    # A checkpoint inside a long turn stops the turn; no model work continues.
    boundary = worker._make_checkpoint("task-x", "execution-x")
    with pytest.raises(WorkerStopped, match="control:pause"):
        boundary("progress", progress=10)
    # And no new dispatch is issued after the hold.
    dispatches = worker.dispatches
    report = worker.tick()
    assert report["control"] == "pause" and report["task_id"] is None
    assert worker.dispatches == dispatches
    assert lifecycle.close()["closed"] is True


def test_concurrent_policy_checks_dedup_intent_and_ack_is_snapshot_exact(world, tmp_path):
    lease = coordinator(world)
    agent, token, run = make_worker(world, name="Concurrent",
                                    actions=["pause", "resume", "cancel"])
    client = LocalClient(world, token)
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    lifecycle.start()
    root = tmp_path / "workspace"
    lifecycle._watcher.scan()
    (root / "a.ts").write_text("a")
    lifecycle._watcher.scan()
    lifecycle.drain()
    controls = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run["id"],
        tmp_path / "concurrent-controls.json", adapter=AckingAdapter(),
        control_client=LenientClient(world, lease[0]),
        coordinator_epoch=lambda: lease[1]["coordinator_epoch"],
        policy=wc.AutomaticPolicy(enabled=True, observed_changes_threshold=1))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime=None,
                         enabled=True, poll_interval=0.1,
                         capture_lifecycle=lifecycle, automatic_controls=controls)

    # Force overlap: the first check holds the policy lock inside the snapshot.
    original = lifecycle.signal_snapshot
    entered = threading.Barrier(2)

    def slow_snapshot():
        entered.wait(timeout=10)
        time.sleep(0.2)
        return original()

    lifecycle.signal_snapshot = slow_snapshot
    outcomes = []
    first = threading.Thread(target=lambda: outcomes.append(worker._policy_check()))
    first.start()
    entered.wait(timeout=10)  # The first check holds the lock now.
    skipped = worker._policy_check()  # Concurrent check: skips, hold governs.
    first.join(timeout=30)
    lifecycle.signal_snapshot = original
    assert skipped is None  # No concurrent double tick.
    assert outcomes[0]["status"] == "applied"
    intents = controls.controller.state["intents"]
    assert len(intents) == 1  # Exactly one durable intent, hence one control.
    control_id = next(iter(intents.values()))["control_id"]
    assert world.call("GET", f"controls/{control_id}", lease[0])["state"] == "applied"
    # A later serialized check dedups on the stable intent; still one intent.
    again = worker._policy_check()
    assert again["status"] == "held"  # Hold now governs; no second dispatch.
    assert len(controls.controller.state["intents"]) == 1
    assert lifecycle.close()["closed"] is True


def test_ack_consumes_only_the_snapshot_ids(world, tmp_path):
    agent, token, run = make_worker(world, name="AckRace")
    client = LocalClient(world, token)
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    lifecycle.start()
    root = tmp_path / "workspace"
    lifecycle._watcher.scan()
    (root / "a.ts").write_text("a")
    lifecycle._watcher.scan()
    lifecycle.drain()
    snapshot = lifecycle.signal_snapshot()
    assert snapshot["signals"]["observed_changes"] == 1
    # A new change arrives between the policy evaluation and the ack.
    (root / "b.ts").write_text("b")
    lifecycle._watcher.scan()
    lifecycle.drain()
    assert lifecycle.pending_signals()["observed_changes"] == 2
    lifecycle.ack_signals({"ids": snapshot["ids"],
                           "reset_count": snapshot["reset_count"]})
    # The race arrival survives the ack; only the consumed snapshot cleared.
    assert lifecycle.pending_signals() == {"observed_changes": 1, "capture_gaps": 0}
    lifecycle.ack_signals()
    assert lifecycle.pending_signals() == {"observed_changes": 0, "capture_gaps": 0}
    assert lifecycle.close()["closed"] is True


# --------------------------------------------------------------------------
# Trusted signal collector: drift vs observed writes, real conflicts
# --------------------------------------------------------------------------

def make_collector(world, tmp_path, name="Collected", *, with_reconciler=True,
                   policy=None, lease=None):
    lease = lease or coordinator(world)
    agent, token, run = make_worker(world, name, actions=["pause", "resume", "cancel"])
    client = LocalClient(world, token)
    lifecycle = wc.WorkerCaptureLifecycle(client, world.project, agent["id"], run["id"],
                                          config(tmp_path))
    lifecycle.start()
    reconciler = None
    if with_reconciler:
        reconciler = rl.RuntimeReconciler(client, world.project, agent["id"], run["id"],
                                          state_dir=tmp_path / "reconciler",
                                          workspace_root=tmp_path / "workspace")
    collector = wc.TrustedSignalCollector(lifecycle, reconciler)
    controls = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run["id"],
        tmp_path / f"{name}-controls.json", adapter=AckingAdapter(),
        control_client=LenientClient(world, lease[0]),
        coordinator_epoch=lambda: lease[1]["coordinator_epoch"],
        policy=policy or wc.AutomaticPolicy(enabled=True, drift_findings_threshold=1))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime=None,
                         enabled=True, poll_interval=0.1,
                         capture_lifecycle=lifecycle, automatic_controls=controls,
                         control_signals=collector)
    return lease, lifecycle, collector, controls, worker


def test_collector_expected_writes_are_not_drift(world, tmp_path):
    lease, lifecycle, collector, controls, worker = make_collector(world, tmp_path)
    root = tmp_path / "workspace"
    reconciler = collector.reconciler
    lifecycle._watcher.scan()               # Watcher baseline on the quiet workspace.
    reconciler.drift(update_baseline=True)  # Reconciler baseline on the quiet workspace.
    # An expected tool write: the watcher observes and delivers it.
    (root / "expected.ts").write_text("x")
    lifecycle._watcher.scan()
    lifecycle.drain()
    # An unobserved external write: the watcher never scanned it.
    (root / "sneaky.ts").write_text("y")
    signals, _token = collector.snapshot()
    assert signals["observed_changes"] == 1  # The expected write is observed evidence.
    assert signals["drift_findings"] == 1    # Only sneaky.ts is drift.
    assert signals["conflicts"] == []
    # A fresh unobserved write for the worker's own policy check (the explicit
    # snapshot above advanced the reconciler baseline, as designed).
    (root / "sneaky2.ts").write_text("z")
    outcome = worker._policy_check()
    assert outcome["status"] == "applied" and controls.held
    assert lifecycle.pending_signals() == {"observed_changes": 0, "capture_gaps": 0}
    signals, _token = collector.snapshot()
    assert signals["drift_findings"] == 0  # Baseline advanced; no perpetual retrigger.
    assert lifecycle.close()["closed"] is True


def test_collector_relays_actual_conflict_replies(world, tmp_path):
    lease, lifecycle, collector, controls, worker = make_collector(
        world, tmp_path, name="Conflicts",
        policy=wc.AutomaticPolicy(enabled=True, conflict_signals=("revision_conflict",
                                                                  "claim_conflict")))
    # Only canonical codes relay; synthetic or unknown codes record nothing.
    worker._relay_conflict("revision_conflict")
    worker._relay_conflict("made_up_code")
    worker._relay_conflict(None)
    signals, _token = collector.snapshot()
    assert signals["conflicts"] == ["revision_conflict"]
    outcome = worker._policy_check()
    assert outcome["status"] == "applied" and controls.hold()["state"] == "pause_applied"
    # The persisted pause intent consumed exactly the recorded conflict.
    assert collector.snapshot()[0]["conflicts"] == []
    assert lifecycle.close()["closed"] is True


def test_policy_requiring_missing_sources_fails_unavailable(world, tmp_path):
    lease = coordinator(world)
    controls, run = build_controls(
        world, tmp_path, lease, name="NoSource",
        policy=wc.AutomaticPolicy(enabled=True, drift_findings_threshold=1))
    # Drift threshold with no lifecycle and no provider: refuse to wire.
    with pytest.raises(ValueError, match="drift_findings"):
        AgentWorker(controls.controller.client, world.project, controls.controller.agent,
                    run["id"], runtime=None, enabled=True, automatic_controls=controls)
    # A collector without a reconciler still cannot feed drift.
    agent, token, run2 = make_worker(world, name="NoReconciler",
                                     actions=["pause", "resume", "cancel"])
    lifecycle = wc.WorkerCaptureLifecycle(LocalClient(world, token), world.project,
                                          agent["id"], run2["id"],
                                          config(tmp_path / "nr"))
    lifecycle.start()
    collector = wc.TrustedSignalCollector(lifecycle, None)
    assert "drift_findings" not in collector.provides
    controls2 = wc.AutomaticControls(
        LenientClient(world, token), world.project, agent["id"], run2["id"],
        tmp_path / "norec-controls.json", adapter=AckingAdapter(),
        control_client=LenientClient(world, lease[0]),
        coordinator_epoch=lambda: lease[1]["coordinator_epoch"],
        policy=wc.AutomaticPolicy(enabled=True, drift_findings_threshold=1))
    with pytest.raises(ValueError, match="drift_findings"):
        AgentWorker(LocalClient(world, token), world.project, agent["id"], run2["id"],
                    runtime=None, enabled=True, capture_lifecycle=lifecycle,
                    automatic_controls=controls2, control_signals=collector)
    assert lifecycle.close()["closed"] is True
