import json
import secrets
import subprocess
import sys
import textwrap
import time

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import runtime_lifecycle as rl
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.models import Event
from hivemind.storage import Problem, canonical, now, uid


class LocalClient:
    """HTTP-shaped client over the in-process test application; no network or paid session."""

    def __init__(self, world, token):
        self.world, self.token = world, token

    def request(self, method, path, body=None, key=None):
        response = self.world.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + self.token, "Idempotency-Key": key or uid("key")})
        assert response.status_code < 400, response.text
        return response.json()["data"]

    def close(self):
        pass


class AckingAdapter:
    """Minimal in-process adapter: establishes a stop with a checkpoint on request."""

    name = "acking"
    authoritative_absence = True
    trusted_attribution = False

    def __init__(self, *, applied=True, status="paused", checkpoint="checkpoint-local", present=True,
                 trusted=False):
        self.applied, self.status, self.checkpoint, self.present = applied, status, checkpoint, present
        self.trusted_attribution = trusted

    def inspect(self, run=None):
        return rl.AdapterObservation(present=self.present, status="running", observed_at=time.time())

    def pause(self, run=None):
        return {"applied": self.applied, "status": self.status, "checkpoint_id": self.checkpoint}

    def resume(self, run=None):
        return {"applied": self.applied, "status": "running", "checkpoint_id": self.checkpoint}

    def cancel(self, run=None):
        return {"applied": self.applied, "status": "cancelled", "checkpoint_id": self.checkpoint}


def coordinator_lease(world):
    """One Nexus lease per project; the service rejects a second active lease."""
    nexus = world.agent("Nexus")
    token = world.credential(nexus, coordinator=True)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": world.run(nexus, token)["id"]}, token)
    return token, lease


def runtime_control(world, token, lease, run_id, action="pause"):
    """Create a control against the run's current revision under an existing lease."""
    current = world.call("GET", f"runs/{run_id}")
    return world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": run_id, "action": action,
        "target_run_revision": current["control_revision"], "coordinator_epoch": lease["coordinator_epoch"],
        "reason": "Runtime lifecycle boundary"}, token)


def age_presence(world, run_id, seconds=3600):
    store = world.app.state.store
    body = json.loads(store.raw(run_id)["body"])
    body["last_seen_at"] = time.time() - seconds
    with store.transaction():
        store.db.execute("UPDATE resources SET body=? WHERE id=?", (canonical(body), run_id))


def test_scan_is_bounded_contained_and_secret_excluded(tmp_path):
    root = tmp_path / "workspace"
    (root / "src").mkdir(parents=True)
    (root / "src" / "index.ts").write_text("export const header = 1;")
    (root / ".env").write_text("SERVICE_TOKEN=super-secret-value")
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text("[core]")
    (root / "node_modules").mkdir()
    (root / "node_modules" / "package.js").write_text("module.exports = 1")
    (root / ".ssh").mkdir()
    (root / ".ssh" / "id_rsa").write_text("PRIVATE")
    (root / "server.pem").write_text("-----BEGIN CERTIFICATE-----")
    deep = root / "deep"
    for level in range(4):
        deep = deep / f"l{level}"
        deep.mkdir(parents=True)
    (deep / "leaf.txt").write_text("leaf")
    outside = tmp_path / "outside.ts"
    outside.write_text("outside")
    linked = True
    try:
        (root / "link.ts").symlink_to(outside)
    except OSError:
        linked = False

    snapshot = rl.scan_workspace(root, limits=rl.ScanLimits(max_depth=3))
    files = snapshot["files"]
    assert "src/index.ts" in files
    assert files["src/index.ts"]["hashed"] is True
    assert len(files["src/index.ts"]["sha256"]) == 64
    for excluded_path in (".env", ".git/config", "node_modules/package.js", ".ssh/id_rsa", "server.pem"):
        assert excluded_path not in files
    assert not any(path.startswith("deep/") for path in files)
    reasons = {item["reason"] for item in snapshot["skipped"]}
    assert "max_depth" in reasons and "excluded" in reasons
    # Only hashes and sizes cross the boundary: no content, no secrets, no outside paths.
    rendered = json.dumps(snapshot)
    assert snapshot["root"] == str(root)
    assert "super-secret-value" not in rendered
    assert "export const header" not in rendered
    assert "outside.ts" not in rendered
    if linked:
        assert "link.ts" not in files
        assert "symlink" in reasons

    # File-count, byte, and hashing bounds truncate rather than growing without limit.
    (root / "src" / "second.ts").write_text("export const second = 2;")
    bounded = rl.scan_workspace(root, limits=rl.ScanLimits(max_files=1))
    assert bounded["truncated"] is True and len(bounded["files"]) == 1
    assert "max_files" in {item["reason"] for item in bounded["skipped"]}
    unhashed = rl.scan_workspace(root, limits=rl.ScanLimits(max_file_bytes=1))
    assert all(entry["hashed"] is False and entry["sha256"] is None for entry in unhashed["files"].values())
    assert rl.diff_snapshots(unhashed, unhashed) == []
    with pytest.raises(ValueError):
        rl.scan_workspace(root / "missing")


def test_drift_reports_hash_only_unattributed_evidence(world, tmp_path):
    agent = world.agent("Reconciler")
    token = world.credential(agent)
    run = world.run(agent, token)
    root = tmp_path / "workspace"
    (root / "src").mkdir(parents=True)
    (root / "src" / "kept.ts").write_text("CONTENT-ONE")
    changed = root / "src" / "changed.ts"
    changed.write_text("CONTENT-BEFORE")
    removed = root / "src" / "removed.ts"
    removed.write_text("CONTENT-GONE")
    reconciler = rl.RuntimeReconciler(
        LocalClient(world, token), world.project, agent["id"], run["id"], state_dir=tmp_path / "state",
        workspace_root=root, enabled=True, capture_authority=True,
        adapter=rl.FileAdapter(tmp_path / "runtime.json"))
    first, _ = reconciler.drift(update_baseline=True)
    assert {finding.change for finding in first} == {"created"}
    changed.write_text("CONTENT-AFTER")
    (root / "src" / "new.ts").write_text("CONTENT-NEW")
    removed.unlink()
    findings, _ = reconciler.drift()
    assert {(finding.path, finding.change) for finding in findings} == {
        ("src/changed.ts", "modified"), ("src/new.ts", "created"), ("src/removed.ts", "deleted")}
    assert all(finding.hashed for finding in findings)

    # Workspace drift never carries a confirmed actor, and mismatched workspaces are skipped.
    with pytest.raises(ValueError):
        reconciler.attribution("confirmed")
    assert reconciler.attribution() == "unattributed"
    assert reconciler.attribution("reported") == "reported"
    trusted = rl.RuntimeReconciler(
        LocalClient(world, token), world.project, agent["id"], run["id"], state_dir=tmp_path / "state-trusted",
        workspace_root=root, enabled=True, capture_authority=True,
        adapter=AckingAdapter(trusted=True))
    assert trusted.attribution("confirmed") == "confirmed"
    with pytest.raises(ValueError):
        trusted.observe_drift(findings, attribution="confirmed")
    observed = reconciler.observe_drift(findings, target_run=run)
    assert observed["attribution"] == "unattributed" and observed["kind"] == "filesystem"
    assert len(observed["reported"]) == 3 and observed["skipped"] == []
    mismatch = reconciler.observe_drift(findings, target_run={"id": "run_other", "workspace_id": "ws_other"})
    assert mismatch["reported"] == [] and mismatch["skipped"][0]["reason"] == "workspace_mismatch"

    assert reconciler.submit() == {"flushed": True, "pending": 0, "quarantined": 0}
    run_view = world.call("GET", f"runs/{run['id']}")
    assert run_view["files"]["src/new.ts"]["attribution"] == "unattributed"
    assert run_view["files"]["src/changed.ts"]["change"] == "modified"
    events = [event for event in world.call("GET", f"events?project_id={world.project}")
              if event["type"].endswith("file.changed")]
    assert {event["data"]["change"] for event in events} == {"created", "modified", "deleted"}
    assert {event["source"]["attribution"] for event in events} == {"unattributed"}
    assert {event["source"]["kind"] for event in events} == {"filesystem"}
    rendered = json.dumps(events)
    assert "CONTENT-AFTER" not in rendered and "CONTENT-GONE" not in rendered
    assert "CONTENT-NEW" not in rendered and "CONTENT-BEFORE" not in rendered

    disabled = rl.RuntimeReconciler(LocalClient(world, token), world.project, agent["id"], run["id"],
                                    state_dir=tmp_path / "state-disabled", workspace_root=root, enabled=False)
    assert disabled.observe_drift(findings) == {"reported": [], "skipped": [{"reason": "disabled"}],
                                                "attribution": None}


def test_abandoned_requires_authoritative_absence_and_grants(world, tmp_path):
    agent = world.agent("Lifecycle", ["pause", "cancel", "resume"])
    token = world.credential(agent)
    root = tmp_path / "workspace"
    root.mkdir()
    state = tmp_path / "runtime.json"

    def build(run, **options):
        options.setdefault("capture_authority", True)
        options.setdefault("enabled", True)
        return rl.RuntimeReconciler(LocalClient(world, options.pop("token", token)), world.project,
                                    agent["id"], run["id"], state_dir=tmp_path / "state",
                                    workspace_root=root, adapter=rl.FileAdapter(state, timeout=0.05),
                                    **options)

    def runtime_state(present, run_id, status="running", updated=None):
        state.write_text(json.dumps({"run_id": run_id, "present": present, "status": status,
                                     "updated_at": time.time() if updated is None else updated}))

    def audit(run_id):
        return json.loads((tmp_path / "state" / f"{run_id}-lifecycle.json").read_text())

    # No adapter observation at all: unknown presence is stale, never stopped.
    run = world.run(agent, token)
    decision = build(run).reconcile().decisions[0]
    assert (decision["classification"], decision["reason"]) == ("stale", "presence_unverified")
    assert decision["action"] == "none" and decision["outcome"] == "reported"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "running"
    no_adapter = rl.RuntimeReconciler(LocalClient(world, token), world.project, agent["id"], run["id"],
                                      state_dir=tmp_path / "state-none", enabled=True, capture_authority=True,
                                      terminal_grant=True)
    decision = no_adapter.reconcile().decisions[0]
    assert (decision["classification"], decision["reason"]) == ("stale", "no_adapter_observation")
    assert decision["observation"] is None and decision["action"] == "none"

    # An ancient last_seen_at is derived evidence only; a stale "running" claim is not absence.
    runtime_state(True, run["id"], updated=time.time() - 3600)
    age_presence(world, run["id"])
    decision = build(run).reconcile().decisions[0]
    assert (decision["classification"], decision["reason"]) == ("stale", "presence_unverified")
    assert decision["derived_presence"] == "offline" and decision["action"] == "none"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "running"

    runtime_state(True, run["id"])
    assert build(run).reconcile().decisions[0]["classification"] == "live"

    # A marker for a different run cannot end this one.
    runtime_state(False, "run_somewhere_else", status="stopped")
    decision = build(run).reconcile().decisions[0]
    assert (decision["classification"], decision["reason"]) == ("stale", "presence_unverified")

    # Authoritative absence without the operator grant stays evidence.
    runtime_state(False, run["id"], status="stopped")
    decision = build(run).reconcile().decisions[0]
    assert (decision["classification"], decision["reason"]) == ("abandoned", "terminal_grant_required")
    assert decision["action"] == "none" and world.call("GET", f"runs/{run['id']}")["status"] == "running"

    # Authoritative absence without the actor grant stays evidence.
    uncaptured = world.credential(agent, capture=False)
    decision = build(run, token=uncaptured, capture_authority=False, terminal_grant=True).reconcile().decisions[0]
    assert decision["reason"] == "capture_authority_required" and decision["action"] == "none"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "running"

    # An expired absence observation is stale: a restarted runtime must not be ended.
    runtime_state(False, run["id"], status="stopped", updated=time.time() - 3600)
    decision = build(run, terminal_grant=True).reconcile().decisions[0]
    assert decision["reason"] == "absence_observation_expired" and decision["action"] == "none"

    # Opt-out is inert even with both grants.
    runtime_state(False, run["id"], status="stopped")
    decision = build(run, enabled=False, terminal_grant=True).reconcile().decisions[0]
    assert decision["classification"] == "abandoned" and decision["reason"] == "disabled"

    # Both grants plus authoritative absence end the run through the canonical event path.
    report = build(run, terminal_grant=True).reconcile()
    decision = report.decisions[0]
    assert decision["action"] == "ended" and decision["outcome"] == "applied"
    assert decision["terminal_status"] == "failed" and decision["evidence"]["control_revision"] == 1
    assert decision["observation"]["detail"]["state"] == "reported"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "failed"
    assert audit(run["id"])["last_transition"]["state"] == "applied"

    # A terminal run is never re-classified as abandoned.
    decision = build(run, terminal_grant=True).reconcile().decisions[0]
    assert (decision["classification"], decision["reason"]) == ("terminal", "run_already_terminal")

    # Cancellation is an explicit operator choice; completion is never inferred.
    cancelled = world.run(agent, token)
    runtime_state(False, cancelled["id"], status="stopped")
    report = build(cancelled, terminal_grant=True, abandoned_status="cancelled").reconcile()
    assert report.decisions[0]["terminal_status"] == "cancelled"
    assert world.call("GET", f"runs/{cancelled['id']}")["status"] == "cancelled"
    with pytest.raises(ValueError):
        build(cancelled, abandoned_status="completed")


def test_pause_never_reports_applied_without_runtime_checkpoint(world, tmp_path):
    agent = world.agent("Paused", ["pause", "cancel", "resume"])
    token = world.credential(agent)
    lease_token, lease = coordinator_lease(world)
    state = tmp_path / "runtime.json"
    state.write_text(json.dumps({"present": True, "status": "running", "updated_at": time.time()}))

    def build(run, adapter):
        return rl.RuntimeReconciler(LocalClient(world, token), world.project, agent["id"], run["id"],
                                    state_dir=tmp_path / "state", enabled=True, capture_authority=True,
                                    adapter=adapter)

    # 1. The runtime never acknowledges: outcome_unknown, but the local host still stops.
    run = world.run(agent, token)
    control = runtime_control(world, lease_token, lease, run["id"])
    boundary = build(run, rl.FileAdapter(state, timeout=0.05)).boundary()
    boundary.boundary()
    settled = world.call("GET", f"controls/{control['id']}")
    assert settled["state"] == "outcome_unknown" and settled["result"]["outcome"] == "outcome_unknown"
    assert boundary.state["paused"] is True and control["id"] in boundary.state["unresolved"]
    assert boundary.state["runtime_observations"][control["id"]]["detail"]["state"] == "no_acknowledgement"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "running"
    with pytest.raises(RuntimeError):
        boundary.permit()
    boundary.boundary()
    assert boundary.state["runtime_settled"] == [control["id"]]

    # 2. The runtime establishes the stop and a checkpoint: applied.
    verified = world.run(agent, token)
    control = runtime_control(world, lease_token, lease, verified["id"])
    boundary = build(verified, AckingAdapter()).boundary()
    boundary.boundary()
    assert world.call("GET", f"controls/{control['id']}")["state"] == "applied"
    assert world.call("GET", f"runs/{verified['id']}")["status"] == "paused"
    assert boundary.state["paused"] is True and boundary.state.get("unresolved") in (None, [])

    # 3. A runtime that reports success without a checkpoint cannot claim applied.
    uncheckpointed = world.run(agent, token)
    control = runtime_control(world, lease_token, lease, uncheckpointed["id"])
    boundary = build(uncheckpointed, AckingAdapter(checkpoint=None)).boundary()
    boundary.boundary()
    assert world.call("GET", f"controls/{control['id']}")["state"] == "outcome_unknown"

    # 4. An adapter without the action, and an adapter that raises, are both unverifiable.
    class Inert:
        name = "inert"
        authoritative_absence = False

        def inspect(self, run=None):
            return rl.AdapterObservation(present=True, observed_at=time.time())

    unsupported = world.run(agent, token)
    control = runtime_control(world, lease_token, lease, unsupported["id"])
    boundary = build(unsupported, Inert()).boundary()
    boundary.boundary()
    assert boundary.state["runtime_observations"][control["id"]]["detail"]["state"] == "unsupported_action"
    assert world.call("GET", f"controls/{control['id']}")["state"] == "outcome_unknown"

    class Broken(AckingAdapter):
        def pause(self, run=None):
            raise RuntimeError("adapter exploded")

    broken = world.run(agent, token)
    control = runtime_control(world, lease_token, lease, broken["id"])
    boundary = build(broken, Broken()).boundary()
    boundary.boundary()
    assert boundary.state["runtime_observations"][control["id"]]["detail"]["state"] == "adapter_error"
    assert world.call("GET", f"controls/{control['id']}")["state"] == "outcome_unknown"

    # 5. Verified cancel, then pause and resume, carry the required observed status.
    cancelled = world.run(agent, token)
    control = runtime_control(world, lease_token, lease, cancelled["id"], "cancel")
    boundary = build(cancelled, AckingAdapter()).boundary()
    boundary.boundary()
    assert world.call("GET", f"controls/{control['id']}")["state"] == "applied"
    assert world.call("GET", f"runs/{cancelled['id']}")["status"] == "cancelled"
    assert boundary.state["cancelled"] is True

    resumed = world.run(agent, token)
    world.claim(agent, resumed, world.scope("resume-scope"), token)
    boundary = build(resumed, AckingAdapter()).boundary()
    control = runtime_control(world, lease_token, lease, resumed["id"])
    boundary.boundary()
    assert world.call("GET", f"runs/{resumed['id']}")["status"] == "paused"
    control = runtime_control(world, lease_token, lease, resumed["id"], "resume")
    boundary.boundary()
    assert world.call("GET", f"controls/{control['id']}")["state"] == "applied"
    assert world.call("GET", f"runs/{resumed['id']}")["status"] == "running"
    assert boundary.state["paused"] is False and boundary.state["cancelled"] is False
    assert boundary.state["unresolved"] == []


def test_pending_control_conflict_settles_before_a_new_control_is_accepted(world, tmp_path):
    agent = world.agent("Conflicted", ["pause", "cancel", "resume"])
    token = world.credential(agent)
    lease_token, lease = coordinator_lease(world)
    run = world.run(agent, token)
    first = runtime_control(world, lease_token, lease, run["id"])
    current = world.call("GET", f"runs/{run['id']}")
    duplicate = world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": run["id"], "action": "pause",
        "target_run_revision": current["control_revision"], "coordinator_epoch": lease["coordinator_epoch"],
        "reason": "Duplicate pause"}, lease_token, status=409)
    assert duplicate["error"]["code"] == "control_already_pending"

    reconciler = rl.RuntimeReconciler(LocalClient(world, token), world.project, agent["id"], run["id"],
                                      state_dir=tmp_path / "state", enabled=True, capture_authority=True,
                                      adapter=AckingAdapter())
    reconciler.boundary().boundary()
    assert world.call("GET", f"controls/{first['id']}")["state"] == "applied"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "paused"
    # Once the pending control is settled, a further control is accepted and verified.
    follow = runtime_control(world, lease_token, lease, run["id"], "cancel")
    reconciler.boundary().boundary()
    assert world.call("GET", f"controls/{follow['id']}")["state"] == "applied"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "cancelled"


CHILD_RUNTIME = """
import json, os, sys, time

state_path, stop_path, run_id = sys.argv[1], sys.argv[2], sys.argv[3]
control_path, ack_path = state_path + ".control.json", state_path + ".control-ack.json"
STATUS = {"pause": "paused", "cancel": "cancelled", "resume": "running"}


def write(present, status):
    with open(state_path, "w", encoding="utf-8") as handle:
        json.dump({"run_id": run_id, "present": present, "status": status,
                   "updated_at": time.time()}, handle)


write(True, "running")
deadline = time.time() + 60
while time.time() < deadline and not os.path.exists(stop_path):
    if os.path.exists(control_path):
        try:
            with open(control_path, encoding="utf-8") as handle:
                request = json.load(handle)
        except (OSError, ValueError):
            request = None
        if isinstance(request, dict):
            with open(ack_path, "w", encoding="utf-8") as handle:
                json.dump({"request_id": request["request_id"], "applied": True,
                           "status": STATUS.get(request["action"], "running"),
                           "checkpoint_id": "checkpoint-" + request["request_id"]}, handle)
    time.sleep(0.02)
write(False, "stopped")
"""


def test_acknowledged_file_control_survives_windows_cleanup_sharing_violation(tmp_path, monkeypatch):
    from pathlib import Path
    monkeypatch.setattr(rl, "uid", lambda prefix: "control_test")
    state = tmp_path / "runtime.json"
    adapter = rl.FileAdapter(state)
    adapter.ack_path.write_text(json.dumps({"request_id": "control_test", "applied": True,
        "status": "paused", "checkpoint_id": "checkpoint_test"}), encoding="utf-8")
    original = Path.unlink
    def busy_file(path, *args, **kwargs):
        if path in (adapter.ack_path, adapter.control_path):
            raise PermissionError("Sharing violation")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", busy_file)
    attempt = adapter.pause({"id": "run_test"})
    assert attempt.applied is True and attempt.checkpoint_id == "checkpoint_test"


def test_live_local_file_adapter_with_trusted_child_process(world, tmp_path):
    """Contract/live validation against a trusted local Python child process, not a vendor session."""
    agent = world.agent("Local runtime", ["pause", "cancel", "resume"])
    token = world.credential(agent)
    run = world.run(agent, token)
    state, stop = tmp_path / "runtime.json", tmp_path / "stop"
    script = tmp_path / "runtime_wrapper.py"
    script.write_text(textwrap.dedent(CHILD_RUNTIME), encoding="utf-8")
    process = subprocess.Popen([sys.executable, str(script), str(state), str(stop), run["id"]])
    try:
        deadline = time.monotonic() + 20
        while not state.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert state.exists(), "local runtime wrapper did not publish its state file"
        adapter = rl.FileAdapter(state, timeout=10.0)
        reconciler = rl.RuntimeReconciler(
            LocalClient(world, token), world.project, agent["id"], run["id"], state_dir=tmp_path / "state",
            enabled=True, capture_authority=True, terminal_grant=True, adapter=adapter)
        report = reconciler.reconcile()
        assert report.adapter == "file" and report.decisions[0]["classification"] == "live"
        assert report.decisions[0]["action"] == "none"

        lease_token, lease = coordinator_lease(world)
        control = runtime_control(world, lease_token, lease, run["id"])
        reconciler.boundary().boundary()
        assert world.call("GET", f"controls/{control['id']}")["state"] == "applied"
        assert world.call("GET", f"runs/{run['id']}")["status"] == "paused"

        # Unobserved workspace changes are reported from the same observer run while it is alive.
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / "produced.txt").write_text("runtime output")
        drifted = rl.RuntimeReconciler(
            LocalClient(world, token), world.project, agent["id"], run["id"], state_dir=tmp_path / "state",
            workspace_root=workspace, enabled=True, capture_authority=True, terminal_grant=True,
            adapter=adapter)
        first = drifted.reconcile(scan=True, update_baseline=True)
        assert [item["change"] for item in first.drift] == ["created"]
        assert first.drift_report["attribution"] == "unattributed"
        assert first.submitted == {"flushed": True, "pending": 0, "quarantined": 0}
        (workspace / "second.txt").write_text("more output")
        second = drifted.reconcile(scan=True)
        assert [item["change"] for item in second.drift] == ["created"]
        assert world.call("GET", f"runs/{run['id']}")["files"]["produced.txt"]["attribution"] == "unattributed"

        # The local runtime exits on its own; only then is absence authoritative.
        stop.write_text("stop")
        assert process.wait(timeout=20) == 0
        assert json.loads(state.read_text())["present"] is False
        report = reconciler.reconcile()
        decision = report.decisions[0]
        assert decision["classification"] == "abandoned" and decision["action"] == "ended"
        assert decision["outcome"] == "applied" and decision["reason"] == "runtime_absent"
        assert decision["terminal_status"] == "failed"
        assert world.call("GET", f"runs/{run['id']}")["status"] == "failed"
        assert world.call("GET", f"controls/{control['id']}")["state"] == "applied"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)


def test_service_transport_maps_reconciler_routes(world):
    service, store = world.app.state.service, world.app.state.store
    agent = world.agent("Transport", ["pause", "cancel", "resume"])
    token = world.credential(agent)
    principal = dict(store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                                     (hash_token(token),)).fetchone())
    transport = rl.ServiceTransport(service, store, principal)
    run = transport.request("POST", "runs", {"project_id": world.project, "agent_id": agent["id"]}, key="run")
    assert transport.request("GET", f"runs/{run['id']}")["id"] == run["id"]
    listed = transport.request("GET", f"runs?project_id={world.project}")["runs"]
    assert any(item["id"] == run["id"] for item in listed)
    heartbeat = transport.request("POST", f"runs/{run['id']}/heartbeat",
                                  {"capture_health": {"state": "healthy"}})
    assert heartbeat["last_seen_at"] >= run["last_seen_at"]
    event = Event(event_id=uid("evt"), type="hivemind.context.agent.status_reported", agent_id=agent["id"],
                  run_id=run["id"], project_id=world.project, occurred_at=now(),
                  source={"source_id": uid("source"), "kind": "sdk", "sequence": 0, "attribution": "reported"},
                  data={"text": "transport check"}).model_dump()
    results = transport.request("POST", "events:batch", {"events": [event, {"event_id": "broken"}]})
    assert results[0]["accepted"] is True
    assert results[1] == {"accepted": False, "event_id": "broken", "error": "invalid_request",
                          "retryable": False}

    lease_token, lease = coordinator_lease(world)
    control = runtime_control(world, lease_token, lease, run["id"])
    controls = transport.request("GET", f"agents/{agent['id']}/controls?project_id={world.project}")
    assert [item["id"] for item in controls] == [control["id"]]
    transport.request("POST", f"controls/{control['id']}:ack", {}, key="ack")
    transport.request("POST", f"controls/{control['id']}:result", {"outcome": "outcome_unknown"}, key="result")
    assert world.call("GET", f"controls/{control['id']}")["state"] == "outcome_unknown"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "running"
    with pytest.raises(Problem):
        transport.request("GET", "unknown/route")


def test_install_reconcile_route_is_opt_in_and_administrator_gated(world, tmp_path):
    directory = tmp_path / "second"
    config = json.loads(initialize(directory).read_text())
    app = create_app(directory, workers=False)
    store = app.state.store

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    rl.install(app, app.state.service, principal)
    with TestClient(app) as client:
        def call(method, path, body=None, token=None, status=200):
            response = client.request(method, "/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + (token or config["token"]), "Idempotency-Key": uid("key")})
            assert response.status_code == status, response.text
            return response.json().get("data", response.json())

        agent = call("POST", "agents", {"project_id": config["project_id"], "display_name": "Lifecycle",
                                       "supported_actions": ["pause"], "scope_permit_enforcement": True})
        token = secrets.token_urlsafe(32)
        with store.transaction():
            store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                             (uid("principal"), hash_token(token), "agent", config["project_id"],
                              agent["id"], 0, 1))
        run = call("POST", "runs", {"project_id": config["project_id"], "agent_id": agent["id"]}, token)
        workspace = tmp_path / "workspace"
        (workspace / "src").mkdir(parents=True)
        (workspace / "src" / "app.ts").write_text("export const app = 1;")
        body = {"project_id": config["project_id"], "agent_id": agent["id"], "run_id": run["id"],
                "workspace_root": str(workspace), "scan": True, "enabled": True}
        # Local filesystem reads are administrator-only.
        call("POST", "runtime:reconcile", body, token, status=403)
        report = call("POST", "runtime:reconcile", body)
        assert report["enabled"] is True and report["adapter"] is None
        assert [item["change"] for item in report["drift"]] == ["created"]
        assert report["submitted"] == {"flushed": True, "pending": 0, "quarantined": 0}
        # No adapter is injected by the route, so presence stays unverified: no transition.
        assert report["decisions"][0]["classification"] == "stale"
        assert report["decisions"][0]["action"] == "none"
        events = call("GET", f"events?project_id={config['project_id']}")
        assert any(event["type"].endswith("file.changed") for event in events)
        state = call("GET", f"runtime:state?project_id={config['project_id']}&agent_id={agent['id']}"
                            f"&run_id={run['id']}")
        assert state["last_report"]["enabled"] is True and state["run_id"] == run["id"]

        other = call("POST", "agents", {"project_id": config["project_id"], "display_name": "Other",
                                       "supported_actions": [], "scope_permit_enforcement": False})
        other_run = call("POST", "runs", {"project_id": config["project_id"], "agent_id": other["id"]})
        call("POST", "runtime:reconcile", {**body, "scan": False, "targets": [other_run["id"]]}, token,
             status=403)
        # The transport is directly reusable by the parent without registering routes.
        transport = rl.ServiceTransport(app.state.service, store, dict(
            store.db.execute("SELECT * FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()))
        assert transport.request("GET", f"runs/{run['id']}")["id"] == run["id"]
