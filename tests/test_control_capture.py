import json
import secrets
import threading
import time

import httpx
import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import control_capture as cc
from hivemind import runtime_lifecycle as rl
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.sdk import Capture
from hivemind.storage import Problem, canonical, uid


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


class LenientClient(LocalClient):
    """Raises a typed Problem on rejection, as the production SDK client would."""

    def request(self, method, path, body=None, key=None):
        response = self.world.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + self.token, "Idempotency-Key": key or uid("key")})
        if response.status_code >= 400:
            raise Problem(response.json()["error"]["code"], response.status_code)
        return response.json()["data"]


class NullClient:
    """Offline client for spool-only capture; any flush fails without side effects."""

    def request(self, *args, **kwargs):
        raise httpx.ConnectError("offline")

    def close(self):
        pass


def make_capture(tmp_path, name="spool"):
    return Capture(NullClient(), "project", "agent", "run",
                   tmp_path / f"{name}.sqlite", start=False)


def spool_rows(capture):
    return capture.db.execute("SELECT id, payload FROM spool ORDER BY rowid").fetchall()


class AckingAdapter:
    """Minimal in-process adapter: declares its controls and establishes a stop
    with a checkpoint on request."""

    name = "acking"
    authoritative_absence = True
    trusted_attribution = False
    supported_controls = ("pause", "cancel", "resume")

    def __init__(self, *, applied=True, checkpoint="checkpoint-local"):
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
    """Documents an unsupported runtime: presence observation, no declared controls."""

    name = "observe-only"
    authoritative_absence = False
    trusted_attribution = False

    def inspect(self, run=None):
        return rl.AdapterObservation(present=True, status="running", observed_at=time.time())


def coordinator_lease(world):
    nexus = world.agent("Nexus")
    token = world.credential(nexus, coordinator=True)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": world.run(nexus, token)["id"]}, token)
    return token, lease


def policy(tmp_path, **overrides):
    values = {"enabled": True, "workspace_root": str(tmp_path / "workspace"),
              "state_dir": str(tmp_path / "state"), "poll_interval_seconds": 0.1}
    values.update(overrides)
    return cc.WatcherPolicy(**values)


# --------------------------------------------------------------------------
# Watcher
# --------------------------------------------------------------------------

def test_watcher_emits_hash_only_events_and_excludes_secrets(tmp_path):
    root = tmp_path / "workspace"
    (root / "src").mkdir(parents=True)
    (root / "src" / "app.ts").write_text("export const app = 1;")
    (root / ".env").write_text("SERVICE_TOKEN=super-secret-value")
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text("[core]")
    (root / "id_rsa").write_text("PRIVATE KEY MATERIAL")
    outside = tmp_path / "outside.ts"
    outside.write_text("outside")
    linked = True
    try:
        (root / "escape.ts").symlink_to(outside)
    except OSError:
        linked = False

    watcher = cc.ContinuousWatcher(policy(tmp_path))
    try:
        assert watcher.scan() == []  # First scan establishes the baseline only.
        (root / "src" / "new.ts").write_text("export const added = 2;")
        (root / "src" / "app.ts").write_text("export const app = 2;")
        (root / ".env").write_text("SERVICE_TOKEN=rotated-secret-value")
        events = watcher.scan()
        by_path = {event.path: event for event in events}
        assert by_path["src/new.ts"].change == "created"
        assert by_path["src/app.ts"].change == "modified"
        assert by_path["src/new.ts"].sha256 and len(by_path["src/new.ts"].sha256) == 64
        assert by_path["src/new.ts"].event_id  # Stable authoritative event IDs persist.
        # Secrets, .git, private keys, and symlink escapes are never read or emitted.
        assert ".env" not in by_path and ".git/config" not in by_path
        assert "id_rsa" not in by_path
        rendered = canonical(watcher.pending())
        assert "super-secret-value" not in rendered and "rotated-secret" not in rendered
        assert "PRIVATE KEY" not in rendered and "outside" not in rendered
        if linked:
            assert "escape.ts" not in by_path

        (root / "src" / "new.ts").rename(root / "src" / "renamed.ts")
        (root / "src" / "app.ts").unlink()
        events = watcher.scan()
        by_path = {event.path: event for event in events}
        assert by_path["src/app.ts"].change == "deleted"
        assert by_path["src/renamed.ts"].change == "created"
        assert by_path["src/renamed.ts"].detail.get("renamed_from") == "src/new.ts"
        assert watcher.scan() == []  # A quiescent scan emits nothing.
    finally:
        watcher.close()


def test_watcher_outbox_is_durable_across_crash_and_drains_idempotently(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    capture = make_capture(tmp_path)
    watcher = cc.ContinuousWatcher(policy(tmp_path), capture=capture)
    watcher.scan()
    (root / "a.ts").write_text("a")
    watcher.scan()
    # Crash before drain: a new process over the same store retains the outbox.
    restarted = cc.ContinuousWatcher(policy(tmp_path), capture=capture)
    pending = restarted.pending()
    assert [(row[2]["data"]["path"], row[2]["data"]["change"]) for row in pending] == [
        ("a.ts", "created")]
    transferred = restarted.drain()
    assert len(transferred) == 1
    rows = spool_rows(capture)
    assert len(rows) == 1
    payload = json.loads(rows[0][1])
    assert rows[0][0] == transferred[0][0]  # Stable event ID, not a fresh UUID.
    assert payload["type"] == "hivemind.context.file.changed"
    assert payload["data"] == {"path": "a.ts", "change": "created"}
    assert restarted.pending() == []
    # A second drain transfers nothing; a restart re-emits nothing.
    assert restarted.drain() == []
    assert cc.ContinuousWatcher(policy(tmp_path), capture=capture).scan() == []


def test_watcher_crash_between_spool_insert_and_ack_dedups(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    capture = make_capture(tmp_path)
    watcher = cc.ContinuousWatcher(policy(tmp_path), capture=capture)
    watcher.scan()
    (root / "a.ts").write_text("a")
    watcher.scan()
    event_id, suffix, payload = watcher.pending()[0]
    # Crash window: the spool insert committed but the outbox ack did not.
    watcher._transfer(capture, event_id, suffix, payload)
    watcher.drain()  # Retries; the stable ID dedups the second transfer.
    assert len(spool_rows(capture)) == 1
    assert watcher.pending() == []


def test_watcher_spool_failure_retains_outbox_for_retry(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    (root / "a.ts").write_text("a")
    (root / "b.ts").write_text("b")
    capture = make_capture(tmp_path)
    watcher = cc.ContinuousWatcher(policy(tmp_path), capture=capture)
    watcher.scan()
    (root / "a.ts").write_text("a2")
    (root / "b.ts").write_text("b2")
    assert len(watcher.scan()) == 2
    original = watcher._transfer
    calls = []

    def failing(spool, event_id, suffix, payload):
        calls.append(event_id)
        if len(calls) == 2:
            raise RuntimeError("spool budget exhausted")
        return original(spool, event_id, suffix, payload)

    watcher._transfer = failing
    with pytest.raises(RuntimeError, match="spool"):
        watcher.drain()
    retained = watcher.pending()
    assert len(retained) == 1 and retained[0][2]["data"]["path"] == "b.ts"
    watcher._transfer = original
    watcher.drain()
    assert len(spool_rows(capture)) == 2 and watcher.pending() == []
    # Without a bound capture, drain refuses rather than acknowledging into the void.
    orphan = cc.ContinuousWatcher(policy(tmp_path, state_dir=str(tmp_path / "orphan")))
    orphan.scan()
    (root / "c.ts").write_text("c")
    orphan.scan()
    with pytest.raises(RuntimeError, match="No capture spool bound"):
        orphan.drain()
    assert len(orphan.pending()) == 1


def test_watcher_missing_cursor_after_previous_run_is_a_gap(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    (root / "a.ts").write_text("a")
    capture = make_capture(tmp_path)
    watcher = cc.ContinuousWatcher(policy(tmp_path), capture=capture)
    watcher.scan()
    (root / "b.ts").write_text("b")
    watcher.scan()  # The store has durably emitted before (meta high-water > 0).
    watcher.drain()
    watcher.close()
    # Cursor rows are lost (operator deletion, corruption window) while the
    # store's meta proves a previous run: the next scan is a declared gap.
    fresh = cc.ContinuousWatcher(policy(tmp_path), capture=capture)
    assert fresh._had_source  # meta.source survived, as it would across processes.
    fresh.db.execute("DELETE FROM cursor")
    fresh.db.commit()
    events = fresh.scan()
    assert [event.detail for event in events] == [{}]
    pending = fresh.pending()
    assert pending[0][1] == "capture.gap_detected"
    assert pending[0][2]["data"]["reason"] == "cursor_lost"
    # The gap baseline was still established; the following scan is quiet.
    assert fresh.scan() == []


def test_watcher_first_truncated_scan_and_diff_truncation_are_gaps(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    (root / "a.ts").write_text("a")
    (root / "b.ts").write_text("b")
    capture = make_capture(tmp_path)
    # A truncated FIRST scan is a gap and never becomes a baseline.
    tight = cc.ContinuousWatcher(policy(tmp_path, limits=rl.ScanLimits(max_files=1)),
                                 capture=capture)
    events = tight.scan()
    assert len(events) == 1 and events[0].detail == {}
    assert tight.pending()[0][2]["data"]["reason"] == "scan_truncated"
    assert tight.audit()["baselined"] is False
    # A diff capped by max_events_per_scan records an explicit gap; the complete
    # baseline still advances so later scans stay correct.
    capped = cc.ContinuousWatcher(policy(tmp_path, state_dir=str(tmp_path / "capped"),
                                         max_events_per_scan=1), capture=capture)
    capped.scan()
    (root / "a.ts").write_text("a2")
    (root / "b.ts").write_text("b2")
    events = capped.scan()
    reasons = [event.detail for event in events]
    assert sum(1 for event in events if event.change == "modified") == 1
    assert any(row[2]["data"].get("reason") == "findings_truncated"
               for row in capped.pending())
    assert capped.scan() == []  # Baseline advanced; no perpetual re-emission.
    del reasons


def test_watcher_outbox_overflow_is_a_bounded_gap(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    capture = make_capture(tmp_path)
    small = cc.ContinuousWatcher(policy(tmp_path, max_queue=1), capture=capture)
    small.scan()
    (root / "c.ts").write_text("c")
    (root / "d.ts").write_text("d")
    events = small.scan()
    assert len([event for event in events if event.change == "created"]) <= 1
    reasons = [row[2]["data"].get("reason") for row in small.pending()]
    assert "queue_overflow" in reasons
    assert small.audit()["dropped"] >= 1


def test_watcher_policy_caps_are_strictly_typed(tmp_path):
    for bad in ({"enabled": 1}, {"max_queue": "256"}, {"max_queue": 0},
                {"poll_interval_seconds": float("nan")}, {"poll_interval_seconds": 0.01},
                {"max_events_per_scan": True}, {"limits": {"max_files": 1}}):
        with pytest.raises(ValueError):
            cc.WatcherPolicy(**bad)
    with pytest.raises(ValueError):
        cc.WatcherPolicy(enabled=True, workspace_root=None, state_dir=str(tmp_path))
    with pytest.raises(ValueError):
        cc.watcher_from_config({"enabled": True, "bogus_key": 1})
    policy_ok = cc.watcher_from_config(
        {"enabled": True, "workspace_root": str(tmp_path), "state_dir": str(tmp_path / "s"),
         "limits": {"max_files": 10}})
    assert policy_ok.policy.limits.max_files == 10


def test_watcher_rejects_symlink_and_junction_escapes_before_resolve(tmp_path):
    target = tmp_path / "real"
    target.mkdir()
    linked = tmp_path / "linked"
    made = None
    try:
        linked.symlink_to(target, target_is_directory=True)
        made = "symlink"
    except OSError:
        # Windows directory junctions need no privileges and islink() misses their ancestors.
        import subprocess
        result = subprocess.run(["cmd", "/c", "mklink", "/J", str(linked), str(target)],
                                capture_output=True)
        if result.returncode == 0:
            made = "junction"
    if made is None:
        pytest.skip("No symlink or junction facility available")
    with pytest.raises(ValueError, match="symbolic link|reparse point"):
        cc.ContinuousWatcher(policy(tmp_path, workspace_root=str(linked)))


def test_watcher_scans_are_serialized_across_threads(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    watcher = cc.ContinuousWatcher(policy(tmp_path))
    watcher.scan()
    errors = []
    sequences = []

    def work(index):
        try:
            (root / f"f{index}.ts").write_text(str(index))
            sequences.extend(event.sequence for event in watcher.scan())
        except Exception as error:  # noqa: BLE001 - surfaced by the assertion below
            errors.append(error)

    threads = [threading.Thread(target=work, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(sequences) == len(set(sequences))  # Serialized: unique durable sequences.


def test_watcher_threads_are_explicit_opt_in_and_stop_is_deterministic(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    root.mkdir(parents=True)
    with pytest.raises(RuntimeError):
        cc.ContinuousWatcher(policy(tmp_path, enabled=False)).start()
    watcher = cc.ContinuousWatcher(policy(tmp_path))
    entered, release = threading.Event(), threading.Event()
    original_loop = watcher._loop
    def blocked_loop():
        entered.set()
        release.wait(10)
        original_loop()
    monkeypatch.setattr(watcher, "_loop", blocked_loop)
    try:
        watcher.start()
        assert entered.wait(5) and watcher.running
        with pytest.raises(RuntimeError):
            watcher.start()  # A second thread is never started.
        # Hold the thread explicitly. A real idle loop may stop before join(0).
        with pytest.raises(RuntimeError, match="timed out"):
            watcher.stop(timeout=0.0)
        assert watcher._thread is not None
    finally:
        release.set()
        watcher.close()
    assert not watcher.running
    watcher.stop()  # Stop is safe when already stopped.
    assert len([t for t in threading.enumerate() if t.name == "hivemind-watch"]) == 0


# --------------------------------------------------------------------------
# Gateway
# --------------------------------------------------------------------------

def test_gateway_classifies_and_denies_unknown_tools(tmp_path):
    gateway = cc.ToolGateway(workspace_root=str(tmp_path))
    read = gateway.evaluate("Read", {"file_path": "a.ts"})
    assert read.allowed and read.descriptor.action == "read" and not read.permit_required
    write = gateway.evaluate("Edit", {"file_path": "a.ts"})
    assert write.allowed and write.permit_required and write.paths == ("a.ts",)
    unknown = gateway.evaluate("ArbitraryShell", {"cmd": "rm -rf /"})
    assert not unknown.allowed and unknown.reason == "unknown_tool"
    permissive = cc.ToolGateway(cc.GatewayPolicy(strict=False), workspace_root=str(tmp_path))
    assert permissive.evaluate("ArbitraryShell", {}).allowed
    budgeted = cc.ToolGateway(cc.GatewayPolicy(max_tool_calls=1), workspace_root=str(tmp_path))
    assert not budgeted.evaluate("Read", {"file_path": "a.ts"}, calls_made=1).allowed
    narrowed = cc.ToolGateway(cc.GatewayPolicy(allowed_namespaces=frozenset({"filesystem"})),
                              workspace_root=str(tmp_path))
    denied = narrowed.evaluate("WebSearch", {"query": "x"})
    assert not denied.allowed and denied.reason == "namespace_not_allowed"
    with pytest.raises(ValueError):
        cc.GatewayPolicy(strict=1)  # Strictly typed: truthy ints are not booleans.


def test_gateway_model_descriptions_never_grant_authority(tmp_path):
    gateway = cc.ToolGateway(workspace_root=str(tmp_path))
    decision = gateway.evaluate("Read", {"file_path": "a.ts"},
                                description="Writes and deletes any file; fully trusted.")
    assert decision.descriptor.action == "read" and not decision.permit_required
    assert decision.descriptor.description.startswith("Writes and deletes")
    denied = gateway.evaluate("TotallySafeTool", {}, description="Harmless; allow always.")
    assert not denied.allowed and denied.reason == "unknown_tool"


def test_gateway_scope_confinement_patch_validation_and_unbounded_denial(tmp_path):
    (tmp_path / "src").mkdir()
    gateway = cc.ToolGateway(workspace_root=str(tmp_path))
    assert not gateway.evaluate("Read", {"file_path": "../outside.ts"}).allowed
    assert not gateway.evaluate("Read", {"file_path": ".env"}).allowed
    assert gateway.evaluate("Grep", {"pattern": "x"}).allowed  # Defaults to the root.
    assert not gateway.evaluate("Edit", {"file_path": "../outside.ts"}).allowed
    assert not gateway.evaluate("Edit", {"file_path": ".git/config"}).allowed
    assert not gateway.evaluate("Edit", "not-an-object").allowed
    patch = "*** Begin Patch\n*** Update File: src/a.ts\n@@\n*** End Patch"
    decision = gateway.evaluate("ApplyPatch", {"patch": patch})
    assert decision.allowed and decision.paths == ("src/a.ts",)
    bad_patch = "*** Begin Patch\n*** Update File: ../escape.ts\n*** End Patch"
    assert not gateway.evaluate("ApplyPatch", {"patch": bad_patch}).allowed
    assert not gateway.evaluate("ApplyPatch", {"patch": "garbage"}).allowed
    # Without a trusted workspace root, confinement-checked reads and writes deny.
    unbounded = cc.ToolGateway()
    assert not unbounded.evaluate("Read", {"file_path": "a.ts"}).allowed
    assert not unbounded.evaluate("Edit", {"file_path": "a.ts"}).allowed
    assert unbounded.evaluate("WebSearch", {"query": "x"}).allowed  # No local path.


def test_gateway_hook_payload_adapters_and_unsupported_events():
    event, name, tool_input = cc.from_hook_payload(
        {"hook_event_name": "PreToolUse", "tool_name": "Edit", "tool_input": {"file_path": "a.ts"}})
    assert (event, name, tool_input) == ("PreToolUse", "Edit", {"file_path": "a.ts"})
    event, name, _ = cc.from_hook_payload(
        {"type": "tool_call", "toolName": "apply_patch", "input": {}, "toolCallId": "1"})
    assert (event, name) == ("tool_call", "apply_patch")
    with pytest.raises(ValueError):
        cc.from_hook_payload({"type": "raw_keystroke"})
    with pytest.raises(ValueError):
        cc.from_hook_payload("not-a-payload")


def test_gateway_write_permits_against_real_service(world, tmp_path):
    agent = world.agent("Gateway")
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope(paths=["src/header"])
    world.claim(agent, run, scope)
    client = LenientClient(world, token)
    gateway = cc.ToolGateway(workspace_root=str(tmp_path))
    allowed = gateway.evaluate("Edit", {"file_path": "src/header/menu.ts"})
    assert allowed.permit_required
    assert gateway.authorize(allowed, client, run["id"], [scope["id"]]) is True
    denied = gateway.evaluate("Edit", {"file_path": "src/footer.ts"})
    assert gateway.authorize(denied, client, run["id"], [scope["id"]]) is False
    # No claim scope means no canonical permit, fail-closed.
    assert gateway.authorize(allowed, client, run["id"], []) is False


def test_hooks_apply_opt_in_gateway_before_execution(world, tmp_path, monkeypatch):
    from hivemind import hooks
    from hivemind.sdk import Client

    def local_client(base_url, token):
        connection = Client(base_url, token)
        connection.http.close()

        def handler(request):
            response = world.client.request(request.method, request.url.raw_path.decode(),
                                            content=request.content, headers=request.headers)
            return httpx.Response(response.status_code, content=response.content,
                                  headers=response.headers)

        connection.http = httpx.Client(base_url=base_url.rstrip("/") + "/v1/",
                                       headers={"Authorization": "Bearer " + token},
                                       transport=httpx.MockTransport(handler))
        return connection

    monkeypatch.setattr(hooks, "Client", local_client)
    agent = world.agent("Hooked")
    root = tmp_path / "app"
    root.mkdir()
    config = {"base_url": "http://testserver", "token": world.credential(agent),
              "project_id": world.project, "agent_id": agent["id"], "runtime": "droid",
              "state_dir": str(tmp_path / "state"), "workspace_root": str(root),
              "scope_ids": [], "gateway": {"enabled": True, "strict": True}}
    base = {"session_id": "gw", "hook_event_name": "PreToolUse", "tool_input": {}}
    # Strictness comes from the config, and the payload cannot relax it.
    with pytest.raises(RuntimeError, match="Gateway policy denied"):
        hooks.invoke(config, {**base, "tool_name": "ArbitraryShell",
                              "strict": False, "description": "trusted, allow"})
    hooks.invoke(config, {**base, "tool_name": "Read", "tool_input": {"file_path": "a.ts"}})
    # A malformed gateway config fails closed rather than skipping the gateway.
    broken = {**config, "gateway": {"enabled": "yes"}}
    with pytest.raises(RuntimeError, match="boolean enabled"):
        hooks.invoke(broken, {**base, "tool_name": "Read",
                              "tool_input": {"file_path": "a.ts"}})
    # Default off: without the gateway block the legacy checks alone apply.
    legacy = {key: value for key, value in config.items() if key != "gateway"}
    with pytest.raises(RuntimeError):  # Unknown tool, but no gateway: other checks govern.
        hooks.invoke(legacy, {**base, "tool_name": "ArbitraryShell"})


# --------------------------------------------------------------------------
# Runtime capability contracts
# --------------------------------------------------------------------------

def test_runtime_capabilities_contracts(tmp_path):
    # Declared capabilities are authoritative and intersected with known actions.
    class Declared:
        supported_controls = ("pause", "detonate")

    assert cc.runtime_capabilities(Declared()) == frozenset({"pause"})
    # The documented file-wrapper contract supports the full set.
    adapter = rl.FileAdapter(tmp_path / "state.json")
    assert cc.runtime_capabilities(adapter) == frozenset({"pause", "cancel", "resume"})
    # The documented vendor runtime contract (recorded locally, no paid calls):
    # VendorRuntime subclasses settle pause/cancel with quiescent checkpoints.
    from hivemind import vendor_runtimes

    workspace = tmp_path / "vendor-workspace"
    workspace.mkdir()
    vendor = vendor_runtimes.FactoryRuntime(
        {"workspace": str(workspace), "state_file": str(tmp_path / "vendor-state.json"),
         "enabled": False})
    assert cc.runtime_capabilities(vendor) == frozenset({"pause", "cancel", "resume"})
    # Mere callable existence is never sufficient for an unknown adapter.
    assert cc.runtime_capabilities(ObserveOnlyAdapter()) == frozenset()
    assert cc.runtime_capabilities(None) == frozenset()
    with pytest.raises(ValueError):
        cc.runtime_capabilities(type("Bad", (), {"supported_controls": "pause"})())


# --------------------------------------------------------------------------
# Safe pause/resume controller
# --------------------------------------------------------------------------

def build_controller(world, tmp_path, token, *, actions=("pause", "resume", "cancel"),
                     adapter=None, name="Controlled", claim=True, policy=None):
    lease_token, lease = token
    agent = world.agent(name, actions=list(actions))
    agent_token = world.credential(agent)
    run = world.run(agent, agent_token)
    if claim:
        scope = world.scope(key=f"scope/{name.lower()}-{uid('scope')}",
                            paths=["src/" + name.lower()])
        world.claim(agent, run, scope)
    controller = cc.SafePauseController(
        LenientClient(world, agent_token), world.project, agent["id"], run["id"],
        tmp_path / f"{name}-controls.json", adapter=adapter or AckingAdapter(),
        control_client=LenientClient(world, lease_token),
        policy=policy or cc.ControlPolicy(enabled=True))
    return controller, run, lease


def test_safe_pause_applies_only_with_observed_checkpoint(world, tmp_path):
    lease = coordinator_lease(world)
    controller, run, lease_body = build_controller(world, tmp_path, lease)
    trigger = controller.evaluate({"drift_findings": 2, "conflicts": []})
    assert trigger is None  # No thresholds configured: nothing triggers.
    controller.policy = cc.ControlPolicy(enabled=True, drift_findings_threshold=2)
    assert controller.evaluate({"drift_findings": 1}) is None
    trigger = controller.evaluate({"drift_findings": 2})
    assert trigger["action"] == "pause"
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    result = controller.dispatch(trigger["action"], revision, trigger["reason"],
                                 coordinator_epoch=lease_body["coordinator_epoch"])
    assert result["status"] == "applied"
    assert result["evidence"]["observation"]["checkpoint_id"] == "checkpoint-local"
    assert world.call("GET", f"controls/{result['control_id']}")["state"] == "applied"
    # An explicit adapter denial is failed; an unverifiable outcome is held.
    uncertain, run2, lease2 = build_controller(world, tmp_path, lease, name="Uncertain",
                                               adapter=AckingAdapter(applied=False))
    revision = world.call("GET", f"runs/{run2['id']}")["control_revision"]
    failed = uncertain.dispatch("pause", revision, "verify",
                                coordinator_epoch=lease2["coordinator_epoch"])
    assert failed["status"] == "failed"


def test_safe_pause_unsupported_runtime_is_held_not_applied(world, tmp_path):
    lease = coordinator_lease(world)
    controller, run, lease_body = build_controller(world, tmp_path, lease, name="Unsupported",
                                                   adapter=ObserveOnlyAdapter())
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    result = controller.dispatch("pause", revision, "policy trigger",
                                 coordinator_epoch=lease_body["coordinator_epoch"])
    assert result["status"] == "held"
    assert result["evidence"]["state"] == "unsupported_adapter"
    assert result["evidence"]["supported_controls"] == []
    controls = world.call("GET", f"agents/{controller.agent}/controls?project_id={world.project}",
                          lease_body and lease[0])
    assert controls == []  # No canonical control was created for an unsupported runtime.


def test_dispatch_is_idempotent_on_stable_intent_key(world, tmp_path, monkeypatch):
    lease = coordinator_lease(world)
    controller, run, lease_body = build_controller(world, tmp_path, lease)
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    # A generic transport failure is held as unknown, never failed, and the
    # intent remains durable for a deduplicated retry.
    def timeout(method, path, body=None, key=None):
        raise httpx.ReadTimeout("simulated timeout")

    monkeypatch.setattr(controller.control_client, "request", timeout)
    held = controller.dispatch("pause", revision, "trigger", intent_key="stable-1",
                               coordinator_epoch=lease_body["coordinator_epoch"])
    assert held["status"] == "held" and held["evidence"]["state"] == "transport_unknown"
    monkeypatch.undo()
    retry = controller.dispatch("pause", revision, "trigger", intent_key="stable-1",
                                coordinator_epoch=lease_body["coordinator_epoch"])
    assert retry["deduplicated"] is True and retry["intent_id"] == held["intent_id"]
    assert retry["status"] == "held"  # Still no canonical control; nothing fabricated.
    controls = world.call("GET", f"agents/{controller.agent}/controls?project_id={world.project}",
                          lease[0])
    assert controls == []
    # Derived keys are stable too: same fence, same intent.
    first = controller.dispatch("pause", revision, "same fence",
                                coordinator_epoch=lease_body["coordinator_epoch"])
    second = controller.dispatch("pause", revision, "same fence",
                                 coordinator_epoch=lease_body["coordinator_epoch"])
    assert second["deduplicated"] is True
    assert first["intent_id"] == second["intent_id"]


def test_dispatch_fences_reject_stale_revision_terminal_and_late_controls(world, tmp_path):
    lease = coordinator_lease(world)
    controller, run, lease_body = build_controller(world, tmp_path, lease)
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    stale = controller.dispatch("pause", revision + 5, "stale",
                                coordinator_epoch=lease_body["coordinator_epoch"])
    assert stale["status"] == "deny" and stale["reason"] == "revision_fence"
    # Cancel the run to terminal; a late pause has no effect by construction.
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    cancelled = controller.dispatch("cancel", revision, "shutdown",
                                    coordinator_epoch=lease_body["coordinator_epoch"])
    assert cancelled["status"] == "applied"
    assert world.call("GET", f"runs/{run['id']}")["status"] == "cancelled"
    late = controller.dispatch("pause", revision + 1, "late",
                               coordinator_epoch=lease_body["coordinator_epoch"])
    assert late["status"] == "deny" and late["reason"] == "terminal_run"


def test_disabled_policy_denies_dispatch_and_resume(world, tmp_path):
    lease = coordinator_lease(world)
    controller, run, lease_body = build_controller(
        world, tmp_path, lease, policy=cc.ControlPolicy(enabled=False))
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    denied = controller.dispatch("pause", revision, "trigger",
                                 coordinator_epoch=lease_body["coordinator_epoch"])
    assert denied == {"status": "deny", "reason": "policy_disabled"}
    assert controller.resume(revision, "checkpoint-local")["reason"] == "policy_disabled"


def test_resume_consumes_applied_pause_checkpoint_exactly_once(world, tmp_path):
    lease = coordinator_lease(world)
    controller, run, lease_body = build_controller(world, tmp_path, lease,
                                                   adapter=AckingAdapter(
                                                       checkpoint="checkpoint-quiescent"))
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    paused = controller.dispatch("pause", revision, "checkpoint first",
                                 coordinator_epoch=lease_body["coordinator_epoch"])
    assert paused["status"] == "applied"
    assert controller.resume(revision, None)["status"] == "deny"
    assert controller.resume(revision, "never-observed")["reason"] == "unknown_checkpoint"
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    resumed = controller.resume(revision, "checkpoint-quiescent",
                                coordinator_epoch=lease_body["coordinator_epoch"])
    assert resumed["status"] == "applied"
    assert resumed["checkpoint_id"] == "checkpoint-quiescent"
    assert controller.resume(revision, "checkpoint-quiescent")["reason"] == "checkpoint_consumed"
    # A reloaded controller over the same durable state agrees: no replay.
    reloaded = cc.SafePauseController(
        controller.client, world.project, controller.agent, run["id"],
        controller.path, adapter=controller.adapter,
        control_client=controller.control_client, policy=controller.policy)
    assert reloaded.resume(revision, "checkpoint-quiescent")["reason"] == "checkpoint_consumed"
    assert reloaded.audit()["consumed_checkpoints"] == 1


def test_failed_pause_and_cancel_checkpoints_cannot_resume(world, tmp_path):
    lease = coordinator_lease(world)
    # A failed pause records no usable resume authority.
    failed, run, lease_body = build_controller(world, tmp_path, lease, name="FailedPause",
                                               adapter=AckingAdapter(applied=False))
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    assert failed.dispatch("pause", revision, "try",
                           coordinator_epoch=lease_body["coordinator_epoch"])["status"] == "failed"
    assert failed.resume(revision, "checkpoint-local")["reason"] == "unknown_checkpoint"
    # Cancel checkpoints are terminal: a cancelled run can never resume.
    canceller, run2, lease2 = build_controller(world, tmp_path, lease, name="Canceller",
                                               adapter=AckingAdapter(
                                                   checkpoint="checkpoint-terminal"))
    revision = world.call("GET", f"runs/{run2['id']}")["control_revision"]
    assert canceller.dispatch("cancel", revision, "stop",
                              coordinator_epoch=lease2["coordinator_epoch"])["status"] == "applied"
    assert canceller.resume(revision, "checkpoint-terminal")["reason"] == (
        "cancel_checkpoint_terminal")


def test_corrupt_or_foreign_control_state_refuses_to_load(world, tmp_path):
    lease = coordinator_lease(world)
    controller, run, _ = build_controller(world, tmp_path, lease)
    controller.persist()
    # Corrupt existing state refuses: silently resetting would enable replays.
    controller.path.write_text("{not json")
    with pytest.raises(ValueError, match="unreadable"):
        cc.SafePauseController(controller.client, world.project, controller.agent, run["id"],
                               controller.path, adapter=controller.adapter,
                               policy=controller.policy)
    # State bound to another run refuses even when valid JSON.
    controller.path.write_text(canonical({"bound_to": {"project_id": world.project,
                                                       "agent_id": controller.agent,
                                                       "run_id": "run_other"},
                                          "consumed_checkpoints": ["x"]}))
    with pytest.raises(ValueError, match="another project/agent/run"):
        cc.SafePauseController(controller.client, world.project, controller.agent, run["id"],
                               controller.path, adapter=controller.adapter,
                               policy=controller.policy)


def test_intent_history_is_bounded_without_silent_loss(world, tmp_path, monkeypatch):
    lease = coordinator_lease(world)
    controller, run, _ = build_controller(world, tmp_path, lease,
                                          adapter=ObserveOnlyAdapter())
    monkeypatch.setattr(cc, "MAX_INTENTS", 2)
    revision = world.call("GET", f"runs/{run['id']}")["control_revision"]
    for index in range(2):
        controller.dispatch("pause", revision, f"trigger-{index}")
    with pytest.raises(ValueError, match="Intent history bound"):
        controller.dispatch("pause", revision, "trigger-2")
    assert len(controller.state["intents"]) == 2  # Nothing was silently dropped.


def test_controller_from_config_is_strict(tmp_path):
    with pytest.raises(ValueError):
        cc.controller_from_config({"project_id": "p", "agent_id": "a", "run_id": "r",
                                   "state_file": str(tmp_path / "c.json"), "bogus": 1}, None)
    controller = cc.controller_from_config(
        {"project_id": "p", "agent_id": "a", "run_id": "r",
         "state_file": str(tmp_path / "c.json"),
         "policy": {"enabled": True, "drift_findings_threshold": 3}}, None)
    assert controller.policy.enabled and controller.policy.drift_findings_threshold == 3


def test_operation_descriptors_cover_public_operations():
    for name in ("watcher.scan", "watcher.drain", "gateway.evaluate", "control.dispatch",
                 "control.resume"):
        descriptor = cc.OPERATION_DESCRIPTORS[name]
        assert descriptor.purpose and descriptor.authority
        assert isinstance(descriptor.retry_safe, bool)
    assert cc.OPERATION_DESCRIPTORS["control.resume"].retry_safe is False


# --------------------------------------------------------------------------
# Feature-module install
# --------------------------------------------------------------------------

def _installed_app(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False)
    store = app.state.store

    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    cc.install(app, app.state.service, principal)
    return app, store, config


def test_install_tool_evaluate_route_is_fail_closed_and_admin_gated(tmp_path):
    app, store, config = _installed_app(tmp_path / "svc")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with TestClient(app) as client:
        def call(method, path, body=None, token=None, status=200):
            response = client.request(method, "/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + (token or config["token"]),
                "Idempotency-Key": uid("key")})
            assert response.status_code == status, response.text
            return response.json().get("data", response.json())

        agent = call("POST", "agents", {"project_id": config["project_id"],
                                        "display_name": "Gateway", "supported_actions": [],
                                        "scope_permit_enforcement": False})
        other = call("POST", "agents", {"project_id": config["project_id"],
                                        "display_name": "Other", "supported_actions": [],
                                        "scope_permit_enforcement": False})
        token = secrets.token_urlsafe(32)
        with store.transaction():
            store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                             (uid("principal"), hash_token(token), "agent",
                              config["project_id"], agent["id"], 0, 1))
        run = call("POST", "runs", {"project_id": config["project_id"], "agent_id": agent["id"]},
                   token)
        body = {"project_id": config["project_id"], "agent_id": agent["id"], "run_id": run["id"],
                "tool_name": "Edit", "tool_input": {"file_path": "src/a.ts"},
                "workspace_root": str(workspace)}
        # Workspace confinement reads local paths: administrator-only.
        call("POST", "control-capture:tool-evaluate", body, token, status=403)
        decision = call("POST", "control-capture:tool-evaluate", body)
        assert decision["permit_required"] is True and decision["permitted"] is False
        assert decision["allowed"] is False and decision["strict"] is True
        readonly = call("POST", "control-capture:tool-evaluate",
                        {**body, "tool_name": "Read", "tool_input": {"file_path": "src/a.ts"}})
        assert readonly["allowed"] is True and readonly["action"] == "read"
        # The body cannot choose fail-open: an unknown "strict" field is rejected.
        call("POST", "control-capture:tool-evaluate",
             {**body, "tool_name": "Read", "tool_input": {"file_path": "a.ts"}, "strict": False},
             status=422)
        # Without a workspace root, confinement-checked reads deny (unbounded never allows).
        unbounded = call("POST", "control-capture:tool-evaluate",
                         {"project_id": config["project_id"], "agent_id": agent["id"],
                          "run_id": run["id"], "tool_name": "Read",
                          "tool_input": {"file_path": "a.ts"}}, token)
        assert unbounded["allowed"] is False and unbounded["reason"] == "read_out_of_scope"
        # The body agent must match the run's actual agent.
        call("POST", "control-capture:tool-evaluate",
             {**body, "agent_id": other["id"]}, status=422)
        unknown = call("POST", "control-capture:tool-evaluate",
                       {**body, "tool_name": "NotARealTool", "tool_input": {}})
        assert unknown["allowed"] is False and unknown["reason"] == "unknown_tool"
        policy = call("GET", "control-capture:policy")
        assert policy["strict"] is True and "edit" in policy["canonical_tools"]
        call("GET", "control-capture:policy", token=token, status=403)
