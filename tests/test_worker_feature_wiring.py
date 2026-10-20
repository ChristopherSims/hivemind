from types import SimpleNamespace

import pytest

from hivemind.recovery import Journal
from hivemind.worker_feature_wiring import WorkerFeatures, guarded_resource
from test_worker_capture import AckingAdapter, LenientClient


def test_real_observer_spool_controls_and_journal_lifecycle(world, tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    agent = world.agent("Worker", actions=["pause", "resume", "cancel"])
    token = world.credential(agent)
    run = world.run(agent, token)
    nexus = world.agent("Coordinator")
    coordinator_token = world.credential(nexus, coordinator=True)
    coordinator_run = world.run(nexus, coordinator_token)
    config = {"project_id": world.project, "agent_id": agent["id"], "run_id": run["id"],
        "base_url": "http://fixture", "capture": {"enabled": True, "state_dir": str(tmp_path / "capture")},
        "automatic_controls": {"policy": {"enabled": True, "capture_gap_threshold": 1},
            "state_file": str(tmp_path / "controls.json"),
            "coordinator": {"token": coordinator_token, "agent_id": nexus["id"],
                            "run_id": coordinator_run["id"]}}}
    monkeypatch.setattr("hivemind.worker_feature_wiring.Client",
                        lambda url, credential: LenientClient(world, credential))
    with Journal(tmp_path / "journal.sqlite", workspace=workspace) as journal:
        features = WorkerFeatures(LenientClient(world, token), config, journal, workspace)
        try:
            features.start()
            controls = features.bind_runtime(AckingAdapter())
            assert features.signals is not None
            result = controls.tick({"capture_gaps": 1})
            assert result["receipt"]
            assert controls.hold()["state"] == "pause_applied"
            assert features.capture.audit()["observer_run_id"] != run["id"]
        finally:
            features.close()
        assert not features.shutdown_pending
        assert journal.db.execute("SELECT count(*) FROM capture_lifecycle_audit").fetchone()[0] >= 2


def test_dependency_guard_retains_context_on_pending_cleanup():
    events = []

    class Resource:
        def __enter__(self):
            events.append("entered")
            return self

        def __exit__(self, *args):
            events.append("closed")

    owner = SimpleNamespace(shutdown_pending=False)
    with guarded_resource(Resource(), {"owner": owner}):
        owner.shutdown_pending = True
    assert events == ["entered"]
    owner.shutdown_pending = False
    with guarded_resource(Resource(), {"owner": owner}):
        pass
    assert events[-1] == "closed"


def test_default_off_and_workspace_confinement(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    config = {"project_id": "project", "agent_id": "agent", "run_id": "run"}
    features = WorkerFeatures(None, config, None, workspace)
    features.start()
    assert features.bind_runtime(None) is None
    features.close()
    config["capture"] = {"enabled": True, "state_dir": str(workspace / "state")}
    with pytest.raises(ValueError, match="outside"):
        WorkerFeatures(None, config, None, workspace)


def test_shutdown_joins_without_lifecycle_mutex_and_retains_failure():
    events = []

    class Watcher:
        def stop(self):
            events.append("stop")
            raise RuntimeError("still running")

    features = WorkerFeatures(None, {}, None, ".")
    features.capture = SimpleNamespace(_watcher=Watcher(), close=lambda: events.append("unsafe close"))
    from hivemind.worker_capture import CleanupError
    with pytest.raises(CleanupError):
        features.close()
    assert features.shutdown_pending
    assert events == ["stop"]


def test_cli_refuses_unavailable_drift_source(tmp_path):
    with pytest.raises(ValueError, match="require the capture"):
        WorkerFeatures(None, {"automatic_controls": {
            "policy": {"enabled": True, "drift_findings_threshold": 1}}}, None, tmp_path)
