"""Operator-only worker feature lifetimes and dependency-aware shutdown."""
from contextlib import contextmanager
from pathlib import Path
import httpx

from .sdk import Client
from .coordinated_operations import _check_raw_components
from .storage import Problem, canonical
from .worker_capture import (AutomaticControls, CleanupError, WorkerCaptureLifecycle,
                             TrustedSignalCollector, automatic_policy_from_mapping,
                             capture_config_from_mapping)
from .runtime_lifecycle import RuntimeReconciler

_RETAINED = []


class CaptureJournal:
    def __init__(self, journal):
        self.journal = journal
        with journal.lock:
            journal.db.execute("CREATE TABLE IF NOT EXISTS capture_lifecycle_audit("
                               "id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            journal.db.commit()

    def append(self, entry):
        body = canonical(entry)
        if len(body.encode()) > 16384:
            raise ValueError("Capture audit entry exceeds its bound")
        with self.journal.lock:
            self.journal.db.execute("INSERT INTO capture_lifecycle_audit(body) VALUES(?)", (body,))
            self.journal.db.commit()


class WorkerFeatures:
    def __init__(self, client, config, journal, workspace):
        self.client, self.config, self.journal = client, config, journal
        self.capture = None
        self.controls = None
        self.coordinator = None
        self.signals = None
        self.reconciler = None
        self.shutdown_pending = False
        capture = dict(config.get("capture", {}))
        for name in ("workspace_root", "state_dir"):
            if capture.get(name):
                _check_raw_components(Path(capture[name]))
        if capture.get("enabled") is True:
            capture.setdefault("workspace_root", str(workspace))
            if Path(capture["workspace_root"]).resolve() != Path(workspace).resolve():
                raise ValueError("Capture must observe the worker's actual workspace")
            policy = capture_config_from_mapping(capture)
            if Path(policy.state_dir).resolve().is_relative_to(Path(workspace).resolve()):
                raise ValueError("Capture state must live outside the worker workspace")
            self.capture = WorkerCaptureLifecycle(client, config["project_id"], config["agent_id"],
                                                   config["run_id"], policy, journal=CaptureJournal(journal))
        else:
            capture_config_from_mapping(capture)
        controls = config.get("automatic_controls", {})
        if not isinstance(controls, dict) or set(controls) - {"policy", "coordinator", "state_file"}:
            raise ValueError("Invalid automatic control configuration")
        self.control_config = controls
        self.control_policy = automatic_policy_from_mapping(controls.get("policy", {}))
        if (self.control_policy.enabled and self.capture is None
                and (self.control_policy.drift_findings_threshold is not None
                     or self.control_policy.conflict_signals)):
            raise ValueError("CLI drift/conflict policies require the capture signal collector")

    def start(self):
        if self.capture is not None:
            self.capture.start()

    def bind_runtime(self, runtime):
        if not self.control_policy.enabled:
            return None
        values = self.control_config
        coordinator = values.get("coordinator")
        if not isinstance(coordinator, dict) or set(coordinator) != {"token", "agent_id", "run_id"}:
            raise ValueError("Automatic controls require an explicit coordinator identity and run")
        if not all(isinstance(value, str) and value for value in coordinator.values()):
            raise ValueError("Invalid coordinator binding")
        if not isinstance(values.get("state_file"), str) or not values["state_file"]:
            raise ValueError("Automatic controls require a private state_file")
        _check_raw_components(Path(values["state_file"]))
        if Path(values["state_file"]).resolve().is_relative_to(Path(self.journal.workspace).resolve()):
            raise ValueError("Automatic control state must live outside the worker workspace")
        if self.coordinator is None:
            self.coordinator = Client(self.config["base_url"], coordinator["token"])

        def epoch():
            body = {"agent_id": coordinator["agent_id"], "run_id": coordinator["run_id"],
                    "requested_ttl_seconds": 60}
            path = f"projects/{self.config['project_id']}/nexus"
            try:
                lease = self.coordinator.request("POST", path + ":renew", body)
            except (Problem, httpx.HTTPStatusError) as error:
                if isinstance(error, Problem):
                    stale = error.code == "stale_coordinator_epoch"
                else:
                    try:
                        stale = (error.response.status_code == 409 and error.response.json()
                                 .get("error", {}).get("code") == "stale_coordinator_epoch")
                    except ValueError:
                        stale = False
                if not stale:
                    raise
                # Canonical acquire refuses when another coordinator owns the lease.
                lease = self.coordinator.request("POST", path + ":acquire", body)
            if (lease["agent_id"], lease["run_id"]) != (coordinator["agent_id"], coordinator["run_id"]):
                raise ValueError("Coordinator lease binding mismatch")
            return lease["coordinator_epoch"]

        self.controls = AutomaticControls(self.client, self.config["project_id"],
            self.config["agent_id"], self.config["run_id"], values["state_file"],
            adapter=runtime, policy=self.control_policy, control_client=self.coordinator,
            coordinator_epoch=epoch)
        if self.capture is not None and self.signals is None:
            if self.control_policy.drift_findings_threshold is not None:
                self.reconciler = RuntimeReconciler(self.client, self.config["project_id"],
                    self.config["agent_id"], self.capture.state["observer_run_id"],
                    state_dir=Path(self.capture.config.state_dir) / "reconciliation",
                    workspace_root=self.capture.config.workspace_root,
                    scan_limits=self.capture.config.limits, exclude=self.capture.config.exclude,
                    enabled=True)
            self.signals = TrustedSignalCollector(self.capture, self.reconciler)
        return self.controls

    def close(self):
        if self.capture is not None:
            # Join outside the lifecycle mutex: the delivery-accounting callback
            # needs that mutex to finish its last batch.
            watcher = self.capture._watcher
            if watcher is not None:
                try:
                    watcher.stop()
                except (RuntimeError, OSError):
                    self.shutdown_pending = True
                    _RETAINED.append(self)
                    raise CleanupError("Watcher still active; dependent resources retained",
                                       {"shutdown_pending": True, "closed": False,
                                        "errors": ["watcher_stop_pending"]}) from None
            report = self.capture.close()
            self.shutdown_pending = report["shutdown_pending"]
            if self.shutdown_pending:
                _RETAINED.append(self)
                raise CleanupError("Capture shutdown pending; dependent resources retained", report)
        if self.coordinator is not None:
            self.coordinator.close()
        if self.reconciler is not None:
            self.reconciler.close()


@contextmanager
def guarded_resource(resource, features):
    """ExitStack exceptions do not halt unwinding: explicitly retain dependencies."""
    value = resource.__enter__()
    try:
        yield value
    finally:
        owner = features.get("owner")
        if owner is not None and owner.shutdown_pending:
            _RETAINED.append(resource)
        else:
            resource.__exit__(None, None, None)
