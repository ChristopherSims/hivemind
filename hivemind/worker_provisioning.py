"""Project-bound, explicitly enabled worker launches. No API-controlled executable."""
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import Depends, Header
from pydantic import Field

from . import models as m
from .process_boundary import ProcessGroup
from .storage import Problem, canonical, now, uid
from .worker_permissions import WorkerPermissionRequest, WorkerPermissions, read_violation


class WorkerCreate(m.Model):
    profile: m.ID
    display_name: str = Field(min_length=1, max_length=120)
    instructions: str = Field(min_length=1, max_length=65536)
    max_runtime_seconds: int = Field(default=300, ge=1, le=604800)
    scope_ids: list[m.ID] = Field(default_factory=list, max_length=32)
    coordinator_epoch: int | None = Field(default=None, ge=1)
    start: bool = True
    permissions: WorkerPermissionRequest | None = None


class WorkerStart(m.Model):
    expected_revision: int = Field(ge=1)
    coordinator_epoch: int | None = Field(default=None, ge=1)


MODELS = (WorkerCreate, WorkerStart)


def checked_directory(value):
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("Worker directories must be absolute")
    for item in (path, *path.parents):
        info = item.lstat()
        if item.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("Worker directories cannot contain links")
    if not path.is_dir():
        raise ValueError("Worker directory unavailable")
    return path.resolve(strict=True)


@dataclass(frozen=True)
class WorkerProfile:
    name: str
    project_id: str
    runtime: str
    model: str
    repository_root: str
    workspace_root: str
    private_root: str
    policy_sha256: str
    max_runtime_seconds: int = 900
    allow_model_calls: bool = False
    auto_mcp_registration: bool = True
    workspace_mode: str = "empty"
    allow_native_git: bool = False
    repository_ref: str = "HEAD"
    checkout_timeout_seconds: int = 30
    checkout_max_files: int = 10000
    checkout_max_bytes: int = 67108864
    permissions: WorkerPermissions = field(default_factory=WorkerPermissions)
    repository_metadata_roots: tuple = ()
    submodule_sources: tuple = ()
    checkout_max_submodules: int = 32
    checkout_max_submodule_depth: int = 8
    checkout_max_object_bytes: int = 268435456
    materialize_lfs: bool = False
    lfs_object_roots: tuple = ()
    require_os_isolation: bool = False
    vendor_model_origins: tuple = ()

    def __post_init__(self):
        if isinstance(self.permissions, dict):
            object.__setattr__(self, "permissions", WorkerPermissions(**self.permissions))
        if not isinstance(self.permissions, WorkerPermissions):
            raise ValueError("Invalid worker permission profile")
        if any(not isinstance(value, str) or not value for value in (
                self.name, self.project_id, self.model, self.repository_root,
                self.workspace_root, self.private_root, self.policy_sha256)):
            raise ValueError("Worker profile fields require nonempty strings")
        if len(self.name) > 128 or len(self.model) > 256:
            raise ValueError("Worker profile fields exceed limits")
        if self.runtime not in ("factory", "oh_my_pi") or not self.name or not self.model:
            raise ValueError("Invalid worker runtime profile")
        if type(self.allow_model_calls) is not bool:
            raise ValueError("Worker model opt-in requires a boolean")
        if type(self.auto_mcp_registration) is not bool:
            raise ValueError("Worker MCP registration requires a boolean")
        if type(self.require_os_isolation) is not bool:
            raise ValueError("Worker isolation opt-in requires a boolean")
        if not isinstance(self.vendor_model_origins, (list, tuple)):
            raise ValueError("Worker model origins require a bounded list")
        object.__setattr__(self, "vendor_model_origins", tuple(self.vendor_model_origins))
        if self.require_os_isolation:
            if self.auto_mcp_registration:
                raise ValueError("Sandboxed automatic MCP enrollment is not yet validated")
            if not 1 <= len(self.vendor_model_origins) <= 16:
                raise ValueError("Isolated workers require explicit model origins")
            for origin in self.vendor_model_origins:
                if not isinstance(origin, str) or any(char.isspace() or ord(char) < 32 for char in origin):
                    raise ValueError("Invalid model origin")
                parsed = urlsplit(origin)
                if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                        or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
                    raise ValueError("Model grants must be HTTPS origins")
        elif self.vendor_model_origins:
            raise ValueError("Model gateways require strict OS isolation")
        from .worker_repository import validate_repository_options
        validate_repository_options(self)
        if self.workspace_mode not in ("empty", "git_clone") or type(self.allow_native_git) is not bool:
            raise ValueError("Invalid worker repository mode")
        if self.workspace_mode == "git_clone" and not self.allow_native_git:
            raise ValueError("Repository checkout requires the native Git opt-in")
        if (not isinstance(self.repository_ref, str) or not re.fullmatch(
                r"HEAD|[0-9a-f]{40}|[0-9a-f]{64}|refs/heads/[A-Za-z0-9][A-Za-z0-9._/-]{0,200}",
                self.repository_ref) or ".." in self.repository_ref
                or "//" in self.repository_ref or self.repository_ref.endswith(("/", ".", ".lock"))):
            raise ValueError("Invalid trusted repository ref")
        for value, minimum, maximum in (
                (self.checkout_timeout_seconds, 1, 300),
                (self.checkout_max_files, 1, 100000),
                (self.checkout_max_bytes, 1, 1073741824)):
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("Invalid repository checkout bound")
        if type(self.max_runtime_seconds) is not int or not 1 <= self.max_runtime_seconds <= 86400:
            raise ValueError("Invalid profile runtime ceiling")
        if len(self.policy_sha256) != 64 or any(c not in "0123456789abcdef" for c in self.policy_sha256):
            raise ValueError("Repository worker policy requires a SHA-256 pin")
        repository = checked_directory(self.repository_root)
        workspace = checked_directory(self.workspace_root)
        private = checked_directory(self.private_root)
        if self.workspace_mode == "git_clone" and any(
                path.drive.startswith("\\\\") for path in (repository, workspace, private)):
            raise ValueError("Repository checkout requires local filesystem roots")
        if private.is_relative_to(repository) or private.is_relative_to(workspace):
            raise ValueError("Private worker root must be outside model-readable roots")

    def ceiling(self):
        try:
            return self._ceiling()
        except (OSError, ValueError):
            raise Problem("worker_policy_unavailable", 409) from None

    def fingerprint(self):
        return hashlib.sha256(canonical(asdict(self)).encode("utf-8")).hexdigest()

    def _ceiling(self):
        repository = checked_directory(self.repository_root)
        path = repository / "hivemind-worker-policy.json"
        info = path.lstat()
        if (path.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400
                or not path.is_file() or info.st_size > 16384):
            raise Problem("worker_policy_untrusted", 409)
        # Open once, bounded, and verify the exact bytes pinned by the operator.
        with path.open("rb") as stream:
            data = stream.read(16385)
        if hashlib.sha256(data).hexdigest() != self.policy_sha256:
            raise Problem("worker_policy_changed", 409)
        try:
            policy = json.loads(data)
        except (ValueError, UnicodeError):
            raise Problem("worker_policy_invalid", 409) from None
        if (not isinstance(policy, dict) or set(policy) != {"version", "max_runtime_seconds"}
                or type(policy["version"]) is not int or policy["version"] != 1
                or type(policy["max_runtime_seconds"]) is not int
                or not 1 <= policy["max_runtime_seconds"] <= 86400):
            raise Problem("worker_policy_invalid", 409)
        return min(policy["max_runtime_seconds"], self.max_runtime_seconds)


@dataclass(frozen=True)
class ProvisioningPolicy:
    enabled: bool = False
    api_base_url: str = "http://127.0.0.1:8080"
    max_active_workers: int = 4
    profiles: tuple = ()
    launcher: object = field(default=None, repr=False)  # trusted test/operator injection only
    linux_cgroup_root: str | None = None

    def __post_init__(self):
        if type(self.enabled) is not bool:
            raise ValueError("Worker provisioning requires a boolean")
        if self.linux_cgroup_root is not None and (
                not isinstance(self.linux_cgroup_root, str) or not self.linux_cgroup_root):
            raise ValueError("Linux cgroup delegation must be an explicit path")
        if type(self.max_active_workers) is not int or not 1 <= self.max_active_workers <= 32:
            raise ValueError("Invalid worker capacity")
        url = urlsplit(self.api_base_url)
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password:
            raise ValueError("Invalid private worker API URL")
        if url.query or url.fragment or url.path not in ("", "/"):
            raise ValueError("Worker API URL must be an origin")
        if len(self.profiles) > 32 or len({p.name for p in self.profiles}) != len(self.profiles):
            raise ValueError("Worker profiles must be bounded and uniquely named")


class WorkerProvisioner:
    def __init__(self, service, policy):
        self.service, self.store, self.policy = service, service.store, policy
        self.profiles = {profile.name: profile for profile in policy.profiles}
        self.handles = {}
        self.lock = threading.RLock()
        self.closing = False
        # Never attach to a PID or replay a launch after a service restart.
        with self.store.transaction():
            for row in self.store.db.execute(
                    "SELECT * FROM resources WHERE kind='worker_provision' AND deleted=0"):
                body = json.loads(row["body"])
                if body["state"] in ("preparing", "running"):
                    body.update(state="unknown", reason="service_restart")
                    self._revoke(body)
                    self.store.save(row["kind"], row["project"], row["owner"], body,
                                    expected=row["revision"])

    def _authorize(self, principal, project, epoch=None):
        row = self.store.db.execute("SELECT * FROM principals WHERE id=?", (principal["id"],)).fetchone()
        if row is None or any(row[key] != principal[key] for key in
                              ("role", "project", "agent", "token_hash", "coordinator")):
            raise Problem("authentication_required", 401)
        self.service.project_access(principal, project)
        if principal["role"] != "admin":
            if epoch is None:
                raise Problem("coordinator_epoch_required", 403)
            lease = self.service.coordinator(principal, project, epoch)
            run = self.service.run(principal, lease["run_id"])
            if run["status"] not in ("running", "idle"):
                raise Problem("coordinator_not_live", 409)
            pending = self.store.db.execute(
                "SELECT 1 FROM resources WHERE kind='control' AND project=? AND deleted=0"
                " AND json_extract(body,'$.target_run_id')=?"
                " AND json_extract(body,'$.state') IN ('queued','acknowledged','applying')"
                " AND json_extract(body,'$.expires_at')>? LIMIT 1",
                (project, run["id"], time.time())).fetchone()
            if pending:
                raise Problem("coordinator_control_pending", 403)

    def _profile(self, name, project):
        profile = self.profiles.get(name)
        if profile is None or profile.project_id != project:
            raise Problem("worker_profile_unavailable", 404)
        return profile

    def _assignment(self, principal, body):
        task = self.service.reference(principal, body["project_id"], body["task_id"], "task")
        run = self.service.reference(principal, body["project_id"], body["run_id"], "run")
        if (task["status"] != "queued" or task.get("provisioned_agent_id") != body["agent_id"]
                or run["agent_id"] != body["agent_id"] or run.get("task_id") != body["task_id"]
                or run["status"] not in ("running", "idle")):
            raise Problem("worker_assignment_changed", 409)

    def reserve(self, principal, project, request):
        if not self.policy.enabled or self.closing:
            raise Problem("worker_provisioning_disabled", 403)
        self._authorize(principal, project, request.coordinator_epoch)
        profile = self._profile(request.profile, project)
        try:
            permissions = profile.permissions.narrow(request.permissions)
        except ValueError:
            raise Problem("worker_permissions_widened", 403) from None
        effective = min(request.max_runtime_seconds, profile.ceiling())
        if request.start and not profile.allow_model_calls:
            raise Problem("worker_model_calls_disabled", 403)
        if request.start:
            self._require_native_boundary()
            if profile.require_os_isolation and sys.platform != "linux":
                raise Problem("worker_vendor_sandbox_unavailable", 409)
        for scope in request.scope_ids:
            self.service.reference(principal, project, scope, "scope")
            if self.store.raw(scope, "scope")["private_agent"]:
                raise Problem("worker_scope_unavailable", 403)
        active = self.store.db.execute(
            "SELECT COUNT(*) FROM resources WHERE kind='worker_provision' AND deleted=0"
            " AND json_extract(body,'$.state') IN ('prepared','preparing','running','unknown')").fetchone()[0]
        if active >= self.policy.max_active_workers:
            raise Problem("worker_capacity_exhausted", 429)
        agent = self.service.create(principal, "agent", m.AgentCreate(
            project_id=project, display_name=request.display_name, runtime=profile.runtime,
            supported_actions=["pause", "cancel"], scope_permit_enforcement=True).model_dump())
        task = self.service.create(principal, "task", {
            **m.TaskCreate(project_id=project, title=request.display_name,
                           description=request.instructions, scope_ids=request.scope_ids).model_dump(),
            "status": "queued", "provisioned_agent_id": agent["id"]})
        run = self.service.create(principal, "run", {
            **m.RunCreate(project_id=project, agent_id=agent["id"], task_id=task["id"],
                          purpose="task_worker", runtime=profile.runtime).model_dump(),
            "status": "running", "last_seen_at": time.time(), "capture_health": {"state": "unknown"},
            "tools": {}, "files": {}, "summary": None, "source_highwater": {}, "control_revision": 1})
        self.store.relate(project, agent["id"], run["id"], "HAS_RUN")
        self.store.relate(project, run["id"], task["id"], "WORKS_ON")
        for scope in request.scope_ids:
            self.store.relate(project, task["id"], scope, "WORKS_ON")
        return self.service.create(principal, "worker_provision", {
            "project_id": project, "profile": profile.name, "agent_id": agent["id"],
            "profile_fingerprint": profile.fingerprint(),
            "run_id": run["id"], "task_id": task["id"], "state": "prepared",
            "requested_max_runtime_seconds": request.max_runtime_seconds,
            "max_runtime_seconds": effective, "runtime_capped": effective < request.max_runtime_seconds,
            "model": profile.model, "runtime": profile.runtime, "principal_id": None,
            "launch_id": None, "started_at": None, "reason": None,
            "permissions": permissions.as_dict(),
            "monitoring": {"mode": "tool_api_boundaries", "terminate_on_violation": True,
                           "sandbox_verified": False, "incident": None},
            "mcp": {"configured": False, "automatic": profile.auto_mcp_registration,
                    "connection_verified": False,
                    "bootstrap_available": True,
                    "coordination": "managed_worker_sdk"},
            "workspace": {"mode": profile.workspace_mode, "state": "pending",
                          "source_commit": None, "branch": None},
        })

    def view(self, principal, identity):
        body = self.store.get(identity, principal, "worker_provision")
        epoch = None
        if principal["role"] != "admin":
            lease = self.service.lease(principal, body["project_id"])
            epoch = lease["coordinator_epoch"] if lease else None
        if (principal["role"] != "admin" and
                self.service.nexus_delegation.restricted(body["project_id"], principal["agent"])):
            self.service.coordinator(principal, body["project_id"], epoch, "monitor", body["agent_id"])
        else:
            self._authorize(principal, body["project_id"], epoch)
        for kind in ("agent", "run", "task"):
            self.service.reference(principal, body["project_id"], body[kind + "_id"], kind)
        return body

    def _revoke(self, body):
        if body.get("principal_id"):
            self.store.db.execute("DELETE FROM principals WHERE id=?", (body["principal_id"],))

    def start(self, principal, identity, request):
        with self.lock:
            if not self.policy.enabled or self.closing:
                raise Problem("worker_provisioning_disabled", 403)
            with self.store.transaction():
                row = self.store.raw(identity, "worker_provision")
                body = self.store.get(identity, principal, "worker_provision")
                self._authorize(principal, body["project_id"], request.coordinator_epoch)
                if body["revision"] != request.expected_revision:
                    raise Problem("revision_conflict", 412)
                if body["state"] != "prepared":
                    return self.view(principal, identity)  # Receipt, never invocation permission.
                profile = self._profile(body["profile"], body["project_id"])
                if body.get("profile_fingerprint") != profile.fingerprint():
                    raise Problem("worker_profile_changed", 409)
                self._assignment(principal, body)
                if not profile.allow_model_calls:
                    raise Problem("worker_model_calls_disabled", 403)
                self._require_native_boundary()
                if profile.require_os_isolation and sys.platform != "linux":
                    raise Problem("worker_vendor_sandbox_unavailable", 409)
                effective = min(body["max_runtime_seconds"], profile.ceiling())
                body.update(state="preparing", launch_id=uid("launch"), max_runtime_seconds=effective)
                body = self.store.save(row["kind"], row["project"], row["owner"], body,
                                       expected=row["revision"])
            try:
                private = checked_directory(profile.private_root) / identity
                workspace = checked_directory(profile.workspace_root) / identity
                private.mkdir(mode=0o700)
                workspace.mkdir(mode=0o700)
                if profile.workspace_mode == "git_clone":
                    from .worker_repository import RepositoryCheckout
                    checkout = RepositoryCheckout(profile, workspace, private, effective)
                    commit, counts = checkout.resolve()
                    # Freeze the exact resolved commit before any fetch or checkout.
                    with self.store.transaction():
                        self._authorize(principal, body["project_id"], request.coordinator_epoch)
                        self._assignment(principal, body)
                        profile.ceiling()
                        row = self.store.raw(identity, "worker_provision")
                        body["workspace"].update(state="preparing", source_commit=commit, **counts)
                        body = self.store.save(row["kind"], row["project"], row["owner"], body,
                                               expected=row["revision"])
                    branch = checkout.materialize(commit, identity)
                    body["workspace"].update(state="ready", branch=branch,
                                             working_tree_bytes=checkout.working_tree_bytes)
                else:
                    body["workspace"].update(state="ready")
                token, principal_id = secrets.token_urlsafe(48), uid("principal")
                config_path = private / "worker.json"
                config = {
                    "base_url": self.policy.api_base_url, "token": token,
                    "project_id": body["project_id"], "agent_id": body["agent_id"],
                    "run_id": body["run_id"], "max_runtime_seconds": effective,
                    "workspace_source": body["workspace"],
                    "agent_permissions": body["permissions"],
                    "permission_monitor": {
                        "worker_id": identity, "launch_id": body["launch_id"],
                        "nonce": secrets.token_hex(32),
                        "incident_file": str(private / "permission-violation.json")},
                    "runtime_options": {
                        "enabled": True, "allow_model_calls": True, "model": profile.model,
                        "auto_mcp_registration": profile.auto_mcp_registration,
                        **({"require_os_isolation": True,
                            "vendor_network_gateway": {"model_origins": list(profile.vendor_model_origins)}}
                           if profile.require_os_isolation else {}),
                        "workspace": str(workspace), "config": str(config_path),
                        "state_file": str(private / "runtime.json"),
                        "provisioned_task_id": body["task_id"],
                        "timeout_seconds": min(effective, 300)},
                }
                with config_path.open("x", encoding="utf-8") as stream:
                    os.chmod(config_path, 0o600)
                    stream.write(canonical(config))
                    stream.flush()
                    os.fsync(stream.fileno())
                mcp_path = private / "mcp.json"
                with mcp_path.open("x", encoding="utf-8") as stream:
                    os.chmod(mcp_path, 0o600)
                    stream.write(canonical({"mcpServers": {"hivemind": {
                        "command": sys.executable, "args": ["-m", "hivemind.worker_mcp"],
                        "env": {"HIVEMIND_CONFIG": str(config_path)}}}}))
                    stream.flush()
                    os.fsync(stream.fileno())
                body["mcp"]["configured"] = profile.auto_mcp_registration
                with self.store.transaction():
                    self._authorize(principal, body["project_id"], request.coordinator_epoch)
                    self._assignment(principal, body)
                    profile.ceiling()  # Pin rechecked before accepting private credentials.
                    from .api import hash_token
                    self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                        (principal_id, hash_token(token), "agent", body["project_id"],
                         body["agent_id"], 0, 1))
                    row = self.store.raw(identity, "worker_provision")
                    body.update(principal_id=principal_id)
                    body = self.store.save(row["kind"], row["project"], row["owner"], body,
                                           expected=row["revision"])
                if self.policy.launcher:
                    process, group = self.policy.launcher(config_path, workspace, profile.runtime)
                else:
                    process, group = self._spawn(config_path, workspace, profile.runtime,
                                                 linux_cgroup_root=self.policy.linux_cgroup_root)
                with self.store.transaction():
                    self._authorize(principal, body["project_id"], request.coordinator_epoch)
                    self._assignment(principal, body)
                    profile.ceiling()
                    row = self.store.raw(identity, "worker_provision")
                    body.update(state="running", started_at=time.time())
                    body = self.store.save(row["kind"], row["project"], row["owner"], body,
                                           expected=row["revision"])
                handle = {"process": process, "group": group, "stop": threading.Event(),
                          "drained": False, "violation": None,
                          "monitor": config["permission_monitor"]}
                self.handles[identity] = handle
                thread = threading.Thread(target=self._watch, args=(identity, handle, effective),
                                          name="hivemind-provisioned-worker", daemon=True)
                handle["thread"] = thread
                thread.start()
                # Child has not loaded config or started a vendor until this handshake.
                process.stdin.write(b"GO\n")
                process.stdin.flush()
                process.stdin.close()
                return body
            except Exception:
                if "group" in locals():
                    group.close()
                with self.store.transaction():
                    row = self.store.raw(identity, "worker_provision")
                    current = json.loads(row["body"])
                    self._revoke(current)
                    current.update(state="unknown", reason="launch_unconfirmed")
                    self.store.save(row["kind"], row["project"], row["owner"], current,
                                    expected=row["revision"])
                raise Problem("worker_launch_unconfirmed", 409) from None

    def _require_native_boundary(self):
        if self.policy.launcher is not None or os.name == "nt":
            return
        try:
            from .linux_process_boundary import delegated_root
            if self.policy.linux_cgroup_root is None:
                raise PermissionError("No cgroup delegation")
            delegated_root(self.policy.linux_cgroup_root)
        except (OSError, ValueError):
            raise Problem("worker_process_boundary_unavailable", 409) from None

    @staticmethod
    def _spawn(config_path, workspace, runtime, *, linux_cgroup_root=None):
        if os.name != "nt":
            from .linux_process_boundary import delegated_root
            if linux_cgroup_root is None:
                raise PermissionError("Native workers require delegated cgroup containment")
            delegated_root(linux_cgroup_root)
        environment = {key: value for key, value in os.environ.items() if key.upper() in
            {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE",
             "LOCALAPPDATA", "APPDATA", "HF_HOME", "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE"}}
        process = subprocess.Popen([sys.executable, "-I", "-m", "hivemind.worker_bootstrap",
            "--config", str(config_path), "--runtime", runtime], cwd=workspace,
            env=environment, shell=False, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=os.name != "nt")
        try:
            if os.name == "nt":
                return process, ProcessGroup(process)
            from .linux_process_boundary import CgroupProcessGroup
            return process, CgroupProcessGroup(process, linux_cgroup_root)
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
            if process.stdin:
                process.stdin.close()
            raise

    def _watch(self, identity, handle, effective):
        deadline = time.monotonic() + effective
        reason = "process_exited"
        violation = None
        while handle["process"].poll() is None:
            monitor = handle["monitor"]
            try:
                violation = read_violation(Path(monitor["incident_file"]),
                    {key: monitor[key] for key in ("worker_id", "launch_id", "nonce")})
            except OSError:
                violation = "monitor_integrity_failure"
            if handle["violation"] or violation:
                reason = handle["violation"] or violation
                break
            if handle["stop"].wait(min(0.1, max(0, deadline - time.monotonic()))):
                reason = "stop_requested"
                break
            if time.monotonic() >= deadline:
                reason = "runtime_limit"
                break
        # A hook refusal can exit the child before the next supervisor poll.
        if violation is None:
            monitor = handle["monitor"]
            try:
                violation = read_violation(Path(monitor["incident_file"]),
                    {key: monitor[key] for key in ("worker_id", "launch_id", "nonce")})
            except OSError:
                violation = "monitor_integrity_failure"
        violation = handle["violation"] or violation
        if violation:
            reason = violation
        try:
            handle["group"].close()
            handle["process"].wait(timeout=10)
            handle["drained"] = True
        except Exception:
            reason = "process_stop_unconfirmed"
        with self.store.transaction():
            row = self.store.raw(identity, "worker_provision")
            body = json.loads(row["body"])
            self._revoke(body)
            if violation:
                body["monitoring"]["incident"] = {"reason": violation, "recorded_at": now(),
                    "launch_id": body["launch_id"], "termination_confirmed": handle["drained"]}
            body.update(state="stopped" if handle["drained"] else "unknown", reason=reason, stopped_at=now(),
                        process_exit_code=handle["process"].poll(),
                        external_effects_proved=False)
            self.store.save(row["kind"], row["project"], row["owner"], body,
                            expected=row["revision"])
        if handle["drained"]:
            with self.lock:
                if self.handles.get(identity) is handle:
                    self.handles.pop(identity)

    def api_denial(self, principal):
        """Associate a denial only with a current authenticated owned worker."""
        with self.store.transaction():
            rows = self.store.db.execute(
                "SELECT * FROM resources WHERE kind='worker_provision' AND deleted=0 "
                "AND json_extract(body,'$.principal_id')=?",
                (principal["id"],)).fetchall()
            for row in rows:
                body = json.loads(row["body"])
                handle = self.handles.get(body["id"])
                if (handle is not None and body["state"] == "running"
                        and body["agent_id"] == principal["agent"] and body["project_id"] == principal["project"]):
                    handle["violation"] = "api_policy_violation"

    def stop(self, principal, identity, expected_revision, epoch):
        with self.lock, self.store.transaction():
            body = self.view(principal, identity)
            self._authorize(principal, body["project_id"], epoch)
            if body["revision"] != expected_revision:
                raise Problem("revision_conflict", 412)
            handle = self.handles.get(identity)
            if handle is None and body["state"] not in ("prepared", "stopped"):
                raise Problem("worker_ownership_unknown", 409)
            if handle is not None:
                handle["stop"].set()
            elif body["state"] == "prepared":
                row = self.store.raw(identity, "worker_provision")
                body.update(state="stopped", reason="canceled_before_launch")
                body = self.store.save(row["kind"], row["project"], row["owner"], body,
                                       expected=row["revision"])
            return body

    def close(self):
        with self.lock:
            self.closing = True
            handles = list(self.handles.values())
            for handle in handles:
                handle["stop"].set()
        for handle in handles:
            handle["thread"].join(15)
        return not any(handle["thread"].is_alive() or not handle["drained"] for handle in handles)


def install(app, service, principal, command, envelope, *, policy=None):
    from .agent_feature_gates import set_resource_projection
    from .capabilities import replace_routes
    existing = getattr(app.state, "worker_provisioner", None)
    if existing is not None and any(h["thread"].is_alive() for h in existing.handles.values()):
        raise ValueError("Cannot replace provisioning while owned workers are active")
    replace_routes(app, __name__)
    handle = WorkerProvisioner(service, policy or ProvisioningPolicy())
    app.state.worker_provisioner = handle
    set_resource_projection(service, "worker_provision", handle.view)
    if not handle.policy.enabled:
        return

    @app.post("/v1/projects/{project_id}/workers")
    async def create(project_id: str, body: WorkerCreate, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        if not idempotency_key:
            raise Problem("idempotency_key_required", 422)
        handle._authorize(p, project_id, body.coordinator_epoch)
        result = command(p, "workers:create:" + project_id, idempotency_key, body,
                         lambda: handle.reserve(p, project_id, body))
        with service.store.lock:
            current = handle.view(p, result["data"]["id"])
        if body.start and current["state"] == "prepared":
            current = await app.state.run_detached(handle.start, p, current["id"], WorkerStart(
                expected_revision=current["revision"], coordinator_epoch=body.coordinator_epoch))
        return envelope(current)
    create.hivemind_detached_work = True

    @app.get("/v1/workers/{worker_id}")
    async def get(worker_id: str, p=Depends(principal)):
        return envelope(handle.view(p, worker_id))

    @app.get("/v1/projects/{project_id}/workers")
    async def listing(project_id: str, p=Depends(principal)):
        if p["role"] != "admin":
            raise Problem("admin_required", 403)
        handle._authorize(p, project_id)
        items = service.store.list("worker_provision", project_id, p, 201)
        workers = []
        for item in items[:200]:
            snapshot = handle.view(p, item["id"])
            snapshot.setdefault("monitoring", {})["credential_revoked"] = bool(snapshot.get("principal_id")) and not bool(
                service.store.db.execute("SELECT 1 FROM principals WHERE id=? AND token_hash NOT LIKE 'revoked:%'",
                                         (snapshot.get("principal_id"),)).fetchone())
            workers.append(snapshot)
        return envelope({"workers": workers,
                         "has_more": len(items) > 200})

    @app.post("/v1/workers/{worker_id}:start")
    async def start(worker_id: str, body: WorkerStart, p=Depends(principal)):
        return envelope(await app.state.run_detached(handle.start, p, worker_id, body))
    start.hivemind_detached_work = True

    @app.post("/v1/workers/{worker_id}:stop")
    async def stop(worker_id: str, body: WorkerStart, p=Depends(principal)):
        return envelope(await app.state.run_detached(
            handle.stop, p, worker_id, body.expected_revision, body.coordinator_epoch))
    stop.hivemind_detached_work = True

    @app.get("/v1/projects/{project_id}/workers:defaults")
    async def defaults(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        profiles = [{"profile": profile.name, "runtime": profile.runtime,
                     "model": profile.model, "max_runtime_seconds": profile.ceiling(),
                     "workspace_mode": profile.workspace_mode,
                     "auto_mcp_registration": profile.auto_mcp_registration,
                     "permissions": profile.permissions.as_dict(),
                     "allow_model_calls": profile.allow_model_calls}
                    for profile in handle.profiles.values() if profile.project_id == project_id]
        return envelope({"profiles": profiles, "payload": {
            "profile": profiles[0]["profile"] if profiles else "<operator-profile>",
            "display_name": "Task worker", "instructions": "<task instructions>",
            "max_runtime_seconds": 300, "scope_ids": [], "start": False},
            "mcp": {"configured": False, "coordination": "managed_worker_sdk"}})
