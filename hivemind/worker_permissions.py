"""Typed tool-boundary permissions, not a filesystem/network sandbox."""
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from pydantic import Field, model_validator

from . import models as m
from .storage import canonical

TOOLS = frozenset({"read", "grep", "glob", "ls", "create", "write", "edit", "applypatch"})


def prefix(path, parent):
    path, parent = (os.path.normcase(value).replace("\\", "/") for value in (path, parent))
    return parent == "." or path == parent or path.startswith(parent.rstrip("/") + "/")


@dataclass(frozen=True)
class WorkerPermissions:
    allowed_tools: tuple[str, ...] = tuple(sorted(TOOLS))
    read_paths: tuple[str, ...] = (".",)
    write_paths: tuple[str, ...] = (".",)
    allow_coordination_mcp: bool = True

    def __post_init__(self):
        for key in ("allowed_tools", "read_paths", "write_paths"):
            values = getattr(self, key)
            if not isinstance(values, (list, tuple)) or len(values) > 64:
                raise ValueError("Permission lists must be bounded")
            if any(not isinstance(value, str) or not value or len(value) > 4096 for value in values):
                raise ValueError("Invalid permission value")
            if len(set(values)) != len(values):
                raise ValueError("Permission lists must be unique")
            object.__setattr__(self, key, tuple(values))
        if not set(self.allowed_tools) <= TOOLS:
            raise ValueError("Only typed filesystem tools can be permitted")
        for value in (*self.read_paths, *self.write_paths):
            if value != "." and m.safe_path(value) != value:
                raise ValueError("Permission paths must be normalized workspace-relative prefixes")
        if type(self.allow_coordination_mcp) is not bool:
            raise ValueError("MCP permission must be a strict boolean")

    def narrow(self, request):
        if request is None:
            return self
        candidate = WorkerPermissions(**request.model_dump())
        if (not set(candidate.allowed_tools) <= set(self.allowed_tools)
                or candidate.allow_coordination_mcp and not self.allow_coordination_mcp
                or any(not any(prefix(path, base) for base in self.read_paths) for path in candidate.read_paths)
                or any(not any(prefix(path, base) for base in self.write_paths) for path in candidate.write_paths)):
            raise ValueError("Worker permissions cannot widen the operator profile")
        return candidate

    def check(self, config, name, tool_input):
        if name in config.get("allowed_mcp_tools", []):
            if not self.allow_coordination_mcp:
                raise PermissionError("Coordination MCP is not permitted")
            return
        name = name.lower()
        if name not in self.allowed_tools:
            raise PermissionError("Tool is not permitted")
        from .hooks import mutation_paths
        from .sdk import excluded
        if name in ("read", "grep", "glob", "ls"):
            if not isinstance(tool_input, dict):
                raise PermissionError("Unsupported read input")
            paths = [{
                "read": tool_input.get("file_path", tool_input.get("path")),
                "grep": tool_input.get("path", "."),
                "glob": tool_input.get("folder", "."),
                "ls": tool_input.get("directory_path", "."),
            }[name]]
            allowed = self.read_paths
        else:
            paths, allowed = mutation_paths(name, tool_input), self.write_paths
        root = Path(config["workspace_root"]).resolve(strict=True)
        for raw in paths:
            if not isinstance(raw, str) or not raw:
                raise PermissionError("A typed path is required")
            path = Path(raw)
            path = (root / path).resolve() if not path.is_absolute() else path.resolve()
            relative = path.relative_to(root).as_posix()
            if excluded(relative) or not any(prefix(relative, base) for base in allowed):
                raise PermissionError("Path is not permitted")

    def as_dict(self):
        return asdict(self)


class WorkerPermissionRequest(m.Model):
    allowed_tools: list[str] = Field(default_factory=lambda: sorted(TOOLS), max_length=64)
    read_paths: list[str] = Field(default_factory=lambda: ["."], max_length=64)
    write_paths: list[str] = Field(default_factory=lambda: ["."], max_length=64)
    allow_coordination_mcp: bool = True

    @model_validator(mode="after")
    def valid(self):
        WorkerPermissions(**self.model_dump())
        return self


def signal_violation(config):
    """Trusted hook writes a bounded, exclusive marker with no model input/secrets."""
    monitor = config.get("permission_monitor")
    if not monitor:
        return
    root = Path(config["workspace_root"]).resolve(strict=True)
    path = Path(monitor["incident_file"])
    if not path.is_absolute() or path.resolve().is_relative_to(root):
        raise PermissionError("Permission monitor must be private")
    data = canonical({"worker_id": monitor["worker_id"], "launch_id": monitor["launch_id"],
                      "nonce": monitor["nonce"], "reason": "tool_policy_violation"})
    try:
        with path.open("x", encoding="utf-8") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        pass  # Never clear or overwrite an earlier violation.


def read_violation(path, expected):
    """Absent means no observed incident, not evidence of isolation."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if path.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400 or info.st_size > 2048:
        return "monitor_integrity_failure"
    try:
        with path.open("rb") as stream:
            record = json.loads(stream.read(2049))
    except (OSError, ValueError):
        return "monitor_integrity_failure"
    if record != {**expected, "reason": "tool_policy_violation"}:
        return "monitor_integrity_failure"
    return "tool_policy_violation"
