"""Opt-in, content-free OTLP/HTTP JSON traces with a bounded durable local queue."""
import contextvars
import json
import math
import re
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import Depends

from .storage import Problem, canonical

OPERATION_DESCRIPTORS = {
    "telemetry.status": {"purpose": "Inspect redacted diagnostic queue health, not agent progress proof.",
                         "authority": "Project administrator", "side_effects": "None",
                         "limits": "No prompts, contents, credentials or exporter URL are returned."},
    "telemetry.export": {"purpose": "Export bounded diagnostic spans using OTLP/HTTP JSON.",
                         "authority": "Project administrator plus explicit operator exporter opt-ins",
                         "side_effects": "Configured collector POST; accepted local spans are removed.",
                         "retry_safety": "At-least-once diagnostics; failed/partial/unknown exports stay queued.",
                         "limits": "Traces only, not metrics/logs; no authoritative task or recovery decisions."},
}
_PARENT = contextvars.ContextVar("hivemind_trace_parent", default=None)
_IDENTITY = re.compile(r"^(?:project|agent|run|task|exec|execution|control|msg|memory|claim)_"
                       r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_ATTRIBUTES = {"project_id", "agent_id", "run_id", "task_id", "execution_id", "control_id", "message_id"}
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,95}$")


@dataclass(frozen=True)
class TelemetryPolicy:
    enabled: bool = False
    directory: str | None = None
    endpoint: str | None = None
    allow_export: bool = False
    allow_remote_export: bool = False
    max_spans: int = 10000
    batch_size: int = 100
    timeout_seconds: float = 5

    def __post_init__(self):
        for key in ("enabled", "allow_export", "allow_remote_export"):
            if type(getattr(self, key)) is not bool:
                raise ValueError("Telemetry opt-ins require booleans")
        if (type(self.max_spans) is not int or not 1 <= self.max_spans <= 100000
                or type(self.batch_size) is not int or not 1 <= self.batch_size <= 1000
                or type(self.timeout_seconds) not in (int, float)
                or not math.isfinite(self.timeout_seconds) or not 0 < self.timeout_seconds <= 30):
            raise ValueError("Invalid telemetry bounds")
        if self.enabled and not self.directory:
            raise ValueError("Enabled telemetry requires a private local directory")
        if self.endpoint:
            url = urlsplit(self.endpoint)
            local = url.hostname in ("127.0.0.1", "localhost", "::1")
            if (len(self.endpoint) > 2048 or url.scheme not in ("http", "https") or not url.hostname
                    or url.username or url.password or url.query or url.fragment
                    or url.path != "/v1/traces"
                    or (not local and (url.scheme != "https" or not self.allow_remote_export))):
                raise ValueError("Collector must be an explicit credential-free /v1/traces URL")
        if self.allow_export and (not self.enabled or not self.endpoint):
            raise ValueError("Export requires enabled telemetry and a configured collector")


class Telemetry:
    def __init__(self, policy=None, *, transport=None):
        self.policy = policy or TelemetryPolicy()
        self.transport = transport
        self.lock, self.export_lock = threading.RLock(), threading.Lock()
        self.db, self.dropped = None, 0
        if self.policy.enabled:
            directory = Path(self.policy.directory)
            if not directory.is_absolute():
                raise ValueError("Telemetry directory must be absolute")
            for path in (directory, *directory.parents):
                if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                    raise ValueError("Telemetry cannot follow linked directories")
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            path = directory / "telemetry.sqlite"
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise ValueError("Telemetry queue cannot be a link")
            self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("CREATE TABLE IF NOT EXISTS spans(id INTEGER PRIMARY KEY,project TEXT,body TEXT)")

    @contextmanager
    def span(self, name, **attributes):
        if not self.policy.enabled:
            yield
            return
        if not _NAME.fullmatch(name):
            raise ValueError("Span names must be static content-free operation labels")
        parent = _PARENT.get()
        trace_id = parent[0] if parent else secrets.token_hex(16)
        span_id = secrets.token_hex(8)
        token = _PARENT.set((trace_id, span_id))
        started, failed = time.time_ns(), False
        try:
            yield
        except BaseException:
            failed = True
            raise
        finally:
            _PARENT.reset(token)
            attrs = [{"key": "hivemind." + key, "value": {"stringValue": value}}
                     for key, value in attributes.items()
                     if key in _ATTRIBUTES and isinstance(value, str) and _IDENTITY.fullmatch(value)]
            value = {"traceId": trace_id, "spanId": span_id, "name": name, "kind": 1,
                     "startTimeUnixNano": str(started), "endTimeUnixNano": str(time.time_ns()),
                     "attributes": attrs, "status": {"code": 2 if failed else 1}}
            if parent:
                value["parentSpanId"] = parent[1]
            # Diagnostics must never replace a real operation's result or exception.
            try:
                with self.lock:
                    if self.db is None:
                        self.dropped += 1
                    elif self.db.execute("SELECT COUNT(*) FROM spans").fetchone()[0] >= self.policy.max_spans:
                        self.dropped += 1
                    else:
                        project = attributes.get("project_id")
                        project = project if isinstance(project, str) and _IDENTITY.fullmatch(project) else None
                        self.db.execute("INSERT INTO spans(project,body) VALUES(?,?)",
                                        (project, canonical(value)))
            except Exception:
                self.dropped += 1

    def status(self, project_id):
        with self.lock:
            queued = self.db.execute("SELECT COUNT(*) FROM spans WHERE project=?", (project_id,)).fetchone()[0] \
                if self.db else 0
        return {"enabled": self.policy.enabled, "export_enabled": self.policy.allow_export,
                "queued": queued, "dropped": self.dropped, "delivery": "at-least-once",
                "content_capture": False}

    def export(self, project_id):
        if not self.policy.allow_export:
            raise PermissionError("Telemetry export requires an explicit operator opt-in")
        if not self.export_lock.acquire(blocking=False):
            raise Problem("telemetry_export_busy", 409)
        try:
            with self.lock:
                rows = self.db.execute("SELECT id,body FROM spans WHERE project=? ORDER BY id LIMIT ?",
                                       (project_id, self.policy.batch_size)).fetchall()
            if not rows:
                return {"exported": 0, "remaining": self.status(project_id)["queued"]}
            payload = {"resourceSpans": [{"resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "hivemind"}}]},
                "scopeSpans": [{"scope": {"name": "hivemind", "version": "0.1.0"},
                                "spans": [json.loads(row[1]) for row in rows]}]}]}
            with httpx.Client(transport=self.transport, timeout=self.policy.timeout_seconds,
                              follow_redirects=False, trust_env=False) as client:
                response = client.post(self.policy.endpoint, json=payload,
                                       headers={"Content-Type": "application/json"})
            if response.status_code != 200:
                raise RuntimeError("Collector did not confirm this batch")
            receipt = response.json()
            if not isinstance(receipt, dict) or receipt.get("partialSuccess"):
                raise RuntimeError("Partial or malformed collector response; batch remains queued")
            with self.lock:
                self.db.executemany("DELETE FROM spans WHERE id=?", [(row[0],) for row in rows])
            return {"exported": len(rows), "remaining": self.status(project_id)["queued"]}
        finally:
            self.export_lock.release()

    def close(self):
        with self.export_lock, self.lock:
            if self.db:
                self.db.close()
                self.db = None


class TracedRuntime:
    """Preserve adapter behavior while adding content-free turn/control spans."""
    def __init__(self, runtime, telemetry):
        self.runtime, self.telemetry = runtime, telemetry

    def __getattr__(self, name):
        value = getattr(self.runtime, name)
        if name not in ("pause", "resume", "cancel") or not callable(value):
            return value

        def control(*args, **kwargs):
            run = args[0] if args and isinstance(args[0], dict) else {}
            with self.telemetry.span("agent.control." + name, project_id=run.get("project_id"),
                                     agent_id=run.get("agent_id"), run_id=run.get("id")):
                return value(*args, **kwargs)
        return control

    def execute(self, task, context, checkpoint):
        with self.telemetry.span("agent.turn", project_id=context.get("project_id"),
                                 agent_id=context.get("agent_id"), run_id=context.get("run_id"),
                                 task_id=task.get("id"), execution_id=context.get("execution_id")):
            return self.runtime.execute(task, context, checkpoint)


def install(app, service, principal, command, envelope, policy=None, transport=None):
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    app.state.telemetry = Telemetry(policy, transport=transport)

    def permitted(p, project_id):
        with service.store.lock:
            service.project_access(p, project_id)
            if p["role"] != "admin":
                raise Problem("administrator_required", 403)

    @app.get("/v1/projects/{project_id}/telemetry")
    async def status(project_id: str, p=Depends(principal)):
        """Inspect content-free diagnostic queue health; diagnostics are not execution evidence."""
        permitted(p, project_id)
        return envelope(app.state.telemetry.status(project_id))

    @app.post("/v1/projects/{project_id}/telemetry:export")
    async def export(project_id: str, p=Depends(principal)):
        """Export to the operator-approved collector; partial/unknown batches remain queued."""
        import asyncio
        permitted(p, project_id)
        try:
            result = await asyncio.to_thread(app.state.telemetry.export, project_id)
        except PermissionError:
            raise Problem("telemetry_export_disabled", 403) from None
        except (httpx.HTTPError, RuntimeError, ValueError):
            raise Problem("telemetry_export_unknown", 503) from None
        return envelope(result)
    export.hivemind_detached_work = True
