"""Compose the worker client with a durable operation journal without caching reads."""
import os
from pathlib import Path

from .recovery import UnknownOutcome, derive_operation_id
from .sdk import Client
from .storage import uid


class SessionLock:
    """OS-released single-owner lock, so a crashed session can safely reopen its journal."""

    def __init__(self, path):
        self.path = Path(str(path) + ".lock")
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        try:
            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            stream.close()
            raise RuntimeError("Agent journal already has an active session owner") from None
        self.file = stream
        return self

    def __exit__(self, *args):
        if self.file is not None:
            self.file.close()
            self.file = None


class JournalClient(Client):
    """Use inherited SDK helpers while persisting only canonical command intentions."""

    def __init__(self, client, journal):
        self.client, self.journal = client, journal

    def request(self, method, path, body=None, key=None):
        if (method == "GET" or path in ("context:query", "context:assemble", "search", "graph:query")
                or path.endswith("/heartbeat") or path.endswith("/permits:check")):
            return self.client.request(method, path, body, key=key)
        operation_id = derive_operation_id(method, path, key or uid("operation"))
        # A SQLite receipt does not fence side effects of injected models, checks, or adapters.
        invokes_host = (path.startswith("verifications/") and path.endswith(":check")) or path in (
            "memory-pipeline:process", "memory-pipeline:tick", "runtime:reconcile", "review-automation:tick")
        invokes_host = invokes_host or path.startswith(("git-workspaces", "merge-requests")) or path.endswith(
            "/git:initialize")
        return self.journal.request(self.client, method, path, body, operation_id=operation_id,
                                    safe_to_retry=not invokes_host)

    def close(self):
        self.client.close()

    def resume_commands(self):
        """Explicitly retry persisted retry-safe unknown commands, never runtime effects."""
        results = []
        for operation in self.journal.operations(state="unknown"):
            if not operation["retryable"] or not operation["body_persisted"]:
                continue
            results.append(self.journal.request(self.client, operation["method"], operation["path"],
                operation["body"], operation_id=operation["operation_id"], safe_to_retry=True))
        return results


class WorkerJournal:
    """Persist worker transitions using the recovery journal's public checkpoint API."""

    def __init__(self, journal, project_id, agent_id, files=()):
        self.journal, self.project_id, self.agent_id = journal, project_id, agent_id
        self.files = files

    def append(self, entry):
        runtime_id = derive_operation_id("runtime", entry.get("task_id"),
                                        str(entry.get("execution_id")) + (entry.get("phase_id") or ""))
        if entry.get("kind") == "start":
            self.journal.begin_operation(runtime_id, "RUNTIME", "execute", {
                "task_id": entry["task_id"], "execution_id": entry["execution_id"],
                "phase_id": entry.get("phase_id")}, safe_to_retry=False)
            self.journal.mark_attempt(runtime_id)
        elif entry.get("kind") == "result":
            self.journal.complete_operation(runtime_id, {"status": entry.get("status")})
        self.journal.checkpoint(label="worker-transition", files=self.files, metadata=entry)

    def recover(self, run_id):
        self.journal.recover()
        if self.journal.has_unknown_effects():
            raise UnknownOutcome("Runtime outcome must be reconciled explicitly before resuming")
        checkpoint = self.journal.latest_checkpoint()
        metadata = (checkpoint or {}).get("metadata") or {}
        if metadata.get("project_id") != self.project_id or metadata.get("agent_id") != self.agent_id:
            return None
        if metadata.get("kind") == "result" and metadata.get("status") in ("completed", "failed"):
            return None
        # Cross-run recovery still needs fresh file claims and the server's stale-run fence.
        return metadata if metadata.get("task_id") else None


class HandlerJournal:
    """Stable-ID handler deduplication; a crash during a callback remains unknown."""
    def __init__(self, journal, project_id, agent_id):
        self.journal, self.project_id, self.agent_id = journal, project_id, agent_id

    def handle(self, kind, identifier, callback):
        operation_id = derive_operation_id("handler", kind, f"{self.project_id}:{self.agent_id}:{identifier}")
        lock = self.journal.path.with_name(self.journal.path.name + "." + operation_id + ".handler")
        with SessionLock(lock):
            record = self.journal.begin_operation(operation_id, "HANDLER", kind, {
                "project_id": self.project_id, "agent_id": self.agent_id, "id": identifier}, safe_to_retry=False)
            if record["state"] == "completed":
                return {"deduplicated": True, "result": record["result"]}
            if record["state"] == "unknown" or record["attempting"]:
                raise UnknownOutcome("Handler effects need explicit reconciliation")
            self.journal.mark_attempt(operation_id)
            try:
                value = callback()
            except Exception:
                self.journal.fail_operation(operation_id, RuntimeError("handler_outcome_unknown"))
                raise UnknownOutcome("Handler effects need explicit reconciliation") from None
            completed = self.journal.complete_operation(operation_id, {"handled": True, "value": value})
            return {"handled": True, "result": completed["result"]}
