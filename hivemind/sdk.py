import json
import os
import re
import sqlite3
import threading
from pathlib import Path

import httpx

from .models import Event
from .storage import canonical, now, uid


def redact(text):
    text = re.sub(r"(?i)(bearer\s+)[\w.\-]+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)((?:api[_-]?key|password|secret|token)\s*[=:]\s*)[^\s,;]+", r"\1[REDACTED]", text)
    text = re.sub(r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
                  "[REDACTED PRIVATE KEY]", text, flags=re.S)
    return text


def excluded(path):
    parts = Path(path).parts
    return any(p.startswith(".env") or p in (".ssh", ".git", "node_modules", ".hivemind-data",
                                             ".venv", "dist") for p in parts)


def redact_value(value):
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return value


class Client:
    def create_nexus_grant(self, request, key=None):
        return self.request("POST", "nexus-grants", request, key=key)

    def nexus_grants(self, project_id):
        from urllib.parse import quote
        return self.request("GET", f"projects/{quote(project_id, safe='')}/nexus-grants")

    def revoke_nexus_grant(self, grant_id, expected_revision, key=None):
        from urllib.parse import quote
        return self.request("POST", f"nexus-grants/{quote(grant_id, safe='')}:revoke",
                            {"expected_revision": expected_revision}, key=key)

    def mirror_memory(self, memory_id, request, key=None):
        from urllib.parse import quote
        return self.request("POST", f"memories/{quote(memory_id, safe='')}:mirror", request, key=key)

    def create_worker(self, project_id, request, key=None):
        """Provision through server-side profile and repository-policy checks."""
        from urllib.parse import quote
        return self.request("POST", f"projects/{quote(project_id, safe='')}/workers", request, key=key)

    def create_team(self, request, key=None):
        return self.request("POST", "teams", request, key=key)

    def retrieve_provider_memories(self, project_id, request, key=None):
        from urllib.parse import quote
        return self.request("POST", f"projects/{quote(project_id, safe='')}/memory-providers:retrieve",
                            request, key=key)

    def provider_memory_result(self, result_id):
        from urllib.parse import quote
        return self.request("GET", f"memory-provider-results/{quote(result_id, safe='')}")

    def import_provider_memory(self, project_id, request, key=None):
        from urllib.parse import quote
        return self.request("POST", f"projects/{quote(project_id, safe='')}/memory-providers:import",
                            request, key=key)

    def reconcile_provider_memories(self, project_id, adapter):
        from urllib.parse import quote
        return self.request("POST", f"projects/{quote(project_id, safe='')}/memory-providers:reconcile",
                            {"adapter": adapter})

    def provider_memory_status(self, project_id):
        from urllib.parse import quote
        return self.request("GET", f"projects/{quote(project_id, safe='')}/memory-providers:status")

    def teams(self, project_id):
        from urllib.parse import quote
        return self.request("GET", f"projects/{quote(project_id, safe='')}/teams")

    def update_team(self, team_id, request, key=None):
        from urllib.parse import quote
        return self.request("PATCH", f"teams/{quote(team_id, safe='')}", request, key=key)

    def create_board(self, request, key=None):
        return self.request("POST", "boards", request, key=key)

    def resource_access(self, resource_id):
        from urllib.parse import quote
        return self.request("GET", f"resources/{quote(resource_id, safe='')}/access")

    def set_resource_access(self, resource_id, request, key=None):
        from urllib.parse import quote
        return self.request("PUT", f"resources/{quote(resource_id, safe='')}/access", request, key=key)

    def worker_defaults(self, project_id):
        from urllib.parse import quote
        return self.request("GET", f"projects/{quote(project_id, safe='')}/workers:defaults")

    def worker_status(self, worker_id):
        from urllib.parse import quote
        return self.request("GET", f"workers/{quote(worker_id, safe='')}")

    def start_worker(self, worker_id, request):
        from urllib.parse import quote
        return self.request("POST", f"workers/{quote(worker_id, safe='')}:start", request)

    def stop_worker(self, worker_id, request):
        from urllib.parse import quote
        return self.request("POST", f"workers/{quote(worker_id, safe='')}:stop", request)

    def blocked_task_status(self, project_id, task_id, *, run_id=None, agent_id=None):
        """Current readiness evidence, never execution permission."""
        from urllib.parse import quote, urlencode
        query = {name: value for name, value in
                 (("run_id", run_id), ("agent_id", agent_id)) if value is not None}
        path = (f"projects/{quote(project_id, safe='')}/tasks/"
                f"{quote(task_id, safe='')}/blocked-status")
        return self.request("GET", path + ("?" + urlencode(query) if query else ""))

    def request_task_resolution(self, project_id, request, key=None):
        from urllib.parse import quote
        return self.request("POST", f"projects/{quote(project_id, safe='')}/blocked-tasks:request",
                            request, key=key)

    def task_resolution_requests(self, project_id, *, task_id=None):
        from urllib.parse import quote, urlencode
        path = f"projects/{quote(project_id, safe='')}/blocked-task-requests"
        return self.request("GET", path + ("?" + urlencode({"task_id": task_id}) if task_id else ""))

    def task_resolution_request(self, request_id):
        from urllib.parse import quote
        return self.request("GET", f"blocked-task-requests/{quote(request_id, safe='')}")

    def acknowledge_task_resolution(self, request_id, request, key=None):
        from urllib.parse import quote
        return self.request("POST", f"blocked-task-requests/{quote(request_id, safe='')}:acknowledge",
                            request, key=key)

    def close_task_resolution(self, request_id, request, key=None):
        from urllib.parse import quote
        return self.request("POST", f"blocked-task-requests/{quote(request_id, safe='')}:resolve",
                            request, key=key)

    def trigger_memory_events(self, project_id, *, binding_key=None, key=None):
        return self.request("POST", "memory-events:trigger", {
            "project_id": project_id, "binding_key": binding_key}, key=key)

    def memory_event_status(self, project_id):
        return self.request("GET", f"memory-events:status?project_id={project_id}")

    def memory_event_reviews(self, project_id):
        return self.request("GET", f"memory-events:reviews?project_id={project_id}")

    def release_memory_event_hold(self, project_id, *, fingerprint=None, key=None):
        """Skip held work, never authorize uncertain computation replay."""
        return self.request("POST", "memory-events:release", {
            "project_id": project_id, "fingerprint": fingerprint}, key=key)

    def subscribe_agent_activity(self, project_id, *, cursor_path=None, read_timeout=10.0):
        """Cancellable authorized activity frames, with optional durable cursor."""
        from .activity_stream import ActivitySubscription
        return ActivitySubscription(self.http, project_id=project_id, path_prefix="",
                                    cursor_path=cursor_path, read_timeout=read_timeout)

    def __init__(self, base_url, token, *, transport=None, telemetry=None, trace_identity=None):
        self.http = httpx.Client(base_url=base_url.rstrip("/") + "/v1/",
                                 headers={"Authorization": f"Bearer {token}"}, timeout=15, transport=transport)
        self.telemetry = telemetry
        self.trace_identity = dict(trace_identity or {})

    def memory_pipeline(self, action, request=None, key=None):
        if action not in ("enqueue", "process", "tick", "freshness", "retrieve"):
            raise ValueError("Unsupported memory pipeline action")
        return self.request("POST", f"memory-pipeline:{action}", request, key=key)

    def operation_context(self, operation_id=None):
        """Get built-in operation purpose/schema/authority/retry context, never a grant."""
        from urllib.parse import urlencode
        query = "?" + urlencode({"operation_id": operation_id}) if operation_id is not None else ""
        return self.request("GET", "tool-context" + query)

    def command_receipt(self, operation, key):
        """Prove own committed canonical command and current visibility, not model/file effects."""
        from urllib.parse import urlencode
        return self.request("GET", "commands/receipt?" + urlencode({"operation": operation, "key": key}))
    def search_page(self, request, cursor=None):
        """Authorized bounded retrieval; cursor is opaque and bound to this principal/query."""
        body = dict(request)
        if cursor is not None:
            body["cursor"] = cursor
        return self.request("POST", "retrieval:search-page", body)

    def reconcile_runtime(self, request, key=None):
        return self.request("POST", "runtime:reconcile", request, key=key)

    def request(self, method, path, body=None, key=None):
        if self.telemetry is not None:
            with self.telemetry.span("agent.query" if method == "GET" else "agent.command",
                                     **self.trace_identity):
                return self._request(method, path, body, key)
        return self._request(method, path, body, key)

    def _request(self, method, path, body=None, key=None):
        headers = {"Idempotency-Key": key or uid("cmd")} if method != "GET" else {}
        # A colon in the first segment otherwise becomes a URL scheme:
        # httpx resolves "events:batch" as "batch", losing the command name.
        response = self.http.request(method, "./" + path.lstrip("/"), json=body, headers=headers)
        response.raise_for_status()
        return response.json()["data"]

    def close(self):
        self.http.close()

    def message_inbox(self, agent_id, project_id):
        return self.request("GET", f"agents/{agent_id}/inbox?project_id={project_id}")

    def board_posts(self, board_id, query="", before=None, limit=100, agent_id=None):
        from urllib.parse import urlencode
        params = {"query": query, "limit": limit}
        if before is not None:
            params["before"] = before
        if agent_id is not None:
            params["agent_id"] = agent_id
        return self.request("GET", f"boards/{board_id}/posts?{urlencode(params)}")

    def conversations(self, project_id, query="", before=None, limit=100, agent_id=None):
        from urllib.parse import urlencode
        params = {"query": query, "limit": limit}
        if before is not None:
            params["before"] = before
        path = f"agents/{agent_id}/conversation" if agent_id else f"projects/{project_id}/conversations"
        return self.request("GET", f"{path}?{urlencode(params)}")

    def claim_message(self, message_id, run_id, key=None):
        return self.request("POST", f"messages/{message_id}:claim", {"run_id": run_id}, key=key)

    def ack_message(self, message_id, run_id, delivery_id, key=None):
        return self.request("POST", f"messages/{message_id}:ack",
                            {"run_id": run_id, "delivery_id": delivery_id}, key=key)

    def fail_message(self, message_id, run_id, delivery_id, reason="handler_failed", key=None):
        return self.request("POST", f"messages/{message_id}:fail",
                            {"run_id": run_id, "delivery_id": delivery_id, "reason": reason}, key=key)

    def delivery_summary(self, project_id):
        return self.request("GET", f"projects/{project_id}/delivery-summary")

    def mark_conversations_read(self, project_id, message_ids, key=None):
        return self.request("POST", f"projects/{project_id}/conversations:read",
                            {"message_ids": message_ids}, key=key)

    def requeue_message(self, message_id, expected_revision, key=None):
        return self.request("POST", f"messages/{message_id}:requeue",
                            {"expected_revision": expected_revision}, key=key)

    def deliver_inbox(self, agent_id, project_id, run_id, handler, should_stop=None):
        """One poll; retries occur on later polls. Handlers must deduplicate stable message IDs."""
        from .recovery import UnknownOutcome
        results = []
        for message in self.message_inbox(agent_id, project_id):
            if should_stop and should_stop():
                break
            try:
                receipt = self.claim_message(message["id"], run_id)
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 409:
                    continue
                raise
            try:
                handler(receipt)
            except UnknownOutcome:
                raise  # Do not ack/fail a receipt while its actual side effects are uncertain.
            except Exception:
                results.append(self.fail_message(message["id"], run_id, receipt["delivery_id"]))
            else:
                results.append(self.ack_message(message["id"], run_id, receipt["delivery_id"]))
        return results

    def memory_versions(self, memory_id):
        return self.request("GET", f"memories/{memory_id}/versions")

    def extract_memories(self, request, key=None):
        return self.request("POST", "memories:extract", request, key=key)
    def task(self, task_id):
        return self.request("GET", f"tasks/{task_id}")

    def assemble_context(self, request):
        return self.request("POST", "context:assemble", request)

    def wait_unblocked(self, project_id, task_id, *, run_id=None, agent_id=None,
                       timeout_seconds=30, poll_interval_seconds=2):
        """Bounded block-on-upstream (R6): poll readiness until startable/deadline."""
        from urllib.parse import urlencode
        query = {name: value for name, value in
                 (("run_id", run_id), ("agent_id", agent_id),
                  ("timeout_seconds", timeout_seconds),
                  ("poll_interval_seconds", poll_interval_seconds)) if value is not None}
        path = (f"projects/{project_id}/tasks/{task_id}/wait-unblocked?"
                + urlencode(query))
        return self.request("GET", path)

    def create_task_plan(self, request, key=None):
        return self.request("POST", "task-plans", request, key=key)

    def initialize_git_repository(self, project_id, key=None):
        return self.request("POST", f"projects/{project_id}/git:initialize", {}, key=key)

    def git_repository(self, project_id):
        return self.request("GET", f"projects/{project_id}/git")

    def create_git_workspace(self, project_id, agent_id, key=None):
        return self.request("POST", "git-workspaces", {"project_id": project_id, "agent_id": agent_id}, key=key)

    def git_workspace_action(self, workspace_id, action, request, key=None):
        if action not in ("commit", "push", "reconcile", "fetch"):
            raise ValueError("Unsupported Git workspace action")
        return self.request("POST", f"git-workspaces/{workspace_id}:{action}", request, key=key)

    def submit_merge_request(self, request, key=None):
        return self.request("POST", "merge-requests", request, key=key)

    def merge_request_action(self, merge_request_id, action, request, key=None):
        if action not in ("review", "check", "merge", "reconcile"):
            raise ValueError("Unsupported merge request action")
        return self.request("POST", f"merge-requests/{merge_request_id}:{action}", request, key=key)

    def task_plan(self, plan_id):
        return self.request("GET", f"task-plans/{plan_id}")

    def create_goal(self, request, key=None):
        return self.request("POST", "goals", request, key=key)

    def goal(self, goal_id):
        return self.request("GET", f"goals/{goal_id}")

    def revise_goal(self, goal_id, request, key=None):
        return self.request("POST", f"goals/{goal_id}:revise", request, key=key)

    def tick_goal(self, goal_id, request=None, key=None):
        return self.request("POST", f"goals/{goal_id}:tick", request or {}, key=key)

    def propose_goal_plan(self, goal_id, request=None, key=None):
        return self.request("POST", f"goals/{goal_id}:propose", request or {}, key=key)

    def handoff_task(self, task_id, request, key=None):
        return self.request("POST", f"tasks/{task_id}:handoff", request, key=key)

    def offer_structured_handoff(self, request, key=None):
        return self.request("POST", "structured-handoffs", request, key=key)

    def structured_handoff(self, handoff_id):
        return self.request("GET", f"structured-handoffs/{handoff_id}")

    def decide_structured_handoff(self, handoff_id, action, request, key=None):
        if action not in ("accept", "reject", "revoke"):
            raise ValueError("Unsupported handoff decision")
        return self.request("POST", f"structured-handoffs/{handoff_id}:{action}", request, key=key)

    def consume_handoff_context(self, request, key=None):
        return self.request("POST", "structured-handoffs:consume", request, key=key)

    def set_routing_declaration(self, agent_id, request, key=None):
        return self.request("POST", f"agents/{agent_id}/routing-declaration", request, key=key)

    def query_routing(self, project_id, request):
        return self.request("POST", f"projects/{project_id}/routing:query", request)

    def approve_routing(self, project_id, request, key=None):
        return self.request("POST", f"projects/{project_id}/routing:dispatch", request, key=key)

    def create_artifact_contract(self, request, key=None):
        return self.request("POST", "artifact-contracts", request, key=key)

    def artifact_contract(self, contract_id):
        return self.request("GET", f"artifact-contracts/{contract_id}")

    def artifact_contract_action(self, contract_id, action, request, key=None):
        if action not in ("revise", "revoke", "submit"):
            raise ValueError("Unsupported artifact contract action")
        return self.request("POST", f"artifact-contracts/{contract_id}:{action}", request, key=key)

    def set_task_budget(self, project_id, request, key=None):
        return self.request("POST", f"projects/{project_id}/budgets", request, key=key)

    def task_budget(self, project_id, task_id):
        return self.request("GET", f"projects/{project_id}/budgets/{task_id}")

    def task_budget_action(self, project_id, action, request, key=None):
        if action not in ("reserve", "settle", "checkpoint"):
            raise ValueError("Unsupported task budget action")
        return self.request("POST", f"projects/{project_id}/budgets:{action}", request, key=key)

    def review_automation_tick(self, project_id, limit=20, key=None):
        return self.request("POST", "review-automation:tick", {"project_id": project_id, "limit": limit}, key=key)

    def begin_correction(self, verification_id, request, key=None):
        return self.request("POST", f"verifications/{verification_id}:begin-correction", request, key=key)

    def dispatch_task(self, project_id, agent_id, run_id, key=None):
        return self.request("POST", f"projects/{project_id}/tasks:dispatch",
                            {"agent_id": agent_id, "run_id": run_id}, key=key)

    def report_dispatch_outcome(self, task_id, request, key=None):
        return self.request("POST", f"tasks/{task_id}:dispatch-outcome", request, key=key)

    def set_capabilities(self, agent_id, capabilities, key=None):
        return self.request("POST", f"agents/{agent_id}/capabilities",
                            {"capabilities": capabilities}, key=key)

    def renew_message_lease(self, message_id, run_id, delivery_id, ttl_seconds=30, key=None):
        return self.request("POST", f"messages/{message_id}:renew",
                            {"run_id": run_id, "delivery_id": delivery_id,
                             "ttl_seconds": ttl_seconds}, key=key)

    def submit_verification(self, request, key=None):
        return self.request("POST", "verifications", request, key=key)

    def verification(self, verification_id):
        return self.request("GET", f"verifications/{verification_id}")

    def update_verification(self, verification_id, action, request, key=None):
        if action not in ("update", "assign-reviewer", "check", "decide"):
            raise ValueError("Unsupported verification action")
        return self.request("POST", f"verifications/{verification_id}:{action}", request, key=key)

    def start_task(self, task_id, run_id, expected_revision, recover=False, key=None):
        return self.request("POST", f"tasks/{task_id}:start", {
            "run_id": run_id, "expected_revision": expected_revision, "recover": recover}, key=key)

    def checkpoint_task(self, task_id, checkpoint, key=None):
        return self.request("POST", f"tasks/{task_id}:checkpoint", checkpoint, key=key)

    def notifications(self, agent_id, project_id):
        return self.request("GET", f"agents/{agent_id}/notifications?project_id={project_id}")

    def ack_notification(self, notification_id, key=None):
        return self.request("POST", f"notifications/{notification_id}:ack", {}, key=key)

    def memory_proposal(self, proposal_id):
        return self.request("GET", f"memory-proposals/{proposal_id}")

    def memory_dispute(self, dispute_id):
        return self.request("GET", f"memory-disputes/{dispute_id}")

    def propose_memory_version(self, memory_id, proposal, key=None):
        return self.request("POST", f"memories/{memory_id}/versions", proposal, key=key)

    def review_memory(self, memory_id, review, key=None):
        return self.request("POST", f"memories/{memory_id}:review", review, key=key)

    def review_memory_proposal(self, proposal_id, review, key=None):
        return self.request("POST", f"memory-proposals/{proposal_id}:review", review, key=key)

    def report_memory_dispute(self, dispute, key=None):
        return self.request("POST", "memory-disputes", dispute, key=key)

    def review_memory_dispute(self, dispute_id, review, key=None):
        return self.request("POST", f"memory-disputes/{dispute_id}:review", review, key=key)


class MemoryAutomation:
    """Opt-in extractive proposals; no model execution, self-verification, or background scanner."""

    def __init__(self, client, project_id, agent_id, run_id=None, enabled=False):
        self.client, self.project_id, self.agent_id = client, project_id, agent_id
        self.run_id, self.enabled = run_id, enabled

    def observe_sources(self, source_refs, key=None):
        if not self.enabled:
            return {"memories": [], "skipped": [], "enabled": False}
        return self.client.extract_memories({
            "project_id": self.project_id, "agent_id": self.agent_id,
            "requester_run_id": self.run_id, "source_refs": source_refs,
        }, key=key)

    def review_updates(self, handler=None):
        """Explicit poll; acknowledge only after an optional handler returns successfully."""
        notices = [item for item in self.client.notifications(self.agent_id, self.project_id)
                   if item["type"] == "memory_review"]
        if handler is not None:
            for notice in notices:
                handler(notice)
                self.client.ack_notification(notice["id"], key="memory-review-" + notice["id"])
        return notices


class Capture:
    """Persistent, redacted, at-least-once spool; ordinary observations flush every five seconds."""

    def __init__(self, client, project_id, agent_id, run_id, spool, flush_interval=5, start=True):
        if not 0.1 <= flush_interval <= 60:
            raise ValueError("Flush interval must be between 0.1 and 60 seconds")
        self.client, self.project_id, self.agent_id, self.run_id = client, project_id, agent_id, run_id
        self.interval = flush_interval
        self.path = Path(spool)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS spool(id TEXT PRIMARY KEY, payload TEXT, state TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT)")
        self.db.commit()
        if os.name != "nt":
            self.path.chmod(0o600)
        # Do not re-use a source's sequence on process restart.
        self.source = uid("source")
        self.sequence = 0
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.thread = None
        if start:
            self.thread = threading.Thread(target=self._loop, daemon=True)
            self.thread.start()

    def observe(self, suffix, data, attribution="reported", critical=False, kind="runtime_hook"):
        with self.lock:
            if self.path.stat().st_size > 256 * 1024 * 1024:
                raise RuntimeError("Capture spool budget exhausted; capture is incomplete")
            clean = redact_value(data)
            event = Event(
                event_id=uid("evt"), type="hivemind.context." + suffix,
                agent_id=self.agent_id, run_id=self.run_id, project_id=self.project_id,
                occurred_at=now(), source={"source_id": self.source, "kind": kind,
                                          "sequence": self.sequence, "attribution": attribution}, data=clean)
            self.sequence += 1
            self.db.execute("INSERT INTO spool VALUES(?,?,'pending')",
                            (event.event_id, canonical(event.model_dump())))
            self.db.commit()
        if critical:
            self.flush()
        return event.event_id

    def flush(self):
        with self.lock:
            rows = self.db.execute("SELECT id,payload FROM spool WHERE state='pending' LIMIT 100").fetchall()
            batch, size = [], 0
            for _resource, payload in rows:
                size += len(payload.encode())
                if size > 1000000:
                    break
                batch.append(json.loads(payload))
            if not batch:
                return
            results = self.client.request("POST", "events:batch", {"events": batch})
            for result in results:
                if result["accepted"]:
                    self.db.execute("DELETE FROM spool WHERE id=?", (result["event_id"],))
                elif not result["retryable"]:
                    self.db.execute("UPDATE spool SET state='quarantined' WHERE id=?", (result["event_id"],))
            self.db.commit()

    def _loop(self):
        while not self.stop.wait(self.interval):
            try:
                self.flush()
                with self.lock:
                    quarantined = self.db.execute("SELECT count(*) FROM spool WHERE state='quarantined'").fetchone()[0]
                self.client.request("POST", f"runs/{self.run_id}/heartbeat",
                                    {"capture_health": {"state": "gaps" if quarantined else "healthy",
                                                        "quarantined_event_count": quarantined}})
            except (httpx.HTTPError, OSError):
                # Retain the original IDs and payloads. No secrets in logs.
                pass

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=20)
            if self.thread.is_alive():
                raise RuntimeError("Capture shutdown timed out; spool retained")
        try:
            self.flush()
        finally:
            self.db.close()


class ControlledAdapter:
    """Safe-boundary adapter: the host calls boundary before each mutating tool."""

    def __init__(self, client, project, agent, run, state_file):
        self.client, self.project, self.agent, self.run = client, project, agent, run
        self.path = Path(state_file)
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {
            "applied": [], "scope_ids": [], "paused": False, "pending_results": {}}
        self.state.setdefault("pending_results", {})

    def persist(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(canonical(self.state), encoding="utf-8")
        if os.name != "nt":
            temporary.chmod(0o600)
        os.replace(temporary, self.path)

    def boundary(self):
        # Never report completion before durable local checkpoint and state transition.
        for control_id, result in list(self.state["pending_results"].items()):
            self.client.request("POST", f"controls/{control_id}:result", result, key="result-" + control_id)
            self.state["applied"].append(control_id)
            del self.state["pending_results"][control_id]
            if self.state.get("transition", {}).get("control_id") == control_id:
                del self.state["transition"]
            self.persist()
        controls = self.client.request("GET", f"agents/{self.agent}/controls?project_id={self.project}")
        for control in reversed(controls):
            control_id = control["id"]
            if control_id in self.state["applied"]:
                continue
            if self.state.get("transition", {}).get("control_id") == control_id:
                if control.get("state") == "acknowledged":
                    self.client.request("POST", f"controls/{control_id}:checkpoint",
                                        {"checkpoint_id": self.state["transition"]["checkpoint_id"]},
                                        key="checkpoint-" + control_id)
                continue
            if (control.get("action") not in ("pause", "resume", "cancel", "steer", "assign_task")
                    or control.get("target_run_id", self.run) != self.run):
                raise RuntimeError("Unsupported or foreign control at safe boundary")
            self.client.request("POST", f"controls/{control_id}:ack", {}, key="ack-" + control_id)
            action = control["action"]
            if action in ("steer", "assign_task"):
                # Replacement is already reserved by Hivemind; host consumes the new assignment as context.
                self.state["task_id"] = control["replacement_task_id"]
                claims = self.client.request("GET", f"work-claims?project_id={self.project}")
                replacement = next(c for c in claims if c["id"] == control["reserved_claim_id"])
                self.state["scope_ids"] = replacement["scope_ids"]
                self.state["transition"] = {"control_id": control_id, "checkpoint_id": uid("checkpoint")}
                self.persist()
                self.client.request("POST", f"controls/{control_id}:checkpoint",
                                    {"checkpoint_id": self.state["transition"]["checkpoint_id"]},
                                    key="checkpoint-" + control_id)
                # Applied is deferred until the host actually performs a scoped replacement operation.
                continue
            self.state["paused"] = action == "pause"
            self.state["cancelled"] = action == "cancel"
            status = "paused" if action == "pause" else "cancelled" if action == "cancel" else "running"
            result = {"outcome": "applied", "checkpoint_id": uid("checkpoint"), "observed_status": status}
            self.state["pending_results"][control_id] = result
            self.persist()
            self.client.request("POST", f"controls/{control_id}:result", result, key="result-" + control_id)
            self.state["applied"].append(control_id)
            del self.state["pending_results"][control_id]
            self.persist()
        return self.state

    def confirm_transition(self):
        transition = self.state.get("transition")
        if not transition:
            return
        control_id = transition["control_id"]
        result = {"outcome": "applied", "checkpoint_id": transition["checkpoint_id"], "observed_status": "running"}
        self.state["pending_results"][control_id] = result
        self.persist()
        self.client.request("POST", f"controls/{control_id}:result", result, key="result-" + control_id)
        self.state["applied"].append(control_id)
        del self.state["pending_results"][control_id]
        del self.state["transition"]
        self.persist()

    def permit(self, paths=None):
        self.boundary()
        if self.state.get("paused") or self.state.get("cancelled"):
            raise RuntimeError("Run paused or cancelled at safe boundary")
        return self.client.request("POST", f"runs/{self.run}/permits:check",
                                    {"scope_ids": self.state["scope_ids"], "paths": paths or []})
