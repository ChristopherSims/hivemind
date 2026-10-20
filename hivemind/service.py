import json
import random
import time

from . import models as m
from .storage import Problem, canonical, digest, now, searchable, uid

TERMINAL = {"completed", "failed", "cancelled"}
PALETTE = ["blue", "teal", "violet", "coral", "green", "magenta"]


class Service:
    def __init__(self, store):
        self.store = store
        self.indexes = None
        from .nexus_delegation import NexusDelegation
        self.nexus_delegation = NexusDelegation(self)

    def project_access(self, principal, project, human=False):
        self.store.project(project)
        if principal["role"] != "admin" and principal["project"] != project:
            raise Problem("not_found", 404)
        if human and principal["role"] not in ("admin", "human", "observer"):
            raise Problem("forbidden", 403)

    def represent(self, principal, agent):
        self.store.get(agent, principal, "agent")
        if principal["role"] != "admin" and principal["agent"] != agent:
            raise Problem("forbidden", 403)

    def run(self, principal, run_id):
        run = self.store.get(run_id, principal, "run")
        self.represent(principal, run["agent_id"])
        return run

    def mutate(self, principal, operation, key, payload, function):
        if not key or len(key) > 128:
            raise Problem("idempotency_key_required", 422)
        with self.store.transaction(), self.store.acting_as(principal):
            old = self.store.db.execute(
                "SELECT * FROM commands WHERE principal=? AND operation=? AND key=?",
                (principal["id"], operation, key),
            ).fetchone()
            checksum = digest(payload)
            if old:
                if old["hash"] != checksum:
                    raise Problem("idempotency_conflict")
                cached = json.loads(old["response"])
                if operation == "memory-extract":
                    for ref in payload["source_refs"]:
                        self.reference(principal, payload["project_id"], ref["id"])
                    for memory in cached["memories"]:
                        self.memory_get(principal, memory["id"])
                        if not self.store.valid_memory(memory, principal, check_activity=False):
                            raise Problem("not_found", 404)
                if operation.startswith("conversations-read:"):
                    self.validate_read_messages(principal, operation.split(":", 1)[1], payload["message_ids"])
                if isinstance(cached, dict) and cached.get("id"):
                    # A retry is not an escape hatch around deletion, source invalidation, or current ACLs.
                    row = self.store.raw(cached["id"])
                    if row["kind"] == "control" and operation == "controls":
                        current = json.loads(row["body"])
                        self.coordinator(principal, current["project_id"], current["coordinator_epoch"],
                                         current["action"], current["target_agent_id"])
                        if principal["role"] != "admin":
                            self.nexus_delegation.check_control(current)
                    if row["kind"] == "nexus_grant":
                        return self.store.get(cached["id"], principal, "nexus_grant")
                    self.store.get(cached["id"], principal, review_memory=operation.startswith("memory-"))
                    self.store.team_access.require_contribute(row, principal)
                    if row["kind"] in ("team", "resource_access"):
                        return self.store.get(cached["id"], principal)
                    if row["kind"] == "message" and operation.startswith("claim:"):
                        current = json.loads(row["body"])
                        if (self.delivery_state(current)["delivery_status"] != "leased" or
                                current.get("delivery_id") != cached.get("delivery_id")):
                            raise Problem("delivery_receipt_stale")
                        if self.run(principal, current["claim_run_id"])["status"] in TERMINAL:
                            raise Problem("terminal_run")
                    if row["kind"] == "task" and operation.startswith("task-"):
                        current = self.store.get(cached["id"], principal, "task")
                        if (current.get("execution") or {}).get("id") != (cached.get("execution") or {}).get("id"):
                            raise Problem("stale_task_execution")
                        if operation.startswith("task-start:"):
                            self.run(principal, payload["run_id"])
                        cached = self.store.task_view(cached, principal)
                    if row["kind"] == "memory_dispute":
                        if self.memory_dispute_get(principal, cached["id"]).get("evidence_unavailable"):
                            raise Problem("not_found", 404)
                    if row["kind"] == "memory_proposal":
                        self.memory_proposal_get(principal, cached["id"])
                    if row["kind"] == "notification":
                        self.notification_get(principal, cached["id"])
                    if row["kind"] in ("memory", "memory_proposal") and not self.store.valid_memory(
                            cached, principal, check_activity=False):
                        raise Problem("not_found", 404)
                return cached
            result = function()
            self.store.db.execute("INSERT INTO commands VALUES(?,?,?,?,?)",
                                  (principal["id"], operation, key, checksum, canonical(result)))
            return result

    def create(self, principal, kind, data, private=None):
        self.project_access(principal, data["project_id"])
        with self.store.acting_as(principal):
            return self.store.save(kind, data["project_id"], principal["id"],
                                   {"id": uid(kind), "created_at": now(), **data}, private)

    def agent(self, principal, request):
        if principal["role"] != "admin":
            raise Problem("administrator_required", 403)
        return self.create(principal, "agent", {
            **request.model_dump(), "base_color_token": random.choice(PALETTE),
        })

    def start_run(self, principal, request):
        self.represent(principal, request.agent_id)
        agent = self.store.get(request.agent_id, principal)
        if agent["project_id"] != request.project_id:
            raise Problem("not_found", 404)
        if request.task_id:
            self.reference(principal, request.project_id, request.task_id, "task")
        body = self.create(principal, "run", {
            **request.model_dump(), "status": "running", "last_seen_at": time.time(),
            "capture_health": {"state": "unknown"}, "tools": {}, "files": {},
            "summary": None, "source_highwater": {}, "control_revision": 1,
        })
        self.store.relate(request.project_id, request.agent_id, body["id"], "HAS_RUN")
        if request.task_id:
            self.store.relate(request.project_id, body["id"], request.task_id, "WORKS_ON")
        return body

    def reference(self, principal, project, resource_id, kind=None):
        body = self.store.get(resource_id, principal, kind)
        if body["project_id"] != project:
            raise Problem("not_found", 404)
        return body

    def task_start(self, principal, task_id, request):
        task = self.store.get(task_id, principal, "task")
        run = self.run(principal, request.run_id)
        if run["project_id"] != task["project_id"]:
            raise Problem("not_found", 404)
        if run["status"] not in ("running", "idle"):
            raise Problem("run_not_ready")
        if task["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if task["status"] in ("completed", "failed"):
            raise Problem("terminal_task")
        if (task.get("scheduling") or {}).get("state") == "superseded":
            raise Problem("task_superseded")
        prior = task.get("execution")
        if prior:
            old = self.store.get(prior["run_id"], principal, "run")
            if old["agent_id"] != run["agent_id"]:
                raise Problem("task_assigned_elsewhere", 403)
            if not request.recover or (old["id"] != run["id"] and old["status"] not in TERMINAL
                                      and old["last_seen_at"] > time.time() - 30):
                raise Problem("task_execution_active")
        elif request.recover:
            raise Problem("no_execution_to_recover")
        if task.get("depends_on") and (getattr(self, "task_dependency_gates", ()) or task.get("plan_id")):
            from .scheduling import Scheduling
            if not Scheduling(self)._dependencies_met(principal, task):
                raise Problem("task_dependencies_held")
        for gate in getattr(self, "task_start_gates", getattr(self, "task_dispatch_gates", ())):
            verdict = gate(principal, task, run)
            if verdict is not None:
                if not isinstance(verdict, dict) or verdict.get("action") not in ("allow", "defer"):
                    raise Problem("invalid_dispatch_gate", 503)
                if verdict["action"] == "defer":
                    raise Problem("task_dispatch_held")
        self.task_permit(principal, run["id"], task)
        if run.get("task_id") not in (None, task_id):
            raise Problem("run_already_assigned")
        history = task.get("execution_history", [])
        if prior:
            history = [*history, prior][-20:]
        task.update(status="running", execution_history=history,
                    execution={"id": uid("execution"), "run_id": run["id"], "agent_id": run["agent_id"],
                               "started_at": now(), "progress": prior.get("progress", 0) if prior else 0,
                               "checkpoint": prior.get("checkpoint") if prior else None})
        budgets = getattr(self, "task_budgets", None)
        if budgets is not None and budgets.policy.enabled:
            task["execution"]["budget_required"] = True
        row = self.store.raw(task_id)
        result = self.store.save("task", row["project"], row["owner"], task, expected=request.expected_revision)
        run["task_id"] = task_id
        row = self.store.raw(run["id"])
        self.store.save("run", row["project"], row["owner"], run)
        self.store.relate(task["project_id"], run["id"], task_id, "WORKS_ON")
        return result

    def task_progress(self, principal, task_id, request):
        task = self.store.get(task_id, principal, "task")
        run = self.run(principal, request.run_id)
        execution = task.get("execution")
        if not execution or execution["run_id"] != run["id"] or execution["id"] != request.execution_id:
            raise Problem("stale_task_execution")
        if task["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if task["status"] in ("completed", "failed") or run["status"] not in ("running", "idle"):
            raise Problem("task_not_running")
        if run.get("task_id") != task_id:
            raise Problem("stale_task_execution")
        self.task_permit(principal, run["id"], task)
        if request.progress < execution["progress"] or (request.status == "completed" and request.progress != 100):
            raise Problem("invalid_task_progress", 422)
        refs = [ref.model_dump(exclude_none=True) for ref in request.evidence_refs]
        if any(ref["id"] == task_id for ref in refs):
            raise Problem("self_evidence_forbidden", 422)
        self.evidence(principal, task["project_id"], refs)
        for ref in refs:
            ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
        previous = execution.get("checkpoint")
        if previous and previous.get("evidence_unavailable"):
            previous = None
        if previous and previous["id"] == request.checkpoint_id:
            if (previous["summary"] != request.summary or previous["evidence_refs"] != refs
                    or task["status"] != request.status or execution["progress"] != request.progress):
                raise Problem("checkpoint_id_conflict")
            return task
        execution = {**execution, "progress": request.progress, "checkpoint": {
            "id": request.checkpoint_id, "summary": request.summary, "evidence_refs": refs, "recorded_at": now()}}
        task.update(status=request.status, execution=execution)
        row = self.store.raw(task_id)
        result = self.store.save("task", row["project"], row["owner"], task, expected=request.expected_revision)
        if request.status in ("completed", "failed"):
            run["task_id"] = None
            row = self.store.raw(run["id"])
            self.store.save("run", row["project"], row["owner"], run)
        return result

    def evidence(self, principal, project, refs, private=None):
        for ref in refs:
            body = self.reference(principal, project, ref["id"])
            if ref.get("revision") and ref["revision"] != body["revision"]:
                raise Problem("stale_evidence")
            row = self.store.raw(ref["id"])
            if row["kind"] != ref["kind"]:
                raise Problem("evidence_kind_mismatch", 422)
            if row["private_agent"] and row["private_agent"] != private:
                raise Problem("evidence_scope_violation", 403)
            if row["kind"] == "message":
                raise Problem("evidence_scope_violation", 403)

    def accept_event(self, principal, request):
        data = request.model_dump()
        if len(canonical(data).encode()) > 131072:
            raise Problem("payload_too_large", 413)
        self.project_access(principal, request.project_id)
        self.represent(principal, request.agent_id)
        run = self.run(principal, request.run_id)
        if (run["agent_id"], run["project_id"]) != (request.agent_id, request.project_id):
            raise Problem("not_found", 404)
        if request.source.attribution == "confirmed" and not principal["capture_authority"]:
            raise Problem("unregistered_capture_authority", 403)
        if request.source.kind == "filesystem" and request.source.attribution == "confirmed":
            raise Problem("unattributed_observation_required", 422)
        checksum = digest(data)
        with self.store.transaction(), self.store.acting_as(principal):
            self.store.team_access.require_contribute(self.store.raw(request.run_id, "run"), principal)
            old = self.store.db.execute(
                "SELECT * FROM events WHERE project=? AND (id=? OR (source=? AND source_seq=?))",
                (request.project_id, request.event_id, request.source.source_id, request.source.sequence),
            ).fetchone()
            if old:
                if old["hash"] != checksum:
                    raise Problem("idempotency_conflict")
                return json.loads(old["result"])
            # Bind a source to its original run and principal, rather than allowing source takeover.
            prior = self.store.db.execute(
                "SELECT body FROM events WHERE project=? AND source=? LIMIT 1",
                (request.project_id, request.source.source_id),
            ).fetchone()
            if prior and json.loads(prior[0])["run_id"] != request.run_id:
                raise Problem("source_scope_mismatch", 403)
            run = self.run(principal, request.run_id)
            seq = self.store.tick(request.project_id)
            suffix = request.type.removeprefix("hivemind.context.")
            payload = request.data
            source = request.source.source_id
            high = run["source_highwater"].get(source, -1)
            if request.source.sequence > high + 1:
                run["capture_health"] = {"state": "gaps", "missing_source_sequences":
                                         [{"source_id": source, "from": high + 1,
                                           "to": request.source.sequence - 1}]}
            if suffix == "tool.completed":
                call = run["tools"].get(payload["call_id"], {})
                run["tools"][payload["call_id"]] = {**call, **payload, "state": "completed"}
            elif suffix == "tool.started":
                call = run["tools"].get(payload["call_id"])
                if not call or call["state"] != "completed":
                    run["tools"][payload["call_id"]] = {**payload, "state": "active"}
            elif suffix == "file.changed":
                path_key = payload["path"] if self.store.project(request.project_id)["path_case_sensitive"] else (
                    payload["path"].casefold())
                previous = run["files"].get(path_key, {})
                if not previous or previous.get("source_id") != source or request.source.sequence > previous["sequence"]:
                    run["files"][path_key] = {
                        **payload, "source_id": source, "sequence": request.source.sequence,
                        "event_id": request.event_id, "attribution": request.source.attribution,
                    }
                    workspace = run.get("workspace_id") or run["id"]
                    existing_files = self.store.db.execute(
                        """SELECT * FROM resources WHERE kind='file' AND project=? AND deleted=0
                        AND json_extract(body,'$.workspace_id')=? AND json_extract(body,'$.path_key')=? LIMIT 1""",
                        (request.project_id, workspace, path_key)).fetchone()
                    file_body = json.loads(existing_files["body"]) if existing_files else {
                        "id": uid("file"), "project_id": request.project_id, "workspace_id": workspace,
                        "path": payload["path"], "path_key": path_key, "created_at": now()}
                    file_body.update(change=payload["change"], attribution=request.source.attribution,
                                     source_event_id=request.event_id)
                    file_record = self.store.save("file", request.project_id,
                        existing_files["owner"] if existing_files else principal["id"], file_body, seq=seq)
                    self.store.relate(request.project_id, run["id"], file_record["id"], "TOUCHED")
            elif suffix == "agent.status_reported" and request.source.sequence > high:
                run["summary"] = {"text": payload["text"], "origin": "agent_reported",
                                  "evidence_event_ids": [request.event_id]}
            elif suffix in ("run.status_changed", "run.ended"):
                if payload["expected_revision"] != run["control_revision"]:
                    raise Problem("revision_conflict", 412)
                if run["status"] in TERMINAL:
                    raise Problem("terminal_run")
                run["status"] = payload["status"]
                run["control_revision"] += 1
            elif suffix == "capture.gap_detected":
                run["capture_health"] = {"state": "gaps", **payload}
            elif suffix == "decision.recorded":
                self.evidence(principal, request.project_id, payload["evidence_refs"])
            run["source_highwater"][source] = max(high, request.source.sequence)
            run["last_seen_at"] = time.time()
            self.store.save("run", request.project_id, self.store.raw(run["id"])["owner"], run, seq=seq)
            result = {"event_id": request.event_id, "ingestion_seq": seq, "received_at": now()}
            self.store.db.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?)", (
                request.project_id, request.event_id, seq, source, request.source.sequence,
                checksum, canonical(data), canonical(result),
            ))
            return result

    def board_post(self, principal, board_id, request):
        board = self.store.get(board_id, principal, "board")
        self.store.team_access.require_contribute(self.store.raw(board_id, "board"), principal)
        project = board["project_id"]
        self.project_access(principal, project)
        refs = [x.model_dump(exclude_none=True) for x in request.evidence_refs]
        self.evidence(principal, project, refs)
        if request.parent_post_id:
            parent = self.reference(principal, project, request.parent_post_id, "board_post")
            if parent["board_id"] != board_id or request.thread_id != parent["thread_id"]:
                raise Problem("thread_mismatch", 422)
        elif request.thread_id:
            raise Problem("parent_required", 422)
        content = request.content.model_dump(exclude_none=True)
        mentions = {s["agent_id"] for s in content.get("segments", []) if s["kind"] == "agent_mention"}
        for agent in mentions:
            self.reference(principal, project, agent, "agent")
            recipient = {"role": "agent", "agent": agent, "project": project}
            prospective = {"id": uid("board_post"), "kind": "board_post", "project": project,
                           "private_agent": None, "body": canonical({
                               **request.model_dump(exclude_none=True), "board_id": board_id})}
            if not self.store.visible(prospective, recipient):
                raise Problem("mention_recipient_unavailable", 422)
        author = ({"kind": "agent", "agent_id": principal["agent"]}
                  if principal["agent"] else {"kind": "human", "principal_id": principal["id"]})
        post = self.create(principal, "board_post", {
            **request.model_dump(exclude_none=True), "board_id": board_id, "project_id": project,
            "thread_id": request.thread_id or uid("thread"), "author": author,
            "trust": "agent_reported" if principal["agent"] else "human_reported",
        })
        for agent in mentions:
            self.create(principal, "notification", {
                "project_id": project, "recipient_agent_id": agent, "post_id": post["id"],
                "thread_id": post["thread_id"], "type": "board_mention", "state": "available",
                "expires_at": time.time() + 86400,
            }, private=agent)
            self.store.relate(project, post["id"], agent, "MENTIONS")
        if request.parent_post_id:
            self.store.relate(project, post["id"], request.parent_post_id, "REPLIES_TO")
        if principal["agent"]:
            self.store.relate(project, principal["agent"], post["id"], "AUTHORED")
        return post

    def message(self, principal, request):
        self.represent(principal, request.from_agent)
        self.reference(principal, request.project_id, request.to, "agent")
        if request.sender_run_id:
            run = self.run(principal, request.sender_run_id)
            if run["agent_id"] != request.from_agent:
                raise Problem("sender_run_mismatch", 403)
        if request.in_reply_to:
            parent = self.store.get(request.in_reply_to, principal, "message")
            if (parent["thread_id"] != request.thread_id or
                    {parent["from"], parent["to"]} != {request.from_agent, request.to} or
                    parent["correlation_id"] != (request.correlation_id or request.thread_id)):
                raise Problem("thread_mismatch", 422)
            if parent.get("reply_depth", 0) >= 12:
                raise Problem("reply_depth_limit", 422)
        else:
            parent = {}
        checksum = digest(request.model_dump(by_alias=True, exclude_none=True))
        existing = self.store.db.execute("SELECT * FROM resources WHERE id=?", (request.message_id,)).fetchone()
        if existing:
            if existing["kind"] != "message" or existing["deleted"]:
                raise Problem("idempotency_conflict")
            original = self.store.get(request.message_id, principal, "message")
            if original.get("payload_hash") != checksum:
                raise Problem("idempotency_conflict")
            return original
        return self.store.save("message", request.project_id, principal["id"], {
            **request.model_dump(by_alias=True, exclude_none=True),
            "id": request.message_id, "created_at": now(), "state": "available",
            "correlation_id": request.correlation_id or request.thread_id,
            "reply_depth": parent.get("reply_depth", -1) + 1,
            "expires_at": time.time() + request.ttl_seconds,
            "claim_run_id": None, "claim_until": 0,
            "attempt_count": 0, "total_attempts": 0, "available_after": 0, "delivery_history": [],
            "payload_hash": checksum,
        })

    def message_read(self, principal, message):
        self.project_access(principal, message["project_id"])
        if principal["role"] not in ("admin", "observer") and principal["agent"] not in (
                message["from"], message["to"]):
            raise Problem("not_found", 404)
        return message

    def inbox(self, principal, agent, project):
        self.represent(principal, agent)
        return [self.delivery_state(x) for x in self.store.list("message", project, principal, 1000)
                if x["to"] == agent and self.delivery_state(x)["delivery_status"] == "available"]

    def retry_delay(self, message):
        policy = message.get("retry_policy") or m.MessageRetryPolicy().model_dump()
        return min(policy["max_delay_seconds"],
                   policy["base_delay_seconds"] * 2 ** max(0, min(message.get("attempt_count", 1) - 1, 20)))

    def delivery_state(self, message):
        attempts = message.get("attempt_count", int(message["state"] == "delivered"))
        policy = message.get("retry_policy") or m.MessageRetryPolicy().model_dump()
        next_attempt = message.get("available_after", 0)
        if message["state"] == "acked":
            state = "acked"
        elif message["expires_at"] <= time.time():
            state = "expired"
        elif message["state"] == "delivered" and message["claim_until"] > time.time():
            state = "leased"
        elif message["state"] == "dead_letter" or attempts >= policy["max_attempts"]:
            state = "dead_letter"
        else:
            if message["state"] == "delivered":
                next_attempt = max(next_attempt, message["claim_until"] + self.retry_delay(message))
            state = "retry_wait" if next_attempt > time.time() else "available"
        return {**message, "delivery_status": state, "attempt_count": attempts,
                "next_attempt_at": next_attempt if state == "retry_wait" else None}

    def delivery_save(self, message, action, **details):
        # Bounded structured receipts, never handler output or hidden reasoning.
        history = message.get("delivery_history", [])
        message["delivery_history"] = [*history, {"action": action, "at": now(),
            "delivery_id": message.get("delivery_id"), "run_id": message.get("claim_run_id"), **details}][-50:]
        row = self.store.raw(message["id"])
        return self.delivery_state(self.store.save("message", row["project"], row["owner"], message))


    def claim_message(self, principal, message_id, run_id):
        message = self.store.get(message_id, principal, "message")
        run = self.run(principal, run_id)
        if run["agent_id"] != message["to"] or run["project_id"] != message["project_id"]:
            raise Problem("not_found", 404)
        if run["status"] in TERMINAL:
            raise Problem("terminal_run")
        state = self.delivery_state(message)["delivery_status"]
        if state == "leased":
            if message["claim_run_id"] != run_id:
                raise Problem("delivery_leased")
            return self.delivery_state(message)
        if state != "available":
            raise Problem("message_unavailable")
        if message["state"] == "delivered":
            message["delivery_history"] = [*message.get("delivery_history", []), {
                "action": "lease_expired", "at": now(), "delivery_id": message.get("delivery_id"),
                "run_id": message["claim_run_id"], "expired_at": message["claim_until"]}]
        message.update(state="delivered", claim_run_id=run_id,
                       claim_until=min(time.time() + 30, message["expires_at"]), delivery_id=uid("delivery"),
                       attempt_count=self.delivery_state(message)["attempt_count"] + 1,
                       total_attempts=message.get("total_attempts", int(message["state"] == "delivered")) + 1,
                       available_after=0)
        return self.delivery_save(message, "claimed", attempt=message["attempt_count"])

    def delivery_receipt(self, principal, message_id, run_id, delivery_id):
        message = self.store.get(message_id, principal, "message")
        run = self.run(principal, run_id)
        if (run["agent_id"] != message["to"] or run["project_id"] != message["project_id"]
                or message["claim_run_id"] != run_id):
            raise Problem("not_found", 404)
        if (delivery_id is not None and delivery_id != message.get("delivery_id")) or (
                delivery_id is None and message.get("total_attempts", 1) > 1):
            raise Problem("delivery_receipt_stale")
        return message, run

    def ack_message(self, principal, message_id, run_id, delivery_id=None):
        message, run = self.delivery_receipt(principal, message_id, run_id, delivery_id)
        if message["state"] == "acked":
            return self.delivery_state(message)
        if run["status"] in TERMINAL or self.delivery_state(message)["delivery_status"] != "leased":
            raise Problem("delivery_lease_expired")
        message.update(state="acked", acked_at=now())
        return self.delivery_save(message, "acked")

    def fail_message(self, principal, message_id, request):
        message, run = self.delivery_receipt(principal, message_id, request.run_id, request.delivery_id)
        if run["status"] in TERMINAL or self.delivery_state(message)["delivery_status"] != "leased":
            raise Problem("delivery_lease_expired")
        message.update(state="available", claim_until=0,
                       available_after=time.time() + self.retry_delay(message), last_failure=request.reason)
        return self.delivery_save(message, "failed", reason=request.reason)

    def requeue_message(self, principal, message_id, request):
        message = self.store.get(message_id, principal, "message")
        if principal["role"] != "admin" and principal["agent"] not in (message["from"], message["to"]):
            raise Problem("forbidden", 403)
        if message["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if self.delivery_state(message)["delivery_status"] != "dead_letter":
            raise Problem("message_not_dead_letter")
        message.update(state="available", attempt_count=0, claim_run_id=None, claim_until=0,
                       available_after=0, delivery_id=None)
        return self.delivery_save(message, "requeued")

    def conversation_access(self, principal, project):
        self.project_access(principal, project)
        if principal["role"] not in ("admin", "observer", "agent"):
            raise Problem("conversation_observer_grant_required", 403)

    def validate_read_messages(self, principal, project, message_ids):
        self.conversation_access(principal, project)
        for identity in set(message_ids):
            message = self.store.get(identity, principal, "message")
            if message["project_id"] != project:
                raise Problem("not_found", 404)

    def read_messages(self, principal, project, request):
        self.validate_read_messages(principal, project, request.message_ids)
        for identity in set(request.message_ids):
            self.store.db.execute("INSERT OR IGNORE INTO conversation_reads VALUES(?,?,?,?)",
                                 (principal["id"], project, identity, now()))
        return {"marked_count": len(set(request.message_ids))}

    def conversation_items(self, principal, project):
        self.conversation_access(principal, project)
        reads = {row[0] for row in self.store.db.execute(
            "SELECT message FROM conversation_reads WHERE principal=? AND project=?", (principal["id"], project))}
        return [{**self.delivery_state(item), "is_read": item["id"] in reads}
                for item in self.store.list("message", project, principal, 1000)]

    def timeline_page(self, principal, project, kind, before=None, limit=100, query="", board_id=None, agent_id=None):
        self.project_access(principal, project)
        if kind == "message":
            self.conversation_access(principal, project)
        # Stable creation sequence, not a revision's moving ingestion sequence.
        creation = """COALESCE(json_extract(r.body,'$.created_seq'),
            (SELECT json_extract(v.body,'$.ingestion_seq') FROM versions v
             WHERE v.id=r.id ORDER BY v.revision LIMIT 1),r.seq)"""
        sql = f"SELECT r.*, {creation} AS creation_seq FROM resources r WHERE r.project=? AND r.kind=? AND r.deleted=0"
        args = [project, kind]
        if board_id:
            sql += " AND json_extract(r.body,'$.board_id')=?"
            args.append(board_id)
        if before is not None:
            sql += f" AND {creation}<?"
            args.append(before)
        sql += " ORDER BY creation_seq DESC"
        threads = set()
        if kind == "board_post" and agent_id:
            for row in self.store.db.execute(
                    "SELECT * FROM resources WHERE project=? AND kind='board_post' AND deleted=0", (project,)):
                if not self.store.visible(row, principal):
                    continue
                body = json.loads(row["body"])
                if body["board_id"] == board_id and (body.get("author", {}).get("agent_id") == agent_id or
                        any(segment.get("agent_id") == agent_id for segment in body["content"].get("segments", []) or [])):
                    threads.add(body["thread_id"])
        reads = {row[0] for row in self.store.db.execute(
            "SELECT message FROM conversation_reads WHERE principal=? AND project=?", (principal["id"], project))}
        items = []
        for row in self.store.db.execute(sql, args):
            if not self.store.visible(row, principal):
                continue
            body = json.loads(row["body"])
            if agent_id and ((kind == "message" and agent_id not in (body["from"], body["to"])) or
                             (kind == "board_post" and body["thread_id"] not in threads)):
                continue
            if query.strip().casefold() not in searchable(body).casefold():
                continue
            body["created_seq"] = row["creation_seq"]
            if kind == "message":
                body = {**self.delivery_state(body), "is_read": body["id"] in reads,
                        "origin_kind": "message", "visibility": "Agent direct message"}
            items.append(body)
            if len(items) > limit:
                break
        more = len(items) > limit
        items = items[:limit]
        return {"items": list(reversed(items)), "next_before": items[-1]["created_seq"] if more else None}

    def delivery_summary(self, principal, project):
        items = self.conversation_items(principal, project)
        states = {key: 0 for key in ("available", "leased", "retry_wait", "dead_letter", "expired", "acked")}
        unread, by_agent = 0, {}
        for item in items:
            states[item["delivery_status"]] += 1
            incoming = principal["role"] != "agent" or item["to"] == principal["agent"]
            unseen = incoming and not item["is_read"]
            unread += int(unseen)
            bucket = by_agent.setdefault(item["to"], {"pending": 0, "dead_letter": 0, "unread": 0})
            bucket["pending"] += int(item["delivery_status"] in ("available", "leased", "retry_wait"))
            bucket["dead_letter"] += int(item["delivery_status"] == "dead_letter")
            bucket["unread"] += int(unseen)
        return {"states": states, "unread": unread, "by_agent": by_agent,
                "count": len(items), "limit": 1000}

    def memory(self, principal, request, extraction=None, provider=None):
        private = principal["agent"] if request.private else None
        if request.private and not private:
            raise Problem("agent_private_scope_required", 422)
        scope_kind = request.scope_kind
        if scope_kind == "project":
            if request.scope_owner_id != request.project_id:
                raise Problem("scope_mismatch", 422)
        else:
            scope = self.reference(principal, request.project_id, request.scope_owner_id, scope_kind)
            if scope_kind == "agent":
                self.represent(principal, scope["id"])
                private = scope["id"]
            if scope_kind == "run":
                self.run(principal, scope["id"])
                private = scope["agent_id"]
        refs = [r.model_dump(exclude_none=True) for r in request.evidence_refs]
        self.evidence(principal, request.project_id, refs, private)
        for ref in refs:
            ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
        memory = self.create(principal, "memory", {
            **request.model_dump(), "private": bool(private), "status": "proposed", "origin": "explicit",
            "memory_version_id": uid("memver"), "evidence_refs": refs,
            "created_by": principal["id"],
            **({"origin": "extracted", "extraction": extraction} if extraction else {}),
            **({"origin": "provider_import", "provider_import": provider, "trust": "provider_reported"}
               if provider else {}),
        }, private)
        for ref in refs:
            self.store.relate(request.project_id, memory["id"], ref["id"], "SUPPORTED_BY")
        return memory

    def extract_memories(self, principal, request):
        self.project_access(principal, request.project_id)
        self.represent(principal, request.agent_id)
        if request.requester_run_id:
            run = self.run(principal, request.requester_run_id)
            if run["agent_id"] != request.agent_id or run["project_id"] != request.project_id:
                raise Problem("not_found", 404)
            if run["status"] in TERMINAL:
                raise Problem("terminal_run")
        owner = request.scope_owner_id or (request.agent_id if request.scope_kind == "agent" else None)
        if not owner:
            raise Problem("scope_owner_required", 422)
        if request.scope_kind == "agent" and owner != request.agent_id:
            raise Problem("scope_mismatch", 422)
        if request.scope_kind == "run":
            scope_run = self.run(principal, owner)
            if scope_run["agent_id"] != request.agent_id or scope_run["project_id"] != request.project_id:
                raise Problem("scope_mismatch", 422)
        # Validate every selected source before writing any candidate.
        sources = []
        for ref in request.source_refs:
            if ref.kind not in ("board_post", "artifact"):
                raise Problem("unsupported_extraction_source", 422)
            source = self.reference(principal, request.project_id, ref.id, ref.kind)
            if ref.revision is not None and source["revision"] != ref.revision:
                raise Problem("stale_evidence")
            sources.append(source)
        memories, skipped, seen = [], [], set()
        for source in sources:
            if source["id"] in seen:
                continue
            seen.add(source["id"])
            if len(memories) >= request.max_candidates:
                skipped.append({"source_id": source["id"], "reason": "candidate_limit"})
                continue
            if self.store.raw(source["id"])["kind"] == "board_post" and source["kind"] not in (
                    "finding", "decision", "answer"):
                skipped.append({"source_id": source["id"], "reason": "not_a_finding"})
                continue
            content = source.get("content", {})
            text = content.get("text") or " ".join(
                segment.get("text", "") if segment["kind"] == "text" else "@" + segment["agent_id"]
                for segment in content.get("segments") or [])
            if not text.strip() or len(text.encode("utf-8")) > request.max_source_bytes:
                skipped.append({"source_id": source["id"], "reason": "empty_or_oversized"})
                continue
            ref = {"kind": self.store.raw(source["id"])["kind"], "id": source["id"], "revision": source["revision"]}
            fingerprint = digest([request.project_id, request.scope_kind, owner, ref, text, "extractive-v1"])
            previous = self.store.db.execute("SELECT memory FROM memory_extractions WHERE fingerprint=?",
                                             (fingerprint,)).fetchone()
            if previous:
                try:
                    existing = self.memory_get(principal, previous["memory"])
                except Problem:
                    skipped.append({"source_id": source["id"], "reason": "previous_memory_unavailable"})
                    continue
                if (existing.get("extraction") or {}).get("fingerprint") != fingerprint:
                    skipped.append({"source_id": source["id"], "reason": "previous_version_replaced"})
                    continue
                memories.append(existing)
                continue
            candidate = m.MemoryCreate(project_id=request.project_id, scope_kind=request.scope_kind,
                scope_owner_id=owner, title=text.strip().splitlines()[0][:120],
                content=m.Content(media_type="text/plain", text=text), evidence_refs=[m.Ref(**ref)])
            memory = self.memory(principal, candidate, extraction={
                "method": "extractive-v1", "agent_id": request.agent_id,
                "requester_run_id": request.requester_run_id, "source_revision": source["revision"],
                "fingerprint": fingerprint})
            self.store.db.execute("INSERT INTO memory_extractions VALUES(?,?,?)",
                                 (fingerprint, memory["id"], request.agent_id))
            memories.append(memory)
        return {"memories": memories, "skipped": skipped, "method": "extractive-v1"}

    def review_notification(self, principal, subject, memory, decision):
        row = self.store.raw(subject["id"])
        owner = self.store.db.execute("SELECT agent FROM principals WHERE id=?", (row["owner"],)).fetchone()
        agent = (subject.get("extraction") or {}).get("agent_id") or (owner["agent"] if owner else None)
        agent = agent or row["private_agent"]
        if not agent:
            return
        self.create(principal, "notification", {
            "project_id": row["project"], "recipient_agent_id": agent, "type": "memory_review",
            "subject_id": subject["id"], "subject_kind": row["kind"], "subject_revision": subject["revision"],
            "memory_id": memory["id"], "memory_version_id": subject["memory_version_id"],
            "decision": decision, "state": "available", "expires_at": time.time() + 86400,
        }, private=agent)

    def notification_get(self, principal, identity):
        notice = self.store.get(identity, principal, "notification")
        if notice["type"] == "memory_review":
            if notice["subject_kind"] == "memory_proposal":
                subject = self.memory_proposal_get(principal, notice["subject_id"])
            else:
                subject = self.memory_get(principal, notice["subject_id"])
            memory = self.memory_get(principal, notice["memory_id"])
            if subject["revision"] != notice["subject_revision"]:
                raise Problem("not_found", 404)
            if notice["decision"] in ("verify", "approve") and memory["memory_version_id"] != notice["memory_version_id"]:
                raise Problem("not_found", 404)
            notice = {**notice, "current_memory_status": memory["effective_status"]}
        return notice

    def notifications(self, principal, agent, project):
        self.project_access(principal, project)
        self.represent(principal, agent)
        items = []
        for item in self.store.list("notification", project, principal, 1000):
            if item["recipient_agent_id"] != agent or item["state"] == "acked" or item["expires_at"] <= time.time():
                continue
            try:
                items.append(self.notification_get(principal, item["id"]))
            except Problem:
                continue
        return items

    def memory_reviewer(self, principal):
        if principal["role"] not in ("admin", "human", "observer"):
            raise Problem("verification_grant_required", 403)

    def memory_get(self, principal, memory_id):
        body = self.store.get(memory_id, principal, "memory", review_memory=True)
        return {**body, "effective_status": self.store.memory_state(body)}

    def memory_review(self, principal, memory_id, revision, decision, reason):
        self.memory_reviewer(principal)
        body = self.memory_get(principal, memory_id)
        if body["revision"] != revision:
            raise Problem("revision_conflict", 412)
        if body.get("active_dispute_ids"):
            raise Problem("dispute_review_required")
        if body["status"] not in ("proposed", "verified"):
            raise Problem("memory_not_reviewable")
        if decision == "verify" and self.store.memory_state(body) in ("expired", "not_yet_valid"):
            raise Problem("memory_not_current")
        body.pop("effective_status", None)
        body.update(status="verified" if decision == "verify" else "rejected",
                    reviewed_by=principal["id"], review_reason=reason)
        if decision == "verify":
            body["verified_by"] = principal["id"]
        row = self.store.raw(memory_id)
        result = self.store.save("memory", row["project"], row["owner"], body, private=row["private_agent"],
                                 expected=revision)
        self.review_notification(principal, result, result, decision)
        return result

    def memory_propose_version(self, principal, memory_id, request):
        current = self.memory_get(principal, memory_id)
        if current["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if current.get("active_dispute_ids") or current["status"] not in ("proposed", "verified"):
            raise Problem("memory_not_reviewable")
        row = self.store.raw(memory_id)
        refs = [ref.model_dump(exclude_none=True) for ref in request.evidence_refs]
        if any(ref["id"] == memory_id for ref in refs):
            raise Problem("self_evidence_forbidden", 422)
        self.evidence(principal, row["project"], refs, row["private_agent"])
        for ref in refs:
            ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
        if not self.store.valid_memory({**current, "evidence_refs": refs}, principal, check_activity=False):
            raise Problem("cyclic_or_invalid_memory_evidence", 422)
        proposal = self.create(principal, "memory_proposal", {
            **request.model_dump(), "project_id": current["project_id"], "memory_id": memory_id,
            "evidence_refs": refs,
            "base_revision": current["revision"], "supersedes_version_id": current["memory_version_id"],
            "memory_version_id": uid("memver"), "status": "proposed",
            "proposed_by": principal["id"],
        }, private=row["private_agent"])
        for ref in refs:
            self.store.relate(row["project"], proposal["id"], ref["id"], "SUPPORTED_BY")
        return proposal

    def memory_proposal_get(self, principal, proposal_id):
        proposal = self.store.get(proposal_id, principal, "memory_proposal", review_memory=True)
        self.memory_get(principal, proposal["memory_id"])
        return proposal

    def memory_review_proposal(self, principal, proposal_id, request):
        self.memory_reviewer(principal)
        proposal = self.memory_proposal_get(principal, proposal_id)
        if proposal["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if proposal["status"] != "proposed":
            raise Problem("proposal_already_reviewed")
        current = self.memory_get(principal, proposal["memory_id"])
        if request.decision == "approve":
            if current["revision"] != proposal["base_revision"]:
                raise Problem("stale_memory_proposal", 412)
            if current.get("active_dispute_ids") or current["status"] not in ("proposed", "verified"):
                raise Problem("memory_not_reviewable")
            candidate = {**current, **{key: proposal[key] for key in
                        ("title", "content", "evidence_refs", "validity", "expires_at", "memory_version_id")},
                         "status": "verified"}
            if self.store.memory_state(candidate) != "verified":
                raise Problem("memory_not_current")
            if not self.store.valid_memory(candidate, principal, check_activity=False):
                raise Problem("cyclic_or_invalid_memory_evidence", 422)
            row = self.store.raw(current["id"])
            candidate.pop("effective_status", None)
            candidate.pop("extraction", None)
            candidate["origin"] = "explicit"
            candidate.update(supersedes_version_id=current["memory_version_id"],
                             verified_by=principal["id"], review_reason=request.reason,
                             version_created_by=self.store.raw(proposal_id)["owner"],
                             version_proposal_id=proposal_id, version_created_at=proposal["created_at"])
            self.store.save("memory", row["project"], row["owner"], candidate, private=row["private_agent"],
                            expected=current["revision"])
            self.store.db.execute("DELETE FROM relations WHERE source=? AND kind='SUPPORTED_BY'", (current["id"],))
            for ref in candidate["evidence_refs"]:
                self.store.relate(row["project"], current["id"], ref["id"], "SUPPORTED_BY")
        proposal.update(status="approved" if request.decision == "approve" else "rejected",
                        reviewed_by=principal["id"], review_reason=request.reason)
        row = self.store.raw(proposal_id)
        result = self.store.save("memory_proposal", row["project"], row["owner"], proposal,
                                 private=row["private_agent"], expected=request.expected_revision)
        self.review_notification(principal, result, self.memory_get(principal, proposal["memory_id"]), request.decision)
        return result

    def memory_versions(self, principal, memory_id):
        current = self.memory_get(principal, memory_id)
        rows = self.store.db.execute("SELECT body FROM versions WHERE id=? ORDER BY revision DESC LIMIT 200",
                                     (memory_id,)).fetchall()
        versions, seen = [], set()
        for row in rows:
            body = json.loads(row["body"])
            version_id = body["memory_version_id"]
            if version_id in seen:
                continue
            seen.add(version_id)
            if not self.store.valid_memory(body, principal, check_activity=False):
                continue
            versions.append({**body, "effective_status": self.store.memory_state(body)
                             if version_id == current["memory_version_id"] else "superseded"})
        return versions

    def memory_dispute(self, principal, request):
        if request.left_memory_id == request.right_memory_id:
            raise Problem("distinct_memories_required", 422)
        left, right = (self.memory_get(principal, identity) for identity in
                       (request.left_memory_id, request.right_memory_id))
        if any(body["project_id"] != request.project_id for body in (left, right)):
            raise Problem("not_found", 404)
        if (left["revision"], right["revision"]) != (request.left_revision, request.right_revision):
            raise Problem("revision_conflict", 412)
        if any(self.store.memory_state(body) not in ("proposed", "verified")
               for body in (left, right)):
            raise Problem("memory_not_reviewable")
        private = self.store.raw(left["id"])["private_agent"]
        if private != self.store.raw(right["id"])["private_agent"]:
            raise Problem("evidence_scope_violation", 403)
        if any(ref.id in (left["id"], right["id"]) for ref in request.evidence_refs):
            raise Problem("independent_dispute_evidence_required", 422)
        refs = [ref.model_dump(exclude_none=True) for ref in request.evidence_refs]
        self.evidence(principal, request.project_id, refs, private)
        for ref in refs:
            ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
        return self.create(principal, "memory_dispute", {**request.model_dump(), "status": "reported",
                                                       "evidence_refs": refs, "resolution": None}, private=private)

    def memory_dispute_get(self, principal, dispute_id):
        body = self.store.get(dispute_id, principal, "memory_dispute")
        self.memory_get(principal, body["left_memory_id"])
        self.memory_get(principal, body["right_memory_id"])
        try:
            self.evidence(principal, body["project_id"], body["evidence_refs"], self.store.raw(dispute_id)["private_agent"])
        except Problem:
            body = {**body, "reason": "Dispute evidence is no longer available.", "evidence_refs": [],
                    "review_reason": None, "evidence_unavailable": True}
        return body

    def memory_review_dispute(self, principal, dispute_id, request):
        self.memory_reviewer(principal)
        dispute = self.memory_dispute_get(principal, dispute_id)
        if dispute.get("evidence_unavailable") and request.decision != "dismiss":
            raise Problem("valid_dispute_evidence_required")
        dispute = self.store.get(dispute_id, principal, "memory_dispute")
        if dispute["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        left, right = (self.memory_get(principal, dispute[key]) for key in ("left_memory_id", "right_memory_id"))
        if (left["revision"], right["revision"]) != (request.left_expected_revision, request.right_expected_revision):
            raise Problem("revision_conflict", 412)
        if dispute["status"] not in ("reported", "confirmed"):
            raise Problem("dispute_already_resolved")
        if request.decision != "dismiss" and dispute["status"] == "reported" and (left["revision"], right["revision"]) != (
                dispute["left_revision"], dispute["right_revision"]):
            raise Problem("stale_memory_dispute", 412)
        if request.decision == "confirm" and dispute["status"] != "reported":
            raise Problem("dispute_already_confirmed")
        for index, memory in enumerate((left, right)):
            memory.pop("effective_status", None)
            active = set(memory.get("active_dispute_ids", []))
            if request.decision == "confirm":
                active.add(dispute_id)
            else:
                active.discard(dispute_id)
            memory["active_dispute_ids"] = sorted(active)
            preferred = (index == 0 and request.decision == "prefer_left") or (
                index == 1 and request.decision == "prefer_right")
            if request.decision.startswith("prefer_"):
                if preferred and self.store.memory_state(
                        {**memory, "active_dispute_ids": [], "status": "verified"}) != "verified":
                    raise Problem("memory_not_current")
                memory["status"] = "verified" if preferred else "superseded"
                if preferred:
                    memory["verified_by"] = principal["id"]
            row = self.store.raw(memory["id"])
            self.store.save("memory", row["project"], row["owner"], memory, private=row["private_agent"],
                            expected=memory["revision"])
        dispute.update(status="confirmed" if request.decision == "confirm" else "resolved",
                       resolution=request.decision, reviewed_by=principal["id"], review_reason=request.reason)
        row = self.store.raw(dispute_id)
        self.store.save("memory_dispute", row["project"], row["owner"], dispute,
                        private=row["private_agent"], expected=request.expected_revision)
        return self.memory_dispute_get(principal, dispute_id)

    def active_claims(self, principal, project):
        return [x for x in self.store.list("work_claim", project, principal, 1000)
                if x["state"] in ("active", "reserved") and x["expires_at"] > time.time()]

    def overlaps(self, principal, project, left, right):
        case_sensitive = self.store.project(project)["path_case_sensitive"]
        def comparison(path):
            return path if case_sensitive else path.casefold()
        def ancestry(scope):
            seen = set()
            while scope and scope not in seen:
                seen.add(scope)
                scope = self.reference(principal, project, scope, "scope").get("parent_scope_id")
            return seen
        for a in left:
            for b in right:
                if a in ancestry(b) or b in ancestry(a):
                    return True
                left_scope = self.reference(principal, project, a, "scope")
                right_scope = self.reference(principal, project, b, "scope")
                if any(comparison(x) == comparison(y) or comparison(x).startswith(comparison(y) + "/") or
                       comparison(y).startswith(comparison(x) + "/")
                       for x in left_scope.get("allowed_paths", []) for y in right_scope.get("allowed_paths", [])):
                    return True
        return False

    def claim(self, principal, request, state="active"):
        run = self.run(principal, request.run_id)
        if run["agent_id"] != request.agent_id or run["project_id"] != request.project_id:
            raise Problem("not_found", 404)
        if run["status"] in TERMINAL:
            raise Problem("terminal_run")
        for scope in request.scope_ids:
            self.reference(principal, request.project_id, scope, "scope")
        if request.task_id:
            self.reference(principal, request.project_id, request.task_id, "task")
        for existing in reversed(self.active_claims(principal, request.project_id)):
            if existing["run_id"] == request.run_id:
                continue
            incompatible = ("exclusive_owner" in (request.mode, existing["mode"])
                            and request.mode != "observer" and existing["mode"] != "observer")
            if incompatible and self.overlaps(principal, request.project_id, request.scope_ids, existing["scope_ids"]):
                conflict = self.create(principal, "conflict", {
                    "project_id": request.project_id, "winner_claim_id": existing["id"],
                    "requesting_run_id": request.run_id, "scope_ids": request.scope_ids,
                    "policy_version": 1, "state": "denied", "enforcement": "new_claim_denied",
                })
                return {"denied": True, "conflict": conflict}
        claim = self.create(principal, "work_claim", {
            **request.model_dump(), "state": state, "expires_at": time.time() + request.ttl_seconds,
            "ownership_epoch": self.store.project(request.project_id)["seq"] + 1,
        })
        for scope in request.scope_ids:
            self.store.relate(request.project_id, claim["id"], scope, "CLAIMS_SCOPE")
        self.store.relate(request.project_id, request.run_id, claim["id"], "WORKS_ON")
        if request.task_id:
            self.store.relate(request.project_id, claim["id"], request.task_id, "WORKS_ON")
        return claim

    def lease(self, principal, project):
        leases = self.store.list("nexus_lease", project, principal, 1000)
        return next((x for x in leases if x["state"] == "active" and x["expires_at"] > time.time()), None)

    def acquire_nexus(self, principal, project, request):
        self.project_access(principal, project)
        self.represent(principal, request.agent_id)
        run = self.run(principal, request.run_id)
        if run["agent_id"] != request.agent_id or run["project_id"] != project or run["status"] in TERMINAL:
            raise Problem("not_found", 404)
        if principal["role"] != "admin" and not principal["coordinator"]:
            raise Problem("coordinator_grant_required", 403)
        if principal["role"] != "admin" and getattr(self, "nexus_delegation", None):
            self.nexus_delegation.check(project, request.agent_id, "lease")
        if self.lease(principal, project):
            raise Problem("coordinator_already_active")
        self.retire_controls(principal, project)
        self.store.db.execute("UPDATE projects SET epoch=epoch+1 WHERE id=?", (project,))
        lease = self.create(principal, "nexus_lease", {
            "project_id": project, "agent_id": request.agent_id, "run_id": request.run_id,
            "coordinator_epoch": self.store.project(project)["epoch"],
            "expires_at": time.time() + request.requested_ttl_seconds, "state": "active",
        })
        git = getattr(self, "git_collaboration", None)
        if git:
            git.fence(lease)
        return lease

    def coordinator(self, principal, project, epoch, action=None, target_agent=None):
        lease = self.lease(principal, project)
        if not lease or lease["coordinator_epoch"] != epoch:
            raise Problem("stale_coordinator_epoch")
        current = self.store.db.execute("SELECT * FROM principals WHERE id=?", (principal["id"],)).fetchone()
        if current is None or any(current[key] != principal[key] for key in
                                  ("role", "project", "agent", "token_hash", "coordinator")):
            raise Problem("authentication_required", 401)
        self.represent(principal, lease["agent_id"])
        if principal["role"] != "admin" and not principal["coordinator"]:
            raise Problem("coordinator_grant_required", 403)
        if principal["role"] != "admin" and getattr(self, "nexus_delegation", None):
            self.nexus_delegation.check(project, lease["agent_id"], action, target_agent)
        return lease

    def control(self, principal, request):
        target = self.reference(principal, request.project_id, request.target_run_id, "run")
        lease = self.coordinator(principal, request.project_id, request.coordinator_epoch,
                                 request.action, target["agent_id"])
        self.store.team_access.require_contribute(self.store.raw(target["id"], "run"), principal)
        grant = (self.nexus_delegation.check(request.project_id, lease["agent_id"],
                                            request.action, target["agent_id"])
                 if principal["role"] != "admin" else None)
        self.retire_controls(principal, request.project_id)
        if target["control_revision"] != request.target_run_revision:
            raise Problem("revision_conflict", 412)
        if target["status"] in TERMINAL:
            raise Problem("terminal_run")
        agent = self.store.get(target["agent_id"], principal, "agent")
        if request.action not in agent["supported_actions"] or not agent["scope_permit_enforcement"]:
            raise Problem("unsupported_control", 422)
        if any(c["state"] in ("queued", "acknowledged", "applying") and c["target_run_id"] == target["id"]
               for c in self.store.list("control", request.project_id, principal)):
            raise Problem("control_already_pending")
        if request.action == "assign_task" and target["status"] != "idle":
            raise Problem("target_not_idle")
        reserved_claim = None
        if request.replacement_task_id:
            task = self.reference(principal, request.project_id, request.replacement_task_id, "task")
            self.store.team_access.require_contribute(self.store.raw(task["id"], "task"), principal)
            if task["status"] != "queued" or not task["scope_ids"]:
                raise Problem("replacement_task_unavailable")
            # Reserve under the administrator-like coordinator service path, without impersonating the target.
            reserved_claim = self.reserve_replacement(principal, target, task)
        return self.create(principal, "control", {
            **request.model_dump(), "state": "queued", "target_agent_id": target["agent_id"],
            "issuer_agent_id": lease["agent_id"], "lease_id": lease["id"],
            "reserved_claim_id": reserved_claim["id"] if reserved_claim else None,
            "delegation": ({"grant_id": grant["id"], "revision": grant["revision"]} if grant else None),
            "expires_at": min(lease["expires_at"], time.time() + 120,
                              grant["expires_at"] if grant else lease["expires_at"]), "policy_version": 1,
        })

    def retire_controls(self, principal, project):
        lease = self.lease(principal, project)
        for control in self.store.list("control", project, principal, 1000):
            if control["state"] not in ("queued", "acknowledged", "applying"):
                continue
            authorized = True
            issuer = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                           (self.store.raw(control["id"])["owner"],)).fetchone()
            if issuer is None or (issuer["role"] != "admin" and
                    (not issuer["coordinator"] or issuer["agent"] != control["issuer_agent_id"]
                     or issuer["project"] != project)):
                authorized = False
            elif issuer["role"] != "admin" and getattr(self, "nexus_delegation", None):
                try:
                    self.nexus_delegation.check_control(control)
                except Problem:
                    authorized = False
            if authorized:
                try:
                    self.store.team_access.require_contribute(self.store.raw(control["id"]), dict(issuer))
                except Problem:
                    authorized = False
            if authorized and lease and control["lease_id"] == lease["id"] and control["expires_at"] > time.time():
                continue
            control["state"] = ("outcome_unknown" if control["state"] == "applying" else
                                "expired" if control["expires_at"] <= time.time() else "superseded")
            row = self.store.raw(control["id"])
            self.store.save("control", project, row["owner"], control)
            if control["reserved_claim_id"] and control["state"] != "outcome_unknown":
                claim = self.store.raw(control["reserved_claim_id"])
                body = json.loads(claim["body"])
                if body["state"] == "reserved":
                    body["state"] = "released"
                    with self.store.acting_as(None):
                        self.store.save("work_claim", project, claim["owner"], body)

    def reserve_replacement(self, principal, target, task):
        for claim in self.active_claims(principal, target["project_id"]):
            if claim["mode"] != "observer" and self.overlaps(
                    principal, target["project_id"], claim["scope_ids"], task["scope_ids"]):
                raise Problem("replacement_scope_conflict")
        claim = self.create(principal, "work_claim", {
            "project_id": target["project_id"], "agent_id": target["agent_id"], "run_id": target["id"],
            "task_id": task["id"], "scope_ids": task["scope_ids"], "mode": "exclusive_owner",
            "intent": task["title"], "state": "reserved", "expires_at": time.time() + 120,
            "ownership_epoch": self.store.project(target["project_id"])["seq"] + 1,
        })
        self.store.relate(target["project_id"], target["id"], claim["id"], "WORKS_ON")
        self.store.relate(target["project_id"], claim["id"], task["id"], "WORKS_ON")
        for scope in task["scope_ids"]:
            self.store.relate(target["project_id"], claim["id"], scope, "CLAIMS_SCOPE")
        return claim

    def live_control(self, principal, control_id):
        control = self.store.get(control_id, principal, "control")
        if control["state"] not in ("queued", "acknowledged", "applying"):
            raise Problem("stale_control")
        self.run(principal, control["target_run_id"])
        lease = self.lease(principal, control["project_id"])
        if (not lease or lease["id"] != control["lease_id"] or
                lease["coordinator_epoch"] != control["coordinator_epoch"] or control["expires_at"] <= time.time()):
            raise Problem("stale_control")
        issuer = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                       (self.store.raw(control_id)["owner"],)).fetchone()
        if issuer is None or (issuer["role"] != "admin" and
                              (not issuer["coordinator"] or issuer["agent"] != lease["agent_id"]
                               or issuer["project"] != control["project_id"])):
            raise Problem("stale_control")
        if issuer["role"] != "admin" and getattr(self, "nexus_delegation", None):
            self.nexus_delegation.check_control(control)
        self.store.team_access.require_contribute(self.store.raw(control_id), dict(issuer))
        self.store.team_access.require_contribute(self.store.raw(control["target_run_id"]), principal)
        target = self.run(principal, control["target_run_id"])
        if target["control_revision"] != control["target_run_revision"]:
            raise Problem("revision_conflict", 412)
        return control

    def control_result(self, principal, control_id, request):
        control = self.store.get(control_id, principal, "control")
        self.run(principal, control["target_run_id"])
        if control["state"] in ("applied", "failed", "outcome_unknown"):
            if control.get("result") != request.model_dump():
                raise Problem("idempotency_conflict")
            return control
        self.live_control(principal, control_id)
        if control["state"] not in ("acknowledged", "applying"):
            raise Problem("control_not_acknowledged")
        run = self.run(principal, control["target_run_id"])
        if request.outcome == "applied":
            required = {"pause": "paused", "cancel": "cancelled", "steer": "running",
                        "assign_task": "running", "resume": "running"}[control["action"]]
            if request.observed_status != required or not request.checkpoint_id:
                raise Problem("runtime_evidence_required", 422)
            if control["action"] in ("steer", "assign_task"):
                if control["state"] != "applying" or request.checkpoint_id != control.get("checkpoint_id"):
                    raise Problem("checkpoint_transition_required", 422)
                replacement = self.store.get(control["reserved_claim_id"], principal, "work_claim")
                if replacement["expires_at"] <= time.time():
                    raise Problem("replacement_reservation_expired")
                for claim in self.active_claims(principal, control["project_id"]):
                    if claim["run_id"] == run["id"] and claim["id"] != replacement["id"]:
                        claim["state"] = "released"
                        row = self.store.raw(claim["id"])
                        self.store.save("work_claim", row["project"], row["owner"], claim)
                replacement["state"] = "active"
                row = self.store.raw(replacement["id"])
                self.store.save("work_claim", row["project"], row["owner"], replacement)
                run["task_id"] = control["replacement_task_id"]
                task = self.store.get(run["task_id"], principal, "task")
                task["status"] = "running"
                row = self.store.raw(task["id"])
                self.store.save("task", row["project"], row["owner"], task)
            if control["action"] == "resume":
                if not any(c["run_id"] == run["id"] and c["state"] == "active"
                           for c in self.active_claims(principal, run["project_id"])):
                    raise Problem("active_claim_required")
            run["status"] = request.observed_status
            run["control_revision"] += 1
            row = self.store.raw(run["id"])
            self.store.save("run", row["project"], row["owner"], run)
            if run.get("task_id"):
                self.store.relate(run["project_id"], run["id"], run["task_id"], "WORKS_ON")
            self.store.relate(run["project_id"], control["id"], run["id"], "AFFECTS")
        elif control["reserved_claim_id"]:
            replacement = self.store.get(control["reserved_claim_id"], principal)
            replacement["state"] = "released"
            row = self.store.raw(replacement["id"])
            self.store.save("work_claim", row["project"], row["owner"], replacement)
        control.update(state=request.outcome, result=request.model_dump())
        row = self.store.raw(control_id)
        return self.store.save("control", row["project"], row["owner"], control)

    def checkpoint(self, principal, control_id, request):
        control = self.live_control(principal, control_id)
        if control["state"] != "acknowledged" or control["action"] not in ("steer", "assign_task"):
            raise Problem("checkpoint_transition_not_available")
        control.update(state="applying", checkpoint_id=request.checkpoint_id)
        row = self.store.raw(control_id)
        return self.store.save("control", row["project"], row["owner"], control)

    def task_permit(self, principal, run_id, task):
        from types import SimpleNamespace
        # Scope-less planned work still passes liveness and control fences, but has no file grant.
        return self.permits(principal, run_id, SimpleNamespace(scope_ids=task["scope_ids"], paths=[]))

    def permits(self, principal, run_id, request):
        run = self.run(principal, run_id)
        if run["status"] not in ("running", "idle"):
            raise Problem("mutation_not_permitted", 403)
        pending = [c for c in self.store.list("control", run["project_id"], principal, 1000)
                   if c["target_run_id"] == run_id and c["state"] in ("queued", "acknowledged", "applying")
                   and c["expires_at"] > time.time()]
        transition = next((c for c in pending if c["state"] == "applying"), None)
        if pending and not transition:
            raise Problem("control_pending", 403)
        claims = [c for c in self.active_claims(principal, run["project_id"])
                  if c["run_id"] == run_id and c["state"] == "active" and c["mode"] == "exclusive_owner"]
        if transition:
            self.live_control(principal, transition["id"])
            claim = self.store.get(transition["reserved_claim_id"], principal, "work_claim")
            if claim["state"] != "reserved" or claim["expires_at"] <= time.time():
                raise Problem("replacement_reservation_expired")
            claims = [claim]
        granted = {s for c in claims for s in c["scope_ids"]}
        if not set(request.scope_ids).issubset(granted):
            raise Problem("scope_not_owned", 403)
        if request.paths:
            scopes = [self.reference(principal, run["project_id"], s, "scope") for s in request.scope_ids]
            allowed = [path for scope in scopes for path in scope.get("allowed_paths", [])]
            case_sensitive = self.store.project(run["project_id"])["path_case_sensitive"]
            if not case_sensitive:
                allowed = [path.casefold() for path in allowed]
            paths = request.paths if case_sensitive else [path.casefold() for path in request.paths]
            if any(not any(path == prefix or path.startswith(prefix + "/") for prefix in allowed)
                   for path in paths):
                raise Problem("path_outside_scope", 403)
        return {"permit_id": uid("permit"), "run_id": run_id, "scope_ids": request.scope_ids,
                "expires_at": time.time() + 10, "control_revision": run["control_revision"]}

    def context(self, principal, request):
        project = request.project_id
        self.project_access(principal, project)
        if request.requester_run_id:
            caller_run = self.run(principal, request.requester_run_id)
            if caller_run["project_id"] != project:
                raise Problem("not_found", 404)
        freshness = self.store.freshness(project)
        if request.min_ingestion_seq is not None and (
                freshness["vector_through_ingestion_seq"] < request.min_ingestion_seq):
            raise Problem("index_not_ready", 503)
        selector = request.agent_selector
        all_agents = self.store.list("agent", project, principal, 1000)
        runs = self.store.list("run", project, principal, 1000)
        active = {r["agent_id"] for r in runs if r["status"] not in TERMINAL and
                  time.time() - r["last_seen_at"] <= 30}
        eligible = {a["id"]: a for a in all_agents if not selector.active_only or a["id"] in active}
        explicit = [a for a in selector.agent_ids or [] if a in eligible]
        semantic = []
        warnings = []
        if selector.semantic_query:
            hits = self.indexes.search(principal, m.SearchQuery(
                project_id=project, query=selector.semantic_query, resource_kinds=["agent", "work_claim", "task"],
                limit=50,
            ))
            warnings.extend(hits["warnings"])
            for hit in hits["results"]:
                body = hit["resource"]
                ids = [body["id"]] if hit["kind"] == "agent" else (
                    [body["agent_id"]] if hit["kind"] == "work_claim" else
                    [r["agent_id"] for r in runs if r.get("task_id") == body["id"]])
                semantic.extend(x for x in ids if x in eligible and x not in semantic)
        if selector.match == "intersection":
            selected = [a for a in semantic if a in explicit]
        else:
            selected = explicit + [a for a in semantic if a not in explicit]
        selected = selected[:selector.max_agents]
        candidates = []
        included = set(request.include)
        for agent in selected:
            for run in runs:
                if run["agent_id"] == agent:
                    if "ledger" in included:
                        candidates.append(("ledger", run))
                    if "tasks" in included and run.get("task_id"):
                        candidates.append(("tasks", self.store.get(run["task_id"], principal, "task")))
        for claim in self.active_claims(principal, project):
            if "work_claims" in included and claim["agent_id"] in selected:
                candidates.append(("work_claims", claim))
        for memory in self.store.list("memory", project, principal):
            if request.require_verified_memory and memory["status"] != "verified":
                continue
            if memory["scope_kind"] == "agent" and memory["scope_owner_id"] not in selected:
                continue
            if memory["scope_kind"] == "run" and memory["scope_owner_id"] not in {
                    r["id"] for r in runs if r["agent_id"] in selected}:
                continue
            if memory["scope_kind"] == "task" and memory["scope_owner_id"] not in {
                    r["task_id"] for r in runs if r["agent_id"] in selected}:
                continue
            if "memory" in included:
                candidates.append(("memory", memory))
        if request.query:
            kinds = [kind for include, kind in (("board", "board_post"), ("artifacts", "artifact"), ("tasks", "task"))
                     if include in included]
            hits = self.indexes.search(principal, m.SearchQuery(
                project_id=project, query=request.query, resource_kinds=kinds)) if kinds else {"results": [], "warnings": []}
            candidates.extend(("retrieval", h["resource"]) for h in hits["results"])
            warnings.extend(hits["warnings"])
        if "messages" in included and principal["agent"]:
            candidates.extend(("messages", self.delivery_state(message))
                              for message in self.store.list("message", project, principal)
                              if message["to"] == principal["agent"] and message["expires_at"] > time.time())
        if included & {"controls", "tasks"}:
            lease = self.lease(principal, project)
            for control in self.store.list("control", project, principal):
                if (control["target_agent_id"] not in selected or
                        control["state"] not in ("queued", "acknowledged", "applying") or
                        control["expires_at"] <= time.time() or not lease or control["lease_id"] != lease["id"]):
                    continue
                if "controls" in included:
                    candidates.append(("controls", control))
                if "tasks" in included and control.get("replacement_task_id"):
                    # The run's committed task changes only after replacement work is
                    # observed; expose the pending task as evidence during that interval.
                    candidates.append(("tasks", self.store.get(control["replacement_task_id"], principal, "task")))
        graph_edges = []
        if "graph" in included and selected:
            graph = self.indexes.query_graph(principal, m.GraphQuery(
                project_id=project, seed_ids=selected[:20], max_hops=request.graph_max_hops))
            for node in graph["nodes"]:
                if node["id"] in selected:
                    continue
                if self.store.raw(node["id"])["kind"] == "memory" and request.require_verified_memory and (
                        node["status"] != "verified"):
                    continue
                candidates.append(("graph", node))
            graph_edges = graph["edges"]
            warnings.extend(graph["warnings"])
            if len(selected) > 20:
                warnings.append("Graph expansion bounded to the first 20 selected agents")
            if freshness["graph_pending_jobs"]:
                warnings.append("Graph projection is behind canonical state")
        items, omitted, used = [], [], 0
        seen = set()
        for kind, body in candidates:
            if body["id"] in seen:
                continue
            seen.add(body["id"])
            estimate = len(canonical(body).encode())  # Conservative UTF-8 byte upper bound, not model tokens.
            if used + estimate > request.token_budget:
                omitted.append(body["id"])
                continue
            used += estimate
            items.append({"kind": kind, "resource_id": body["id"], "revision": body["revision"],
                          "trust": body.get("origin", body.get("trust", "structured")),
                          "token_estimate": estimate, "data": body})
        permitted_packet_ids = set(selected) | {item["resource_id"] for item in items}
        packet_edges = []
        for edge in graph_edges:
            cost = len(canonical(edge).encode())
            if edge["source"] in permitted_packet_ids and edge["target"] in permitted_packet_ids and (
                    used + cost <= request.token_budget):
                packet_edges.append(edge)
                used += cost
        return {
            "selected_agents": [{"agent_id": a, "selection_reasons":
                                 (["explicit_id"] if a in explicit else []) +
                                 (["semantic_match"] if a in semantic else [])} for a in selected],
            "items": items, "omitted_resource_ids": omitted, "truncated": bool(omitted),
            "graph_edges": packet_edges,
            "token_estimate": used, "token_estimation": "conservative_utf8_bytes_upper_bound",
            "warnings": sorted(set(warnings)), "freshness": self.store.freshness(project),
        }
