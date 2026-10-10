"""Persistent, opt-in execution loop for one Hivemind agent run.

The worker polls for assignments, messages, and review notifications, heartbeats its
run, renews claims near expiry, and drives the existing durable task lifecycle
(``start``/``checkpoint``) through the ordinary :class:`hivemind.sdk.Client` API.

It launches **no** paid/vendor/model sessions, installs no hooks, and never executes
task text. All execution is delegated to an injected, host-supplied runtime adapter::

    class Runtime:
        def execute(self, task, context, checkpoint):
            ...

``execute`` receives the current task detail, a plain execution context, and a
``checkpoint`` callable it calls to durably record progress. It must return a
structured result dict (``status``, ``summary``, optional ``progress``/``evidence_refs``).
Task titles, descriptions, and any observed text remain untrusted evidence: they are
data handed to the adapter, never code interpreted by this module.

Optional durability: an injected *journal* object is used only by duck typing. If it
provides ``append(entry)`` (or ``record(entry)``) the worker records transitions; if it
provides ``recover(run_id)`` the worker reads a resume hint at construction. No journal
internals are imported or assumed, and ``journal=None`` is fully standalone.

This is a cooperative client-side loop, not an OS sandbox or an exactly-once scheduler.
"""

from __future__ import annotations

import threading
import time

import httpx

from .storage import uid

TERMINAL_STATUSES = ("completed", "failed")
RUNTIME_STATUSES = ("running", "paused", "completed", "failed")
CONTROL_HALTS = ("pause", "cancel", "steer")


class WorkerStopped(Exception):
    """The worker lost its task execution fence or was asked to stop cooperatively."""


def problem_code(error):
    """Best-effort protocol error code from an HTTP failure; never raises."""
    try:
        return (error.response.json() or {}).get("error", {}).get("code")
    except Exception:
        return None


def summary_text(value, fallback="working"):
    text = "" if value is None else str(value)
    return text[:4096] or fallback


def progress_value(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, min(100, number))


class AgentWorker:
    """Bounded, opt-in poll/execute loop bound to one agent and run.

    ``runtime`` is required to execute assignments; without it the loop still polls,
    heartbeats, renews claims, and records a failed task result rather than guessing.
    ``enabled`` defaults to ``False`` so constructing a worker performs no requests.
    """

    def __init__(self, client, project_id, agent_id, run_id, runtime=None, *,
                 journal=None, enabled=False, poll_interval=5.0,
                 claim_renew_margin=60.0, capture_health=None, assemble_context=False,
                 context_token_budget=6000, submit_verification=False, control_boundary=None,
                 renew_message_leases=False, process_corrections=False, context_tokenizer=None,
                 require_exact_context_tokens=False, git_automation=None,
                 messaging_policy=None, messaging_adapter=None):
        if not 0.0 <= poll_interval <= 600:
            raise ValueError("poll_interval must be between 0 and 600 seconds")
        if claim_renew_margin < 0:
            raise ValueError("claim_renew_margin must not be negative")
        self.client = client
        self.project_id = project_id
        self.agent_id = agent_id
        self.run_id = run_id
        self.runtime = runtime
        self.journal = journal
        self.enabled = bool(enabled)
        self.poll_interval = float(poll_interval)
        self.claim_renew_margin = float(claim_renew_margin)
        self.capture_health = capture_health or {"state": "unknown"}
        self.assemble_context = bool(assemble_context)
        self.context_token_budget = context_token_budget
        self.context_tokenizer = context_tokenizer or getattr(runtime, "context_tokenizer_id", None)
        self.require_exact_context_tokens = bool(require_exact_context_tokens)
        self.submit_verification = bool(submit_verification)
        self.control_boundary = control_boundary
        self.renew_message_leases = bool(renew_message_leases)
        self.process_corrections = bool(process_corrections)
        self.handler_journal = None
        if journal is not None and getattr(journal, "journal", None) is not None:
            from .agent_session import HandlerJournal
            self.handler_journal = HandlerJournal(journal.journal, project_id, agent_id)
        self.git_automation = git_automation
        self.messaging = None
        if messaging_policy is not None:
            from .runtime_messaging import RuntimeMessageGateway
            self.messaging = RuntimeMessageGateway(
                client, project_id, agent_id, run_id, adapter=messaging_adapter or runtime,
                policy=messaging_policy, handler_journal=self.handler_journal)
        self._boundary_lock = threading.RLock()
        self.stop_event = threading.Event()
        self.halt_reason = None
        self.dispatches = 0
        self.progress = 0
        self._task_id = None
        self._execution_id = None
        self.resume_hint = self._journal_recover() if self.enabled else None
        self._maintenance_error = None

    # ------------------------------------------------------------------ journal

    def _journal_append(self, entry):
        append = getattr(self.journal, "append", None) or getattr(self.journal, "record", None)
        if callable(append):
            append({**entry, "project_id": self.project_id, "agent_id": self.agent_id,
                    "run_id": self.run_id, "at": time.time()})

    def _journal_recover(self):
        recover = getattr(self.journal, "recover", None)
        if not callable(recover):
            return None
        try:
            hint = recover(self.run_id)
        except TypeError:
            hint = recover()
        return hint if isinstance(hint, dict) else None

    # ------------------------------------------------------------------ control

    def stop(self, reason=None):
        """Request a cooperative stop; an in-flight runtime call is not interrupted."""
        if reason:
            self.halt_reason = reason
        self.stop_event.set()

    @property
    def stopped(self):
        return self.stop_event.is_set()

    def status(self):
        return {"enabled": self.enabled, "stopped": self.stopped, "halt_reason": self.halt_reason,
                "task_id": self._task_id, "execution_id": self._execution_id, "progress": self.progress,
                "resume_hint": self.resume_hint}

    # ------------------------------------------------------------------ polling

    def _heartbeat(self):
        self.client.request("POST", f"runs/{self.run_id}/heartbeat",
                            {"capture_health": self.capture_health})

    def _renew_claims(self):
        now, renewed = time.time(), []
        claims = self.client.request("GET", f"work-claims?project_id={self.project_id}")
        for claim in claims or []:
            if claim.get("run_id") != self.run_id or claim.get("state") != "active":
                continue
            if claim.get("expires_at", 0) - now > self.claim_renew_margin:
                continue
            self.client.request("POST", f"work-claims/{claim['id']}:renew",
                                {"expected_revision": claim["revision"]},
                                key=f"renew-{claim['id']}-{claim['revision']}")
            renewed.append(claim["id"])
        return renewed

    def _poll_messages(self):
        handler = (self.messaging.message_handler if self.messaging is not None else
                   getattr(self.runtime, "handle_message", None) if self.runtime else None)
        if not callable(handler):
            # Do not claim/ack messages nothing can act on: leave them for a real processor.
            return 0

        def receive(receipt):
            from .recovery import UnknownOutcome
            def apply():
                return handler(receipt)
            def invoke():
                try:
                    if self.handler_journal and self.messaging is None:
                        return self.handler_journal.handle("message", receipt["id"], apply)
                    return apply()
                except UnknownOutcome:
                    self.stop("handler_outcome_unknown")
                    raise
            if not self.renew_message_leases:
                return invoke()
            stop, errors = threading.Event(), []
            def maintain():
                while not stop.wait(min(5.0, max(0.1, self.poll_interval))):
                    try:
                        self._heartbeat()
                        self._renew_claims()
                        if self._pending_control() is not None:
                            raise WorkerStopped("message_control_pending")
                        self.client.renew_message_lease(receipt["id"], self.run_id, receipt["delivery_id"])
                    except Exception as error:
                        errors.append(type(error).__name__)
                        self.stop("message_maintenance_unknown")
                        return
            thread = threading.Thread(target=maintain, daemon=True, name="hivemind-message-maintenance")
            thread.start()
            try:
                invoke()
            finally:
                stop.set()
                thread.join(timeout=20)
            if errors or thread.is_alive():
                raise WorkerStopped("message_processing_outcome_unknown")

        options = {"should_stop": self.stop_event.is_set} if self.renew_message_leases else {}
        return len(self.client.deliver_inbox(self.agent_id, self.project_id, self.run_id, receive, **options) or [])

    def _poll_notifications(self):
        notices = self.client.notifications(self.agent_id, self.project_id) or []
        handler = (self.messaging.notification_handler if self.messaging is not None else
                   getattr(self.runtime, "review_notification", None) if self.runtime else None)
        if not callable(handler):
            # Leave review notices unacknowledged until something can actually handle them.
            return 0
        handled = 0
        for notice in notices:
            from .recovery import UnknownOutcome
            try:
                if self.handler_journal and self.messaging is None:
                    self.handler_journal.handle("notification", notice["id"], lambda item=notice: handler(item))
                else:
                    handler(notice)
            except UnknownOutcome:
                self.stop("handler_outcome_unknown")
                raise
            except PermissionError:
                if self.messaging is None:
                    raise
                continue
            self.client.ack_notification(notice["id"], key=f"ack-{notice['id']}")
            handled += 1
        return handled

    def _pending_control(self):
        if self.control_boundary is not None:
            with self._boundary_lock:
                state = self.control_boundary.boundary()
            if state.get("cancelled") or state.get("paused") or state.get("unresolved"):
                return {"action": "cancel" if state.get("cancelled") else "pause"}
        controls = self.client.request(
            "GET", f"agents/{self.agent_id}/controls?project_id={self.project_id}")
        for control in controls or []:
            if control.get("target_run_id") not in (None, self.run_id):
                continue
            if control.get("action") in CONTROL_HALTS:
                return control
        return None

    def _dispatch(self):
        """Ask the scheduler for an assignment. Absent scheduler surface means idle."""
        self.dispatches += 1
        try:
            data = self.client.request("POST", f"projects/{self.project_id}/tasks:dispatch",
                                       {"agent_id": self.agent_id, "run_id": self.run_id})
        except httpx.HTTPStatusError as error:
            if error.response is not None and error.response.status_code in (404, 405, 501):
                return None, None
            raise
        if not isinstance(data, dict) or not data.get("task"):
            return None, None
        return data["task"], data.get("claim")

    # ------------------------------------------------------------------ execution

    def _claim_for(self, task):
        scopes = set(task.get("scope_ids") or [])
        owned, candidates = set(), []
        for claim in self.client.request("GET", f"work-claims?project_id={self.project_id}") or []:
            if (claim.get("run_id") == self.run_id and claim.get("state") == "active"
                    and claim.get("mode") == "exclusive_owner"
                    and claim.get("expires_at", 0) > time.time()):
                owned.update(claim.get("scope_ids") or [])
                candidates.append(claim)
        if candidates and scopes.issubset(owned):
            return {**candidates[0], "scope_ids": sorted(owned), "claim_ids": [item["id"] for item in candidates]}
        return None

    def _context(self, task, claim, resumed):
        execution = task.get("execution") or {}
        context = {
            "project_id": self.project_id,
            "agent_id": self.agent_id,
            "run_id": self.run_id,
            "task_id": task["id"],
            "execution_id": execution.get("id"),
            "title": task.get("title"),
            "description": task.get("description", ""),
            "scope_ids": list(task.get("scope_ids") or []),
            "handoff": task.get("handoff"),
            "claim_id": (claim or {}).get("id"),
            "claim_ids": (claim or {}).get("claim_ids") or ([claim["id"]] if claim else []),
            "resumed": bool(resumed),
            "progress": int(execution.get("progress") or 0),
            "poll_interval": self.poll_interval,
            "should_stop": self.stop_event.is_set,
        }
        if self.assemble_context:
            context["packet"] = self.client.assemble_context({
                "project_id": self.project_id, "agent_selector": {"agent_ids": [self.agent_id]},
                "requester_run_id": self.run_id, "query": str(task.get("description") or task["title"])[:4096],
                "token_budget": self.context_token_budget,
                "target_tokenizer": self.context_tokenizer,
            })
            if (self.require_exact_context_tokens
                    and not context["packet"]["tokenization"]["exact_for_target_tokenizer"]):
                raise WorkerStopped("runtime_tokenizer_not_configured")
        return context

    def _make_checkpoint(self, task_id, execution_id):
        def checkpoint(summary, progress=None, status="running", evidence_refs=None):
            if self._maintenance_error is not None:
                raise WorkerStopped(self._maintenance_error)
            self._heartbeat()
            self._renew_claims()
            control = self._pending_control()
            if control is not None:
                raise WorkerStopped(f"control:{control.get('action')}")
            if status not in RUNTIME_STATUSES:
                status = "running"
            if status == "running" and self.stop_event.is_set():
                status = "paused"
            return self._commit(task_id, execution_id, {
                "status": status, "summary": summary, "progress": progress,
                "evidence_refs": evidence_refs})
        return checkpoint

    @staticmethod
    def _normalize(result):
        if isinstance(result, str):
            return {"status": "completed", "summary": result, "progress": 100}
        if not isinstance(result, dict):
            return {"status": "failed", "progress": None,
                    "summary": "Runtime adapter returned an unsupported result type"}
        status = str(result.get("status") or "").lower()
        if status not in RUNTIME_STATUSES:
            status = "completed" if result.get("ok") else "failed"
        return {"status": status, "summary": result.get("summary") or status,
                "progress": result.get("progress"), "evidence_refs": result.get("evidence_refs"),
                "test_evidence": result.get("test_evidence") or []}

    def _commit(self, task_id, execution_id, final):
        """GET current state, then record one fenced checkpoint. Never trusts stale state."""
        task = self.client.task(task_id)
        execution = task.get("execution") or {}
        if execution.get("id") != execution_id or execution.get("run_id") != self.run_id:
            raise WorkerStopped("task execution is no longer owned by this run")
        current = int(execution.get("progress") or 0)
        status = final["status"]
        progress = progress_value(final.get("progress"))
        if status == "completed":
            progress = 100
        elif progress is None:
            progress = current
        else:
            progress = max(progress, current)
        checkpoint_id = uid("checkpoint")
        body = {"expected_revision": task["revision"], "run_id": self.run_id,
                "execution_id": execution_id, "checkpoint_id": checkpoint_id, "status": status,
                "progress": progress, "summary": summary_text(final.get("summary")),
                "evidence_refs": list(final.get("evidence_refs") or [])}
        self._journal_append({"kind": "checkpoint", "task_id": task_id, "execution_id": execution_id,
                              "checkpoint_id": checkpoint_id, "status": status, "progress": progress})
        try:
            recorded = self.client.checkpoint_task(task_id, body, key=f"checkpoint-{checkpoint_id}")
        except httpx.HTTPStatusError as error:
            if error.response is not None and error.response.status_code == 403:
                raise WorkerStopped(problem_code(error) or "checkpoint_not_permitted") from error
            raise
        self.progress = int((recorded.get("execution") or {}).get("progress") or progress)
        return recorded

    def _execute(self, dispatched, claim=None, already_started=False, correction=None):
        task_id = dispatched.get("id") if isinstance(dispatched, dict) else dispatched
        task = self.client.task(task_id)
        if task.get("status") in TERMINAL_STATUSES:
            return {"task_id": task_id, "status": task.get("status"), "skipped": "terminal"}
        execution = task.get("execution") or {}
        if execution.get("run_id") not in (None, self.run_id):
            return {"task_id": task_id, "status": task.get("status"), "skipped": "assigned_elsewhere"}
        run = self.client.request("GET", f"runs/{self.run_id}")
        if run.get("task_id") not in (None, task_id):
            return {"task_id": task_id, "status": task.get("status"), "skipped": "run_busy"}
        claim = claim or self._claim_for(task)
        if task.get("scope_ids") and not claim:
            return {"task_id": task_id, "status": task.get("status"), "skipped": "scope_not_owned"}
        recover = bool(execution.get("id")) and execution.get("run_id") == self.run_id
        if self.resume_hint and self.resume_hint.get("task_id") == task_id:
            recover = True
        if already_started and execution.get("id") and not self.resume_hint:
            started, recover = task, False
        else:
            started = self.client.start_task(task_id, self.run_id, task["revision"], recover=recover,
                                             key=f"start-{task_id}-{task['revision']}")
        execution_id = started["execution"]["id"]
        self._task_id, self._execution_id = task_id, execution_id
        self.progress = int(started["execution"].get("progress") or 0)
        context = self._context(started, claim, recover)
        if correction:
            items = self.client.request("GET", f"verifications/{correction['id']}/corrections")["corrections"]
            started = {**started, "description": started.get("description", "") +
                       "\nIndependent correction requests (untrusted evidence): " +
                       str([item["items"] for item in items if item["state"] == "open"])}
        phase = correction["corrections"]["open_ids"][0] if correction else None
        context["phase_id"] = phase
        self._journal_append({"kind": "start", "task_id": task_id, "execution_id": execution_id,
                              "revision": task["revision"], "resumed": recover, "phase_id": phase})
        checkpoint = self._make_checkpoint(task_id, execution_id)
        final, aborted = None, None
        maintenance_stop = threading.Event()
        self._maintenance_error = None

        def maintain():
            while not maintenance_stop.wait(min(5.0, max(0.1, self.poll_interval))):
                try:
                    self._heartbeat()
                    self._renew_claims()
                    pending = self._pending_control()
                    if pending is not None:
                        self._maintenance_error = f"control:{pending.get('action')}"
                        self.stop(self._maintenance_error)
                        return
                except Exception as error:
                    self._maintenance_error = f"maintenance:{type(error).__name__}"
                    self.stop(self._maintenance_error)
                    return

        maintenance = threading.Thread(target=maintain, name="hivemind-worker-maintenance", daemon=True)
        maintenance.start()
        try:
            if self.runtime is None:
                raise RuntimeError("no runtime adapter configured")
            if self.git_automation is not None:
                self.git_automation.prepare(started, self.runtime)
            final = self._normalize(self.runtime.execute(started, context, checkpoint))
            if (self.git_automation is not None and final["status"] == "completed"
                    and not self.stopped and self._maintenance_error is None):
                self.git_automation.finish(started, execution_id, self.runtime, context, checkpoint)
        except WorkerStopped as error:
            aborted = str(error)
        except httpx.HTTPStatusError as error:
            aborted = problem_code(error) or f"http_{error.response.status_code}"
        except Exception as error:  # host adapter failed: report a failure, never a silent success
            if self.journal is not None:
                aborted = f"runtime_outcome_unknown:{type(error).__name__}"
            else:
                final = {"status": "failed", "progress": self.progress,
                         "summary": f"Runtime adapter raised {type(error).__name__}"}
        finally:
            maintenance_stop.set()
            maintenance.join(timeout=20)
            if maintenance.is_alive():
                aborted = "maintenance_shutdown_pending"
            elif self._maintenance_error is not None:
                aborted = self._maintenance_error
        if aborted is not None:
            self.stop(self.halt_reason or aborted)
            return {"task_id": task_id, "status": "aborted", "error": aborted}
        if final is None:
            final = {"status": "paused" if self.stopped else "failed", "progress": self.progress,
                     "summary": self.halt_reason or "Runtime adapter returned no result"}
        if self.stopped and final["status"] == "running":
            final["status"] = "paused"
        try:
            scheduled = (started.get("scheduling") or {}).get("state") == "dispatched"
            terminal = final["status"] in ("completed", "failed")
            recorded = self._commit(task_id, execution_id,
                                    {**final, "status": "running"} if scheduled and terminal else final)
            if correction and final["status"] == "completed":
                refs = list(final.get("evidence_refs") or [])
                for identifier in correction["corrections"]["open_ids"]:
                    current = self.client.request("GET", f"corrections/{identifier}")
                    self.client.request("POST", f"corrections/{identifier}:resolve", {
                        "expected_revision": current["revision"], "run_id": self.run_id,
                        "execution_id": execution_id, "summary": summary_text(final.get("summary")),
                        "artifact_refs": refs}, key=f"correction-{identifier}-{current['revision']}")
                current = self.client.verification(correction["id"])
                self.client.update_verification(correction["id"], "update", {
                    "expected_revision": current["revision"], "task_id": task_id, "run_id": self.run_id,
                    "execution_id": execution_id, "completion_claimed": True,
                    "summary": summary_text(final.get("summary")), "artifact_refs": refs,
                    "test_evidence": final.get("test_evidence") or []},
                    key=f"resubmit-{correction['id']}-{current['revision']}")
            elif self.submit_verification and final["status"] == "completed":
                self.client.submit_verification({
                    "task_id": task_id, "run_id": self.run_id, "execution_id": execution_id,
                    "completion_claimed": True, "summary": summary_text(final.get("summary")),
                    "artifact_refs": list(final.get("evidence_refs") or []),
                    "test_evidence": final.get("test_evidence") or [],
                }, key=f"verification-{task_id}-{execution_id}")
            if scheduled and terminal:
                recorded = self.client.request("POST", f"tasks/{task_id}:dispatch-outcome", {
                    "expected_revision": recorded["revision"], "agent_id": self.agent_id,
                    "run_id": self.run_id, "execution_id": execution_id,
                    "outcome": "succeeded" if final["status"] == "completed" else "failed",
                    "summary": summary_text(final.get("summary")),
                    "evidence_refs": list(final.get("evidence_refs") or []),
                }, key=f"outcome-{task_id}-{execution_id}-{recorded['revision']}")
        except WorkerStopped as error:
            self.stop(self.halt_reason or str(error))
            return {"task_id": task_id, "status": "aborted", "error": self.halt_reason}
        except httpx.HTTPStatusError as error:
            self.stop(problem_code(error) or f"http_{error.response.status_code}")
            return {"task_id": task_id, "status": "aborted", "error": self.halt_reason}
        self._task_id = None if recorded.get("status") in TERMINAL_STATUSES else task_id
        self._journal_append({"kind": "result", "task_id": task_id, "execution_id": execution_id,
                              "status": recorded.get("status"), "progress": self.progress, "phase_id": phase})
        return {"task_id": task_id, "status": recorded.get("status"), "progress": self.progress}

    # ------------------------------------------------------------------ loop

    def tick(self):
        """Run exactly one bounded poll cycle and return a structured report."""
        report = {"enabled": self.enabled, "at": time.time(), "heartbeat": False, "renewed": [],
                  "messages": 0, "notifications": 0, "control": None, "task_id": None,
                  "status": None, "skipped": None, "stopped": self.stopped}
        if not self.enabled:
            return report
        self._heartbeat()
        report["heartbeat"] = True
        report["renewed"] = self._renew_claims()
        control = self._pending_control()
        if control is not None:
            self.halt_reason = self.halt_reason or f"control:{control.get('action')}"
            report["control"] = control.get("action")
            report["stopped"] = self.stopped
            return report
        if self.stopped:
            return report
        report["messages"] = self._poll_messages()
        if self.stopped:
            report["stopped"] = True
            return report
        report["notifications"] = self._poll_notifications()
        run = self.client.request("GET", f"runs/{self.run_id}")
        if self.process_corrections and not run.get("task_id"):
            items = self.client.request("GET",
                f"verifications?project_id={self.project_id}&status=needs_corrections")["verifications"]
            for item in items:
                if item["agent_id"] != self.agent_id or item["run_id"] != self.run_id:
                    continue
                if any((item.get("automation_attempts") or {}).get(identifier, 0) >= 3
                       for identifier in item["corrections"]["open_ids"]):
                    report["skipped"] = "correction_attempt_budget_exhausted"
                    continue
                task = self.client.request("POST", f"verifications/{item['id']}:begin-correction", {
                    "expected_revision": item["revision"], "run_id": self.run_id},
                    key=f"begin-correction-{item['id']}-{item['revision']}")
                report.update(self._execute(task, already_started=True, correction=item))
                return report
        if run.get("task_id"):
            task, claim = self.client.task(run["task_id"]), None
        else:
            task, claim = self._dispatch()
        if not task:
            return report
        outcome = self._execute(task, claim, already_started=not run.get("task_id"))
        report.update({key: outcome.get(key) for key in ("task_id", "status", "skipped", "error", "progress")
                       if key in outcome})
        report["stopped"] = self.stopped
        return report

    def run(self, iterations=None, poll_interval=None):
        """Run bounded cycles (``iterations`` given) or until :meth:`stop` is called."""
        interval = self.poll_interval if poll_interval is None else float(poll_interval)
        reports, count = [], 0
        while not self.stopped:
            if iterations is not None and count >= iterations:
                break
            count += 1
            try:
                reports.append(self.tick())
            except httpx.HTTPError as error:
                reports.append({"enabled": self.enabled, "error": type(error).__name__, "retryable": True})
            except (WorkerStopped, OSError, ValueError) as error:
                reports.append({"enabled": self.enabled, "error": type(error).__name__})
            except RuntimeError as error:
                self.stop(type(error).__name__)
                reports.append({"enabled": self.enabled, "error": type(error).__name__, "retryable": False})
            reports = reports[-1000:]
            if iterations is None and interval > 0 and not self.stopped:
                self.stop_event.wait(interval)
        return {"ticks": count, "stopped": self.stopped, "halt_reason": self.halt_reason,
                "reports": reports, "reports_truncated": count > len(reports)}
