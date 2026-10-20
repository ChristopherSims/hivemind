"""Bounded task scheduling: plans, dependencies, priority, capability matching, attempts.

The scheduling layer stores its own state without changing core models:

``task_plan``
    A group of related tasks plus the plan-local dependency keys that produced them.
``agent_capability``
    Optional explicit capability metadata for one agent, additive to that agent's
    existing ``supported_actions``.
``task``
    Planned tasks gain ``priority``, ``max_attempts``, ``required_capabilities``,
    ``depends_on``, ``plan_id``/``plan_key``/``plan_order``, and a ``scheduling``
    metadata block. Every core task field keeps its existing meaning.

Nothing here executes agent work, starts paid runs, releases claims, or verifies
results. Dispatch selects at most one eligible queued task for one live run, acquires
the task's exact scopes through ``Service.claim``, and starts the existing durable task
execution through ``Service.task_start`` so checkpoints and permits keep working.
Attempt budgets live in ``scheduling.attempts`` and remain independent of work claims.
Completion and failure reports are agent self-reports, not verification.
"""

import json
import time
from typing import Annotated, Literal

from fastapi import Depends, Header
from fastapi.responses import JSONResponse
from pydantic import Field, field_validator, model_validator

from . import models as m
from .models import ID
from .service import TERMINAL
from .storage import Problem, now, uid

DEFAULT_PRIORITY = 0
MAX_PRIORITY = 1000
DEFAULT_MAX_ATTEMPTS = 3
MAX_ATTEMPTS_LIMIT = 10
BACKOFF_BASE_SECONDS = 5
BACKOFF_MAX_SECONDS = 300
STALE_EXECUTION_SECONDS = 30
UNPLANNED_ORDER = 1000000
HISTORY_LIMIT = 20

Capability = Annotated[str, Field(min_length=1, max_length=64)]

MODELS = ("PlannedTask", "TaskPlanCreate", "TaskDispatch", "TaskDispatchOutcome",
          "TaskReconcile", "AgentCapabilities", "TaskHandoff")


def scheduling_defaults():
    return {"state": "queued", "attempts": 0, "dispatches": 0, "reassignments": 0,
            "assigned_agent_id": None, "last_assigned_agent_id": None,
            "last_dispatch_run_id": None, "last_dispatch_at": None, "retry_after": None,
            "escalated": False, "escalation": None, "last_outcome": None,
            "last_reconcile": None, "attempt_history": []}


def as_int(value, default):
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def as_number(value):
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def backoff_seconds(attempts):
    """Deterministic exponential backoff; jitter is omitted so arbitration is reproducible."""
    return min(BACKOFF_BASE_SECONDS * 2 ** max(0, attempts - 1), BACKOFF_MAX_SECONDS)


def topological_order(steps):
    """Stable topological order of plan steps; raises when dependencies contain a cycle."""
    remaining, emitted, order = list(steps), set(), []
    while remaining:
        progressed = False
        for step in list(remaining):
            if all(dependency in emitted for dependency in step.depends_on):
                emitted.add(step.key)
                order.append(step)
                remaining.remove(step)
                progressed = True
        if not progressed:
            raise ValueError("Task plan dependencies must not contain cycles")
    return order


def strip_text(value):
    return value.strip() if isinstance(value, str) else value


def strip_list(value):
    if isinstance(value, list):
        return [item.strip() if isinstance(item, str) else item for item in value]
    return value


class PlannedTask(m.Model):
    key: str = Field(min_length=1, max_length=128)
    title: str = Field(min_length=1, max_length=512)
    description: str = Field(default="", max_length=65536)
    scope_ids: list[ID] = Field(default_factory=list, max_length=32)
    depends_on: list[str] = Field(default_factory=list, max_length=64)
    external_dependency_ids: list[ID] = Field(default_factory=list, max_length=64)
    priority: int = Field(default=DEFAULT_PRIORITY, ge=0, le=MAX_PRIORITY)
    required_capabilities: list[Capability] = Field(default_factory=list, max_length=32)
    max_attempts: int = Field(default=DEFAULT_MAX_ATTEMPTS, ge=1, le=MAX_ATTEMPTS_LIMIT)
    require_verified_dependencies: bool = False
    routing: m.TaskRoutingRequirements | None = None

    _clean_key = field_validator("key", mode="before")(strip_text)
    _clean_title = field_validator("title", mode="before")(strip_text)
    _clean_dependencies = field_validator("depends_on", mode="before")(strip_list)
    _clean_capabilities = field_validator("required_capabilities", mode="before")(strip_list)

    @model_validator(mode="after")
    def consistent(self):
        if len(self.depends_on) + len(self.external_dependency_ids) > 64:
            raise ValueError("Combined task dependencies must not exceed 64")
        if self.key in self.depends_on:
            raise ValueError("A plan task cannot depend on itself")
        for values, label in ((self.depends_on, "Dependencies"),
                              (self.external_dependency_ids, "External dependencies"),
                              (self.scope_ids, "Scope IDs"),
                              (self.required_capabilities, "Required capabilities")):
            if len(set(values)) != len(values):
                raise ValueError(f"{label} must be unique per task")
        return self


class TaskPlanCreate(m.Model):
    project_id: ID
    title: str = Field(min_length=1, max_length=512)
    description: str = Field(default="", max_length=65536)
    tasks: list[PlannedTask] = Field(min_length=1, max_length=64)

    _clean_title = field_validator("title", mode="before")(strip_text)

    @model_validator(mode="after")
    def resolvable(self):
        keys = [step.key for step in self.tasks]
        if len(set(keys)) != len(keys):
            raise ValueError("Plan task keys must be unique")
        known = set(keys)
        for step in self.tasks:
            if any(dependency not in known for dependency in step.depends_on):
                raise ValueError("Dependencies must reference keys in the same plan")
        topological_order(self.tasks)
        return self


class TaskDispatch(m.Model):
    """Worker contract: the caller claims work for one of its own live runs."""

    agent_id: ID
    run_id: ID


class TaskDispatchOutcome(m.Model):
    """Agent self-report of how a dispatched attempt ended; never treated as verification."""

    expected_revision: int = Field(ge=1)
    agent_id: ID
    run_id: ID
    execution_id: ID | None = None
    outcome: Literal["succeeded", "failed"]
    summary: str = Field(min_length=1, max_length=4096)
    evidence_refs: list[m.TaskEvidenceRef] = Field(default_factory=list, max_length=32)

    _clean_summary = field_validator("summary", mode="before")(strip_text)


class TaskReconcile(m.Model):
    """Explicit recovery for terminal or stale scheduling state; never automatic."""

    expected_revision: int = Field(ge=1)
    action: Literal["requeue", "reassign", "escalate"]
    reason: str = Field(min_length=1, max_length=4096)

    _clean_reason = field_validator("reason", mode="before")(strip_text)


class AgentCapabilities(m.Model):
    capabilities: list[Capability] = Field(default_factory=list, max_length=32)


    _clean_capabilities = field_validator("capabilities", mode="before")(strip_list)

    @model_validator(mode="after")
    def unique_capabilities(self):
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("Capabilities must be unique")
        return self


class TaskHandoff(m.Model):
    expected_revision: int = Field(ge=1)
    target_agent_id: ID
    target_run_id: ID | None = None
    reason: str = Field(min_length=1, max_length=4096)
    _clean_reason = field_validator("reason", mode="before")(strip_text)


class Scheduling:
    """Domain logic for task scheduling; call mutations inside ``Service.mutate``."""

    def __init__(self, service):
        self.service = service
        self.store = service.store

    # ---- metadata helpers -------------------------------------------------

    def _body(self, resource_id, kind):
        row = self.store.raw(resource_id, kind)
        return json.loads(row["body"])

    def _save(self, kind, body, expected):
        row = self.store.raw(body["id"], kind)
        return self.store.save(kind, row["project"], row["owner"], body, expected=expected)

    def task(self, principal, task_id):
        """Read a task through the core visibility rules."""
        return self.store.get(task_id, principal, "task")

    @staticmethod
    def _settings(task):
        stored = task.get("scheduling")
        settings = scheduling_defaults()
        settings.update(stored if isinstance(stored, dict) else {})
        settings["attempts"] = as_int(settings.get("attempts"), 0)
        settings["dispatches"] = as_int(settings.get("dispatches"), 0)
        settings["reassignments"] = as_int(settings.get("reassignments"), 0)
        history = settings.get("attempt_history")
        settings["attempt_history"] = [item for item in history if isinstance(item, dict)][-HISTORY_LIMIT:] \
            if isinstance(history, list) else []
        return settings

    @staticmethod
    def _max_attempts(task):
        return min(max(as_int(task.get("max_attempts"), DEFAULT_MAX_ATTEMPTS), 1), MAX_ATTEMPTS_LIMIT)

    @staticmethod
    def _priority(task):
        return as_int(task.get("priority"), DEFAULT_PRIORITY)

    @staticmethod
    def _dependencies(task):
        return [dependency for dependency in task.get("depends_on") or [] if isinstance(dependency, str)]

    @staticmethod
    def _required(task):
        return {capability for capability in task.get("required_capabilities") or []
                if isinstance(capability, str)}

    @staticmethod
    def _history(task):
        return [item for item in task.get("execution_history") or [] if isinstance(item, dict)]

    def _order_key(self, task):
        return (-self._priority(task), as_int(task.get("plan_order"), UNPLANNED_ORDER),
                as_int(task.get("ingestion_seq"), 0), str(task["id"]))

    # ---- capabilities -----------------------------------------------------

    def _capability_record(self, principal, project_id, agent_id):
        for record in self.store.list("agent_capability", project_id, principal, 1000):
            if record.get("agent_id") == agent_id:
                return record
        return None

    def capability_set(self, principal, agent_id):
        agent = self.store.get(agent_id, principal, "agent")
        values = {action for action in agent.get("supported_actions") or [] if isinstance(action, str)}
        record = self._capability_record(principal, agent["project_id"], agent_id)
        if record:
            values |= {item for item in record.get("capabilities") or [] if isinstance(item, str)}
        else:
            values |= set(agent.get("capabilities") or [])
        return values

    def set_capabilities(self, principal, agent_id, request):
        # Self-registration for the caller's own agent, or an administrator acting for it.
        self.service.represent(principal, agent_id)
        agent = self.store.get(agent_id, principal, "agent")
        record = self._capability_record(principal, agent["project_id"], agent_id)
        if record is None:
            return self.service.create(principal, "agent_capability", {
                "project_id": agent["project_id"], "agent_id": agent_id,
                "capabilities": list(request.capabilities), "updated_by": principal["agent"]})
        body = self._body(record["id"], "agent_capability")
        body.update(capabilities=list(request.capabilities), updated_by=principal["agent"])
        return self._save("agent_capability", body, expected=record["revision"])

    def capabilities_view(self, principal, agent_id):
        agent = self.store.get(agent_id, principal, "agent")
        record = self._capability_record(principal, agent["project_id"], agent_id)
        return {"agent_id": agent_id, "project_id": agent["project_id"],
                "supported_actions": list(agent.get("supported_actions") or []),
                "capabilities": list((record or {}).get("capabilities") or []),
                "effective": sorted(self.capability_set(principal, agent_id))}

    # ---- plans ------------------------------------------------------------

    def create_plan(self, principal, request):
        self.service.project_access(principal, request.project_id)
        order = topological_order(request.tasks)
        for step in order:
            for scope in step.scope_ids:
                self.service.reference(principal, request.project_id, scope, "scope")
            for dependency in step.external_dependency_ids:
                self.service.reference(principal, request.project_id, dependency, "task")
        plan_id = uid("task_plan")
        identifiers = {step.key: uid("task") for step in order}
        created = {}
        for index, step in enumerate(order):
            task = self.service.create(principal, "task", {
                "id": identifiers[step.key],
                "project_id": request.project_id,
                "title": step.title,
                "description": step.description,
                "scope_ids": list(step.scope_ids),
                "status": "queued",
                "priority": step.priority,
                "max_attempts": step.max_attempts,
                "required_capabilities": list(step.required_capabilities),
                "plan_id": plan_id,
                "plan_key": step.key,
                "plan_order": index,
                "depends_on": [identifiers[key] for key in step.depends_on] + list(step.external_dependency_ids),
                "require_verified_dependencies": step.require_verified_dependencies,
                **({"routing": step.routing.model_dump()} if step.routing is not None else {}),
                "scheduling": scheduling_defaults(),
            })
            for scope in step.scope_ids:
                self.store.relate(request.project_id, task["id"], scope, "WORKS_ON")
            for dependency in task["depends_on"]:
                self.store.relate(request.project_id, task["id"], dependency, "DEPENDS_ON")
            created[step.key] = task
        plan = self.service.create(principal, "task_plan", {
            "id": plan_id, "project_id": request.project_id, "title": request.title,
            "description": request.description,
            "task_ids": [created[step.key]["id"] for step in order],
            "created_by_agent": principal["agent"]})
        return self._plan_view(principal, plan)

    def plans(self, principal, project_id):
        self.service.project_access(principal, project_id)
        # Bounded plan summaries; read one plan for its task list.
        return self.store.list("task_plan", project_id, principal, 200)

    def plan(self, principal, plan_id):
        return self._plan_view(principal, self.store.get(plan_id, principal, "task_plan"))

    def plan_tasks(self, principal, plan_id):
        return self.plan(principal, plan_id)["tasks"]

    def _plan_view(self, principal, plan):
        tasks = []
        for task_id in plan.get("task_ids") or []:
            try:
                task = self.store.get(task_id, principal, "task")
            except Problem:
                continue
            dependencies_met = self._dependencies_met(principal, task)
            tasks.append({**task, "dependencies_met": dependencies_met,
                          "dispatchable": dependencies_met and self._dispatchable(task)})
        return {"plan": plan, "tasks": tasks}

    # ---- eligibility ------------------------------------------------------

    def _dependencies_met(self, principal, task):
        for dependency in self._dependencies(task):
            try:
                current = self.store.get(dependency, principal, "task")
                if current["status"] != "completed":
                    return False
                for gate in getattr(self.service, "task_dependency_gates", ()):
                    verdict = gate(principal, task, current)
                    if verdict is not None:
                        if not isinstance(verdict, dict) or verdict.get("action") not in ("allow", "defer"):
                            raise Problem("invalid_dependency_gate", 503)
                        if verdict["action"] == "defer":
                            return False
                if task.get("require_verified_dependencies"):
                    execution = current.get("execution") or {}
                    if not execution.get("id"):
                        return False
                    row = self.store.db.execute(
                        "SELECT id FROM resources WHERE kind='verification' AND project=? AND deleted=0 "
                        "AND json_extract(body,'$.task_id')=? AND json_extract(body,'$.execution_id')=? "
                        "ORDER BY seq DESC LIMIT 1",
                        (task["project_id"], dependency, execution["id"])).fetchone()
                    if not row:
                        return False
                    from .verification import Verifications
                    verification = Verifications(self.service).get(principal, row["id"])
                    if (not verification["independently_verified"]
                            or verification["reviewer_agent_id"] == execution.get("agent_id")
                            or verification["task_snapshot"].get("checkpoint_id")
                            != (execution.get("checkpoint") or {}).get("id")):
                        return False
            except Problem:
                return False
        return True

    def _dispatchable(self, task):
        """Agent-independent scheduling gates: state, budget, backoff."""
        settings = self._settings(task)
        if task.get("status") != "queued" or settings.get("escalated"):
            return False
        if settings["attempts"] >= self._max_attempts(task):
            return False
        retry_after = as_number(settings.get("retry_after"))
        return retry_after is None or retry_after <= time.time()

    def _eligible(self, principal, task, capabilities, agent_id):
        if not self._dispatchable(task):
            return False
        assigned = self._settings(task).get("assigned_agent_id")
        if assigned and assigned != agent_id:
            return False
        if not self._required(task).issubset(capabilities):
            return False
        return self._dependencies_met(principal, task)

    def _candidate(self, principal, project_id, agent_id, capabilities, run_id=None):
        tasks = self.store.list("task", project_id, principal, 1000)
        eligible = [task for task in tasks if self._eligible(principal, task, capabilities, agent_id)
                    and (task.get("handoff") or {}).get("target_run_id") in (None, run_id)]
        return sorted(eligible, key=self._order_key)[0] if eligible else None

    # ---- dispatch ---------------------------------------------------------

    def _acquire(self, principal, task, run):
        scope_ids = [scope for scope in task.get("scope_ids") or [] if isinstance(scope, str)]
        if not scope_ids:
            return None
        wanted = set(scope_ids)
        for claim in self.service.active_claims(principal, task["project_id"]):
            if (claim["run_id"] == run["id"] and claim["mode"] == "exclusive_owner"
                    and set(claim["scope_ids"]) == wanted):
                return claim
        return self.service.claim(principal, m.ClaimCreate(
            project_id=task["project_id"], agent_id=run["agent_id"], run_id=run["id"],
            task_id=task["id"], scope_ids=scope_ids, mode="exclusive_owner",
            intent=f"Dispatch scheduled task {task['id']}"))

    def dispatch(self, principal, project_id, request):
        """Select at most one eligible task for one live run and start it."""
        return self._dispatch(principal, project_id, request)

    def dispatch_exact(self, principal, project_id, request, task_id):
        """Dispatch only this task, with the same authority, gates and claim fences.

        Call inside Service.mutate, as for dispatch. Ineligible work returns no
        assignment, never a different project task.
        """
        return self._dispatch(principal, project_id, request, task_id=task_id)

    def _dispatch(self, principal, project_id, request, task_id=None):
        self.service.project_access(principal, project_id)
        self.service.represent(principal, request.agent_id)
        run = self.service.run(principal, request.run_id)
        if run["agent_id"] != request.agent_id or run["project_id"] != project_id:
            raise Problem("not_found", 404)
        if run["status"] not in ("running", "idle"):
            raise Problem("run_not_ready")
        if run.get("task_id"):
            return {"task": None, "claim": None}
        capabilities = self.capability_set(principal, request.agent_id)
        if task_id is None:
            candidate = self._candidate(principal, project_id, request.agent_id, capabilities, run["id"])
        else:
            candidate = self.service.reference(principal, project_id, task_id, "task")
            if (not self._eligible(principal, candidate, capabilities, request.agent_id)
                    or (candidate.get("handoff") or {}).get("target_run_id") not in (None, run["id"])):
                return {"task": None, "claim": None}
        if candidate is None:
            return {"task": None, "claim": None}
        for gate in getattr(self.service, "task_dispatch_gates", ()):
            verdict = gate(principal, candidate, run)
            if verdict is not None:
                if not isinstance(verdict, dict) or verdict.get("action") not in ("allow", "defer"):
                    raise Problem("invalid_dispatch_gate", 503)
                if verdict["action"] == "defer":
                    return {"task": None, "claim": None, "deferral":
                            {"task_id": candidate["id"], "reason": verdict.get("reason")}}
        gate = getattr(self.service, "coordination_gate", None)
        if gate is not None:
            verdict = gate(principal, candidate, run)
            if verdict and verdict.get("action") == "defer":
                # Declared-resource conflict or unknown effects: defer deterministically.
                # No attempt budget is consumed and no claim is touched.
                task = self._body(candidate["id"], "task")
                settings = self._settings(task)
                settings["retry_after"] = verdict["retry_after"]
                task["scheduling"] = settings
                self._save("task", task, expected=task["revision"])
                return {"task": None, "claim": None, "deferral":
                        {"task_id": candidate["id"], "reason": verdict.get("reason"),
                         "retry_after": verdict["retry_after"],
                         "evidence": verdict.get("evidence")}}
        claim = self._acquire(principal, candidate, run)
        if claim and claim.get("denied"):
            # The deterministic conflict stays canonical; no attempt budget is consumed.
            return {"task": None, "claim": None, "conflict": claim["conflict"]}
        return self._start(principal, candidate, run, claim)

    def _start(self, principal, candidate, run, claim):
        task = self._body(candidate["id"], "task")
        settings = self._settings(task)
        attempt = settings["attempts"] + 1
        record = {"attempt": attempt, "agent_id": run["agent_id"], "run_id": run["id"],
                  "dispatched_at": now(), "outcome": None, "summary": None,
                  "reported_at": None, "evidence_refs": []}
        settings.update(attempts=attempt, dispatches=settings["dispatches"] + 1,
                        assigned_agent_id=run["agent_id"], last_assigned_agent_id=run["agent_id"],
                        last_dispatch_run_id=run["id"], last_dispatch_at=now(),
                        state="dispatched", retry_after=None, escalated=False, escalation=None)
        settings["attempt_history"] = [*settings["attempt_history"], record][-HISTORY_LIMIT:]
        task["scheduling"] = settings
        saved = self._save("task", task, expected=task["revision"])
        started = self.service.task_start(principal, task["id"], m.TaskStart(
            expected_revision=saved["revision"], run_id=run["id"]))
        handoff = started.get("handoff")
        if handoff and handoff.get("state") == "pending":
            started["execution"]["progress"] = handoff["progress"]
            if handoff.get("checkpoint") and not handoff["checkpoint"].get("evidence_unavailable"):
                started["execution"]["checkpoint"] = handoff["checkpoint"]
            started["handoff"] = {**handoff, "state": "accepted", "accepted_run_id": run["id"],
                                  "accepted_execution_id": started["execution"]["id"]}
            self._save("task", started, expected=started["revision"])
        return {"task": self.store.get(task["id"], principal, "task"), "claim": claim}

    def handoff(self, principal, task_id, request):
        if principal["role"] not in ("admin", "human"):
            raise Problem("forbidden", 403)
        task = self.store.get(task_id, principal, "task")
        if task["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if task["status"] in ("completed", "cancelled"):
            raise Problem("terminal_task")
        target = self.service.reference(principal, task["project_id"], request.target_agent_id, "agent")
        if not self._required(task).issubset(self.capability_set(principal, target["id"])):
            raise Problem("capability_mismatch")
        execution = task.get("execution")
        if not execution:
            raise Problem("task_execution_required")
        old_run = self.store.get(execution["run_id"], principal, "run")
        if not self._execution_stale(principal, execution) and old_run["status"] != "paused":
            raise Problem("task_execution_active")
        if target["id"] == execution["agent_id"]:
            raise Problem("handoff_requires_different_agent", 422)
        if request.target_run_id:
            run = self.service.reference(principal, task["project_id"], request.target_run_id, "run")
            if run["agent_id"] != target["id"] or run["status"] not in ("running", "idle") or run.get("task_id"):
                raise Problem("target_run_not_ready")
        task["handoff"] = {"id": uid("handoff"), "state": "pending", "from_agent_id": execution["agent_id"],
            "from_run_id": execution["run_id"], "from_execution_id": execution["id"],
            "target_agent_id": target["id"], "target_run_id": request.target_run_id,
            "progress": execution.get("progress", 0), "checkpoint": execution.get("checkpoint"),
            "reason": request.reason, "created_at": now()}
        settings = self._settings(task)
        settings.update(assigned_agent_id=target["id"], state="queued", retry_after=None, attempts=0,
                        escalated=False, escalation=None, reassignments=settings["reassignments"] + 1)
        task.update(status="queued", scheduling=settings)
        self._archive_execution(task, {"action": "handoff", "at": now(), "to_agent_id": target["id"]})
        self._save("task", task, expected=task["revision"])
        self._clear_run_assignment(principal, old_run["id"], task_id)
        return self.store.get(task_id, principal, "task")

    # ---- self-reported outcomes -------------------------------------------

    def _evidence(self, principal, project_id, refs):
        result = [ref.model_dump(exclude_none=True) for ref in refs]
        if result:
            self.service.evidence(principal, project_id, result)
            for ref in result:
                ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
        return result

    def _archive_execution(self, task, result):
        execution = task.get("execution")
        if not execution:
            return None
        record = {**execution, "result": result, "ended_at": now()}
        task["execution_history"] = [*self._history(task), record][-HISTORY_LIMIT:]
        task.pop("execution", None)
        return execution

    def _clear_run_assignment(self, principal, run_id, task_id):
        if not run_id:
            return
        try:
            body = self._body(run_id, "run")
        except Problem:
            return
        if body.get("task_id") != task_id:
            return
        body["task_id"] = None
        self._save("run", body, expected=body["revision"])

    def report(self, principal, task_id, request):
        """Record an agent self-report for the current dispatched attempt."""
        self.store.get(task_id, principal, "task")
        self.service.represent(principal, request.agent_id)
        run = self.service.run(principal, request.run_id)
        task = self._body(task_id, "task")
        if run["agent_id"] != request.agent_id or run["project_id"] != task["project_id"]:
            raise Problem("not_found", 404)
        if task["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if task["status"] in TERMINAL:
            raise Problem("terminal_task")
        settings = self._settings(task)
        if settings["state"] != "dispatched":
            raise Problem("task_not_dispatched")
        if settings.get("assigned_agent_id") != request.agent_id:
            raise Problem("task_assigned_elsewhere", 403)
        execution = task.get("execution") or {}
        if (execution.get("run_id") != request.run_id or run.get("task_id") != task_id
                or (request.execution_id and request.execution_id != execution.get("id"))):
            raise Problem("stale_task_execution")
        if run["status"] not in ("running", "idle"):
            raise Problem("run_not_ready")
        self.service.task_permit(principal, run["id"], task)
        if not settings["attempt_history"] or settings["attempt_history"][-1].get("attempt") != settings["attempts"]:
            raise Problem("task_not_dispatched")
        refs = self._evidence(principal, task["project_id"], request.evidence_refs)
        reported_at = now()
        history = settings["attempt_history"]
        history[-1] = {**history[-1], "outcome": request.outcome, "summary": request.summary,
                       "reported_at": reported_at, "evidence_refs": refs}
        settings["attempt_history"] = history
        settings["last_outcome"] = {"outcome": request.outcome, "summary": request.summary,
                                    "reported_at": reported_at, "run_id": run["id"],
                                    "agent_id": request.agent_id, "evidence_refs": refs,
                                    "self_report": True}
        if request.outcome == "succeeded":
            task["status"] = "completed"
            settings.update(state="completed", escalated=False, escalation=None, retry_after=None)
        else:
            exhausted = settings["attempts"] >= self._max_attempts(task)
            task["status"] = "failed" if exhausted else "queued"
            settings.update(state="escalated" if exhausted else "retry_scheduled", escalated=exhausted,
                            retry_after=None if exhausted else time.time() + backoff_seconds(settings["attempts"]))
            if exhausted:
                settings["escalation"] = {"reason": "attempt_budget_exhausted",
                                          "attempts": settings["attempts"], "at": reported_at,
                                          "agent_id": request.agent_id, "summary": request.summary}
        task["scheduling"] = settings
        execution = self._archive_execution(task, {"outcome": request.outcome, "summary": request.summary,
                                                   "run_id": run["id"], "reported_at": reported_at,
                                                   "evidence_refs": refs, "self_report": True})
        if request.outcome == "succeeded" and execution:
            task["execution"] = execution
        self._save("task", task, expected=request.expected_revision)
        self._clear_run_assignment(principal, execution and execution.get("run_id"), task_id)
        return self.store.get(task_id, principal, "task")

    # ---- explicit reconciliation ------------------------------------------

    def _execution_stale(self, principal, execution):
        run_id = execution.get("run_id")
        if not isinstance(run_id, str):
            return False
        try:
            run = self.store.get(run_id, principal, "run")
        except Problem:
            # Unverifiable liveness is treated as live; recovery must never release unknown work.
            return False
        if run["status"] in TERMINAL:
            return True
        return as_number(run.get("last_seen_at")) is not None and \
            run["last_seen_at"] < time.time() - STALE_EXECUTION_SECONDS

    def reconcile(self, principal, task_id, request):
        """Explicitly recover terminal or stale scheduling state; claims are never released."""
        self.store.get(task_id, principal, "task")
        task = self._body(task_id, "task")
        self.service.project_access(principal, task["project_id"])
        if principal["role"] not in ("admin", "human"):
            raise Problem("forbidden", 403)
        if task["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if task["status"] in ("completed", "cancelled"):
            raise Problem("terminal_task")
        execution = task.get("execution")
        if execution and not self._execution_stale(principal, execution):
            raise Problem("task_execution_active")
        settings = self._settings(task)
        settings["last_reconcile"] = {"action": request.action, "reason": request.reason,
                                      "at": now(), "by": principal["id"],
                                      "previous_status": task["status"]}
        settings["retry_after"] = None
        if request.action == "escalate":
            settings["escalated"] = True
            settings["escalation"] = {"reason": "review_requested", "detail": request.reason,
                                      "at": now(), "agent_id": settings.get("assigned_agent_id")}
        else:
            task["status"] = "queued"
            settings.update(state="queued", escalated=False, escalation=None, attempts=0)
            if request.action == "reassign":
                settings["reassignments"] = settings["reassignments"] + 1
                settings["last_assigned_agent_id"] = settings.get("assigned_agent_id")
                settings["assigned_agent_id"] = None
        task["scheduling"] = settings
        execution = self._archive_execution(task, {"action": request.action,
                                                   "reason": request.reason, "at": now(),
                                                   "by": principal["id"], "self_report": False})
        self._save("task", task, expected=request.expected_revision)
        self._clear_run_assignment(principal, execution and execution.get("run_id"), task_id)
        return self.store.get(task_id, principal, "task")


def install(app, service, principal, command, envelope):
    """Register task-scheduling endpoints on the application built by ``create_app``."""
    scheduling = Scheduling(service)
    endpoints = []

    @app.post("/v1/task-plans")
    async def task_plans_create(body: TaskPlanCreate, p=Depends(principal),
                                idempotency_key: str | None = Header(None)):
        return command(p, "scheduling-plans", idempotency_key, body,
                       lambda: scheduling.create_plan(p, body))

    @app.get("/v1/task-plans")
    async def task_plans(project_id: str, p=Depends(principal)):
        return envelope(scheduling.plans(p, project_id))

    @app.get("/v1/task-plans/{plan_id}")
    async def task_plan(plan_id: str, p=Depends(principal)):
        return envelope(scheduling.plan(p, plan_id))

    @app.post("/v1/projects/{project_id}/tasks:dispatch")
    async def tasks_dispatch(project_id: str, body: TaskDispatch, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        result = command(p, "scheduling-dispatch:" + project_id, idempotency_key, body,
                         lambda: scheduling.dispatch(p, project_id, body))
        if result["data"]["task"] is None and result["data"].get("conflict"):
            return JSONResponse(result, status_code=409)
        return result

    @app.post("/v1/tasks/{task_id}:dispatch-outcome")
    async def task_dispatch_outcome(task_id: str, body: TaskDispatchOutcome, p=Depends(principal),
                                    idempotency_key: str | None = Header(None)):
        return command(p, "scheduling-outcome:" + task_id, idempotency_key, body,
                       lambda: scheduling.report(p, task_id, body))

    @app.post("/v1/tasks/{task_id}:reconcile")
    async def task_reconcile(task_id: str, body: TaskReconcile, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        return command(p, "scheduling-reconcile:" + task_id, idempotency_key, body,
                       lambda: scheduling.reconcile(p, task_id, body))
    @app.post("/v1/tasks/{task_id}:handoff")
    async def task_handoff(task_id: str, body: TaskHandoff, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        return command(p, "scheduling-handoff:" + task_id, idempotency_key, body,
                       lambda: scheduling.handoff(p, task_id, body))

    @app.post("/v1/agents/{agent_id}/capabilities")
    async def agent_capabilities_set(agent_id: str, body: AgentCapabilities, p=Depends(principal),
                                     idempotency_key: str | None = Header(None)):
        return command(p, "scheduling-capabilities:" + agent_id, idempotency_key, body,
                       lambda: scheduling.set_capabilities(p, agent_id, body))

    @app.get("/v1/agents/{agent_id}/capabilities")
    async def agent_capabilities(agent_id: str, p=Depends(principal)):
        return envelope(scheduling.capabilities_view(p, agent_id))

    endpoints.append("POST /v1/task-plans")
    endpoints.append("GET /v1/task-plans")
    endpoints.append("GET /v1/task-plans/{plan_id}")
    endpoints.append("POST /v1/projects/{project_id}/tasks:dispatch")
    endpoints.append("POST /v1/tasks/{task_id}:dispatch-outcome")
    endpoints.append("POST /v1/tasks/{task_id}:reconcile")
    endpoints.append("POST /v1/tasks/{task_id}:handoff")
    endpoints.append("POST /v1/agents/{agent_id}/capabilities")
    endpoints.append("GET /v1/agents/{agent_id}/capabilities")
    return {"endpoints": endpoints, "models": list(MODELS)}
