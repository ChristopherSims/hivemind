"""Goal-level orchestration: bounded explicit plans over canonical scheduling.

A goal is a durable operator-owned objective that compiles into a bounded, explicit
task DAG through the existing ``Scheduling.create_plan`` path — the same canonical
tasks, dependency relations, claims, and durable executions, never a parallel runtime.

Plan sources, in order of trust:

``supplied``
    The operator provides the full ``PlannedTask`` list; it is validated exactly
    like a canonical task plan (unique keys, in-plan dependencies, acyclic).
``extractive``
    Deterministic decomposition of the goal statement: explicit list items
    (``-``, ``*``, ``•``, ``1.``/``1)`` markers) become independent steps; a
    statement without list markers becomes a single step. This is extraction of
    operator-supplied structure, not NLP planning — no model call is made or
    implied.
``provider``
    An optional operator-configured synchronous ``plan_provider`` callable. It is
    *availability*, not inferred intent: it stays disabled unless explicitly
    installed. Because a trusted provider is assumed side-effect free but the
    network/process around it is not, proposals run under a durable compute
    reservation: phase zero persists a ``reserved`` ``goal_plan_intent`` pinning
    the principal, operation, idempotency key, payload hash, and the source goal
    revision/epoch *before* any inference; the provider then runs *off* the
    canonical lock; commit revalidates that the goal is unchanged (revision and
    epoch) and writes the digest, transitioning the intent to ``done``. Replays
    of a ``done`` intent re-check current authority and never re-invoke the
    provider; orphaned ``reserved`` intents are held forever and never
    auto-re-invoked. A goal that moved during compute fails the commit
    definitely (``plan_intent_source_changed``) — a stale plan is never rebound
    to a new epoch.

Ready work is dispatched only through canonical ``Scheduling.dispatch_exact`` —
the exact intended task of this goal's current plan, with the same authority,
gates, claims, and durable executions; a tick can never select another goal's or
an obsolete plan's work. A tick never impersonates anyone — administrators may
place work on any authorized agent's idle run, other principals only on runs for
agents they currently represent. A Nexus coordinator epoch fences orchestration
authority; it never widens dispatch identity.

Revision builds a *new* immutable plan from canonical observed outcomes. Old
non-terminal tasks without in-flight executions are atomically superseded
(``scheduling.escalated`` plus a ``superseded`` record bound to the new goal
epoch) so regular worker dispatch can never pick up obsolete goal work; completed
tasks and their evidence are never touched, and claims are never copied. Unknown
in-flight executions always hold the revision: a stale heartbeat or a terminal
run alone is *not* quiescence — only canonical terminal processing (an outcome
report or an explicit reconcile) releases a task. Silence or self-reported
success is never recorded as external proof.

Derived views re-authorize their sources: revision records' observed outcomes,
plan ids, and intent ids that are no longer visible to the caller (private,
deleted, or ACL-revoked) are redacted, never served raw from the cached record.

Nothing here reads boards, memories, or messages; decomposition consumes only
the explicit goal statement, so no unrelated project context leaks into plans.
"""

import json
import re
import time

from fastapi import Depends, Header, Request
from fastapi.responses import JSONResponse
from pydantic import Field, StrictBool, field_validator, model_validator

from . import models as m
from .models import ID
from .scheduling import PlannedTask, Scheduling, TaskDispatch, TaskPlanCreate
from .storage import Problem, digest, now

MAX_GOAL_TASKS = 64
REVISION_LIMIT = 20
GOAL_LIST_LIMIT = 200
GOAL_SCOPE_LIMIT = 32
DEFAULT_TICK_LIMIT = 8

_STEP_ITEM = re.compile(r"^(?:[-*•]|\d{1,2}[.)])\s+(?P<text>\S.*)$")
_STEP_FIELDS = {"key", "title", "description", "scope_ids", "depends_on", "priority",
                "required_capabilities", "max_attempts", "require_verified_dependencies", "routing",
                "external_dependency_ids"}

MODELS = ("GoalCreate", "GoalRevise", "GoalTick", "GoalPropose", "GoalSchedule")


def extract_steps(statement, limit=MAX_GOAL_TASKS, scope_ids=()):
    """Deterministic extractive decomposition of an explicit goal statement.

    List-marked lines become independent steps; anything else becomes one step.
    No inference, no model: the operator's own structure is the plan. Declared
    goal scopes (already authorized by the caller) are applied to every step.
    """
    items = []
    for line in statement.splitlines():
        match = _STEP_ITEM.match(line.strip())
        if match:
            items.append(match.group("text").strip())
    if not items:
        collapsed = " ".join(statement.split())
        if collapsed:
            items = [collapsed]
    if not items:
        raise Problem("goal_decomposition_empty", 422)
    if len(items) > limit:
        raise Problem("goal_step_limit_exceeded", 422)
    return [PlannedTask(key=f"step-{index}", title=text[:512], description=text,
                        scope_ids=list(scope_ids))
            for index, text in enumerate(items, start=1)]


class GoalCreate(m.Model):
    project_id: ID
    title: str = Field(min_length=1, max_length=512)
    statement: str = Field(min_length=1, max_length=65536)
    description: str = Field(default="", max_length=65536)
    scope_ids: list[ID] = Field(default_factory=list, max_length=GOAL_SCOPE_LIMIT)
    steps: list[PlannedTask] | None = Field(default=None, max_length=MAX_GOAL_TASKS)

    _clean_title = field_validator("title", mode="before")(
        lambda value: value.strip() if isinstance(value, str) else value)

    @model_validator(mode="after")
    def consistent(self):
        if self.steps is not None and not self.steps:
            raise ValueError("Supplied steps must not be empty")
        if len(set(self.scope_ids)) != len(self.scope_ids):
            raise ValueError("Scope IDs must be unique")
        return self


class GoalRevise(m.Model):
    """Replace the current plan with a new immutable revision.

    At most one explicit source: ``steps`` (supplied), ``statement`` (extractive),
    or ``intent_id`` (durable provider intent in ``done`` state). With none, the
    revision carries over the not-yet-completed tasks of the current plan.
    """

    expected_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=4096)
    expected_epoch: int | None = Field(default=None, ge=1)
    coordinator_epoch: int | None = Field(default=None, ge=1)
    statement: str | None = Field(default=None, min_length=1, max_length=65536)
    steps: list[PlannedTask] | None = Field(default=None, max_length=MAX_GOAL_TASKS)
    intent_id: ID | None = None

    _clean_reason = field_validator("reason", mode="before")(
        lambda value: value.strip() if isinstance(value, str) else value)

    @model_validator(mode="after")
    def single_source(self):
        sources = sum(value is not None
                      for value in (self.statement, self.steps, self.intent_id))
        if sources > 1:
            raise ValueError("A revision accepts at most one explicit plan source")
        if self.steps is not None and not self.steps:
            raise ValueError("Supplied steps must not be empty")
        return self


class GoalTick(m.Model):
    expected_epoch: int | None = Field(default=None, ge=1)
    coordinator_epoch: int | None = Field(default=None, ge=1)
    limit: int = Field(default=DEFAULT_TICK_LIMIT, ge=1, le=MAX_GOAL_TASKS)
    dispatch: StrictBool = True


class GoalPropose(m.Model):
    statement: str | None = Field(default=None, min_length=1, max_length=65536)
    limit: int = Field(default=DEFAULT_TICK_LIMIT, ge=1, le=MAX_GOAL_TASKS)


class GoalSchedule(m.Model):
    """Explicit operator configuration for background goal ticks; default off."""

    enabled: StrictBool = False
    operator_principal_id: ID | None = None
    project_ids: list[ID] = Field(default_factory=list, max_length=20)
    interval_seconds: float = Field(default=10, ge=1, le=300, allow_inf_nan=False)
    tick_limit: int = Field(default=DEFAULT_TICK_LIMIT, strict=True, ge=1, le=MAX_GOAL_TASKS)
    dispatch: StrictBool = True
    revise_failed: StrictBool = False
    max_plan_revisions: int = Field(default=4, strict=True, ge=1, le=20)

    @model_validator(mode="after")
    def identity(self):
        if self.enabled and (not self.operator_principal_id or not self.project_ids):
            raise ValueError("Enabled scheduling requires an operator and explicit projects")
        if len(self.project_ids) != len(set(self.project_ids)):
            raise ValueError("Scheduled projects must be unique")
        return self


class GoalOrchestration:
    """Domain logic for goal orchestration; call mutations inside ``Service.mutate``.

    ``hooks`` is the parent-owned dependency gate: optional callables injected at
    install time so sibling features (routing, artifacts, budgets) can participate
    without this module importing or editing them. Hooks must be pure bounded
    reads — no encoding, model, or blocking work — because several run under
    canonical locks:

    ``budgets(principal, project_id)``
        Return ``None``/allowed or ``{"allowed": False, "reason": str}`` to deny
        an entire tick's dispatch.
    ``routing(principal, task, run)``
        Return ``None``/``{"action": "allow"}`` or ``{"action": "defer", ...}`` /
        ``{"action": "deny", ...}`` to skip a candidate (task, run) pair; mirrors
        the parent's ``service.task_dispatch_gates`` contract.
    ``artifacts(principal, goal, revision_record)``
        Post-revision observer called inside the revision mutation; raising
        :class:`Problem` aborts the revision definitely.
    """

    def __init__(self, service, plan_provider=None, hooks=None):
        self.service = service
        self.store = service.store
        self.scheduling = Scheduling(service)
        self.plan_provider = plan_provider
        self.hooks = dict(hooks or {})

    # ---- metadata helpers -------------------------------------------------

    def _body(self, resource_id, kind):
        row = self.store.raw(resource_id, kind)
        return json.loads(row["body"])

    def _save(self, kind, body, expected):
        row = self.store.raw(body["id"], kind)
        return self.store.save(kind, row["project"], row["owner"], body, expected=expected)

    def _plan_view(self, principal, goal):
        plan_id = goal.get("current_plan_id")
        if not plan_id:
            return None, []
        try:
            view = self.scheduling.plan(principal, plan_id)
        except Problem:
            return None, []
        return view["plan"], view["tasks"]

    def _state(self, goal, tasks):
        if not goal.get("current_plan_id"):
            return "draft"
        if goal.get("hold"):
            return "blocked"
        if tasks and all(task["status"] == "completed" for task in tasks):
            return "completed"
        if any((self.scheduling._settings(task).get("escalation") or {}).get("reason")
               not in (None, "superseded") for task in tasks):
            return "failed"
        return "active"

    # ---- derived-view source re-authorization -------------------------------

    def _visible(self, principal, resource_id, kind=None):
        if not isinstance(resource_id, str):
            return False
        try:
            self.store.get(resource_id, principal, kind)
        except Problem:
            return False
        return True

    def _authorized_goal(self, principal, goal):
        """Redact revision sources the caller can no longer see; never serve raw."""
        body = dict(goal)
        revisions = []
        for record in body.get("revisions") or []:
            observed = [entry for entry in record.get("observed_outcomes") or []
                        if self._visible(principal, entry.get("task_id"), "task")]
            sanitized = {**record, "observed_outcomes": observed}
            if "superseded_task_ids" in sanitized:
                sanitized["superseded_task_ids"] = [
                    task_id for task_id in sanitized["superseded_task_ids"]
                    if self._visible(principal, task_id, "task")]
            for field, kind in (("plan_id", "task_plan"),
                                ("previous_plan_id", "task_plan"),
                                ("intent_id", "goal_plan_intent")):
                if sanitized.get(field) and not self._visible(principal, sanitized[field], kind):
                    sanitized[field] = None
            revisions.append(sanitized)
        body["revisions"] = revisions
        if body.get("hold"):
            body["hold"] = {**body["hold"], "task_ids": [
                task_id for task_id in body["hold"].get("task_ids") or []
                if self._visible(principal, task_id, "task")]}
        if body.get("current_plan_id") and not self._visible(principal, body["current_plan_id"], "task_plan"):
            body["current_plan_id"] = None
        return body

    def view(self, principal, goal_id):
        goal = self.store.get(goal_id, principal, "goal")
        plan, tasks = self._plan_view(principal, goal)
        ready = [task["id"] for task in tasks
                 if task["dependencies_met"] and task["dispatchable"]]
        return {"id": goal["id"], "goal": self._authorized_goal(principal, goal),
                "state": self._state(goal, tasks), "plan": plan, "tasks": tasks,
                "ready": ready,
                "held": [item["task_id"] for item in self._held(tasks)]}

    def goals(self, principal, project_id):
        self.service.project_access(principal, project_id)
        return [self._authorized_goal(principal, goal)
                for goal in self.store.list("goal", project_id, principal, GOAL_LIST_LIMIT)]

    # ---- holds: unknown in-flight work is never quiescent ---------------------

    def _held(self, tasks):
        """Non-terminal tasks with unknown-active work; revision and reassignment hold.

        A stale heartbeat or a terminal run is NOT quiescence: any current
        execution on a non-terminal task holds until canonical terminal processing
        (an outcome report or an explicit reconcile) archives it. A dispatched
        marker without a canonical execution is equally unknown.
        """
        held = []
        for task in tasks:
            if task["status"] in ("completed", "cancelled"):
                continue
            execution = task.get("execution")
            if execution:
                held.append({"task_id": task["id"], "reason": "unknown_active_execution"})
            elif self.scheduling._settings(task)["state"] == "dispatched":
                held.append({"task_id": task["id"], "reason": "dispatched_without_execution"})
        return held

    def _externally_verified(self, principal, task):
        """Independent verification only; silence or self-reports never count."""
        if task.get("status") != "completed":
            return False
        execution = task.get("execution") or {}
        if not execution.get("id"):
            return False
        row = self.store.db.execute(
            "SELECT id FROM resources WHERE kind='verification' AND project=? AND deleted=0 "
            "AND json_extract(body,'$.task_id')=? AND json_extract(body,'$.execution_id')=? "
            "ORDER BY seq DESC LIMIT 1",
            (task["project_id"], task["id"], execution["id"])).fetchone()
        if not row:
            return False
        from .verification import Verifications
        verification = Verifications(self.service).get(principal, row["id"])
        return bool(verification["independently_verified"]
                    and verification["reviewer_agent_id"] != execution.get("agent_id")
                    and verification["task_snapshot"].get("checkpoint_id")
                    == (execution.get("checkpoint") or {}).get("id"))

    def _observed_outcomes(self, principal, goal):
        _, tasks = self._plan_view(principal, goal)
        outcomes = []
        for task in tasks:
            settings = self.scheduling._settings(task)
            last = settings.get("last_outcome") or {}
            execution = task.get("execution") or {}
            outcomes.append({
                "task_id": task["id"], "plan_key": task.get("plan_key"),
                "status": task["status"], "execution_id": execution.get("id"),
                "self_reported_outcome": last.get("outcome"),
                "externally_verified": self._externally_verified(principal, task)})
        return outcomes[-MAX_GOAL_TASKS:]

    # ---- plan sources -------------------------------------------------------

    def _validate_steps(self, steps):
        try:
            TaskPlanCreate(project_id="validation", title="validation", tasks=list(steps))
        except ValueError as error:
            raise Problem("invalid_plan_steps", 422) from error
        return list(steps)

    def _check_step_scopes(self, goal_scopes, steps):
        """Supplied/generated steps stay inside declared goal scopes.

        Declared goal scopes are an explicit bound: every step scope must be a
        subset. With no declared goal scopes, per-step scopes are the explicit
        contract (and are still resolved canonically at plan creation); the goal
        then carries no overall scope claim — metadata only, honestly empty.
        """
        if not goal_scopes:
            return
        allowed = set(goal_scopes)
        for step in steps:
            if not set(step.scope_ids) <= allowed:
                raise Problem("goal_scope_exceeded", 422)

    def _coerce_steps(self, proposal):
        """Strictly coerce a provider proposal; failure is definite, never partial."""
        if not isinstance(proposal, (list, tuple)) or not proposal:
            raise Problem("plan_provider_invalid", 422)
        if len(proposal) > MAX_GOAL_TASKS:
            raise Problem("goal_step_limit_exceeded", 422)
        steps = []
        for index, item in enumerate(proposal, start=1):
            try:
                if isinstance(item, PlannedTask):
                    step = item
                elif isinstance(item, str):
                    text = item.strip()
                    step = PlannedTask(key=f"step-{index}", title=text[:512] or f"Step {index}",
                                       description=text)
                elif isinstance(item, dict):
                    unknown = set(item) - _STEP_FIELDS
                    if unknown:
                        raise ValueError(f"Unknown step fields: {sorted(unknown)}")
                    step = PlannedTask(**item)
                else:
                    raise ValueError("Unsupported step shape")
            except ValueError as error:
                raise Problem("plan_provider_invalid", 422) from error
            steps.append(step)
        return self._validate_steps(steps)

    def _carryover_steps(self, principal, goal):
        """Remaining canonical tasks as new steps; completed dependencies are dropped."""
        _, tasks = self._plan_view(principal, goal)
        remaining = [task for task in tasks if task["status"] not in ("completed", "cancelled")]
        if not remaining:
            raise Problem("goal_already_complete", 409)
        keys = {task["id"]: task.get("plan_key") for task in tasks}
        remaining_keys = {keys[task["id"]] for task in remaining}
        steps = []
        for position, task in enumerate(remaining, start=1):
            dependencies = [keys[dependency] for dependency in task.get("depends_on") or []
                            if keys.get(dependency) in remaining_keys]
            external = [dependency for dependency in task.get("depends_on") or []
                        if keys.get(dependency) not in remaining_keys]
            steps.append(PlannedTask(
                key=keys[task["id"]] or f"step-{position}",
                title=task["title"], description=task.get("description", ""),
                scope_ids=list(task.get("scope_ids") or []), depends_on=dependencies,
                external_dependency_ids=external,
                priority=self.scheduling._priority(task),
                required_capabilities=list(task.get("required_capabilities") or []),
                max_attempts=self.scheduling._max_attempts(task),
                require_verified_dependencies=bool(task.get("require_verified_dependencies")),
                routing=task.get("routing")))
        return self._validate_steps(steps)

    def _resolve_steps(self, principal, goal, request):
        if request.intent_id:
            intent = self.store.get(request.intent_id, principal, "goal_plan_intent")
            if intent["goal_id"] != goal["id"] or intent["project_id"] != goal["project_id"]:
                raise Problem("not_found", 404)
            if intent.get("state") != "done":
                raise Problem("plan_intent_not_ready", 409)
            if intent.get("consumed"):
                raise Problem("plan_intent_consumed", 409)
            if intent.get("goal_epoch") != goal["epoch"]:
                raise Problem("stale_goal_epoch", 409)
            # Re-validate the durable proposal at apply time: no uncertain replay.
            steps = self._coerce_steps(intent["steps"])
            if digest([step.model_dump(mode="json") for step in steps]) != intent["digest"]:
                raise Problem("plan_intent_mismatch", 409)
            self._check_step_scopes(goal.get("scope_ids") or [], steps)
            return steps, "provider", intent
        if request.steps is not None:
            steps = self._validate_steps(request.steps)
            self._check_step_scopes(goal.get("scope_ids") or [], steps)
            return steps, "supplied", None
        if request.statement is not None:
            return extract_steps(request.statement,
                                 scope_ids=list(goal.get("scope_ids") or [])), "extractive", None
        return self._carryover_steps(principal, goal), "carryover", None

    def _create_plan(self, principal, goal, steps, title, description):
        request = TaskPlanCreate(project_id=goal["project_id"], title=title,
                                 description=description, tasks=list(steps))
        return self.scheduling.create_plan(principal, request)

    def _supersede_plan_tasks(self, principal, goal, epoch, revision, reason):
        """Atomically retire the old plan's non-terminal, not-in-flight tasks.

        Superseded tasks keep their immutable history but gain
        ``scheduling.escalated`` (so canonical dispatch never selects them again)
        plus a deterministic ``superseded`` record bound to the new goal epoch.
        Completed tasks and their evidence are untouched; unknown in-flight
        executions are never auto-denied (held revisions prevent reaching this).
        """
        superseded = []
        for task in self._plan_view(principal, goal)[1]:
            if task["status"] in ("completed", "cancelled"):
                continue
            body = self._body(task["id"], "task")
            if body.get("execution"):
                continue
            settings = self.scheduling._settings(body)
            if settings["state"] == "dispatched":
                continue
            settings.update(escalated=True, state="superseded", retry_after=None,
                            escalation={"reason": "superseded", "goal_id": goal["id"],
                                        "goal_epoch": epoch, "plan_revision": revision,
                                        "detail": reason, "at": now()})
            body["scheduling"] = settings
            body["superseded"] = {"goal_id": goal["id"], "goal_epoch": epoch,
                                  "plan_revision": revision, "reason": reason,
                                  "at": now()}
            self._save("task", body, expected=body["revision"])
            superseded.append(body["id"])
        return superseded

    # ---- lifecycle ----------------------------------------------------------

    def create(self, principal, request):
        self.service.project_access(principal, request.project_id)
        for scope in request.scope_ids:
            self.service.reference(principal, request.project_id, scope, "scope")
        if request.steps is not None:
            steps = self._validate_steps(request.steps)
            self._check_step_scopes(request.scope_ids, steps)
            mode = "supplied"
        else:
            steps, mode = extract_steps(request.statement,
                                        scope_ids=request.scope_ids), "extractive"
        goal = self.service.create(principal, "goal", {
            "project_id": request.project_id, "title": request.title,
            "statement": request.statement, "description": request.description,
            "scope_ids": list(request.scope_ids),
            "current_plan_id": None, "plan_revision": 0, "epoch": 1,
            "revisions": [], "hold": None, "decomposition": {"mode": mode},
            "created_by": principal["id"], "created_by_agent": principal.get("agent")})
        plan_view = self._create_plan(principal, goal, steps, request.title,
                                      request.description)
        record = {"revision": 1, "plan_id": plan_view["plan"]["id"],
                  "previous_plan_id": None, "reason": "initial plan", "source": mode,
                  "intent_id": None, "observed_outcomes": [], "superseded_task_ids": [],
                  "created_at": now(), "by": principal["id"]}
        body = self._body(goal["id"], "goal")
        body.update(current_plan_id=record["plan_id"], plan_revision=1,
                    revisions=[record])
        self._save("goal", body, expected=goal["revision"])
        return {"id": goal["id"], "goal": self.view(principal, goal["id"])}

    def revise(self, principal, goal_id, request):
        goal = self.store.get(goal_id, principal, "goal")
        if goal["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if request.expected_epoch is not None and request.expected_epoch != goal["epoch"]:
            raise Problem("stale_goal_epoch", 409)
        if request.coordinator_epoch is not None:
            self.service.coordinator(principal, goal["project_id"], request.coordinator_epoch)
        elif principal["role"] not in ("admin", "human"):
            raise Problem("forbidden", 403)
        _, tasks = self._plan_view(principal, goal)
        held = self._held(tasks)
        if held:
            # Unknown-active executions are held, never reassigned. The hold is
            # canonical and the endpoint reports it as a definite 409.
            body = self._body(goal_id, "goal")
            body["hold"] = {"reason": "unknown_active_executions",
                            "task_ids": [item["task_id"] for item in held][:MAX_GOAL_TASKS],
                            "at": now(), "by": principal["id"]}
            self._save("goal", body, expected=goal["revision"])
            return {"id": goal_id, "revised": False, "hold": body["hold"],
                    "goal": self.view(principal, goal_id)}
        steps, source, intent = self._resolve_steps(principal, goal, request)
        observed = self._observed_outcomes(principal, goal)
        plan_view = self._create_plan(principal, goal, steps, goal["title"],
                                      f"Revision of goal {goal_id}: {request.reason}")
        body = self._body(goal_id, "goal")
        epoch = int(body.get("epoch") or 1) + 1
        record = {"revision": body["plan_revision"] + 1, "plan_id": plan_view["plan"]["id"],
                  "previous_plan_id": body.get("current_plan_id"), "reason": request.reason,
                  "source": source, "intent_id": intent["id"] if intent else None,
                  "observed_outcomes": observed, "created_at": now(), "by": principal["id"]}
        record["superseded_task_ids"] = self._supersede_plan_tasks(
            principal, body, epoch, record["revision"], request.reason)[:MAX_GOAL_TASKS]
        revisions = [*(body.get("revisions") or []), record][-REVISION_LIMIT:]
        body.update(current_plan_id=record["plan_id"], plan_revision=record["revision"],
                    epoch=epoch, revisions=revisions, hold=None)
        self._save("goal", body, expected=body["revision"])
        if intent:
            intent_body = self._body(intent["id"], "goal_plan_intent")
            intent_body.update(consumed=True, consumed_at=now(),
                               consumed_revision=record["revision"])
            self._save("goal_plan_intent", intent_body, expected=intent_body["revision"])
        artifacts = self.hooks.get("artifacts")
        if artifacts is not None:
            artifacts(principal, body, record)
        return {"id": goal_id, "revised": True, "revision": record,
                "goal": self.view(principal, goal_id)}

    # ---- provider proposals under durable compute reservation -----------------

    def reserve(self, principal, goal_id, request, idempotency_key):
        """Phase zero: durably pin the compute reservation BEFORE any inference.

        The intent records the exact principal, operation, idempotency key, payload
        hash, and the source goal revision/epoch. Commit later proves the goal did
        not move during off-lock compute; orphans stay ``reserved`` (held) forever
        and are never auto-re-invoked.
        """
        goal = self.store.get(goal_id, principal, "goal")
        if self.plan_provider is None:
            raise Problem("plan_provider_unavailable", 422)
        payload = request.model_dump(by_alias=True, exclude_none=True)
        intent = self.service.create(principal, "goal_plan_intent", {
            "project_id": goal["project_id"], "goal_id": goal["id"],
            "goal_revision": goal["revision"], "goal_epoch": goal["epoch"],
            "principal_id": principal["id"],
            "operation": f"goals-propose:{goal_id}",
            "idempotency_key": idempotency_key, "payload_hash": digest(payload),
            "state": "reserved", "compute_invocations": 0,
            "steps": None, "digest": None,
            "consumed": False, "consumed_at": None, "consumed_revision": None,
            "source": "provider"})
        return {"id": goal_id, "intent": intent}

    def intent(self, principal, intent_id):
        return self.store.get(intent_id, principal, "goal_plan_intent")

    def claim_compute(self, principal, intent):
        """Durably claim the single provider invocation; every later caller holds."""
        if intent.get("state") == "done":
            return intent
        if intent.get("state") != "reserved" or intent.get("compute_invocations"):
            raise Problem("plan_intent_held", 409)
        with self.store.transaction():
            body = self._body(intent["id"], "goal_plan_intent")
            if body.get("state") != "reserved" or body.get("compute_invocations"):
                raise Problem("plan_intent_held", 409)
            body["compute_invocations"] = 1
            body["compute_claimed_at"] = now()
            return self._save("goal_plan_intent", body, expected=body["revision"])

    def compute(self, principal, goal_id, request):
        """Phase one: run the operator provider OFF the canonical lock/transaction.

        The callable receives a minimal goal summary (no board/memory context) and
        must be side-effect free; its output is validated again at commit time.
        """
        if self.plan_provider is None:
            raise Problem("plan_provider_unavailable", 422)
        with self.store.lock:
            goal = self.store.get(goal_id, principal, "goal")
            statement = request.statement or goal["statement"]
        return self.plan_provider({"id": goal["id"], "project_id": goal["project_id"],
                                   "title": goal["title"], "statement": statement,
                                   "epoch": goal["epoch"]}, limit=request.limit)

    def commit_intent(self, principal, goal_id, request, intent, proposal):
        """Phase two: validate the unchanged source pins, then mark the intent done.

        A goal that moved (revision or epoch) while the provider computed fails
        definitely — the stale proposal is never rebound to a new epoch.
        """
        goal = self.store.get(goal_id, principal, "goal")
        current = self.store.get(intent["id"], principal, "goal_plan_intent")
        if current.get("principal_id") != principal["id"]:
            raise Problem("forbidden", 403)
        if current.get("state") != "reserved":
            raise Problem("plan_intent_held", 409)
        if current.get("payload_hash") != digest(request.model_dump(
                by_alias=True, exclude_none=True)):
            raise Problem("plan_intent_mismatch", 409)
        if (current.get("goal_revision") != goal["revision"]
                or current.get("goal_epoch") != goal["epoch"]):
            raise Problem("plan_intent_source_changed", 409)
        steps = self._coerce_steps(proposal)
        payload = [step.model_dump(mode="json") for step in steps]
        body = self._body(intent["id"], "goal_plan_intent")
        body.update(state="done", steps=payload, digest=digest(payload),
                    finished_at=now())
        saved = self._save("goal_plan_intent", body, expected=body["revision"])
        return {"id": goal_id, "intent": saved, "steps": payload}

    # ---- ready-work dispatch --------------------------------------------------

    def _authorized_runs(self, principal, project_id):
        """Idle live runs whose agent the caller may act for; never impersonation."""
        runs = []
        for run in self.store.list("run", project_id, principal, 1000):
            if run["status"] not in ("running", "idle") or run.get("task_id"):
                continue
            try:
                self.service.represent(principal, run["agent_id"])
            except Problem:
                continue
            runs.append(run)
        return sorted(runs, key=lambda run: (str(run.get("created_at")), run["id"]))

    def tick(self, principal, goal_id, request):
        goal = self.store.get(goal_id, principal, "goal")
        project_id = goal["project_id"]
        self.service.project_access(principal, project_id)
        if request.coordinator_epoch is not None:
            self.service.coordinator(principal, project_id, request.coordinator_epoch)
        if request.expected_epoch is not None and request.expected_epoch != goal["epoch"]:
            raise Problem("stale_goal_epoch", 409)
        _, tasks = self._plan_view(principal, goal)
        ready = [task for task in tasks
                 if task["dependencies_met"] and task["dispatchable"]]
        held = self._held(tasks)
        dispatched, skipped = [], []
        denied = None
        if request.dispatch and ready:
            budgets = self.hooks.get("budgets")
            if budgets is not None:
                denied = budgets(principal, project_id)
                if denied is not None and denied.get("allowed", True):
                    denied = None
            if denied is None:
                runs = self._authorized_runs(principal, project_id)
                used = set()
                for task in ready[:request.limit]:
                    required = self.scheduling._required(task)
                    placed = False
                    candidate_runs = runs
                    router = getattr(self.service, "capability_routing", None)
                    if router is not None and router.policy.enabled:
                        ranked = router.choose(principal, task, [run["agent_id"] for run in runs])
                        by_id = {run["id"]: run for run in runs}
                        candidate_runs = [by_id[entry["run_id"]] for entry in ranked["eligible"]
                                          if entry["run_id"] in by_id]
                    for run in candidate_runs:
                        if run["id"] in used:
                            continue
                        if not required.issubset(
                                self.scheduling.capability_set(principal, run["agent_id"])):
                            continue
                        routing = self.hooks.get("routing")
                        if routing is not None:
                            verdict = routing(principal, task, run)
                            if verdict and verdict.get("action") in ("deny", "defer"):
                                continue
                        try:
                            # Exact task only: canonical gates/claims/execution, and
                            # never another goal's or an obsolete plan's work.
                            result = self.scheduling.dispatch_exact(
                                principal, project_id,
                                TaskDispatch(agent_id=run["agent_id"], run_id=run["id"]),
                                task["id"])
                        except Problem as error:
                            skipped.append({"task_id": task["id"], "run_id": run["id"],
                                            "reason": error.code})
                            continue
                        if result.get("task"):
                            used.add(run["id"])
                            dispatched.append({"task_id": result["task"]["id"],
                                               "run_id": run["id"],
                                               "agent_id": run["agent_id"],
                                               "execution_id": (result["task"].get("execution")
                                                                or {}).get("id")})
                        elif result.get("conflict"):
                            skipped.append({"task_id": task["id"], "run_id": run["id"],
                                            "reason": "claim_conflict"})
                        elif result.get("deferral"):
                            skipped.append({"task_id": task["id"], "run_id": run["id"],
                                            "reason": "deferred"})
                        placed = True
                        break
                    if not placed:
                        skipped.append({"task_id": task["id"],
                                        "reason": "no_authorized_run"})
        return {"id": goal_id, "epoch": goal["epoch"],
                "dispatched": dispatched, "skipped": skipped, "held": held,
                "budget_denied": denied, "goal": self.view(principal, goal_id)}


class GoalRunner:
    """Explicit operator-configured goal ticks under fresh canonical authority.

    Default off: a ``GoalSchedule(enabled=False)`` never acts. Each tick re-fetches
    the configured operator principal; a missing operator fails definitely, and a
    non-administrator operator additionally requires a live Nexus lease whose exact
    current epoch fences every tick — without one the project is skipped. This
    mirrors ``feature_scheduling.CoordinationRunner``: administrators act under
    their standing authority (no lease), everyone else only under a live lease.
    """

    def __init__(self, service, orchestration, config):
        self.service, self.orchestration, self.config = service, orchestration, config
        self.last = None

    def scheduled_tick(self):
        moment = time.monotonic()
        if not self.config.enabled or (
                self.last is not None and moment - self.last < self.config.interval_seconds):
            return
        self.last = moment
        for project in self.config.project_ids:
            with self.service.store.transaction():
                row = self.service.store.db.execute(
                    "SELECT * FROM principals WHERE id=?",
                    (self.config.operator_principal_id,)).fetchone()
                if row is None:
                    raise Problem("configured_operator_unavailable", 403)
                principal = dict(row)
                self.service.project_access(principal, project)
                epoch = None
                if principal["role"] not in ("admin", "human"):
                    lease = self.service.lease(principal, project)
                    if lease is None:
                        continue
                    epoch = lease["coordinator_epoch"]
                for goal in self.service.store.list("goal", project, principal,
                                                    GOAL_LIST_LIMIT):
                    if goal.get("hold"):
                        continue
                    try:
                        if self.config.revise_failed and goal["plan_revision"] < self.config.max_plan_revisions:
                            _, tasks = self.orchestration._plan_view(principal, goal)
                            if (any(task["status"] == "failed" for task in tasks)
                                    and not self.orchestration._held(tasks)):
                                self.orchestration.revise(principal, goal["id"], GoalRevise(
                                    expected_revision=goal["revision"], expected_epoch=goal["epoch"],
                                    coordinator_epoch=epoch, reason="Revise from canonical failed outcomes"))
                        self.orchestration.tick(
                            principal, goal["id"],
                            GoalTick(coordinator_epoch=epoch, limit=self.config.tick_limit,
                                     dispatch=self.config.dispatch))
                    except Problem:
                        continue


def descriptors():
    """Self-describing operation metadata for parent discovery/MCP registration."""
    return {name: dict(descriptor) for name, descriptor in OPERATION_DESCRIPTORS.items()}


OPERATION_DESCRIPTORS = {
    "goal.create": {
        "name": "goal.create",
        "purpose": "Create a durable goal and compile it into a bounded explicit "
                   "task plan (max 64 tasks) through canonical scheduling.",
        "input": "POST /v1/goals {project_id, title, statement, scope_ids?, steps?}; "
                 "operator principal with project access; Idempotency-Key required.",
        "output": "Goal view: goal record, derived state, current plan, tasks with "
                  "dependency/dispatchability flags, ready task ids, held task ids.",
        "authority": "project access; every scope reference is resolved canonically; "
                     "steps are validated exactly like a canonical task plan; "
                     "declared goal scopes bound every generated/supplied step scope.",
        "side_effects": "creates one goal resource, one task_plan, and its tasks; "
                        "appends immutable revision 1.",
        "limits": "max 64 tasks, max 32 goal scopes; extractive decomposition reads "
                  "only the explicit statement, never boards/memories/messages.",
        "retry_safety": "idempotent by Idempotency-Key; a replay re-validates goal "
                        "existence and current ACLs before returning the cached result.",
    },
    "goal.read": {
        "name": "goal.read",
        "purpose": "Read one goal with its current plan, derived state, ready set, "
                   "and held (unknown-active) tasks.",
        "input": "GET /v1/goals/{goal_id} (or GET /v1/goals?project_id= for a "
                 "bounded list); any principal with project visibility.",
        "output": "Goal view or bounded goal list (max 200); historical revision "
                  "sources that are no longer visible are redacted.",
        "authority": "canonical resource visibility rules, re-applied to every "
                     "derived revision record's source ids at read time.",
        "side_effects": "none.",
        "limits": "derived state is computed from canonical task reads; no cached "
                  "success is ever synthesized.",
        "retry_safety": "read-only.",
    },
    "goal.revise": {
        "name": "goal.revise",
        "purpose": "Replace the current plan with a new immutable plan revision "
                   "built from canonical observed outcomes, an explicit step list, "
                   "a fresh statement, a durable done-state provider intent, or "
                   "carryover of incomplete work; old non-terminal, not-in-flight "
                   "tasks are atomically superseded.",
        "input": "POST /v1/goals/{goal_id}:revise {expected_revision, reason, "
                 "expected_epoch?, coordinator_epoch?, statement?|steps?|intent_id?}.",
        "output": "Revision record (observed outcomes, superseded task ids) and the "
                  "updated goal view; 409 with a persisted hold when unknown-active "
                  "executions exist.",
        "authority": "administrator/human, or a Nexus coordinator under a live "
                     "fenced epoch; revision and epoch are checked optimistically.",
        "side_effects": "creates a new task_plan and tasks; appends one immutable "
                        "revision record (bounded history); supersedes the old "
                        "plan's queued/failed tasks (scheduling.escalated + a "
                        "superseded record bound to the new goal epoch) without "
                        "touching completed evidence, claims, or in-flight "
                        "executions; consumes provider intents.",
        "limits": "max 64 tasks per revision; at most 20 embedded revision records "
                  "(older plans remain durably immutable); held revisions do not "
                  "bump the plan epoch.",
        "retry_safety": "idempotent by Idempotency-Key; stale revisions/epochs fail "
                        "definitely (412/409); consumed intents can never be "
                        "reapplied; stale-heartbeat executions never release a hold.",
    },
    "goal.tick": {
        "name": "goal.tick",
        "purpose": "Dispatch this goal's ready work (dependencies met, scheduling "
                   "gates open) through canonical dispatch_exact: the exact "
                   "intended task with exact scope claims and durable executions, "
                   "at most one task per idle run per tick.",
        "input": "POST /v1/goals/{goal_id}:tick {expected_epoch?, "
                 "coordinator_epoch?, limit?, dispatch?}.",
        "output": "Dispatched (task, run, execution) tuples, skipped candidates "
                  "with reasons, held unknown-active tasks, and the goal view.",
        "authority": "project access; an optional coordinator epoch is fenced "
                     "against the live Nexus lease. Dispatch targets only runs of "
                     "agents the caller may currently represent — never arbitrary "
                     "identities, never another goal's tasks.",
        "side_effects": "canonical dispatch side effects only (claims, executions, "
                        "attempt budgets); unknown-active tasks are reported held, "
                        "never reassigned.",
        "limits": "bounded per-tick dispatch limit (default 8, max 64); tasks stay "
                  "under scheduling's own attempt/escalation budgets.",
        "retry_safety": "idempotent by Idempotency-Key; a stale epoch fails with "
                        "409 before any dispatch.",
    },
    "goal.propose": {
        "name": "goal.propose",
        "purpose": "Provider plan proposals under a durable compute reservation: "
                   "reserve (pinning principal, operation, idempotency key, payload "
                   "hash, source goal revision/epoch) BEFORE inference; compute "
                   "off the canonical lock; commit only when the goal is unchanged.",
        "input": "POST /v1/goals/{goal_id}:propose {statement?, limit?}; requires an "
                 "operator-configured plan_provider at install time (availability, "
                 "not inference; disabled by default).",
        "output": "Durable intent in done state (id, digest, steps) ready for "
                  "goal.revise; 409 plan_intent_held for orphaned reservations; "
                  "409 plan_intent_source_changed when the goal moved mid-compute.",
        "authority": "the caller needs goal visibility; the provider receives only "
                     "a minimal goal summary (no board/memory context); commit "
                     "re-checks the reserving principal.",
        "side_effects": "creates and transitions one durable intent; no plan or "
                        "task is touched until a revision applies the intent.",
        "limits": "the provider is invoked at most once per reservation — "
                  "simultaneous, replayed, or post-crash requests hold instead of "
                  "re-invoking; proposals are strictly coerced to at most 64 steps.",
        "retry_safety": "a crash between reserve and commit leaves a held reserved "
                        "intent that is never auto-re-invoked; replays of a done "
                        "intent re-check current authority and return the durable "
                        "result without recompute.",
    },
    "goal.scheduled_tick": {
        "name": "goal.scheduled_tick",
        "purpose": "Optional background goal ticks under an explicit operator "
                   "configuration; default off.",
        "input": "GoalSchedule {enabled, operator_principal_id, project_ids, "
                 "interval_seconds, tick_limit, dispatch} driving GoalRunner.",
        "output": "Per-goal tick results identical to goal.tick.",
        "authority": "the operator principal is re-fetched fresh every tick; a "
                     "missing operator fails definitely, and non-administrator "
                     "operators require a live Nexus lease (exact current fenced "
                     "epoch) or the project is skipped — matching "
                     "feature_scheduling's runner policy.",
        "side_effects": "those of goal.tick; goals with an active hold are skipped.",
        "limits": "bounded interval, project list (max 20), per-goal dispatch "
                  "limit; at most 200 goals scanned per project per tick.",
        "retry_safety": "each tick is a fresh canonical transaction; a crash "
                        "simply delays the next interval.",
    },
}


def install(app, service, principal, command, envelope, *, plan_provider=None,
            hooks=None, background=None):
    """Conventional capability install for goal orchestration.

    ``plan_provider`` is an optional operator-supplied synchronous callable
    ``(goal_summary, *, limit) -> proposal`` used only by ``goal.propose``; without
    it the endpoint fails definitely with ``plan_provider_unavailable`` (explicit
    availability, disabled by default — never inferred). ``hooks`` is the
    parent-owned gate mapping described on :class:`GoalOrchestration`; this module
    never imports sibling feature modules.
    """
    from .capabilities import replace_routes
    from .agent_feature_gates import set_resource_projection
    replace_routes(app, __name__)
    orchestration = GoalOrchestration(service, plan_provider=plan_provider, hooks=hooks)
    app.state.goal_orchestration = orchestration
    service.goal_orchestration = orchestration
    set_resource_projection(service, "goal", orchestration.view)
    schedule = background if isinstance(background, GoalSchedule) else GoalSchedule(**(background or {}))
    app.state.goal_runner = GoalRunner(service, orchestration, schedule) if schedule.enabled else None
    endpoints = []

    def response(p, result):
        """Reauthorize cached command effects instead of replaying old source data."""
        data = dict(result["data"])
        current = orchestration.view(p, data["id"])
        data["goal"] = current
        if "hold" in data:
            data["hold"] = current["goal"].get("hold")
        if "revision" in data:
            number = data["revision"].get("revision")
            data["revision"] = next((item for item in current["goal"]["revisions"]
                                     if item["revision"] == number), None)
        for field in ("dispatched", "skipped", "held"):
            if field not in data:
                continue
            filtered = []
            for item in data[field]:
                try:
                    for key, kind in (("task_id", "task"), ("run_id", "run"), ("agent_id", "agent")):
                        if item.get(key):
                            service.reference(p, current["goal"]["project_id"], item[key], kind)
                except Problem:
                    continue
                filtered.append(item)
            data[field] = filtered
        return envelope(data)

    @app.post("/v1/goals")
    async def goals_create(body: GoalCreate, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        result = command(p, "goals", idempotency_key, body,
                         lambda: orchestration.create(p, body))
        return response(p, result)

    @app.get("/v1/goals")
    async def goals_list(project_id: str, p=Depends(principal)):
        return envelope(orchestration.goals(p, project_id))

    @app.get("/v1/goals/{goal_id}")
    async def goal_read(goal_id: str, p=Depends(principal)):
        return envelope(orchestration.view(p, goal_id))

    @app.post("/v1/goals/{goal_id}:revise")
    async def goal_revise(goal_id: str, body: GoalRevise, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        result = command(p, "goals-revise:" + goal_id, idempotency_key, body,
                         lambda: orchestration.revise(p, goal_id, body))
        if result["data"].get("revised") is False:
            return JSONResponse(response(p, result), status_code=409)
        return response(p, result)

    @app.post("/v1/goals/{goal_id}:tick")
    async def goal_tick(goal_id: str, body: GoalTick, p=Depends(principal),
                        idempotency_key: str | None = Header(None)):
        result = command(p, "goals-tick:" + goal_id, idempotency_key, body,
                         lambda: orchestration.tick(p, goal_id, body))
        return response(p, result)

    @app.post("/v1/goals/{goal_id}:propose")
    async def goal_propose(goal_id: str, body: GoalPropose, request: Request, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        # Phase zero reserves durably under the canonical mutation; phase one runs
        # the provider off the canonical lock (detached handler); phase two commits
        # only when the pinned source goal revision/epoch are unchanged.
        reservation = command(p, "goals-propose-reserve:" + goal_id, idempotency_key,
                              body,
                              lambda: orchestration.reserve(p, goal_id, body,
                                                            idempotency_key))
        with service.store.lock:
            current = orchestration.intent(p, reservation["data"]["intent"]["id"])
        if current["state"] == "done":
            # Replay of a completed proposal: re-authorized fresh read, no recompute.
            return envelope({"id": goal_id, "intent": current,
                             "steps": current["steps"]})
        orchestration.claim_compute(p, current)
        run_detached = getattr(app.state, "run_detached", None)
        if run_detached is None:
            import asyncio
            proposal = await asyncio.to_thread(orchestration.compute, p, goal_id, body)
        else:
            proposal = await run_detached(orchestration.compute, p, goal_id, body)
        fresh = await principal(request)
        if fresh["id"] != p["id"]:
            raise Problem("authentication_required", 401)
        p = fresh
        return command(p, "goals-propose:" + goal_id, idempotency_key, body,
                       lambda: orchestration.commit_intent(p, goal_id, body, current,
                                                           proposal))

    goal_propose.hivemind_detached_work = True

    endpoints.append("POST /v1/goals")
    endpoints.append("GET /v1/goals")
    endpoints.append("GET /v1/goals/{goal_id}")
    endpoints.append("POST /v1/goals/{goal_id}:revise")
    endpoints.append("POST /v1/goals/{goal_id}:tick")
    endpoints.append("POST /v1/goals/{goal_id}:propose")
    return {"endpoints": endpoints, "models": list(MODELS)}
