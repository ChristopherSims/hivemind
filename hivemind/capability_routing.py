"""Deterministic capability-based workload routing.

This module answers one question: *which currently eligible agent/run pair could
take this queued task?* It never starts work, never impersonates an agent, and
never treats a query or a recorded approval as dispatch authority. Workers still
dispatch themselves through the canonical ``tasks:dispatch`` path (or the
parent's exact-pair ``Scheduling.dispatch_exact``), which re-checks claims,
revisions, and liveness at claim time.

Evidence (all canonical, all currently authorized):

- ``Scheduling.capability_set`` / task ``required_capabilities`` for exact
  capability compatibility; no generative or semantic inference.
- The ``agent_activity`` roster for *advisory, privacy-filtered* workload counts
  (``current_task`` only — private or cross-project work is never counted or
  disclosed).
- Bounded raw canonical run/control rows for the *authoritative* gate: the
  chosen run and every peer worker run of the same agent are re-checked for
  task bindings, pending controls, and executions. Any binding that cannot be
  verified (private, missing, truncated) yields the generic reason
  ``agent_not_available`` — never the private identity, never a count, and
  never a quiet heartbeat-silence "proof" of safety.
- Worker-run binding: only runs bound to the task-worker purpose (canonical
  run metadata ``purpose == "task_worker"``) or named in the operator's
  explicit ``worker_run_ids`` allowlist are eligible. A newer independent
  observer/auxiliary run is never routable work capacity.
- Typed runtime declarations: an authorized per-agent ``routing_declaration``
  resource, or the agent's canonical ``runtime`` metadata. Runtime names come
  from the approved ``runtime_registry`` — never arbitrary imports, URLs, or
  model-supplied strings. Declarations are frozen while the agent has
  in-flight bound work (CAS ``expected_revision``; mid-flight changes are
  rejected), so a currently bound different runtime cannot be re-claimed as
  approved eligibility.
- Strict sandbox: the declaration flag is *advisory intent only*. A task's
  strict-sandbox requirement is satisfiable solely by the trusted operator's
  run-bound ``sandbox_verifier`` hook, which must enforce launch-attestation
  checks of the platform and the process/storage boundary for the exact run.
  No API-provided boolean is accepted as proof, and the vendor runtime family
  alone (factory/oh_my_pi) never suffices — e.g. Windows lacks the vendor
  sandbox entirely.

Everything is default-off and bounded: the query, the fenced approval, and
declarations all require an enabled ``RoutingPolicy``. Gate hooks
(``dispatch_gate``, ``start_gate``, ``gate_callback``) are pure reads: they
create no records and perform no external compute inside the caller's
canonical transaction. ``start_gate`` supports the parent's post-dispatch
recheck (after ``scheduling._start`` has incremented the attempt, before the
canonical execution starts) by validating the exact dispatch record instead of
the generic queued-task gates, so the final allowed attempt is never denied.
"""

import json
import time
from typing import Annotated

from fastapi import Depends, Header
from pydantic import Field, StrictBool, ValidationError, field_validator, model_validator

from . import models as m
from .agent_activity import MAX_RUNS_PER_AGENT, activity_roster
from .control_capture import _TOOL_MAP as CANONICAL_TOOLS
from .models import ID
from .runtime_registry import RUNTIME_FACTORIES
from .scheduling import Scheduling
from .service import TERMINAL
from .storage import Problem, now

MAX_CANDIDATES = 64
MAX_DECLARED_TOOLS = 32
MAX_WORKER_RUN_IDS = 1024
MAX_DECISION_LOOKUP = 200
ELIGIBLE_RUN_STATUSES = ("running", "idle")
PENDING_CONTROL_STATES = ("queued", "acknowledged", "applying")
WORKER_RUN_PURPOSE = "task_worker"
GATE_DEFER_SECONDS = 30.0

# Runtimes whose registry limits declare strict OS sandbox controls unsupported
# ("a2a": strict OS sandbox controls unsupported; "acp": no native
# hostile-process sandbox). Membership here is advisory intent only — actual
# strict-sandbox eligibility requires the operator's run-bound verifier.
STRICT_SANDBOX_RUNTIMES = frozenset({"factory", "oh_my_pi"})

# Stable, privacy-safe rejection reason codes.
REASON_AGENT_NOT_VISIBLE = "agent_not_visible"
REASON_DECLARATION_MISSING = "declaration_missing"
REASON_RUNTIME_NOT_ALLOWED = "runtime_not_allowed"
REASON_CAPABILITY_MISMATCH = "capability_mismatch"
REASON_RUNTIME_MISMATCH = "runtime_mismatch"
REASON_TOOL_MISMATCH = "tool_constraint_mismatch"
REASON_SANDBOX_UNSUPPORTED = "strict_sandbox_unsupported"
REASON_ASSIGNMENT_MISMATCH = "assignment_mismatch"
REASON_HANDOFF_MISMATCH = "handoff_target_mismatch"
REASON_AGENT_BUSY = "agent_busy"
REASON_CONTROL_HOLD = "control_hold"
REASON_NO_LIVE_RUN = "no_live_run"
REASON_WORKER_RUN_REQUIRED = "worker_run_required"
# Generic authoritative-gate reason for bindings that cannot be verified.
# Deliberately reveals neither the private identity nor any count.
REASON_NOT_AVAILABLE = "agent_not_available"

# Gate verdict reasons (parent gate-callback contract).
REASON_FEATURE_DISABLED = "feature_disabled"
REASON_NO_DISPATCH_RECORD = "no_dispatch_record"
REASON_DISPATCH_MISMATCH = "dispatch_record_mismatch"
REASON_ATTEMPTS_EXHAUSTED = "attempt_budget_exceeded"

# Task-level block reasons (the task itself is unroutable right now).
BLOCKED_NOT_DISPATCHABLE = "task_not_dispatchable"
BLOCKED_INVALID_REQUIREMENTS = "invalid_routing_requirements"

MODELS = ("RuntimeDeclaration", "RoutingQuery", "RoutingDispatch")

ToolName = Annotated[str, Field(min_length=1, max_length=64)]


def _strict_bool(value, name):
    if type(value) is not bool:
        raise ValueError(f"{name} must be a boolean")
    return value


def _strict_int(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer between {low} and {high}")
    return value


def _strict_float(value, name, low, high):
    if type(value) not in (int, float) or isinstance(value, bool) or not low <= float(value) <= high:
        raise ValueError(f"{name} must be a number between {low} and {high}")
    return float(value)


class RoutingPolicy:
    """Trusted operator routing configuration. Deny-by-default, strictly bounded.

    ``sandbox_verifier`` is an optional trusted callable
    ``(principal, task, run, declaration) -> bool`` enforcing launch-attestation
    checks of the platform and the process/storage boundary for the exact run.
    It must be a pure, side-effect-free local check: gates call it inside the
    caller's canonical transaction and never create records or perform external
    compute. Absent a verifier, no strict-sandbox requirement is satisfiable.
    """

    def __init__(self, *, enabled=False, max_candidates=32, max_heartbeat_gap_seconds=30.0,
                 allow_busy_agents=False, require_declaration=True, allowed_runtimes=(),
                 require_worker_run=True, worker_run_ids=(), sandbox_verifier=None):
        _strict_bool(enabled, "enabled")
        _strict_int(max_candidates, "max_candidates", 1, MAX_CANDIDATES)
        _strict_float(max_heartbeat_gap_seconds, "max_heartbeat_gap_seconds", 1.0, 300.0)
        _strict_bool(allow_busy_agents, "allow_busy_agents")
        _strict_bool(require_declaration, "require_declaration")
        _strict_bool(require_worker_run, "require_worker_run")
        if not isinstance(allowed_runtimes, (tuple, list)) or len(allowed_runtimes) > len(RUNTIME_FACTORIES):
            raise ValueError("allowed_runtimes must be a bounded list of approved runtime names")
        for name in allowed_runtimes:
            if name not in RUNTIME_FACTORIES:
                raise ValueError(f"Unknown approved runtime name: {name!r}")
        if len(set(allowed_runtimes)) != len(allowed_runtimes):
            raise ValueError("allowed_runtimes must be unique")
        if not isinstance(worker_run_ids, (tuple, list)) or len(worker_run_ids) > MAX_WORKER_RUN_IDS:
            raise ValueError("worker_run_ids must be a bounded list of canonical run IDs")
        if any(not isinstance(run_id, str) or not run_id for run_id in worker_run_ids):
            raise ValueError("worker_run_ids must be non-empty strings")
        if len(set(worker_run_ids)) != len(worker_run_ids):
            raise ValueError("worker_run_ids must be unique")
        if sandbox_verifier is not None and not callable(sandbox_verifier):
            raise ValueError("sandbox_verifier must be callable")
        self.enabled = enabled
        self.max_candidates = max_candidates
        self.max_heartbeat_gap_seconds = float(max_heartbeat_gap_seconds)
        self.allow_busy_agents = allow_busy_agents
        self.require_declaration = require_declaration
        self.allowed_runtimes = tuple(allowed_runtimes)
        self.require_worker_run = require_worker_run
        self.worker_run_ids = frozenset(worker_run_ids)
        self.sandbox_verifier = sandbox_verifier

    def describe(self):
        return {"enabled": self.enabled, "max_candidates": self.max_candidates,
                "max_heartbeat_gap_seconds": self.max_heartbeat_gap_seconds,
                "allow_busy_agents": self.allow_busy_agents,
                "require_declaration": self.require_declaration,
                "allowed_runtimes": list(self.allowed_runtimes),
                "require_worker_run": self.require_worker_run,
                "worker_run_allowlist_size": len(self.worker_run_ids),
                "sandbox_verifier_configured": self.sandbox_verifier is not None,
                "strict_sandbox_intent_runtimes": sorted(STRICT_SANDBOX_RUNTIMES)}


class RuntimeDeclaration(m.Model):
    """Authorized per-agent runtime/tool declaration; names are registry-approved.

    The flags are advisory intent, never proof: strict-sandbox eligibility
    additionally requires the operator's run-bound verifier at route time.
    ``expected_revision`` is an optional CAS fence; declarations freeze while
    the agent has in-flight bound work.
    """

    runtime: str = Field(min_length=1, max_length=128)
    tools: list[ToolName] = Field(default_factory=list, max_length=MAX_DECLARED_TOOLS)
    strict_sandbox: StrictBool = False
    network: StrictBool = False
    expected_revision: int | None = Field(default=None, ge=1)

    _clean_runtime = field_validator("runtime", mode="before")(
        lambda value: value.strip() if isinstance(value, str) else value)

    @model_validator(mode="after")
    def unique_tools(self):
        if len(set(self.tools)) != len(self.tools):
            raise ValueError("Declared tools must be unique")
        return self


class TaskRoutingRequirements(m.Model):
    """Typed optional ``routing`` block on a task. Unknown keys fail closed."""

    required_runtime: str | None = Field(default=None, min_length=1, max_length=128)
    required_tools: list[ToolName] = Field(default_factory=list, max_length=32)
    strict_sandbox: StrictBool = False

    @model_validator(mode="after")
    def unique_required(self):
        if len(set(self.required_tools)) != len(self.required_tools):
            raise ValueError("Required tools must be unique")
        return self


class RoutingQuery(m.Model):
    """Advisory routing query. A query grant is never dispatch authority."""

    task_id: ID
    candidate_agent_ids: list[ID] = Field(default_factory=list, max_length=MAX_CANDIDATES)

    @model_validator(mode="after")
    def unique_candidates(self):
        if len(set(self.candidate_agent_ids)) != len(self.candidate_agent_ids):
            raise ValueError("Candidate agent IDs must be unique")
        return self


class RoutingDispatch(m.Model):
    """Nexus-fenced durable approval of one exact (task, agent, run) pair."""

    task_id: ID
    agent_id: ID
    run_id: ID
    coordinator_epoch: int | None = Field(default=None, ge=1)
    reason: str = Field(min_length=1, max_length=4096)

    _clean_reason = field_validator("reason", mode="before")(
        lambda value: value.strip() if isinstance(value, str) else value)


class CapabilityRouting:
    """Domain logic for capability routing; mutations run inside ``Service.mutate``."""

    def __init__(self, service, policy=None):
        self.service = service
        self.store = service.store
        self.policy = policy if policy is not None else RoutingPolicy()
        self.scheduling = Scheduling(service)

    def _enabled(self):
        if not self.policy.enabled:
            raise Problem("feature_disabled", 403)

    def _body(self, resource_id, kind):
        return json.loads(self.store.raw(resource_id, kind)["body"])

    def _save(self, kind, body, expected):
        row = self.store.raw(body["id"], kind)
        return self.store.save(kind, row["project"], row["owner"], body, expected=expected)

    # ---- raw canonical evidence (bounded, read-only) -------------------------

    def _raw_agent_runs(self, principal, project_id, agent_id):
        """Newest raw run rows for one exact agent/project, with visibility flags.

        Returns (entries, truncated): entries are ``(body, visible)`` pairs.
        Invisible rows are included so the authoritative gate can fail closed
        on bindings it cannot verify without ever exposing them.
        """
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE project=? AND kind='run' AND deleted=0 "
            "AND json_extract(body,'$.agent_id')=? ORDER BY seq DESC,id DESC LIMIT ?",
            (project_id, agent_id, MAX_RUNS_PER_AGENT + 1)).fetchall()
        return [(json.loads(row["body"]), self.store.visible(row, principal))
                for row in rows[:MAX_RUNS_PER_AGENT]], len(rows) > MAX_RUNS_PER_AGENT

    def _raw_pending_controls(self, principal, project_id, run_ids):
        """Pending unexpired control rows targeting any of the given runs.

        Returns (held_run_ids, unknown): ``unknown`` is True when a pending row
        targeting these runs is not canonically visible to the caller, i.e. its
        state cannot be fully verified here.
        """
        if not run_ids:
            return set(), False
        current = time.time()
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE project=? AND kind='control' AND deleted=0 "
            "ORDER BY seq DESC LIMIT 1000", (project_id,)).fetchall()
        held, unknown = set(), False
        for row in rows:
            body = json.loads(row["body"])
            if body.get("target_run_id") not in run_ids:
                continue
            if body.get("state") not in PENDING_CONTROL_STATES or body.get("expires_at", 0) <= current:
                continue
            if not self.store.visible(row, principal):
                unknown = True
            else:
                held.add(body["target_run_id"])
        return held, unknown

    # ---- runtime declarations ------------------------------------------------

    def _declaration_record(self, principal, project_id, agent_id):
        for record in self.store.list("routing_declaration", project_id, principal, 1000):
            if record.get("agent_id") == agent_id:
                return record
        return None

    def declaration(self, principal, agent):
        """Effective declaration: authorized record, else canonical agent metadata."""
        record = self._declaration_record(principal, agent["project_id"], agent["id"])
        if record is not None:
            return {"source": "declaration", "runtime": record.get("runtime"),
                    "tools": [t for t in record.get("tools") or [] if isinstance(t, str)],
                    "strict_sandbox": record.get("strict_sandbox") is True,
                    "network": record.get("network") is True,
                    "revision": record.get("revision")}
        runtime = agent.get("runtime")
        if isinstance(runtime, str) and runtime in RUNTIME_FACTORIES:
            # Canonical agent metadata carries the runtime name only; tool and
            # sandbox constraints stay undeclared and therefore unsatisfiable.
            return {"source": "agent_metadata", "runtime": runtime, "tools": [],
                    "strict_sandbox": False, "network": False, "revision": None}
        return None

    def _in_flight(self, project_id, agent_id):
        """True when any live run of the agent has a bound nonterminal task.

        Runs under administrator visibility; an unverifiable or truncated
        binding freezes the declaration rather than guessing.
        """
        admin = {"role": "admin", "project": project_id, "agent": None, "id": "routing"}
        entries, truncated = self._raw_agent_runs(admin, project_id, agent_id)
        if truncated:
            return True
        for body, _visible in entries:
            if body.get("status") in TERMINAL:
                continue
            task_id = body.get("task_id")
            if not isinstance(task_id, str):
                continue
            try:
                task = self.store.get(task_id, admin, "task")
            except Problem:
                return True  # Unverifiable binding: preserve the hold.
            if task.get("status") not in TERMINAL:
                return True
        return False

    def set_declaration(self, principal, agent_id, request):
        """Self-declaration for the caller's own agent, or an administrator.

        Updates are CAS-fenced (``expected_revision``) and frozen while the
        agent has in-flight bound work, so a currently bound runtime cannot be
        swapped out from under approved eligibility.
        """
        self._enabled()
        self.service.represent(principal, agent_id)
        agent = self.store.get(agent_id, principal, "agent")
        if request.runtime not in RUNTIME_FACTORIES:
            raise Problem("unsupported_runtime", 422)
        unknown = [tool for tool in request.tools if tool not in CANONICAL_TOOLS]
        if unknown:
            raise Problem("unsupported_tool", 422)
        if request.strict_sandbox and request.runtime not in STRICT_SANDBOX_RUNTIMES:
            # The registry limits of this runtime declare sandbox controls unsupported;
            # even advisory intent is meaningless there.
            raise Problem("strict_sandbox_unsupported", 422)
        record = self._declaration_record(principal, agent["project_id"], agent_id)
        if record is not None and request.expected_revision is not None \
                and request.expected_revision != record["revision"]:
            raise Problem("revision_conflict", 412)
        current_effective = self.declaration(principal, agent)
        changed = current_effective is None or (
            current_effective["runtime"] != request.runtime
            or current_effective["tools"] != list(request.tools)
            or current_effective["strict_sandbox"] != request.strict_sandbox
            or current_effective["network"] != request.network)
        if changed and self._in_flight(agent["project_id"], agent_id):
            raise Problem("declaration_in_flight", 409)
        body = {"project_id": agent["project_id"], "agent_id": agent_id,
                "runtime": request.runtime, "tools": list(request.tools),
                "strict_sandbox": request.strict_sandbox, "network": request.network,
                "updated_by": principal["agent"]}
        if record is None:
            return self.service.create(principal, "routing_declaration", body)
        current = self._body(record["id"], "routing_declaration")
        current.update(body)
        return self._save("routing_declaration", current, expected=record["revision"])

    def declaration_view(self, principal, agent_id):
        agent = self.store.get(agent_id, principal, "agent")
        record = self._declaration_record(principal, agent["project_id"], agent_id)
        return {"agent_id": agent_id, "project_id": agent["project_id"],
                "declaration": record, "effective": self.declaration(principal, agent),
                "approved_runtimes": sorted(RUNTIME_FACTORIES),
                "advisory_only": "strict_sandbox is intent; eligibility requires the "
                                 "operator run-bound verifier at route time"}

    # ---- requirement/constraint evaluation ------------------------------------

    def _requirements(self, task):
        """Typed routing requirements; malformed declarations fail closed."""
        raw = task.get("routing")
        if raw is None:
            return TaskRoutingRequirements(), None
        try:
            requirements = TaskRoutingRequirements.model_validate(raw)
        except ValidationError:
            return None, BLOCKED_INVALID_REQUIREMENTS
        if requirements.required_runtime is not None \
                and requirements.required_runtime not in RUNTIME_FACTORIES:
            return None, BLOCKED_INVALID_REQUIREMENTS
        if [tool for tool in requirements.required_tools if tool not in CANONICAL_TOOLS]:
            return None, BLOCKED_INVALID_REQUIREMENTS
        return requirements, None

    def _worker_run(self, run):
        """A run is routable work capacity only when bound to the worker purpose."""
        if not self.policy.require_worker_run:
            return True
        return run.get("purpose") == WORKER_RUN_PURPOSE or run.get("id") in self.policy.worker_run_ids

    def _sandbox_verified(self, principal, task, run, declaration):
        """Strict sandbox requires the trusted run-bound verifier; flags never suffice."""
        if not declaration.get("strict_sandbox"):
            return False
        if declaration.get("runtime") not in STRICT_SANDBOX_RUNTIMES:
            return False
        verifier = self.policy.sandbox_verifier
        if verifier is None:
            return False
        try:
            return verifier(principal, task, run, declaration) is True
        except Exception:
            return False  # A failing verifier is never proof.

    def _declaration_agent_gate(self, requirements, declaration):
        """Agent-level runtime/tool constraints; returns a reason or None."""
        constrained = requirements.required_runtime is not None \
            or bool(requirements.required_tools) or requirements.strict_sandbox
        if declaration is None:
            if self.policy.require_declaration or constrained or self.policy.allowed_runtimes:
                return REASON_DECLARATION_MISSING
            return None
        runtime = declaration["runtime"]
        if self.policy.allowed_runtimes and runtime not in self.policy.allowed_runtimes:
            return REASON_RUNTIME_NOT_ALLOWED
        if requirements.required_runtime is not None and requirements.required_runtime != runtime:
            return REASON_RUNTIME_MISMATCH
        if not set(requirements.required_tools).issubset(set(declaration["tools"])):
            return REASON_TOOL_MISMATCH
        return None

    def _declaration_run_gate(self, principal, task, requirements, declaration, run):
        """Run-bound constraints: the actual chosen run's runtime and sandbox proof."""
        if declaration is None:
            return None  # Unconstrained task, undeclared agent allowed by policy.
        run_runtime = run.get("runtime")
        if isinstance(run_runtime, str) and run_runtime != declaration["runtime"]:
            return REASON_RUNTIME_MISMATCH
        if requirements.strict_sandbox \
                and not self._sandbox_verified(principal, task, run, declaration):
            return REASON_SANDBOX_UNSUPPORTED
        return None

    def _fresh(self, run):
        seen = run.get("last_seen_at")
        return isinstance(seen, (int, float)) and not isinstance(seen, bool) \
            and seen >= time.time() - self.policy.max_heartbeat_gap_seconds

    @staticmethod
    def _presence(run):
        seen = run.get("last_seen_at")
        gap = time.time() - seen if isinstance(seen, (int, float)) else None
        return "online" if gap is not None and gap < 15 else \
            "stale" if gap is not None and gap < 30 else "offline"

    # ---- advisory candidate evaluation (choose) --------------------------------

    def _evaluate(self, principal, task, settings, requirements, entry, holds, claim_counts):
        """Return (eligible_entry, rejection_reason) for one rostered agent.

        Advisory: workload counts come only from the privacy-filtered roster's
        ``current_task``. Permitted runs come from bounded raw canonical rows
        the caller can see; invisible live rows are skipped silently here (the
        authoritative gate fails closed on them instead).
        """
        agent_id = entry["id"]
        if settings.get("assigned_agent_id") not in (None, agent_id):
            return None, REASON_ASSIGNMENT_MISMATCH
        handoff = task.get("handoff") or {}
        if handoff.get("state") == "pending" and handoff.get("target_agent_id") not in (None, agent_id):
            return None, REASON_HANDOFF_MISMATCH
        capabilities = self.scheduling.capability_set(principal, agent_id)
        if not self.scheduling._required(task).issubset(capabilities):
            return None, REASON_CAPABILITY_MISMATCH
        declaration = self.declaration(principal, entry)
        agent_reason = self._declaration_agent_gate(requirements, declaration)
        if agent_reason is not None:
            return None, agent_reason
        context = entry.get("context") or {}
        workload = sum(1 for run in context.get("runs") or []
                       if run.get("status") not in TERMINAL and run.get("current_task"))
        if workload and not self.policy.allow_busy_agents:
            return None, REASON_AGENT_BUSY
        entries, truncated = self._raw_agent_runs(principal, task["project_id"], agent_id)
        if truncated:
            return None, REASON_NOT_AVAILABLE
        target_run = handoff.get("target_run_id") if handoff.get("state") == "pending" else None
        permitted, held_seen, worker_seen, constraint_seen = [], False, False, None
        for body, visible in entries:
            if not visible or body.get("status") not in ELIGIBLE_RUN_STATUSES or body.get("task_id"):
                continue
            if target_run is not None and body.get("id") != target_run:
                continue
            reason = self._declaration_run_gate(principal, task, requirements, declaration, body)
            if reason is not None:
                constraint_seen = constraint_seen or reason
                continue
            if not self._fresh(body):
                continue
            if not self._worker_run(body):
                worker_seen = True
                continue
            if body.get("id") in holds:
                held_seen = True
                continue
            permitted.append(body)
        if not permitted:
            if constraint_seen is not None:
                return None, constraint_seen
            if held_seen:
                return None, REASON_CONTROL_HOLD
            if worker_seen:
                return None, REASON_WORKER_RUN_REQUIRED
            return None, REASON_NO_LIVE_RUN
        # Deterministic run choice: freshest presence, latest heartbeat, then id.
        chosen = sorted(permitted, key=lambda run: (
            {"online": 0, "stale": 1, "offline": 2}[self._presence(run)],
            -float(run.get("last_seen_at") or 0), str(run.get("id"))))[0]
        return {"agent_id": agent_id, "run_id": chosen["id"],
                "runtime": (declaration or {}).get("runtime"),
                "declaration_source": (declaration or {}).get("source"),
                "presence": self._presence(chosen), "workload": workload,
                "active_claims": claim_counts.get(agent_id, 0)}, None

    @staticmethod
    def _rank_key(candidate):
        """Stable, explicit tie-break: workload, claims, then canonical agent id."""
        return (candidate["workload"], candidate["active_claims"], candidate["agent_id"])

    def _control_holds(self, principal, project_id):
        held = set()
        current = time.time()
        for control in self.store.list("control", project_id, principal, 1000):
            if control.get("state") in PENDING_CONTROL_STATES \
                    and control.get("expires_at", 0) > current \
                    and isinstance(control.get("target_run_id"), str):
                held.add(control["target_run_id"])
        return held

    def _claim_counts(self, principal, project_id):
        counts = {}
        for claim in self.service.active_claims(principal, project_id):
            agent_id = claim.get("agent_id")
            if isinstance(agent_id, str):
                counts[agent_id] = counts.get(agent_id, 0) + 1
        return counts

    def _task_gate(self, principal, task):
        """Agent-independent advisory gates for a still-queued task."""
        if not self.scheduling._dispatchable(task):
            return BLOCKED_NOT_DISPATCHABLE
        if not self.scheduling._dependencies_met(principal, task):
            return BLOCKED_NOT_DISPATCHABLE
        return None

    def choose(self, principal, task, candidates=None):
        """Rank eligible (agent, run) pairs for one canonical task. Advisory only.

        Trusted callers (parent/goal worker) pass a canonical task body; the
        HTTP query always loads the task canonically and never trusts a
        caller-supplied dict. Rejections are stable codes with no other-project
        identities and no private task counts.
        """
        self._enabled()
        project_id = task["project_id"]
        self.service.project_access(principal, project_id)
        settings = self.scheduling._settings(task)
        requirements, invalid = self._requirements(task)
        result = {"task_id": task["id"], "project_id": project_id, "eligible": [],
                  "rejected": [], "blocked": None, "advisory": True,
                  "authority": "advisory_query_grant_is_not_dispatch_authority",
                  "policy": self.policy.describe()}
        if invalid is not None:
            result["blocked"] = invalid
            return result
        blocked = self._task_gate(principal, task)
        if blocked is not None:
            result["blocked"] = blocked
            return result
        roster = {entry["id"]: entry
                  for entry in activity_roster(self.service, principal, project_id)}
        wanted = sorted({str(agent_id) for agent_id in (candidates or [])})
        truncated = False
        if not wanted:
            wanted = sorted(roster)
        if len(wanted) > self.policy.max_candidates:
            wanted, truncated = wanted[:self.policy.max_candidates], True
        holds = self._control_holds(principal, project_id)
        claim_counts = self._claim_counts(principal, project_id)
        for agent_id in wanted:
            entry = roster.get(agent_id)
            if entry is None:
                result["rejected"].append({"agent_id": agent_id,
                                           "reason": REASON_AGENT_NOT_VISIBLE})
                continue
            candidate, reason = self._evaluate(principal, task, settings, requirements,
                                               entry, holds, claim_counts)
            if candidate is None:
                result["rejected"].append({"agent_id": agent_id, "reason": reason})
            else:
                result["eligible"].append(candidate)
        result["eligible"].sort(key=self._rank_key)
        result["eligible"] = [{**candidate, "rank": index}
                              for index, candidate in enumerate(result["eligible"])]
        result["candidates_truncated"] = truncated
        return result

    # ---- authoritative gates (pure reads; no records, no external compute) -----

    def _authoritative_run_check(self, principal, task, run, allow_self_binding=False):
        """Raw canonical recheck of the chosen run and its peer worker runs.

        Any binding that cannot be verified — private, missing, truncated, or
        pending-control evidence not canonically visible — yields the generic
        ``agent_not_available``; nothing private is identified or counted.
        ``allow_self_binding`` (start gate only) accepts a run already bound to
        this exact task, matching the recorded dispatch.
        """
        project_id, agent_id = task["project_id"], run.get("agent_id")
        if run.get("project_id") != project_id or not isinstance(agent_id, str):
            return REASON_AGENT_NOT_VISIBLE
        entries, truncated = self._raw_agent_runs(principal, project_id, agent_id)
        if truncated:
            return REASON_NOT_AVAILABLE
        live = [(body, visible) for body, visible in entries if body.get("status") not in TERMINAL]
        if any(not visible for _body, visible in live):
            return REASON_NOT_AVAILABLE
        raw = next((body for body, _ in entries if body.get("id") == run.get("id")), None)
        if raw is None:
            return REASON_AGENT_NOT_VISIBLE
        if raw.get("status") not in ELIGIBLE_RUN_STATUSES or not self._fresh(raw):
            return REASON_NO_LIVE_RUN
        if raw.get("task_id") and not (
                allow_self_binding and raw.get("task_id") == task.get("id")):
            return REASON_AGENT_BUSY
        if not self._worker_run(raw):
            return REASON_WORKER_RUN_REQUIRED
        run_ids = [body["id"] for body, _ in live]
        held, unknown_controls = self._raw_pending_controls(principal, project_id, run_ids)
        if unknown_controls:
            return REASON_NOT_AVAILABLE
        if raw["id"] in held:
            return REASON_CONTROL_HOLD
        if not self.policy.allow_busy_agents:
            for peer, _ in live:
                if peer["id"] == raw["id"]:
                    continue
                if peer["id"] in held and self._worker_run(peer):
                    return REASON_NOT_AVAILABLE  # Peer worker run under a control transition.
                task_id = peer.get("task_id")
                if not isinstance(task_id, str):
                    continue
                try:
                    bound = self.store.get(task_id, principal, "task")
                except Problem:
                    return REASON_NOT_AVAILABLE  # Unverifiable binding keeps its hold.
                execution = bound.get("execution") or {}
                if bound.get("status") not in TERMINAL and (
                        not execution or (execution.get("run_id") == peer["id"]
                                          and execution.get("agent_id") == agent_id)):
                    return REASON_AGENT_BUSY
        return None

    def dispatch_gate(self, principal, task, run):
        """Authoritative pre-dispatch check for one exact (task, run) pair.

        Pure read: creates no records and performs no external compute inside
        the caller's canonical transaction. Unverifiable state defers; it is
        never treated as availability.
        """
        verdict = {"task_id": task.get("id"), "run_id": run.get("id"),
                   "agent_id": run.get("agent_id"), "allowed": False, "reason": None}
        if not self.policy.enabled:
            verdict["reason"] = REASON_FEATURE_DISABLED
            return verdict
        settings = self.scheduling._settings(task)
        requirements, invalid = self._requirements(task)
        if invalid is not None:
            verdict["reason"] = invalid
            return verdict
        blocked = self._task_gate(principal, task)
        if blocked is not None:
            verdict["reason"] = blocked
            return verdict
        if settings.get("assigned_agent_id") not in (None, run.get("agent_id")):
            verdict["reason"] = REASON_ASSIGNMENT_MISMATCH
            return verdict
        handoff = task.get("handoff") or {}
        if handoff.get("state") == "pending":
            if handoff.get("target_agent_id") not in (None, run.get("agent_id")) \
                    or handoff.get("target_run_id") not in (None, run.get("id")):
                verdict["reason"] = REASON_HANDOFF_MISMATCH
                return verdict
        capabilities = self.scheduling.capability_set(principal, run["agent_id"])
        if not self.scheduling._required(task).issubset(capabilities):
            verdict["reason"] = REASON_CAPABILITY_MISMATCH
            return verdict
        agent = self.store.get(run["agent_id"], principal, "agent")
        declaration = self.declaration(principal, agent)
        reason = self._declaration_agent_gate(requirements, declaration)
        if reason is not None:
            verdict["reason"] = reason
            return verdict
        reason = self._authoritative_run_check(principal, task, run)
        if reason is not None:
            verdict["reason"] = reason
            return verdict
        reason = self._declaration_run_gate(principal, task, requirements, declaration, run)
        if reason is not None:
            verdict["reason"] = reason
            return verdict
        verdict["allowed"] = True
        return verdict

    def start_gate(self, principal, task, run):
        """Parent ``task_start`` recheck for the exact recorded dispatch.

        ``scheduling._start`` has already incremented the attempt and marked
        ``scheduling.state == "dispatched"`` (task still ``queued``) when the
        parent calls this, so the generic queued-task gates do not apply: the
        gate validates the exact dispatch record for this run — assignment,
        dispatch run, attempt bounds (``attempts <= max_attempts``, so the
        final allowed attempt is never denied) — then re-checks capabilities,
        dependencies, and the authoritative raw run/control/peer evidence.
        No attempt is consumed, no generic queued bypass is introduced, and no
        record is created.
        """
        verdict = {"task_id": task.get("id"), "run_id": run.get("id"),
                   "agent_id": run.get("agent_id"), "action": "defer", "reason": None}
        if not self.policy.enabled:
            verdict["reason"] = REASON_FEATURE_DISABLED
            return verdict
        settings = self.scheduling._settings(task)
        if task.get("status") != "queued" or settings.get("state") != "dispatched":
            verdict["reason"] = REASON_NO_DISPATCH_RECORD
            return verdict
        attempts = settings["attempts"]
        record = settings["attempt_history"][-1] if settings["attempt_history"] else {}
        if settings.get("assigned_agent_id") != run.get("agent_id") \
                or settings.get("last_dispatch_run_id") != run.get("id") \
                or record.get("run_id") != run.get("id"):
            verdict["reason"] = REASON_DISPATCH_MISMATCH
            return verdict
        if not 1 <= attempts <= self.scheduling._max_attempts(task):
            verdict["reason"] = REASON_ATTEMPTS_EXHAUSTED
            return verdict
        requirements, invalid = self._requirements(task)
        if invalid is not None:
            verdict["reason"] = invalid
            return verdict
        if not self.scheduling._dependencies_met(principal, task):
            verdict["reason"] = BLOCKED_NOT_DISPATCHABLE
            return verdict
        capabilities = self.scheduling.capability_set(principal, run["agent_id"])
        if not self.scheduling._required(task).issubset(capabilities):
            verdict["reason"] = REASON_CAPABILITY_MISMATCH
            return verdict
        agent = self.store.get(run["agent_id"], principal, "agent")
        declaration = self.declaration(principal, agent)
        reason = self._declaration_agent_gate(requirements, declaration)
        if reason is not None:
            verdict["reason"] = reason
            return verdict
        reason = self._authoritative_run_check(principal, task, run, allow_self_binding=True)
        if reason is not None:
            verdict["reason"] = reason
            return verdict
        reason = self._declaration_run_gate(principal, task, requirements, declaration, run)
        if reason is not None:
            verdict["reason"] = reason
            return verdict
        verdict["action"] = "allow"
        return verdict

    def gate_callback(self, principal, task, run):
        """Parent gate-registry adapter: the ``{'action', 'reason'}`` contract.

        Registered callbacks receive ``(principal, task, run)`` and return
        ``{"action": "allow" | "defer", "reason": <code|None>, "retry_after"}``.
        Deferrals are deterministic and carry a bounded retry hint; the parent
        decides whether to persist it.
        """
        verdict = self.dispatch_gate(principal, task, run)
        return {"action": "allow" if verdict["allowed"] else "defer",
                "reason": verdict["reason"],
                "retry_after": None if verdict["allowed"] else time.time() + GATE_DEFER_SECONDS}

    # ---- HTTP entry points -----------------------------------------------------

    def query(self, principal, project_id, request):
        self._enabled()
        # The task is always loaded canonically; caller-supplied bodies are
        # never trusted as tasks.
        task = self.service.reference(principal, project_id, request.task_id, "task")
        return self.choose(principal, task, request.candidate_agent_ids)

    def _fence(self, principal, project_id, request):
        self.service.project_access(principal, project_id)
        if principal["role"] in ("admin", "human"):
            return {"fenced_by": "operator"}
        if request.coordinator_epoch is None:
            raise Problem("coordinator_epoch_required", 422)
        lease = self.service.coordinator(principal, project_id, request.coordinator_epoch)
        return {"fenced_by": "nexus_lease", "lease_id": lease["id"],
                "coordinator_epoch": lease["coordinator_epoch"]}

    def fenced_dispatch(self, principal, project_id, request):
        """Record a Nexus-fenced durable approval of one exact routing pair.

        The approval is a canonical ``routing_decision`` record (IDs, fence,
        reason only — no task content, so a later privacy change cannot leak
        history through it). It never starts the run, never impersonates the
        agent, and never releases a hold: the worker still dispatches itself
        canonically, or a parent hook picks up the pending approval via
        ``approved_decision`` and drives ``Scheduling.dispatch_exact`` under
        the same canonical gates.
        """
        self._enabled()
        fence = self._fence(principal, project_id, request)
        task = self.service.reference(principal, project_id, request.task_id, "task")
        run = self.service.reference(principal, project_id, request.run_id, "run")
        gate = self.dispatch_gate(principal, task, run)
        if not gate["allowed"] or gate["agent_id"] != request.agent_id:
            raise Problem("routing_rejected", 422)
        decision = self.service.create(principal, "routing_decision", {
            "project_id": project_id, "task_id": task["id"],
            "agent_id": request.agent_id, "run_id": run["id"],
            "decision": "approved", "reason": request.reason,
            "fence": fence, "decided_by": principal["id"], "decided_at": now()})
        return {"kind": "durable_approval", "decision": decision, "gate": gate, "fence": fence,
                "authority": "advisory_approval_only; canonical worker dispatch "
                             "(or parent-gated dispatch_exact) remains required"}

    def approved_decision(self, principal, project_id, task_id, agent_id, run_id):
        """Latest matching durable approval, or None. Pure read for parent hooks."""
        self._enabled()
        self.service.project_access(principal, project_id)
        for record in self.store.list("routing_decision", project_id, principal,
                                      MAX_DECISION_LOOKUP):
            if record.get("decision") == "approved" \
                    and record.get("task_id") == task_id \
                    and record.get("agent_id") == agent_id \
                    and record.get("run_id") == run_id:
                return record
        return None


OPERATION_DESCRIPTORS = {
    "capability_routing.query": {
        "name": "capability_routing.query",
        "method": "POST",
        "endpoint": "/v1/projects/{project_id}/routing:query",
        "request_model": "RoutingQuery",
        "purpose": "Deterministically rank currently eligible (agent, run) pairs for one "
                   "canonically loaded queued task by capability compatibility, worker-run "
                   "binding, live permitted run status, control holds, roster workload, "
                   "canonical claims, and exact runtime/tool/sandbox constraints. Advisory "
                   "only; heartbeat silence is never proof of safe ownership.",
        "permissions": "any principal with project access, when the operator policy is "
                       "enabled; a query grant is never dispatch authority",
        "inputs": "RoutingQuery {task_id, candidate_agent_ids?}",
        "outputs": "{eligible[ranked], rejected[{agent_id, reason}], blocked, advisory}",
        "limits": f"candidates bounded to policy max_candidates (<= {MAX_CANDIDATES}); "
                  "workload counts come only from the privacy-filtered roster; rejection "
                  "reasons are stable codes with no cross-project or private data",
        "retry_semantics": "Read-only advisory; safe to retry."},
    "capability_routing.dispatch": {
        "name": "capability_routing.dispatch",
        "method": "POST",
        "endpoint": "/v1/projects/{project_id}/routing:dispatch",
        "request_model": "RoutingDispatch",
        "purpose": "Record a Nexus-fenced durable approval of one exact (task, agent, run) "
                   "pair as a canonical routing_decision (IDs/fence/reason only). The "
                   "approval is advisory: the worker still dispatches itself canonically, "
                   "or a parent hook consumes the pending approval and drives "
                   "Scheduling.dispatch_exact under the same gates. No agent impersonation.",
        "permissions": "admin/human, or a coordinator principal fenced by the Nexus lease "
                       "with a matching epoch; requires the enabled operator policy",
        "inputs": "RoutingDispatch {task_id, agent_id, run_id, coordinator_epoch?, reason}",
        "outputs": "{kind: durable_approval, decision, gate, fence, authority}",
        "limits": "the authoritative dispatch gate re-verifies raw run/control/peer "
                  "bindings at approval time; unverifiable state is rejected, never "
                  "treated as available",
        "retry_semantics": "Idempotent via Idempotency-Key; replay returns the recorded "
                           "decision without re-approving."},
    "capability_routing.declare": {
        "name": "capability_routing.declare",
        "method": "POST",
        "endpoint": "/v1/agents/{agent_id}/routing-declaration",
        "request_model": "RuntimeDeclaration",
        "purpose": "Declare the agent's approved runtime name, canonical tool constraints, "
                   "and advisory sandbox/network intent. Runtime names come only from the "
                   "approved runtime registry. CAS-fenced and frozen while the agent has "
                   "in-flight bound work.",
        "permissions": "the agent itself or an administrator; requires enabled policy",
        "inputs": "RuntimeDeclaration {runtime, tools, strict_sandbox, network, "
                  "expected_revision?}",
        "outputs": "the canonical routing_declaration resource",
        "limits": f"tools bounded to {MAX_DECLARED_TOOLS} canonical tool names; the "
                  "strict_sandbox flag is intent only — eligibility requires the "
                  "operator's run-bound launch-attestation verifier at route time",
        "retry_semantics": "Idempotent via Idempotency-Key; latest declaration wins."},
}


def descriptors():
    """Self-describing operation metadata for parent discovery/MCP registration."""
    return {name: dict(descriptor) for name, descriptor in OPERATION_DESCRIPTORS.items()}


def install(app, service, principal, command, envelope, policy=None):
    """Register capability-routing endpoints on the application built by ``create_app``."""
    from .agent_feature_gates import set_feature_gates
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    routing = CapabilityRouting(service, policy)
    app.state.capability_routing = routing
    service.capability_routing = routing
    if routing.policy.enabled:
        set_feature_gates(service, "capability_routing", dispatch=routing.gate_callback,
                          start=routing.start_gate)
    elif "capability_routing" in getattr(service, "_agent_feature_gates", {}):
        set_feature_gates(service, "capability_routing")
    endpoints = []

    @app.post("/v1/projects/{project_id}/routing:query")
    async def routing_query(project_id: str, body: RoutingQuery, p=Depends(principal)):
        return envelope(routing.query(p, project_id, body))

    @app.post("/v1/projects/{project_id}/routing:dispatch")
    async def routing_dispatch(project_id: str, body: RoutingDispatch, p=Depends(principal),
                               idempotency_key: str | None = Header(None)):
        return command(p, "routing-dispatch:" + project_id, idempotency_key, body,
                       lambda: routing.fenced_dispatch(p, project_id, body))

    @app.post("/v1/agents/{agent_id}/routing-declaration")
    async def routing_declaration_set(agent_id: str, body: RuntimeDeclaration, p=Depends(principal),
                                      idempotency_key: str | None = Header(None)):
        return command(p, "routing-declaration:" + agent_id, idempotency_key, body,
                       lambda: routing.set_declaration(p, agent_id, body))

    @app.get("/v1/agents/{agent_id}/routing-declaration")
    async def routing_declaration(agent_id: str, p=Depends(principal)):
        return envelope(routing.declaration_view(p, agent_id))

    endpoints.append("POST /v1/projects/{project_id}/routing:query")
    endpoints.append("POST /v1/projects/{project_id}/routing:dispatch")
    endpoints.append("POST /v1/agents/{agent_id}/routing-declaration")
    endpoints.append("GET /v1/agents/{agent_id}/routing-declaration")
    return {"endpoints": endpoints, "models": list(MODELS)}
