"""Policy-driven coordination: bounded escalation, reassignment, dead-lettering, and
declared-resource conflict handling beyond literal path claims.

This module is opt-in. Installing it adds endpoints but changes no scheduling behavior
until a project stores a ``coordination_policy`` record with ``enabled: true``. It owns
four record kinds, all created through ``Service.create`` inside ``Service.mutate`` so
ids, revisions, audit, and idempotency stay canonical:

``coordination_policy``
    One typed, validated, bounded policy record per project.
``resource_declaration``
    One record per task declaring read/write resource keys plus declared
    interface/dependency keys. Keys are normalized and, when they reference actual
    resources (``scope:``, ``artifact:``, ``dependency:``), authorized against the
    canonical store: forged, private, or cross-project references are rejected.
    ``interface:`` keys are explicit semantic labels and are clearly non-authoritative.
``coordination_action``
    Durable per-action intent/audit record. Every automatic or manual policy action is
    first recorded as ``intended`` and then marked ``applied``, ``refused``, or left
    ``manual_hold`` for an operator. Records are never deleted by this module.
``dead_letter``
    Terminal preservation record. The full task envelope (body, scheduling state,
    attempt and execution history) is snapshotted so nothing is lost. Dead-lettering
    never marks a task completed and never invents success.

Safety invariants (review-hardened):

- Heartbeat/presence age is NEVER authoritative termination, quiescence, or no-effect
  evidence. A dispatched task whose execution is silent, stale, paused, or whose run is
  missing is *ambiguous* and becomes a ``manual_hold``; nothing is archived, reassigned,
  or cleared on heartbeat thresholds alone. Absent capture/event data is never used to
  fabricate negative proof.
- Automatic reassignment only acts on work with no live execution: never-started
  canonical assignments (no execution record) or executions with binding-matched
  canonical settled evidence (a terminal task checkpoint applied under the current
  task/run/execution) or an authoritative adjudication from the installed evidence
  provider (the parent recovery/ops seam). Anything else is held.
- ``acknowledge_unknown_effects`` on a hold resolution is an acknowledgment, not
  evidence. It is recorded for audit but never bypasses the settled-evidence
  requirement for reassignment or dead-lettering. Operators may ``dismiss`` or
  ``escalate`` a hold without altering ownership; explicit ownership transfer for
  live work remains the existing scheduling ``:handoff`` surface.
- Dead-lettering never terminalizes an active or ambiguous execution. It requires an
  absent or canonically settled execution, preserves the original envelope in a
  ``dead_letter`` record, and otherwise reports the task as held.
- Resource declarations freeze once an execution exists: an agent cannot rewrite its
  declared effects mid-flight. Changes require a canonical new execution boundary
  (the execution must be archived by the canonical seams first).
- Claims are never stolen or released here.
- Non-admin ticks require the Nexus coordinator lease fence (``Service.coordinator``)
  with a matching epoch; expired epochs are rejected.
- Semantic conflicts are not auto-merged. Conflicts produce deterministic deferrals
  with actionable evidence; resolution stays with operators.
"""

import json
import re
import time
from datetime import datetime
from typing import Literal

from fastapi import Depends, Header
from pydantic import ConfigDict, Field, field_validator, model_validator

from . import models as m
from .scheduling import STALE_EXECUTION_SECONDS, Scheduling, as_int, as_number
from .service import TERMINAL
from .storage import Problem, now

# ---- bounds ----------------------------------------------------------------

TICK_DEFAULT_LIMIT = 50
TICK_MAX_LIMIT = 100
MAX_ESCALATIONS_LIMIT = 5
MAX_REASSIGNMENTS_LIMIT = 10
MAX_DEADLINE_SECONDS = 86400
MIN_DEADLINE_SECONDS = 30
MAX_COOLDOWN_SECONDS = 86400
MAX_CONFLICT_COOLDOWN_SECONDS = 3600
RECENT_ACTIONS_LIMIT = 20
RESOURCE_KEYS_LIMIT = 64
INTERFACE_KEYS_LIMIT = 32
POLICY_HISTORY_LIMIT = 20

# Prefixes that reference canonical resources and are authorized against the store.
AUTHORIZED_PREFIXES = ("scope", "artifact", "dependency")
# Explicit semantic label namespaces; ``interface`` participates in exact-key conflict
# detection but asserts nothing about real resources, and ``label`` is a hint namespace
# that never participates in conflict decisions. Both are non-authoritative.
LABEL_PREFIXES = ("interface", "label")
RESOURCE_PREFIXES = ("path",) + AUTHORIZED_PREFIXES + LABEL_PREFIXES

MODELS = ("PolicyUpdate", "ResourceDeclaration", "PolicyTick", "HoldResolution")

_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*:[^\s]+$")


def wall_time():
    """Single clock seam so tests can advance time deterministically."""
    return time.time()


def parse_time(value):
    """Parse an ISO-8601 ``Z`` timestamp to epoch seconds; None when unparseable."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _clean(value):
    return value.strip() if isinstance(value, str) else value


def normalize_resource_key(value):
    """Canonicalize a declared resource key; raises ValueError when malformed.

    Normalization is syntactic only: lowercase prefix, trimmed/collapsed ``path:``
    values with ``..`` rejected. Authorization of resource-referencing prefixes
    happens separately against the canonical store at declaration time.
    """
    if not isinstance(value, str):
        raise ValueError("Resource keys must be strings")
    key = value.strip()
    prefix, _, rest = key.partition(":")
    prefix = prefix.lower()
    if prefix not in RESOURCE_PREFIXES or not rest or any(c.isspace() for c in rest):
        raise ValueError(
            "Resource keys must be '<prefix>:<value>' with prefix in "
            f"{list(RESOURCE_PREFIXES)} and no whitespace")
    if prefix == "path":
        segments = [segment for segment in rest.split("/") if segment not in ("", ".")]
        if any(segment == ".." for segment in segments):
            raise ValueError("path: keys must not contain '..'")
        rest = "/".join(segments)
        if not rest:
            raise ValueError("path: keys must name a path")
    key = f"{prefix}:{rest}"
    if not _KEY_PATTERN.match(key) or len(key) > 320:
        raise ValueError("Resource keys are limited to 320 characters")
    return key


# ---- request models ---------------------------------------------------------


class PolicyUpdate(m.Model):
    """Opt-in, typed, bounded per-project coordination policy.

    Every field has hard bounds so a policy can never request unbounded retries,
    unbounded scans, or zero-cost automation. Defaults are the safest posture:
    disabled, no automatic reassignment, no automatic dead-lettering, and a required
    coordinator lease for non-admin ticks.
    """

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    enabled: bool = False
    max_escalations: int = Field(default=2, ge=0, le=MAX_ESCALATIONS_LIMIT)
    escalation_cooldown_seconds: int = Field(default=60, ge=0, le=MAX_COOLDOWN_SECONDS)
    attempt_deadline_seconds: int = Field(default=900, ge=MIN_DEADLINE_SECONDS,
                                          le=MAX_DEADLINE_SECONDS)
    dead_letter_after_escalations: int = Field(default=2, ge=1, le=MAX_ESCALATIONS_LIMIT)
    max_reassignments: int = Field(default=2, ge=0, le=MAX_REASSIGNMENTS_LIMIT)
    auto_reassign: bool = False
    auto_dead_letter: bool = False
    strict_resources: bool = False
    require_coordinator_lease: bool = True
    conflict_cooldown_seconds: int = Field(default=30, ge=0, le=MAX_CONFLICT_COOLDOWN_SECONDS)


class ResourceDeclaration(m.Model):
    """Explicit effect declaration for one task, scoped to the task's project.

    ``reads``/``writes`` hold normalized ``scope:``, ``path:``, ``artifact:``,
    ``dependency:``, or ``interface:`` keys. Keys referencing real resources are
    authorized at declaration time. ``interfaces``/``dependencies`` are treated as
    shared writes for conflict purposes. A key may not be both read and written.
    Declarations freeze while an execution exists for the task.
    """

    expected_task_revision: int = Field(ge=1)
    reads: list[str] = Field(default_factory=list, max_length=RESOURCE_KEYS_LIMIT)
    writes: list[str] = Field(default_factory=list, max_length=RESOURCE_KEYS_LIMIT)
    interfaces: list[str] = Field(default_factory=list, max_length=INTERFACE_KEYS_LIMIT)
    dependencies: list[str] = Field(default_factory=list, max_length=INTERFACE_KEYS_LIMIT)

    @field_validator("reads", "writes", "interfaces", "dependencies", mode="before")
    @classmethod
    def keys(cls, value):
        if not isinstance(value, list):
            raise ValueError("Resource keys must be an explicit list")
        return [normalize_resource_key(x) for x in value]

    @model_validator(mode="after")
    def consistent(self):
        for values, label in ((self.reads, "Reads"), (self.writes, "Writes"),
                              (self.interfaces, "Interfaces"), (self.dependencies, "Dependencies")):
            if len(set(values)) != len(values):
                raise ValueError(f"{label} must be unique per declaration")
        if set(self.reads) & set(self.writes):
            raise ValueError("A resource key cannot be both read and written")
        return self


class PolicyTick(m.Model):
    """One bounded evaluation pass. Non-admin callers must fence with the Nexus lease."""

    coordinator_epoch: int | None = Field(default=None, ge=1)
    limit: int = Field(default=TICK_DEFAULT_LIMIT, ge=1, le=TICK_MAX_LIMIT)


class HoldResolution(m.Model):
    """Operator decision for a ``manual_hold`` coordination action.

    ``dismiss`` and ``escalate`` never alter ownership. ``reassign`` and
    ``dead_letter`` require the execution to be absent or canonically settled;
    ``acknowledge_unknown_effects`` is recorded for audit but is NOT evidence and
    never bypasses that requirement.
    """

    expected_revision: int = Field(ge=1)
    action: Literal["reassign", "escalate", "dead_letter", "dismiss"]
    reason: str = Field(min_length=1, max_length=4096)
    acknowledge_unknown_effects: bool = False

    _clean_reason = field_validator("reason", mode="before")(_clean)


# ---- conflict evaluation (pure, deterministic) ------------------------------


def _effects(declaration):
    """Effective read/write sets; interfaces and dependencies act as shared writes.

    ``label:`` keys are hints and never participate in conflict detection.
    """
    def authoritative(keys):
        return {k for k in keys or [] if isinstance(k, str) and not k.startswith("label:")}

    body = declaration or {}
    reads = authoritative(body.get("reads"))
    writes = authoritative(body.get("writes"))
    writes |= authoritative(body.get("interfaces"))
    writes |= authoritative(body.get("dependencies"))
    return reads, writes


def declaration_conflicts(left, right):
    """Deterministic conflicts between two declarations, sorted by (kind, key).

    Exact normalized key equality only: this module never guesses path or interface
    semantics. read/read is never a conflict; write/write and write/read are.
    """
    left_reads, left_writes = _effects(left)
    right_reads, right_writes = _effects(right)
    conflicts = [{"kind": "write_write", "key": key}
                 for key in sorted(left_writes & right_writes)]
    conflicts += [{"kind": "write_read", "key": key}
                  for key in sorted((left_writes & right_reads) | (left_reads & right_writes))]
    return conflicts


# ---- domain -----------------------------------------------------------------


def policy_defaults(overrides=None):
    body = PolicyUpdate().model_dump()
    if overrides:
        body.update(PolicyUpdate(**{**body, **dict(overrides)}).model_dump())
    return body


class CoordinationPolicies:
    """Policy domain logic; call mutations inside ``Service.mutate``.

    ``evidence`` is an optional authoritative adjudication provider installed by the
    parent integration (the recovery/ops seam). Callable signature::

        evidence(principal, task_body) -> None | {
            "adjudication": "settled" | "active" | "ambiguous",
            "basis": {...},          # durable proof references
        }

    Returning ``None`` or ``"ambiguous"`` means no opinion; the execution is then
    treated as ambiguous and held. Heartbeat-derived judgments are never produced or
    accepted as no-effect evidence.
    """

    def __init__(self, service, defaults=None, evidence=None):
        self.service = service
        self.store = service.store
        self.scheduling = Scheduling(service)
        self.defaults = policy_defaults(defaults)
        self.evidence = evidence

    # ---- records ------------------------------------------------------------

    def _record(self, principal, project_id, kind, match):
        for record in self.store.list(kind, project_id, principal, 1000):
            if match(record):
                return record
        return None

    def _policy_record(self, principal, project_id):
        return self._record(principal, project_id, "coordination_policy",
                            lambda record: record.get("project_id") == project_id)

    def policy(self, principal, project_id):
        self.service.project_access(principal, project_id)
        record = self._policy_record(principal, project_id)
        merged = dict(self.defaults)
        if record:
            merged.update({key: record[key] for key in self.defaults if key in record})
            merged["id"] = record["id"]
            merged["revision"] = record["revision"]
        return merged

    def _declaration_record(self, principal, project_id, task_id):
        return self._record(principal, project_id, "resource_declaration",
                            lambda record: record.get("task_id") == task_id)

    def _actions(self, principal, project_id, limit=RECENT_ACTIONS_LIMIT):
        return self.store.list("coordination_action", project_id, principal, limit)

    # ---- policy administration ------------------------------------------------

    def set_policy(self, principal, project_id, request):
        self.service.project_access(principal, project_id)
        if principal["role"] not in ("admin", "human"):
            raise Problem("forbidden", 403)
        record = self._policy_record(principal, project_id)
        body = {**self.defaults, **request.model_dump()}
        if record is None:
            return self.service.create(principal, "coordination_policy", {
                "project_id": project_id, **body, "updated_by": principal["id"]})
        current = json.loads(self.store.raw(record["id"], "coordination_policy")["body"])
        current.update(body, updated_by=principal["id"])
        return self.store.save("coordination_policy", project_id, principal["id"],
                               current, expected=record["revision"])

    def status(self, principal, project_id):
        policy = self.policy(principal, project_id)
        counts = {"escalated": 0, "dead_lettered": 0, "manual_holds": 0, "deferred": 0}
        for task in self.store.list("task", project_id, principal, 1000):
            settings = self.scheduling._settings(task)
            if settings.get("escalated"):
                counts["escalated"] += 1
            if (settings.get("policy") or {}).get("dead_lettered"):
                counts["dead_lettered"] += 1
        for action in self.store.list("coordination_action", project_id, principal, 1000):
            if action.get("state") == "manual_hold":
                counts["manual_holds"] += 1
            elif action.get("action") == "defer" and action.get("state") == "applied":
                counts["deferred"] += 1
        return {"project_id": project_id, "policy": policy, "counts": counts,
                "recent_actions": self._actions(principal, project_id)}

    # ---- resource declarations --------------------------------------------------

    def _authorize_key(self, principal, project_id, key):
        """Authorize keys that reference real resources; reject forged references."""
        prefix, _, value = key.partition(":")
        if prefix == "scope":
            self.service.reference(principal, project_id, value, "scope")
        elif prefix == "dependency":
            self.service.reference(principal, project_id, value, "task")
        elif prefix == "artifact":
            # Visibility-checked: private or cross-project artifacts are not found.
            artifact = self.store.get(value, principal, "artifact")
            if artifact["project_id"] != project_id:
                raise Problem("not_found", 404)
        # path:/interface:/label: keys are syntactic and non-authoritative.

    def declare_resources(self, principal, task_id, request):
        task = self.store.get(task_id, principal, "task")
        if task["status"] in TERMINAL:
            raise Problem("terminal_task")
        if task["revision"] != request.expected_task_revision:
            raise Problem("revision_conflict", 412)
        if task.get("execution") or self.scheduling._settings(task).get("state") == "dispatched":
            # Declarations freeze once effects may be running; rewriting them mid-flight
            # would erase evidence. A canonical new execution boundary (the archived
            # execution) unfreezes them.
            raise Problem("declaration_frozen", 409)
        assigned = self.scheduling._settings(task).get("assigned_agent_id")
        if principal["role"] not in ("admin", "human"):
            # Only the agent currently accountable for the task may declare its effects.
            if principal.get("agent") != assigned:
                raise Problem("forbidden", 403)
            self.service.represent(principal, assigned)
        project_id = task["project_id"]
        for key in [*request.reads, *request.writes, *request.interfaces,
                    *request.dependencies]:
            self._authorize_key(principal, project_id, key)
        record = self._declaration_record(principal, project_id, task_id)
        body = {"project_id": project_id, "task_id": task_id,
                "reads": list(request.reads), "writes": list(request.writes),
                "interfaces": list(request.interfaces),
                "dependencies": list(request.dependencies),
                "declared_by": principal["id"]}
        if record is None:
            return self.service.create(principal, "resource_declaration", body)
        current = json.loads(self.store.raw(record["id"], "resource_declaration")["body"])
        current.update(body)
        return self.store.save("resource_declaration", project_id, principal["id"],
                               current, expected=record["revision"])

    def resource_view(self, principal, task_id):
        task = self.store.get(task_id, principal, "task")
        record = self._declaration_record(principal, task["project_id"], task_id)
        return {"task_id": task_id, "project_id": task["project_id"],
                "declaration": record or None}

    # ---- dispatch seam: resource conflict gate --------------------------------

    def _in_flight(self, principal, project_id, exclude_task_id):
        peers = []
        for task in self.store.list("task", project_id, principal, 1000):
            if task["id"] == exclude_task_id or task.get("status") in TERMINAL:
                continue
            if self.scheduling._settings(task).get("state") == "dispatched":
                peers.append(task)
        return sorted(peers, key=lambda peer: str(peer["id"]))

    def resource_gate(self, principal, task, run):
        """Dispatch seam installed as ``service.coordination_gate``.

        Returns ``{"action": "allow"}`` or a deterministic ``defer`` verdict. It never
        releases claims, never marks work done, and never consumes attempt budget; the
        caller (scheduling dispatch) only records the deferral cooldown.
        """
        policy = self.policy(principal, task["project_id"])
        if not policy.get("enabled"):
            return {"action": "allow"}
        declaration = self._declaration_record(principal, task["project_id"], task["id"])
        cooldown = as_int(policy.get("conflict_cooldown_seconds"), 30)
        retry_after = wall_time() + cooldown
        if declaration is None:
            if not policy.get("strict_resources"):
                return {"action": "allow"}
            verdict = {"action": "defer", "reason": "undeclared_effects",
                       "retry_after": retry_after,
                       "evidence": {"detail": "Task declares no resource intents and the "
                                              "project policy is strict; effects are unknown."}}
            self._record_deferral(principal, task, verdict)
            return verdict
        conflicts, unknown_peers = [], []
        for peer in self._in_flight(principal, task["project_id"], task["id"]):
            other = self._declaration_record(principal, task["project_id"], peer["id"])
            if other is None:
                unknown_peers.append(peer["id"])
                continue
            for conflict in declaration_conflicts(declaration, other):
                conflicts.append({**conflict, "peer_task_id": peer["id"]})
        if conflicts:
            verdict = {"action": "defer", "reason": "resource_conflict",
                       "retry_after": retry_after,
                       "evidence": {"conflicts": conflicts,
                                    "detail": "Declared effects overlap an in-flight task; "
                                              "deferring deterministically instead of merging."}}
            self._record_deferral(principal, task, verdict)
            return verdict
        if unknown_peers and policy.get("strict_resources"):
            verdict = {"action": "defer", "reason": "peer_effects_unknown",
                       "retry_after": retry_after,
                       "evidence": {"peer_task_ids": unknown_peers,
                                    "detail": "In-flight tasks have no declarations under a "
                                              "strict policy; refusing to assume safety."}}
            self._record_deferral(principal, task, verdict)
            return verdict
        return {"action": "allow"}

    def _record_deferral(self, principal, task, verdict):
        settings = self.scheduling._settings(task)
        self.service.create(principal, "coordination_action", {
            "project_id": task["project_id"], "task_id": task["id"], "action": "defer",
            "state": "applied", "reason": verdict["reason"], "evidence": verdict["evidence"],
            "retry_after": verdict["retry_after"], "attempt": as_int(settings.get("attempts"), 0),
            "created_by": principal["id"]})

    # ---- execution adjudication -------------------------------------------------

    def _run_present(self, principal, execution):
        """Positive presence check only: a verifiably live run means 'do not touch'.

        Freshness is never used in the opposite direction: silence, staleness, or a
        missing run is ambiguity, not evidence of termination or no-effects.
        """
        run_id = (execution or {}).get("run_id")
        if not isinstance(run_id, str):
            return False
        try:
            run = self.store.get(run_id, principal, "run")
        except Problem:
            return False
        if run["status"] in TERMINAL:
            return False
        last_seen = as_number(run.get("last_seen_at"))
        return last_seen is not None and last_seen >= wall_time() - STALE_EXECUTION_SECONDS

    def adjudicate(self, principal, task):
        """Classify the task's execution as absent/settled/active/ambiguous.

        ``absent``
            No execution record: never-started or already archived by canonical seams.
        ``settled``
            A canonical terminal checkpoint was applied under the current
            task/run/execution (the task status is terminally recorded with the
            execution still attached), or the installed evidence provider adjudicates
            the execution settled with durable basis.
        ``active``
            The executing run is verifiably present (fresh heartbeat). Automation must
            not interfere.
        ``ambiguous``
            Everything else: silent/stale/paused/missing runtimes. Heartbeat age is
            not termination or no-effect evidence, so these tasks are held.
        """
        execution = task.get("execution")
        if not execution:
            return {"state": "absent",
                    "proof": {"detail": "No execution record; the work never started or "
                                        "was already archived by a canonical seam."}}
        if task.get("status") in ("completed", "failed"):
            # task_progress only applies a terminal status to the currently bound
            # task/run/execution, so this is binding-matched canonical evidence.
            return {"state": "settled",
                    "proof": {"basis": "terminal_task_checkpoint",
                              "task_status": task.get("status"),
                              "execution_id": execution.get("id"),
                              "run_id": execution.get("run_id"),
                              "checkpoint": execution.get("checkpoint")}}
        if self.evidence is not None:
            provided = self.evidence(principal, task)
            if isinstance(provided, dict) and provided.get("adjudication") in (
                    "settled", "active", "ambiguous"):
                return {"state": provided["adjudication"],
                        "proof": {"basis": "evidence_provider",
                                  "provider": {k: v for k, v in provided.items()
                                               if k != "adjudication"}}}
        if self._run_present(principal, execution):
            return {"state": "active",
                    "proof": {"basis": "run_presence",
                              "run_id": execution.get("run_id")}}
        return {"state": "ambiguous",
                "proof": {"detail": "Execution exists without a terminal applied checkpoint "
                                    "or authoritative adjudication; heartbeat silence, "
                                    "staleness, pause, or a missing run is not evidence of "
                                    "termination or of no effects.",
                          "run_id": execution.get("run_id")}}

    # ---- tick -----------------------------------------------------------------

    def _authorize_tick(self, principal, project_id, request, policy):
        self.service.project_access(principal, project_id)
        if principal["role"] in ("admin", "human"):
            return {"fenced_by": "operator"}
        # Automated coordination requires the Nexus lease fence with a live epoch.
        if not policy.get("require_coordinator_lease", True):
            if not principal.get("coordinator"):
                raise Problem("coordinator_grant_required", 403)
            return {"fenced_by": "coordinator_grant"}
        if request.coordinator_epoch is None:
            raise Problem("coordinator_epoch_required", 422)
        lease = self.service.coordinator(principal, project_id, request.coordinator_epoch)
        return {"fenced_by": "nexus_lease", "lease_id": lease["id"],
                "coordinator_epoch": lease["coordinator_epoch"]}

    def tick(self, principal, project_id, request):
        """One bounded policy evaluation pass over a project's tasks.

        Runs inside ``Service.mutate`` so a replayed idempotency key returns the cached
        outcome without reapplying actions. Processes at most ``request.limit`` tasks.
        """
        policy = self.policy(principal, project_id)
        fence = self._authorize_tick(principal, project_id, request, policy)
        if not policy.get("enabled"):
            return {"project_id": project_id, "evaluated": 0, "actions": [], "fence": fence,
                    "detail": "Policy disabled; tick is a no-op."}
        evaluated, actions, scanned = 0, [], 0
        for task in self.store.list("task", project_id, principal, 1000):
            if evaluated >= request.limit:
                break
            scanned += 1
            decision = self._evaluate_task(principal, task, policy)
            if decision is None:
                continue
            evaluated += 1
            actions.append(self._apply_decision(principal, task, decision, policy))
        return {"project_id": project_id, "evaluated": evaluated, "scanned": scanned,
                "actions": actions, "fence": fence}

    def _evaluate_task(self, principal, task, policy):
        """Decide what a task needs, or None when nothing is actionable."""
        if task.get("status") in ("completed", "cancelled"):
            return None
        settings, state = self._policy_state(task)
        if state["dead_lettered"]:
            return None
        cooldown_until = state["cooldown_until"]
        if cooldown_until is not None and cooldown_until > wall_time():
            return None
        adjudication = self.adjudicate(principal, task)
        if adjudication["state"] == "active":
            return None  # Live work is never interfered with.
        if adjudication["state"] == "ambiguous":
            if settings.get("escalated"):
                # Never dead-letter or reassign an ambiguous execution, whatever the budget.
                return {"action": "manual_hold", "reason": "escalated_execution_ambiguous",
                        "evidence": {"adjudication": adjudication["proof"],
                                     "detail": "Escalation budget reached but the execution "
                                               "is ambiguous; held instead of dead-lettered."}}
            dispatched_at = parse_time(settings.get("last_dispatch_at"))
            deadline = as_int(policy.get("attempt_deadline_seconds"), 900)
            if settings.get("state") != "dispatched" or dispatched_at is None \
                    or dispatched_at + deadline > wall_time():
                return None
            return {"action": "manual_hold", "reason": "execution_liveness_unknown",
                    "evidence": {"adjudication": adjudication["proof"],
                                 "dispatched_at": settings.get("last_dispatch_at"),
                                 "detail": "Attempt deadline passed and the runtime is silent, "
                                           "stale, or missing; that is ambiguity, not proof. "
                                           "Operator review required."}}
        # Execution is absent or canonically settled.
        if settings.get("escalated"):
            # Count the live escalation too when it was not policy-driven (e.g. the core
            # attempt budget was exhausted), so the dead-letter threshold reflects reality.
            current = settings.get("escalation") or {}
            escalations = state["escalations"] + (0 if current.get("policy") else 1)
            threshold = min(as_int(policy.get("dead_letter_after_escalations"), 2),
                            MAX_ESCALATIONS_LIMIT)
            if escalations >= threshold and policy.get("auto_dead_letter"):
                return {"action": "dead_letter", "reason": "escalations_exhausted",
                        "evidence": {"escalations": escalations,
                                     "escalation": settings.get("escalation"),
                                     "adjudication": adjudication["proof"]}}
            return None  # Escalated tasks otherwise wait for an operator.
        if adjudication["state"] == "settled":
            if (policy.get("auto_reassign")
                    and as_int(settings.get("reassignments"), 0)
                    < as_int(policy.get("max_reassignments"), 0)):
                return {"action": "reassign", "reason": "execution_settled",
                        "evidence": {"adjudication": adjudication["proof"]}}
            return {"action": "escalate", "reason": "execution_settled_unrecovered",
                    "evidence": {"adjudication": adjudication["proof"]}}
        # Absent execution with a stale assignment: legitimately never-started work.
        if (settings.get("state") == "dispatched"):
            return {"action": "manual_hold", "reason": "execution_record_missing",
                    "evidence": {"detail": "Scheduling state is dispatched but no execution "
                                           "record exists; refusing to guess."}}
        if (settings.get("assigned_agent_id") and policy.get("auto_reassign")
                and as_int(settings.get("reassignments"), 0)
                < as_int(policy.get("max_reassignments"), 0)):
            return {"action": "reassign", "reason": "assignment_never_started",
                    "evidence": {"adjudication": adjudication["proof"],
                                 "assigned_agent_id": settings.get("assigned_agent_id")}}
        return None

    @staticmethod
    def _policy_state(task):
        settings = Scheduling._settings(task)
        state = settings.get("policy") if isinstance(settings.get("policy"), dict) else {}
        return settings, {"escalations": as_int(state.get("escalations"), 0),
                          "dead_lettered": bool(state.get("dead_lettered")),
                          "cooldown_until": as_number(state.get("cooldown_until")),
                          "history": [item for item in state.get("history") or []
                                      if isinstance(item, dict)][-POLICY_HISTORY_LIMIT:]}

    def _apply_decision(self, principal, task, decision, policy):
        """Record the intent durably, apply it, then mark the outcome."""
        intent = self.service.create(principal, "coordination_action", {
            "project_id": task["project_id"], "task_id": task["id"], "action": decision["action"],
            "state": "intended", "reason": decision["reason"], "evidence": decision["evidence"],
            "created_by": principal["id"]})
        try:
            outcome = self._execute(principal, task, decision, policy)
        except Problem as error:
            return self._finish_action(principal, intent, "refused",
                                       {"error": error.code, "detail": decision["reason"]})
        state = "manual_hold" if decision["action"] == "manual_hold" else "applied"
        return self._finish_action(principal, intent, state, outcome)

    def _finish_action(self, principal, intent, state, outcome):
        body = json.loads(self.store.raw(intent["id"], "coordination_action")["body"])
        body.update(state=state, outcome=outcome, finished_at=now())
        return self.store.save("coordination_action", body["project_id"], principal["id"],
                               body, expected=intent["revision"])

    def _execute(self, principal, task, decision, policy):
        body = self.scheduling._body(task["id"], "task")
        settings, state = self._policy_state(body)
        cooldown = as_int(policy.get("escalation_cooldown_seconds"), 60)
        action = decision["action"]
        execution = None
        if action == "manual_hold":
            # No task mutation: the hold record itself is the durable artifact.
            return {"held": True, "reason": decision["reason"]}
        if action == "escalate":
            # Flag-only: never archives an execution, never clears a run assignment.
            if state["escalations"] >= as_int(policy.get("max_escalations"), 0):
                raise Problem("policy_escalation_budget_exhausted", 422)
            settings.update(escalated=True, retry_after=None)
            settings["escalation"] = {"reason": decision["reason"], "at": now(),
                                      "agent_id": settings.get("assigned_agent_id"),
                                      "policy": True, "evidence": decision["evidence"]}
            state["escalations"] += 1
        elif action == "reassign":
            adjudication = self.adjudicate(principal, body)
            if adjudication["state"] not in ("absent", "settled"):
                # Re-adjudicated inside the mutation: active or ambiguous executions are
                # never reassigned, whatever the caller acknowledged.
                raise Problem("execution_not_settled", 409)
            if as_int(settings.get("reassignments"), 0) >= as_int(policy.get("max_reassignments"), 0):
                raise Problem("policy_reassignment_budget_exhausted", 422)
            body["status"] = "queued"
            settings.update(state="queued", escalated=False, escalation=None,
                            retry_after=None,
                            reassignments=as_int(settings.get("reassignments"), 0) + 1,
                            last_assigned_agent_id=settings.get("assigned_agent_id"),
                            assigned_agent_id=None)
            execution = self.scheduling._archive_execution(
                body, {"action": "policy_reassign", "reason": decision["reason"],
                       "at": now(), "by": principal["id"],
                       "adjudication": adjudication["proof"]})
        elif action == "dead_letter":
            adjudication = self.adjudicate(principal, body)
            if adjudication["state"] not in ("absent", "settled"):
                # Never terminalize an active or ambiguous execution.
                raise Problem("execution_not_settled", 409)
            execution = self.scheduling._archive_execution(
                body, {"action": "policy_dead_letter", "reason": decision["reason"],
                       "at": now(), "by": principal["id"],
                       "adjudication": adjudication["proof"]})
            if body.get("status") not in TERMINAL:
                body["status"] = "failed"  # Terminal non-success; never invented success.
            settings.update(state="dead_lettered", escalated=True, retry_after=None)
            settings["escalation"] = {"reason": "dead_lettered", "at": now(),
                                      "detail": decision["reason"], "policy": True}
            state["dead_lettered"] = True
        else:
            raise Problem("policy_action_unknown", 422)
        state["cooldown_until"] = wall_time() + cooldown
        state["history"] = [*state["history"], {"action": action, "reason": decision["reason"],
                                                "at": now(), "by": principal["id"]}][-POLICY_HISTORY_LIMIT:]
        settings["policy"] = state
        body["scheduling"] = settings
        saved = self.store.save("task", body["project_id"], principal["id"], body,
                                expected=body["revision"])
        outcome = {"task_id": task["id"], "task_revision": saved["revision"],
                   "status": saved["status"]}
        if action == "reassign" and execution:
            # Clear only the run's task pointer; work claims are never touched.
            self.scheduling._clear_run_assignment(principal, execution.get("run_id"),
                                                  task["id"])
        if action == "dead_letter":
            letter = self.service.create(principal, "dead_letter", {
                "project_id": task["project_id"], "task_id": task["id"],
                "reason": decision["reason"], "envelope": saved,
                "created_by": principal["id"]})
            outcome["dead_letter_id"] = letter["id"]
        return outcome

    # ---- manual hold resolution -------------------------------------------------

    def resolve_hold(self, principal, action_id, request):
        record = self.store.get(action_id, principal, "coordination_action")
        self.service.project_access(principal, record["project_id"])
        if principal["role"] not in ("admin", "human"):
            raise Problem("forbidden", 403)
        if record["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if record.get("state") != "manual_hold":
            raise Problem("action_not_held", 422)
        task = self.store.get(record["task_id"], principal, "task")
        policy = self.policy(principal, record["project_id"])
        if request.action == "dismiss":
            # Dismissal alters no ownership; it simply closes the hold.
            return self._finish_action(principal, record, "dismissed",
                                       {"reason": request.reason, "by": principal["id"]})
        adjudication = self.adjudicate(principal, task)
        if request.action in ("reassign", "dead_letter") \
                and adjudication["state"] not in ("absent", "settled"):
            # An acknowledgment is not evidence: ambiguous executions require
            # binding-matched canonical settlement or evidence-provider adjudication.
            # Ownership transfer for live work remains the scheduling :handoff surface.
            raise Problem("execution_not_settled", 409)
        decision = {"action": request.action, "reason": request.reason,
                    "evidence": {"manual": True, "by": principal["id"],
                                 "adjudication": adjudication["proof"],
                                 "acknowledge_unknown_effects":
                                     bool(request.acknowledge_unknown_effects),
                                 "original_hold": record["id"]}}
        outcome = self._execute(principal, task, decision, policy)
        outcome["resolution"] = request.action
        return self._finish_action(principal, record, "resolved", outcome)


# ---- agent-facing descriptors -------------------------------------------------


def descriptors():
    """Public, agent-consumable description of every operation this module exposes."""
    return [
        {"name": "coordination_policy.set", "method": "PUT",
         "endpoint": "/v1/projects/{project_id}/coordination-policy",
         "request_model": "PolicyUpdate",
         "purpose": "Install or replace the opt-in per-project coordination policy.",
         "permissions": "admin or human principal with project access",
         "evidence_required": "none; the policy record itself is the audit artifact",
         "inputs": "PolicyUpdate (all fields bounded; disabled by default)",
         "outputs": "The stored coordination_policy record",
         "limits": f"escalations<= {MAX_ESCALATIONS_LIMIT}, "
                   f"reassignments<= {MAX_REASSIGNMENTS_LIMIT}, "
                   f"deadline {MIN_DEADLINE_SECONDS}..{MAX_DEADLINE_SECONDS}s",
         "retry_semantics": "Idempotent via Idempotency-Key; replay returns the stored record."},
        {"name": "coordination_policy.status", "method": "GET",
         "endpoint": "/v1/projects/{project_id}/coordination-policy",
         "request_model": None,
         "purpose": "Read the effective policy plus escalation/dead-letter/hold counts and "
                    "the most recent coordination actions.",
         "permissions": "any principal with project access",
         "evidence_required": "none",
         "inputs": "project_id path parameter",
         "outputs": "{policy, counts, recent_actions}",
         "limits": f"recent_actions bounded to {RECENT_ACTIONS_LIMIT}",
         "retry_semantics": "Read-only; safe to retry."},
        {"name": "coordination_policy.tick", "method": "POST",
         "endpoint": "/v1/projects/{project_id}/coordination-policy:tick",
         "request_model": "PolicyTick",
         "purpose": "Run one bounded policy evaluation pass. Past-deadline attempts with "
                    "silent, stale, paused, or missing runtimes become manual holds "
                    "(heartbeat age is never termination or no-effect evidence). Only "
                    "never-started assignments or canonically settled executions are "
                    "automatically reassigned; escalation-exhausted tasks dead-letter "
                    "only with no active/ambiguous execution, preserving the envelope.",
         "permissions": "admin/human, or a coordinator principal fenced by the Nexus lease "
                        "with a matching epoch when the policy requires it",
         "evidence_required": "coordinator_epoch when ticking as a coordinator; "
                              "reassignment/dead-letter additionally require absent or "
                              "binding-matched settled execution evidence",
         "inputs": "PolicyTick {coordinator_epoch?, limit}",
         "outputs": "{evaluated, scanned, actions[], fence}",
         "limits": f"limit<= {TICK_MAX_LIMIT} tasks per tick; per-task cooldown "
                   "suppresses repeated automation",
         "retry_semantics": "Idempotent via Idempotency-Key; replay never re-applies actions. "
                            "Each action is its own durable coordination_action record."},
        {"name": "resources.declare", "method": "PUT",
         "endpoint": "/v1/tasks/{task_id}/resource-intents",
         "request_model": "ResourceDeclaration",
         "purpose": "Declare a task's read/write resource keys and interface/dependency "
                    "keys so conflicts can be decided by exact normalized key, beyond "
                    "literal path overlap. Declarations freeze while an execution exists.",
         "permissions": "admin/human, or the agent currently assigned to the task; "
                        "declarations are rejected while the task has an execution",
         "evidence_required": "expected_task_revision; scope:/artifact:/dependency: keys "
                              "are authorized against the canonical store (forged, "
                              "private, or cross-project references are rejected)",
         "inputs": "ResourceDeclaration {reads, writes, interfaces, dependencies}",
         "outputs": "The stored resource_declaration record",
         "limits": f"reads/writes<= {RESOURCE_KEYS_LIMIT} keys, "
                   f"interfaces/dependencies<= {INTERFACE_KEYS_LIMIT} keys, key length<= 320",
         "retry_semantics": "Idempotent via Idempotency-Key; optimistic concurrency via "
                            "expected_task_revision."},
        {"name": "resources.view", "method": "GET",
         "endpoint": "/v1/tasks/{task_id}/resource-intents",
         "request_model": None,
         "purpose": "Read a task's current resource declaration, if any.",
         "permissions": "any principal that can see the task",
         "evidence_required": "none",
         "inputs": "task_id path parameter",
         "outputs": "{task_id, project_id, declaration}",
         "limits": "none",
         "retry_semantics": "Read-only; safe to retry."},
        {"name": "coordination_action.resolve", "method": "POST",
         "endpoint": "/v1/coordination-actions/{action_id}:resolve",
         "request_model": "HoldResolution",
         "purpose": "Operator decision on a manual_hold action. dismiss/escalate never "
                    "alter ownership. reassign/dead_letter require an absent or "
                    "canonically settled execution; acknowledge_unknown_effects is audit "
                    "metadata, not evidence, and never bypasses that. Ownership transfer "
                    "for live work uses the scheduling :handoff endpoint instead.",
         "permissions": "admin or human principal",
         "evidence_required": "expected_revision of the action record; absent or "
                              "binding-matched settled execution for reassign/dead_letter",
         "inputs": "HoldResolution {action, reason, acknowledge_unknown_effects}",
         "outputs": "The updated coordination_action record (state 'resolved'/'dismissed')",
         "limits": "one resolution per hold; policy budgets still apply",
         "retry_semantics": "Idempotent via Idempotency-Key; optimistic concurrency via "
                            "expected_revision."},
        {"name": "coordination_action.get", "method": "GET",
         "endpoint": "/v1/coordination-actions/{action_id}",
         "request_model": None,
         "purpose": "Read one coordination action record with its intent, evidence, and outcome.",
         "permissions": "any principal with project access",
         "evidence_required": "none",
         "inputs": "action_id path parameter",
         "outputs": "The coordination_action record",
         "limits": "none",
         "retry_semantics": "Read-only; safe to retry."},
        {"name": "coordination_policy.descriptors", "method": "GET",
         "endpoint": "/v1/coordination-policy/descriptors",
         "request_model": None,
         "purpose": "Machine-readable description of every operation, its permissions, "
                    "evidence, limits, and retry semantics, for agent discovery/MCP "
                    "wiring. Conflict detection is exact-key over declared resources; "
                    "it does not attempt arbitrary semantic conflict solving.",
         "permissions": "any authenticated principal",
         "evidence_required": "none",
         "inputs": "none",
         "outputs": "list of operation descriptors",
         "limits": "none",
         "retry_semantics": "Read-only; safe to retry."},
    ]


def install(app, service, principal, command, envelope, policy=None, evidence=None, background=None):
    """Register coordination-policy endpoints on the application built by ``create_app``.

    ``policy`` optionally overrides install-time policy defaults (validated through
    ``PolicyUpdate``). ``evidence`` optionally installs the authoritative adjudication
    provider (see ``CoordinationPolicies``); without it, only canonical local records
    settle an execution. Installing also exposes ``service.coordination_gate``, which
    the scheduling dispatch seam consults when a project policy is enabled.
    """
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    policies = CoordinationPolicies(service, defaults=policy, evidence=evidence)
    app.state.coordination_policies = policies
    from .feature_scheduling import CoordinationRunner, CoordinationSchedule
    app.state.coordination_runner = CoordinationRunner(
        service, policies, CoordinationSchedule(**(background or {})))
    service.coordination_gate = policies.resource_gate
    endpoints = []

    @app.put("/v1/projects/{project_id}/coordination-policy")
    async def policy_set(project_id: str, body: PolicyUpdate, p=Depends(principal),
                         idempotency_key: str | None = Header(None)):
        return command(p, "policy-set:" + project_id, idempotency_key, body,
                       lambda: policies.set_policy(p, project_id, body))

    @app.get("/v1/projects/{project_id}/coordination-policy")
    async def policy_status(project_id: str, p=Depends(principal)):
        return envelope(policies.status(p, project_id))

    @app.post("/v1/projects/{project_id}/coordination-policy:tick")
    async def policy_tick(project_id: str, body: PolicyTick, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        return command(p, "policy-tick:" + project_id, idempotency_key, body,
                       lambda: policies.tick(p, project_id, body))

    @app.put("/v1/tasks/{task_id}/resource-intents")
    async def resources_declare(task_id: str, body: ResourceDeclaration, p=Depends(principal),
                                idempotency_key: str | None = Header(None)):
        return command(p, "policy-resources:" + task_id, idempotency_key, body,
                       lambda: policies.declare_resources(p, task_id, body))

    @app.get("/v1/tasks/{task_id}/resource-intents")
    async def resources_view(task_id: str, p=Depends(principal)):
        return envelope(policies.resource_view(p, task_id))

    @app.post("/v1/coordination-actions/{action_id}:resolve")
    async def action_resolve(action_id: str, body: HoldResolution, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        return command(p, "policy-hold:" + action_id, idempotency_key, body,
                       lambda: policies.resolve_hold(p, action_id, body))

    @app.get("/v1/coordination-actions/{action_id}")
    async def action_get(action_id: str, p=Depends(principal)):
        return envelope(policies.store.get(action_id, p, "coordination_action"))

    @app.get("/v1/coordination-policy/descriptors")
    async def policy_descriptors(p=Depends(principal)):
        return envelope(descriptors())

    endpoints.append("PUT /v1/projects/{project_id}/coordination-policy")
    endpoints.append("GET /v1/projects/{project_id}/coordination-policy")
    endpoints.append("POST /v1/projects/{project_id}/coordination-policy:tick")
    endpoints.append("PUT /v1/tasks/{task_id}/resource-intents")
    endpoints.append("GET /v1/tasks/{task_id}/resource-intents")
    endpoints.append("POST /v1/coordination-actions/{action_id}:resolve")
    endpoints.append("GET /v1/coordination-actions/{action_id}")
    endpoints.append("GET /v1/coordination-policy/descriptors")
    return {"endpoints": endpoints, "models": list(MODELS),
            "descriptors": [entry["name"] for entry in descriptors()]}
