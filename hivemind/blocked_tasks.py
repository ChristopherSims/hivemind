"""Governed blocked-task resolution.

Agents and operators can ask *why* one canonical task cannot start right now
(missing artifacts, budget holds, permission/scope fences, unmet dependencies,
unavailable capacity) and file a durable, structured request for one specific
corrective action. The feature never fixes anything by itself: requests are
records routed to the responsible human/operator surfaces, and closure with
``resolved`` requires a FRESH canonical readiness explanation — never an
acknowledgment, an operator note, or arbitrary evidence.

Authority and honesty rules:

- Explanations are pure bounded canonical reads fenced to the exact
  (principal, project, task, optional run/agent) tuple: the composed parent
  gate registries (``task_dependency_gates`` / ``task_dispatch_gates``), the
  scheduling eligibility rules, the budget ledger snapshot (SQL aggregates
  only; no vendor budget helpers, counters, or heavy I/O are imported or
  invoked), an independent bounded dependency-cycle DFS, and RAW bounded
  canonical claim/control rows for availability (mirroring the capability
  router's raw approach). Gates are CALLED, never mutated; no attempt budget
  is consumed, no claim is created, no retry hint is persisted, and no model
  or native compute runs. Gate callbacks that raise or return malformed
  verdicts fail closed to a generic hold — they never become a 500 and never
  become permission.
- Readiness is honest: ``ready`` is true ONLY for a queued task with zero
  current blockers. Running, paused, terminal, or unverifiable-execution
  states are reported as explicit ``readiness`` states
  (``already_running``/``blocked``/``terminal``), never as ready. An
  execution's run id is returned only when that run is CURRENTLY visible to
  the caller; private or missing runs are redacted, and an unverifiable prior
  execution holds the task with a generic blocker.
- Privacy is fail-closed. Unknown, private, or unverifiable blockers collapse
  to generic codes. Raw availability checks disclose the EXISTENCE of a
  hidden conflicting claim or control as a generic hold — never the private
  holder identity, never an explicit scope-holder id, never cross-scope
  counts. Private dependency identities, hidden contract text/ids, and busy
  run/agent/control identities are never disclosed.
- Requests are durable canonical ``resolution_request`` resources created
  through the parent's command/idempotency seam. Actions are allowlisted
  (``artifact_needed``, ``budget_review``, ``dependency_review``,
  ``permission_review``, ``recovery_review``, ``operator_attention``) and
  carry an exact request fingerprint plus a fingerprint of the blocker
  snapshot they were filed against. Basis refs must be currently visible,
  revision-pinned, and non-private to the requester (private basis across a
  namespace boundary is rejected). An agent may REQUEST; it can never raise a
  budget, self-verify an artifact, release unknown effects, change a
  permission, or reassign a claim through this surface.
- Every read — including idempotent mutation replays — is re-authorized and
  re-redacted against CURRENT state: the task, the request resource itself,
  the requester principal, and every basis reference. Unavailable basis ids
  and source-derived notes are redacted on all reads.
- Triage (acknowledge/resolve/decline) requires an explicit operator
  (admin/human) or the responsible actor (the canonical task owner); the
  requester may only cancel. Acknowledgment is NOT resolution. ``resolved``
  requires a fresh canonical proof that the REQUESTED condition cleared,
  scoped by the request's frozen candidate binding (an explicit run/agent the
  caller supplied at filing — binding grants no authority and is
  re-authorized at close): candidate-scoped actions (``artifact_needed``,
  ``permission_review``, ``recovery_review``, ``operator_attention``) need a
  candidate-scope zero-blocker explanation; ``dependency_review`` may be
  proven task-only once all bounded dependencies and gates clear;
  ``budget_review`` re-snapshots the ledger and rejects unknown/open/expired
  holds even when the task is completed; source-hook (unknown
  effects/recovery) blockers are never cleared by task metadata; terminal
  ``completed`` proves only the canonical task condition. No evidence
  override, acknowledgment, or operator intent is ever accepted as proof.
  ``declined`` and ``request_canceled`` are distinct, explicitly-not-fixed
  outcomes. Acknowledge/close read the request row once and publish with CAS
  on that captured revision inside the command transaction, so a concurrent
  update conflicts instead of being silently overwritten.

Everything is deterministic and local: the read-only explanation surface is
on by default once installed; durable requests require the explicit opt-in
``BlockedTasksPolicy(requests_enabled=True)``. All handlers are pure bounded
SQL under the canonical lock, so no handler is marked detached.
"""
import json
import time

from fastapi import Depends, Header, Query
from pydantic import Field

from . import models as m
from .scheduling import Scheduling, as_number
from .storage import Problem, digest, now

VERSION = "blocked-tasks-v2"
MAX_DEPENDENCIES = 64
MAX_CYCLE_NODES = 64
MAX_CYCLE_EDGES = 256
MAX_BASIS_REFS = 8
MAX_LIST = 200
MAX_RAW_ROWS = 1000
MAX_NOTE = 1024
DEFAULT_MAX_REQUESTS_PER_TASK = 64

REQUEST_ACTIONS = ("artifact_needed", "budget_review", "dependency_review",
                   "permission_review", "recovery_review", "operator_attention")
CLOSE_OUTCOMES = ("resolved", "declined", "request_canceled")
TERMINAL_TASK_STATUSES = frozenset({"completed", "failed", "cancelled"})
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
PENDING_CONTROL_STATES = ("queued", "acknowledged", "applying")

GENERIC_CODE = "blocker_unavailable"

# Gate verdict reason -> (blocker code, suggested remediation action). Only
# privacy-safe, stable codes appear here; anything else (and every private
# reason) collapses to the generic blocker.
_REASON_MAP = {
    # task_budgets gate/snapshot reasons
    "budget_not_configured": ("budget_not_configured", "budget_review"),
    "budget_settlement_unknown": ("budget_settlement_unknown", "budget_review"),
    "budget_reservation_outstanding": ("budget_reservation_outstanding", "budget_review"),
    "token_budget_insufficient": ("token_budget_insufficient", "budget_review"),
    "time_budget_exhausted": ("time_budget_exhausted", "budget_review"),
    "cost_budget_insufficient": ("cost_budget_insufficient", "budget_review"),
    # artifact contract / dependency gate reasons
    "dependency_cycle": ("dependency_cycle", "dependency_review"),
    "dependency_unavailable": ("dependency_unavailable", "dependency_review"),
    "submission_missing": ("artifact_missing", "artifact_needed"),
    "contract_revoked": ("artifact_contract_revoked", "artifact_needed"),
    "contract_revised": ("artifact_contract_revised", "artifact_needed"),
    "execution_binding_stale": ("artifact_binding_stale", "artifact_needed"),
    "contract_unsatisfied": ("artifact_contract_unsatisfied", "artifact_needed"),
    "artifact_deleted": ("artifact_deleted", "artifact_needed"),
    "artifact_superseded": ("artifact_superseded", "artifact_needed"),
    "artifact_checksum_mismatch": ("artifact_checksum_mismatch", "artifact_needed"),
    "artifact_cross_project": ("artifact_cross_project", "artifact_needed"),
    # capability routing gate reasons
    "capability_mismatch": ("capability_mismatch", "operator_attention"),
    "runtime_not_allowed": ("runtime_not_allowed", "operator_attention"),
    "runtime_mismatch": ("runtime_mismatch", "operator_attention"),
    "tool_constraint_mismatch": ("tool_constraint_mismatch", "operator_attention"),
    "strict_sandbox_unsupported": ("strict_sandbox_unsupported", "operator_attention"),
    "invalid_routing_requirements": ("invalid_routing_requirements", "operator_attention"),
    "agent_busy": ("agent_busy", "operator_attention"),
    "control_hold": ("control_hold", "operator_attention"),
    "no_live_run": ("no_live_run", "recovery_review"),
    "worker_run_required": ("worker_run_required", "operator_attention"),
    "task_not_dispatchable": ("task_not_dispatchable", "operator_attention"),
    "attempt_budget_exceeded": ("attempt_budget_exhausted", "recovery_review"),
    "assignment_mismatch": ("assignment_mismatch", "operator_attention"),
    "handoff_target_mismatch": ("handoff_target_mismatch", "operator_attention"),
    # permit / claim fence Problem codes
    "scope_not_owned": ("scope_not_owned", "permission_review"),
    "path_outside_scope": ("path_outside_scope", "permission_review"),
    "control_pending": ("control_hold", "operator_attention"),
    "mutation_not_permitted": ("run_not_ready", "recovery_review"),
    "run_not_ready": ("run_not_ready", "recovery_review"),
}

# Reasons that must never surface: they would identify a private or deleted
# resource. They collapse to the generic blocker with no identifier.
_PRIVATE_REASONS = frozenset({
    "contract_not_visible", "artifact_not_visible", "contract_deleted_unreleased",
    "agent_not_visible", "agent_not_available",
})

_SUMMARY = {
    GENERIC_CODE: "A blocker exists but its details are not visible to this principal.",
    "task_paused": "The task is paused and cannot be dispatched.",
    "escalated_hold": "Scheduling is escalated for this task and awaits operator review.",
    "attempt_budget_exhausted": "The task's attempt budget is exhausted.",
    "retry_backoff": "A deterministic retry backoff is still in effect.",
    "dependency_incomplete": "A declared dependency is not completed.",
    "dependency_unverified": "A completed dependency lacks the required independent verification.",
    "dependency_cycle": "The declared dependency graph contains a cycle or exceeds its bound.",
    "dependency_unavailable": "A declared dependency is unavailable.",
    "budget_not_configured": "Budgets are enforced but no budget is configured for this task.",
    "budget_settlement_unknown": "An unknown or unsettled budget hold blocks this task.",
    "budget_reservation_outstanding": "An open budget reservation blocks this task.",
    "token_budget_insufficient": "The task's token budget cannot cover the mandatory output reserve.",
    "time_budget_exhausted": "The task's time budget is exhausted.",
    "cost_budget_insufficient": "The task's cost budget is exhausted.",
    "artifact_missing": "A required artifact submission is missing.",
    "artifact_contract_revoked": "A required artifact contract was revoked.",
    "artifact_contract_revised": "A required artifact contract was revised; prior evidence no longer applies.",
    "artifact_binding_stale": "Artifact evidence is bound to a stale execution.",
    "artifact_contract_unsatisfied": "A required artifact contract is unsatisfied.",
    "artifact_deleted": "A required artifact was deleted.",
    "artifact_superseded": "A required artifact was superseded by a newer revision.",
    "artifact_checksum_mismatch": "A required artifact no longer matches its pinned checksum.",
    "artifact_cross_project": "A required artifact is bound to another project.",
    "capability_mismatch": "The run's agent lacks a capability the task requires.",
    "runtime_not_allowed": "The run's runtime is not allowed for this task.",
    "runtime_mismatch": "The run's runtime does not match the task requirement.",
    "tool_constraint_mismatch": "The run's tool constraints do not satisfy the task.",
    "strict_sandbox_unsupported": "The task requires a strict sandbox the run cannot attest.",
    "invalid_routing_requirements": "The task's routing requirements are invalid.",
    "agent_busy": "The agent is bound to other work.",
    "control_hold": "A control transition holds the run.",
    "control_state_unavailable": "A control state exists that cannot be verified by this principal.",
    "no_live_run": "The run is not live.",
    "worker_run_required": "Only a task-worker run can take this task.",
    "task_not_dispatchable": "The task is not currently dispatchable.",
    "assignment_mismatch": "The task is assigned to a different agent.",
    "handoff_target_mismatch": "A pending handoff targets a different agent or run.",
    "scope_not_owned": "The run does not hold the task's scopes.",
    "path_outside_scope": "Task paths fall outside the granted scopes.",
    "run_not_ready": "The run is not in a runnable state.",
    "scope_claimed": "The task's scopes conflict with another active claim.",
    "scope_conflict_unavailable": "A conflicting claim or scope exists that this principal cannot verify.",
    "execution_active": "A prior execution is still active.",
    "execution_interrupted": "A prior execution's run ended without completing the task.",
    "budget_hold": "A budget hold is reported for this task.",
    "budget_unknown": "Budget state for this task is reported unknown.",
    "budget_exhausted": "The task budget is reported exhausted.",
    "recovery_required": "Recovery review is reported as required.",
    "recovery_in_progress": "Recovery is reported in progress.",
    "effects_unknown": "Unknown effects are reported for this task.",
}

# Operator hook reason classes -> (blocker code, suggested action).
_HOOK_REASONS = {
    "budget_hold": ("budget_hold", "budget_review"),
    "budget_unknown": ("budget_unknown", "budget_review"),
    "budget_exhausted": ("budget_exhausted", "budget_review"),
    "recovery_required": ("recovery_required", "recovery_review"),
    "recovery_in_progress": ("recovery_in_progress", "recovery_review"),
    "effects_unknown": ("effects_unknown", "recovery_review"),
}

MODELS = ("ResolutionRequestCreate", "ResolutionAcknowledge", "ResolutionClose")

OPERATION_DESCRIPTORS = [
    {
        "name": "blocked_tasks.explain",
        "method": "GET",
        "endpoint": "/v1/projects/{project_id}/tasks/{task_id}/blocked-status",
        "request_model": None,
        "purpose": "Explain, from current canonical authority, the composed parent gate "
                   "registries, an independent bounded cycle check, and RAW bounded "
                   "claim/control availability rows, why one exact task cannot start "
                   "right now. Fenced to the exact (principal, project, task, run/agent) "
                   "tuple; a read-only snapshot, never execution permission.",
        "permissions": "any principal that can currently see the task; run-dependent "
                       "gates require the caller's own authorized run",
        "inputs": "path project_id/task_id; optional run_id/agent_id query",
        "outputs": "{ready, readiness, readiness_scope, run_checks_evaluated, "
                   "blockers[{code, action, summary, source, provenance}], "
                   "suggested_actions, gates_evaluated}; ready is true only for a "
                   "queued task with zero blockers; gates_evaluated lists only the "
                   "phases that actually ran; without a candidate run the scope is "
                   "task_only and no dispatch/permit/claim permission is claimed; "
                   "private/unverifiable state collapses to generic codes with no "
                   "identifiers",
        "limits": f"dependencies<= {MAX_DEPENDENCIES}, cycle DFS nodes<= "
                  f"{MAX_CYCLE_NODES}/edges<= {MAX_CYCLE_EDGES} (fail closed), raw "
                  f"claim/control rows<= {MAX_RAW_ROWS}; gates are pure bounded reads; "
                  "gate exceptions/malformed verdicts become generic holds, never 500s",
        "retry_semantics": "Read-only; safe to retry. Each call re-reads canonical state.",
        "default_enabled": True,
    },
    {
        "name": "blocked_tasks.request",
        "method": "POST",
        "endpoint": "/v1/projects/{project_id}/blocked-tasks:request",
        "request_model": "ResolutionRequestCreate",
        "purpose": "Durably record one structured, allowlisted corrective-action request "
                   "with exact request and source-blocker fingerprints. A request never "
                   "raises budgets, self-verifies artifacts, releases unknown effects, "
                   "changes permissions, or reassigns claims.",
        "permissions": "any principal that can currently see the task; basis refs must "
                       "be visible, revision-current, and non-private to the requester; "
                       "mutation requires the explicit operator opt-in policy",
        "inputs": "ResolutionRequestCreate {task_id, expected_task_revision, action, "
                  "note?, basis?, run_id?/agent_id? frozen candidate binding — "
                  "explicit IDs only, re-authorized at every use, never authority}",
        "outputs": "canonical resolution_request (state open) with fingerprints, "
                   "redacted per current state on every response including replays",
        "limits": f"note<= {MAX_NOTE} chars, basis<= {MAX_BASIS_REFS} canonical refs, "
                  "open requests per task<= policy bound",
        "retry_semantics": "Idempotent via Idempotency-Key; replay re-authorizes and "
                           "re-redacts against current state.",
        "default_enabled": False,
    },
    {
        "name": "blocked_tasks.triage",
        "method": "POST",
        "endpoint": "/v1/blocked-task-requests/{request_id}:acknowledge|:resolve",
        "request_model": "ResolutionAcknowledge | ResolutionClose",
        "purpose": "Operator/responsible-actor triage with single-read CAS publish. "
                   "Acknowledgment is not resolution. 'resolved' requires a fresh "
                   "canonical proof that the REQUESTED condition cleared: candidate-"
                   "scoped actions (artifact_needed/permission_review/recovery_review/"
                   "operator_attention) need the frozen candidate run re-authorized "
                   "with a candidate-scope zero-blocker explanation; dependency_review "
                   "may be proven task-only; budget_review re-snapshots the ledger and "
                   "rejects unknown/open/expired holds even when completed; source-hook "
                   "(unknown effects/recovery) blockers are never cleared by task "
                   "metadata; terminal 'completed' proves only the canonical task "
                   "condition. 'declined' and 'request_canceled' are distinct "
                   "explicitly-not-fixed outcomes.",
        "permissions": "acknowledge/decline/resolve: admin/human or the canonical task "
                       "owner; request_canceled: the requester (or an operator)",
        "inputs": "ResolutionClose {outcome, note?}",
        "outputs": "the updated canonical resolution_request, redacted per current state",
        "limits": "closed requests reject further changes (idempotent same-outcome "
                  "replay excepted); publish is CAS on the captured revision inside the "
                  "command transaction",
        "retry_semantics": "Idempotent via Idempotency-Key and same-outcome replay.",
        "default_enabled": False,
    },
]


def describe_operations():
    return {"version": VERSION, "descriptors": [dict(op) for op in OPERATION_DESCRIPTORS]}


class BlockedTasksPolicy:
    """Operator configuration. Read-only explanations default on; durable
    requests require explicit opt-in. ``budget_source`` / ``recovery_source`` /
    ``extra_sources`` are parent-injected callables
    ``(service, principal, task) -> None | {"blocked": bool, "reason_class": str,
    "summary": str?}`` reporting GENERIC reason classes only. Hooks are guarded:
    exceptions and malformed output collapse to a generic blocker. This module
    never imports worker, budget, or recovery implementation modules."""
    def __init__(self, *, explain_enabled=True, requests_enabled=False,
                 max_requests_per_task=DEFAULT_MAX_REQUESTS_PER_TASK,
                 budget_source=None, recovery_source=None, extra_sources=()):
        for flag in (explain_enabled, requests_enabled):
            if type(flag) is not bool:
                raise ValueError("Blocked-task policy flags must be booleans")
        if (type(max_requests_per_task) is not int
                or not 1 <= max_requests_per_task <= 1000):
            raise ValueError("Request bound must be an integer in [1, 1000]")
        for hook in (budget_source, recovery_source, *(extra_sources or ())):
            if hook is not None and not callable(hook):
                raise ValueError("Blocked-task source hooks must be callables")
        self.explain_enabled = explain_enabled
        self.requests_enabled = requests_enabled
        self.max_requests_per_task = max_requests_per_task
        self.budget_source = budget_source
        self.recovery_source = recovery_source
        self.extra_sources = tuple(extra_sources or ())


class ResolutionRequestCreate(m.Model):
    task_id: m.ID
    expected_task_revision: int = Field(ge=1)
    action: str = Field(min_length=1, max_length=64)
    note: str = Field(default="", max_length=MAX_NOTE)
    basis: list[m.Ref] = Field(default_factory=list, max_length=MAX_BASIS_REFS)
    # Optional frozen candidate binding: the exact run (and agent) the request
    # is about. Explicit caller-supplied IDs only — binding grants NO authority;
    # the candidate is re-authorized against current state at every use.
    run_id: m.ID | None = None
    agent_id: m.ID | None = None

    def model_post_init(self, context, /):
        if self.action not in REQUEST_ACTIONS:
            raise ValueError("Action must be one of the allowlisted remediation codes")
        if self.agent_id is not None and self.run_id is None:
            raise ValueError("An agent candidate requires its exact run")


class ResolutionAcknowledge(m.Model):
    note: str = Field(default="", max_length=MAX_NOTE)


class ResolutionClose(m.Model):
    outcome: str = Field(min_length=1, max_length=32)
    note: str = Field(default="", max_length=MAX_NOTE)

    def model_post_init(self, context, /):
        if self.outcome not in CLOSE_OUTCOMES:
            raise ValueError("Outcome must be resolved, declined, or request_canceled")


class BlockedTasks:
    """Domain logic; mutations run inside the parent's command seam (canonical
    transaction + durable idempotent receipt)."""

    def __init__(self, app, service, policy):
        self.app, self.service, self.policy = app, service, policy
        self.store = service.store
        self.scheduling = Scheduling(service)

    # -- authorization ------------------------------------------------------
    def _fresh_principal(self, principal):
        """Current-role reauthorization: the cached request principal must still
        exist with the same role/project/agent binding."""
        row = self.store.db.execute(
            "SELECT * FROM principals WHERE id=?", (principal["id"],)).fetchone()
        if row is None or any(row[key] != principal[key]
                              for key in ("role", "project", "agent", "token_hash")):
            raise Problem("authentication_required", 401)
        return dict(row)

    def _authorized_task(self, principal, task_id, project_id=None):
        with self.store.lock:
            principal = self._fresh_principal(principal)
            task = self.store.get(task_id, principal, "task")
        if project_id is not None and task.get("project_id") != project_id:
            raise Problem("not_found", 404)
        return task

    # -- explanation --------------------------------------------------------
    @staticmethod
    def _blocker(code, action, source, provenance):
        return {"code": code, "action": action,
                "summary": _SUMMARY.get(code, _SUMMARY[GENERIC_CODE]),
                "source": source, "provenance": provenance}

    def _map_reason(self, reason, source):
        if not isinstance(reason, str) or reason in _PRIVATE_REASONS \
                or reason not in _REASON_MAP:
            return self._blocker(GENERIC_CODE, "operator_attention", source, "gate_snapshot")
        code, action = _REASON_MAP[reason]
        return self._blocker(code, action, source, "gate_snapshot")

    def _call_gate(self, gate, source, *args):
        """Gate callbacks are parent-registry contracts: malformed verdicts and
        exceptions fail closed to a generic hold, never a 500, never permission."""
        try:
            verdict = gate(*args)
        except Exception:
            return self._blocker(GENERIC_CODE, "operator_attention", source, "gate_snapshot")
        if verdict is None:
            return None
        if not isinstance(verdict, dict) or verdict.get("action") not in ("allow", "defer"):
            return self._blocker(GENERIC_CODE, "operator_attention", source, "gate_snapshot")
        if verdict["action"] == "allow" or verdict.get("reason") == "feature_disabled":
            return None
        return self._map_reason(verdict.get("reason"), source)

    def _dependency_cycle(self, project_id, task_id):
        """Independent bounded gray/black DFS over DEPENDS_ON relations; mirrors
        the artifact-contract bound and works whether or not contracts are
        enabled. Exhaustion fails closed (treated as a cycle)."""
        def edges(node):
            rows = self.store.db.execute(
                "SELECT target FROM relations WHERE project=? AND source=?"
                " AND kind='DEPENDS_ON' LIMIT 64", (project_id, node)).fetchall()
            return [row["target"] for row in rows]
        color = {task_id: 1}  # 1 = gray, 2 = black
        stack = [(task_id, edges(task_id))]
        edges_seen = 0
        while stack:
            node, targets = stack[-1]
            descended = False
            for target in targets:
                edges_seen += 1
                if edges_seen > MAX_CYCLE_EDGES:
                    return True
                state = color.get(target, 0)
                if state == 1:
                    return True
                if state == 0:
                    if len(color) >= MAX_CYCLE_NODES:
                        return True
                    color[target] = 1
                    stack.append((target, edges(target)))
                    descended = True
                    break
            if not descended:
                color[node] = 2
                stack.pop()
        return False

    def _dependency_blockers(self, principal, task, blockers, evaluated):
        evaluated.add("dependency_graph")
        dependencies = self.scheduling._dependencies(task)
        truncated = len(dependencies) > MAX_DEPENDENCIES
        if truncated or self._dependency_cycle(task["project_id"], task["id"]):
            blockers.append(self._blocker("dependency_cycle", "dependency_review",
                                          "dependencies", "canonical_snapshot"))
        gates = getattr(self.service, "task_dependency_gates", ())
        if truncated:
            blockers.append(self._blocker(GENERIC_CODE, "dependency_review",
                                          "dependencies", "canonical_snapshot"))
        for dependency_id in dependencies[:MAX_DEPENDENCIES]:
            try:
                dependency = self.store.get(dependency_id, principal, "task")
            except Problem:
                # Hidden/private/missing dependency: generic, never its identity.
                blockers.append(self._blocker(GENERIC_CODE, "dependency_review",
                                              "dependencies", "canonical_snapshot"))
                continue
            gated = False
            for gate in gates:
                evaluated.add("dependency_gates")
                blocker = self._call_gate(gate, "dependency_gates",
                                          principal, task, dependency)
                if blocker is not None:
                    gated = True
                    if blocker["code"] != GENERIC_CODE:
                        blocker = {**blocker, "dependency_id": dependency_id}
                    blockers.append(blocker)
            if dependency["status"] != "completed" and not gated:
                blockers.append({"dependency_id": dependency_id,
                                 **self._blocker("dependency_incomplete", "dependency_review",
                                                 "dependencies", "canonical_snapshot")})
        if task.get("require_verified_dependencies") and not truncated:
            completed = True
            for dependency_id in dependencies[:MAX_DEPENDENCIES]:
                try:
                    if self.store.get(dependency_id, principal, "task")["status"] != "completed":
                        completed = False
                        break
                except Problem:
                    completed = False
                    break
            if completed and dependencies \
                    and not self.scheduling._dependencies_met(principal, task):
                blockers.append(self._blocker("dependency_unverified", "dependency_review",
                                              "dependencies", "canonical_snapshot"))
        # One blocker per (code, dependency) pair, order preserved.
        seen = set()
        blockers[:] = [b for b in blockers
                       if not ((b["code"], b.get("dependency_id")) in seen
                               or seen.add((b["code"], b.get("dependency_id"))))]

    def _raw_availability_blockers(self, principal, task, run, blockers):
        """Authoritative availability snapshot from RAW bounded canonical
        claim/control rows (mirrors the capability router's raw approach): a
        hidden conflicting claim, unverifiable scope, or unverifiable pending
        control holds the task generically — existence only, never the private
        holder identity or an explicit scope-holder id."""
        project_id = task["project_id"]
        current = time.time()
        scope_ids = [scope for scope in task.get("scope_ids") or [] if isinstance(scope, str)]
        if scope_ids:
            rows = self.store.db.execute(
                "SELECT * FROM resources WHERE kind='work_claim' AND project=?"
                " AND deleted=0 ORDER BY seq DESC LIMIT ?",
                (project_id, MAX_RAW_ROWS)).fetchall()
            conflict_visible = conflict_hidden = False
            for row in rows:
                body = json.loads(row["body"])
                if body.get("state") not in ("active", "reserved") \
                        or body.get("expires_at", 0) <= current \
                        or body.get("run_id") == run["id"] \
                        or body.get("mode") != "exclusive_owner":
                    continue
                peer_scopes = [scope for scope in body.get("scope_ids") or []
                               if isinstance(scope, str)]
                try:
                    overlap = self.service.overlaps(principal, project_id,
                                                    scope_ids, peer_scopes)
                except Problem:
                    conflict_hidden = True  # Unverifiable scope keeps its hold.
                    continue
                if overlap:
                    if self.store.visible(row, principal):
                        conflict_visible = True
                    else:
                        conflict_hidden = True
            if conflict_visible:
                blockers.append(self._blocker("scope_claimed", "permission_review",
                                              "claims", "canonical_snapshot"))
            if conflict_hidden:
                blockers.append(self._blocker("scope_conflict_unavailable",
                                              "permission_review", "claims",
                                              "canonical_snapshot"))
        held_visible = held_hidden = False
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='control' AND project=?"
            " AND deleted=0 ORDER BY seq DESC LIMIT ?",
            (project_id, MAX_RAW_ROWS)).fetchall()
        for row in rows:
            body = json.loads(row["body"])
            if body.get("target_run_id") != run["id"] \
                    or body.get("state") not in PENDING_CONTROL_STATES \
                    or body.get("expires_at", 0) <= current:
                continue
            if self.store.visible(row, principal):
                held_visible = True
            else:
                held_hidden = True
        if held_visible:
            blockers.append(self._blocker("control_hold", "operator_attention",
                                          "controls", "canonical_snapshot"))
        if held_hidden:
            blockers.append(self._blocker("control_state_unavailable",
                                          "operator_attention", "controls",
                                          "canonical_snapshot"))

    def _run_blockers(self, principal, task, run, blockers, evaluated):
        if run["project_id"] != task["project_id"]:
            raise Problem("not_found", 404)
        evaluated.add("run_liveness")
        if run["status"] not in ("running", "idle"):
            blockers.append(self._blocker("run_not_ready", "recovery_review",
                                          "run", "canonical_snapshot"))
        # Assignment/handoff fences are canonical scheduling metadata; they
        # apply even when no routing feature is installed.
        evaluated.add("scheduling_fences")
        settings = self.scheduling._settings(task)
        if settings.get("assigned_agent_id") not in (None, run.get("agent_id")):
            blockers.append(self._blocker("assignment_mismatch", "operator_attention",
                                          "scheduling", "canonical_snapshot"))
        handoff = task.get("handoff") or {}
        if handoff.get("state") == "pending" and (
                handoff.get("target_agent_id") not in (None, run.get("agent_id"))
                or handoff.get("target_run_id") not in (None, run.get("id"))):
            blockers.append(self._blocker("handoff_target_mismatch", "operator_attention",
                                          "scheduling", "canonical_snapshot"))
        for gate in getattr(self.service, "task_dispatch_gates", ()):
            evaluated.add("dispatch_gates")
            blocker = self._call_gate(gate, "dispatch_gates", principal, task, run)
            if blocker is not None:
                blockers.append(blocker)
        evaluated.add("capabilities")
        required = self.scheduling._required(task)
        if required and not required.issubset(
                self.scheduling.capability_set(principal, run["agent_id"])):
            blockers.append(self._blocker("capability_mismatch", "operator_attention",
                                          "capabilities", "canonical_snapshot"))
        evaluated.add("permit_fence")
        try:
            self.service.task_permit(principal, run["id"], task)
        except Problem as error:
            blockers.append(self._map_reason(error.code, "permit_fence"))
        evaluated.update(("claims", "controls"))
        self._raw_availability_blockers(principal, task, run, blockers)

    def _budget_snapshot(self, task):
        """Read-only ledger snapshot, or None when budgets are not enforced."""
        ledger = getattr(self.service, "task_budgets", None)
        if ledger is None or not getattr(ledger.policy, "enabled", False):
            return None
        try:
            return ledger, ledger.snapshot(task["id"])
        except Exception:
            return ledger, None

    def _budget_holds(self, task):
        """Unknown/open/expired durable holds; True also when the snapshot itself
        cannot be verified. Budget holds are never cleared by task metadata."""
        result = self._budget_snapshot(task)
        if result is None:
            return False
        _ledger, snap = result
        if snap is None:
            return True
        return bool(snap["unknown_holds"] or snap["open_holds"] or snap["expired_holds"])

    def _budget_blockers(self, task, blockers, evaluated):
        """Read-only ledger snapshot only: SQL aggregates via ``snapshot``, no
        counters, no vendor helpers, no heavy I/O. Zero budgets are exhausted
        budgets: the mandatory output reserve can never fit a zero token
        budget, and an enforced zero cost budget admits no spend."""
        result = self._budget_snapshot(task)
        if result is None:
            return
        ledger, snap = result
        evaluated.add("budgets")
        if snap is None:
            blockers.append(self._blocker(GENERIC_CODE, "budget_review",
                                          "budgets", "canonical_snapshot"))
            return
        if snap["budget"] is None:
            blockers.append(self._blocker("budget_not_configured", "budget_review",
                                          "budgets", "canonical_snapshot"))
            return
        if snap["unknown_holds"] or snap["expired_holds"]:
            blockers.append(self._blocker("budget_settlement_unknown", "budget_review",
                                          "budgets", "canonical_snapshot"))
        elif snap["open_holds"] and not ledger.policy.allow_concurrent_open:
            blockers.append(self._blocker("budget_reservation_outstanding", "budget_review",
                                          "budgets", "canonical_snapshot"))
        budget = snap["budget"]
        token_budget = budget.get("token_budget")
        if isinstance(token_budget, int) and (
                snap["spent_tokens"] >= token_budget
                or token_budget < budget.get("output_reserve", 1)):
            blockers.append(self._blocker("token_budget_insufficient", "budget_review",
                                          "budgets", "canonical_snapshot"))
        seconds_budget = budget.get("seconds_budget")
        if isinstance(seconds_budget, (int, float)) \
                and snap["spent_seconds"] >= seconds_budget:
            blockers.append(self._blocker("time_budget_exhausted", "budget_review",
                                          "budgets", "canonical_snapshot"))
        if budget.get("enforce_cost"):
            cost_budget = budget.get("cost_budget")
            spent = snap["spent_cost"].get(budget.get("currency"), 0.0)
            if isinstance(cost_budget, (int, float)) and spent >= cost_budget:
                blockers.append(self._blocker("cost_budget_insufficient", "budget_review",
                                              "budgets", "canonical_snapshot"))

    def _hook_blockers(self, principal, task, blockers, evaluated):
        hooks = (("budget_source", self.policy.budget_source),
                 ("recovery_source", self.policy.recovery_source),
                 *(("extra_source", hook) for hook in self.policy.extra_sources))
        for source, hook in hooks:
            if hook is None:
                continue
            evaluated.add(source)
            try:
                report = hook(self.service, principal, task)
            except Exception:
                report = "invalid"
            if report is None:
                continue
            if (not isinstance(report, dict) or report.get("blocked") is not True
                    or report.get("reason_class") not in _HOOK_REASONS):
                blockers.append(self._blocker(GENERIC_CODE, "operator_attention",
                                              source, "source_hook"))
                continue
            code, action = _HOOK_REASONS[report["reason_class"]]
            blockers.append(self._blocker(code, action, source, "source_hook"))

    def _execution_view(self, principal, task, blockers, evaluated):
        """Current-execution view for a non-terminal task. The run id is
        returned only when the run is CURRENTLY visible to this caller; an
        unverifiable (private/missing) prior execution holds the task with a
        generic blocker — unknown executions are never 'ready'."""
        execution = task.get("execution") or None
        if not execution or not execution.get("id"):
            return None
        evaluated.add("execution")
        view = {"id": execution["id"]}
        run_id = execution.get("run_id")
        if isinstance(run_id, str):
            try:
                run = self.store.get(run_id, principal, "run")
                view["run_id"] = run["id"]
                if run["status"] in TERMINAL_RUN_STATUSES \
                        and task.get("status") not in TERMINAL_TASK_STATUSES:
                    blockers.append(self._blocker("execution_interrupted",
                                                  "recovery_review", "execution",
                                                  "canonical_snapshot"))
                elif task.get("status") == "queued":
                    blockers.append(self._blocker("execution_active",
                                                  "recovery_review", "execution",
                                                  "canonical_snapshot"))
            except Problem:
                view["run_redacted"] = True
                blockers.append(self._blocker(GENERIC_CODE, "recovery_review",
                                              "execution", "canonical_snapshot"))
        return view

    def explain(self, principal, project_id, task_id, run_id=None, agent_id=None):
        """Read-only readiness explanation fenced to the exact
        (principal, project, task, run/agent) tuple. ``ready`` is true ONLY for
        a queued task with zero current blockers; running/terminal/unknown
        states are reported as explicit ``readiness`` states.

        ``gates_evaluated`` lists the phases that ACTUALLY ran for this call —
        never the registry contents. Without an exact candidate run no
        dispatch/permit/claim/control phase executes, the scope is reported as
        ``task_only`` with ``run_checks_evaluated: False``, and a task-only
        "startable" never claims candidate or start permission."""
        task = self._authorized_task(principal, task_id, project_id)
        status = task.get("status")
        blockers = []
        evaluated = set()
        if agent_id is not None:
            agent = self.store.get(agent_id, principal, "agent")
            if agent.get("project_id") != project_id:
                raise Problem("not_found", 404)
        run = None
        if run_id is not None:
            run = self.service.run(principal, run_id)
            if agent_id is not None and run["agent_id"] != agent_id:
                raise Problem("not_found", 404)
        execution_view = None
        run_checks = False
        if status in TERMINAL_TASK_STATUSES:
            readiness = "terminal"
        elif status == "running":
            readiness = "already_running"
        else:
            evaluated.add("scheduling")
            if status == "paused":
                blockers.append(self._blocker("task_paused", "operator_attention",
                                              "task", "canonical_snapshot"))
            settings = self.scheduling._settings(task)
            if settings.get("escalated"):
                blockers.append(self._blocker("escalated_hold", "operator_attention",
                                              "scheduling", "canonical_snapshot"))
            if settings["attempts"] >= self.scheduling._max_attempts(task):
                blockers.append(self._blocker("attempt_budget_exhausted", "recovery_review",
                                              "scheduling", "canonical_snapshot"))
            retry_after = as_number(settings.get("retry_after"))
            if retry_after is not None and retry_after > time.time():
                blockers.append(self._blocker("retry_backoff", "operator_attention",
                                              "scheduling", "canonical_snapshot"))
            self._dependency_blockers(principal, task, blockers, evaluated)
            self._budget_blockers(task, blockers, evaluated)
            execution_view = self._execution_view(principal, task, blockers, evaluated)
            if run is not None:
                self._run_blockers(principal, task, run, blockers, evaluated)
                run_checks = True
            readiness = "blocked" if blockers else "startable"
        self._hook_blockers(principal, task, blockers, evaluated)
        # One blocker per (code, source, dependency) triple, order preserved.
        seen = set()
        blockers[:] = [
            b for b in blockers
            if not ((b["code"], b["source"], b.get("dependency_id")) in seen
                    or seen.add((b["code"], b["source"], b.get("dependency_id"))))]
        if readiness == "startable" and blockers:
            readiness = "blocked"
        return {
            "task_id": task["id"], "project_id": project_id,
            "task_revision": task["revision"], "task_status": status,
            "scheduling_state": (task.get("scheduling") or {}).get("state"),
            "execution": execution_view,
            "run_id": run["id"] if run else None, "agent_id": agent_id,
            "readiness": readiness,
            "readiness_scope": "candidate" if run_checks else "task_only",
            "run_checks_evaluated": run_checks,
            "ready": readiness == "startable",
            "blockers": blockers,
            "suggested_actions": sorted({b["action"] for b in blockers}),
            "gates_evaluated": sorted(evaluated),
            "provenance": {"canonical_snapshot": "direct canonical store reads",
                           "gate_snapshot": "registered parent gate verdicts (pure reads)",
                           "source_hook": "operator-injected report, generic reason class"},
            "authority": "explanation_snapshot_not_execution_permission",
            "as_of": now(),
        }

    # -- durable requests ---------------------------------------------------
    def _require_requests(self):
        if not self.policy.requests_enabled:
            raise Problem("blocked_requests_disabled", 403)

    def _request_view(self, principal, row):
        """Current-state reauthorization and redaction for EVERY read of a
        request, including idempotent mutation replays: the request resource
        itself and the task must be visible NOW; the requester must still
        exist with project standing; every basis ref must still be visible,
        kind-correct, and revision-current, else it is withdrawn from the view."""
        if not self.store.visible(row, principal):
            raise Problem("not_found", 404)
        body = json.loads(row["body"])
        self._authorized_task(principal, body["task_id"])
        requester = self.store.db.execute(
            "SELECT * FROM principals WHERE id=?",
            (body["requester_principal_id"],)).fetchone()
        source_redacted = requester is None or (
            requester["role"] != "admin" and requester["project"] != body["project_id"])
        if requester is not None:
            source_redacted = source_redacted or body.get("requester_binding") != digest([
                requester[key] for key in ("id", "role", "project", "agent", "token_hash")])
        basis, basis_redacted = [], source_redacted
        if not source_redacted:
            for ref in body.get("basis") or []:
                try:
                    ref_row = self.store.raw(ref["id"])
                    if not self.store.visible(ref_row, principal) \
                            or not self.store.visible(ref_row, dict(requester)) \
                            or ref_row["kind"] != ref.get("kind") \
                            or (ref.get("revision")
                                and ref["revision"] != ref_row["revision"]):
                        raise Problem("not_found", 404)
                    basis.append(ref)
                except Problem:
                    basis_redacted = True
        return {**body,
                "note": None if basis_redacted else body.get("note"),
                "close_note": None if basis_redacted else body.get("close_note"),
                "basis": basis, "basis_redacted": basis_redacted,
                "source_redacted": source_redacted}

    def request(self, principal, project_id, body):
        self._require_requests()
        with self.store.lock:
            self._fresh_principal(principal)
            self.service.project_access(principal, project_id)
        task = self._authorized_task(principal, body.task_id, project_id)
        task_row = self.store.raw(task["id"], "task")
        if task["revision"] != body.expected_task_revision:
            raise Problem("revision_conflict", 412)
        basis = [ref.model_dump(exclude_none=True) for ref in body.basis]
        for ref in basis:
            row = self.store.raw(ref["id"])
            if row["kind"] != ref["kind"]:
                raise Problem("not_found", 404)
            if row["project"] != project_id:
                raise Problem("not_found", 404)
            if not self.store.visible(row, principal):
                raise Problem("not_found", 404)
            if ref.get("revision") and ref["revision"] != row["revision"]:
                raise Problem("stale_basis", 409)
            if row["private_agent"] and (
                    row["private_agent"] != principal.get("agent")
                    or row["private_agent"] != task_row["private_agent"]):
                # Strict namespace intersection: private basis is admissible only
                # for the exact private actor, never across a namespace boundary.
                raise Problem("private_basis_forbidden", 403)
        candidate = None
        if body.run_id is not None:
            # Frozen candidate binding: authorized NOW against the requester's
            # own authority (service.run enforces represent), fenced to the
            # project and the declared agent. Binding grants no authority; the
            # candidate is re-authorized fresh at every later use.
            run = self.service.run(principal, body.run_id)
            if run["project_id"] != project_id:
                raise Problem("not_found", 404)
            if body.agent_id is not None and run["agent_id"] != body.agent_id:
                raise Problem("not_found", 404)
            candidate = {"run_id": run["id"], "agent_id": run["agent_id"]}
        count = self.store.db.execute(
            "SELECT COUNT(*) FROM resources WHERE kind='resolution_request'"
            " AND project=? AND deleted=0 AND json_extract(body,'$.task_id')=?",
            (project_id, task["id"])).fetchone()[0]
        if count >= self.policy.max_requests_per_task:
            raise Problem("resolution_request_limit", 429)
        explanation = self.explain(principal, project_id, task["id"],
                                   run_id=body.run_id, agent_id=body.agent_id)
        source_fingerprint = digest(["blocked-task-source", task["id"], task["revision"],
                                     candidate, [b["code"] for b in explanation["blockers"]]])
        request_fingerprint = digest(["blocked-task-resolution", task["id"], body.action,
                                      principal["id"], basis, body.note, candidate])
        created = self.service.create(principal, "resolution_request", {
            "project_id": project_id, "schema": VERSION,
            "task_id": task["id"], "task_revision": task["revision"],
            "action": body.action, "note": body.note, "basis": basis,
            "candidate": candidate,
            "requester_principal_id": principal["id"],
            "requester_agent_id": principal.get("agent"),
            "requester_binding": digest([
                principal[key] for key in ("id", "role", "project", "agent", "token_hash")]),
            "request_fingerprint": request_fingerprint,
            "source_fingerprint": source_fingerprint,
            "state": "open", "outcome": None,
            "acknowledged_by": None, "acknowledged_at": None,
            "closed_by": None, "closed_at": None, "close_note": None,
        }, private=task_row["private_agent"])
        return self._request_view(principal, self.store.raw(created["id"],
                                                            "resolution_request"))

    def list_requests(self, principal, project_id, task_id=None):
        self._require_requests()
        with self.store.lock:
            self._fresh_principal(principal)
            self.service.project_access(principal, project_id)
        views = []
        for row in self.store.db.execute(
                "SELECT * FROM resources WHERE kind='resolution_request' AND project=?"
                " AND deleted=0 ORDER BY seq DESC LIMIT ?", (project_id, MAX_LIST)):
            if task_id is not None \
                    and json.loads(row["body"])["task_id"] != task_id:
                continue
            try:
                views.append(self._request_view(principal, row))
            except Problem:
                continue  # Not currently visible to this principal.
        return views

    def get_request(self, principal, request_id):
        self._require_requests()
        with self.store.lock:
            self._fresh_principal(principal)
            row = self.store.raw(request_id, "resolution_request")
        return self._request_view(principal, row)

    def _triage_authorized(self, principal, task_id):
        if principal["role"] in ("admin", "human"):
            return True
        return self.store.raw(task_id, "task")["owner"] == principal["id"]

    def acknowledge(self, principal, request_id, body):
        """Single-read, CAS publish: the row is read once, authorized fresh
        inside the command transaction, and saved against THAT captured
        revision — a concurrent update conflicts instead of being overwritten."""
        self._require_requests()
        with self.store.lock:
            self._fresh_principal(principal)
            row = self.store.raw(request_id, "resolution_request")
            if not self.store.visible(row, principal):
                raise Problem("not_found", 404)
            request = json.loads(row["body"])
            self._authorized_task(principal, request["task_id"])
            if not self._triage_authorized(principal, request["task_id"]):
                raise Problem("triage_role_required", 403)
            if request["state"] == "acknowledged":
                if request.get("acknowledged_by") != principal["id"]:
                    raise Problem("resolution_request_closed", 409)
                return self._request_view(principal, row)
            if request["state"] != "open":
                raise Problem("resolution_request_closed", 409)
            request.update(state="acknowledged", acknowledged_by=principal["id"],
                           acknowledged_at=now())
            saved = self.store.save("resolution_request", row["project"], row["owner"],
                                    request, expected=row["revision"])
            return self._request_view(principal, self.store.raw(saved["id"],
                                                                "resolution_request"))

    # Actions whose requested condition is candidate/context-scoped: they can
    # only be proven by re-evaluating the bound candidate's exact dispatch,
    # permit, claim, control, and capability gates — a task-only view or an
    # arbitrary zero-blocker read is never proof for them.
    _CANDIDATE_ACTIONS = frozenset({
        "artifact_needed", "permission_review", "recovery_review", "operator_attention"})

    def _require_resolution_proof(self, principal, request, task):
        """Fresh canonical proof that the REQUESTED condition cleared.

        - Unknown provider effects / recovery reports (source hooks) are never
          cleared by task metadata, completion, or operator intent.
        - Every resolution re-snapshots the ledger and rejects unknown, open,
          or expired holds even when the task is completed.
        - Terminal ``completed`` proves only the canonical task condition.
        - Candidate-scoped actions require the request's frozen candidate run,
          re-authorized now, with a candidate-scoped zero-blocker explanation.
        - ``dependency_review`` may be proven task-only once every bounded
          dependency and dependency gate clears."""
        candidate = request.get("candidate") or None
        run_id = agent_id = None
        if candidate is not None:
            run_id, agent_id = candidate.get("run_id"), candidate.get("agent_id")
            try:
                run = self.store.get(run_id, principal, "run")
            except Problem:
                raise Problem("resolution_candidate_unavailable", 409) from None
            if run["project_id"] != task["project_id"] or run.get("agent_id") != agent_id:
                raise Problem("resolution_candidate_unavailable", 409)
        explanation = self.explain(principal, task["project_id"], task["id"],
                                   run_id=run_id, agent_id=agent_id)
        blockers = explanation["blockers"]
        if any(b["provenance"] == "source_hook" for b in blockers):
            raise Problem("resolution_not_evidenced", 409)
        if self._budget_holds(task):
            raise Problem("resolution_not_evidenced", 409)
        if task["status"] == "completed":
            if request["action"] in self._CANDIDATE_ACTIONS:
                # Completion cannot substitute for artifact, permission or effect
                # proof. A terminal task cannot run candidate readiness checks.
                raise Problem("resolution_not_evidenced", 409)
            dependencies = []
            self._dependency_blockers(principal, task, dependencies, set())
            if dependencies:
                raise Problem("resolution_not_evidenced", 409)
            return  # Only the independently rechecked task-level condition.
        if explanation["readiness"] != "startable":
            raise Problem("resolution_not_evidenced", 409)
        if request["action"] in self._CANDIDATE_ACTIONS:
            if candidate is None or not explanation["run_checks_evaluated"]:
                raise Problem("resolution_not_evidenced", 409)

    def close(self, principal, request_id, body):
        self._require_requests()
        with self.store.lock:
            self._fresh_principal(principal)
            row = self.store.raw(request_id, "resolution_request")
            if not self.store.visible(row, principal):
                raise Problem("not_found", 404)
            request = json.loads(row["body"])
            task = self._authorized_task(principal, request["task_id"])
            operator = self._triage_authorized(principal, request["task_id"])
            if body.outcome == "request_canceled":
                if principal["id"] != request["requester_principal_id"] and not operator:
                    raise Problem("triage_role_required", 403)
            elif not operator:
                raise Problem("triage_role_required", 403)
            if request["state"] in CLOSE_OUTCOMES:
                if request["state"] == body.outcome \
                        and request.get("close_note") == body.note:
                    # Same-outcome replay is a receipt, never a new decision.
                    return self._request_view(principal, row)
                raise Problem("resolution_request_closed", 409)
            if body.outcome == "resolved":
                self._require_resolution_proof(principal, request, task)
            request.update(state=body.outcome, outcome=body.outcome,
                           closed_by=principal["id"], closed_at=now(),
                           close_note=body.note)
            saved = self.store.save("resolution_request", row["project"], row["owner"],
                                    request, expected=row["revision"])
            return self._request_view(principal, self.store.raw(saved["id"],
                                                                "resolution_request"))


def install(app, service, principal, command, envelope, *, policy=None):
    """Install the blocked-task explanation surface and, with the explicit
    opt-in policy, the durable resolution-request workflow. Reinstall replaces
    only this module's own routes. All handlers are pure bounded SQL under the
    canonical lock; none is marked detached."""
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    handle = BlockedTasks(app, service, policy or BlockedTasksPolicy())
    app.state.blocked_tasks = handle
    service.blocked_tasks = handle
    from .agent_feature_gates import set_resource_projection
    set_resource_projection(service, "resolution_request", handle.get_request)
    endpoints = []
    if not handle.policy.explain_enabled:
        return {"handle": handle, "endpoints": endpoints, "models": list(MODELS)}

    def _bounded_id(value, name):
        if value is not None and (not value or len(value) > 128):
            raise Problem("invalid_request", 422)
        return value

    @app.get("/v1/projects/{project_id}/tasks/{task_id}/blocked-status")
    async def blocked_status(project_id: str, task_id: str, run_id: str | None = Query(None),
                             agent_id: str | None = Query(None), p=Depends(principal)):
        _bounded_id(run_id, "run_id")
        _bounded_id(agent_id, "agent_id")  # fenced even without a run
        return envelope(handle.explain(p, project_id, task_id,
                                       run_id=run_id, agent_id=agent_id))
    endpoints.append("GET /v1/projects/{project_id}/tasks/{task_id}/blocked-status")

    if handle.policy.requests_enabled:
        @app.post("/v1/projects/{project_id}/blocked-tasks:request")
        async def request_create(project_id: str, body: ResolutionRequestCreate,
                                 p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
            result = command(p, "blocked-tasks:request:" + project_id, idempotency_key, body,
                             lambda: handle.request(p, project_id, body))
            # Re-authorize and re-redact on every response, including replays.
            return envelope(handle.get_request(p, result["data"]["id"]))
        endpoints.append("POST /v1/projects/{project_id}/blocked-tasks:request")

        @app.get("/v1/projects/{project_id}/blocked-task-requests")
        async def request_list(project_id: str, task_id: str | None = Query(None),
                               p=Depends(principal)):
            _bounded_id(task_id, "task_id")
            return envelope(handle.list_requests(p, project_id, task_id))
        endpoints.append("GET /v1/projects/{project_id}/blocked-task-requests")

        @app.get("/v1/blocked-task-requests/{request_id}")
        async def request_get(request_id: str, p=Depends(principal)):
            return envelope(handle.get_request(p, request_id))
        endpoints.append("GET /v1/blocked-task-requests/{request_id}")

        @app.post("/v1/blocked-task-requests/{request_id}:acknowledge")
        async def request_acknowledge(request_id: str, body: ResolutionAcknowledge,
                                      p=Depends(principal),
                                      idempotency_key: str | None = Header(None)):
            result = command(p, "blocked-tasks:acknowledge:" + request_id, idempotency_key,
                             body, lambda: handle.acknowledge(p, request_id, body))
            return envelope(handle.get_request(p, result["data"]["id"]))
        endpoints.append("POST /v1/blocked-task-requests/{request_id}:acknowledge")

        @app.post("/v1/blocked-task-requests/{request_id}:resolve")
        async def request_resolve(request_id: str, body: ResolutionClose,
                                  p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
            result = command(p, "blocked-tasks:resolve:" + request_id, idempotency_key,
                             body, lambda: handle.close(p, request_id, body))
            return envelope(handle.get_request(p, result["data"]["id"]))
        endpoints.append("POST /v1/blocked-task-requests/{request_id}:resolve")

    return {"handle": handle, "endpoints": endpoints, "models": list(MODELS)}
