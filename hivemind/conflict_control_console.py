"""Read-only aggregate for the dashboard conflict/control console.

One bounded, authorized, snapshot-fenced read that assembles the *existing*
canonical coordination records for a project:

- active work claims and their canonical overlap pairs (``Service.claim`` is
  the only writer of claims; this module never claims, releases, or reassigns),
- ``conflict`` records produced by the canonical claim authority,
- ``coordination_action`` records (manual holds and recent policy actions)
  written by ``coordination_policy``,
- ``control`` records written by the Nexus control path (``Service.control``),
- the active Nexus lease, runs (presence is volatile presence only, never an
  acknowledgment of any control), agents, and steer-eligible queued tasks.

Nothing here mutates state, impersonates Nexus, invents atomic reassignment,
or treats HTTP acceptance as an applied control. Command surfaces stay with
the canonical modules (``POST /v1/controls``, ``:resolve``, ``nexus:*``);
this module only reads. Hidden target/task/resource ids are redacted to
counts, never enumerated. Registered by the integration layer
(``hivemind.capabilities`` or ``create_app``) via the standard install seam.
"""
import time

from fastapi import Depends, Query, Request

from .coordination_policy import policy_defaults
from .storage import Problem

CLAIMS_LIMIT = 200
OVERLAP_LIMIT = 200
CONFLICTS_LIMIT = 200
ACTIONS_LIMIT = 100
CONTROLS_LIMIT = 200
RUNS_LIMIT = 200
AGENTS_LIMIT = 200
STEER_TARGETS_LIMIT = 100

# Presence buckets mirror /v1/ledger: volatile liveness only, never evidence
# that a control was acknowledged or applied.
PRESENCE_STALE_SECONDS = 15
PRESENCE_OFFLINE_SECONDS = 30


def _visible_id(store, principal, project_id, resource_id, kind=None):
    """Return the id when currently visible to the principal, else None."""
    if not isinstance(resource_id, str):
        return None
    try:
        store.get(resource_id, principal, kind)
    except Problem:
        return None
    return resource_id


def _presence(run):
    last_seen = run.get("last_seen_at")
    if not isinstance(last_seen, (int, float)):
        return "offline"
    gap = time.time() - last_seen
    if gap >= PRESENCE_OFFLINE_SECONDS:
        return "offline"
    if gap >= PRESENCE_STALE_SECONDS:
        return "stale"
    return "online"


def console_snapshot(app, service, principal, project_id, *, limit=CLAIMS_LIMIT):
    """Assemble the authorized console aggregate. Read-only; in-lock callers only."""
    store = service.store
    service.project_access(principal, project_id)
    redactions = {"hidden_claim_scopes": 0, "hidden_conflict_scopes": 0,
                  "hidden_control_fields": 0, "hidden_steer_targets": 0}

    # Policy: effective defaults overlaid with the stored record (read-only).
    policy = policy_defaults()
    policy_record = next(
        (record for record in store.list("coordination_policy", project_id, principal, 1000)
         if record.get("project_id") == project_id), None)
    if policy_record:
        policy.update({key: policy_record[key] for key in policy if key in policy_record})

    claims, claims_truncated = [], False
    active = service.active_claims(principal, project_id)
    if len(active) > limit:
        active, claims_truncated = active[:limit], True
    for claim in active:
        scopes, hidden = [], 0
        for scope_id in claim.get("scope_ids") or []:
            if _visible_id(store, principal, project_id, scope_id, "scope"):
                scopes.append(scope_id)
            else:
                hidden += 1
        redactions["hidden_claim_scopes"] += hidden
        claims.append({"id": claim["id"], "run_id": claim["run_id"],
                       "task_id": claim.get("task_id"), "mode": claim.get("mode"),
                       "state": claim.get("state"), "scope_ids": scopes,
                       "hidden_scope_count": hidden,
                       "expires_at": claim.get("expires_at"),
                       "ownership_epoch": claim.get("ownership_epoch"),
                       "revision": claim.get("revision")})

    # Canonical overlap pairs among the returned claims. ``Service.overlaps``
    # is the single authority; a pair it cannot evaluate (hidden scopes) is
    # counted as redacted, never guessed at or enumerated.
    overlaps, overlaps_truncated = [], False
    for index, left in enumerate(claims):
        for right in claims[index + 1:]:
            if len(overlaps) >= OVERLAP_LIMIT:
                overlaps_truncated = True
                break
            if left["run_id"] == right["run_id"]:
                continue
            modes = {left.get("mode"), right.get("mode")}
            if "exclusive_owner" not in modes or "observer" in modes:
                continue
            try:
                if service.overlaps(principal, project_id, left["scope_ids"],
                                    right["scope_ids"]):
                    overlaps.append({"kind": "scope_overlap", "claim_ids": [left["id"], right["id"]],
                                     "run_ids": [left["run_id"], right["run_id"]],
                                     "modes": sorted(str(mode) for mode in modes)})
            except Problem:
                redactions["hidden_claim_scopes"] += 1
        if overlaps_truncated:
            break

    conflicts, conflicts_truncated = [], False
    for conflict in store.list("conflict", project_id, principal, CONFLICTS_LIMIT + 1):
        if len(conflicts) >= CONFLICTS_LIMIT:
            conflicts_truncated = True
            break
        scopes, hidden = [], 0
        for scope_id in conflict.get("scope_ids") or []:
            if _visible_id(store, principal, project_id, scope_id, "scope"):
                scopes.append(scope_id)
            else:
                hidden += 1
        redactions["hidden_conflict_scopes"] += hidden
        conflicts.append({**conflict, "scope_ids": scopes, "hidden_scope_count": hidden})

    actions, actions_truncated = [], False
    for action in store.list("coordination_action", project_id, principal, ACTIONS_LIMIT + 1):
        if len(actions) >= ACTIONS_LIMIT:
            actions_truncated = True
            break
        entry = dict(action)
        if not _visible_id(store, principal, project_id, entry.get("task_id"), "task"):
            # Held/acted task not visible: keep the decision, redact the target.
            entry["task_id"] = None
            entry["task_redacted"] = True
            redactions["hidden_control_fields"] += 1
        actions.append(entry)
    holds = [action for action in actions if action.get("state") == "manual_hold"]

    operator = principal["role"] in ("admin", "human")
    controls, controls_truncated = [], False
    for control in store.list("control", project_id, principal, CONTROLS_LIMIT + 1):
        if len(controls) >= CONTROLS_LIMIT:
            controls_truncated = True
            break
        entry = {key: control.get(key) for key in (
            "id", "project_id", "action", "state", "reason", "target_run_id",
            "target_agent_id", "issuer_agent_id", "lease_id", "coordinator_epoch",
            "replacement_task_id", "expires_at", "revision", "created_at",
            "result", "outcome")}
        if control.get("target_run_id") and not _visible_id(
                store, principal, project_id, control["target_run_id"], "run"):
            entry["target_run_id"] = None
            entry["target_redacted"] = True
            redactions["hidden_control_fields"] += 1
        if operator:
            entry["delegation"] = control.get("delegation")
            entry["reserved_claim_id"] = control.get("reserved_claim_id")
        controls.append(entry)

    lease = service.lease(principal, project_id)
    lease_view = None
    if lease:
        lease_view = {"id": lease["id"], "agent_id": lease["agent_id"],
                      "coordinator_epoch": lease["coordinator_epoch"],
                      "expires_at": lease["expires_at"], "state": lease["state"]}

    runs, runs_truncated = [], False
    for run in store.list("run", project_id, principal, RUNS_LIMIT + 1):
        if len(runs) >= RUNS_LIMIT:
            runs_truncated = True
            break
        runs.append({"id": run["id"], "agent_id": run["agent_id"],
                     "status": run.get("status"), "control_revision": run.get("control_revision"),
                     "task_id": run.get("task_id"),
                     "presence": _presence(run), "presence_is_acknowledgment": False,
                     "revision": run.get("revision")})

    agents, agents_truncated = [], False
    for agent in store.list("agent", project_id, principal, AGENTS_LIMIT + 1):
        if len(agents) >= AGENTS_LIMIT:
            agents_truncated = True
            break
        agents.append({"id": agent["id"], "display_name": agent.get("display_name"),
                       "supported_actions": list(agent.get("supported_actions") or []),
                       "scope_permit_enforcement": bool(agent.get("scope_permit_enforcement"))})

    # Steer/assign targets mirror the canonical replacement-task requirement:
    # visible, queued, and scoped. Hidden tasks are counted, never listed.
    steer_targets, steer_truncated = [], False
    for task in store.list("task", project_id, principal, 1000):
        if len(steer_targets) >= STEER_TARGETS_LIMIT:
            steer_truncated = True
            break
        if task.get("status") != "queued" or not task.get("scope_ids"):
            continue
        steer_targets.append({"id": task["id"], "title": task.get("title"),
                              "revision": task.get("revision")})

    # Truthful, read-only issuer-authority hint. ``Service.coordinator`` requires the
    # caller to represent the lease-holding agent and hold a coordinator grant (admin
    # bypasses both); a merely visible lease never confers issuance. The hint is
    # advisory only: the canonical control path re-authorizes everything on issue.
    issuer_hint = {"lease_visible": lease_view is not None, "advisory": True,
                   "principal_may_issue": False, "explanation": ""}
    if lease_view and lease_view["expires_at"] > time.time():
        if principal["role"] == "admin":
            issuer_hint.update(
                principal_may_issue=True,
                explanation="Administrator principal; the canonical control path still "
                            "re-checks the lease epoch, target revision, and delegation.")
        elif principal.get("coordinator") and principal.get("agent") == lease_view["agent_id"]:
            issuer_hint.update(
                principal_may_issue=True,
                explanation="This principal represents the lease-holding coordinator agent; "
                            "delegation, team-access, and revision checks still apply.")
        else:
            issuer_hint["explanation"] = (
                "Controls are issued by the lease-holding coordinator agent under a "
                "coordinator grant; your credentials do not represent it, so issuance "
                "would be refused by the canonical path.")
    else:
        issuer_hint["explanation"] = (
            "No active coordination lease is visible; controls cannot be issued.")

    counts = {"claims": len(claims), "overlaps": len(overlaps), "conflicts": len(conflicts),
              "holds": len(holds), "actions": len(actions), "controls": len(controls),
              "runs": len(runs),
              "pending_controls": sum(1 for c in controls
                                      if c.get("state") in ("queued", "acknowledged", "applying"))}
    return {
        "project_id": project_id,
        "generated_at": time.time(),
        "policy": policy,
        "counts": counts,
        "claims": claims, "claim_overlaps": overlaps, "conflicts": conflicts,
        "holds": holds, "actions": actions, "controls": controls,
        "lease": lease_view, "issuer_hint": issuer_hint, "runs": runs, "agents": agents,
        "steer_targets": steer_targets,
        "redactions": redactions,
        "truncated": {"claims": claims_truncated, "overlaps": overlaps_truncated,
                      "conflicts": conflicts_truncated, "actions": actions_truncated,
                      "controls": controls_truncated, "runs": runs_truncated,
                      "agents": agents_truncated, "steer_targets": steer_truncated},
        "limits": {"claims": CLAIMS_LIMIT, "overlaps": OVERLAP_LIMIT,
                   "conflicts": CONFLICTS_LIMIT, "actions": ACTIONS_LIMIT,
                   "controls": CONTROLS_LIMIT, "runs": RUNS_LIMIT,
                   "agents": AGENTS_LIMIT, "steer_targets": STEER_TARGETS_LIMIT},
        "honesty": {
            "presence_is_acknowledgment": False,
            "control_acceptance_is_applied": False,
            "aggregate_is_read_only": True,
            "command_surfaces": ["/v1/controls", "/v1/coordination-actions/{id}:resolve",
                                 "/v1/projects/{id}/nexus:acquire"],
        },
    }


def install(app, service, principal, command, envelope, **_options):
    """Register the read-only console aggregate on the standard capability seam."""
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    app.state.conflict_control_console = {"available": True}

    @app.get("/v1/projects/{project_id}/conflict-console")
    async def conflict_console(request: Request, project_id: str,
                               snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                               p=Depends(principal)):
        """Authorized, bounded, snapshot-fenced aggregate; never mutates state."""
        with service.store.lock:
            p = app.state.authenticate_request(request)
            snapshot = console_snapshot(app, service, p, project_id)
            cursor = app.state.checked_snapshot(p, project_id, snapshot_cursor)
            return envelope({**snapshot, "snapshot_cursor": cursor})

    return {"endpoints": ["GET /v1/projects/{project_id}/conflict-console"],
            "models": [], "descriptors": []}
