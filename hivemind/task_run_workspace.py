"""Aggregated, authorized task/run workspace reads for the dashboard.

The task/run workspace needs one bounded, snapshot-bound read of a project's
assignments, dependencies, progress, and blockers instead of N+1 calls against
the canonical surfaces. This module is strictly read-only: it never mutates
canonical state, never executes or dispatches work, and never widens
authorization. Every handler reauthenticates the request through
``app.state.authenticate_request`` inside ``service.store.lock``, re-checks
project access, and binds the read to ``app.state.checked_snapshot`` when the
caller supplies a cursor.

Visibility rules are inherited, never reimplemented:

- Tasks are read through ``store.get``/``store.visible``/``store.task_view``,
  so private evidence redaction and team access stay canonical.
- ``depends_on``/``external_dependency_ids`` reference task IDs; any
  dependency the caller cannot currently read is dropped from the view so
  hidden task IDs never leak through the dependency graph.
- Blocked-task explanations and resolution requests are included only when
  the installed ``service.blocked_tasks`` policy enables them, and are read
  through the canonical ``BlockedTasks`` handle.

Mutations (reconcile, handoff, resolution-request triage, dispatch) stay on
their existing canonical endpoints; this module registers no command routes.
"""
from fastapi import Depends, Query, Request

from .scheduling import Scheduling, as_int
from .storage import Problem

PAGE_LIMIT_MAX = 200
PLAN_LIMIT_MAX = 200
HISTORY_LIMIT_MAX = 100
# Hard ceiling on the visible set scanned for counts/dependency filtering;
# when hit, the response is explicitly marked truncated.
SCAN_LIMIT = 1000

TERMINAL_STATUSES = ("completed", "cancelled")


def _as_str_list(value, limit=64):
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)][:limit]


def _visible_task_ids(store, principal, candidate_ids):
    """Keep only dependency IDs the principal can read right now."""
    visible = []
    for candidate in candidate_ids:
        try:
            store.get(candidate, principal, "task")
        except Problem:
            continue  # Hidden and missing dependencies are both omitted.
        visible.append(candidate)
    return visible


def _authorized_link(store, principal, resource_id, kind):
    """Return the referenced ID only when it is readable right now, else None.

    Run/agent references are navigation links, not content: an unreadable
    reference is omitted while the surrounding self-report metadata stays.
    """
    if not isinstance(resource_id, str) or not resource_id:
        return None
    try:
        store.get(resource_id, principal, kind)
    except Problem:
        return None
    return resource_id


def _execution_summary(execution, store=None, principal=None):
    """Self-reported execution view; progress is passed through, never derived.

    Run/agent links are re-authorized against the CURRENT store when a
    principal is supplied; unavailable links are omitted, never leaked.
    """
    if not isinstance(execution, dict) or not execution.get("id"):
        return None
    checkpoint = execution.get("checkpoint")
    run_id = execution.get("run_id")
    agent_id = execution.get("agent_id")
    if store is not None and principal is not None:
        run_id = _authorized_link(store, principal, run_id, "run")
        agent_id = _authorized_link(store, principal, agent_id, "agent")
    summary = {
        "id": execution.get("id"),
        "run_id": run_id,
        "agent_id": agent_id,
        "started_at": execution.get("started_at"),
        "progress": execution.get("progress"),
        "progress_basis": "agent_self_report",
    }
    if isinstance(checkpoint, dict):
        summary["checkpoint"] = {
            "id": checkpoint.get("id"),
            "summary": checkpoint.get("summary"),
            "evidence_unavailable": bool(checkpoint.get("evidence_unavailable")),
        }
    return summary


def authorize_entry_links(entries, store, principal):
    """Drop unreadable run/agent links from history entries, keeping metadata."""
    authorized = []
    for entry in entries:
        updated = dict(entry)
        for key, kind in (("run_id", "run"), ("agent_id", "agent")):
            if key in updated:
                link = _authorized_link(store, principal, updated[key], kind)
                if link is None:
                    del updated[key]
        authorized.append(updated)
    return authorized


def task_view(store, principal, body, blocked_handle=None):
    """Bounded workspace view of one already-authorized, already-redacted task."""
    scheduling = body.get("scheduling") if isinstance(body.get("scheduling"), dict) else {}
    history = [item for item in body.get("execution_history") or [] if isinstance(item, dict)]
    attempt_history = [item for item in scheduling.get("attempt_history") or []
                       if isinstance(item, dict)]
    view = {
        "id": body["id"],
        "revision": body["revision"],
        "title": body.get("title", ""),
        "status": body.get("status", "queued"),
        "created_at": body.get("created_at"),
        "priority": as_int(body.get("priority"), 0),
        "max_attempts": as_int(body.get("max_attempts"), 3),
        "required_capabilities": _as_str_list(body.get("required_capabilities"), 32),
        "plan_id": body.get("plan_id") if isinstance(body.get("plan_id"), str) else None,
        "plan_key": body.get("plan_key") if isinstance(body.get("plan_key"), str) else None,
        "plan_order": as_int(body.get("plan_order"), 0),
        "depends_on": _visible_task_ids(store, principal, _as_str_list(body.get("depends_on"))),
        "external_dependency_ids": _visible_task_ids(
            store, principal, _as_str_list(body.get("external_dependency_ids"))),
        "scheduling": {
            "state": scheduling.get("state", "queued"),
            "attempts": as_int(scheduling.get("attempts"), 0),
            "dispatches": as_int(scheduling.get("dispatches"), 0),
            "reassignments": as_int(scheduling.get("reassignments"), 0),
            "assigned_agent_id": scheduling.get("assigned_agent_id"),
            "escalated": bool(scheduling.get("escalated")),
            "escalation": scheduling.get("escalation")
            if isinstance(scheduling.get("escalation"), dict) else None,
            "retry_after": scheduling.get("retry_after"),
            "last_outcome": scheduling.get("last_outcome")
            if isinstance(scheduling.get("last_outcome"), dict) else None,
            "last_reconcile": scheduling.get("last_reconcile")
            if isinstance(scheduling.get("last_reconcile"), dict) else None,
            "attempt_history_count": len(attempt_history),
        },
        "execution": _execution_summary(body.get("execution"), store, principal),
        "execution_history_count": len(history),
        "latest_execution": _execution_summary(history[-1], store, principal) if history else None,
    }
    if blocked_handle is not None:
        try:
            explanation = blocked_handle.explain(
                principal, body["project_id"], body["id"])
            view["blocked"] = {
                "readiness": explanation.get("readiness"),
                "readiness_scope": explanation.get("readiness_scope"),
                "blocker_count": len(explanation.get("blockers") or []),
                "blockers": [
                    {key: blocker.get(key) for key in ("code", "action", "source", "message")}
                    for blocker in (explanation.get("blockers") or [])[:20]
                    if isinstance(blocker, dict)
                ],
                "suggested_actions": explanation.get("suggested_actions") or [],
                "authority": explanation.get("authority"),
            }
        except Problem:
            view["blocked"] = None  # Explanation unavailable; never guessed.
    return view


def history_entries(body):
    """Merged execution/attempt history, newest first, for explicit paging."""
    fields = ("id", "execution_id", "run_id", "agent_id", "at", "started_at", "ended_at",
              "status", "state", "outcome", "summary", "progress", "reason", "reason_code")
    entries = []
    for item in body.get("execution_history") or []:
        if isinstance(item, dict):
            entries.append({"kind": "execution", **{
                key: item[key] for key in fields if key in item
                and isinstance(item[key], (str, int, float, bool, type(None)))}})
    scheduling = body.get("scheduling") if isinstance(body.get("scheduling"), dict) else {}
    for item in scheduling.get("attempt_history") or []:
        if isinstance(item, dict):
            entries.append({"kind": "attempt", **{
                key: item[key] for key in fields if key in item
                and isinstance(item[key], (str, int, float, bool, type(None)))}})
    entries.sort(key=lambda item: str(item.get("at") or item.get("started_at") or ""),
                 reverse=True)
    return entries


def workspace_page(store, service, principal, project_id, limit, offset):
    """One bounded page of the visible task set plus plans and blockers."""
    # Visible task bodies via the canonical redaction path (task_view).
    bodies = store.list("task", project_id, principal, SCAN_LIMIT)
    truncated = len(bodies) >= SCAN_LIMIT
    counts = {"total": len(bodies), "by_status": {}, "truncated": truncated}
    for body in bodies:
        status = body.get("status", "queued")
        counts["by_status"][status] = counts["by_status"].get(status, 0) + 1
    # store.list returns newest first; keep that order stable for paging.
    page_bodies = bodies[offset:offset + limit]
    has_more = offset + limit < len(bodies)

    blocked_handle = getattr(service, "blocked_tasks", None)
    if blocked_handle is not None and not getattr(
            getattr(blocked_handle, "policy", None), "explain_enabled", False):
        blocked_handle = None

    tasks = [task_view(store, principal, body, blocked_handle) for body in page_bodies]

    visible_ids = {body["id"] for body in bodies}
    plans = []
    plans_truncated = False
    try:
        plan_bodies = Scheduling(service).plans(principal, project_id)
        plans_truncated = len(plan_bodies) >= PLAN_LIMIT_MAX
        for plan in plan_bodies:
            task_ids = [task_id for task_id in _as_str_list(plan.get("task_ids"), 64)
                        if task_id in visible_ids]
            plans.append({
                "id": plan["id"], "revision": plan["revision"],
                "title": plan.get("title", ""),
                "task_ids": task_ids,
                "task_count": len(task_ids),
            })
    except Problem:
        plans = []

    blocked_requests = None
    if blocked_handle is not None and getattr(
            blocked_handle.policy, "requests_enabled", False):
        try:
            blocked_requests = blocked_handle.list_requests(principal, project_id)
        except Problem:
            blocked_requests = []

    return {
        "project_id": project_id,
        "tasks": tasks,
        "task_count": len(tasks),
        "tasks_has_more": has_more,
        "next_offset": offset + limit if has_more else None,
        "counts": counts,
        "plans": plans,
        "plans_truncated": plans_truncated,
        "blocked_requests": blocked_requests,
    }


def install(app, service, principal, command, envelope, policy=None):
    """Register the read-only workspace routes; reinstall replaces only these."""
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    app.state.task_run_workspace = {"available": True}
    endpoints = []

    @app.get("/v1/projects/{project_id}/task-workspace")
    async def task_workspace(request: Request, project_id: str,
                             limit: int = Query(100, ge=1, le=PAGE_LIMIT_MAX),
                             offset: int = Query(0, ge=0),
                             snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                             p=Depends(principal)):
        """Bounded authorized aggregate of assignments, dependencies, blockers."""
        with service.store.lock:
            p = app.state.authenticate_request(request)
            service.project_access(p, project_id)
            cursor = app.state.checked_snapshot(p, project_id, snapshot_cursor)
            page = workspace_page(service.store, service, p, project_id, limit, offset)
            return envelope({**page, "snapshot_cursor": cursor})

    endpoints.append("GET /v1/projects/{project_id}/task-workspace")

    @app.get("/v1/tasks/{task_id}/workspace-history")
    async def task_workspace_history(request: Request, task_id: str,
                                     limit: int = Query(50, ge=1, le=HISTORY_LIMIT_MAX),
                                     offset: int = Query(0, ge=0),
                                     snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                                     p=Depends(principal)):
        """Explicitly paged execution/attempt history for one authorized task.

        Bound to the caller's checked snapshot so a mid-pagination source
        invalidation reads as 409, never as silently stale evidence.
        """
        with service.store.lock:
            p = app.state.authenticate_request(request)
            task = service.store.get(task_id, p, "task")
            cursor = app.state.checked_snapshot(p, task["project_id"], snapshot_cursor)
            entries = authorize_entry_links(history_entries(task), service.store, p)
            page = entries[offset:offset + limit]
            has_more = offset + limit < len(entries)
            return envelope({
                "task_id": task["id"],
                "revision": task["revision"],
                "entries": page,
                "count": len(page),
                "total": len(entries),
                "has_more": has_more,
                "next_offset": offset + limit if has_more else None,
                "snapshot_cursor": cursor,
                "basis": "agent_self_reports_and_operator_actions",
            })

    endpoints.append("GET /v1/tasks/{task_id}/workspace-history")
    return {"endpoints": endpoints, "models": []}
