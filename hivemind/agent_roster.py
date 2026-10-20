"""Caller-visible work snapshots for the dashboard roster."""

from .storage import Problem

TERMINAL = {"completed", "failed", "cancelled"}


def roster_work(service, principal, project):
    store = service.store
    runs = {run["id"]: run for run in store.list("run", project, principal, 1000)
            if run["status"] not in TERMINAL}
    tasks = {task["id"]: task for task in store.list("task", project, principal, 1000)
             if task["status"] not in TERMINAL}
    scopes = {scope["id"]: scope for scope in store.list("scope", project, principal, 1000)}
    claims = service.active_claims(principal, project)
    result = {}

    def work(agent):
        return result.setdefault(agent, {"current_tasks": [], "owned_scopes": [], "blockers": []})

    task_ids = {}
    for run in runs.values():
        snapshot = work(run["agent_id"])
        if run.get("task_id") in tasks:
            task_ids.setdefault(run["agent_id"], set()).add(run["task_id"])
        if run["status"] in {"blocked", "waiting_input", "paused"}:
            snapshot["blockers"].append({
                "code": "run_" + run["status"], "run_id": run["id"],
                "label": {"blocked": "Run blocked, reason not reported",
                          "waiting_input": "Waiting for input", "paused": "Run paused"}[run["status"]],
            })
    for claim in claims:
        run = runs.get(claim["run_id"])
        if not run or run["agent_id"] != claim["agent_id"]:
            continue
        if claim.get("task_id") in tasks:
            task_ids.setdefault(run["agent_id"], set()).add(claim["task_id"])
        if claim["state"] == "active" and claim["mode"] == "exclusive_owner":
            owned = work(run["agent_id"])["owned_scopes"]
            for scope_id in claim["scope_ids"]:
                scope = scopes.get(scope_id)
                if scope and not any(item["id"] == scope_id for item in owned):
                    owned.append({"id": scope_id, "display_name": scope["display_name"]})
    for task in tasks.values():
        run = runs.get((task.get("execution") or {}).get("run_id"))
        if run:
            task_ids.setdefault(run["agent_id"], set()).add(task["id"])
    for agent, ids in task_ids.items():
        snapshot = work(agent)
        for task_id in sorted(ids):
            task = tasks[task_id]
            snapshot["current_tasks"].append({
                "id": task_id, "title": task["title"], "status": task["status"],
            })
            explainer = getattr(service, "blocked_tasks", None)
            if explainer:
                try:
                    explanation = explainer.explain(principal, project, task_id)
                except Problem as error:
                    if error.status != 404:
                        raise
                    continue
                for blocker in explanation["blockers"]:
                    snapshot["blockers"].append({
                        "code": blocker["code"], "task_id": task_id,
                        "label": blocker["code"].replace("_", " ").capitalize(),
                    })
    return result
