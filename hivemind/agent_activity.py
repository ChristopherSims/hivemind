"""Current, authorized activity roster; reported status is not process attestation."""
import json
import time

from .storage import Problem

MAX_RUNS_PER_AGENT = 32
TERMINAL = {"completed", "failed", "cancelled"}


def activity_roster(service, principal, project_id):
    store = service.store
    service.project_access(principal, project_id)
    result = []
    now = time.time()
    for agent in store.list("agent", project_id, principal):
        # Read the newest visible runs for this exact canonical agent/project.
        private = "" if principal["role"] == "admin" else " AND (private_agent IS NULL OR private_agent=?)"
        params = [project_id, agent["id"]]
        if private:
            params.append(principal.get("agent"))
        rows = store.db.execute(
            "SELECT * FROM resources WHERE project=? AND kind='run' AND deleted=0 "
            "AND json_extract(body,'$.agent_id')=?" + private +
            " ORDER BY seq DESC,id DESC LIMIT ?", (*params, MAX_RUNS_PER_AGENT + 1)).fetchall()
        runs = []
        for row in rows[:MAX_RUNS_PER_AGENT]:
            if not store.visible(row, principal):
                continue
            run = json.loads(row["body"])
            task = None
            task_id = run.get("task_id")
            if task_id and run["status"] not in TERMINAL:
                try:
                    candidate = store.get(task_id, principal, "task")
                    execution = candidate.get("execution")
                    if (candidate["project_id"] == project_id
                            and candidate["status"] not in TERMINAL
                            and (not execution or (
                                execution.get("run_id") == run["id"]
                                and execution.get("agent_id") == agent["id"]))):
                        checkpoint = (execution or {}).get("checkpoint") or {}
                        task = {"id": candidate["id"], "title": candidate["title"],
                                "status": candidate["status"], "revision": candidate["revision"],
                                "execution_id": (execution or {}).get("id"),
                                "progress": (execution or {}).get("progress"),
                                "summary": checkpoint.get("summary")}
                except Problem as error:
                    if error.status != 404:
                        raise
            # Never surface a task-derived run summary when its task is hidden/stale.
            summary = task["summary"] if task else None
            origin = "task_checkpoint" if summary is not None else None
            if summary is None and (task is not None or not task_id):
                reported = run.get("summary") or {}
                summary, origin = reported.get("text"), reported.get("origin")
            gap = now - run.get("last_seen_at", 0)
            runs.append({"run_id": run["id"], "status": run["status"],
                         "presence": "offline" if gap >= 30 else "stale" if gap >= 15 else "online",
                         "last_seen_at": run.get("last_seen_at"), "current_task": task,
                         "summary": summary, "summary_origin": origin})
        # Prefer a task-bearing live run, then another live run, then latest terminal.
        current = next((r for r in runs if r["current_task"]), None)
        current = current or next((r for r in runs if r["status"] not in TERMINAL), None)
        current = current or (runs[0] if runs else None)
        context = {**(current or {"run_id": None, "status": "idle", "presence": "offline",
                                 "last_seen_at": None, "current_task": None, "summary": None,
                                 "summary_origin": None}),
                   "runs": runs, "runs_truncated": len(rows) > MAX_RUNS_PER_AGENT,
                   "authority": "canonical_reported_activity"}
        result.append({**agent, "status": context["status"],
                       "current_task": context["current_task"], "summary": context["summary"],
                       "context": context})
    return result
