"""Authorized, bounded agent evidence for the dashboard details drawer.

The drawer needs a single read that shows genuine evidence the caller is already
allowed to see: canonical run ledger entries, artifacts produced by those runs,
agent/run-scoped memory evidence, and coordination events. Each section reuses the
same authorization primitives as the dedicated routes (``Store.visible``,
``Store.get``, ``Service.evidence``), and every query is keyset-paginated by the
canonical sequence so opening the drawer never triggers an unbounded scan.

Registration is owned by the integration layer:

    from .agent_evidence import install as install_agent_evidence
    install_agent_evidence(app, service, principal, command, envelope)
"""

from __future__ import annotations

import json
import time

from fastapi import Depends, Query, Request

from .storage import Problem

DEFAULT_SECTION_LIMIT = 20
MAX_SECTION_LIMIT = 50
MAX_AGENT_RUNS = 200
PREVIEW_CHARS = 2000
SCAN_WINDOW = 256
MAX_EXAMINED = 5000
TERMINAL_PRESENCE = (30, 15)


def _private_clause(principal):
    """SQL mirror of ``Store.visible`` for private rows (hidden rows are never read)."""
    if principal["role"] == "admin":
        return "", []
    return " AND (private_agent IS NULL OR private_agent=?)", [principal.get("agent")]


def _paginate(store, principal, sql, args, before, limit, shape, visible=None):
    """Return ``(items, next_before)`` for one exact, bounded keyset page.

    ``before`` is an exclusive canonical-sequence cursor. A non-positive cursor is a
    client request to skip this section entirely (used when paging another section).
    ``shape(row, body)`` returns the public item or ``None`` to veto a row that passed
    ``visible`` but fails a stricter check (stale evidence, invalid memory). Windows are
    read in ``SCAN_WINDOW`` chunks up to ``MAX_EXAMINED`` rows, so ``next_before`` is
    ``None`` exactly when no further authorized row exists, and no scan is unbounded.
    """
    if before is not None and before <= 0:
        return [], None
    check = visible or (lambda row: store.visible(row, principal))
    pairs, cursor, examined, exhausted = [], before, 0, False
    while len(pairs) <= limit and examined < MAX_EXAMINED:
        window = min(SCAN_WINDOW, MAX_EXAMINED - examined)
        if examined == 0:
            window = min(window, limit + 1)  # the common case needs exactly one small read
        page_sql = sql
        page_args = list(args)
        if cursor is not None:
            page_sql += " AND seq<?"
            page_args.append(cursor)
        page_sql += " ORDER BY seq DESC LIMIT ?"
        page_args.append(window)
        rows = store.db.execute(page_sql, page_args).fetchall()
        if not rows:
            exhausted = True
            break
        examined += len(rows)
        for row in rows:
            cursor = row["seq"]
            if not check(row):
                continue
            item = shape(row, json.loads(row["body"]))
            if item is None:
                continue
            pairs.append((row["seq"], item))
            if len(pairs) > limit:
                break
        if len(rows) < window:
            exhausted = True
            break
    items = [item for _, item in pairs[:limit]]
    if len(pairs) > limit:
        next_before = pairs[limit - 1][0]
    elif not exhausted and pairs:
        next_before = pairs[-1][0]
    elif not exhausted and cursor is not None:
        next_before = cursor
    else:
        next_before = None
    return items, next_before


def _run_ids(store, principal, project, agent_id):
    """Newest bounded run ids for this agent, used to attribute produced artifacts."""
    sql = ("SELECT * FROM resources WHERE project=? AND kind='run' AND deleted=0 "
           "AND json_extract(body,'$.agent_id')=?")
    args = [project, agent_id]
    clause, clause_args = _private_clause(principal)
    sql += clause + " ORDER BY seq DESC LIMIT ?"
    args += clause_args
    args.append(MAX_AGENT_RUNS)
    return [row["id"] for row in store.db.execute(sql, args).fetchall()
            if store.visible(row, principal)]


def _ledger(store, principal, project, agent_id, before, limit):
    sql = ("SELECT * FROM resources WHERE project=? AND kind='run' AND deleted=0 "
           "AND json_extract(body,'$.agent_id')=?")
    args = [project, agent_id]
    clause, clause_args = _private_clause(principal)
    sql += clause
    args += clause_args
    now = time.time()

    def shape(row, run):
        gap = now - run.get("last_seen_at", 0)
        task_id = run.get("task_id")
        if task_id:
            try:
                task = store.get(task_id, principal, "task")
                if task.get("project_id") != project:
                    task_id = None
            except Problem:
                task_id = None
        return {
            "id": run["id"], "revision": row["revision"], "status": run.get("status"),
            "presence": "offline" if gap >= TERMINAL_PRESENCE[0] else
                        "stale" if gap >= TERMINAL_PRESENCE[1] else "online",
            "task_id": task_id, "last_seen_at": run.get("last_seen_at"),
            "started_at": run.get("started_at"), "capture_health": run.get("capture_health"),
            "summary": run.get("summary"), "control_revision": run.get("control_revision"),
        }

    return _paginate(store, principal, sql, args, before, limit, shape)


def _preview(content, limit=PREVIEW_CHARS):
    if not isinstance(content, dict):
        return content
    text = content.get("text")
    if isinstance(text, str) and len(text) > limit:
        return {**content, "text": text[:limit], "truncated": True}
    return content


def _artifacts(store, principal, project, run_ids, before, limit):
    if not run_ids:
        return [], None
    placeholders = ",".join("?" * len(run_ids))
    sql = ("SELECT * FROM resources WHERE project=? AND kind='artifact' AND deleted=0 "
           f"AND json_extract(body,'$.run_id') IN ({placeholders})")
    args = [project, *run_ids]
    clause, clause_args = _private_clause(principal)
    sql += clause
    args += clause_args

    def shape(row, body):
        return {
            "id": body["id"], "revision": row["revision"], "title": body.get("title"),
            "run_id": body.get("run_id"), "created_at": body.get("created_at"),
            "content": _preview(body.get("content")),
        }

    return _paginate(store, principal, sql, args, before, limit, shape)


def _memories(store, principal, project, agent_id, run_ids, before, limit):
    clauses = ["(json_extract(body,'$.scope_kind')='agent' "
               "AND json_extract(body,'$.scope_owner_id')=?)"]
    args = [project, agent_id]
    if run_ids:
        placeholders = ",".join("?" * len(run_ids))
        clauses.append("(json_extract(body,'$.scope_kind')='run' "
                       f"AND json_extract(body,'$.scope_owner_id') IN ({placeholders}))")
        args += run_ids
    sql = ("SELECT * FROM resources WHERE project=? AND kind='memory' AND deleted=0 AND ("
           + " OR ".join(clauses) + ")")
    clause, clause_args = _private_clause(principal)
    sql += clause
    args += clause_args

    def shape(row, body):
        # Hidden, expired, disputed, or unsupported-evidence memories are not evidence.
        if not store.valid_memory(body, principal):
            return None
        return {
            "id": body["id"], "revision": row["revision"], "title": body.get("title"),
            "category": body.get("category"), "scope_kind": body.get("scope_kind"),
            "scope_owner_id": body.get("scope_owner_id"), "status": body.get("status"),
            "effective_status": store.memory_state(body), "origin": body.get("origin"),
            "evidence_refs": body.get("evidence_refs", []), "validity": body.get("validity"),
            "expires_at": body.get("expires_at"), "created_at": body.get("created_at"),
            "content": _preview(body.get("content")),
        }

    return _paginate(store, principal, sql, args, before, limit, shape)


def _events(store, service, principal, project, agent_id, before, limit):
    sql = ("SELECT * FROM events WHERE project=? AND json_extract(body,'$.agent_id')=?")
    args = [project, agent_id]

    def shape(row, event):
        try:
            # An event is evidence only while its run and referenced evidence stay authorized.
            store.get(event["run_id"], principal, "run")
            service.evidence(principal, project, event.get("data", {}).get("evidence_refs", []))
        except Problem:
            return None
        shaped = {**event, **json.loads(row["result"]), "ingestion_seq": row["seq"]}
        # R4 presentation: denied-mutation advisories surface as first-class denials
        # (agent-reported; never authority or proof the mutation happened).
        if event.get("type", "").endswith("agent.status_reported") and (
                event.get("data", {}).get("text", "").startswith("mutation denied:")):
            shaped["denied_mutation"] = True
            shaped["denied_tool"] = event["data"]["text"].split(":", 1)[-1].split("—")[0].strip()
        return shaped

    # The events table has no private_agent column; visibility is enforced in shape().
    return _paginate(store, principal, sql, args, before, limit, shape, visible=lambda row: True)


def install(app, service, principal, command, envelope, **options):
    """Register the bounded agent evidence read; ``command`` is unused (read-only)."""
    store = service.store
    default_limit = int(options.get("section_limit", DEFAULT_SECTION_LIMIT))

    @app.get("/v1/agents/{agent_id}/evidence")
    async def agent_evidence(request: Request, agent_id: str, project_id: str | None = None,
                             limit: int = Query(default_limit, ge=1, le=MAX_SECTION_LIMIT),
                             ledger_before: int | None = None,
                             artifacts_before: int | None = None,
                             memories_before: int | None = None,
                             events_before: int | None = None,
                             snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                             p=Depends(principal)):
        with store.lock:
            p = app.state.authenticate_request(request)
            agent = store.get(agent_id, p, "agent")
            project = agent["project_id"]
            if project_id is not None and project_id != project:
                raise Problem("not_found", 404)
            service.project_access(p, project)
            cursor = app.state.checked_snapshot(p, project, snapshot_cursor)
            run_ids = _run_ids(store, p, project, agent_id)
            ledger, ledger_next = _ledger(store, p, project, agent_id, ledger_before, limit)
            artifacts, artifacts_next = _artifacts(store, p, project, run_ids, artifacts_before, limit)
            memories, memories_next = _memories(store, p, project, agent_id, run_ids, memories_before, limit)
            events, events_next = _events(store, service, p, project, agent_id, events_before, limit)
            return envelope({
                "snapshot_cursor": cursor,
                "agent": {key: agent.get(key) for key in
                          ("id", "display_name", "description", "project_id")},
                "generated_at": time.time(),
                "limit": limit,
                "ledger": {"items": ledger, "next_before": ledger_next},
                "artifacts": {"items": artifacts, "next_before": artifacts_next},
                "memories": {"items": memories, "next_before": memories_next},
                "events": {"items": events, "next_before": events_next},
            })
