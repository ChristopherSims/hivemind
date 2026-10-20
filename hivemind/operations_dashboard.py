"""Read-only operations health summary for the dashboard.

This module answers one question per request: what is measurably healthy,
degraded, or unknown in this project's operations surface *right now*. It never
mutates canonical state (no retries, resets, rebuilds, exports, or worker
control) and it never invents measurements: every section carries an explicit
``available`` flag, an explicit scope label, scan-window bounds, and the
sample timestamp, so an empty or unreachable subsystem reads as "unknown",
never as "healthy".

Sections:
  ingestion  -- canonical watermark vs. semantic/vector and graph projection
                watermarks (jobs backlog), index read-path fallback, encoder
                and graph projection availability.
  jobs       -- bounded SQL counts of indexing job states for this project;
                the cross-project total is a separate, admin-only, clearly
                labeled ``global`` object and is never presented as a project
                count.
  delivery   -- delivery-state counts over the principal's *authorized*
                message window (agents see only their own conversations), with
                an explicit truncation flag at the scan limit.
  runs       -- heartbeat-presence and reported capture-health buckets plus a
                bounded sample of run ids. Presence reflects heartbeat recency
                only; it is never equated with verified capture.
  telemetry  -- content-free diagnostic queue counters; administrator only.
  warnings   -- coarse, derived degraded-service signals with stable codes.

Authorization follows the canonical seams: the request is re-authenticated
under the store lock via ``app.state.authenticate_request`` and project access
is the canonical ``Service.project_access`` check, so an unauthorized project
is indistinguishable from a missing one (404). No session, token, message
body, task id, job body, file path, or raw exception ever appears here.
"""
import time

from fastapi import Depends, Request

DELIVERY_STATES = ("available", "leased", "retry_wait", "dead_letter", "expired", "acked")
CAPTURE_STATES = ("healthy", "gaps", "unknown")
MESSAGE_SCAN_LIMIT = 1000
RUN_SCAN_LIMIT = 200
RUN_SAMPLE_LIMIT = 50
# Mirrors the presence thresholds of GET /v1/projects/{id}/dashboard-snapshot;
# they are emitted in the payload so the UI never hard-codes its own semantics.
ONLINE_WITHIN_SECONDS = 15
STALE_WITHIN_SECONDS = 30


def _unavailable(reason):
    return {"available": False, "reason": reason}


def _ingestion_section(app, project):
    store = app.state.store
    freshness = store.freshness(project)
    canonical = freshness["canonical_through_ingestion_seq"]
    indexes = getattr(app.state, "indexes", None)

    def watermark(name):
        through = freshness[f"{name}_through_ingestion_seq"]
        return {"available": True, "through_ingestion_seq": through,
                "pending_jobs": freshness[f"{name}_pending_jobs"],
                # A lag is a backlog measurement, not a verdict of health.
                "lag_ingestion_seq": max(0, canonical - through)}

    return {"available": True,
            "canonical_through_ingestion_seq": canonical,
            "vector": watermark("vector"),
            "graph": watermark("graph"),
            "index_read_path": ("dedicated" if indexes is not None and indexes.ro_db is not None
                                else "canonical_fallback"),
            "graph_projection": ("available" if indexes is not None and indexes.graph is not None
                                 else "unavailable"),
            "semantic_encoder": ("configured" if indexes is not None and indexes.encoder is not None
                                 else "absent")}


def _jobs_section(store, project, admin, global_admin=False):
    if not admin:
        return {**_unavailable("administrator_required"), "scope": "project", "global": None}
    rows = store.db.execute(
        "SELECT kind,state,COUNT(*) FROM jobs WHERE project=? GROUP BY kind,state",
        (project,)).fetchall()
    total = {"pending": 0, "done": 0, "dead": 0, "other": 0}
    by_kind = {}
    for kind, state, count in rows:
        key = state if state in total else "other"
        bucket = by_kind.setdefault(kind, {"pending": 0, "done": 0, "dead": 0, "other": 0})
        bucket[key] += count
        total[key] += count
    section = {"available": True, "scope": "project",
               "project": {"total": total, "by_kind": by_kind},
               "global": None}
    if global_admin:
        # Global operational counters are administrator-only and are labeled as
        # cross-project so they can never be mistaken for project counts.
        global_rows = store.db.execute("SELECT state,COUNT(*) FROM jobs GROUP BY state").fetchall()
        global_counts = {state: 0 for state in total}
        for state, count in global_rows:
            global_counts[state if state in global_counts else "other"] += count
        section["global"] = {"scope": "global_all_projects", "by_state": global_counts}
    return section


def _delivery_section(service, principal, project, now):
    # store.list applies the canonical visibility rules, so an agent principal
    # only ever counts messages it may read; the window is explicitly bounded.
    items = service.store.list("message", project, principal, MESSAGE_SCAN_LIMIT)
    states = {key: 0 for key in DELIVERY_STATES}
    for item in items:
        states[service.delivery_state(item)["delivery_status"]] += 1
    return {"available": True, "scope": "authorized_principal",
            "states": states, "scanned": len(items), "limit": MESSAGE_SCAN_LIMIT,
            "truncated": len(items) >= MESSAGE_SCAN_LIMIT,
            "window_note": ("Counts cover the newest bounded window of messages visible to the "
                            "requesting principal; they are not a global queue total.")}


def _presence(seen, now):
    if not isinstance(seen, (int, float)):
        return "unknown"
    age = now - seen
    if age < ONLINE_WITHIN_SECONDS:
        return "online"
    if age < STALE_WITHIN_SECONDS:
        return "stale"
    return "offline"


def _runs_section(store, project, principal, now):
    runs = store.list("run", project, principal, RUN_SCAN_LIMIT)
    presence = {"online": 0, "stale": 0, "offline": 0, "unknown": 0}
    capture = {key: 0 for key in (*CAPTURE_STATES, "other")}
    sample = []
    for run in runs:
        state = _presence(run.get("last_seen_at"), now)
        presence[state] += 1
        capture_state = (run.get("capture_health") or {}).get("state")
        capture[capture_state if capture_state in capture else "other"] += 1
        if len(sample) < RUN_SAMPLE_LIMIT:
            # Only coarse operational fields; no task ids, tools, files, or summaries.
            sample.append({"id": run["id"], "agent_id": run.get("agent_id"),
                           "status": run.get("status"), "presence": state,
                           "capture_state": capture_state if capture_state in CAPTURE_STATES else "unknown",
                           "last_seen_at": run.get("last_seen_at")})
    return {"available": True, "scanned": len(runs), "limit": RUN_SCAN_LIMIT,
            "truncated": len(runs) >= RUN_SCAN_LIMIT,
            "presence": presence, "capture": capture,
            "presence_thresholds_seconds": {"online_within": ONLINE_WITHIN_SECONDS,
                                            "stale_within": STALE_WITHIN_SECONDS},
            "presence_basis": "last_seen_at_heartbeats",
            "presence_note": ("Presence reflects heartbeat recency only; an online run is not "
                              "evidence of verified capture."),
            "sample": sample, "sample_limit": RUN_SAMPLE_LIMIT,
            "sample_truncated": len(runs) > RUN_SAMPLE_LIMIT}


def _telemetry_section(app, project, admin, global_admin=False):
    if not admin:
        return _unavailable("administrator_required")
    telemetry = getattr(app.state, "telemetry", None)
    if telemetry is None:
        return _unavailable("telemetry_not_installed")
    status = telemetry.status(project)
    return {"available": True, "scope": "project", "enabled": status["enabled"],
            "export_enabled": status["export_enabled"], "queued": status["queued"],
            "dropped": status["dropped"] if global_admin else None,
            "dropped_scope": "global_all_projects", "delivery": status["delivery"],
            "content_capture": status["content_capture"]}


def _warnings(sections):
    """Coarse degraded-service signals derived only from measured sections."""
    warnings = []
    delivery = sections["delivery"]
    if delivery.get("available"):
        dead = delivery["states"]["dead_letter"]
        if dead:
            warnings.append({"code": "dead_letter_messages", "scope": "project", "count": dead,
                             "message": f"{dead} message(s) in the authorized window reached dead letter."})
        if delivery["truncated"]:
            warnings.append({"code": "delivery_window_truncated", "scope": "project",
                             "count": delivery["scanned"],
                             "message": "Delivery counts cover only the newest bounded window."})
    jobs = sections["jobs"]
    if jobs.get("available"):
        dead = jobs["project"]["total"]["dead"]
        if dead:
            warnings.append({"code": "index_jobs_dead", "scope": "project", "count": dead,
                             "message": f"{dead} indexing job(s) exhausted retries for this project."})
    ingestion = sections["ingestion"]
    if ingestion.get("available"):
        if ingestion["index_read_path"] == "canonical_fallback":
            warnings.append({"code": "index_read_path_fallback", "scope": "project",
                             "count": None,
                             "message": "Retrieval is running on the canonical fallback read path."})
        if ingestion["graph_projection"] == "unavailable":
            warnings.append({"code": "graph_projection_unavailable", "scope": "project",
                             "count": None,
                             "message": "The graph projection store is unavailable on this node."})
        if ingestion["semantic_encoder"] == "absent":
            warnings.append({"code": "semantic_encoder_absent", "scope": "project",
                             "count": None,
                             "message": "No semantic encoder is configured; vector indexing cannot advance."})
    runs = sections["runs"]
    if runs.get("available"):
        gaps = runs["capture"]["gaps"]
        if gaps:
            warnings.append({"code": "capture_gaps_reported", "scope": "project", "count": gaps,
                             "message": f"{gaps} run(s) report capture gaps (missing source sequences)."})
        if runs["truncated"]:
            warnings.append({"code": "runs_window_truncated", "scope": "project",
                             "count": runs["scanned"],
                             "message": "Run buckets cover only the newest bounded window."})
    telemetry = sections["telemetry"]
    if telemetry.get("available") and telemetry["dropped"]:
        warnings.append({"code": "telemetry_spans_dropped", "scope": telemetry["dropped_scope"],
                         "count": telemetry["dropped"],
                         "message": f"{telemetry['dropped']} diagnostic span(s) were dropped."})
    return warnings


def operations_summary(app, service, principal, project):
    """Assemble the read-only summary; a failed section degrades to 'unknown'."""
    now = time.time()
    admin = principal["role"] == "admin"
    global_admin = admin and principal.get("project") is None

    def guard(build):
        try:
            return build()
        except Exception:
            # A measurement failure is reported as unknown, never raised raw and
            # never converted into a false "healthy" reading.
            return _unavailable("section_measurement_failed")

    sections = {
        "ingestion": guard(lambda: _ingestion_section(app, project)),
        "jobs": guard(lambda: _jobs_section(service.store, project, admin, global_admin)),
        "delivery": guard(lambda: _delivery_section(service, principal, project, now)),
        "runs": guard(lambda: _runs_section(service.store, project, principal, now)),
        "telemetry": guard(lambda: _telemetry_section(app, project, admin, global_admin)),
    }
    return {"project_id": project, "scope": "project", "role": principal["role"],
            "generated_at": now, "generated_at_unix_seconds": now,
            "generated_at_note": "All watermarks and counts are point-in-time samples taken together.",
            **sections, "warnings": _warnings(sections)}


def install(app, service, principal, command, envelope, policy=None):
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    app.state.operations_dashboard = {"available": True}

    @app.get("/v1/projects/{project_id}/operations-summary")
    async def operations_summary_route(request: Request, project_id: str, p=Depends(principal)):
        """Point-in-time operations health; read-only, project-scoped, role-gated."""
        with service.store.lock:
            p = app.state.authenticate_request(request)
            service.project_access(p, project_id)
            return envelope(operations_summary(app, service, p, project_id))
