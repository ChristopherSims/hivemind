import asyncio
import hashlib
import json
import secrets
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pathlib import Path

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError
from starlette.routing import Match

from . import live_state, models as m
from .indexes import Indexes
from .service import Service
from .storage import Problem, Store, canonical, uid


def hash_token(token):
    return hashlib.sha256(token.encode()).hexdigest()


def discovery_server_id(app) -> str:
    """Stable per-deployment server identity from the canonical store directory.

    SHA-256 over the resolved data-dir path: derived, not secret. Two apps on one
    directory share the ID; a moved/renamed directory gets a new ID. This is
    discovery metadata, never an authentication or authorization input.
    """
    directory = getattr(getattr(app.state, "store", None), "directory", None)
    if directory is None:
        raise Problem("store_not_available", 500)
    return "srv_" + hashlib.sha256(
        str(Path(directory).resolve()).encode()).hexdigest()[:24]


def envelope(data):
    return {"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"), "data": data}


def create_app(directory=".hivemind-data", encoder=None, workers=True, capability_options=None, enable_ui=False):
    isolation_guard = (capability_options or {}).get("agent_automation", {}).get("isolation_guard")
    if isolation_guard is not None:
        isolation_guard.bind_resources(directory, Path(directory) / "git-collaboration")
    store = Store(directory)
    service = Service(store)
    indexes = Indexes(store, encoder)
    service.indexes = indexes
    request_lock = asyncio.Lock()
    snapshot_context = ContextVar("memory_repository_snapshot", default=None)
    in_flight = set()

    def finished(job):
        in_flight.discard(job)
        if not job.cancelled():
            job.exception()  # Observe failures even when scheduler shutdown detached its waiter.

    async def threaded(function, *args):
        job = asyncio.create_task(asyncio.to_thread(function, *args))
        in_flight.add(job)
        job.add_done_callback(finished)
        # Cancelling the scheduler does not cancel the only handle observing a
        # still-running model/process callback that may later publish evidence.
        return await asyncio.shield(job)

    async def worker():
        while True:
            await threaded(indexes.drain, 100)
            for name in ("review_automation", "nexus_git_automation", "memory_lifecycle",
                         "coordination_runner", "goal_runner"):
                automation = getattr(app.state, name, None)
                if automation:
                    try:
                        await threaded(automation.scheduled_tick)
                    except Exception as error:
                        setattr(app.state, name + "_error", type(error).__name__)
            await asyncio.sleep(1)

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(worker()) if workers else None
        yield
        pipeline = getattr(app.state, "memory_pipeline", None)
        if pipeline:
            pipeline.stop_worker()
            if pipeline._worker:
                await asyncio.to_thread(pipeline._worker.join, 30)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if pipeline and pipeline._worker and pipeline._worker.is_alive():
            app.state.shutdown_pending = True
            return  # A stuck trusted extractor must not use storage that has been closed.
        if in_flight:
            _, pending = await asyncio.wait(tuple(in_flight), timeout=30)
            if pending:
                app.state.shutdown_pending = True
                return  # Detached work can still publish; keep its canonical storage open.
        # Stop every commit bridge before closing anything it can still snapshot.
        pending_bridges = []
        for name in ("worker_provisioner", "memory_event_bridge", "activity_stream_bridge",
                     "message_push_bridge", "memory_adapter_bridge"):
            bridge = getattr(app.state, name, None)
            if bridge is None:
                continue
            try:
                result = await asyncio.to_thread(bridge.close)
                thread = getattr(bridge, "_thread", None)
                if (result is False or isinstance(result, dict) and result.get("drained") is False
                        or thread is not None and thread.is_alive()):
                    pending_bridges.append(name)
            except Exception as error:
                pending_bridges.append(name)
                setattr(app.state, name + "_shutdown_error", type(error).__name__)
        if pending_bridges:
            app.state.shutdown_pending = True
            app.state.shutdown_pending_features = tuple(pending_bridges)
            return  # Keep hub/ledger/index/store dependencies available for late work.
        activity_hub = getattr(app.state, "activity_stream_hub", None)
        if activity_hub is not None:
            await asyncio.to_thread(activity_hub.close)
        hub = getattr(app.state, "message_push_hub", None)
        if hub is not None:
            await asyncio.to_thread(hub.close)
        ledger = getattr(app.state, "task_budgets", None)
        if ledger is not None:
            await asyncio.to_thread(ledger.close)
        # A thread dispatched before cancellation may still own the graph.
        with store.lock, indexes.lock:
            indexes.close()
            store.close()
        telemetry = getattr(app.state, "telemetry", None)
        if telemetry:
            await asyncio.to_thread(telemetry.close)

    app = FastAPI(title="Hivemind", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.service = service
    app.state.indexes = indexes
    app.state.run_detached = threaded

    def monitor_denial(request):
        provisioner = getattr(app.state, "worker_provisioner", None)
        if provisioner is not None:
            try:
                provisioner.api_denial(authenticate(request))
            except Problem:
                pass  # Unknown credentials cannot target someone else's process.

    @app.exception_handler(Problem)
    async def problem_handler(request, exc):
        if exc.status in (403, 404):
            monitor_denial(request)
        return JSONResponse({"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"),
                             "error": {"code": exc.code, "message": exc.code.replace("_", " "),
                                       "retryable": exc.status in (429, 503)}}, status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request, exc):
        # Do not echo Pydantic input fields, which can contain secrets.
        return JSONResponse({"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"),
                             "error": {"code": "invalid_request", "message": "Request schema validation failed",
                                       "retryable": False}}, status_code=422)

    def _authenticate(request):
        auth = request.headers.get("authorization", "")
        principal = None
        if auth.startswith("Bearer "):
            principal = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                                         (hash_token(auth[7:]),)).fetchone()
        elif request.cookies.get("hivemind_session"):
            session = store.db.execute(
                "SELECT * FROM sessions WHERE hash=? AND expires>?",
                (hash_token(request.cookies["hivemind_session"]), time.time())).fetchone()
            if session:
                principal = store.db.execute("SELECT * FROM principals WHERE id=?", (session["principal"],)).fetchone()
                if request.method not in ("GET", "HEAD", "OPTIONS"):
                    csrf = request.headers.get("x-csrf-token", "")
                    if not secrets.compare_digest(hash_token(csrf), session["csrf_hash"]):
                        raise Problem("csrf_required", 403)
        if not principal:
            raise Problem("authentication_required", 401)
        return dict(principal)

    def authenticate(request):
        with store.lock:
            return _authenticate(request)

    app.state.authenticate_request = authenticate

    async def principal(request: Request):
        return authenticate(request)

    @app.middleware("http")
    async def boundaries(request, call_next):
        # Enforce aggregate size on the actual bytes, not only Content-Length.
        if request.method in ("POST", "PATCH", "PUT"):
            total, chunks = 0, []
            async for chunk in request.stream():
                total += len(chunk)
                if total > 1048576:
                    return await problem_handler(request, Problem("payload_too_large", 413))
                chunks.append(chunk)
            request._body = b"".join(chunks)
        origin = request.headers.get("origin")
        if request.method not in ("GET", "HEAD", "OPTIONS") and origin and origin != str(request.base_url).rstrip("/"):
            return await problem_handler(request, Problem("origin_not_allowed", 403))
        detached = any(
            getattr(getattr(route, "endpoint", None), "hivemind_detached_work", False)
            and route.matches(request.scope)[0] == Match.FULL
            for route in app.router.routes)
        snapshot_token = snapshot_context.set({})
        try:
            if detached:
                # Only explicitly marked, two-phase handlers manage their own short
                # canonical snapshots/transactions around long model/process work.
                response = await call_next(request)
            else:
                # Single canonical connection, serialized quick requests.
                async with request_lock:
                    with store.lock:
                        response = await call_next(request)
        finally:
            snapshot_context.reset(snapshot_token)
        if response.status_code in (403, 404):
            monitor_denial(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        if request.url.path.startswith("/v1"):
            response.headers["Cache-Control"] = "no-store"
        return response

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) if isinstance(request, m.Model) else request
        telemetry = getattr(app.state, "telemetry", None)
        if telemetry:
            with telemetry.span("canonical.command", project_id=p.get("project"), agent_id=p.get("agent")):
                return envelope(service.mutate(p, operation, key, payload, function))
        return envelope(service.mutate(p, operation, key, payload, function))
    from .capabilities import install
    install(app, service, principal, command, envelope, options=capability_options)
    def memory_current(memory):
        from .memory_pipeline import currentness
        pipeline = app.state.memory_pipeline
        if not pipeline.config.enabled or not pipeline.snapshot_available():
            return True
        validity = memory.get("validity") or {}
        if not any(validity.get(key) for key in ("repository_id", "branch", "observed_commit")):
            return True
        box = snapshot_context.get()
        box = {} if box is None else box
        if "snapshot" not in box:
            try:
                box["snapshot"] = pipeline.snapshot()
            except Exception:
                box["snapshot"] = None
        return box["snapshot"] is not None and currentness(memory, box["snapshot"])[0] != "drifted"
    store.memory_freshness_filter = memory_current

    @app.get("/.well-known/hivemind")
    async def discovery():
        from .credential_management import CAPABILITY_STATUS, DISCOVERY_CAPABILITIES
        credential_features = getattr(app.state, "credential_management", None) is not None
        # Stable server identity: derived once per app from the canonical store directory,
        # not a rotating secret and not a credential.
        from .api import discovery_server_id
        return {"protocol": "hivemind", "supported_versions": ["0.1.0"], "api_base": "/v1",
                "server_id": discovery_server_id(app),
                "agent_tool_context": {"url": "/v1/tool-context",
                                       "description": "Operation purpose, schemas, authority, side effects, "
                                                      "retry safety and limits; descriptions grant no permissions.",
                                       "authentication_required": True},
                "capabilities": ["events.batch", "messages.durable", "board.human_participation",
                                 *(DISCOVERY_CAPABILITIES if credential_features else ()),
                                 *(["board.unread"] if getattr(app.state, "board_reads", None) else []),
                                 *(["dashboard.context_explorer"] if getattr(app.state, "context_explorer", None) else []),
                                 *(["dashboard.operations"] if getattr(app.state, "operations_dashboard", None) else []),
                                 *(["dashboard.tasks"] if getattr(app.state, "task_run_workspace", None) else []),
                                 *(["dashboard.coordination"] if getattr(app.state, "conflict_control_console", None) else []),
                                 *(["dashboard.privacy_audit"] if getattr(app.state, "privacy_audit", None) else []),
                                 "resources.team_access",
                                 "agents.tool_context", "telemetry.otlp_traces",
                                 "coordination.policy", "coordination.declared_resource_conflicts",
                                 "runtime.a2a", "runtime.acp",
                                 "runtime.continuous_capture", "runtime.tool_gateway",
                                 "runtime.safe_pause_resume", "messages.push_signals",
                                 "memory.versioned", "context.agent_selectors", "coordination.deterministic",
                                 "memory.lifecycle",
                                 "messages.retry", "conversations.unread",
                                 "dashboard.timeline_search", "dashboard.keyset_pagination",
                                 "memory.extraction", "memory.review_notifications",
                                 "tasks.durable_execution",
                                 "tasks.scheduling", "tasks.verification", "agents.capabilities",
                                 "goals.orchestration", "agents.structured_handoffs",
                                 "agents.capability_routing", "tasks.artifact_contracts", "tasks.budgets",
                                 "agents.activity_subscriptions",
                                 "memory.event_maintenance",
                                 "conversations.workflows", "messages.lease_renewal",
                                 "context.compact_assembly", "memory.pipeline_opt_in",
                                 "runtime.reconciliation", "runtime.cooperative_controls",
                                 "coordination.nexus", "coordination.controls", "streams.sse",
                                 *(["graph.ladybugdb"] if indexes.graph else ["graph.canonical_fallback"]),
                                 * (["board.hybrid_search"] if encoder else ["board.lexical_search"])],
                "authentication": {"schemes": ["bearer", "same_origin_session"]},
                "capability_status": {
                    **(CAPABILITY_STATUS if credential_features else {}),
                    "memory.event_maintenance": {
                        "enabled": bool(getattr(app.state, "memory_event_bridge", None)
                                        and app.state.memory_event_bridge.policy.enabled
                                        and app.state.memory_event_bridge.automation.policy.enabled),
                        "source_review": "durable_authorized_revision_review",
                        "invalidation": "explicit_operator_opt_in",
                        "unknown_compute_replay": False},
                    "agents.activity_subscriptions": {
                        "enabled": bool(getattr(app.state, "activity_stream_hub", None)),
                        "presence_is_attestation": False, "delivery": "scoped_at_least_once"},
                    "goals.orchestration": {"enabled": True, "automatic_revision": bool(
                        getattr(app.state, "goal_runner", None)
                        and app.state.goal_runner.config.revise_failed),
                        "model_planning": "explicit_provider_only",
                        "external_effect_proof": False},
                    "agents.structured_handoffs": {"enabled": True, "recipient_acceptance": True,
                                                  "acceptance_transfers_ownership": False},
                    "agents.capability_routing": {"enabled": bool(
                        getattr(app.state, "capability_routing", None)
                        and app.state.capability_routing.policy.enabled),
                        "declarations_are_attestations": False},
                    "tasks.artifact_contracts": {"enabled": bool(
                        getattr(app.state, "artifact_contracts", None)),
                        "native_checks": "trusted_evidence_bound_verifier_only"},
                    "tasks.budgets": {"enabled": bool(
                        getattr(app.state, "task_budgets", None)
                        and app.state.task_budgets.policy.enabled),
                        "runtime_contract": "exact_serializer_and_enforced_output_max",
                        "stock_vendor_adapters": False,
                        "cancellation": "cooperative"},
                    "telemetry.otlp_traces": {
                    "enabled": bool(getattr(app.state, "telemetry", None)
                                    and app.state.telemetry.policy.enabled),
                    "export_enabled": bool(getattr(app.state, "telemetry", None)
                                           and app.state.telemetry.policy.allow_export)},
                    "coordination.policy": {
                        "per_project_opt_in": True,
                        "background_enabled": bool(getattr(app.state, "coordination_runner", None)
                                                   and app.state.coordination_runner.config.enabled),
                        "unknown_execution_action": "manual_hold"},
                    "runtime.continuous_capture": {
                        "mode": "polling", "content_captured": False,
                        "sub_interval_changes": "net effect only",
                        "operator_opt_in": True},
                    "runtime.tool_gateway": {
                        "strictness": "operator_configured_only",
                        "enforcement": "host hook", "advisory_endpoint": True},
                    "runtime.safe_pause_resume": {
                        "requires": "supported runtime adapter with quiescent checkpoints",
                        "unsupported_adapter_outcome": "held"},
                    "messages.push_signals": {
                        "enabled": bool(getattr(app.state, "message_push_hub", None)),
                        "delivery": "at_least_once_identity_only",
                        "content_in_signals": False,
                        "fallback": "bounded canonical reconcile polling"},
                    "runtime.a2a": {"protocol": "A2A", "version": "1.0",
                                    "explicit_operator_configuration": True,
                                    "pause_resume": False, "os_sandbox": False},
                    "runtime.acp": {"protocol": "Agent Client Protocol", "version": 1,
                                    "explicit_operator_configuration": True,
                                    "pause_resume": False, "os_sandbox": False}},
                "limits": {"max_batch_events": 100, "max_batch_bytes": 1048576, "max_event_bytes": 131072,
                           "max_inline_content_bytes": 65536}}

    @app.get("/v1/tool-context")
    async def tool_context(operation_id: str | None = Query(None, max_length=512), p=Depends(principal)):
        """Describe tools and queries without returning project content or granting authority."""
        from .tool_context import manifest
        value = manifest(app)
        if operation_id is not None:
            selected = [item for item in value["operations"] if item["id"] == operation_id]
            if not selected:
                raise Problem("operation_not_found", 404)
            value = {**value, "operations": selected}
        return envelope(value)

    @app.get("/v1/commands/receipt")
    async def command_receipt(operation: str = Query(..., min_length=1, max_length=256),
                              key: str = Query(..., min_length=1, max_length=128),
                              p=Depends(principal)):
        """Prove this principal's committed canonical command, not external/model effects.
        Return only currently authorized resource state, never a stale cached snapshot.
        """
        row = store.db.execute(
            "SELECT hash,response FROM commands WHERE principal=? AND operation=? AND key=?",
            (p["id"], operation, key)).fetchone()
        if row is None:
            raise Problem("command_receipt_not_found", 404)
        cached = json.loads(row["response"])
        current = None
        identity = cached.get("id") if isinstance(cached, dict) else None
        if identity:
            resource = store.raw(identity)
            service.project_access(p, resource["project"])
            current = store.get(identity, p, review_memory=operation.startswith("memory-"))
            projection = getattr(service, "resource_projections", {}).get(resource["kind"])
            if projection is not None:
                current = projection(p, identity)
        return envelope({"state": "committed", "operation": operation, "key": key,
                         "payload_sha256": row["hash"],
                         "response_sha256": hashlib.sha256(
                             json.dumps(cached, sort_keys=True, separators=(",", ":"),
                                        ensure_ascii=False, allow_nan=False).encode()).hexdigest(),
                         "resource_id": identity, "resource": current,
                         "scope": "canonical_command_only",
                         "external_effects_proved": False})

    @app.post("/v1/session")
    async def login(request: Request, p=Depends(principal)):
        if not request.headers.get("authorization", "").startswith("Bearer "):
            raise Problem("bearer_required", 401)
        if p["role"] not in ("admin", "human", "observer"):
            raise Problem("human_session_required", 403)
        session, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        with store.transaction():
            store.db.execute("INSERT INTO sessions VALUES(?,?,?,?)",
                             (hash_token(session), p["id"], hash_token(csrf), time.time() + 28800))
        response = JSONResponse(envelope({"csrf_token": csrf, "role": p["role"]}))
        response.set_cookie("hivemind_session", session, httponly=True, samesite="strict",
                            secure=request.url.scheme == "https", max_age=28800)
        return response

    @app.delete("/v1/session")
    async def logout(request: Request, p=Depends(principal)):
        with store.transaction():
            store.db.execute("DELETE FROM sessions WHERE hash=?",
                             (hash_token(request.cookies.get("hivemind_session", "")),))
        response = JSONResponse(envelope({"logged_out": True}))
        response.delete_cookie("hivemind_session")
        return response

    @app.get("/v1/session")
    async def restore_session(request: Request, p=Depends(principal)):
        if p["role"] not in ("admin", "human", "observer") or not request.cookies.get("hivemind_session"):
            raise Problem("human_session_required", 401)
        csrf = secrets.token_urlsafe(32)
        with store.transaction():
            store.db.execute("UPDATE sessions SET csrf_hash=? WHERE hash=?",
                             (hash_token(csrf), hash_token(request.cookies["hivemind_session"])))
        return envelope({"csrf_token": csrf, "role": p["role"]})

    @app.get("/v1/me")
    async def me(p=Depends(principal)):
        return envelope({"principal_id": p["id"], "role": p["role"], "project_id": p["project"]})

    @app.get("/v1/projects")
    async def projects(p=Depends(principal)):
        rows = store.db.execute("SELECT id,title FROM projects").fetchall()
        return envelope([dict(r) for r in rows if p["role"] == "admin" or r["id"] == p["project"]])

    @app.post("/v1/agents")
    async def agents_create(body: m.AgentCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "agents", idempotency_key, body, lambda: service.agent(p, body))

    @app.get("/v1/agents")
    async def agents(project_id: str, p=Depends(principal)):
        """List agents with authorized current task, status, summary and per-run activity context."""
        from .agent_activity import activity_roster
        return envelope(activity_roster(service, p, project_id))

    @app.get("/v1/agents/{agent_id}")
    async def agent(agent_id: str, p=Depends(principal)):
        return envelope(store.get(agent_id, p, "agent"))

    @app.patch("/v1/agents/{agent_id}")
    async def agent_update(agent_id: str, body: m.AgentUpdate, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        def update():
            service.represent(p, agent_id)
            agent = store.get(agent_id, p, "agent")
            agent.update(display_name=body.display_name, description=body.description)
            row = store.raw(agent_id)
            return store.save("agent", row["project"], row["owner"], agent, expected=body.expected_revision)
        return command(p, "agent-edit:" + agent_id, idempotency_key, body, update)

    @app.post("/v1/runs")
    async def runs_create(body: m.RunCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "runs", idempotency_key, body, lambda: service.start_run(p, body))

    @app.get("/v1/runs/{run_id}")
    async def run_get(run_id: str, p=Depends(principal)):
        return envelope(store.get(run_id, p, "run"))

    @app.post("/v1/runs/{run_id}/heartbeat")
    async def heartbeat(run_id: str, body: m.Heartbeat, p=Depends(principal)):
        with store.transaction():
            run = service.run(p, run_id)
            if run["status"] in ("completed", "failed", "cancelled"):
                raise Problem("terminal_run")
            run.update(last_seen_at=time.time(), capture_health=body.capture_health)
            # Volatile presence does not invalidate control revisions or create indexing jobs.
            store.db.execute("UPDATE resources SET body=? WHERE id=?", (canonical(run), run_id))
        return envelope({"last_seen_at": run["last_seen_at"]})

    @app.post("/v1/events:batch")
    async def events_batch(body: m.Batch, request: Request, p=Depends(principal)):
        results = []
        for raw_event in body.events:
            try:
                event = m.Event.model_validate(raw_event)
                results.append({"accepted": True, **service.accept_event(p, event)})
            except ValidationError:
                identity = raw_event.get("event_id")
                results.append({"accepted": False, "event_id": identity if isinstance(identity, str) and
                                0 < len(identity) <= 128 else None, "error": "invalid_request", "retryable": False})
            except Problem as exc:
                if exc.status in (403, 404):
                    monitor_denial(request)
                results.append({"accepted": False, "event_id": event.event_id, "error": exc.code,
                                "retryable": exc.status in (429, 503)})
        return envelope(results)

    @app.get("/v1/events")
    async def events(project_id: str, after: int = 0, p=Depends(principal)):
        service.project_access(p, project_id)
        rows = store.db.execute("SELECT body,result FROM events WHERE project=? AND seq>? ORDER BY seq LIMIT 100",
                                 (project_id, max(0, after))).fetchall()
        visible_events = []
        for row in rows:
            event = json.loads(row["body"])
            try:
                store.get(event["run_id"], p, "run")
                service.evidence(p, project_id, event.get("data", {}).get("evidence_refs", []))
            except Problem:
                continue
            visible_events.append({**event, **json.loads(row["result"])})
        return envelope(visible_events)

    @app.get("/v1/ledger")
    async def ledger(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        result = []
        for run in store.list("run", project_id, p):
            gap = time.time() - run["last_seen_at"]
            result.append({**run, "presence": "offline" if gap >= 30 else "stale" if gap >= 15 else "online"})
        return envelope({"runs": result, "freshness": store.freshness(project_id)})

    @app.post("/v1/scopes")
    async def scopes_create(body: m.ScopeCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        def create():
            if body.parent_scope_id:
                service.reference(p, body.project_id, body.parent_scope_id, "scope")
            existing = [s for s in store.list("scope", body.project_id, p, 1000)
                        if s["canonical_key"] == body.canonical_key]
            if existing:
                raise Problem("scope_key_exists")
            result = service.create(p, "scope", body.model_dump())
            if body.parent_scope_id:
                store.relate(body.project_id, result["id"], body.parent_scope_id, "PART_OF")
            return result
        return command(p, "scopes", idempotency_key, body, create)

    @app.post("/v1/tasks")
    async def tasks_create(body: m.TaskCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        def create():
            for scope in body.scope_ids:
                service.reference(p, body.project_id, scope, "scope")
            result = service.create(p, "task", {**body.model_dump(), "status": "queued"})
            for scope in body.scope_ids:
                store.relate(body.project_id, result["id"], scope, "WORKS_ON")
            return result
        return command(p, "tasks", idempotency_key, body, create)

    @app.get("/v1/tasks")
    async def tasks(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("task", project_id, p))
    @app.get("/v1/tasks/{task_id}")
    async def task_get(task_id: str, p=Depends(principal)):
        return envelope(store.get(task_id, p, "task"))
    @app.post("/v1/tasks/{task_id}:start")
    async def task_start(task_id: str, body: m.TaskStart, p=Depends(principal),
                         idempotency_key: str | None = Header(None)):
        return command(p, "task-start:" + task_id, idempotency_key, body,
                       lambda: service.task_start(p, task_id, body))
    @app.post("/v1/tasks/{task_id}:checkpoint")
    async def task_checkpoint(task_id: str, body: m.TaskProgress, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, "task-checkpoint:" + task_id, idempotency_key, body,
                       lambda: service.task_progress(p, task_id, body))

    @app.post("/v1/work-claims")
    async def claims_create(body: m.ClaimCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        result = command(p, "claims", idempotency_key, body, lambda: service.claim(p, body))
        return JSONResponse(result, status_code=409 if result["data"].get("denied") else 200)

    @app.get("/v1/work-claims")
    async def claims(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(service.active_claims(p, project_id))

    @app.post("/v1/work-claims/{claim_id}:renew")
    async def claims_renew(claim_id: str, body: m.Revision, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        def renew():
            claim = store.get(claim_id, p, "work_claim")
            service.run(p, claim["run_id"])
            if claim["state"] != "active" or claim["expires_at"] <= time.time():
                raise Problem("claim_expired")
            claim["expires_at"] = time.time() + 120
            row = store.raw(claim_id)
            return store.save("work_claim", row["project"], row["owner"], claim, expected=body.expected_revision)
        return command(p, "renew:" + claim_id, idempotency_key, body, renew)

    @app.delete("/v1/work-claims/{claim_id}")
    async def claims_release(claim_id: str, body: m.Revision, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        def release():
            claim = store.get(claim_id, p, "work_claim")
            service.run(p, claim["run_id"])
            claim["state"] = "released"
            row = store.raw(claim_id)
            return store.save("work_claim", row["project"], row["owner"], claim, expected=body.expected_revision)
        return command(p, "release:" + claim_id, idempotency_key, body, release)

    @app.get("/v1/conflicts")
    async def conflicts(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("conflict", project_id, p))

    @app.post("/v1/messages")
    async def send_message(body: m.MessageCreate, p=Depends(principal)):
        return command(p, "messages", body.message_id, body, lambda: service.message(p, body))

    @app.get("/v1/agents/{agent_id}/inbox")
    async def inbox(agent_id: str, project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(service.inbox(p, agent_id, project_id))

    @app.post("/v1/messages/{message_id}:claim")
    async def claim_message(message_id: str, body: m.DeliveryClaim, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "claim:" + message_id, idempotency_key, body,
                       lambda: service.claim_message(p, message_id, body.run_id))

    @app.post("/v1/messages/{message_id}:ack")
    async def ack_message(message_id: str, body: m.DeliveryReceipt, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        return command(p, "ack:" + message_id, idempotency_key, body,
                       lambda: service.ack_message(p, message_id, body.run_id, body.delivery_id))

    @app.post("/v1/messages/{message_id}:fail")
    async def fail_message(message_id: str, body: m.DeliveryFailure, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        return command(p, "delivery-fail:" + message_id, idempotency_key, body,
                       lambda: service.fail_message(p, message_id, body))

    @app.post("/v1/messages/{message_id}:requeue")
    async def requeue_message(message_id: str, body: m.MessageRequeue, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, "delivery-requeue:" + message_id, idempotency_key, body,
                       lambda: service.requeue_message(p, message_id, body))

    @app.get("/v1/messages/{message_id}")
    async def message_detail(message_id: str, p=Depends(principal)):
        message = store.get(message_id, p, "message")
        return envelope(service.delivery_state(message))

    @app.get("/v1/projects/{project_id}/delivery-summary")
    async def delivery_summary(project_id: str, p=Depends(principal)):
        return envelope(service.delivery_summary(p, project_id))

    @app.post("/v1/projects/{project_id}/conversations:read")
    async def conversations_read(project_id: str, body: m.ConversationsRead, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return command(p, "conversations-read:" + project_id, idempotency_key, body,
                       lambda: service.read_messages(p, project_id, body))

    @app.get("/v1/boards")
    async def boards(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("board", project_id, p))

    @app.post("/v1/boards/{board_id}/posts")
    async def posts_create(board_id: str, body: m.PostCreate, p=Depends(principal)):
        return command(p, "post:" + board_id, body.client_post_id, body,
                       lambda: service.board_post(p, board_id, body))

    @app.get("/v1/boards/{board_id}/posts")
    async def posts(request: Request, board_id: str, before: int | None = Query(None, ge=1),
                    limit: int = Query(100, ge=1, le=100), query: str = Query("", max_length=256),
                    agent_id: str | None = None,
                    snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                    p=Depends(principal)):
        with store.lock:
            p = authenticate(request)
            board = store.get(board_id, p, "board")
            if agent_id:
                service.reference(p, board["project_id"], agent_id, "agent")
            cursor = checked_snapshot(p, board["project_id"], snapshot_cursor)
            from .board_reads import decorate_posts
            page = service.timeline_page(p, board["project_id"], "board_post", before, limit, query,
                                         board_id=board_id, agent_id=agent_id)
            return envelope({**page, "items": decorate_posts(store, p, page["items"]), "snapshot_cursor": cursor})

    @app.patch("/v1/posts/{post_id}")
    async def post_update(post_id: str, body: m.PostUpdate, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        def update():
            row = store.raw(post_id, "board_post")
            post = store.get(post_id, p)
            if row["owner"] != p["id"] and p["role"] != "admin":
                raise Problem("forbidden", 403)
            # Mention targets are immutable in this milestone; revisions may edit text, not routing.
            if body.content.segments is not None:
                raise Problem("rich_post_revision_not_supported", 422)
            post["content"] = body.content.model_dump(exclude_none=True)
            return store.save("board_post", row["project"], row["owner"], post, expected=body.expected_revision)
        return command(p, "edit:" + post_id, idempotency_key, body, update)

    async def delete_resource(resource_id, body, p):
        with store.transaction():
            store.delete(resource_id, p, body.expected_revision)
        return envelope({"deleted": True, "derived_cleanup": "pending"})

    @app.delete("/v1/posts/{post_id}")
    async def post_delete(post_id: str, body: m.Revision, p=Depends(principal)):
        store.raw(post_id, "board_post")
        return await delete_resource(post_id, body, p)

    @app.get("/v1/boards/{board_id}/work-cards")
    async def work_cards(board_id: str, p=Depends(principal)):
        board = store.get(board_id, p, "board")
        return envelope(service.active_claims(p, board["project_id"]))

    @app.get("/v1/projects/{project_id}/mention-options")
    async def mention_options(project_id: str, query: str = "", board_id: str | None = None,
                              p=Depends(principal)):
        service.project_access(p, project_id)
        board_row = None
        if board_id:
            board_row = store.raw(board_id, "board")
            service.reference(p, project_id, board_id, "board")
        agents = store.list("agent", project_id, p)
        return envelope([a for a in agents if query.lower() in a["display_name"].lower()
                         and (board_row is None or store.visible(board_row, {
                             "role": "agent", "project": project_id, "agent": a["id"]}))][:8])

    def appearances(p, project):
        lease = service.lease(p, project)
        return [{**a, "effective_role": "nexus" if lease and lease["agent_id"] == a["id"] else "worker",
                 "coordinator_epoch": lease["coordinator_epoch"] if lease else None}
                for a in store.list("agent", project, p)]

    def roster_appearances(p, project_id):
        from .agent_roster import roster_work
        work = roster_work(service, p, project_id)
        return [{**agent, **work.get(agent["id"], {
            "current_tasks": [], "owned_scopes": [], "blockers": [],
        })} for agent in appearances(p, project_id)]
    @app.get("/v1/projects/{project_id}/agent-appearances")
    async def appearance(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(roster_appearances(p, project_id))
    @app.get("/v1/projects/{project_id}/dashboard-snapshot")
    async def dashboard_snapshot(request: Request, project_id: str, p=Depends(principal)):
        with store.lock:
            p = authenticate(request)
            service.project_access(p, project_id)
            stamp = time.time()
            runs = [{**run, "presence": "offline" if stamp - run["last_seen_at"] >= 30 else
                     "stale" if stamp - run["last_seen_at"] >= 15 else "online"}
                    for run in store.list("run", project_id, p)]
            from .board_reads import project_unread_summary
            return envelope({
                "agents": roster_appearances(p, project_id), "runs": runs,
                "boards": store.list("board", project_id, p),
                "delivery": service.delivery_summary(p, project_id) if p["role"] in ("admin", "observer") else None,
                "snapshot_cursor": stream_cursor(p, project_id),
                "board_unread": project_unread_summary(store, p, project_id),
            })

    def conversation(p, project, agent=None, before=None, limit=100, query=""):
        if p["role"] not in ("admin", "observer") and (not agent or p["agent"] != agent):
            raise Problem("conversation_observer_grant_required", 403)
        return service.timeline_page(p, project, "message", before, limit, query, agent_id=agent)

    @app.get("/v1/projects/{project_id}/conversations")
    async def conversations(request: Request, project_id: str, before: int | None = Query(None, ge=1),
                            limit: int = Query(100, ge=1, le=100), query: str = Query("", max_length=256),
                            snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                            p=Depends(principal)):
        with store.lock:
            p = authenticate(request)
            service.project_access(p, project_id)
            page = conversation(p, project_id, before=before, limit=limit, query=query)
            cursor = checked_snapshot(p, project_id, snapshot_cursor)
            return envelope({**page, "snapshot_cursor": cursor})

    @app.get("/v1/agents/{agent_id}/conversation")
    async def agent_conversation(request: Request, agent_id: str, before: int | None = Query(None, ge=1),
                                 limit: int = Query(100, ge=1, le=100), query: str = Query("", max_length=256),
                                 snapshot_cursor: str | None = Query(None, pattern="^[0-9a-f]{64}$"),
                                 p=Depends(principal)):
        with store.lock:
            p = authenticate(request)
            agent = store.get(agent_id, p, "agent")
            page = conversation(p, agent["project_id"], agent_id, before, limit, query)
            cursor = checked_snapshot(p, agent["project_id"], snapshot_cursor)
            return envelope({**page, "snapshot_cursor": cursor})

    @app.get("/v1/agents/{agent_id}/notifications")
    async def notifications(agent_id: str, project_id: str, p=Depends(principal)):
        return envelope(service.notifications(p, agent_id, project_id))

    @app.post("/v1/notifications/{notification_id}:ack")
    async def notification_ack(notification_id: str, p=Depends(principal),
                               idempotency_key: str | None = Header(None)):
        def ack():
            item = service.notification_get(p, notification_id)
            service.represent(p, item["recipient_agent_id"])
            item.pop("current_memory_status", None)
            item["state"] = "acked"
            row = store.raw(notification_id)
            return store.save("notification", row["project"], row["owner"], item, private=row["private_agent"])
        return command(p, "ack:" + notification_id, idempotency_key, {}, ack)

    @app.post("/v1/memories:extract")
    async def memory_extract(body: m.MemoryExtract, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "memory-extract", idempotency_key, body, lambda: service.extract_memories(p, body))

    @app.post("/v1/memories")
    async def memory_create(body: m.MemoryCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "memory", idempotency_key, body, lambda: service.memory(p, body))

    @app.get("/v1/memories")
    async def memories(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("memory", project_id, p))

    @app.get("/v1/memories/review-queue")
    async def memory_queue(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id, human=True)
        memories = [{**body, "effective_status": store.memory_state(body)}
                    for body in store.list("memory", project_id, p, 1000, review_memory=True)]
        proposals, disputes = [], []
        for kind, target, getter in (("memory_proposal", proposals, service.memory_proposal_get),
                                     ("memory_dispute", disputes, service.memory_dispute_get)):
            for body in store.list(kind, project_id, p, 1000):
                try:
                    target.append(getter(p, body["id"]))
                except Problem:
                    continue
        return envelope({"memories": memories, "proposals": proposals, "disputes": disputes})

    @app.get("/v1/memories/{memory_id}")
    async def memory_get(memory_id: str, p=Depends(principal)):
        return envelope(service.memory_get(p, memory_id))

    @app.get("/v1/memories/{memory_id}/versions")
    async def memory_versions(memory_id: str, p=Depends(principal)):
        return envelope(service.memory_versions(p, memory_id))

    @app.post("/v1/memories/{memory_id}/versions")
    async def memory_version_create(memory_id: str, body: m.MemoryVersionProposal, p=Depends(principal),
                                    idempotency_key: str | None = Header(None)):
        return command(p, "memory-version:" + memory_id, idempotency_key, body,
                       lambda: service.memory_propose_version(p, memory_id, body))

    @app.post("/v1/memory-proposals/{proposal_id}:review")
    async def memory_proposal_review(proposal_id: str, body: m.MemoryProposalReview, p=Depends(principal),
                                     idempotency_key: str | None = Header(None)):
        return command(p, "memory-proposal-review:" + proposal_id, idempotency_key, body,
                       lambda: service.memory_review_proposal(p, proposal_id, body))

    @app.get("/v1/memory-proposals/{proposal_id}")
    async def memory_proposal_get(proposal_id: str, p=Depends(principal)):
        return envelope(service.memory_proposal_get(p, proposal_id))

    @app.post("/v1/memories/{memory_id}:review")
    async def memory_review(memory_id: str, body: m.MemoryReview, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "memory-review:" + memory_id, idempotency_key, body,
                       lambda: service.memory_review(p, memory_id, body.expected_revision, body.decision, body.reason))

    @app.post("/v1/memory-disputes")
    async def memory_dispute_create(body: m.MemoryDisputeCreate, p=Depends(principal),
                                   idempotency_key: str | None = Header(None)):
        return command(p, "memory-dispute", idempotency_key, body, lambda: service.memory_dispute(p, body))

    @app.post("/v1/memory-disputes/{dispute_id}:review")
    async def memory_dispute_review(dispute_id: str, body: m.MemoryDisputeReview, p=Depends(principal),
                                   idempotency_key: str | None = Header(None)):
        return command(p, "memory-dispute-review:" + dispute_id, idempotency_key, body,
                       lambda: service.memory_review_dispute(p, dispute_id, body))

    @app.get("/v1/memory-disputes/{dispute_id}")
    async def memory_dispute_get(dispute_id: str, p=Depends(principal)):
        return envelope(service.memory_dispute_get(p, dispute_id))

    @app.post("/v1/memories/{memory_id}:verify")
    async def verify(memory_id: str, body: m.Revision, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        return command(p, "memory-verify:" + memory_id, idempotency_key, body,
                       lambda: service.memory_review(p, memory_id, body.expected_revision, "verify",
                                                     "Explicit human verification"))

    @app.delete("/v1/memories/{memory_id}")
    async def memory_delete(memory_id: str, body: m.Revision, p=Depends(principal)):
        store.raw(memory_id, "memory")
        return await delete_resource(memory_id, body, p)

    @app.post("/v1/context:query")
    async def context(body: m.ContextQuery, p=Depends(principal)):
        return envelope(service.context(p, body))

    @app.get("/v1/agents/{agent_id}/context")
    async def agent_context(agent_id: str, p=Depends(principal)):
        agent = store.get(agent_id, p, "agent")
        return envelope(service.context(p, m.ContextQuery(project_id=agent["project_id"],
                                                         agent_selector=m.Selector(agent_ids=[agent_id],
                                                                                   active_only=False))))

    @app.post("/v1/search")
    async def search(body: m.SearchQuery, p=Depends(principal)):
        service.project_access(p, body.project_id)
        # Indexes.search is a two-phase detached method: short canonical snapshots,
        # lock-free read-only ranking connection, per-statement deadline.
        return envelope(await asyncio.to_thread(indexes.search, p, body))
    search.hivemind_detached_work = True

    @app.post("/v1/graph:query")
    async def graph(body: m.GraphQuery, p=Depends(principal)):
        service.project_access(p, body.project_id)
        return envelope(indexes.query_graph(p, body))

    @app.post("/v1/projects/{project_id}/nexus:acquire")
    async def nexus_acquire(project_id: str, body: m.NexusRequest, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "nexus:" + project_id, idempotency_key, body,
                       lambda: service.acquire_nexus(p, project_id, body))

    @app.post("/v1/projects/{project_id}/nexus:renew")
    async def nexus_renew(project_id: str, body: m.NexusRequest, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        def renew():
            lease = service.lease(p, project_id)
            if not lease or (lease["agent_id"], lease["run_id"]) != (body.agent_id, body.run_id):
                raise Problem("stale_coordinator_epoch")
            service.coordinator(p, project_id, lease["coordinator_epoch"], "lease")
            lease["expires_at"] = time.time() + body.requested_ttl_seconds
            row = store.raw(lease["id"])
            return store.save("nexus_lease", project_id, row["owner"], lease)
        return command(p, "nexus-renew:" + project_id, idempotency_key, body, renew)

    @app.post("/v1/projects/{project_id}/nexus:release")
    async def nexus_release(project_id: str, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        def release():
            lease = service.lease(p, project_id)
            if not lease:
                raise Problem("no_active_coordinator")
            service.coordinator(p, project_id, lease["coordinator_epoch"], "lease_release")
            lease["state"] = "released"
            row = store.raw(lease["id"])
            result = store.save("nexus_lease", project_id, row["owner"], lease)
            service.retire_controls(p, project_id)
            return result
        return command(p, "nexus-release:" + project_id, idempotency_key, {}, release)

    @app.post("/v1/controls")
    async def control_create(body: m.ControlCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "controls", idempotency_key, body, lambda: service.control(p, body))

    @app.get("/v1/controls/{control_id}")
    async def control_get(control_id: str, p=Depends(principal)):
        return envelope(store.get(control_id, p, "control"))

    @app.get("/v1/agents/{agent_id}/controls")
    async def controls(agent_id: str, project_id: str, p=Depends(principal)):
        service.represent(p, agent_id)
        result = []
        for c in store.list("control", project_id, p):
            if c["target_agent_id"] != agent_id or c["state"] not in ("queued", "acknowledged", "applying"):
                continue
            try:
                service.live_control(p, c["id"])
                result.append(c)
            except Problem:
                pass
        return envelope(result)

    @app.post("/v1/controls/{control_id}:ack")
    async def control_ack(control_id: str, p=Depends(principal), idempotency_key: str | None = Header(None)):
        def ack():
            item = service.live_control(p, control_id)
            item["state"] = "acknowledged"
            row = store.raw(control_id)
            return store.save("control", row["project"], row["owner"], item)
        return command(p, "control-ack:" + control_id, idempotency_key, {}, ack)

    @app.post("/v1/controls/{control_id}:result")
    async def control_result(control_id: str, body: m.ControlResult, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        return command(p, "control-result:" + control_id, idempotency_key, body,
                       lambda: service.control_result(p, control_id, body))

    @app.post("/v1/controls/{control_id}:checkpoint")
    async def control_checkpoint(control_id: str, body: m.Checkpoint, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return command(p, "control-checkpoint:" + control_id, idempotency_key, body,
                       lambda: service.checkpoint(p, control_id, body))

    @app.post("/v1/runs/{run_id}/permits:check")
    async def permits(run_id: str, body: m.PermitRequest, p=Depends(principal)):
        with store.transaction():
            return envelope(service.permits(p, run_id, body))

    @app.post("/v1/artifacts")
    async def artifact_create(body: m.ArtifactCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        def create():
            if body.run_id:
                run = service.run(p, body.run_id)
                if run["project_id"] != body.project_id:
                    raise Problem("not_found", 404)
            result = service.create(p, "artifact", body.model_dump())
            if body.run_id:
                store.relate(body.project_id, body.run_id, result["id"], "PRODUCED")
            return result
        return command(p, "artifacts", idempotency_key, body, create)

    @app.get("/v1/artifacts/{artifact_id}")
    async def artifact_get(artifact_id: str, p=Depends(principal)):
        return envelope(store.get(artifact_id, p, "artifact"))

    def stream_cursor(p, project):
        rows = store.db.execute("SELECT * FROM resources WHERE project=? AND deleted=0 ORDER BY id",
                                 (project,)).fetchall()
        revisions = []
        for row in rows:
            if not store.visible(row, p):
                continue
            body = json.loads(row["body"])
            if row["kind"] == "memory" and not store.valid_memory(body, p):
                continue
            revisions.append((row["id"], row["revision"], body.get("last_seen_at")))
        lease = service.lease(p, project)
        visible_ids = {entry[0] for entry in revisions}
        reads = sorted(row[0] for row in store.db.execute(
            "SELECT message FROM conversation_reads WHERE principal=? AND project=?", (p["id"], project))
            if row[0] in visible_ids)
        # Principal/project-bound snapshot hash. Hidden writes do not change an unauthorized reader's cursor.
        from .board_reads import markers_for_cursor
        board_reads = markers_for_cursor(store, p, project, visible_ids)
        payload = canonical([p["id"], project, revisions, lease["id"] if lease else None, reads, board_reads])
        return hashlib.sha256(payload.encode()).hexdigest()

    def checked_snapshot(p, project, expected):
        cursor = stream_cursor(p, project)
        if expected is not None and not secrets.compare_digest(expected, cursor):
            raise Problem("snapshot_changed", 409)
        return cursor

    app.state.checked_snapshot = checked_snapshot

    def stream_snapshot(request, project_id, expected):
        with store.lock:
            fresh = authenticate(request)
            service.project_access(fresh, project_id)
            if any(fresh[key] != expected[key] for key in
                   ("id", "role", "project", "agent", "coordinator", "capture_authority")):
                raise Problem("permissions_changed", 403)
            lease = service.lease(fresh, project_id)
            return stream_cursor(fresh, project_id), (lease["id"] if lease else "")

    @app.get("/v1/streams")
    async def streams(request: Request, project_id: str,
                      cursor: str | None = Query(None, max_length=128),
                      p=Depends(principal)):
        service.project_access(p, project_id)
        # Browser EventSource binds the displayed snapshot through the query cursor.
        # Frames only invalidate snapshots or announce revoked access, never carry content.
        return StreamingResponse(
            live_state.feed(request, lambda: stream_snapshot(request, project_id, p), cursor=cursor),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    if not enable_ui:
        from .tool_context import apply_openapi_descriptions
        apply_openapi_descriptions(app)
        return app

    assets = Path(__file__).resolve().parent.parent / "dashboard" / "dist"
    if (assets / "assets").exists():
        app.mount("/assets", StaticFiles(directory=assets / "assets"), name="assets")

    async def ui():
        if not (assets / "index.html").exists():
            return JSONResponse({"message": "Build the dashboard: cd dashboard; npm install; npm run build"},
                                status_code=503)
        return FileResponse(assets / "index.html")

    app.add_api_route("/", ui, methods=["GET"], include_in_schema=False)
    app.add_api_route("/projects/{project_id}/board", ui, methods=["GET"], include_in_schema=False)
    app.add_api_route("/projects/{project_id}/conversations", ui, methods=["GET"], include_in_schema=False)
    app.add_api_route("/projects/{project_id}/memories", ui, methods=["GET"], include_in_schema=False)
    app.add_api_route("/projects/{project_id}/administration", ui, methods=["GET"], include_in_schema=False)
    for view in ("tasks", "coordination", "context", "operations", "privacy"):
        app.add_api_route(f"/projects/{{project_id}}/{view}", ui, methods=["GET"], include_in_schema=False)
    app.add_api_route("/projects/{project_id}/agents/{agent_id}", ui, methods=["GET"], include_in_schema=False)
    from .tool_context import apply_openapi_descriptions
    apply_openapi_descriptions(app)
    return app
