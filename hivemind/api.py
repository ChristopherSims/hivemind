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

from . import models as m
from .indexes import Indexes
from .service import Service
from .storage import Problem, Store, canonical, uid


def hash_token(token):
    return hashlib.sha256(token.encode()).hexdigest()


def envelope(data):
    return {"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"), "data": data}


def create_app(directory=".hivemind-data", encoder=None, workers=True, capability_options=None):
    store = Store(directory)
    service = Service(store)
    indexes = Indexes(store, encoder)
    service.indexes = indexes
    request_lock = asyncio.Lock()
    snapshot_context = ContextVar("memory_repository_snapshot", default=None)

    async def worker():
        while True:
            await asyncio.to_thread(indexes.drain, 100)
            for name in ("review_automation", "nexus_git_automation"):
                automation = getattr(app.state, name, None)
                if automation:
                    try:
                        await asyncio.to_thread(automation.scheduled_tick)
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
        # A thread dispatched before cancellation may still own the graph.
        with store.lock, indexes.lock:
            indexes.close()
            store.close()

    app = FastAPI(title="Hivemind", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.service = service
    app.state.indexes = indexes

    @app.exception_handler(Problem)
    async def problem_handler(request, exc):
        return JSONResponse({"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"),
                             "error": {"code": exc.code, "message": exc.code.replace("_", " "),
                                       "retryable": exc.status in (429, 503)}}, status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request, exc):
        # Do not echo Pydantic input fields, which can contain secrets.
        return JSONResponse({"protocol": "hivemind", "protocol_version": "0.1.0", "request_id": uid("req"),
                             "error": {"code": "invalid_request", "message": "Request schema validation failed",
                                       "retryable": False}}, status_code=422)

    def authenticate(request):
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
        # Single canonical connection, serialized requests; no shared-thread uncommitted reads.
        async with request_lock:
            with store.lock:
                snapshot_token = snapshot_context.set({})
                try:
                    response = await call_next(request)
                finally:
                    snapshot_context.reset(snapshot_token)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        if request.url.path.startswith("/v0.1"):
            response.headers["Cache-Control"] = "no-store"
        return response

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) if isinstance(request, m.Model) else request
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
        return {"protocol": "hivemind", "supported_versions": ["0.1.0"], "api_base": "/v0.1",
                "capabilities": ["events.batch", "messages.durable", "board.human_participation",
                                 "memory.versioned", "context.agent_selectors", "coordination.deterministic",
                                 "memory.lifecycle",
                                 "messages.retry", "conversations.unread",
                                 "dashboard.timeline_search", "dashboard.keyset_pagination",
                                 "memory.extraction", "memory.review_notifications",
                                 "tasks.durable_execution",
                                 "tasks.scheduling", "tasks.verification", "agents.capabilities",
                                 "conversations.workflows", "messages.lease_renewal",
                                 "context.compact_assembly", "memory.pipeline_opt_in",
                                 "runtime.reconciliation", "runtime.cooperative_controls",
                                 "coordination.nexus", "coordination.controls", "streams.sse",
                                 *(["graph.ladybugdb"] if indexes.graph else ["graph.canonical_fallback"]),
                                 * (["board.hybrid_search"] if encoder else ["board.lexical_search"])],
                "authentication": {"schemes": ["bearer", "same_origin_session"]},
                "limits": {"max_batch_events": 100, "max_batch_bytes": 1048576, "max_event_bytes": 131072}}

    @app.post("/v0.1/session")
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

    @app.delete("/v0.1/session")
    async def logout(request: Request, p=Depends(principal)):
        with store.transaction():
            store.db.execute("DELETE FROM sessions WHERE hash=?",
                             (hash_token(request.cookies.get("hivemind_session", "")),))
        response = JSONResponse(envelope({"logged_out": True}))
        response.delete_cookie("hivemind_session")
        return response

    @app.get("/v0.1/session")
    async def restore_session(request: Request, p=Depends(principal)):
        if p["role"] not in ("admin", "human", "observer") or not request.cookies.get("hivemind_session"):
            raise Problem("human_session_required", 401)
        csrf = secrets.token_urlsafe(32)
        with store.transaction():
            store.db.execute("UPDATE sessions SET csrf_hash=? WHERE hash=?",
                             (hash_token(csrf), hash_token(request.cookies["hivemind_session"])))
        return envelope({"csrf_token": csrf, "role": p["role"]})

    @app.get("/v0.1/me")
    async def me(p=Depends(principal)):
        return envelope({"principal_id": p["id"], "role": p["role"], "project_id": p["project"]})

    @app.get("/v0.1/projects")
    async def projects(p=Depends(principal)):
        rows = store.db.execute("SELECT id,title FROM projects").fetchall()
        return envelope([dict(r) for r in rows if p["role"] == "admin" or r["id"] == p["project"]])

    @app.post("/v0.1/agents")
    async def agents_create(body: m.AgentCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "agents", idempotency_key, body, lambda: service.agent(p, body))

    @app.get("/v0.1/agents")
    async def agents(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("agent", project_id, p))

    @app.get("/v0.1/agents/{agent_id}")
    async def agent(agent_id: str, p=Depends(principal)):
        return envelope(store.get(agent_id, p, "agent"))

    @app.patch("/v0.1/agents/{agent_id}")
    async def agent_update(agent_id: str, body: m.AgentUpdate, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        def update():
            service.represent(p, agent_id)
            agent = store.get(agent_id, p, "agent")
            agent.update(display_name=body.display_name, description=body.description)
            row = store.raw(agent_id)
            return store.save("agent", row["project"], row["owner"], agent, expected=body.expected_revision)
        return command(p, "agent-edit:" + agent_id, idempotency_key, body, update)

    @app.post("/v0.1/runs")
    async def runs_create(body: m.RunCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "runs", idempotency_key, body, lambda: service.start_run(p, body))

    @app.get("/v0.1/runs/{run_id}")
    async def run_get(run_id: str, p=Depends(principal)):
        return envelope(store.get(run_id, p, "run"))

    @app.post("/v0.1/runs/{run_id}/heartbeat")
    async def heartbeat(run_id: str, body: m.Heartbeat, p=Depends(principal)):
        with store.transaction():
            run = service.run(p, run_id)
            if run["status"] in ("completed", "failed", "cancelled"):
                raise Problem("terminal_run")
            run.update(last_seen_at=time.time(), capture_health=body.capture_health)
            # Volatile presence does not invalidate control revisions or create indexing jobs.
            store.db.execute("UPDATE resources SET body=? WHERE id=?", (canonical(run), run_id))
        return envelope({"last_seen_at": run["last_seen_at"]})

    @app.post("/v0.1/events:batch")
    async def events_batch(body: m.Batch, p=Depends(principal)):
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
                results.append({"accepted": False, "event_id": event.event_id, "error": exc.code,
                                "retryable": exc.status in (429, 503)})
        return envelope(results)

    @app.get("/v0.1/events")
    async def events(project_id: str, after: int = 0, p=Depends(principal)):
        service.project_access(p, project_id)
        rows = store.db.execute("SELECT body,result FROM events WHERE project=? AND seq>? ORDER BY seq LIMIT 100",
                                 (project_id, max(0, after))).fetchall()
        return envelope([{**json.loads(r[0]), **json.loads(r[1])} for r in rows])

    @app.get("/v0.1/ledger")
    async def ledger(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        result = []
        for run in store.list("run", project_id, p):
            gap = time.time() - run["last_seen_at"]
            result.append({**run, "presence": "offline" if gap >= 30 else "stale" if gap >= 15 else "online"})
        return envelope({"runs": result, "freshness": store.freshness(project_id)})

    @app.post("/v0.1/scopes")
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

    @app.post("/v0.1/tasks")
    async def tasks_create(body: m.TaskCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        def create():
            for scope in body.scope_ids:
                service.reference(p, body.project_id, scope, "scope")
            result = service.create(p, "task", {**body.model_dump(), "status": "queued"})
            for scope in body.scope_ids:
                store.relate(body.project_id, result["id"], scope, "WORKS_ON")
            return result
        return command(p, "tasks", idempotency_key, body, create)

    @app.get("/v0.1/tasks")
    async def tasks(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("task", project_id, p))
    @app.get("/v0.1/tasks/{task_id}")
    async def task_get(task_id: str, p=Depends(principal)):
        return envelope(store.get(task_id, p, "task"))
    @app.post("/v0.1/tasks/{task_id}:start")
    async def task_start(task_id: str, body: m.TaskStart, p=Depends(principal),
                         idempotency_key: str | None = Header(None)):
        return command(p, "task-start:" + task_id, idempotency_key, body,
                       lambda: service.task_start(p, task_id, body))
    @app.post("/v0.1/tasks/{task_id}:checkpoint")
    async def task_checkpoint(task_id: str, body: m.TaskProgress, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, "task-checkpoint:" + task_id, idempotency_key, body,
                       lambda: service.task_progress(p, task_id, body))

    @app.post("/v0.1/work-claims")
    async def claims_create(body: m.ClaimCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        result = command(p, "claims", idempotency_key, body, lambda: service.claim(p, body))
        return JSONResponse(result, status_code=409 if result["data"].get("denied") else 200)

    @app.get("/v0.1/work-claims")
    async def claims(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(service.active_claims(p, project_id))

    @app.post("/v0.1/work-claims/{claim_id}:renew")
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

    @app.delete("/v0.1/work-claims/{claim_id}")
    async def claims_release(claim_id: str, body: m.Revision, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        def release():
            claim = store.get(claim_id, p, "work_claim")
            service.run(p, claim["run_id"])
            claim["state"] = "released"
            row = store.raw(claim_id)
            return store.save("work_claim", row["project"], row["owner"], claim, expected=body.expected_revision)
        return command(p, "release:" + claim_id, idempotency_key, body, release)

    @app.get("/v0.1/conflicts")
    async def conflicts(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("conflict", project_id, p))

    @app.post("/v0.1/messages")
    async def send_message(body: m.MessageCreate, p=Depends(principal)):
        return command(p, "messages", body.message_id, body, lambda: service.message(p, body))

    @app.get("/v0.1/agents/{agent_id}/inbox")
    async def inbox(agent_id: str, project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(service.inbox(p, agent_id, project_id))

    @app.post("/v0.1/messages/{message_id}:claim")
    async def claim_message(message_id: str, body: m.DeliveryClaim, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "claim:" + message_id, idempotency_key, body,
                       lambda: service.claim_message(p, message_id, body.run_id))

    @app.post("/v0.1/messages/{message_id}:ack")
    async def ack_message(message_id: str, body: m.DeliveryReceipt, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        return command(p, "ack:" + message_id, idempotency_key, body,
                       lambda: service.ack_message(p, message_id, body.run_id, body.delivery_id))

    @app.post("/v0.1/messages/{message_id}:fail")
    async def fail_message(message_id: str, body: m.DeliveryFailure, p=Depends(principal),
                           idempotency_key: str | None = Header(None)):
        return command(p, "delivery-fail:" + message_id, idempotency_key, body,
                       lambda: service.fail_message(p, message_id, body))

    @app.post("/v0.1/messages/{message_id}:requeue")
    async def requeue_message(message_id: str, body: m.MessageRequeue, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, "delivery-requeue:" + message_id, idempotency_key, body,
                       lambda: service.requeue_message(p, message_id, body))

    @app.get("/v0.1/messages/{message_id}")
    async def message_detail(message_id: str, p=Depends(principal)):
        message = store.get(message_id, p, "message")
        return envelope(service.delivery_state(message))

    @app.get("/v0.1/projects/{project_id}/delivery-summary")
    async def delivery_summary(project_id: str, p=Depends(principal)):
        return envelope(service.delivery_summary(p, project_id))

    @app.post("/v0.1/projects/{project_id}/conversations:read")
    async def conversations_read(project_id: str, body: m.ConversationsRead, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return command(p, "conversations-read:" + project_id, idempotency_key, body,
                       lambda: service.read_messages(p, project_id, body))

    @app.get("/v0.1/boards")
    async def boards(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("board", project_id, p))

    @app.post("/v0.1/boards/{board_id}/posts")
    async def posts_create(board_id: str, body: m.PostCreate, p=Depends(principal)):
        return command(p, "post:" + board_id, body.client_post_id, body,
                       lambda: service.board_post(p, board_id, body))

    @app.get("/v0.1/boards/{board_id}/posts")
    async def posts(board_id: str, before: int | None = Query(None, ge=1),
                    limit: int = Query(100, ge=1, le=100), query: str = Query("", max_length=256),
                    agent_id: str | None = None, p=Depends(principal)):
        board = store.get(board_id, p, "board")
        if agent_id:
            service.reference(p, board["project_id"], agent_id, "agent")
        return envelope({**service.timeline_page(p, board["project_id"], "board_post", before, limit, query,
                                                 board_id=board_id, agent_id=agent_id),
                         "snapshot_cursor": stream_cursor(p, board["project_id"])})

    @app.patch("/v0.1/posts/{post_id}")
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

    @app.delete("/v0.1/posts/{post_id}")
    async def post_delete(post_id: str, body: m.Revision, p=Depends(principal)):
        store.raw(post_id, "board_post")
        return await delete_resource(post_id, body, p)

    @app.get("/v0.1/boards/{board_id}/work-cards")
    async def work_cards(board_id: str, p=Depends(principal)):
        board = store.get(board_id, p, "board")
        return envelope(service.active_claims(p, board["project_id"]))

    @app.get("/v0.1/projects/{project_id}/mention-options")
    async def mention_options(project_id: str, query: str = "", p=Depends(principal)):
        service.project_access(p, project_id)
        agents = store.list("agent", project_id, p)
        return envelope([a for a in agents if query.lower() in a["display_name"].lower()][:8])

    def appearances(p, project):
        lease = service.lease(p, project)
        return [{**a, "effective_role": "nexus" if lease and lease["agent_id"] == a["id"] else "worker",
                 "coordinator_epoch": lease["coordinator_epoch"] if lease else None}
                for a in store.list("agent", project, p)]

    @app.get("/v0.1/projects/{project_id}/agent-appearances")
    async def appearance(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(appearances(p, project_id))

    def conversation(p, project, agent=None, before=None, limit=100, query=""):
        if p["role"] not in ("admin", "observer") and (not agent or p["agent"] != agent):
            raise Problem("conversation_observer_grant_required", 403)
        return service.timeline_page(p, project, "message", before, limit, query, agent_id=agent)

    @app.get("/v0.1/projects/{project_id}/conversations")
    async def conversations(project_id: str, before: int | None = Query(None, ge=1),
                            limit: int = Query(100, ge=1, le=100), query: str = Query("", max_length=256),
                            p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope({**conversation(p, project_id, before=before, limit=limit, query=query),
                         "snapshot_cursor": stream_cursor(p, project_id)})

    @app.get("/v0.1/agents/{agent_id}/conversation")
    async def agent_conversation(agent_id: str, before: int | None = Query(None, ge=1),
                                 limit: int = Query(100, ge=1, le=100), query: str = Query("", max_length=256),
                                 p=Depends(principal)):
        agent = store.get(agent_id, p, "agent")
        return envelope({**conversation(p, agent["project_id"], agent_id, before, limit, query),
                         "snapshot_cursor": stream_cursor(p, agent["project_id"])})

    @app.get("/v0.1/agents/{agent_id}/notifications")
    async def notifications(agent_id: str, project_id: str, p=Depends(principal)):
        return envelope(service.notifications(p, agent_id, project_id))

    @app.post("/v0.1/notifications/{notification_id}:ack")
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

    @app.post("/v0.1/memories:extract")
    async def memory_extract(body: m.MemoryExtract, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "memory-extract", idempotency_key, body, lambda: service.extract_memories(p, body))

    @app.post("/v0.1/memories")
    async def memory_create(body: m.MemoryCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "memory", idempotency_key, body, lambda: service.memory(p, body))

    @app.get("/v0.1/memories")
    async def memories(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(store.list("memory", project_id, p))

    @app.get("/v0.1/memories/review-queue")
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

    @app.get("/v0.1/memories/{memory_id}")
    async def memory_get(memory_id: str, p=Depends(principal)):
        return envelope(service.memory_get(p, memory_id))

    @app.get("/v0.1/memories/{memory_id}/versions")
    async def memory_versions(memory_id: str, p=Depends(principal)):
        return envelope(service.memory_versions(p, memory_id))

    @app.post("/v0.1/memories/{memory_id}/versions")
    async def memory_version_create(memory_id: str, body: m.MemoryVersionProposal, p=Depends(principal),
                                    idempotency_key: str | None = Header(None)):
        return command(p, "memory-version:" + memory_id, idempotency_key, body,
                       lambda: service.memory_propose_version(p, memory_id, body))

    @app.post("/v0.1/memory-proposals/{proposal_id}:review")
    async def memory_proposal_review(proposal_id: str, body: m.MemoryProposalReview, p=Depends(principal),
                                     idempotency_key: str | None = Header(None)):
        return command(p, "memory-proposal-review:" + proposal_id, idempotency_key, body,
                       lambda: service.memory_review_proposal(p, proposal_id, body))

    @app.get("/v0.1/memory-proposals/{proposal_id}")
    async def memory_proposal_get(proposal_id: str, p=Depends(principal)):
        return envelope(service.memory_proposal_get(p, proposal_id))

    @app.post("/v0.1/memories/{memory_id}:review")
    async def memory_review(memory_id: str, body: m.MemoryReview, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "memory-review:" + memory_id, idempotency_key, body,
                       lambda: service.memory_review(p, memory_id, body.expected_revision, body.decision, body.reason))

    @app.post("/v0.1/memory-disputes")
    async def memory_dispute_create(body: m.MemoryDisputeCreate, p=Depends(principal),
                                   idempotency_key: str | None = Header(None)):
        return command(p, "memory-dispute", idempotency_key, body, lambda: service.memory_dispute(p, body))

    @app.post("/v0.1/memory-disputes/{dispute_id}:review")
    async def memory_dispute_review(dispute_id: str, body: m.MemoryDisputeReview, p=Depends(principal),
                                   idempotency_key: str | None = Header(None)):
        return command(p, "memory-dispute-review:" + dispute_id, idempotency_key, body,
                       lambda: service.memory_review_dispute(p, dispute_id, body))

    @app.get("/v0.1/memory-disputes/{dispute_id}")
    async def memory_dispute_get(dispute_id: str, p=Depends(principal)):
        return envelope(service.memory_dispute_get(p, dispute_id))

    @app.post("/v0.1/memories/{memory_id}:verify")
    async def verify(memory_id: str, body: m.Revision, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        return command(p, "memory-verify:" + memory_id, idempotency_key, body,
                       lambda: service.memory_review(p, memory_id, body.expected_revision, "verify",
                                                     "Explicit human verification"))

    @app.delete("/v0.1/memories/{memory_id}")
    async def memory_delete(memory_id: str, body: m.Revision, p=Depends(principal)):
        store.raw(memory_id, "memory")
        return await delete_resource(memory_id, body, p)

    @app.post("/v0.1/context:query")
    async def context(body: m.ContextQuery, p=Depends(principal)):
        return envelope(service.context(p, body))

    @app.get("/v0.1/agents/{agent_id}/context")
    async def agent_context(agent_id: str, p=Depends(principal)):
        agent = store.get(agent_id, p, "agent")
        return envelope(service.context(p, m.ContextQuery(project_id=agent["project_id"],
                                                         agent_selector=m.Selector(agent_ids=[agent_id],
                                                                                   active_only=False))))

    @app.post("/v0.1/search")
    async def search(body: m.SearchQuery, p=Depends(principal)):
        service.project_access(p, body.project_id)
        return envelope(indexes.search(p, body))

    @app.post("/v0.1/graph:query")
    async def graph(body: m.GraphQuery, p=Depends(principal)):
        service.project_access(p, body.project_id)
        return envelope(indexes.query_graph(p, body))

    @app.post("/v0.1/projects/{project_id}/nexus:acquire")
    async def nexus_acquire(project_id: str, body: m.NexusRequest, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "nexus:" + project_id, idempotency_key, body,
                       lambda: service.acquire_nexus(p, project_id, body))

    @app.post("/v0.1/projects/{project_id}/nexus:renew")
    async def nexus_renew(project_id: str, body: m.NexusRequest, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        def renew():
            lease = service.lease(p, project_id)
            if not lease or (lease["agent_id"], lease["run_id"]) != (body.agent_id, body.run_id):
                raise Problem("stale_coordinator_epoch")
            service.coordinator(p, project_id, lease["coordinator_epoch"])
            lease["expires_at"] = time.time() + body.requested_ttl_seconds
            row = store.raw(lease["id"])
            return store.save("nexus_lease", project_id, row["owner"], lease)
        return command(p, "nexus-renew:" + project_id, idempotency_key, body, renew)

    @app.post("/v0.1/projects/{project_id}/nexus:release")
    async def nexus_release(project_id: str, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        def release():
            lease = service.lease(p, project_id)
            if not lease:
                raise Problem("no_active_coordinator")
            service.coordinator(p, project_id, lease["coordinator_epoch"])
            lease["state"] = "released"
            row = store.raw(lease["id"])
            result = store.save("nexus_lease", project_id, row["owner"], lease)
            service.retire_controls(p, project_id)
            return result
        return command(p, "nexus-release:" + project_id, idempotency_key, {}, release)

    @app.post("/v0.1/controls")
    async def control_create(body: m.ControlCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "controls", idempotency_key, body, lambda: service.control(p, body))

    @app.get("/v0.1/controls/{control_id}")
    async def control_get(control_id: str, p=Depends(principal)):
        return envelope(store.get(control_id, p, "control"))

    @app.get("/v0.1/agents/{agent_id}/controls")
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

    @app.post("/v0.1/controls/{control_id}:ack")
    async def control_ack(control_id: str, p=Depends(principal), idempotency_key: str | None = Header(None)):
        def ack():
            item = service.live_control(p, control_id)
            item["state"] = "acknowledged"
            row = store.raw(control_id)
            return store.save("control", row["project"], row["owner"], item)
        return command(p, "control-ack:" + control_id, idempotency_key, {}, ack)

    @app.post("/v0.1/controls/{control_id}:result")
    async def control_result(control_id: str, body: m.ControlResult, p=Depends(principal),
                             idempotency_key: str | None = Header(None)):
        return command(p, "control-result:" + control_id, idempotency_key, body,
                       lambda: service.control_result(p, control_id, body))

    @app.post("/v0.1/controls/{control_id}:checkpoint")
    async def control_checkpoint(control_id: str, body: m.Checkpoint, p=Depends(principal),
                                 idempotency_key: str | None = Header(None)):
        return command(p, "control-checkpoint:" + control_id, idempotency_key, body,
                       lambda: service.checkpoint(p, control_id, body))

    @app.post("/v0.1/runs/{run_id}/permits:check")
    async def permits(run_id: str, body: m.PermitRequest, p=Depends(principal)):
        with store.transaction():
            return envelope(service.permits(p, run_id, body))

    @app.post("/v0.1/artifacts")
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

    @app.get("/v0.1/artifacts/{artifact_id}")
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
        payload = canonical([p["id"], project, revisions, lease["id"] if lease else None, reads])
        return hashlib.sha256(payload.encode()).hexdigest()

    @app.get("/v0.1/streams")
    async def streams(request: Request, project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        # SSE only invalidates snapshots, not a content-bearing inbox; clients reread authorized data.
        # Stateless opaque cursors bind to a snapshot; history gaps always trigger a safe resnapshot.
        last = request.headers.get("last-event-id")

        async def feed():
            current = last
            while not await request.is_disconnected():
                with store.lock:
                    fresh = authenticate(request)
                    service.project_access(fresh, project_id)
                    cursor = stream_cursor(fresh, project_id)
                    lease = service.lease(fresh, project_id)
                    role = lease["id"] if lease else ""
                if cursor != current:
                    yield f"id: {cursor}\nevent: resync\ndata: {{\"resync_required\":true}}\n\n"
                    current = cursor
                else:
                    yield ": keepalive\n\n"
                # Role expiry changes styling even without a new SQL mutation.
                await asyncio.sleep(1)
                with store.lock:
                    active = service.lease(fresh, project_id)
                    if (active["id"] if active else "") != role:
                        current = None
        return StreamingResponse(feed(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

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
    app.add_api_route("/projects/{project_id}/agents/{agent_id}", ui, methods=["GET"], include_in_schema=False)
    return app
