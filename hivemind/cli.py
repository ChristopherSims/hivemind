import argparse
import json
import os
import secrets
import sys
import webbrowser
from pathlib import Path

from .api import create_app, hash_token
from .indexes import MODEL_ID, MODEL_REVISION, Gemma
from .storage import Problem, Store, uid


def canonical_retention_guard(store, project, repo):
    from .git_collaboration import GitCollaboration
    from .service import Service
    if GitCollaboration(Service(store))._repo(project).resolve() != repo:
        raise ValueError("Retention repository does not match the canonical project")

    def approved(ref, oid, evidence):
        try:
            row = store.raw(ref.removeprefix("refs/hivemind/candidates/"), "merge_request")
            body = json.loads(row["body"])
            return (row["project"] == project and body["status"] == "merged"
                    and body.get("merged_oid") == oid and body.get("candidate_oid") == oid)
        except Problem:
            return False
    return approved


def private_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ValueError("Credential destination already exists; choose a new path")
    with path.open("x", encoding="utf-8") as file:
        file.write(json.dumps(data, indent=2))
    if os.name != "nt":
        path.chmod(0o600)


def initialize(directory, title="Local project"):
    store = Store(directory)
    destination = Path(directory) / "owner.json"
    if store.db.execute("SELECT 1 FROM principals LIMIT 1").fetchone() or destination.exists():
        store.close()
        raise ValueError("Data directory is already initialized")
    token, project, principal = secrets.token_urlsafe(32), uid("project"), uid("principal")
    with store.transaction():
        store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (project, title))
        store.db.execute("INSERT INTO principals(id,token_hash,role,project,capture_authority) VALUES(?,?,?,?,1)",
                         (principal, hash_token(token), "admin", project))
        store.save("board", project, principal, {"id": uid("board"), "project_id": project, "title": "Main board"})
    private_write(destination, {"token": token, "project_id": project, "principal_id": principal,
                                "base_url": "http://127.0.0.1:8765"})
    store.close()
    return destination


def main():
    parser = argparse.ArgumentParser(description="Hivemind local reference service")
    parser.add_argument("--data-dir", default=".hivemind-data")
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--title", default="Local project")
    serve = sub.add_parser("serve")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--open", action="store_true", help="Open the dashboard (requires --enable-ui)")
    serve.add_argument("--enable-ui", action="store_true", help="Enable the parked dashboard UI")
    serve.add_argument("--embeddings", action="store_true")
    serve.add_argument("--agent-integrations", help="Explicit private operator integration JSON")
    issue = sub.add_parser("issue-token")
    issue.add_argument("--role", choices=["agent", "human", "observer"], required=True)
    issue.add_argument("--project", required=True)
    issue.add_argument("--agent")
    issue.add_argument("--coordinator", action="store_true")
    issue.add_argument("--capture-authority", action="store_true")
    issue.add_argument("--out", required=True)
    sub.add_parser("download-model")
    sub.add_parser("backup")
    sub.add_parser("rebuild-indexes")
    projections = sub.add_parser("rebuild-projections", help="Offline event-only SQL projection maintenance")
    projections.add_argument("--activate", action="store_true", help="Stage, verify, then atomically activate")
    effect = sub.add_parser("resolve-effect", help="Explicitly adjudicate unknown local side effects")
    effect.add_argument("--journal", required=True)
    effect.add_argument("--operation", required=True)
    effect.add_argument("--decision", choices=["completed", "not_applied"], required=True)
    effect.add_argument("--reason", required=True)
    effect.add_argument("--evidence", required=True)
    effect.add_argument("--runtime-state")
    git_ops = sub.add_parser("git-ops")
    git_ops.add_argument("action", choices=("preflight", "reconcile", "backup", "restore",
                                          "retention-plan", "retention-execute"))
    git_ops.add_argument("--repo", required=True, help="Explicit local repository path")
    git_ops.add_argument("--project", help="Canonical project binding required for retention")
    git_ops.add_argument("--destination", help="New bundle or restore directory, never overwritten")
    git_ops.add_argument("--bundle", help="Validated local backup bundle for restore")
    git_ops.add_argument("--plan", help="Private JSON retention plan for explicit execution")
    git_ops.add_argument("--confirm", help="Exact retention plan digest")
    git_ops.add_argument("--remove-stale-locks", action="store_true")
    coordinated = sub.add_parser("coordinated",
                                 help="Offline coordinated snapshot/restore/retention; never a live service")
    coordinated.add_argument("action", choices=("snapshot", "verify", "restore",
                                                "retention-plan", "retention-execute",
                                                "release-hold"))
    coordinated.add_argument("--root", required=True,
                             help="Managed snapshots root (created if missing)")
    coordinated.add_argument("--source", help="Private JSON SnapshotSource configuration")
    coordinated.add_argument("--snapshot", help="Snapshot directory for verify/restore")
    coordinated.add_argument("--destination", help="New restore directory, never overwritten")
    coordinated.add_argument("--keep", type=int, default=3)
    coordinated.add_argument("--plan", help="Private JSON retention plan for explicit execution")
    coordinated.add_argument("--confirm", help="Exact plan/manifest/hold digest")
    coordinated.add_argument("--restored-store",
                             help="Restored canonical store directory for release-hold")
    coordinated.add_argument("--approval", help="Bounded local restore-release approval JSON")
    coordinated.add_argument("--identity", help="Private owner credential file for offline admin verification")
    coordinated.add_argument("--resume-release", action="store_true",
                             help="Explicitly resume an interrupted release with the same approval")
    worker = sub.add_parser("worker")
    worker.add_argument("--config", required=True)
    adapters = worker.add_mutually_exclusive_group(required=True)
    adapters.add_argument("--adapter", help="Trusted Python module:factory entry point")
    adapters.add_argument("--runtime", choices=("factory", "oh_my_pi", "a2a", "acp"),
                          help="Explicit supported runtime; no implicit discovery or paid-call opt-in")
    worker.add_argument("--journal", required=True)
    worker.add_argument("--workspace", required=True)
    worker.add_argument("--iterations", type=int)
    worker.add_argument("--enable", action="store_true")
    worker.add_argument("--supervise", action="store_true")
    worker.add_argument("--max-restarts", type=int, default=3)
    worker.add_argument("--reconcile-interval", type=float, default=30)
    worker.add_argument("--git-automation", action="store_true")
    worker.add_argument("--git-state-file", help="Private durable Git workflow state outside the clone")
    worker.add_argument("--allow-native-git", action="store_true")
    worker.add_argument("--messaging", action="store_true")
    worker.add_argument("--task-budgets", action="store_true",
                        help="Require durable task budgets and a bounded-turn runtime adapter")
    worker.add_argument("--consume-handoffs", action="store_true",
                        help="Load recipient-accepted context for the exact task execution")
    worker.add_argument("--message-sender", action="append", default=[])
    worker.add_argument("--message-replies", action="store_true")
    worker.add_argument("--notification-type", action="append", default=[])
    worker.add_argument("--concurrent-messaging", action="store_true",
                        help="Handle mail under a dedicated auxiliary run while the main task executes")
    worker.add_argument("--message-queue-capacity", type=int, default=16)
    worker.add_argument("--message-slots", type=int, default=1,
                        help="Independent auxiliary runs/private runtimes, 1 to 8; requires concurrent messaging")
    worker.add_argument("--message-push", action="store_true",
                        help="Follow the service's SSE message stream; timer reconcile remains as fallback")
    worker.add_argument("--message-reconcile-seconds", type=float, default=60.0,
                        help="Maximum seconds between canonical inbox reconciles (1 to 3600)")
    worker.add_argument("--recover-message-effects", action="store_true",
                        help="Reconcile proved message effects, never retry uncertain model turns")
    worker.add_argument("--recover-task-creates", action="store_true",
                        help="Recover committed task creation from exact canonical receipts; never replay turns")
    args = parser.parse_args()
    directory = Path(args.data_dir).resolve()
    try:
        if args.command == "resolve-effect":
            from .agent_session import SessionLock
            from .recovery import Journal
            from .vendor_runtimes import atomic_state
            journal_path = Path(args.journal).resolve(strict=True)
            with SessionLock(journal_path), Journal(journal_path) as journal:
                evidence_path = Path(args.evidence).resolve(strict=True)
                if evidence_path.stat().st_size > 65536:
                    raise ValueError("Evidence exceeds the local adjudication limit")
                evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                state_path = Path(args.runtime_state).resolve(strict=True) if args.runtime_state else None
                state = None
                if state_path:
                    if state_path.stat().st_size > 65536:
                        raise ValueError("Runtime state exceeds the local adjudication limit")
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                    identity = (journal.operation(args.operation) or {}).get("body") or {}
                    if (state.get("outcome") not in ("running", "unknown")
                            or any(not identity.get(key) or identity[key] != state.get(key)
                                   for key in ("run_id", "task_id", "execution_id"))):
                        raise ValueError("Runtime state does not match the unknown journal operation")
                record = journal.resolve_unknown(args.operation, args.decision, args.reason, evidence)
                if state_path:
                    atomic_state(state_path, {**state,
                        "outcome": "completed" if args.decision == "completed" else "idle",
                        "operator_reconciled": True})
                print(json.dumps({"operation_id": record["operation_id"], "state": record["state"]}))
            return
        if args.command == "git-ops":
            from .git_operations import GitOperations
            operations = GitOperations(directory / "git-collaboration")
            repo = Path(args.repo).resolve(strict=True)
            if args.action == "preflight":
                report = operations.upgrade_preflight(repo)
            elif args.action == "reconcile":
                report = operations.reconcile_repository(
                    repo, allow_stale_lock_removal=args.remove_stale_locks)
            elif args.action == "backup":
                if not args.destination:
                    raise ValueError("Backup requires a new --destination")
                report = operations.backup_repository(repo, Path(args.destination).resolve())
            elif args.action == "restore":
                if not args.bundle or not args.destination:
                    raise ValueError("Restore requires --bundle and a new --destination")
                report = operations.restore_backup(Path(args.bundle).resolve(strict=True),
                                                    Path(args.destination).resolve())
            elif args.action == "retention-plan":
                if not args.project or not (directory / "owner.json").is_file():
                    raise ValueError("Retention requires initialized canonical data and --project")
                store = Store(directory)
                try:
                    report = operations.plan_retention(
                        repo, guard=canonical_retention_guard(store, args.project, repo))
                finally:
                    store.close()
            else:
                if not args.plan or not args.confirm:
                    raise ValueError("Retention execution requires --plan and exact --confirm digest")
                plan_path = Path(args.plan).resolve(strict=True)
                if plan_path.stat().st_size > 131072:
                    raise ValueError("Retention plan exceeds its loading boundary")
                if not args.project or not (directory / "owner.json").is_file():
                    raise ValueError("Retention requires initialized canonical data and --project")
                store = Store(directory)
                try:
                    report = operations.execute_retention(
                        repo, json.loads(plan_path.read_text()), confirmation=args.confirm,
                        guard=canonical_retention_guard(store, args.project, repo))
                finally:
                    store.close()
            print(json.dumps(report))
            return
        if args.command == "coordinated":
            from .coordinated_operations import (CoordinatedOperations, SnapshotSource,
                                                 load_manifest)
            operations = CoordinatedOperations(args.root)
            if args.action == "snapshot":
                if not args.source:
                    raise ValueError("Snapshot requires --source JSON configuration")
                source_path = Path(args.source).resolve(strict=True)
                if source_path.stat().st_size > 131072:
                    raise ValueError("Snapshot source configuration exceeds its loading limit")
                source_config = json.loads(source_path.read_text(encoding="utf-8"))
                if not isinstance(source_config, dict) or set(source_config) - {
                        "db_path", "repositories", "journals", "artifacts", "exclusions", "name"}:
                    raise ValueError("Unknown snapshot source configuration keys")
                source = SnapshotSource(
                    db_path=source_config.get("db_path", ""),
                    repositories=tuple(source_config.get("repositories", ())),
                    journals=tuple(source_config.get("journals", ())),
                    artifacts=tuple(source_config.get("artifacts", ())),
                    exclusions=tuple(source_config.get("exclusions", ())))
                report = operations.create_snapshot(source, name=source_config.get("name"))
            elif args.action == "verify":
                if not args.snapshot:
                    raise ValueError("Verify requires --snapshot")
                report = operations.verify_snapshot(Path(args.snapshot).resolve(strict=True))
            elif args.action == "restore":
                if not args.snapshot or not args.destination or not args.confirm:
                    raise ValueError("Restore requires --snapshot, a new --destination and "
                                     "--confirm with the exact manifest digest")
                snapshot_dir = Path(args.snapshot).resolve(strict=True)
                expected = load_manifest(snapshot_dir)["digest"]
                if args.confirm != expected:
                    raise ValueError("Confirmation digest does not match the verified manifest")
                report = operations.restore_snapshot(
                    snapshot_dir, Path(args.destination).resolve(),
                    approve=lambda manifest_digest, verification: manifest_digest == args.confirm)
            elif args.action == "retention-plan":
                report = operations.plan_retention(keep=args.keep)
            elif args.action == "retention-execute":
                if not args.plan or not args.confirm:
                    raise ValueError("Retention execution requires --plan and exact --confirm digest")
                plan_path = Path(args.plan).resolve(strict=True)
                if plan_path.stat().st_size > 131072:
                    raise ValueError("Retention plan exceeds its loading boundary")
                report = operations.execute_retention(
                    json.loads(plan_path.read_text(encoding="utf-8")),
                    confirmation=args.confirm)
            else:  # release-hold: typed manual release after fresh verification
                from .restore_release import evidence_digest, load_evidence_file, release_restore_hold
                if not all((args.restored_store, args.confirm, args.approval, args.identity)):
                    raise ValueError("Hold release requires --restored-store, --approval, "
                                     "--identity and --confirm with the approval digest")
                evidence = load_evidence_file(args.approval)
                if args.confirm != evidence_digest(evidence):
                    raise ValueError("Confirmation digest does not match the approval")
                report = release_restore_hold(args.restored_store, evidence,
                                              identity=args.identity, resume=args.resume_release)
            print(json.dumps(report, default=str))
            return
        if args.command == "worker":
            if not args.enable:
                print("Worker is disabled; explicitly pass --enable to execute a trusted adapter.", file=sys.stderr)
                return 1
            if args.iterations is not None and args.iterations < 1:
                raise ValueError("Worker iterations must be positive")
            if (args.git_state_file or args.allow_native_git) and not args.git_automation:
                raise ValueError("Git workflow options require --git-automation")
            if args.git_automation and not args.allow_native_git:
                raise ValueError("Autonomous Git workspace verification requires --allow-native-git")
            if (args.message_sender or args.message_replies or args.notification_type) and not args.messaging:
                raise ValueError("Message options require --messaging")
            if args.messaging and not (args.message_sender or args.notification_type):
                raise ValueError("Messaging requires explicit sender or notification-type allowlists")
            if args.concurrent_messaging and not args.messaging:
                raise ValueError("Concurrent messaging requires --messaging")
            if args.recover_message_effects and not args.concurrent_messaging:
                raise ValueError("Message effect recovery requires --concurrent-messaging")
            if args.message_queue_capacity != 16 and not args.concurrent_messaging:
                raise ValueError("Message queue options require --concurrent-messaging")
            if args.message_slots != 1 and not args.concurrent_messaging:
                raise ValueError("Message slots require --concurrent-messaging")
            if not 1 <= args.message_slots <= 8:
                raise ValueError("Message slots must be between 1 and 8")
            if args.message_push and not args.concurrent_messaging:
                raise ValueError("Message push requires --concurrent-messaging")
            if not 1 <= args.message_reconcile_seconds <= 3600:
                raise ValueError("Message reconcile interval must be between 1 and 3600 seconds")
            if not 1 <= args.message_queue_capacity <= 256:
                raise ValueError("Message queue capacity must be between 1 and 256")
            try:
                from contextlib import ExitStack
                from importlib import import_module
                from .agent_session import JournalClient, SessionLock, WorkerJournal
                from .agent_worker import AgentWorker
                from .recovery import Journal
                from .sdk import Client
                from .runtime_lifecycle import LifecycleBoundary
                config = json.loads(Path(args.config).read_text(encoding="utf-8"))
                from .isolation_runtime import agent_identity_guard
                agent_identity_guard(config.get("runtime_options", {}))
                from .runtime_registry import factory_path
                selected_adapter = args.adapter or factory_path(args.runtime)
                module_name, separator, factory_name = selected_adapter.partition(":")
                if not separator or not factory_name:
                    raise ValueError("Adapter must be module:factory")
                client = Client(config["base_url"], config["token"])
                runtime = None
                messaging_adapter = None
                feature_holder = {}
                try:
                    from .worker_feature_wiring import WorkerFeatures, guarded_resource
                    with (guarded_resource(SessionLock(args.journal), feature_holder),
                          guarded_resource(Journal(args.journal, workspace=args.workspace), feature_holder) as journal,
                          ExitStack() as worker_lifetimes):
                        from .telemetry import Telemetry, TelemetryPolicy, TracedRuntime
                        telemetry = Telemetry(TelemetryPolicy(**config.get("telemetry", {})))
                        worker_lifetimes.callback(telemetry.close)
                        client.telemetry = telemetry
                        client.trace_identity = {"project_id": config["project_id"],
                                                 "agent_id": config["agent_id"], "run_id": config["run_id"]}

                        def runtime_factory(options):
                            adapter = getattr(import_module(module_name), factory_name)(options)
                            return TracedRuntime(adapter, telemetry) if telemetry.policy.enabled else adapter

                        wrapped = JournalClient(client, journal)
                        if args.recover_task_creates:
                            wrapped.recover_task_creates(enabled=True)
                        transitions = WorkerJournal(journal, config["project_id"], config["agent_id"],
                                                    files=config.get("checkpoint_files", []))
                        transitions.recover(config["run_id"])
                        wrapped.resume_commands()
                        git_automation = None
                        git_facade = None
                        runtime_options = dict(config.get("runtime_options", {}))
                        if args.git_automation:
                            from .git_worker_automation import GitWorkerAutomation, WorkerGitFacade
                            git_automation = GitWorkerAutomation(
                                wrapped, config["project_id"], config["agent_id"], config["run_id"],
                                enabled=True, state_file=args.git_state_file or str(args.journal) + ".git.json",
                                allow_native_git=args.allow_native_git, journal=transitions)
                            workspace = git_automation.provision()
                            git_facade = WorkerGitFacade(git_automation)
                            runtime_options["workspace"] = workspace["workspace_path"]
                            journal.workspace = Path(workspace["workspace_path"]).resolve(strict=True)
                        features = WorkerFeatures(wrapped, config, journal, journal.workspace)
                        feature_holder["owner"] = features
                        worker_lifetimes.callback(features.close)
                        features.start()
                        messaging_policy = None
                        concurrent_messaging = None
                        if args.messaging:
                            from .agent_session import HandlerJournal
                            from .runtime_messaging import MessagingPolicy, VendorMessagingExecutor
                            messaging_policy = MessagingPolicy(
                                enabled=True, allowed_senders=args.message_sender,
                                allowed_notification_types=args.notification_type,
                                allow_replies=args.message_replies)
                            message_options = {**runtime_options, "result_contract": "message", "no_tools": True,
                                               "state_file": str(args.journal) + ".message-runtime.json"}
                            if args.concurrent_messaging:
                                from .worker_messaging import WorkerMessaging
                                push_source = None
                                if args.message_push:
                                    import httpx
                                    from .message_push import (PushWakeSource,
                                                               httpx_sse_subscription)
                                    push_http = httpx.Client(base_url=config["base_url"],
                                        headers={"Authorization": "Bearer " + config["token"]})
                                    worker_lifetimes.callback(push_http.close)
                                    push_source_holder = {}

                                    def push_subscribe(after):
                                        source = push_source_holder["source"]
                                        path = (f"/v1/agents/{config['agent_id']}/message-stream"
                                                f"?project_id={config['project_id']}&after={after}")
                                        return httpx_sse_subscription(push_http, path,
                                                                      cancel=source.cancel)

                                    push_source = PushWakeSource(
                                        push_subscribe, project_id=config["project_id"],
                                        agent_id=config["agent_id"],
                                        cursor_path=str(args.journal) + ".push-cursor.json")
                                    push_source_holder["source"] = push_source
                                    worker_lifetimes.callback(push_source.close)
                                    push_source.start()
                                concurrent_messaging = WorkerMessaging(
                                    wrapped, config, journal,
                                    runtime_factory, message_options,
                                    enabled=True, allowed_senders=args.message_sender,
                                    allowed_notification_types=args.notification_type,
                                    allow_replies=args.message_replies,
                                    queue_capacity=args.message_queue_capacity,
                                    slots=args.message_slots,
                                    push_signal=push_source.event if push_source else None,
                                    max_reconcile_seconds=args.message_reconcile_seconds,
                                    recover_message_effects=args.recover_message_effects)
                                worker_lifetimes.callback(concurrent_messaging.close)
                                concurrent_messaging.start()
                            else:
                                messaging_adapter = VendorMessagingExecutor(
                                    wrapped, config["project_id"], config["agent_id"], config["run_id"],
                                    runtime_factory=lambda: runtime_factory(message_options),
                                    handler_journal=HandlerJournal(journal, config["project_id"], config["agent_id"]),
                                    allowed_senders=args.message_sender, allow_replies=args.message_replies,
                                    allowed_notification_types=args.notification_type)
                        def make_worker():
                            nonlocal runtime
                            transitions.recover(config["run_id"])
                            if args.recover_task_creates:
                                wrapped.recover_task_creates(enabled=True)
                            wrapped.resume_commands()
                            runtime = runtime_factory(runtime_options)
                            automatic_controls = features.bind_runtime(runtime)
                            boundary = LifecycleBoundary(wrapped, config["project_id"], config["agent_id"],
                                config["run_id"], str(args.journal) + ".controls.json", adapter=runtime)
                            from .worker_budget_governance import WorkerBudgetGovernance
                            budget_governance = (WorkerBudgetGovernance(wrapped, config["project_id"])
                                                 if args.task_budgets else None)
                            return AgentWorker(wrapped, config["project_id"], config["agent_id"], config["run_id"],
                                runtime, journal=transitions, enabled=True, control_boundary=boundary,
                                assemble_context=config.get("assemble_context", True),
                                context_token_budget=config.get("context_token_budget", 6000),
                                submit_verification=config.get("submit_verification", True),
                                renew_message_leases=config.get("renew_message_leases", True),
                                process_corrections=config.get("process_corrections", True),
                                context_tokenizer=config.get("context_tokenizer"),
                                require_exact_context_tokens=config.get("require_exact_context_tokens", False),
                                consume_handoffs=args.consume_handoffs or config.get("consume_handoffs", False),
                                budget_governance=budget_governance,
                                git_automation=git_facade, messaging_policy=messaging_policy,
                                messaging_adapter=messaging_adapter,
                                capture_lifecycle=features.capture, automatic_controls=automatic_controls,
                                control_signals=features.signals,
                                messaging_delegate=concurrent_messaging.supervisor if concurrent_messaging else None)
                        if args.supervise:
                            from types import SimpleNamespace
                            from .runtime_lifecycle import RuntimeReconciler
                            from .worker_supervisor import WorkerSupervisor
                            observer = wrapped.request("POST", "runs", {
                                "project_id": config["project_id"], "agent_id": config["agent_id"]},
                                key="supervisor-observer-" + config["run_id"])
                            def inspect(run):
                                return runtime.inspect(run) if runtime and hasattr(runtime, "inspect") else {
                                    "present": None, "status": "unknown"}
                            reconciler = RuntimeReconciler(wrapped, config["project_id"], config["agent_id"],
                                observer["id"], state_dir=str(args.journal) + ".lifecycle",
                                adapter=SimpleNamespace(inspect=inspect), enabled=True)
                            manager = WorkerSupervisor(make_worker, journal, str(args.journal) + ".supervisor.json",
                                reconciler=reconciler, enabled=True, max_restarts=args.max_restarts,
                                reconcile_interval=args.reconcile_interval)
                            try:
                                report = manager.run(iterations=args.iterations)
                            finally:
                                reconciler.close()
                            report = {**report, "stopped": report["state"] != "running",
                                      "halt_reason": report.get("reason", report["state"])}
                        else:
                            report = make_worker().run(iterations=args.iterations)
                    print(json.dumps({"ticks": report["ticks"], "stopped": report["stopped"],
                                      "halt_reason": report["halt_reason"]}))
                finally:
                    try:
                        message_runtime = getattr(messaging_adapter, "_cached_runtime", None)
                        message_close = getattr(message_runtime, "close", None)
                        try:
                            if callable(message_close):
                                message_close()
                        finally:
                            close = getattr(runtime, "close", None)
                            if callable(close):
                                close()
                    finally:
                        if not (feature_holder.get("owner") and feature_holder["owner"].shutdown_pending):
                            client.close()
            except KeyboardInterrupt:
                print("Worker interrupted; reconcile its journal before restarting.", file=sys.stderr)
                return 1
            except Exception as exc:
                print(f"Worker failed: {type(exc).__name__}. Inspect private state locally.", file=sys.stderr)
                return 1
        elif args.command == "init":
            destination = initialize(directory, args.title)
            print(f"Initialized. Owner credential saved to {destination}. Keep it private.")
        elif args.command == "issue-token":
            store = Store(directory)
            store.project(args.project)
            if args.role == "agent":
                if not args.agent:
                    raise ValueError("Agent credential requires --agent")
                agent = store.raw(args.agent, "agent")
                if agent["project"] != args.project:
                    raise ValueError("Agent and project do not match")
            elif args.agent or args.coordinator or args.capture_authority:
                raise ValueError("Agent grants require role agent")
            if Path(args.out).exists():
                raise ValueError("Credential destination already exists")
            token = secrets.token_urlsafe(32)
            with store.transaction():
                store.db.execute(
                    "INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                    (uid("principal"), hash_token(token), args.role, args.project, args.agent,
                     int(args.coordinator), int(args.capture_authority)))
            private_write(args.out, {"token": token, "project_id": args.project, "agent_id": args.agent,
                                      "base_url": "http://127.0.0.1:8765"})
            store.close()
            print(f"Credential saved to {Path(args.out).resolve()}. No token was printed.")
        elif args.command == "download-model":
            os.environ.setdefault("HF_HOME", str(directory / "models"))
            from huggingface_hub import snapshot_download
            snapshot_download(MODEL_ID, revision=MODEL_REVISION,
                              ignore_patterns=["onnx/*", "*.onnx", "*.tflite", "*.gguf"])
            print("Pinned model downloaded.")
        elif args.command == "serve":
            if args.open and not args.enable_ui:
                raise ValueError("--open requires --enable-ui")
            import uvicorn
            os.environ.setdefault("HF_HOME", str(directory / "models"))
            if not (directory / "owner.json").exists():
                raise ValueError("Initialize the data directory first")
            encoder = Gemma() if args.embeddings else None
            from .model_wiring import integration_options
            options = integration_options(args.agent_integrations) if args.agent_integrations else None
            app = create_app(directory, encoder, capability_options=options, enable_ui=args.enable_ui)
            if args.open:
                webbrowser.open(f"http://127.0.0.1:{args.port}/")
            uvicorn.run(app, host="127.0.0.1", port=args.port)
        elif args.command == "backup":
            import sqlite3
            store = Store(directory)
            destination = directory / f"backup-{uid('snapshot')}.sqlite"
            with sqlite3.connect(destination) as target:
                store.db.backup(target)
            store.close()
            print(f"Canonical backup: {destination}. Contains private data; apply retention policy.")
        elif args.command == "rebuild-projections":
            import sqlite3
            from .projection_rebuild import activate, stage
            store = None
            try:
                store = Store(directory)
                report = {"stage": stage(store)}
                if args.activate:
                    report["activation"] = activate(store)
                print(json.dumps(report))
            except sqlite3.Error:
                raise Problem("projection_storage_failure", 503) from None
            finally:
                if store is not None:
                    store.close()
        elif args.command == "rebuild-indexes":
            store = Store(directory)
            with store.transaction():
                store.db.execute("UPDATE jobs SET state='pending',attempts=0,error=NULL")
            store.close()
            print("Derived index jobs scheduled for idempotent replay. Start the server to process them.")
    except Problem as exc:
        print(f"Operation refused: {exc.code}", file=sys.stderr)
        return 1
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
