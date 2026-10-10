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
    serve.add_argument("--open", action="store_true")
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
    worker = sub.add_parser("worker")
    worker.add_argument("--config", required=True)
    worker.add_argument("--adapter", required=True, help="Trusted Python module:factory entry point")
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
    worker.add_argument("--message-sender", action="append", default=[])
    worker.add_argument("--message-replies", action="store_true")
    worker.add_argument("--notification-type", action="append", default=[])
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
            try:
                from importlib import import_module
                from .agent_session import JournalClient, SessionLock, WorkerJournal
                from .agent_worker import AgentWorker
                from .recovery import Journal
                from .sdk import Client
                from .runtime_lifecycle import LifecycleBoundary
                config = json.loads(Path(args.config).read_text(encoding="utf-8"))
                module_name, separator, factory_name = args.adapter.partition(":")
                if not separator or not factory_name:
                    raise ValueError("Adapter must be module:factory")
                client = Client(config["base_url"], config["token"])
                runtime = None
                messaging_adapter = None
                try:
                    with SessionLock(args.journal), Journal(args.journal, workspace=args.workspace) as journal:
                        wrapped = JournalClient(client, journal)
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
                        messaging_policy = None
                        if args.messaging:
                            from .agent_session import HandlerJournal
                            from .runtime_messaging import MessagingPolicy, VendorMessagingExecutor
                            messaging_policy = MessagingPolicy(
                                enabled=True, allowed_senders=args.message_sender,
                                allowed_notification_types=args.notification_type,
                                allow_replies=args.message_replies)
                            message_options = {**runtime_options, "result_contract": "message", "no_tools": True,
                                               "state_file": str(args.journal) + ".message-runtime.json"}
                            messaging_adapter = VendorMessagingExecutor(
                                wrapped, config["project_id"], config["agent_id"], config["run_id"],
                                runtime_factory=lambda: getattr(import_module(module_name), factory_name)(message_options),
                                handler_journal=HandlerJournal(journal, config["project_id"], config["agent_id"]),
                                allowed_senders=args.message_sender, allow_replies=args.message_replies,
                                allowed_notification_types=args.notification_type)
                        def make_worker():
                            nonlocal runtime
                            transitions.recover(config["run_id"])
                            wrapped.resume_commands()
                            runtime = getattr(import_module(module_name), factory_name)(runtime_options)
                            boundary = LifecycleBoundary(wrapped, config["project_id"], config["agent_id"],
                                config["run_id"], str(args.journal) + ".controls.json", adapter=runtime)
                            return AgentWorker(wrapped, config["project_id"], config["agent_id"], config["run_id"],
                                runtime, journal=transitions, enabled=True, control_boundary=boundary,
                                assemble_context=config.get("assemble_context", True),
                                context_token_budget=config.get("context_token_budget", 6000),
                                submit_verification=config.get("submit_verification", True),
                                renew_message_leases=config.get("renew_message_leases", True),
                                process_corrections=config.get("process_corrections", True),
                                context_tokenizer=config.get("context_tokenizer"),
                                require_exact_context_tokens=config.get("require_exact_context_tokens", False),
                                git_automation=git_facade, messaging_policy=messaging_policy,
                                messaging_adapter=messaging_adapter)
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
            import uvicorn
            os.environ.setdefault("HF_HOME", str(directory / "models"))
            if not (directory / "owner.json").exists():
                raise ValueError("Initialize the data directory first")
            encoder = Gemma() if args.embeddings else None
            from .model_wiring import integration_options
            options = integration_options(args.agent_integrations) if args.agent_integrations else None
            app = create_app(directory, encoder, capability_options=options)
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
