"""Explicit, bounded live adapter tests. Never run by ordinary pytest."""
import argparse
import hashlib
import json
import secrets
import socket
import threading
import time
from pathlib import Path

import uvicorn

from hivemind.api import create_app, hash_token
from hivemind.agent_session import HandlerJournal
from hivemind.cli import initialize, private_write
from hivemind.git_reviewer_runtime import GitReviewerRuntime, ReviewerRuntimePolicy
from hivemind.recovery import Journal
from hivemind.runtime_messaging import VendorMessagingExecutor
from hivemind.sdk import Client
from hivemind.storage import uid
from hivemind.vendor_runtimes import FactoryRuntime, OhMyPiRuntime


def suite(root, vendor, owner, producer, config, app, agent, task, run, options, clients,
          runtimes, report):
    """Real independent Git review and durable messaging, two model calls per vendor."""
    project = config["project_id"]
    initialized = owner.request("POST", f"projects/{project}/git:initialize")
    clone = producer.request("POST", "git-workspaces", {"project_id": project, "agent_id": agent["id"]})
    folder = f"{vendor}_notes"
    path = Path(clone["workspace_path"]) / folder
    path.mkdir()
    (path / "fixture.txt").write_text("A harmless local integration-test fixture.\n")
    binding = {"task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"]}
    committed = producer.request("POST", f"git-workspaces/{clone['id']}:commit", {
        **binding, "expected_revision": clone["revision"], "message": "Add local live review fixture",
        "paths": [f"{folder}/fixture.txt"]})
    producer.request("POST", f"git-workspaces/{clone['id']}:push",
                     {**binding, "source_oid": committed["head_oid"]})
    request = producer.request("POST", "merge-requests", {
        "project_id": project, "workspace_id": clone["id"], "task_id": task["id"],
        "source_oid": committed["head_oid"], "target_oid": initialized["main_oid"],
        "title": "Harmless live independent review fixture"})
    reviewer = owner.request("POST", "agents", {
        "project_id": project, "display_name": f"Independent live {vendor} reviewer",
        "scope_permit_enforcement": True})
    token = secrets.token_urlsafe(32)
    principal_id = uid("principal")
    with app.state.store.transaction():
        app.state.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)", (
            principal_id, hash_token(token), "agent", project, reviewer["id"], 0, 1))
    base = str(owner.http.base_url).removesuffix("/v1/")
    client = Client(base, token)
    clients.append(client)
    credential = root / vendor / "reviewer-credential.json"
    private_write(credential, {"base_url": base, "token": token,
                              "project_id": project, "agent_id": reviewer["id"]})
    created = []

    def factory(workspace, context, contract="git_review"):
        opts = {**options, "workspace": str(workspace), "config": str(credential),
                "state_file": str(root / vendor / (uid("live-state") + ".json")),
                "result_contract": contract, "read_only_tools": contract == "git_review",
                "no_tools": contract == "message"}
        runtime = (FactoryRuntime if vendor == "factory" else OhMyPiRuntime)(opts)
        runtimes.append(runtime)
        created.append(runtime)
        return runtime

    executor = GitReviewerRuntime(app.state.git_collaboration, runtime_factory=factory,
        policy=ReviewerRuntimePolicy(enabled=True, allow_model_calls=True, timeout_seconds=100),
        journal_path=root / vendor / "reviewer-journal.sqlite")
    report["model_calls"] += 1
    outcome = executor.review_merge(request["id"], reviewer_principal_id=principal_id,
                                    reviewer_agent_id=reviewer["id"])
    review_usage = created[-1].state.get("usage")
    duplicate = executor.review_merge(request["id"], reviewer_principal_id=principal_id,
                                      reviewer_agent_id=reviewer["id"])
    message = producer.request("POST", "messages", {
        "project_id": project, "message_id": uid("msg"), "from": agent["id"], "to": reviewer["id"],
        "type": "request", "thread_id": uid("thread"),
        "content": {"media_type": "text/plain", "text": "What is two plus two? Reply briefly."}})
    receipt = client.claim_message(message["id"], outcome.review_run_id)
    message_workspace = root / vendor / "message-workspace"
    message_workspace.mkdir()

    def message_factory():
        return factory(message_workspace, {}, "message")

    journal_path = root / vendor / "message-journal.sqlite"
    with Journal(journal_path) as journal:
        handler = VendorMessagingExecutor(client, project, reviewer["id"], outcome.review_run_id,
            runtime_factory=message_factory,
            handler_journal=HandlerJournal(journal, project, reviewer["id"]),
            allowed_senders=[agent["id"]], allow_replies=True)
        report["model_calls"] += 1
        handled = handler.handle_message(receipt)
        message_usage = created[-1].state.get("usage")
    calls = len(created)
    with Journal(journal_path) as journal:
        restarted = VendorMessagingExecutor(client, project, reviewer["id"], outcome.review_run_id,
            runtime_factory=message_factory,
            handler_journal=HandlerJournal(journal, project, reviewer["id"]),
            allowed_senders=[agent["id"]], allow_replies=True)
        replay = restarted.handle_message(receipt)
    replies = producer.message_inbox(agent["id"], project)
    correlated = [item for item in replies if item.get("in_reply_to") == message["id"]]
    client.ack_message(message["id"], outcome.review_run_id, receipt["delivery_id"])
    return {"status": "completed", "review_decision": outcome.decision,
            "snapshot_verified": outcome.snapshot_verified_unchanged,
            "independent_identity": outcome.reviewer_agent_id != agent["id"],
            "review_deduplicated": duplicate.already_reviewed, "message_handled": handled["handled"],
            "message_restart_deduplicated": replay["deduplicated"] and len(created) == calls,
            "one_correlated_reply": len(correlated) == 1,
            "review_usage": review_usage, "message_usage": message_usage}


def recovery(vendor, runtime, task, context, client, owner, options, runtimes, report):
    outcomes, errors = [], []

    def turn():
        try:
            outcomes.append(runtime.execute(task, context, None))
        except Exception as error:
            errors.append(type(error).__name__)

    report["model_calls"] += 1
    worker = threading.Thread(target=turn, daemon=True)
    worker.start()
    deadline = time.monotonic() + 15
    while not runtime.connected() and worker.is_alive() and time.monotonic() < deadline:
        time.sleep(.05)
    time.sleep(3)
    in_flight = worker.is_alive() and runtime._active
    control = runtime.pause({"id": context["run_id"]})
    worker.join(100)
    if not in_flight or worker.is_alive() or errors or control.get("applied") is not True:
        raise RuntimeError("Controlled interruption was not confirmed; do not replay")
    if not outcomes or outcomes[0]["status"] != "paused":
        raise RuntimeError("Vendor did not observe a paused turn")
    interrupted_usage = runtime.state.get("usage")
    session_id = runtime.state["session_id"]
    checkpoint_id = runtime.state.get("checkpoint_id")
    runtime.close()
    # Retire the intentionally interrupted fixture before assigning a new canonical task.
    client.request("POST", f"tasks/{task['id']}:checkpoint", {
        "expected_revision": task["revision"], "run_id": context["run_id"],
        "execution_id": context["execution_id"], "checkpoint_id": uid("checkpoint"),
        "status": "failed", "progress": 0, "summary": "Controlled interruption fixture retired"})
    next_task = owner.request("POST", "tasks", {
        "project_id": context["project_id"], "title": "Verify canonical checkpoint consumption",
        "description": ('Read managed_resume_context.checkpoint.summary and return exactly one JSON '
                        'object {"handled":true,"reply_text":"<that exact summary>"}. Do not use tools.'),
        "scope_ids": []})
    next_task = client.request("POST", f"tasks/{next_task['id']}:start", {
        "expected_revision": next_task["revision"], "run_id": context["run_id"], "recover": False})
    marker = "CANONICAL_RECOVERY_42"
    next_task = client.request("POST", f"tasks/{next_task['id']}:checkpoint", {
        "expected_revision": next_task["revision"], "run_id": context["run_id"],
        "execution_id": next_task["execution"]["id"], "checkpoint_id": uid("checkpoint"),
        "status": "running", "progress": 40, "summary": marker})
    resumed = (FactoryRuntime if vendor == "factory" else OhMyPiRuntime)(
        {**options, "result_contract": "message", "no_tools": True})
    runtimes.append(resumed)
    report["model_calls"] += 1
    result = resumed.execute(next_task, {**context, "execution_id": next_task["execution"]["id"],
        "phase_id": "live-recovery-resume", "resumed": True, "progress": 40}, None)
    if result["message_result"]["reply_text"] != marker:
        raise RuntimeError("Resumed vendor did not consume canonical checkpoint")
    client.request("POST", f"tasks/{next_task['id']}:checkpoint", {
        "expected_revision": next_task["revision"], "run_id": context["run_id"],
        "execution_id": next_task["execution"]["id"], "checkpoint_id": uid("checkpoint"),
        "status": "completed", "progress": 100, "summary": "Canonical checkpoint consumption verified"})
    return {"status": "completed", "interrupted_in_flight": in_flight,
            "observed_paused": outcomes[0]["status"] == "paused",
            "quiescent_checkpoint": bool(checkpoint_id),
            "session_reopened": resumed.state["session_id"] == session_id,
            "checkpoint_consumed": True, "interrupted_usage": interrupted_usage,
            "resumed_usage": resumed.state.get("usage")}


def run(root, stage, vendors):
    root.mkdir(parents=True, exist_ok=False)
    config = json.loads(initialize(root / "service").read_text())
    app = create_app(root / "service", workers=False)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(.05)
    if not server.started:
        server.should_exit = True
        thread.join(15)
        raise RuntimeError("Isolated service failed to start")
    owner = Client(base, config["token"])
    report = {"stage": stage, "model_calls": 0, "results": []}
    runtimes = []
    clients = [owner]
    try:
        for vendor in vendors:
            workspace = root / vendor / "workspace"
            workspace.mkdir(parents=True)
            folder = f"{vendor}_notes"
            (workspace / folder).mkdir()
            (workspace / folder / "input.txt").write_text("LOCAL_LIVE_INPUT_42\n")
            agent = owner.request("POST", "agents", {
                "project_id": config["project_id"], "display_name": f"Live {vendor}",
                "supported_actions": ["steer", "pause", "resume", "cancel", "assign_task"],
                "scope_permit_enforcement": True})
            token = secrets.token_urlsafe(32)
            with app.state.store.transaction():
                app.state.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)", (
                    uid("principal"), hash_token(token), "agent", config["project_id"], agent["id"], 0, 1))
            client = Client(base, token)
            clients.append(client)
            run = client.request("POST", "runs", {"project_id": config["project_id"], "agent_id": agent["id"]})
            scope = owner.request("POST", "scopes", {
                "project_id": config["project_id"], "canonical_key": f"live/{vendor}",
                "display_name": f"Live {vendor}", "allowed_paths": [folder]})
            client.request("POST", "work-claims", {
                "project_id": config["project_id"], "agent_id": agent["id"], "run_id": run["id"],
                "scope_ids": [scope["id"]], "intent": "Bounded local live integration test"})
            task = owner.request("POST", "tasks", {
                "project_id": config["project_id"], "title": "Bounded live file-tool smoke test",
                "description": ("Write a detailed 3000-word essay about sorting algorithms. Use no tools."
                                if stage == "recovery" else
                                f"Read {folder}/input.txt. Create {folder}/output.txt containing exactly "
                                "LOCAL_LIVE_OUTPUT_42 followed by a newline. Do not use any other tools "
                                "or paths. Return one short sentence."),
                "scope_ids": [scope["id"]]})
            task = client.request("POST", f"tasks/{task['id']}:start", {
                "expected_revision": task["revision"], "run_id": run["id"], "recover": False})
            credential = root / vendor / "credential.json"
            private_write(credential, {"base_url": base, "token": token,
                                      "project_id": config["project_id"], "agent_id": agent["id"]})
            options = {"workspace": str(workspace), "config": str(credential),
                       "state_file": str(root / vendor / "runtime.json"), "timeout_seconds": 90,
                       "enabled": True, "allow_model_calls": True, "collect_usage": True,
                       "no_tools": stage == "recovery",
                       "executable": (r"C:\Users\sims\bin\droid.exe" if vendor == "factory"
                                      else r"C:\Users\sims\.bun\bin\omp.exe"),
                       "model": ("gpt-6-luna" if vendor == "factory"
                                 else "ollama-cloud/deepseek-v4.1-flash")}
            runtime = (FactoryRuntime if vendor == "factory" else OhMyPiRuntime)(options)
            runtimes.append(runtime)
            context = {"project_id": config["project_id"], "agent_id": agent["id"],
                       "run_id": run["id"], "execution_id": task["execution"]["id"],
                       "phase_id": "live-smoke", "packet": {}}
            started = time.monotonic()
            entry = {"vendor": vendor, "model": options["model"]}
            try:
                if stage == "preflight":
                    runtime._prepare(task, context)
                    if vendor == "factory":
                        runtime._start_loop()
                        runtime._await(runtime._open(), 30)
                        entry["session_context_available"] = runtime._await(runtime.session.context(), 10) is not None
                    else:
                        runtime._open()
                        state = runtime._request("get_state")
                        entry["selected_model"] = {key: (state.get("model") or {}).get(key)
                                                   for key in ("provider", "id")}
                    runtime._settled("idle")
                    entry["status"] = "ready"
                elif stage == "suite":
                    entry.update(suite(root, vendor, owner, client, config, app, agent, task, run,
                                       options, clients, runtimes, report))
                elif stage == "recovery":
                    entry.update(recovery(vendor, runtime, task, context, client, owner, options,
                                          runtimes, report))
                else:
                    report["model_calls"] += 1
                    outcome = runtime.execute(task, context, lambda *a, **k: None)
                    entry["status"] = outcome["status"]
                    output = workspace / folder / "output.txt"
                    entry["file_verified"] = output.exists() and output.read_text().strip() == "LOCAL_LIVE_OUTPUT_42"
                    entry["result_hash"] = hashlib.sha256(outcome["summary"].encode()).hexdigest()
                    entry["usage"] = runtime.state.get("usage")
                    entry["checkpoint"] = bool(runtime.state.get("checkpoint_id"))
            except Exception as error:
                # No raw vendor stderr, credentials, transcripts, or hidden reasoning.
                entry.update(status="error", error_type=type(error).__name__,
                             runtime_outcome=runtime.state.get("outcome"))
                if isinstance(error, PermissionError) and str(error) == "Resumed vendor workspace mismatch":
                    entry["error_code"] = "resumed_workspace_mismatch"
                entry["live_runtime_outcomes"] = [
                    value.state.get("outcome") for value in runtimes if value.vendor == runtime.vendor]
            entry["duration_seconds"] = round(time.monotonic() - started, 3)
            report["results"].append(entry)
            (root / "report.json").write_text(json.dumps(report, indent=2))
            print(json.dumps(entry), flush=True)
            if entry["status"] == "error":
                break  # Observe and reconcile before any further paid turn.
    finally:
        for runtime in reversed(runtimes):
            try:
                runtime.close()
            except Exception as error:
                report.setdefault("cleanup_errors", []).append(type(error).__name__)
        for client in clients:
            client.close()
        server.should_exit = True
        thread.join(15)
        report["service_stopped"] = not thread.is_alive()
        (root / "report.json").write_text(json.dumps(report, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--stage", choices=("preflight", "smoke", "suite", "recovery"), default="preflight")
    parser.add_argument("--vendor", choices=("factory", "omp", "both"), default="both")
    parser.add_argument("--allow-paid", action="store_true")
    args = parser.parse_args()
    if args.stage != "preflight" and not args.allow_paid:
        raise PermissionError("Live inference requires --allow-paid and user-approved budget")
    result = run(Path(args.root).resolve(), args.stage,
                 ("factory", "omp") if args.vendor == "both" else (args.vendor,))
    return int(any(item["status"] == "error" for item in result["results"]))


if __name__ == "__main__":
    raise SystemExit(main())
