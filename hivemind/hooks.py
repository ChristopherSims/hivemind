"""Droid stdin hooks and JSON bridge for the Oh my Pi extension."""
import argparse
import json
import os
import sys
import hashlib
from pathlib import Path


from .sdk import Capture, Client, ControlledAdapter, excluded
from .storage import digest, uid


def file_hash(path):
    if not path.is_file():
        return None
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def mutation_paths(name, tool_input):
    if name.lower() in ("edit", "write", "create") and isinstance(tool_input, dict):
        path = tool_input.get("file_path", tool_input.get("path"))
        if isinstance(path, str) and path:
            return [path]
    if name.lower() == "applypatch":
        patch = tool_input if isinstance(tool_input, str) else (
            tool_input.get("patch", tool_input.get("input")) if isinstance(tool_input, dict) else None)
        if not isinstance(patch, str):
            raise RuntimeError("Patch input shape is not supported")
        lines = patch.strip().splitlines()
        if not lines or lines[0] != "*** Begin Patch" or lines[-1] != "*** End Patch":
            raise RuntimeError("Patch envelope is not supported")
        paths = []
        prefixes = ("*** Add File: ", "*** Update File: ", "*** Delete File: ", "*** Move to: ")
        for line in lines[1:-1]:
            prefix = next((p for p in prefixes if line.startswith(p)), None)
            if prefix:
                path = line[len(prefix):].strip()
                if not path:
                    raise RuntimeError("Empty patch path")
                paths.append(path)
            elif line.startswith("***") and line != "*** End of File":
                raise RuntimeError("Unknown patch directive")
        if paths and len(paths) <= 64:
            return list(dict.fromkeys(paths))
    raise RuntimeError("Controlled mode blocks tools without enforceable file paths")


def check_read_scope(config, name, tool_input):
    if not config.get("restrict_reads_to_workspace"):
        return
    if not isinstance(tool_input, dict):
        raise RuntimeError("Read input shape is not supported")
    path = {
        "read": tool_input.get("file_path", tool_input.get("path")),
        "ls": tool_input.get("directory_path", "."),
        "glob": tool_input.get("folder", "."),
        "grep": tool_input.get("path", "."),
    }.get(name.lower())
    if path is None:
        raise RuntimeError("Read path is required")
    root = Path(config["workspace_root"]).resolve()
    target = Path(path)
    target = (root / target).resolve() if not target.is_absolute() else target.resolve()
    relative = target.relative_to(root)
    if excluded(relative):
        raise RuntimeError("Read path is excluded")


def invoke(config, payload):
    monitored = config.get("permission_monitor") is not None
    pre = payload.get("hook_event_name", payload.get("type")) in ("PreToolUse", "tool_call")
    try:
        if monitored and pre:
            from .worker_permissions import read_violation
            monitor = config["permission_monitor"]
            if read_violation(Path(monitor["incident_file"]),
                    {key: monitor[key] for key in ("worker_id", "launch_id", "nonce")}):
                raise PermissionError("Worker already has a safety incident")
        if pre and config.get("agent_permissions") is not None:
            from .worker_permissions import WorkerPermissions
            WorkerPermissions(**config["agent_permissions"]).check(config,
                payload.get("tool_name", payload.get("toolName", "")),
                payload.get("tool_input", payload.get("input", {})))
        return _invoke(config, payload)
    # Exception types vary by caller's httpx flavor (httpx vs namespace-collision aliases);
    # deny by presence, re-raise unchanged.
    except Exception as denial:  # noqa: BLE001 — wrapper always re-raises
        if monitored and pre:
            from .worker_permissions import signal_violation
            signal_violation(config)
        # R4: denied mutations leave a canonical advisory trace — an agent-reported
        # status event the ledger already types (no permission granted, no retry).
        if pre and config.get("record_denied_mutations", True):
            try:
                from .sdk import Capture
                state_dir = Path(config["state_dir"]).resolve()
                session_id = payload.get("session_id", "default")
                session_key = hashlib.sha256(session_id.encode()).hexdigest()
                state_file = state_dir / f"{session_key}.json"
                if state_file.exists():
                    denied_state = json.loads(state_file.read_text())
                    run_id = denied_state.get("run_id")
                    if run_id:
                        import sys as _sys
                        print("R4-trace run", run_id, file=_sys.stderr)
                        connection = Client(config["base_url"], config["token"])
                        capture = Capture(connection, config["project_id"],
                                          config["agent_id"], run_id,
                                          state_dir / f"{session_key}-spool.sqlite",
                                          start=False)
                        try:
                            capture.observe("agent.status_reported", {"text":
                                f"mutation denied: {payload.get('tool_name', payload.get('toolName', ''))} "
                                f"— {type(denial).__name__}"}, attribution="reported")
                        finally:
                            capture.db.close()
                            connection.close()
            except Exception as _r4error:  # noqa: BLE001 — advisory tracing must never mask the denial
                import sys as _sys
                print("R4-swallowed", type(_r4error).__name__, str(_r4error)[:120], file=_sys.stderr)
        raise


def checkpoint_task(client, config, task_id, paths, call_id, run_id=None):
    """R5: file a self-reported progress checkpoint for the run's bound task.

    Only fires with a live execution this run owns (server-checked). The status
    stays ``running`` — a checkpoint reports progress, never independent
    verification. Failure is advisory (the caller records the error name).
    """
    from .storage import uid
    task = client.request("GET", f"tasks/{task_id}")
    execution = task.get("execution") or {}
    if not execution.get("id"):
        # A provisioned-but-never-started task: the adapter starts it under this run
        # (fresh fenced execution) before checkpointing; the server gates start on
        # valid current claims, so a missing claim fails here and stays advisory.
        started = client.request("POST", f"tasks/{task_id}:start", {
            "run_id": run_id or config.get("run_id"),
            "expected_revision": task["revision"],
            "recover": False}, key="auto-start-" + call_id)
        task = started  # the start response carries the saved task (fresh revision)
        execution = task.get("execution") or {}
    if config.get("run_id") and execution.get("run_id") != config["run_id"]:
        raise RuntimeError("Auto-checkpoint: execution not bound to this run")
    client.checkpoint_task(task_id, {
        "expected_revision": task["revision"], "run_id": execution["run_id"],
        "execution_id": execution["id"], "checkpoint_id": uid("ckp"),
        "status": "running", "progress": max(int(execution.get("progress") or 0), 1),
        "summary": f"Auto-checkpoint after scoped write ({call_id}): "
                   f"{', '.join(paths or []) or 'no observed files'}",
    }, key="auto-" + call_id)


def _invoke(config, payload):
    client = Client(config["base_url"], config["token"])
    runtime = config.get("runtime", "droid")
    session_id = payload.get("session_id", "default")
    # A separate session mapping keeps multiple runs independent.
    state_dir = Path(config["state_dir"]).resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    import hashlib
    session_key = hashlib.sha256(session_id.encode()).hexdigest()
    mapping = state_dir / f"{session_key}.json"
    if mapping.exists():
        state = json.loads(mapping.read_text())
        state.setdefault("session_id", session_id)
        # A mapping without a persisted claim (claim POST failed mid-session earlier)
        # retries claim creation on the next invocation instead of staying claimless.
        if not state.get("claim_id") and state.get("scope_ids") and not config.get("managed_run_id"):
            state["claim_id"] = client.request(
                "POST", "work-claims", {
                    "project_id": config["project_id"], "agent_id": config["agent_id"],
                    "run_id": state["run_id"], "scope_ids": state["scope_ids"],
                    "intent": config.get("intent", "Instrumented agent work"),
                    "ttl_seconds": config.get("initial_claim_ttl_seconds", 120),
                }, key="initial-claim-" + session_key)["id"]
            mapping.write_text(json.dumps(state))
    else:
        if config.get("managed_run_id"):
            run = client.request("GET", f"runs/{config['managed_run_id']}")
            if (run["project_id"], run["agent_id"]) != (config["project_id"], config["agent_id"]):
                raise RuntimeError("Managed runtime identity mismatch")
        else:
            run = client.request("POST", "runs", {"project_id": config["project_id"], "agent_id": config["agent_id"]},
                                 key="session-" + session_key)
        state = {"run_id": run["id"], "session_id": session_id, "scope_ids": config.get("scope_ids", [])}
        # The claim must exist before the mapping does: a mapping written first sticks
        # forever claimless when the claim POST fails (session reruns skip creation).
        if state["scope_ids"] and not config.get("managed_run_id"):
            claim = client.request("POST", "work-claims", {
                "project_id": config["project_id"], "agent_id": config["agent_id"], "run_id": run["id"],
                "scope_ids": state["scope_ids"], "intent": config.get("intent", "Instrumented agent work"),
                "ttl_seconds": config.get("initial_claim_ttl_seconds", 120),
            }, key="initial-claim-" + session_key)
            state["claim_id"] = claim["id"]
        mapping.write_text(json.dumps(state))
    run_id = state["run_id"]
    if config.get("managed_run_id"):
        if run_id != config["managed_run_id"]:
            raise RuntimeError("Managed run changed")
        task = client.task(config["managed_task_id"])
        if ((task.get("execution") or {}).get("id") != config["managed_execution_id"]
                or (task.get("execution") or {}).get("run_id") != run_id
                or task["status"] not in ("running", "paused")):
            raise RuntimeError("Managed execution is stale")
        state["scope_ids"] = task["scope_ids"]
    adapter = ControlledAdapter(client, config["project_id"], config["agent_id"], run_id,
                                 state_dir / f"{session_key}-controls.json")
    if not adapter.state["scope_ids"]:
        adapter.state["scope_ids"] = state["scope_ids"]
        adapter.persist()
    if config.get("managed_run_id"):
        adapter.state.update(task_id=config["managed_task_id"], scope_ids=state["scope_ids"])
        adapter.persist()
    elif config.get("provisioned_task_id") and not adapter.state.get("task_id"):
        # Provisioned (non-managed) worker sessions still know their assigned task.
        adapter.state["task_id"] = config["provisioned_task_id"]
        adapter.persist()
    event = payload.get("hook_event_name", payload.get("type"))
    name = payload.get("tool_name", payload.get("toolName", ""))
    tool_input = payload.get("tool_input", payload.get("input", {}))
    # Opt-in governed tool gateway: applies the trusted operator GatewayPolicy
    # before any tool executes. Strictness comes only from the config file,
    # never from the payload. Existing checks below are preserved unchanged.
    gateway_config = config.get("gateway")
    if gateway_config is not None:
        if not isinstance(gateway_config, dict) or type(gateway_config.get("enabled")) is not bool:
            raise RuntimeError("Gateway configuration requires an explicit boolean enabled flag")
        if gateway_config["enabled"] and event in ("PreToolUse", "tool_call"):
            from .control_capture import GatewayPolicy, ToolGateway
            allowed_tools = gateway_config.get("allowed_tools")
            policy = GatewayPolicy(
                strict=gateway_config.get("strict", True) is True,
                allowed_tools=frozenset(allowed_tools) if allowed_tools is not None else None,
                allowed_namespaces=frozenset(gateway_config.get(
                    "allowed_namespaces", ["filesystem", "network", "discovery"])),
                allowed_mcp_tools=tuple(config.get("allowed_mcp_tools", [])),
                restrict_reads_to_workspace=config.get("restrict_reads_to_workspace",
                                                       True) is True)
            decision = ToolGateway(policy, workspace_root=config["workspace_root"]).evaluate(
                name, tool_input, calls_made=state.get("tool_call_count", 0))
            if not decision.allowed:
                client.close()
                raise RuntimeError("Gateway policy denied this tool request")
    call_key = digest({"name": name, "input": tool_input})
    explicit_call = payload.get("toolCallId") or payload.get("tool_use_id")
    if event in ("PreToolUse", "tool_call"):
        call_id = explicit_call or uid("call")
        state.setdefault("pending_tool_ids", {})[call_key] = call_id
    else:
        call_id = explicit_call or state.get("pending_tool_ids", {}).pop(call_key, None) or uid("partial")
    if event in ("PreToolUse", "tool_call") and config.get("max_tool_calls") is not None:
        state["tool_call_count"] = state.get("tool_call_count", 0) + 1
        mapping.write_text(json.dumps(state))
        if state["tool_call_count"] > config["max_tool_calls"]:
            client.close()
            raise RuntimeError("Live integration tool-call budget exhausted")
    mutating = name.lower() not in ("read", "grep", "glob", "ls", "websearch", "fetchurl")
    approved_mcp = name in config.get("allowed_mcp_tools", [])
    approved_discovery = name == "ToolSearch" and config.get("allow_tool_discovery", False)
    allowed_names = config.get("allowed_tool_names")
    if event in ("PreToolUse", "tool_call") and allowed_names is not None and (
            name not in allowed_names and not approved_mcp):
        client.close()
        raise RuntimeError("Tool is not in the administrator-configured allowlist")
    if approved_mcp:
        # Explicit administrator-configured tool IDs only. API identity/policy still applies.
        mutating = False
    if approved_discovery:
        # Loading a schema does not authorize execution; every discovered tool
        # still goes through the exact tool allowlist and normal host approvals.
        mutating = False
    output = {}
    if event in ("PreToolUse", "tool_call"):
        if not mutating and not approved_mcp and name.lower() in ("read", "grep", "glob", "ls"):
            check_read_scope(config, name, tool_input)
        if state.get("calls"):
            client.close()
            raise RuntimeError("An earlier controlled edit has not reached its completion boundary")
        previous_task = adapter.state.get("task_id")
        if not config.get("managed_run_id"):
            adapter.boundary()
        if adapter.state.get("task_id") != previous_task and mutating:
            client.close()
            raise RuntimeError("Assignment changed; obtain new task context before retrying this tool")
        # Only typed file edits and bounded patch headers are enforceable here.
        if mutating:
            root = Path(config["workspace_root"]).resolve()
            observations = []
            for path in mutation_paths(name, tool_input):
                target = Path(path)
                target = (root / target).resolve() if not target.is_absolute() else target.resolve()
                relative = target.relative_to(root)
                if excluded(relative):
                    client.close()
                    raise RuntimeError("Path excluded from capture and controlled edits")
                observations.append({"path": relative.as_posix(), "before": file_hash(target)})
            paths = [item["path"] for item in observations]
            if config.get("managed_run_id"):
                client.request("POST", f"runs/{run_id}/permits:check",
                               {"scope_ids": state["scope_ids"], "paths": paths})
            else:
                adapter.permit(paths)
            state.setdefault("calls", {})[call_id] = {"files": observations, "name": name}
            mapping.write_text(json.dumps(state))
        context = adapter.state
        if context.get("cancelled"):
            raise RuntimeError("Run cancelled")
        output_context = None
        if context.get("task_id"):
            from .context_assembly import session_context_note
            output_context = session_context_note(
                client, config, request=({
                    "project_id": config["project_id"], "agent_id": config["agent_id"],
                    "run_id": run_id, "task_id": context["task_id"]}),
                task_id=context["task_id"], state=state)
        elif config.get("session_context"):
            from .context_assembly import session_context_note
            output_context = session_context_note(
                client, config, request=({
                    "project_id": config["project_id"], "agent_id": config["agent_id"],
                    "run_id": run_id, "task_id": None}),
                task_id=context.get("task_id"), state=state)
        if output_context:
            output = {"additionalContext": output_context}
    capture = Capture(client, config["project_id"], config["agent_id"], run_id,
                      state_dir / f"{session_key}-spool.sqlite", start=False)
    try:
        if event in ("PreToolUse", "tool_call"):
            capture.observe("tool.started", {"call_id": call_id, "name": name}, attribution="confirmed")
        elif event in ("PostToolUse", "tool_result"):
            # Completion is observed; success is unknown when the host exposes no reliable flag.
            capture.observe("tool.completed", {"call_id": call_id,
                "success": not payload["isError"] if runtime == "oh-my-pi" and "isError" in payload else None},
                attribution="confirmed")
            previous = state.get("calls", {}).pop(call_id, None)
            if previous:
                root = Path(config["workspace_root"]).resolve()
                changed = False
                changed_paths = []
                for file in previous.get("files", []):
                    target = (root / file["path"]).resolve()
                    target.relative_to(root)  # Recheck symlink containment after the tool.
                    after = file_hash(target)
                    if after != file["before"]:
                        changed = True
                        changed_paths.append(file["path"])
                        change = "created" if file["before"] is None else "deleted" if after is None else "modified"
                        capture.observe("file.changed", {"path": file["path"], "change": change},
                                        attribution="confirmed")
                if changed or runtime == "oh-my-pi" and payload.get("isError") is False:
                    adapter.confirm_transition()
                    # R5: adapter-side automatic checkpoint. A confirmed scoped write on
                    # a run with a bound task execution files a self-reported checkpoint
                    # (status running; never an independent verification claim).
                    if config.get("auto_checkpoint") is True and adapter.state.get("task_id"):
                        try:
                            checkpoint_task(client, config, adapter.state["task_id"],
                                            changed_paths, call_id, run_id=run_id)
                        except Exception as checkpoint_error:  # noqa: BLE001
                            state["last_checkpoint_error"] = repr(checkpoint_error)[:200]
                mapping.write_text(json.dumps(state))
        elif event in ("SessionStart", "session_start"):
            capture.observe("agent.status_reported", {"text": f"{runtime} runtime connected"}, critical=True)
        # Periodic drain daemon handles the five-second cadence. Hook invocations only spool.
    finally:
        mapping.write_text(json.dumps(state))
        capture.db.close()
        client.close()
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=os.environ.get("HIVEMIND_CONFIG"))
    args = parser.parse_args()
    if not args.config:
        parser.error("--config or HIVEMIND_CONFIG is required")
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        from filelock import FileLock
        payload = json.load(sys.stdin)
        session = payload.get("session_id", "default")
        key = hashlib.sha256(session.encode()).hexdigest()
        state_dir = Path(config["state_dir"])
        state_dir.mkdir(parents=True, exist_ok=True)
        with FileLock(str(state_dir / f"{key}.lock"), timeout=10):
            output = invoke(config, payload)
        if output:
            if config.get("runtime", "droid") == "droid" and payload.get("hook_event_name") == "PreToolUse":
                output = {"hookSpecificOutput": {"hookEventName": "PreToolUse",
                                                 "additionalContext": output.get("additionalContext", "")}}
            print(json.dumps(output))
        return 0
    except Exception:
        # Fail closed without printing raw credential-bearing requests or local paths.
        print("Hivemind could not validate this operation. Check service, claim, and adapter health.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
