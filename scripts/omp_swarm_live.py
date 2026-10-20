#!/usr/bin/env python3
"""Real OMP headless agent swarm building an example codebase through Hivemind.

Reference run: `.memory/agent-swarm-live-test.md`. Requires:
- A running Hivemind service on 127.0.0.1:8765 (`hivemind serve --embeddings`)
- Bun + installed `omp` (headless `-p` mode) with a configured model
- A configured model provider; each agent runs real model turns

The swarm builds a tiny TODO REST service in three deliberately interlocking
parts, dispatched as Hivemind tasks, with the capture extension observing every
tool call. Each agent can send direct messages and read replies through its
inbox (the durable messaging surface); see the reference recipe in
`.memory/agent-swarm-live-test.md`.

Run:  .venv/Scripts/python.exe scripts/omp_swarm_live.py --workspace .hivemind-swarm
"""
from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# The example project: deliberately interlocking parts so agents need each
# other's progress. Agent assignment (task -> agent) matches AgentTask.
WORK = {
    ("Builder", "src/store.js"): (
        "Implement src/store.js: load() reads todos from data.json (empty array if missing); "
        "save(todos) writes it atomically (write temp file then rename). Export {load, save} "
        "using plain node:fs promises. Nothing else."),
    ("Builder", "src/validate.js"): (
        "Implement src/validate.js: validateTodo(todo) returns {ok, errors} — a todo needs a "
        "non-empty string title and an optional boolean done (default false). Export "
        "{validateTodo}. Nothing else."),
    ("Backend", "src/validate.js"): (
        # Read-only review turn: Builder owns validate.js; Backend audits it before
        # writing app.js (also exercises two-scope claims inside one agent session).
        "Read src/validate.js, then reply with validateTodo(todo)'s exact behavior for a "
        "todo missing its title. Do not modify files."),
    ("Backend", "src/app.js"): (
        "Implement src/app.js: an Express app exposing GET /todos, POST /todos, "
        "PATCH /todos/:id and DELETE /todos/:id backed by data.json. Import {load, save} from "
        "'./store.js' and {validateTodo} from './validate.js'. 404 for unknown ids, 400 for "
        "invalid todos. Export the app as default. IMPORTANT: read src/store.js and "
        "src/validate.js first; they exist already."),
    ("Documenter", "README.md"): (
        "Write README.md for this repo: one paragraph describing the TODO REST service, how to "
        "run it (npm install express && node src/app.js), and the four endpoints with example "
        "curl lines. IMPORTANT: read src/app.js, src/store.js and src/validate.js first so the "
        "documented behavior matches the code."),
}


def private_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def omp_environment(config_path: Path) -> dict:
    import os
    return {**os.environ, "HIVEMIND_CONFIG": str(config_path),
            "HIVEMIND_PYTHON": str(Path(sys.executable))}


def run_agent_turn(name: str, prompt: str, workspace: Path, env: dict,
                   model: str, timeout: int) -> dict:
    started = time.monotonic()
    result = subprocess.run(
        ["omp", "-p", "--no-extensions", "-e",
         str(ROOT / "adapters" / "oh-my-pi" / "hivemind.ts"),
         "--model", model, prompt],
        cwd=workspace, env=env, capture_output=True, text=True, timeout=timeout)
    return {"agent": name, "exit_code": result.returncode,
            "seconds": round(time.monotonic() - started, 1),
            "output_tail": (result.stdout or "")[-400:],
            "error_type": None if result.returncode == 0 else (result.stderr or "")[-300:]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default=".hivemind-swarm")
    parser.add_argument("--service", default="http://127.0.0.1:8765")
    parser.add_argument("--data-dir", default=".hivemind-swarm-data",
                        help="the running service's data directory (must be initialized)")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--model", default="ollama-cloud/glm-5.3-flash:low")
    parser.add_argument("--skip-messages", action="store_true",
                        help="skip the direct-message exchange stage")
    parser.add_argument("--dry-run", action="store_true",
                        help="provision identities/scopes/tasks; no model turns")
    args = parser.parse_args()

    root = Path(args.workspace).resolve()
    data_dir = Path(args.data_dir or ".hivemind-swarm-data").resolve()
    if not (data_dir / "owner.json").exists():
        raise SystemExit(f"Data directory {data_dir} is not initialized: run "
                         "`hivemind --data-dir " + str(data_dir) + " init`, start the "
                         "service on the same directory (`hivemind --data-dir " + str(data_dir) +
                         " serve`), then rerun.")
    private_root = root / "agent-private"
    workspace = root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    from hivemind.api import hash_token
    from hivemind.sdk import Client
    from hivemind.storage import Store, uid

    owner = json.loads((data_dir / "owner.json").read_text())
    owner_client = Client(args.service, owner["token"])
    project = owner["project_id"]
    print(f"project {project}")

    # --- identities and credentials ------------------------------------------
    agents = {}
    agents_list = owner_client.request("GET", f"agents?project_id={project}")
    for name in ("Builder", "Backend", "Documenter"):
        if not any(a["display_name"] == name for a in (agents_list or [])):
            agent = owner_client.request("POST", "agents",
                                         {"project_id": project, "display_name": name})
        else:
            agent = next(a for a in (agents_list or [])
                         if a["display_name"] == name)
        store = Store(str(data_dir))
        token = secrets.token_urlsafe(32)
        with store.transaction():
            store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                             (uid("principal"), hash_token(token), "agent", project,
                              agent["id"], 0, 1))
        config = private_root / name / "credential.json"
        intent = {"Builder": "Implement store module", "Backend": "Implement app",
                  "Documenter": "Write README"}[name]
        private_write(config, {"base_url": args.service, "token": token,
                               "project_id": project, "agent_id": agent["id"],
                               "runtime": "oh-my-pi",
                               "workspace_root": str(workspace),
                               "state_dir": str(private_root / name / "state"),
                               "scope_ids": [],  # set per assignment below
                               "intent": intent,
                               "initial_claim_ttl_seconds": 600,
                               "flush_interval_seconds": 5,
                               "session_context": {"token_budget": 3200},
                               "auto_checkpoint": True})
        agents[name] = {"agent": agent, "token": token, "config": config}
        print(f"agent {name}: {agent['id']}")
    store.close()

    # --- scopes + claims ------------------------------------------------------
    store = Store(str(data_dir))

    def scope_call(principal_token, body):
        response = owner_client.http.post("scopes", json=body,
                                          headers={"Authorization": "Bearer " + principal_token,
                                                   "Idempotency-Key": uid("key")})
        if response.status_code == 409 and response.json()["error"]["code"] == "scope_key_exists":
            # Idempotent rerun: reuse the existing canonical scope with the same key.
            from hivemind.storage import Store
            lookup = Store(str(data_dir))
            rows = lookup.db.execute(
                "SELECT id FROM resources WHERE kind='scope' AND project=? AND deleted=0",
                (project,)).fetchall()
            for (resource_id,) in rows:
                item = lookup.get(resource_id, {"role": "admin", "project": project}, "scope")
                if item["canonical_key"] == body["canonical_key"]:
                    return item
            raise RuntimeError(f"scope {body['canonical_key']} exists but is not visible")
        return response.json()["data"]

    scopes = {}
    for key, paths in (("code/store", ["src/store.js"]),
                       ("code/validate", ["src/validate.js"]),
                       ("code/app", ["src/app.js"]),
                       ("docs", ["README.md"])):
        scopes[key] = scope_call(owner["token"],
                                 {"project_id": project, "canonical_key": key,
                                  "display_name": key, "allowed_paths": paths})

    # --- task plan + dispatch -------------------------------------------------
    # Backend claims validate.js only to READ?? No: reads need no claim. Give Backend
    # exactly app scope; claim overlap denial happens only on exclusive writes.
    assignment = {"Builder": ["code/store", "code/validate"],
                  "Backend": ["code/app"],
                  "Documenter": ["docs"]}
    for name, payload in agents.items():
        config = json.loads(payload["config"].read_text(encoding="utf-8"))
        config["scope_ids"] = [scopes[key]["id"] for key in assignment[name]]
        private_write(payload["config"], config)

    task_specs = [
        ("Build store module", "code/store", "Builder", "src/store.js"),
        ("Build validator", "code/validate", "Backend", "src/validate.js"),
        ("Build app", "code/app", "Backend", "src/app.js"),
        ("Write README", "docs", "Documenter", "README.md"),
    ]
    tasks = {}
    for title, scope_key, _agent_name, _path in task_specs:
        task = owner_client.request("POST", "tasks", {
            "project_id": project, "title": title, "scope_ids": [scopes[scope_key]["id"]]})
        tasks[title] = task
    print(json.dumps({"tasks": {k: v["id"] for k, v in tasks.items()}}, indent=1))

    # Bind each agent's primary task: the hooks session passes it to the session note
    # (R1/R2/R3/R8) and enables adapter-side auto-checkpointing (R5).
    primary_task = {"Builder": "Build store module", "Backend": "Build app",
                    "Documenter": "Write README"}
    for name, title in primary_task.items():
        payload = agents[name]
        config = json.loads(payload["config"].read_text(encoding="utf-8"))
        config["provisioned_task_id"] = tasks[title]["id"]
        private_write(payload["config"], config)

    if args.dry_run:
        return 0

    # --- capture daemon per agent (drain spools every 5s) ---------------------
    daemons = []
    try:
        for _name, payload in agents.items():
            process = subprocess.Popen(
                [sys.executable, "-m", "hivemind.capture_daemon", "--config",
                 str(payload["config"])],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            daemons.append(process)
        print(f"capture daemons: {len(daemons)}")

        # --- model turns, sequential: dependency order (store → validate → app → README).
        # Parallel dispatch without dependency gating burned turns in the first run:
        # Backend's app.js turn started before its upstream modules existed and the
        # Documenter read an empty workspace. A real dependency-gated dispatch would
        # order these; see .memory/agent-swarm-live-test.md findings.
        turns = []
        for (agent_name, _target), prompt in WORK.items():
            payload = agents[agent_name]
            env = omp_environment(payload["config"])
            turns.append(run_agent_turn(agent_name, prompt, workspace, env,
                                        payload.get("model", args.model), args.timeout))
        for turn in turns:
            print(json.dumps(turn, indent=1)[:500])
    finally:
        for process in daemons:
            process.terminate()

    # --- review: capture the evidence Hivemind accumulated --------------------
    review = {
        "agents": {name: owner_client.request("GET", "agents/" + payload["agent"]["id"])
                   for name, payload in agents.items()},
        "conflicts": owner_client.request("GET", f"conflicts?project_id={project}"),
        "ledger": owner_client.request("GET", f"ledger?project_id={project}"),
    }
    report = root / "swarm-report.json"
    report.write_text(json.dumps(review, indent=2, default=str), encoding="utf-8")
    print(f"report: {report}")
    return 0 if all(turn["exit_code"] == 0 for turn in turns) else 1


if __name__ == "__main__":
    raise SystemExit(main())