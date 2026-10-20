"""Current offline full-chain demonstration. Run with pytest, never production data.

The canonical API is real (in-process ASGI transport); A2A uses loopback HTTP,
ACP uses a real stdio child, and trusted verification runs a separate subprocess.
Peers are deterministic fixtures, not live vendors or independent human judges.
"""
import hashlib
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import World
from hivemind.agent_session import JournalClient
from hivemind.agent_worker import AgentWorker
from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.indexes import Gemma
from hivemind.protocol_runtimes import A2ARuntime, ACPRuntime
from hivemind.recovery import Journal, UnknownOutcome
from hivemind.sdk import Client
from hivemind.storage import now, uid
from hivemind.verification import TrustedVerifier


ACP_PEER = '''
import json, sys
from pathlib import Path
def send(value):
    print(json.dumps(value), flush=True)
for line in sys.stdin:
    frame = json.loads(line)
    method, identifier = frame.get("method"), frame.get("id")
    if method == "initialize":
        result = {"protocolVersion": 1, "agentCapabilities": {"loadSession": False},
                  "agentInfo": {"name": "offline-review-peer", "version": "1"}, "authMethods": []}
    elif method == "session/new":
        result = {"sessionId": "offline-review-session"}
    elif method == "session/prompt":
        prompt = frame["params"]["prompt"][0]["text"]
        packet = json.loads(prompt.split("\\n", 1)[1])["context"]
        assert sys.argv[1] in {item["resource_id"] for item in packet["items"]}
        assert packet["token_count"] <= packet["token_budget"]
        assert Path("answer.py").read_text() == "VALUE = 42\\n"
        with Path("review.txt").open("x", newline="") as output:
            output.write("Reviewed VALUE = 42\\n")
        send({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": "offline-review-session", "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "Local output inspected"}}}})
        result = {"stopReason": "end_turn"}
    else:
        raise RuntimeError("Unexpected ACP method")
    send({"jsonrpc": "2.0", "id": identifier, "result": result})
'''

CHECK = '''
from pathlib import Path
assert Path("answer.py").read_bytes() == b"VALUE = 42\\n"
assert Path("review.txt").read_bytes() == b"Reviewed VALUE = 42\\n"
print("Two artifacts match the captured requirement")
'''


def sha(data):
    return hashlib.sha256(data).hexdigest()


def sdk(world, token):
    def forward(request):
        response = world.client.request(request.method, request.url.raw_path.decode(),
                                        content=request.content, headers=request.headers)
        return httpx.Response(response.status_code, content=response.content, headers=response.headers)
    return Client("http://testserver", token, transport=httpx.MockTransport(forward))


@pytest.fixture
def build_peer(tmp_path):
    """One local A2A peer, with exactly-once exclusive output creation."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, body):
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            assert self.path == "/.well-known/agent-card.json"
            self.reply({"name": "offline-build-peer", "version": "1",
                        "protocolVersion": "1.0", "capabilities": {"streaming": False},
                        "defaultInputModes": ["text/plain"], "defaultOutputModes": ["text/plain"],
                        "skills": [], "supportedInterfaces": [{
                            "url": f"http://127.0.0.1:{self.server.server_port}/a2a",
                            "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}]})

        def do_POST(self):
            assert self.path == "/a2a"
            length = int(self.headers["Content-Length"])
            assert 0 < length <= 1024 * 1024
            frame = json.loads(self.rfile.read(length))
            self.server.methods.append(frame["method"])
            if frame["method"] == "SendMessage":
                prompt = frame["params"]["message"]["parts"][0]["text"]
                packet = json.loads(prompt.split("\n", 1)[1])["context"]
                assert self.server.memory_id in {item["resource_id"] for item in packet["items"]}
                assert packet["token_count"] <= packet["token_budget"]
                self.server.context_tokens = packet["token_count"]
                assert "42" in prompt
                with (self.server.workspace / "answer.py").open("x", newline="") as output:
                    output.write("VALUE = 42\n")
                self.server.writes += 1
            else:
                assert frame["method"] == "GetTask"
            self.reply({"jsonrpc": "2.0", "id": frame["id"], "result": {"task": {
                "id": "offline-build-task", "contextId": "offline-build-context",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": "answer", "parts": [{"text": "Wrote answer.py"}]}]}}})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.methods, server.writes = [], 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(10)
        assert not thread.is_alive()


def runtime_options(root, workspace, project, agent, token, **extra):
    private = root / agent["id"]
    private.mkdir()
    config = private / "config.json"
    config.write_text(json.dumps({"project_id": project, "agent_id": agent["id"],
                                 "token": token, **extra.pop("config_extra", {})}))
    return {"workspace": str(workspace), "config": str(config),
            "state_file": str(private / "runtime.json"), "enabled": True,
            "allow_model_calls": True, "allow_execution": True, "timeout_seconds": 30, **extra}


def test_capture_memory_retrieval_assignment_execution_verification_recovery(tmp_path, build_peer):
    real_encoder = os.environ.get("HIVEMIND_DEMO_REAL_EMBEDDING") == "1"
    if real_encoder:
        assert os.environ.get("HF_HUB_OFFLINE") == "1"
        assert os.environ.get("TRANSFORMERS_OFFLINE") == "1"
    encoder = Gemma() if real_encoder else None
    directory, workspace = tmp_path / "canonical", tmp_path / "workspace"
    workspace.mkdir()
    config = json.loads(initialize(directory).read_text())
    journal_path = tmp_path / "worker-journal.sqlite"
    app = create_app(directory, encoder=encoder, workers=False)
    report = {"scenario": "offline-a2a-acp-full-chain-v1", "passed": False,
              "transport": {"canonical": "real ASGI service, in-process TestClient",
                            "A2A": "loopback HTTP v1.0", "ACP": "real stdio child, v1"},
              "encoder": "cached pinned Gemma" if real_encoder else "none; lexical retrieval only",
              "limits": ["deterministic peers, not live vendor sessions",
                         "reviewer is a distinct fixture identity, not an independent human",
                         "graceful application restart, not process crash or power loss",
                         "no OS sandbox assertion, UI or Nexus-control coverage",
                         "context counter is not a vendor target-tokenizer guarantee"]}
    with TestClient(app) as http:
        world = World(app, http, config)
        project = world.project
        builder = world.call("POST", "agents", {"project_id": project,
            "display_name": "Offline A2A builder", "capabilities": ["build"]})
        reviewer = world.call("POST", "agents", {"project_id": project,
            "display_name": "Offline ACP reviewer", "capabilities": ["review"]})
        bt, rt = world.credential(builder), world.credential(reviewer)
        br, rr = world.run(builder, bt), world.run(reviewer, rt)

        def capture(sequence, suffix, data):
            return world.call("POST", "events:batch", {"events": [{
                "event_id": uid("evt"), "type": "hivemind.context." + suffix,
                "project_id": project, "agent_id": builder["id"], "run_id": br["id"],
                "occurred_at": now(), "source": {"source_id": "offline-demo-tool",
                    "kind": "runtime_hook", "sequence": sequence, "attribution": "confirmed"},
                "data": data}]}, bt)[0]

        assert capture(0, "tool.started", {"call_id": "requirements-edit", "name": "edit"})["accepted"]
        requirement = b"The export answer must equal 42.\n"
        (workspace / "requirements.txt").write_bytes(requirement)
        assert capture(1, "file.changed", {"path": "requirements.txt", "change": "created"})["accepted"]
        assert capture(2, "tool.completed", {"call_id": "requirements-edit", "success": True})["accepted"]
        assert world.call("GET", f"runs/{br['id']}", token=bt)["tools"]["requirements-edit"]["state"] == "completed"
        post = world.post((workspace / "requirements.txt").read_text(), bt)
        source = world.call("POST", "artifacts", {"project_id": project,
            "title": "Captured requirements snapshot", "run_id": br["id"],
            "content": {"media_type": "text/plain", "text": requirement.decode()}}, bt)
        memory = world.call("POST", "memories", {"project_id": project, "title": "Export requirement",
            "content": {"media_type": "text/plain", "text": requirement.decode()},
            "scope_kind": "project", "scope_owner_id": project,
            "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": post["revision"]},
                              {"kind": "artifact", "id": source["id"], "revision": source["revision"]}]}, bt)
        memory = world.call("POST", f"memories/{memory['id']}:verify",
                            {"expected_revision": memory["revision"]})
        app.state.indexes.drain(1000)
        graph = world.call("POST", "graph:query", {"project_id": project, "seed_ids": [memory["id"]]})
        assert {memory["id"], post["id"], source["id"]} <= {node["id"] for node in graph["nodes"]}
        mode = "semantic" if real_encoder else "lexical"
        search = world.call("POST", "search", {"project_id": project,
            "query": "What number should the exported result contain?" if real_encoder else "export",
            "mode": mode})
        assert memory["id"] in {hit["resource"]["id"] for hit in search["results"]}
        packet = world.call("POST", "context:assemble", {"project_id": project,
            "agent_selector": {"agent_ids": [reviewer["id"]]}, "include": ["memory"],
            "query": "export", "token_budget": 6000}, rt)
        assert memory["id"] in {item["resource_id"] for item in packet["items"]}
        assert packet["token_count"] <= packet["token_budget"]
        report["capture_memory_retrieval"] = {"requirement_sha256": sha(requirement),
            "memory_id": memory["id"], "evidence_post_id": post["id"], "search_mode": mode,
            "evidence_artifact_id": source["id"],
            "graph_edge_kinds": sorted({edge["kind"] for edge in graph["edges"]}),
            "context_tokens": packet["token_count"], "context_budget": packet["token_budget"]}

        build_scope = world.scope("demo/build", paths=["answer.py"])
        review_scope = world.scope("demo/review", paths=["review.txt"])
        plan = world.call("POST", "task-plans", {"project_id": project, "title": "Offline export",
            "tasks": [{"key": "build", "title": "Build export answer", "description": "export requirement",
                       "required_capabilities": ["build"], "scope_ids": [build_scope["id"]]},
                      {"key": "review", "title": "Review export answer", "description": "export requirement",
                       "required_capabilities": ["review"], "scope_ids": [review_scope["id"]],
                       "depends_on": ["build"]}]})
        build_peer.workspace, build_peer.memory_id = workspace, memory["id"]
        acp_script = tmp_path / "offline_acp_peer.py"
        acp_script.write_text(ACP_PEER)
        aopts = runtime_options(tmp_path, workspace, project, builder, bt, config_extra={
            "agent_url": f"http://127.0.0.1:{build_peer.server_port}/a2a"})
        copts = runtime_options(tmp_path, workspace, project, reviewer, rt,
                               argv=[sys.executable, str(acp_script), memory["id"]])
        a2a, acp = A2ARuntime(aopts), ACPRuntime(copts)
        bc, rc = sdk(world, bt), sdk(world, rt)
        try:
            bw = AgentWorker(bc, project, builder["id"], br["id"], a2a, enabled=True,
                             assemble_context=True, submit_verification=True)
            rw = AgentWorker(rc, project, reviewer["id"], rr["id"], acp, enabled=True,
                             assemble_context=True)
            assert rw.tick()["task_id"] is None  # Dependency is not ready.
            assert bw.tick()["status"] == "completed"
            assert rw.tick()["status"] == "completed"
            process = acp.transport.process
            tasks = [world.call("GET", f"tasks/{task['id']}") for task in plan["tasks"]]
            assert all(task["status"] == "completed" for task in tasks)
            assert [task["execution"]["run_id"] for task in tasks] == [br["id"], rr["id"]]
            assert all(task["scheduling"]["attempts"] == 1 for task in tasks)
            report["execution"] = {"tasks": [task["id"] for task in tasks],
                "runs": [br["id"], rr["id"]], "A2A_methods": list(build_peer.methods),
                "A2A_writes": build_peer.writes, "ACP_session": acp.state["session_id"]}
            report["execution"]["A2A_context_tokens"] = build_peer.context_tokens
        finally:
            a2a.close()
            acp.close()
            bc.close()
            rc.close()
        assert process.poll() is not None
        check_script = tmp_path / "operator_check.py"
        check_script.write_text(CHECK)
        app.state.trusted_verifier = TrustedVerifier(workspace, {
            "export": [sys.executable, str(check_script)]})
        verification = world.call("GET", f"verifications?project_id={project}")["verifications"][0]
        assert not verification["independently_verified"]
        verification = world.call("POST", f"verifications/{verification['id']}:assign-reviewer",
            {"expected_revision": verification["revision"], "reviewer_agent_id": reviewer["id"]}, bt)
        world.call("POST", f"verifications/{verification['id']}:decide", {
            "expected_revision": verification["revision"], "decision": "verified",
            "reason": "Self report alone is insufficient"}, rt, status=422)
        verification = world.call("POST", f"verifications/{verification['id']}:check", {
            "expected_revision": verification["revision"], "check_name": "export",
            "artifact_paths": ["answer.py", "review.txt"]})
        check = verification["checks"][-1]
        assert check["ok"] and check["objective"] and check["returncode"] == 0, check
        verified = world.call("POST", f"verifications/{verification['id']}:decide", {
            "expected_revision": verification["revision"], "decision": "verified",
            "reason": "Distinct ACP fixture identity reviewed output; real operator check passed"}, rt)
        assert verified["independently_verified"]
        report["verification"] = {"id": verified["id"], "status": verified["effective_status"],
            "trust": check["trust"], "returncode": check["returncode"],
            "artifacts": check["artifacts"], "stdout_sha256": check["stdout_sha256"]}

        # Lose a task-create response AFTER the real service has committed it.
        client = sdk(world, rt)
        original = client.request
        def lose_response(method, path, body=None, key=None):
            result = original(method, path, body, key=key)
            if method == "POST" and path == "tasks":
                raise httpx.ReadError("Deliberately lost committed response")
            return result
        client.request = lose_response
        try:
            with Journal(journal_path) as journal:
                with pytest.raises(httpx.ReadError):
                    JournalClient(client, journal).request("POST", "tasks", {
                        "project_id": project, "title": "Post-verification recovery followup"},
                        key="offline-demo-followup")
        finally:
            client.close()
        before = world.call("GET", f"tasks?project_id={project}")
        assert len(before) == 3

    # A new application opens the SAME canonical directory and side ledgers.
    reopened = create_app(directory, encoder=encoder, workers=False)
    with TestClient(reopened) as http:
        world = World(reopened, http, config)
        client = sdk(world, rt)
        calls = []
        request = client.request
        def observed(method, path, body=None, key=None):
            calls.append((method, path))
            return request(method, path, body, key=key)
        client.request = observed
        try:
            with Journal(journal_path) as journal:
                wrapped = JournalClient(client, journal)
                recovery = wrapped.recover_task_creates(enabled=True)
                assert not recovery["held"] and len(recovery["recovered"]) == 1
                entry = recovery["recovered"][0]
                assert entry["outcome"] == "proved_applied"
                assert journal.operation(entry["operation_id"])["state"] == "completed"
                assert wrapped.recover_task_creates(enabled=True)["recovered"] == []
        finally:
            client.close()
        assert all(method == "GET" for method, _ in calls)
        assert world.call("GET", f"tasks?project_id={project}") == before
        assert world.call("GET", f"verifications/{verified['id']}")["effective_status"] == "verified"
        assert world.call("GET", f"memories/{memory['id']}")["status"] == "verified"
        reopened.state.indexes.drain(1000)
        found = world.call("POST", "search", {"project_id": project, "query": "export", "mode": "lexical"})
        assert memory["id"] in {hit["resource"]["id"] for hit in found["results"]}
        # A fresh adapter sees completed remote work and refuses to replay it.
        recovered_adapter = A2ARuntime(aopts)
        try:
            with pytest.raises(UnknownOutcome):
                recovered_adapter.execute(tasks[0], {"run_id": br["id"]}, None)
        finally:
            recovered_adapter.close()
        assert build_peer.writes == 1
        report["recovery"] = {"application_reopened": True, "task_count": len(before),
            "receipt_outcome": entry["outcome"], "recovery_used_GET_only": True,
            "second_sweep_noop": True, "completed_remote_replay_refused": True,
            "A2A_writes_after_restart": build_peer.writes, "ACP_child_exited": True}
    report["passed"] = True
    root = Path(__file__).resolve().parents[1]
    sources = [Path(__file__), *(root / "hivemind" / name for name in (
        "api.py", "agent_worker.py", "protocol_runtimes.py", "indexes.py", "storage.py",
        "verification.py", "agent_session.py", "recovery_automation.py"))]
    report["source_sha256"] = {str(path.relative_to(root)).replace("\\", "/"): sha(path.read_bytes())
                               for path in sources}
    output = os.environ.get("HIVEMIND_DEMO_REPORT")
    if output:
        with Path(output).open("x", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
