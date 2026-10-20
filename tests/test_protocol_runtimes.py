"""Offline loopback tests for the A2A v1.0 and ACP v1 protocol runtimes.

A2A fixtures use a local HTTP server emitting canonical v1.0 shapes (agent card,
JSON-RPC SendMessage/GetTask/CancelTask, SSE SendStreamingMessage). ACP fixtures use a
controlled stdio fixture agent created by this test file only. No network beyond
loopback, no paid calls, no installs, no host changes.
"""
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import hivemind.protocol_runtimes as protocol_runtimes
from hivemind.protocol_runtimes import (A2ARuntime, ACPRuntime, a2a_runtime, acp_runtime,
                                        protocol_runtime_descriptors)
from hivemind.recovery import UnknownOutcome
from hivemind.storage import digest

RESULT_ENVELOPE = json.dumps({"handled": True, "reply_text": "done"})


def options(tmp_path, **overrides):
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    config = tmp_path / "private" / "agent.json"
    config.parent.mkdir(exist_ok=True)
    config.write_text(json.dumps({"project_id": "project", "agent_id": "agent",
                                  "token": "fixture-token-secret"}))
    base = {"workspace": str(workspace), "config": str(config),
            "state_file": str(config.parent / "protocol-runtime.json"),
            "timeout_seconds": 15, "enabled": True, "allow_model_calls": True,
            "allow_execution": True, "result_contract": "message"}
    base.update(overrides)
    return base


def assignment():
    return ({"id": "task", "title": "Local task", "scope_ids": ["scope"]},
            {"project_id": "project", "agent_id": "agent", "run_id": "run",
             "execution_id": "execution"})


class A2AHandler(BaseHTTPRequestHandler):
    """Canonical A2A v1.0 JSON-RPC fixture; mode is selected through the prompt text."""
    token_seen = None
    card_version = "1.0"

    def log_message(self, *args):
        pass

    @property
    def server_state(self):
        return self.server.state

    def _send_json(self, body, status=200):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path != "/.well-known/agent-card.json":
            return self._send_json({}, 404)
        url = f"http://127.0.0.1:{self.server.server_port}{self.server.card_path}"
        interfaces = [{"url": url, "protocolBinding": self.server.card_binding,
                       "protocolVersion": self.card_version}]
        card = {"name": self.server.card_name, "version": "1.0.0",
                "protocolVersion": self.card_version, "url": url,
                "capabilities": {"streaming": self.server.streaming},
                "defaultInputModes": ["text/plain"], "defaultOutputModes": ["text/plain"],
                "skills": []}
        if self.server.card_interfaces is not None:
            card["supportedInterfaces"] = (interfaces if self.server.card_interfaces == "ok"
                                           else self.server.card_interfaces)
        self._send_json(card)

    def _task(self, state):
        task = {"id": "remote-task-1", "contextId": "ctx-1", "status": {"state": state}}
        if state == "TASK_STATE_COMPLETED":
            if self.server.big_artifacts:
                task["artifacts"] = [{"artifactId": f"a{i}", "parts": [{"text": "x" * 40}]}
                                     for i in range(3)]
            else:
                task["artifacts"] = [{"artifactId": "a1",
                                      "parts": [{"text": self.server.result_text}]}]
        return task

    def do_POST(self):
        if self.path == "/a2a-redirect":
            self.send_response(302)
            self.send_header("Location", "/a2a-second")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/a2a-second":
            # Adversarial catcher: proves no credential or body ever reaches a second hop.
            self.server.state["second_hop_auth"] = self.headers.get("Authorization")
            self._send_json({})
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length > 1048576:
            return self._send_json({}, 413)
        frame = json.loads(self.rfile.read(length) or b"{}")
        type(self).token_seen = self.headers.get("Authorization")
        assert self.headers.get("A2A-Version") == "1.0"
        identifier, method = frame.get("id"), frame.get("method")
        params = frame.get("params") or {}

        if method == "SendMessage":
            text = params["message"]["parts"][0]["text"]
            assert params["message"]["role"] == "ROLE_USER"
            assert params["configuration"]["returnImmediately"] is True
            if "RPCERROR" in text:
                return self._send_json({"jsonrpc": "2.0", "id": identifier, "error": {
                    "code": -32001, "message": "remote failure fixture-token-secret"}})
            if "HANG" in text:
                self.server.hang = True
                self.server_state["state"] = "TASK_STATE_WORKING"
                self.server_state["task"] = True
                return self._send_json({"jsonrpc": "2.0", "id": identifier,
                                        "result": {"task": self._task("TASK_STATE_WORKING")}})
            if "BIGART" in text:
                self.server.big_artifacts = True
                self.server_state["state"] = "TASK_STATE_COMPLETED"
                self.server_state["task"] = True
                return self._send_json({"jsonrpc": "2.0", "id": identifier,
                                        "result": {"task": self._task("TASK_STATE_COMPLETED")}})
            if "DIRECT" in text:
                return self._send_json({"jsonrpc": "2.0", "id": identifier, "result": {
                    "message": {"role": "ROLE_AGENT", "messageId": "m1",
                                "parts": [{"text": self.server.result_text}]}}})
            if "INTERRUPT" in text:
                self.server_state["state"] = "TASK_STATE_INPUT_REQUIRED"
            elif "MESSAGE" in text:
                self.server_state["state"] = "TASK_STATE_COMPLETED"
            else:
                self.server_state["state"] = "TASK_STATE_WORKING"
            self.server_state["task"] = True
            return self._send_json({"jsonrpc": "2.0", "id": identifier,
                                    "result": {"task": self._task(self.server_state["state"])}})

        if method == "SendStreamingMessage":
            self.server_state["task"] = True
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            text = params["message"]["parts"][0]["text"]
            if "FLOOD" in text:
                self.wfile.write(b'data: {"task": ' +
                                 json.dumps(self._task("TASK_STATE_WORKING")).encode() + b"\n\n")
                for _ in range(4000):
                    self.wfile.write(b'data: {"statusUpdate": {"taskId": "remote-task-1", '
                                     b'"contextId": "ctx-1", "status": {"state": '
                                     b'"TASK_STATE_WORKING"}}}\n\n')
                    self.wfile.flush()
                return
            frames = [{"task": self._task("TASK_STATE_WORKING")},
                      {"statusUpdate": {"taskId": "remote-task-1", "contextId": "ctx-1",
                                        "status": {"state": "TASK_STATE_COMPLETED"}, "final": True}}]
            self.server_state["state"] = "TASK_STATE_COMPLETED"
            for frame_out in frames:
                self.wfile.write(b"data: " + json.dumps(frame_out).encode() + b"\n\n")
                self.wfile.flush()
            return

        if method == "GetTask":
            assert params["id"] == "remote-task-1"
            if self.server.hang:
                time.sleep(4)
            return self._send_json({"jsonrpc": "2.0", "id": identifier,
                                    "result": {"task": self._task(self.server_state["state"])}})

        if method == "CancelTask":
            self.server_state["state"] = "TASK_STATE_CANCELED"
            return self._send_json({"jsonrpc": "2.0", "id": identifier,
                                    "result": {"task": self._task("TASK_STATE_CANCELED")}})

        self._send_json({"jsonrpc": "2.0", "id": identifier,
                         "error": {"code": -32601, "message": "Method not found"}})


@pytest.fixture
def a2a_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), A2AHandler)
    server.state = {"state": None, "task": False}
    server.streaming = False
    server.result_text = RESULT_ENVELOPE
    server.hang = False
    server.big_artifacts = False
    server.card_name = "fixture-agent"
    server.card_path = "/a2a"
    server.card_binding = "JSONRPC"
    server.card_interfaces = "ok"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join(10)
    A2AHandler.token_seen = None
    A2AHandler.card_version = "1.0"


def a2a_options(tmp_path, server, **overrides):
    base = options(tmp_path, **overrides)
    config = json.loads(open(base["config"]).read())
    config["agent_url"] = f"http://127.0.0.1:{server.server_port}/a2a"
    open(base["config"], "w").write(json.dumps(config))
    return base


def test_a2a_requires_three_explicit_opt_ins(tmp_path, a2a_server):
    task, context = assignment()
    for missing in ("enabled", "allow_model_calls", "allow_execution"):
        opts = a2a_options(tmp_path, a2a_server)
        opts[missing] = False
        runtime = A2ARuntime(opts)
        with pytest.raises(PermissionError):
            runtime.execute(task, context, None)
        assert not runtime.path.exists()
        runtime.close()
    assert a2a_server.state["task"] is False


def test_a2a_agent_card_send_gettask_structured_result_and_auth_not_persisted(tmp_path, a2a_server):
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server))
    task, context = assignment()
    task["title"] = "MESSAGE please"
    outcome = runtime.execute(task, context, None)
    assert outcome["status"] == "completed"
    assert outcome["message_result"] == {"handled": True, "reply_text": "done"}
    assert runtime.state["remote_task_id"] == "remote-task-1"
    assert runtime.state["remote_agent_sha256"] == digest("fixture-agent")
    assert runtime.state["project_id"] == "project" and runtime.state["agent_id"] == "agent"
    assert A2AHandler.token_seen == "Bearer fixture-token-secret"
    persisted = runtime.path.read_text()
    assert "fixture-token-secret" not in persisted and RESULT_ENVELOPE not in persisted
    checkpoint = json.loads(runtime.path.with_name(runtime.path.name + ".checkpoint.json").read_text())
    assert checkpoint["execution_id"] == "execution" and checkpoint["quiescent"] is True
    runtime.close()


def test_a2a_streaming_send_and_direct_message(tmp_path, a2a_server):
    a2a_server.streaming = True
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server))
    task, context = assignment()
    outcome = runtime.execute(task, context, None)
    assert outcome["status"] == "completed"
    assert outcome["message_result"]["handled"] is True
    runtime.close()
    resumed = A2ARuntime(a2a_options(tmp_path, a2a_server))
    task, context = assignment()
    task, context = {**task, "id": "task2", "title": "DIRECT"}, {**context, "execution_id": "e2"}
    outcome = resumed.execute(task, context, None)
    assert outcome["status"] == "completed" and outcome["message_result"]["reply_text"] == "done"
    resumed.close()


def test_a2a_cancel_observed_by_execute_thread(tmp_path, a2a_server):
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server, result_contract=None))
    task, context = assignment()
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(runtime.execute(task, context, None)))
    thread.start()
    for _ in range(400):
        if runtime.state.get("remote_task_id"):
            break
        time.sleep(.01)
    assert runtime.state.get("remote_task_id") == "remote-task-1"
    result = runtime.cancel({"id": "run"})
    thread.join(15)
    assert result["applied"] is True and result["observed_status"] == "cancelled"
    assert outcomes[0]["status"] == "failed"  # cancelled maps to a failed execution outcome
    assert runtime.state["outcome"] == "cancelled"
    runtime.close()


def test_a2a_pause_and_resume_are_denied_controls(tmp_path, a2a_server):
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server))
    for control in (runtime.pause, runtime.resume):
        result = control({"id": "run"})
        assert result["applied"] is False and result["detail"]["state"] == "unsupported_control"
    runtime.close()


def test_a2a_interrupted_remote_state_fails_closed_via_cancel(tmp_path, a2a_server):
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server, result_contract=None))
    task, context = assignment()
    task["title"] = "INTERRUPT"
    outcome = runtime.execute(task, context, None)
    assert outcome["status"] == "failed"
    assert runtime.state["outcome"] == "cancelled"
    assert a2a_server.state["state"] == "TASK_STATE_CANCELED"
    runtime.close()


def test_a2a_malformed_final_answer_fails_closed(tmp_path, a2a_server):
    a2a_server.result_text = "this is not the structured envelope"
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server))
    task, context = assignment()
    task["title"] = "MESSAGE"
    with pytest.raises(ValueError):
        runtime.execute(task, context, None)
    assert runtime.state["outcome"] == "unknown"
    runtime.close()


def test_a2a_restart_with_uncertain_outcome_blocks(tmp_path, a2a_server):
    opts = a2a_options(tmp_path, a2a_server)
    runtime = A2ARuntime(opts)
    runtime._save(outcome="running", run_id="run")
    with pytest.raises(UnknownOutcome):
        runtime.execute(*assignment(), None)
    runtime.close()
    runtime = A2ARuntime(opts)
    runtime._save(outcome="unknown", run_id="run")
    with pytest.raises(UnknownOutcome):
        runtime.execute(*assignment(), None)
    runtime.close()


def test_a2a_rejects_wrong_version_and_remote_plain_http(tmp_path, a2a_server):
    A2AHandler.card_version = "0.3"
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server))
    with pytest.raises(PermissionError):
        runtime.execute(*assignment(), None)
    runtime.close()
    A2AHandler.card_version = "1.0"
    opts = a2a_options(tmp_path, a2a_server,
                       state_file=str(tmp_path / "private" / "protocol-runtime-2.json"))
    config = json.loads(open(opts["config"]).read())
    config["agent_url"] = "http://198.51.100.7/a2a"  # Non-loopback plain HTTP is never approved.
    open(opts["config"], "w").write(json.dumps(config))
    runtime = A2ARuntime(opts)
    with pytest.raises(PermissionError):
        runtime.execute(*assignment(), None)
    runtime.close()


ACP_FIXTURE = r'''
import json, sys

version = int(sys.argv[1])

def send(body):
    sys.stdout.write(json.dumps(body) + "\n")
    sys.stdout.flush()

def read():
    line = sys.stdin.readline()
    if not line:
        sys.exit(3)
    return json.loads(line)

while True:
    frame = read()
    method, identifier = frame.get("method"), frame.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": identifier, "result": {
            "protocolVersion": version, "agentCapabilities": {"loadSession": False},
            "agentInfo": {"name": "fixture-acp", "version": "0.1"}, "authMethods": []}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": identifier, "result": {"sessionId": "sess-fixture"}})
    elif method == "session/prompt":
        text = frame["params"]["prompt"][0]["text"]
        session = frame["params"]["sessionId"]
        if "SLOW" in text:
            while True:
                note = read()
                if note.get("method") == "session/cancel":
                    break
            send({"jsonrpc": "2.0", "id": identifier, "result": {"stopReason": "cancelled"}})
            continue
        if "ASK" in text:
            send({"jsonrpc": "2.0", "id": "perm-1", "method": "session/request_permission",
                  "params": {"sessionId": session,
                             "toolCall": {"toolCallId": "t1", "title": "Run thing"},
                             "options": [{"optionId": "allow-1", "name": "Allow", "kind": "allow_once"},
                                         {"optionId": "reject-1", "name": "Reject", "kind": "reject_once"}]}})
            response = read()
            outcome = (response.get("result") or {}).get("outcome") or {}
            if outcome.get("outcome") != "selected" or outcome.get("optionId") != "reject-1":
                sys.exit(4)  # Permission was not failed closed.
            send({"jsonrpc": "2.0", "id": "fs-1", "method": "fs/read_text_file",
                  "params": {"sessionId": session, "path": "/etc/passwd"}})
            response = read()
            if (response.get("error") or {}).get("code") != -32601:
                sys.exit(5)  # Unadvertised fs capability was not refused.
        send({"jsonrpc": "2.0", "method": "session/update",
              "params": {"sessionId": session, "update": {"sessionUpdate": "agent_message_chunk",
                         "content": {"type": "text", "text": sys.argv[2]}}}})
        send({"jsonrpc": "2.0", "id": identifier, "result": {"stopReason": "end_turn"}})
    elif method:
        send({"jsonrpc": "2.0", "id": identifier, "error": {"code": -32601, "message": "no"}})
'''


@pytest.fixture
def acp_agent(tmp_path):
    script = tmp_path / "fixture_acp_agent.py"
    script.write_text(ACP_FIXTURE)
    return script


def acp_options(tmp_path, script, version=1, result_text=RESULT_ENVELOPE, **overrides):
    return options(tmp_path, argv=[sys.executable, str(script), str(version), result_text],
                   **overrides)


def test_acp_requires_explicit_opt_ins(tmp_path, acp_agent):
    opts = acp_options(tmp_path, acp_agent)
    opts["allow_execution"] = False
    runtime = ACPRuntime(opts)
    with pytest.raises(PermissionError):
        runtime.execute(*assignment(), None)
    assert runtime.transport is None and not runtime.path.exists()
    runtime.close()


def test_acp_initialize_prompt_exact_envelope_permission_denial_and_cleanup(tmp_path, acp_agent):
    runtime = ACPRuntime(acp_options(tmp_path, acp_agent))
    task, context = assignment()
    task["title"] = "ASK"
    outcome = runtime.execute(task, context, None)  # Fixture exits 4/5 unless denial/refusal happen.
    assert outcome["status"] == "completed"
    assert outcome["message_result"] == {"handled": True, "reply_text": "done"}
    assert runtime.state["session_id"] == "sess-fixture"
    assert "fixture-token-secret" not in runtime.path.read_text()
    process = runtime.transport.process
    runtime.close()
    assert process.poll() is not None  # Process group cleanup is confirmed.


def test_acp_version_mismatch_fails_closed(tmp_path, acp_agent):
    runtime = ACPRuntime(acp_options(tmp_path, acp_agent, version=2))
    with pytest.raises(PermissionError):
        runtime.execute(*assignment(), None)
    runtime.close()


def test_acp_cancel_and_pause_denied(tmp_path, acp_agent):
    runtime = ACPRuntime(acp_options(tmp_path, acp_agent, result_contract=None))
    task, context = assignment()
    task["title"] = "SLOW"
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(runtime.execute(task, context, None)))
    thread.start()
    for _ in range(400):
        if runtime._prompt_pending is not None:
            break
        time.sleep(.01)
    denied = runtime.pause({"id": "run"})
    assert denied["applied"] is False and denied["detail"]["state"] == "unsupported_control"
    result = runtime.cancel({"id": "run"})
    thread.join(15)
    assert result["applied"] is True and result["observed_status"] == "cancelled"
    assert outcomes[0]["status"] == "failed"
    runtime.close()


def test_acp_restart_with_uncertain_outcome_blocks(tmp_path, acp_agent):
    opts = acp_options(tmp_path, acp_agent)
    runtime = ACPRuntime(opts)
    runtime._save(outcome="unknown", run_id="run")
    with pytest.raises(UnknownOutcome):
        runtime.execute(*assignment(), None)
    runtime.close()


def test_acp_malformed_final_answer_fails_closed(tmp_path, acp_agent):
    runtime = ACPRuntime(acp_options(tmp_path, acp_agent, result_text="not an envelope"))
    with pytest.raises(UnknownOutcome):
        runtime.execute(*assignment(), None)
    assert runtime.state["outcome"] == "unknown"
    runtime.close()


def test_acp_completed_task_position_requires_reconciliation(tmp_path, acp_agent):
    opts = acp_options(tmp_path, acp_agent)
    runtime = ACPRuntime(opts)
    runtime.execute(*assignment(), None)
    with pytest.raises(UnknownOutcome):
        runtime.execute(*assignment(), None)
    runtime.close()


def test_descriptors_document_every_operation_and_control():
    descriptors = protocol_runtime_descriptors()
    for name, factory in (("a2a", a2a_runtime), ("acp", acp_runtime)):
        entry = descriptors[name]
        assert entry["specification"].startswith("https://")
        assert callable(factory)
        for operation in ("execute", "inspect", "cancel", "pause", "resume"):
            fields = entry["operations"][operation]
            for key in ("purpose", "input", "output", "authority", "side_effects",
                        "limits", "retry_safety"):
                assert key in fields


def test_a2a_redirect_is_rejected_and_no_credential_reaches_second_hop(tmp_path, a2a_server):
    a2a_server.card_path = "/a2a-redirect"
    opts = a2a_options(tmp_path, a2a_server)
    config = json.loads(open(opts["config"]).read())
    config["agent_url"] = f"http://127.0.0.1:{a2a_server.server_port}/a2a-redirect"
    open(opts["config"], "w").write(json.dumps(config))
    runtime = A2ARuntime(opts)
    with pytest.raises(ConnectionError):
        runtime.execute(*assignment(), None)
    assert "second_hop_auth" not in a2a_server.state  # Redirect target never saw the request.
    runtime.close()


def test_a2a_transport_uses_no_redirect_no_proxy_opener(tmp_path):
    transport = protocol_runtimes._HttpTransport("http://127.0.0.1:9/a2a", {}, 5)
    # The opener is built with ProxyHandler({}): an empty proxy map registers no
    # handlers, so environment/system proxies are never consulted at all.
    assert not any(isinstance(handler, protocol_runtimes.urllib.request.ProxyHandler)
                   for handler in transport.opener.handlers)
    assert any(isinstance(handler, protocol_runtimes._NoRedirectHandler)
               for handler in transport.opener.handlers)


@pytest.mark.parametrize("url", ["https://user:pass@127.0.0.1/a2a",
                                 "http://127.0.0.1/a2a?key=value",
                                 "http://127.0.0.1/a2a#frag",
                                 "https:///a2a"])
def test_a2a_rejects_userinfo_query_fragment_and_empty_host(tmp_path, a2a_server, url):
    opts = a2a_options(tmp_path, a2a_server,
                       state_file=str(tmp_path / "private" / f"rt-{len(url)}.json"))
    config = json.loads(open(opts["config"]).read())
    config["agent_url"] = url
    open(opts["config"], "w").write(json.dumps(config))
    runtime = A2ARuntime(opts)
    with pytest.raises(PermissionError):
        runtime.execute(*assignment(), None)
    assert a2a_server.state["task"] is False
    runtime.close()


def test_a2a_card_must_declare_the_exact_approved_jsonrpc_interface(tmp_path, a2a_server):
    task, context = assignment()
    for index, mutate in enumerate((lambda s: setattr(s, "card_interfaces", []),
                                    lambda s: setattr(s, "card_binding", "GRPC"),
                                    lambda s: setattr(s, "card_path", "/other"))):
        mutate(a2a_server)
        opts = a2a_options(tmp_path, a2a_server,
                           state_file=str(tmp_path / "private" / f"card-{index}.json"))
        runtime = A2ARuntime(opts)
        with pytest.raises(PermissionError):
            runtime.execute(task, context, None)
        runtime.close()
        a2a_server.card_interfaces, a2a_server.card_binding = "ok", "JSONRPC"
        a2a_server.card_path = "/a2a"


def test_a2a_stream_total_wire_cap_counts_every_frame(tmp_path, a2a_server, monkeypatch):
    monkeypatch.setattr(protocol_runtimes, "_MAX_HTTP_BODY_BYTES", 8192)
    a2a_server.streaming = True
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server, result_contract=None))
    task, context = assignment()
    task["title"] = "FLOOD"
    with pytest.raises(ValueError):
        runtime.execute(task, context, None)
    runtime.close()


def test_a2a_artifact_text_cap_is_cumulative(tmp_path, a2a_server, monkeypatch):
    monkeypatch.setattr(protocol_runtimes, "_MAX_COLLECTED_TEXT_BYTES", 64)
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server, result_contract=None))
    task, context = assignment()
    task["title"] = "BIGART"  # Three 40-byte artifacts: each fits per-part, total does not.
    with pytest.raises(ValueError):
        runtime.execute(task, context, None)
    runtime.close()


@pytest.mark.parametrize("flag", ["require_os_isolation", "vendor_filesystem_sandbox"])
def test_sandbox_opt_ins_are_refused_before_any_resource_setup(tmp_path, a2a_server,
                                                               acp_agent, flag):
    task, context = assignment()
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server, **{flag: True}))
    with pytest.raises(PermissionError):
        runtime.execute(task, context, None)
    assert not runtime.path.exists() and a2a_server.state["task"] is False
    runtime.close()
    runtime = ACPRuntime(acp_options(tmp_path, acp_agent, **{flag: True}))
    with pytest.raises(PermissionError):
        runtime.execute(task, context, None)
    assert runtime.transport is None and not runtime.path.exists()
    runtime.close()
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server,
                                     state_file=str(tmp_path / "private" / "rt-type.json"),
                                     **{flag: "yes"}))
    with pytest.raises(ValueError):
        runtime.execute(task, context, None)
    runtime.close()


def test_identity_change_against_persisted_state_fails_closed(tmp_path, a2a_server):
    opts = a2a_options(tmp_path, a2a_server)
    runtime = A2ARuntime(opts)
    task, context = assignment()
    runtime.execute({**task, "title": "MESSAGE"}, context, None)
    runtime.close()
    config = json.loads(open(opts["config"]).read())
    config["agent_id"] = "someone-else"
    open(opts["config"], "w").write(json.dumps(config))
    runtime = A2ARuntime(opts)
    task, context = assignment()
    with pytest.raises(PermissionError):
        runtime.execute({**task, "id": "task-foreign"},
                        {**context, "agent_id": "someone-else", "execution_id": "e2"}, None)
    runtime.close()
    # A restarted runtime with the original identity still recognizes its own state.
    config["agent_id"] = "agent"
    open(opts["config"], "w").write(json.dumps(config))
    runtime = A2ARuntime(opts)
    with pytest.raises(UnknownOutcome):  # Completed task position is never replayed.
        runtime.execute(*assignment(), None)
    runtime.close()


def test_remote_error_text_and_card_name_stay_out_of_state_and_errors(tmp_path, a2a_server):
    a2a_server.card_name = "fixture-agent fixture-token-secret"
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server))
    task, context = assignment()
    task["title"] = "RPCERROR"
    with pytest.raises(RuntimeError) as caught:
        runtime.execute(task, context, None)
    assert "fixture-token-secret" not in str(caught.value)
    persisted = runtime.path.read_text()
    assert "fixture-token-secret" not in persisted and "remote failure" not in persisted
    runtime.close()


def test_a2a_overall_deadline_bounds_every_wait(tmp_path, a2a_server):
    runtime = A2ARuntime(a2a_options(tmp_path, a2a_server, result_contract=None,
                                     timeout_seconds=2))
    task, context = assignment()
    task["title"] = "HANG"  # GetTask sleeps 4s; the turn deadline must bound the total.
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        runtime.execute(task, context, None)
    assert time.monotonic() - started < 20  # Never a fresh per-request 60s overshoot.
    runtime.close()
