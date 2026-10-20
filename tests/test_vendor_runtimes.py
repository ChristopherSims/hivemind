import asyncio
import json
import queue
import threading
from types import SimpleNamespace

import pytest

from hivemind.recovery import UnknownOutcome
from hivemind.vendor_runtimes import FactoryRuntime, OhMyPiRuntime
from test_agent_session import client_for
from test_task_execution import setup, start


def options(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    config = tmp_path / "private" / "agent.json"
    config.parent.mkdir()
    config.write_text(json.dumps({"project_id": "project", "agent_id": "agent",
                                 "base_url": "http://testserver", "token": "test-only-placeholder"}))
    return {"workspace": str(workspace), "config": str(config),
            "state_file": str(config.parent / "runtime.json"), "timeout_seconds": 2}


def assignment():
    return ({"id": "task", "title": "Local task", "scope_ids": ["scope"]},
            {"project_id": "project", "agent_id": "agent", "run_id": "run", "execution_id": "execution"})


class Session:
    created = []
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.cwd, self.id = kwargs.get("cwd"), "vendor-session"
        self.interrupted = False
        self.closed = False
        self.created.append(self)
    @classmethod
    def resume(cls, session_id, **kwargs):
        value = cls(**kwargs)
        value.id = session_id
        value.cwd = cls.created[-2].cwd
        return value
    async def open(self):
        pass
    async def close(self):
        self.closed = True
    async def interrupt(self):
        self.interrupted = True
    def stream(self, prompt, timeout):
        owner = self
        class Stream:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            def __aiter__(self):
                return self
            async def __anext__(self):
                if "interrupt" in prompt:
                    while not owner.interrupted:
                        await asyncio.sleep(.01)
                self.result = SimpleNamespace(interrupted=owner.interrupted,
                                              success=not owner.interrupted, text="Local result")
                raise StopAsyncIteration
        return Stream()


SDK = SimpleNamespace(Session=Session, Runtime=lambda **kw: kw, SessionConfig=lambda **kw: kw,
                      Autonomy=SimpleNamespace(LOW="low"))


def test_factory_is_disabled_and_creates_no_sdk_resources(tmp_path):
    config = options(tmp_path)
    before = len(Session.created)
    runtime = FactoryRuntime(config, sdk=SDK)
    with pytest.raises(PermissionError):
        runtime.execute(*assignment(), lambda *args: None)
    assert runtime.loop is None and not runtime.path.exists()
    assert len(Session.created) == before


def test_factory_task_identity_session_resume_and_private_checkpoint(tmp_path):
    config = {**options(tmp_path), "enabled": True, "allow_model_calls": True}
    runtime = FactoryRuntime(config, sdk=SDK)
    task, context = assignment()
    assert runtime.execute(task, context, None)["status"] == "completed"
    session_id = runtime.state["session_id"]
    assert Session.created[-1].kwargs["config"]["restrict_tools"] == runtime.tool_names
    assert "Local result" not in runtime.path.read_text()
    runtime.close()
    resumed = FactoryRuntime(config, sdk=SDK)
    with pytest.raises(UnknownOutcome):
        resumed.execute(task, context, None)
    assert resumed.execute({**task, "id": "task_next"},
                           {**context, "execution_id": "execution_next"}, None)["status"] == "completed"
    assert resumed.state["session_id"] == session_id
    resumed.close()


def test_factory_pause_waits_for_observed_interrupt_and_checkpoint(tmp_path):
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True}, sdk=SDK)
    task, context = assignment()
    task["title"] = "interrupt"
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(runtime.execute(task, context, None)))
    thread.start()
    for _ in range(100):
        if runtime.session:
            break
        threading.Event().wait(.01)
    result = runtime.pause({"id": "run"})
    thread.join(3)
    assert result["applied"] and result["checkpoint_id"]
    assert outcomes[0]["status"] == "paused"
    assert runtime.resume({"id": "run"})["applied"]
    runtime.close()


class OmpTransport:
    def __init__(self, argv, cwd, env):
        self.argv, self.messages, self.frames = argv, [], queue.Queue()
    def send(self, request):
        self.messages.append(request)
        kind = request["type"]
        if kind == "get_state":
            data = {"sessionId": "omp-session", "isStreaming": False, "isSettled": True}
        else:
            data = {}
        self.frames.put({"type": "response", "id": request["id"], "success": True, "data": data})
        if kind == "prompt":
            self.frames.put({"type": "prompt_result", "id": request["id"],
                             "status": "completed", "sessionSettled": True, "agentInvoked": True})
    def receive(self, timeout):
        return self.frames.get(timeout=timeout)
    def close(self):
        pass


def test_omp_structured_public_text_usage_and_explicit_no_tools(tmp_path):
    class PublicTextTransport(OmpTransport):
        def send(self, request):
            data = {
                "get_last_assistant_text": {"text": '{"handled":true,"reply_text":"4"}'},
                "get_session_stats": {"tokens": {"input": 100, "output": 12}, "cost": .00002,
                                      "toolCalls": 0, "toolResults": 0},
            }.get(request["type"])
            if data is None:
                return super().send(request)
            self.messages.append(request)
            self.frames.put({"type": "response", "id": request["id"], "success": True, "data": data})

    runtime = OhMyPiRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                            "no_tools": True, "result_contract": "message", "collect_usage": True},
                           transport_factory=PublicTextTransport)
    try:
        outcome = runtime.execute(*assignment(), None)
        assert outcome["message_result"] == {"handled": True, "reply_text": "4"}
        assert runtime.state["usage"]["cost"] == .00002
        assert "--no-tools" in runtime.transport.argv and "--tools" not in runtime.transport.argv
        assert "get_messages" not in [item["type"] for item in runtime.transport.messages]
        assert "reply_text" not in runtime.path.read_text()
    finally:
        runtime.close()


def test_factory_explicit_executable_forwarding(tmp_path):
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "executable": str(tmp_path / "droid.exe")}, sdk=SDK)
    try:
        runtime.execute(*assignment(), None)
        assert Session.created[-1].kwargs["runtime"]["executable"] == str(tmp_path / "droid.exe")
    finally:
        runtime.close()
        runtime.close()  # Reopening workflows can close before harness-wide cleanup.


def test_factory_resume_workspace_mismatch_fails_before_a_turn(tmp_path):
    config = {**options(tmp_path), "enabled": True, "allow_model_calls": True}
    first = FactoryRuntime(config, sdk=SDK)
    first.execute(*assignment(), None)
    first.close()
    called = []

    class WrongWorkspaceSession(Session):
        @classmethod
        def resume(cls, session_id, **kwargs):
            value = cls(**kwargs)
            value.id = session_id
            value.cwd = tmp_path
            return value

        def stream(self, *args, **kwargs):
            called.append(True)
            return super().stream(*args, **kwargs)

    local_sdk = SimpleNamespace(**{**vars(SDK), "Session": WrongWorkspaceSession})
    resumed = FactoryRuntime(config, sdk=local_sdk)
    task, context = assignment()
    try:
        with pytest.raises(PermissionError, match="workspace mismatch"):
            resumed.execute({**task, "id": "next"}, {**context, "execution_id": "next"}, None)
        assert called == [] and resumed.state["outcome"] == "unknown"
    finally:
        resumed.close()


def test_omp_uses_documented_rpc_and_restricts_tools(tmp_path):
    runtime = OhMyPiRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True},
                            transport_factory=OmpTransport)
    assert runtime.execute(*assignment(), None)["status"] == "completed"
    assert runtime.state["session_id"] == "omp-session"
    assert "--no-extensions" in runtime.transport.argv
    assert runtime.transport.argv[runtime.transport.argv.index("--tools") + 1] == "read,grep,glob,write,edit"
    assert any(item["type"] == "prompt" for item in runtime.transport.messages)
    assert runtime.transport.messages[-1]["type"] == "get_state"
    runtime.close()


def test_unknown_vendor_outcome_and_cross_identity_fail_closed(tmp_path):
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True}, sdk=SDK)
    runtime._save(outcome="unknown")
    with pytest.raises(UnknownOutcome):
        runtime.execute(*assignment(), None)
    runtime._save(outcome="idle")
    task, context = assignment()
    context["agent_id"] = "other"
    with pytest.raises(PermissionError):
        runtime.execute(task, context, None)


def test_private_state_is_rejected_inside_workspace(tmp_path):
    config = options(tmp_path)
    with pytest.raises(ValueError):
        FactoryRuntime({**config, "state_file": str(tmp_path / "workspace" / "private.json")})


def test_managed_hooks_reuse_canonical_run_and_existing_claim(world, tmp_path, monkeypatch):
    from hivemind import hooks
    agent, token, run, scope, task = setup(world)
    task = start(world, task, run, token)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    config = {"project_id": world.project, "agent_id": agent["id"], "token": token,
        "base_url": "http://testserver", "runtime": "droid", "workspace_root": str(workspace),
        "state_dir": str(tmp_path / "hooks"), "scope_ids": [scope["id"]],
        "managed_run_id": run["id"], "managed_task_id": task["id"],
        "managed_execution_id": task["execution"]["id"]}
    monkeypatch.setattr(hooks, "Client", lambda *_: client_for(world, token))
    hooks.invoke(config, {"hook_event_name": "SessionStart", "session_id": "local-session"})
    hooks.invoke(config, {"hook_event_name": "PreToolUse", "session_id": "local-session",
        "tool_name": "Create", "tool_use_id": "local-call", "tool_input": {"file_path": "export/local.txt"}})
    assert world.app.state.store.db.execute(
        "SELECT count(*) FROM resources WHERE kind='run' AND deleted=0").fetchone()[0] == 1
    assert len(world.call("GET", f"work-claims?project_id={world.project}")) == 1
    with pytest.raises(RuntimeError):
        hooks.invoke({**config, "managed_execution_id": "stale"},
                     {"hook_event_name": "PreToolUse", "session_id": "local-session",
                      "tool_name": "Read", "tool_input": {"file_path": "export/local.txt"}})
