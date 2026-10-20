"""End-to-end vendor recovery wiring: real service (TestClient) + managed worker +
fenced vendor doubles. No paid model calls, no real vendor processes."""

import json
from pathlib import Path
from types import SimpleNamespace


from hivemind import vendor_recovery as vr
from hivemind.agent_worker import AgentWorker
from hivemind.vendor_runtimes import FactoryRuntime, OhMyPiRuntime, atomic_state
from test_agent_worker import LocalClient, real_task_setup
from test_vendor_recovery import load_result, wire_settings


class SyntheticPromptStop(RuntimeError):
    """Boundary marker: the prompt was attempted, proving every fence passed."""


class FencedClient:
    """API-compatible double for the connected vendor client behind a real Session."""

    def __init__(self, workspace, on_load=None):
        self.workspace = workspace
        self.on_load = on_load
        self.loads, self.prompts, self.closed_sessions = 0, [], 0
        self.session_id = None
        self.closed = False
        self._restrict = None

    async def load_session(self, **kwargs):
        self.loads += 1
        self.session_id = kwargs["session_id"]
        if self.on_load is not None:
            self.on_load()
        loaded = load_result(self.workspace)
        if self._restrict is not None:
            loaded.settings = wire_settings(restrict=list(self._restrict))
        return loaded

    async def update_session_settings(self, **kwargs):
        self._restrict = kwargs.get("restrict_tool_ids")
        return SimpleNamespace()

    async def add_user_message(self, **kwargs):
        self.prompts.append(kwargs)
        raise SyntheticPromptStop("fence-passed boundary marker")

    async def interrupt_session(self):
        pass

    async def close_session(self, **kwargs):
        self.closed_sessions += 1

    async def close(self):
        self.closed = True

    def set_permission_handler(self, handler):
        self.permission_handler = handler

    def set_ask_user_handler(self, handler):
        self.question_handler = handler

    def on_notification(self, callback):
        return lambda: None

    def on_error(self, callback):
        return lambda: None


def fenced_sdk(client):
    async def connect(runtime_config, *, workspace):
        assert Path(workspace).is_dir()
        return client

    return SimpleNamespace(open_workspace_client=connect, Runtime=lambda **kwargs: dict(kwargs))


def vendor_options(tmp_path, world, agent, token, model="model-a"):
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    private = tmp_path / "private"
    private.mkdir(exist_ok=True)
    config = private / "agent.json"
    config.write_text(
        json.dumps(
            {
                "project_id": world.project,
                "agent_id": agent["id"],
                "base_url": "http://testserver",
                "token": "test-only-placeholder",
            }
        )
    )
    return {
        "workspace": str(workspace),
        "config": str(config),
        "state_file": str(private / "runtime.json"),
        "timeout_seconds": 5,
        "enabled": True,
        "allow_model_calls": True,
        "model": model,
    }


def seed_paused_state(
    options, *, vendor, run_id, task_id, execution_id, session_id, outcome="paused", with_checkpoint=True
):
    path = Path(options["state_file"])
    ws = str(Path(options["workspace"]).resolve())
    atomic_state(
        path,
        {
            "vendor": vendor,
            "workspace": ws,
            "run_id": run_id,
            "task_id": task_id,
            "execution_id": execution_id,
            "session_id": session_id,
            "outcome": outcome,
            "phase_id": None,
            "checkpoint_id": "cp" if with_checkpoint else None,
        },
    )
    if with_checkpoint:
        atomic_state(
            path.with_name(path.name + ".checkpoint.json"),
            {
                "id": "cp",
                "run_id": run_id,
                "task_id": task_id,
                "execution_id": execution_id,
                "quiescent": True,
                "status": outcome,
            },
        )


def started_task(world, task, run, token):
    return world.call(
        "POST",
        f"tasks/{task['id']}:start",
        {"run_id": run["id"], "expected_revision": task["revision"]},
        token=token,
    )


def test_worker_recovery_migrates_execution_and_prompts_only_after_fences(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "vendor-migration")
    started = started_task(world, task, run, token)
    prior_execution = started["execution"]["id"]
    options = vendor_options(tmp_path, world, agent, token)
    ws = Path(options["workspace"]).resolve()
    seed_paused_state(
        options,
        vendor="droid",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=prior_execution,
        session_id="prior-session",
    )
    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    fenced = FencedClient(ws)
    runtime = FactoryRuntime(options, sdk=fenced_sdk(fenced))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    try:
        report = worker.tick()
        # The task recovered under a NEW execution while the vendor state was
        # bound to the prior one (source execution migration), then every fence
        # passed and the prompt boundary was reached exactly once.
        current = world.call("GET", f"tasks/{task['id']}", token=token)
        assert current["execution"]["id"] != prior_execution
        assert fenced.loads == 2  # load + restrict-tools verification reload
        assert len(fenced.prompts) == 1
        assert report["status"] == "failed"  # synthetic transport stop, not a replay
        # The interrupted turn holds as unknown locally; it is never silently settled.
        assert runtime.state["outcome"] == "unknown"
        assert vr.pending_recovery_intent(options["state_file"]) is None  # cleared post-fence
    finally:
        runtime.close()


def test_authority_refuses_after_load_when_control_arrives_before_prompt(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "vendor-control")

    class Holds:
        policy = None

        def __init__(self):
            self.state = "clear"

        def hold(self):
            return {"state": self.state}

        def tick(self, signals):
            return None

    holds = Holds()
    started = started_task(world, task, run, token)
    options = vendor_options(tmp_path, world, agent, token)
    ws = Path(options["workspace"]).resolve()
    seed_paused_state(
        options,
        vendor="droid",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=started["execution"]["id"],
        session_id="prior-session",
    )

    def trip_hold():
        holds.state = "pause_held"  # control/policy hold lands after the load

    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    fenced = FencedClient(ws, on_load=trip_hold)
    runtime = FactoryRuntime(options, sdk=fenced_sdk(fenced))
    worker = AgentWorker(
        client, world.project, agent["id"], run["id"], runtime, enabled=True, automatic_controls=holds
    )
    try:
        report = worker.tick()
        assert fenced.loads >= 1  # the load happened...
        assert fenced.prompts == []  # ...but the fresh gate refused before the prompt
        assert report["status"] in ("aborted", "failed", "paused")
        assert runtime.state["outcome"] == "unknown"
    finally:
        runtime.close()


def test_authority_refuses_when_claim_is_revoked_after_load(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "vendor-revoked")
    started = started_task(world, task, run, token)
    options = vendor_options(tmp_path, world, agent, token)
    ws = Path(options["workspace"]).resolve()
    seed_paused_state(
        options,
        vendor="droid",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=started["execution"]["id"],
        session_id="prior-session",
    )

    def revoke():
        world.call("DELETE", f"work-claims/{claim['id']}", token=token)

    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    fenced = FencedClient(ws, on_load=revoke)
    runtime = FactoryRuntime(options, sdk=fenced_sdk(fenced))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    try:
        worker.tick()
        assert fenced.loads >= 1
        assert fenced.prompts == []  # revoked grant: no prompt, no replay
        assert runtime.state["outcome"] == "unknown"
    finally:
        runtime.close()


def test_corrupt_pending_intent_holds_and_is_never_overwritten(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "vendor-corrupt")
    started = started_task(world, task, run, token)
    options = vendor_options(tmp_path, world, agent, token)
    ws = Path(options["workspace"]).resolve()
    seed_paused_state(
        options,
        vendor="droid",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=started["execution"]["id"],
        session_id="prior-session",
    )
    intent_path = Path(options["state_file"]).with_name("runtime.json.recovery-intent.json")
    intent_path.write_text("{corrupt")
    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    fenced = FencedClient(ws)
    runtime = FactoryRuntime(options, sdk=fenced_sdk(fenced))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    try:
        worker.tick()
        assert fenced.loads == 0 and fenced.prompts == []  # held before any load
        assert intent_path.read_text() == "{corrupt"  # never overwritten
    finally:
        runtime.close()


def test_unknown_outcome_holds_without_load_or_prompt(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "vendor-unknown")
    started = started_task(world, task, run, token)
    options = vendor_options(tmp_path, world, agent, token)
    ws = Path(options["workspace"]).resolve()
    seed_paused_state(
        options,
        vendor="droid",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=started["execution"]["id"],
        session_id="prior-session",
        outcome="unknown",
    )
    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    fenced = FencedClient(ws)
    runtime = FactoryRuntime(options, sdk=fenced_sdk(fenced))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    try:
        worker.tick()
        assert fenced.loads == 0 and fenced.prompts == []
        assert runtime.loop is None  # refused before the SDK loop even started
        assert runtime.state["outcome"] == "unknown"
    finally:
        runtime.close()


class OmpTransport:
    """Fake OMP JSON-lines transport with a recorded message log."""

    def __init__(self, argv, cwd, env, settled=True):
        self.argv, self.messages, self.settled = argv, [], settled
        self.prompted = False

    def send(self, request):
        self.messages.append(request)
        kind = request["type"]
        data = {}
        if kind == "get_state":
            data = {
                "sessionId": "omp-session",
                "isSettled": self.settled,
                "isStreaming": False,
                "sessionFile": self._session_file(),
            }
        self._respond({"type": "response", "id": request["id"], "success": True, "data": data})
        if kind == "prompt":
            self.prompted = True
            self._respond(
                {
                    "type": "prompt_result",
                    "id": request["id"],
                    "status": "completed",
                    "sessionSettled": True,
                    "agentInvoked": True,
                }
            )

    def _session_file(self):
        directory = self.argv[self.argv.index("--session-dir") + 1]
        return str(Path(directory) / "session.jsonl")

    def _respond(self, frame):
        self.frames.append(frame)

    frames = None

    def receive(self, timeout):
        return self.frames.pop(0)

    def close(self):
        pass


def omp_transport_factory(log, settled=True):
    def factory(argv, cwd, env):
        transport = OmpTransport(argv, cwd, env, settled=settled)
        transport.frames = []
        log.append(transport)
        return transport

    return factory


def test_worker_omp_fenced_resume_settles_and_prompts(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "omp-resume")
    started = started_task(world, task, run, token)
    prior_execution = started["execution"]["id"]
    options = vendor_options(tmp_path, world, agent, token)
    options["model"] = "model-a"
    state_path = Path(options["state_file"])
    sessions = state_path.parent / (state_path.stem + "-sessions")
    sessions.mkdir()
    session_file = sessions / "session.jsonl"
    session_file.write_text("{}")
    seed_paused_state(
        options,
        vendor="oh-my-pi",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=prior_execution,
        session_id="omp-session",
    )
    state = json.loads(state_path.read_text())
    state["session_file"] = str(session_file)
    atomic_state(state_path, state)
    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    transports = []
    runtime = OhMyPiRuntime(options, transport_factory=omp_transport_factory(transports))
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    try:
        report = worker.tick()
        assert report["status"] == "completed"
        assert transports and transports[0].prompted
        assert "--resume" in transports[0].argv
        assert vr.pending_recovery_intent(options["state_file"]) is None
        assert runtime.state["outcome"] == "completed"
    finally:
        runtime.close()


def test_omp_resume_holds_without_prompt_when_settlement_unknown(world, tmp_path):
    agent, token, run, claim, task = real_task_setup(world, "omp-hold")
    started = started_task(world, task, run, token)
    options = vendor_options(tmp_path, world, agent, token)
    state_path = Path(options["state_file"])
    sessions = state_path.parent / (state_path.stem + "-sessions")
    sessions.mkdir()
    session_file = sessions / "session.jsonl"
    session_file.write_text("{}")
    seed_paused_state(
        options,
        vendor="oh-my-pi",
        run_id=run["id"],
        task_id=task["id"],
        execution_id=started["execution"]["id"],
        session_id="omp-session",
    )
    state = json.loads(state_path.read_text())
    state["session_file"] = str(session_file)
    atomic_state(state_path, state)
    client = LocalClient(world, token)
    client.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)

    class Unsettled(OmpTransport):
        def send(self, request):
            if request["type"] == "get_state":
                self.messages.append(request)
                self.frames.append(
                    {
                        "type": "response",
                        "id": request["id"],
                        "success": True,
                        "data": {"sessionId": "omp-session", "isSettled": None},
                    }
                )
                return
            super().send(request)

    transports = []

    def factory(argv, cwd, env):
        transport = Unsettled(argv, cwd, env)
        transport.frames = []
        transports.append(transport)
        return transport

    runtime = OhMyPiRuntime(options, transport_factory=factory)
    worker = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    try:
        worker.tick()
        assert transports and not transports[0].prompted  # held: isSettled missing
        assert not any(message["type"] == "prompt" for message in transports[0].messages)
        # The durable resume intent survives for reconciliation; no silent retry.
        intent = vr.pending_recovery_intent(options["state_file"])
        assert intent is not None and intent["session_id"] == "omp-session"
    finally:
        runtime.close()
