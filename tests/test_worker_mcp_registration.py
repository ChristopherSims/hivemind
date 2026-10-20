import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind.vendor_runtimes import FactoryRuntime, JsonLinesProcess, OhMyPiRuntime, atomic_state
from hivemind.vendor_recovery import RecoveryRefused
from hivemind.worker_mcp import OMP_TOOL_IDS, TOOLS
from hivemind.worker_mcp_registration import enabled, verify_omp
from test_vendor_runtimes import OmpTransport, Session, assignment, options


class McpSession(Session):
    mode = "ready"
    prompts = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.settings = SimpleNamespace(restrict_tools=kwargs["config"]["restrict_tools"])

    async def list_mcp_servers(self):
        servers = [SimpleNamespace(name="hivemind", status=(
            "failed" if self.mode == "disconnected" else "connected"))]
        if self.mode == "extra_server":
            servers.append(SimpleNamespace(name="foreign", status="connected"))
        return SimpleNamespace(servers=servers)

    async def list_mcp_tools(self):
        return [SimpleNamespace(server_name="hivemind", name=name,
                                is_enabled=self.mode != "disabled_tool")
                for name in (TOOLS[:-1] if self.mode == "missing_tool" else TOOLS)]

    async def list_tools(self):
        return [SimpleNamespace(source="mcp", display_name=name,
                                id="mcp__hivemind__" + name) for name in TOOLS]

    async def update_settings(self, **kwargs):
        if self.mode != "ignored_restriction":
            self.settings.restrict_tools = kwargs["restrict_tools"]

    def stream(self, prompt, timeout):
        self.prompts.append(prompt)
        return super().stream(prompt, timeout)


def sdk():
    import droid_sdk
    return SimpleNamespace(
        Session=McpSession, Runtime=lambda **kw: kw, SessionConfig=lambda **kw: kw,
        Autonomy=SimpleNamespace(LOW="low"), StdioMcpServerConfig=droid_sdk.StdioMcpServerConfig)


def test_factory_registers_scoped_mcp_before_prompt_and_updates_hooks(tmp_path):
    McpSession.mode, McpSession.prompts = "ready", []
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "auto_mcp_registration": True}, sdk=sdk())
    try:
        assert runtime.execute(*assignment(), None)["status"] == "completed"
        descriptor = runtime.session.kwargs["config"]["mcp_servers"][0]
        assert descriptor.command == sys.executable
        assert descriptor.args == ("-m", "hivemind.worker_mcp")
        assert dict(descriptor.env) == {"HIVEMIND_CONFIG": str(runtime._config)}
        hook = json.loads(runtime._config.read_text())
        assert hook["allowed_mcp_tools"] == runtime.mcp_tool_ids
        assert runtime.state["mcp"]["connection_verified"] is True
        assert len(McpSession.prompts) == 1
    finally:
        runtime.close()


@pytest.mark.parametrize("mode", [
    "disconnected", "extra_server", "missing_tool", "disabled_tool", "ignored_restriction",
])
def test_factory_mcp_missing_evidence_refuses_prompt(tmp_path, mode):
    McpSession.mode, McpSession.prompts = mode, []
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "auto_mcp_registration": True}, sdk=sdk())
    try:
        with pytest.raises(PermissionError):
            runtime.execute(*assignment(), None)
        assert not McpSession.prompts
        assert runtime.state["outcome"] == "unknown"
    finally:
        runtime.close()
        McpSession.mode = "ready"


def test_factory_connected_session_rechecks_mcp_before_next_prompt(tmp_path):
    McpSession.mode, McpSession.prompts = "ready", []
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "auto_mcp_registration": True}, sdk=sdk())
    try:
        runtime.execute(*assignment(), None)
        McpSession.mode = "disconnected"
        task, context = assignment()
        with pytest.raises(PermissionError):
            runtime.execute({**task, "id": "next-task"},
                            {**context, "execution_id": "next-execution"}, None)
        assert len(McpSession.prompts) == 1
        assert runtime.state["mcp"]["connection_verified"] is False
    finally:
        runtime.close()
        McpSession.mode = "ready"


def test_omp_isolated_extension_requires_fresh_nonce_proof(tmp_path, monkeypatch):
    monkeypatch.setattr("hivemind.worker_mcp_registration.omp_host",
                        lambda *args: (sys.executable, tmp_path))
    created = []

    class Registered(OmpTransport):
        def __init__(self, argv, cwd, env):
            super().__init__(argv, cwd, env)
            created.append(self)
            config = json.loads(Path(env["HIVEMIND_CONFIG"]).read_text())
            binding = config["worker_mcp"]
            atomic_state(binding["ready_file"], {
                "server": "hivemind", "connected": True, "nonce": binding["nonce"],
                "tools": list(OMP_TOOL_IDS),
            })

    runtime = OhMyPiRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                             "auto_mcp_registration": True}, transport_factory=Registered)
    try:
        assert runtime.execute(*assignment(), None)["status"] == "completed"
        assert created[0].argv[-1].endswith("worker-host.ts")
        config = json.loads(runtime._config.read_text())
        assert config["worker_mcp"]["host_owned"] is True
        assert config["worker_mcp"]["tools"] == list(OMP_TOOL_IDS)
        assert runtime.state["mcp"]["connection_verified"] is True
        task, context = assignment()
        assert runtime.execute({**task, "id": "next-task"},
                               {**context, "execution_id": "next-execution"}, None)["status"] == "completed"
    finally:
        runtime.close()


def test_omp_missing_registration_proof_refuses_prompt(tmp_path, monkeypatch):
    monkeypatch.setattr("hivemind.worker_mcp_registration.omp_host",
                        lambda *args: (sys.executable, tmp_path))
    created = []

    def transport(*args):
        value = OmpTransport(*args)
        created.append(value)
        return value

    runtime = OhMyPiRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                             "auto_mcp_registration": True}, transport_factory=transport)
    try:
        with pytest.raises((PermissionError, RecoveryRefused)):
            runtime.execute(*assignment(), None)
        assert not any(frame["type"] == "prompt" for frame in created[0].messages)
    finally:
        runtime.close()


@pytest.mark.parametrize("change", [
    {"nonce": "old"}, {"connected": False}, {"tools": []}, {"server": "foreign"},
])
def test_omp_stale_or_drifted_evidence_is_not_permission(tmp_path, change):
    path = tmp_path / "ready.json"
    atomic_state(path, {"nonce": "fresh", "connected": True, "server": "hivemind",
                        "tools": list(OMP_TOOL_IDS), **change})
    with pytest.raises(PermissionError):
        verify_omp(path, "fresh")


@pytest.mark.parametrize("config", [
    {"auto_mcp_registration": "true"},
    {"auto_mcp_registration": True, "no_tools": True},
    {"auto_mcp_registration": True, "vendor_filesystem_sandbox": True},
])
def test_unsupported_registration_options_fail_closed(config):
    with pytest.raises(ValueError):
        enabled(config)


def test_factory_refuses_repository_mcp_launchers_before_open(tmp_path):
    config = {**options(tmp_path), "auto_mcp_registration": True}
    directory = Path(config["workspace"]) / ".factory"
    directory.mkdir()
    (directory / "mcp.json").write_text('{"mcpServers":{"foreign":{}}}')
    before = len(Session.created)
    with pytest.raises(PermissionError):
        FactoryRuntime(config, sdk=sdk())
    assert len(Session.created) == before


def test_saved_factory_load_and_adoption_keep_session_local_descriptor(tmp_path):
    from hivemind.vendor_recovery import resume_session_workspace_bound
    from test_vendor_recovery import FakeVendorClient, load_result

    client = FakeVendorClient(lambda: load_result(tmp_path))
    descriptor = {"type": "stdio", "name": "hivemind", "command": sys.executable,
                  "args": ["-m", "hivemind.worker_mcp"], "env": {}}
    session = asyncio.run(resume_session_workspace_bound(
        client, session_id="session", workspace=tmp_path, expected_model="model-a",
        restrict_tools=["Read"], mcp_servers=[descriptor]))
    try:
        assert all(load["mcp_servers"] == [descriptor] for load in client.loads)
        assert session._config.mcp_servers[0].name == "hivemind"
        assert session._load_mcp_configs == [descriptor]
    finally:
        asyncio.run(session.close())


def test_real_worker_stdio_mcp_handshake_has_only_read_only_catalog(tmp_path):
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "offline-test", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    ]
    transport = JsonLinesProcess([sys.executable, "-m", "hivemind.worker_mcp"],
                                 tmp_path, dict(os.environ))
    try:
        transport.send(requests[0])
        assert transport.receive(30)["id"] == 1
        transport.send(requests[1])
        transport.send(requests[2])
        response = transport.receive(30)
        assert response["id"] == 2
        catalog = response["result"]["tools"]
    finally:
        transport.close()
    assert {tool["name"] for tool in catalog} == set(TOOLS)
    assert all(tool["annotations"]["readOnlyHint"] is True for tool in catalog)


def test_installed_omp_extension_registers_real_worker_mcp_without_model(tmp_path):
    bun = shutil.which("bun")
    package = Path.home() / ".bun/install/global/node_modules/@oh-my-pi/pi-coding-agent"
    if not bun or not (package / "src/extensibility/extensions/loader.ts").is_file():
        pytest.skip("Installed OMP source SDK and Bun required")
    root = Path(__file__).resolve().parents[1]
    config, ready = tmp_path / "agent.json", tmp_path / "ready.json"
    config.write_text(json.dumps({
        "project_id": "test-project", "worker_mcp": {
            "enabled": True, "tools": list(OMP_TOOL_IDS), "nonce": "test-nonce",
            "ready_file": str(ready),
        },
    }))
    script = tmp_path / "probe.ts"
    script.write_text(
        "const {loadExtensions} = await import(" +
        json.dumps((package / "src/extensibility/extensions/loader.ts").as_posix()) +
        "); const result = await loadExtensions([" +
        json.dumps((root / "adapters/oh-my-pi/hivemind.ts").as_posix()) +
        "], process.cwd()); console.log(JSON.stringify({errors:result.errors})); "
        "for (const ext of result.extensions) for (const handler of "
        "ext.handlers.get('session_shutdown') ?? []) await handler({}, {}); "
        "process.exit(result.errors.length ? 1 : 0);")
    env = {**os.environ, "NODE_PATH": str(package.parent.parent), "HOME": str(tmp_path),
           "USERPROFILE": str(tmp_path), "HIVEMIND_CONFIG": str(config),
           "HIVEMIND_PYTHON": sys.executable}
    result = subprocess.run([bun, str(script)], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    assert verify_omp(ready, "test-nonce") == list(OMP_TOOL_IDS)


def test_omp_adapter_typechecks_against_installed_sdk(tmp_path):
    package = Path.home() / ".bun/install/global/node_modules/@oh-my-pi/pi-coding-agent"
    node = shutil.which("node")
    root = Path(__file__).resolve().parents[1]
    tsc = root / "dashboard/node_modules/typescript/bin/tsc"
    if not node or not tsc.is_file() or not (package / "dist/types/index.d.ts").is_file():
        pytest.skip("Installed OMP types and TypeScript compiler required")
    config = tmp_path / "tsconfig.json"
    config.write_text(json.dumps({
        "compilerOptions": {
            "strict": True, "skipLibCheck": True, "noEmit": True,
            "target": "ES2022", "module": "ESNext", "moduleResolution": "Bundler",
            "baseUrl": str(root), "types": ["node"],
            "typeRoots": [str(package.parent.parent / "@types")],
            "paths": {
                "@oh-my-pi/pi-coding-agent": [str(package / "dist/types/index.d.ts")],
                "@oh-my-pi/pi-coding-agent/*": [str(package / "dist/types/*")],
            },
        },
        "files": [str(root / "adapters/oh-my-pi/hivemind.ts"),
                  str(root / "adapters/oh-my-pi/worker-host.ts")],
    }))
    result = subprocess.run([node, str(tsc), "-p", str(config)], cwd=root,
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr


def test_scoped_omp_host_disables_discovery_before_sdk_startup(tmp_path):
    bun = shutil.which("bun")
    package = Path.home() / ".bun/install/global/node_modules/@oh-my-pi/pi-coding-agent"
    if not bun or not (package / "src/mcp/index.ts").is_file():
        pytest.skip("Installed OMP source SDK and Bun required")
    root = Path(__file__).resolve().parents[1]
    sdk_root = tmp_path / "sdk"
    for path in ("mcp", "session", "modes/rpc"):
        (sdk_root / "src" / path).mkdir(parents=True, exist_ok=True)
    (sdk_root / "src/mcp/index.ts").write_text(
        "export {MCPManager} from " + json.dumps((package / "src/mcp/index.ts").as_posix()) + ";")
    (sdk_root / "src/session/session-manager.ts").write_text(
        "export const SessionManager={create(){return {};}};")
    (sdk_root / "src/sdk.ts").write_text(
        "export async function createAgentSession(o:any){"
        "if(o.enableMCP !== false || o.restrictToolNames !== true || "
        "o.allowRestrictedCustomTools !== true || o.disableExtensionDiscovery !== true || "
        "o.cacheWarming !== false || o.customTools.length !== 8) "
        "throw Error('Unsafe startup'); return {session:{}};}")
    (sdk_root / "src/modes/rpc/rpc-mode.ts").write_text(
        "export async function runRpcMode(){console.log('scoped-startup');}")
    config, ready = tmp_path / "agent.json", tmp_path / "ready.json"
    config.write_text(json.dumps({"project_id": "test-project", "worker_mcp": {
        "host_owned": True, "sdk_root": str(sdk_root), "tools": list(OMP_TOOL_IDS),
        "native_tools": ["read"], "nonce": "fresh-host", "ready_file": str(ready),
        "session_dir": str(tmp_path / "sessions"),
        "extension": str(root / "adapters/oh-my-pi/hivemind.ts"),
    }}))
    # A repository config must not participate in discovery, even at startup.
    (tmp_path / ".mcp.json").write_text('{"mcpServers":{"foreign":{"command":"invalid"}}}')
    env = {**os.environ, "NODE_PATH": str(package.parent.parent), "HOME": str(tmp_path),
           "USERPROFILE": str(tmp_path), "HIVEMIND_CONFIG": str(config),
           "HIVEMIND_PYTHON": sys.executable}
    result = subprocess.run([bun, str(root / "adapters/oh-my-pi/worker-host.ts")],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "scoped-startup" in result.stdout
    assert verify_omp(ready, "fresh-host") == list(OMP_TOOL_IDS)
