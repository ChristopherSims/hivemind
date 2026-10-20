"""Session-local registration evidence; no user/project vendor configuration writes."""
import json
import shutil
import sys
from pathlib import Path

from . import vendor_recovery
from .worker_mcp import OMP_TOOL_IDS, TOOLS


def enabled(options):
    value = options.get("auto_mcp_registration", False)
    if type(value) is not bool:
        raise ValueError("auto_mcp_registration must be a boolean")
    if value and options.get("vendor_filesystem_sandbox"):
        raise ValueError("Automatic MCP sandbox bindings are not supported")
    if value and options.get("no_tools") is True:
        raise ValueError("Automatic MCP conflicts with no_tools")
    return value


def factory_server(sdk, config):
    return sdk.StdioMcpServerConfig(
        name="hivemind", command=sys.executable,
        args=("-m", "hivemind.worker_mcp"), env={"HIVEMIND_CONFIG": str(config)},
    )


def omp_host(options, workspace):
    """Locate the installed, operator-owned SDK, never a repository module."""
    bun = shutil.which("bun")
    executable = shutil.which(options.get("executable", "omp"))
    if not bun or not executable:
        raise PermissionError("Automatic OMP MCP requires Bun and the installed SDK")
    root = Path(executable).resolve().parent.parent / "install/global/node_modules/@oh-my-pi/pi-coding-agent"
    root = root.resolve(strict=True)
    if root.is_relative_to(workspace) or Path(bun).resolve().is_relative_to(workspace):
        raise PermissionError("Worker-local vendor executable is untrusted")
    manifest = root / "package.json"
    if manifest.stat().st_size > 65536:
        raise PermissionError("OMP SDK manifest exceeds its boundary")
    package = json.loads(manifest.read_text(encoding="utf-8"))
    if package.get("name") != "@oh-my-pi/pi-coding-agent" or package.get("version") != "18.8.9":
        raise PermissionError("Automatic OMP MCP SDK contract changed")
    if not (root / "src/sdk.ts").is_file():
        raise PermissionError("Automatic OMP MCP requires the installed source SDK")
    return bun, root


async def verify_factory(session, native_tools):
    """No inferred readiness: require a connected sole server and its exact catalog."""
    result = await session.list_mcp_servers()
    if (len(result.servers) != 1 or result.servers[0].name != "hivemind"
            or getattr(result.servers[0].status, "value", result.servers[0].status) != "connected"):
        raise PermissionError("Worker MCP connection is not verified")
    tools = await session.list_mcp_tools()
    if (len(tools) != len(TOOLS)
            or any(tool.server_name != "hivemind" or tool.is_enabled is not True for tool in tools)
            or {tool.name for tool in tools} != set(TOOLS)):
        raise PermissionError("Worker MCP catalog does not match")
    catalog = await session.list_tools()
    mcp_catalog = [tool for tool in catalog if tool.source == "mcp"]
    if len(mcp_catalog) != len(TOOLS):
        raise PermissionError("Worker MCP tool identity is ambiguous")
    ids = []
    for name in TOOLS:
        expected_ids = {f"mcp__hivemind__{name}", f"mcp___hivemind___{name}"}
        matches = [tool.id for tool in mcp_catalog if tool.id in expected_ids]
        if len(matches) != 1:
            raise PermissionError("Worker MCP tool identity is ambiguous")
        ids.append(matches[0])
    if len(set(ids)) != len(TOOLS):
        raise PermissionError("Worker MCP tool identity is ambiguous")
    expected = set(native_tools) | set(ids)
    await session.update_settings(restrict_tools=sorted(expected))
    if set(session.settings.restrict_tools or ()) != expected:
        raise PermissionError("Worker MCP tool restriction is unverified")
    return ids


def verify_omp(path, nonce):
    path = vendor_recovery.trusted_private_file(path, base_dir=path.parent)
    with path.open("rb") as stream:
        data = stream.read(65537)
    if len(data) > 65536:
        raise PermissionError("Worker MCP evidence exceeds its boundary")
    evidence = json.loads(data)
    if (evidence.get("nonce") != nonce or evidence.get("connected") is not True
            or evidence.get("server") != "hivemind"
            or evidence.get("tools") != list(OMP_TOOL_IDS)):
        raise PermissionError("Worker MCP connection is not verified")
    return list(OMP_TOOL_IDS)
