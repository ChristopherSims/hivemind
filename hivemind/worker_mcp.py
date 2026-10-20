"""Worker-local, read-only MCP surface. Canonical HTTP authorization still applies."""
from mcp.server.mcpserver import MCPServer

from . import mcp

TOOLS = (
    "hivemind_describe_tools", "hivemind_get_ledger", "hivemind_query_context",
    "hivemind_get_inbox", "hivemind_search", "hivemind_get_task",
    "hivemind_get_notifications", "hivemind_get_conflicts",
)
OMP_TOOL_IDS = tuple("mcp__" + name for name in TOOLS)


def build_server():
    server = MCPServer("Hivemind worker")
    for name in TOOLS:
        server.tool(annotations=mcp.READ_ONLY)(getattr(mcp, name))
    return server


def main():
    build_server().run()


if __name__ == "__main__":
    main()
