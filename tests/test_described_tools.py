import pytest

from hivemind import mcp
from hivemind.tool_context import render_operation
from test_agent_session import client_for


def test_described_mutation_is_allowlisted_digest_bound_and_idempotent(world, monkeypatch):
    operation = "POST /v1/tasks"
    monkeypatch.setattr(mcp, "configuration", lambda: {"allowed_operation_ids": [operation]})
    monkeypatch.setattr(mcp, "client", lambda: client_for(world, world.owner))
    digest = world.call("GET", "tool-context")["digest"]
    request = {"project_id": world.project, "title": "Described task"}
    with pytest.raises(PermissionError):
        mcp.hivemind_execute_operation(operation, digest, False, "described-task", request)
    with pytest.raises(ValueError, match="context changed"):
        mcp.hivemind_execute_operation(operation, "old", True, "described-task", request)
    first = mcp.hivemind_execute_operation(operation, digest, True, "described-task", request)
    second = mcp.hivemind_execute_operation(operation, digest, True, "described-task", request)
    assert first["id"] == second["id"]
    with pytest.raises(PermissionError, match="allowlist"):
        mcp.hivemind_query_operation("GET /v1/ledger", digest)


def test_query_tool_cannot_be_used_as_a_mutation(world, monkeypatch):
    operation = "POST /v1/tasks"
    monkeypatch.setattr(mcp, "configuration", lambda: {"allowed_operation_ids": [operation]})
    monkeypatch.setattr(mcp, "client", lambda: client_for(world, world.owner))
    digest = world.call("GET", "tool-context")["digest"]
    with pytest.raises(ValueError, match="only invokes HTTP GET"):
        mcp.hivemind_query_operation(operation, digest)


@pytest.mark.parametrize("value", ["../task", "task/other", "%2e%2e", ".", ".."])
def test_catalog_renderer_refuses_route_injection(value):
    descriptor = {"path": "/v1/tasks/{task_id}", "inputs": {
        "path_parameters": ["task_id"], "query_parameters": []}}
    with pytest.raises(ValueError):
        render_operation(descriptor, {"task_id": value})
