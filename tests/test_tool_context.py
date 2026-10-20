def test_agent_discovery_describes_queries_pushes_and_authority(world):
    discovery = world.client.get("/.well-known/hivemind").json()
    assert discovery["agent_tool_context"]["authentication_required"]
    assert world.client.get("/v1/tool-context").status_code == 401
    catalog = world.call("GET", "tool-context")
    assert len(catalog["digest"]) == 64
    operations = {item["id"]: item for item in catalog["operations"]}
    push = operations["POST /v1/git-workspaces/{identity}:push"]
    assert "do not publish externally" in push["context"]
    assert "Idempotency-Key" in push["retry_safety"]
    permit = operations["POST /v1/runs/{run_id}/permits:check"]
    assert permit["side_effects"] == "No domain mutation intended."
    assert "no new claim" in permit["context"]
    assert all(item["description"] and item["authority"] and item["inputs"]
               and item["untrusted_data"] for item in operations.values())
    selected = world.call("GET", "tool-context?operation_id=GET%20%2Fv1%2Fledger")
    assert [item["id"] for item in selected["operations"]] == ["GET /v1/ledger"]
    assert selected["digest"] == catalog["digest"]
    schema = world.client.get("/openapi.json").json()
    assert "do not publish externally" in schema["paths"][
        "/v1/git-workspaces/{identity}:push"]["post"]["description"]
