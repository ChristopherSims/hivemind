"""Shared operator wiring for the next agent features."""
import json
import hashlib

import httpx
import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.model_wiring import integration_options
from hivemind.sdk import Client


def test_goal_background_configuration_is_strict_and_default_off(tmp_path):
    filename = tmp_path / "integrations.json"
    filename.write_text(json.dumps({"goal_orchestration": {"background": {}}}))
    options = integration_options(filename)
    assert options["goal_orchestration"]["background"].enabled is False
    filename.write_text(json.dumps({"goal_orchestration": {
        "background": {"enabled": True, "operator_principal_id": "principal_one",
                       "project_ids": ["project_one"], "tick_limit": 2}}}))
    options = integration_options(filename)
    assert options["goal_orchestration"]["background"].enabled is True
    for invalid in ({"enabled": "true"}, {"enabled": True},
                    {"tick_limit": True}, {"tick_limit": 65},
                    {"unknown": "value"}):
        filename.write_text(json.dumps({"goal_orchestration": {"background": invalid}}))
        with pytest.raises(ValueError):
            integration_options(filename)


def test_real_application_installs_goal_routes_without_enabling_background(tmp_path):
    config = json.loads(initialize(tmp_path).read_text())
    app = create_app(tmp_path, workers=False)
    with TestClient(app) as client:
        response = client.post("/v1/goals", json={
            "project_id": config["project_id"], "title": "Compile a goal",
            "statement": "- inspect\n- implement"},
            headers={"Authorization": "Bearer " + config["token"],
                     "Idempotency-Key": "goal-install"})
        assert response.status_code == 200, response.text
        assert len(response.json()["data"]["goal"]["tasks"]) == 2
        assert app.state.goal_runner is None
        assert app.state.service.goal_orchestration is app.state.goal_orchestration


def test_python_goal_facades_preserve_v1_paths_and_keys():
    calls = []

    def respond(request):
        calls.append((request.method, request.url.path,
                      request.headers.get("Idempotency-Key")))
        return httpx.Response(200, json={"data": {"ok": True}})

    client = Client("http://example.test", "local-test",
                    transport=httpx.MockTransport(respond))
    try:
        client.create_goal({}, key="create")
        client.goal("goal_one")
        client.revise_goal("goal_one", {}, key="revise")
        client.tick_goal("goal_one", key="tick")
        client.propose_goal_plan("goal_one", key="propose")
    finally:
        client.http.close()
    assert calls == [
        ("POST", "/v1/goals", "create"),
        ("GET", "/v1/goals/goal_one", None),
        ("POST", "/v1/goals/goal_one:revise", "revise"),
        ("POST", "/v1/goals/goal_one:tick", "tick"),
        ("POST", "/v1/goals/goal_one:propose", "propose")]


def test_budget_counters_reuse_the_hash_pinned_runtime_registry(tmp_path):
    from tokenizers import Tokenizer, models, pre_tokenizers
    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0, "hello": 1, "world": 2}, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
    data = tokenizer.to_str().encode()
    token_file = tmp_path / "tokenizer.json"
    token_file.write_bytes(data)
    filename = tmp_path / "integrations.json"
    config = {
        "runtime_tokenizers": {"require_exact": True, "tokenizers": [{
            "model_id": "local-test", "tokenizer_file": str(token_file),
            "expected_sha256": hashlib.sha256(data).hexdigest()}]},
        "task_budgets": {"policy": {"enabled": True, "directory": str(tmp_path / "budgets")},
                         "counters": {"exact": "local-test"}}}
    filename.write_text(json.dumps(config))
    options = integration_options(filename)
    counter = options["task_budgets"]["counters"]["exact"]
    registry = options["context_assembly"]["registry"]
    assert counter._count is registry.resolve("local-test", require_exact=True)
    assert counter.count("hello world") == 2
    assert options["task_budgets"]["policy"].enabled is True
    config["task_budgets"]["counters"] = {"exact": "not-pinned"}
    filename.write_text(json.dumps(config))
    with pytest.raises(ValueError, match="pinned"):
        integration_options(filename)


@pytest.mark.parametrize("section", [
    {"task_budgets": {"policy": {"enabled": True, "directory": "relative"}}},
    {"task_budgets": {"policy": {"enabled": "true"}}},
    {"task_budgets": {"provider_token": "not-allowed"}},
    {"capability_routing": {"sandbox_verifier": "untrusted-import"}},
    {"capability_routing": {"allowed_runtimes": ["arbitrary-runtime"]}},
    {"artifact_contracts": {"enabled": "true"}},
    {"structured_handoffs": {"allow_ownership_transfer": "true"}},
])
def test_new_feature_configuration_refuses_untrusted_or_unbounded_fields(tmp_path, section):
    filename = tmp_path / "invalid.json"
    filename.write_text(json.dumps(section))
    with pytest.raises(ValueError):
        integration_options(filename)
