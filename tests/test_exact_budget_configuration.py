"""End-to-end tests for the exact token budget operator configuration surface.

Owns the ``runtime_tokenizers`` wiring in ``hivemind/model_wiring.py``: the strict
object form (``tokenizers``/``require_exact``/``scan_limit``/``rank_depth``) that binds
hash-pinned local tokenizer counters into one shared counter registry used by both
``context_assembly`` and ``retrieval_runtime``, plus the legacy list form. Everything
runs fully offline against a tiny local fixture ``tokenizer.json``; no network access,
no downloads, no API-supplied tokenizer paths/hashes.
"""

import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from hivemind import context_assembly as ca
from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.model_wiring import integration_options, runtime_counter
from hivemind.storage import canonical

from test_context_assembly import memory_body, principal_for, put

FIXTURE_MODEL = "fixture/tiny-wordlevel"
FIXTURE_TOKENIZER = {
    "version": "1.0", "truncation": None, "padding": None, "added_tokens": [],
    "normalizer": None, "pre_tokenizer": {"type": "Whitespace"}, "post_processor": None,
    "decoder": {"type": "WordPiece", "prefix": "##", "cleanup": True},
    "model": {"type": "WordLevel",
              "vocab": {"hello": 0, "world": 1, "zebra": 2, "token": 3, "[UNK]": 4},
              "unk_token": "[UNK]"},
}


def _fixture_file(tmp_path):
    path = tmp_path / "fixture-tokenizer.json"
    path.write_text(json.dumps(FIXTURE_TOKENIZER))
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _write_config(tmp_path, section):
    path = tmp_path / "integrations.json"
    path.write_text(json.dumps({"runtime_tokenizers": section}))
    return str(path)


def _strict_section(tmp_path, **overrides):
    path, digest = _fixture_file(tmp_path)
    section = {"tokenizers": [{"model_id": FIXTURE_MODEL, "tokenizer_file": str(path),
                               "expected_sha256": digest}],
               "require_exact": True, "scan_limit": 8, "rank_depth": 16}
    section.update(overrides)
    return section


def _world(tmp_path, section):
    """A real app built from operator integration options; owner token from initialize."""
    data = tmp_path / "data"
    data.mkdir()
    owner = json.loads(initialize(data).read_text())
    options = integration_options(_write_config(tmp_path, section))
    app = create_app(data, workers=False, capability_options=options)
    return app, owner


def _headers(world):
    return {"Authorization": "Bearer " + world["token"], "Idempotency-Key": "key-1"}


def _make_agent(client, world):
    response = client.post("/v1/agents", headers=_headers(world), json={
        "project_id": world["project_id"], "display_name": "Budgeted",
        "supported_actions": [], "scope_permit_enforcement": False})
    assert response.status_code == 200, response.text
    return response.json()["data"]


def _assemble_body(world, agent_id, target=FIXTURE_MODEL):
    return {"project_id": world["project_id"],
            "agent_selector": {"agent_ids": [agent_id], "active_only": False},
            "token_budget": 8000, "include": ["memory"], "target_tokenizer": target}


def test_strict_wiring_bound_target_counts_exactly(tmp_path):
    app, owner = _world(tmp_path, _strict_section(tmp_path))
    with TestClient(app) as client:
        agent = _make_agent(client, owner)
        store = app.state.store
        principal = principal_for(type("W", (), {"app": app, "owner": owner["token"]}))
        run = client.post("/v1/runs", headers=_headers(owner), json={
            "project_id": owner["project_id"], "agent_id": agent["id"]}).json()["data"]
        put(store, "memory", owner["project_id"], principal["id"], memory_body(
            owner["project_id"], run, title="Seam", text="hello world"))

        response = client.post("/v1/context:assemble", headers=_headers(owner),
                               json=_assemble_body(owner, agent["id"]))
        assert response.status_code == 200, response.text
        assert response.json()["hivemind_detached_work"] is True
        packet = response.json()["data"]
        assert packet["tokenization"]["exact_for_target_tokenizer"] is True
        assert packet["tokenization"]["method"] == "operator_bound_local_runtime_tokenizer"
        assert packet["tokenization"]["vendor_overhead"]["included"] is False
        # The pinned fixture counter measures the complete response envelope exactly.
        counter = runtime_counter(**_strict_section(tmp_path)["tokenizers"][0])
        assert packet["token_count"] == counter(canonical(response.json()))
        assert counter("hello world") == 2  # fixture sanity

        # The same registry serves retrieval discovery; counters are listed honestly.
        operations = client.get("/v1/retrieval:operations", headers=_headers(owner))
        assert operations.status_code == 200, operations.text
        listed = operations.json()["data"]
        assert {op["name"] for op in listed["operations"]} >= {
            "retrieval.search_page", "counter.resolve", "context.assemble"}


def test_strict_wiring_unbound_target_refused_422_before_any_scan(tmp_path, monkeypatch):
    app, owner = _world(tmp_path, _strict_section(tmp_path))
    with TestClient(app) as client:
        agent = _make_agent(client, owner)

        def forbidden_scan(*args, **kwargs):
            raise AssertionError("candidate scan executed despite missing exact counter")

        monkeypatch.setattr(ca, "_scan", forbidden_scan)
        for target in ("vendor/hidden-model", None):
            body = _assemble_body(owner, agent["id"], target)
            if target is None:
                body.pop("target_tokenizer")
            refused = client.post("/v1/context:assemble", headers=_headers(owner), json=body)
            assert refused.status_code == 422, refused.text
            assert refused.json()["error"]["code"] == "exact_counter_unavailable"


def test_offline_traps_and_no_network_anywhere(tmp_path, monkeypatch):
    import socket
    import urllib.request

    def no_network(*args, **kwargs):
        raise AssertionError("network access attempted")

    real_connect = socket.socket.connect

    def no_egress(self, address, *args, **kwargs):
        # Loopback is the test client/event loop itself; only external egress is trapped.
        host = address[0] if isinstance(address, tuple) else address
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError("network access attempted: %r" % (address,))
        return real_connect(self, address, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", no_egress)
    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    monkeypatch.delenv("HF_HUB_CACHE", raising=False)
    monkeypatch.setenv("HF_HOME", str(tmp_path / "empty-home"))

    app, owner = _world(tmp_path, _strict_section(tmp_path))
    with TestClient(app) as client:
        agent = _make_agent(client, owner)
        response = client.post("/v1/context:assemble", headers=_headers(owner),
                               json=_assemble_body(owner, agent["id"]))
        assert response.status_code == 200, response.text


def test_hash_mismatch_refused_at_configuration_time(tmp_path):
    path, _digest = _fixture_file(tmp_path)
    section = [{"model_id": FIXTURE_MODEL, "tokenizer_file": str(path),
                "expected_sha256": "0" * 64}]
    with pytest.raises(ValueError, match="identity/hash mismatch"):
        integration_options(_write_config(tmp_path, section))
    # A file swapped after a valid configuration is never re-read silently either:
    # counters were parsed from the verified bytes at configuration time.


def test_unknown_or_loose_configuration_denied(tmp_path):
    path, digest = _fixture_file(tmp_path)
    item = {"model_id": FIXTURE_MODEL, "tokenizer_file": str(path),
            "expected_sha256": digest}
    cases = [
        {"tokenizers": [item], "bogus": True},                       # unknown section key
        {"tokenizers": [{**item, "source": "https://x"}]},           # unknown item key
        {"tokenizers": [item], "require_exact": "true"},             # bool must be a bool
        {"tokenizers": [item], "require_exact": 1},
        {"tokenizers": [item], "scan_limit": True},                  # bool is not an int
        {"tokenizers": [item], "scan_limit": 0},
        {"tokenizers": [item], "scan_limit": 200001},                # above MAX_SCAN_LIMIT
        {"tokenizers": [item], "rank_depth": 65537},                 # above RANK_DEPTH_MAX
        {"tokenizers": [item], "scan_limit": 5.5},                   # no floats
        "not-a-list-of-objects",                                     # wrong shape
    ]
    for section in cases:
        with pytest.raises(ValueError):
            integration_options(_write_config(tmp_path, section))
    # Unknown top-level sections are denied by the integration loader itself.
    top = tmp_path / "integrations.json"
    top.write_text(json.dumps({"mystery_section": {}}))
    with pytest.raises(ValueError, match="Unknown integration configuration section"):
        integration_options(str(top))
    # Non-finite JSON numbers (Infinity/NaN) are denied outright.
    top.write_text('{"runtime_tokenizers": {"tokenizers": [], "scan_limit": Infinity}}')
    with pytest.raises(ValueError, match="finite"):
        integration_options(str(top))


def test_legacy_list_format_backcompat(tmp_path):
    path, digest = _fixture_file(tmp_path)
    legacy = [{"model_id": FIXTURE_MODEL, "tokenizer_file": str(path),
               "expected_sha256": digest}]
    options = integration_options(_write_config(tmp_path, legacy))
    # Legacy shape: only the context assembly counters are configured.
    assert set(options) == {"context_assembly"}
    assert set(options["context_assembly"]) == {"counters"}
    assert FIXTURE_MODEL in options["context_assembly"]["counters"]

    data = tmp_path / "data"
    data.mkdir()
    owner = json.loads(initialize(data).read_text())
    app = create_app(data, workers=False, capability_options=options)
    with TestClient(app) as client:
        agent = _make_agent(client, owner)
        bound = client.post("/v1/context:assemble", headers=_headers(owner),
                            json=_assemble_body(owner, agent["id"]))
        assert bound.status_code == 200, bound.text
        assert bound.json()["data"]["tokenization"]["exact_for_target_tokenizer"] is True
        # No strict policy: an unbound target honestly degrades to the labeled fallback
        # instead of failing (old behavior preserved).
        unbound = client.post("/v1/context:assemble", headers=_headers(owner),
                              json=_assemble_body(owner, agent["id"], "vendor/other"))
        assert unbound.status_code == 200, unbound.text
        tokenization = unbound.json()["data"]["tokenization"]
        assert tokenization["exact_for_target_tokenizer"] is False


def test_operator_scan_limits_cannot_be_widened_by_requests(tmp_path):
    app, owner = _world(tmp_path, _strict_section(tmp_path, scan_limit=3))
    with TestClient(app) as client:
        agent = _make_agent(client, owner)
        store = app.state.store
        principal = principal_for(type("W", (), {"app": app, "owner": owner["token"]}))
        run = client.post("/v1/runs", headers=_headers(owner), json={
            "project_id": owner["project_id"], "agent_id": agent["id"]}).json()["data"]
        for index in range(10):
            put(store, "memory", owner["project_id"], principal["id"], memory_body(
                owner["project_id"], run, title=f"Zebra {index}",
                text=f"zebra scan policy content {index}"))

        query = {"project_id": owner["project_id"], "query": "zebra", "mode": "lexical"}
        # Clients cannot even name scan policy fields (schema forbids extras).
        widened = client.post("/v1/retrieval:search-page", headers=_headers(owner),
                              json={**query, "scan_limit": 200000})
        assert widened.status_code == 422, widened.text
        widened_depth = client.post("/v1/retrieval:search-page", headers=_headers(owner),
                                    json={**query, "rank_depth": 65536})
        assert widened_depth.status_code == 422, widened_depth.text
        # The operator-fixed bound applies, and its exhaustion is honestly flagged.
        page = client.post("/v1/retrieval:search-page", headers=_headers(owner), json=query)
        assert page.status_code == 200, page.text
        data = page.json()["data"]
        assert data["hivemind_detached_work"] is True
        assert data["scan"]["scan_limit"] == 3
        assert data["scan"]["rank_depth"] == 16
        assert data["exhausted"]["scan_truncated"] is True
        # Context assembly carries the same operator-fixed scan bound.
        assembled = client.post("/v1/context:assemble", headers=_headers(owner),
                                json=_assemble_body(owner, agent["id"]))
        assert assembled.status_code == 200, assembled.text
        assert assembled.json()["data"]["scan"]["scan_limit"] == 3
