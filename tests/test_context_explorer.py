"""Canonical tests for the context-explorer aggregation seam.

The module is installed exactly the way the integration layer will install it
(``hivemind.capabilities`` seam), against synthetic temp stores created by the
``world`` fixture. No real ``.hivemind-data`` directory, no encoder/model downloads
(the fixture binds no encoder, so ``lexical_fallback`` is the truthful path), and no
network access.
"""
import pytest
from fastapi import Request

from hivemind import models as m
from hivemind.context_explorer import (
    EVIDENCE_POLICY, MAX_EDGES, MAX_GRAPH_SEEDS, MAX_HOPS, MAX_NODES, MAX_RESULTS,
    SNIPPET_CHARS, install,
)
from hivemind.storage import uid


def install_explorer(world, **options):
    """Install the module the way ``hivemind.capabilities`` will."""
    app = world.app

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    def command(p, operation, key, request, function):  # parity only; never used here
        payload = request.model_dump(by_alias=True, exclude_none=True) \
            if isinstance(request, m.Model) else request
        return envelope(app.state.service.mutate(p, operation, key, payload, function))

    install(app, app.state.service, principal, command, envelope, **options)
    return world


@pytest.fixture
def explorer(world):
    return install_explorer(world)


def explore(world, body, token=None, status=200):
    return world.call("POST", f"projects/{world.project}/context-explore", body,
                      token, status=status)


def status(world, token=None, code=200):
    return world.call("GET", f"projects/{world.project}/context-explore:status",
                      token=token, status=code)


def test_status_reports_truthful_modes_without_encoder(explorer):
    result = status(explorer)
    assert result["available"] is True
    # No encoder is bound in tests: semantic modes are honestly unavailable and the
    # lexical/canonical mode is the only advertised alternative.
    assert result["semantic_available"] is False
    assert result["modes"] == ["lexical"]
    assert result["evidence_policy"] == EVIDENCE_POLICY
    assert result["freshness"]["canonical_through_ingestion_seq"] >= 0
    assert result["limits"]["max_results"] == MAX_RESULTS


def test_search_returns_authorized_results_with_watermarks(explorer):
    explorer.post("quasar lattice navigation header")
    explorer.post("unrelated baking recipe")
    result = explore(explorer, {"query": "quasar lattice", "mode": "hybrid",
                                "expand_graph": False})
    assert result["result_count"] >= 1
    hit = result["results"][0]
    assert hit["resource_id"].startswith("board_post_")
    assert hit["kind"] == "board_post"
    assert hit["current_at_snapshot"] is True
    assert "quasar lattice" in hit["snippet"]
    assert len(hit["snippet"]) <= SNIPPET_CHARS
    # The raw query is never echoed back.
    assert "quasar lattice" not in str({k: v for k, v in result.items() if k != "results"})
    assert result["mode_requested"] == "hybrid"
    # Without an encoder the effective mode is honestly a lexical fallback.
    assert result["mode_effective"] == "lexical_fallback"
    assert any("lexical fallback" in w for w in result["warnings"])
    assert "canonical_through_ingestion_seq" in result["freshness"]
    assert "stale" in result["projections"]
    assert result["evidence_policy"] == EVIDENCE_POLICY


def test_lexical_mode_is_not_labeled_a_fallback(explorer):
    explorer.post("delta pruning strategy")
    result = explore(explorer, {"query": "delta pruning", "mode": "lexical",
                                "expand_graph": False})
    assert result["mode_effective"] == "lexical"
    assert not any("lexical fallback" in w for w in result["warnings"])


def test_graph_expansion_links_authorized_evidence(explorer):
    agent = explorer.agent("Explorer")
    run = explorer.run(agent)
    scope = explorer.scope()
    claim = explorer.claim(agent, run, scope)
    post = explorer.post("graph seed evidence")
    # Canonical relationship fact, written through the store seam the API uses.
    with explorer.app.state.store.lock:
        explorer.app.state.store.relate(explorer.project, claim["id"], post["id"], "evidence")
    result = explore(explorer, {"query": "graph seed evidence", "mode": "lexical",
                                "expand_graph": True, "graph_seeds": 4, "max_hops": 2})
    assert result["graph"] is not None
    node_ids = {node["id"] for node in result["graph"]["nodes"]}
    assert post["id"] in node_ids
    if {claim["id"]} & node_ids:
        assert any(edge["kind"] == "evidence" for edge in result["graph"]["edges"])
    assert result["graph"]["scope"] == "authorized_resources_only"
    assert result["graph"]["max_hops"] == 2
    # Node payloads are bounded summaries, never full bodies.
    for node in result["graph"]["nodes"]:
        assert set(node) <= {"id", "kind", "revision", "title"}


def test_graph_skipped_when_disabled(explorer):
    explorer.post("solo result")
    result = explore(explorer, {"query": "solo result", "expand_graph": False})
    assert result["graph"] is None


def test_deleted_source_does_not_survive_resync(explorer):
    post = explorer.post("ephemeral phoenix token")
    first = explore(explorer, {"query": "phoenix", "mode": "lexical",
                               "expand_graph": False})
    assert any(item["resource_id"] == post["id"] for item in first["results"])
    explorer.call("DELETE", f"posts/{post['id']}", {"expected_revision": post["revision"]})
    second = explore(explorer, {"query": "phoenix", "mode": "lexical",
                                "expand_graph": True})
    assert all(item["resource_id"] != post["id"] for item in second["results"])
    if second["graph"]:
        assert all(node["id"] != post["id"] for node in second["graph"]["nodes"])
        assert all(post["id"] not in (edge["source"], edge["target"])
                   for edge in second["graph"]["edges"])


def test_private_and_foreign_project_resources_never_appear(explorer):
    public = explorer.post("shared telescope finding")
    owner_agent = explorer.agent("Owner")
    private = explorer.call("POST", f"boards/{explorer.board()['id']}/posts", {
        "client_post_id": uid("client"), "kind": "message",
        "content": {"media_type": "text/plain", "text": "shared telescope secret"}},
        explorer.credential(owner_agent))
    # Make the second post private to its author (private_agent is a column, not body).
    store = explorer.app.state.store
    with store.lock:
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (owner_agent["id"], private["id"]))

    other = explorer.agent("Other")
    other_token = explorer.credential(other)
    result = explore(explorer, {"query": "shared telescope", "mode": "lexical",
                                "expand_graph": True}, token=other_token)
    ids = {item["resource_id"] for item in result["results"]}
    assert public["id"] in ids
    assert private["id"] not in ids
    if result["graph"]:
        assert all(node["id"] != private["id"] for node in result["graph"]["nodes"])


def test_cross_project_and_unknown_principal_rejected(explorer):
    body = {"query": "anything", "expand_graph": False}
    explore(explorer, body, token="bogus-token", status=401)
    status(explorer, token="bogus-token", code=401)
    explorer.call("POST", f"projects/{uid('proj')}/context-explore", body, status=404)


def test_bounds_are_enforced_by_schema(explorer):
    explore(explorer, {"query": ""}, status=422)
    explore(explorer, {"query": "x", "limit": MAX_RESULTS + 1}, status=422)
    explore(explorer, {"query": "x", "max_hops": MAX_HOPS + 1}, status=422)
    explore(explorer, {"query": "x", "graph_seeds": MAX_GRAPH_SEEDS + 1}, status=422)
    explore(explorer, {"query": "x", "max_nodes": MAX_NODES + 1}, status=422)
    explore(explorer, {"query": "x", "max_edges": MAX_EDGES + 1}, status=422)
    explore(explorer, {"query": "x", "project_id": explorer.project}, status=422)


def test_truncation_flags_are_honest(explorer):
    for index in range(3):
        explorer.post(f"bounded truncation probe {index}")
    result = explore(explorer, {"query": "truncation probe", "mode": "lexical",
                                "limit": 2, "expand_graph": False})
    assert result["result_count"] <= 2
    assert isinstance(result["truncated"], bool)
    assert result["limits"]["max_results"] == MAX_RESULTS


def test_nothing_is_journaled(explorer):
    explorer.post("journal canary")
    explore(explorer, {"query": "canary raw query text", "mode": "lexical"})
    store = explorer.app.state.store
    rows = store.db.execute("SELECT * FROM commands").fetchall()
    assert all("canary raw query text" not in str(dict(row)) for row in rows)


def test_module_state_is_exposed_for_discovery(explorer):
    assert explorer.app.state.context_explorer["available"] is True
    assert explorer.app.state.context_explorer["evidence_policy"] == EVIDENCE_POLICY
