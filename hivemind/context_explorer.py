"""Authorized context-explorer aggregation seam for the dashboard.

Why a separate module
---------------------
The dashboard context explorer needs a *single* read-only round trip that combines
hybrid/semantic search, optional bounded graph expansion of the top authorized hits,
projection freshness watermarks and honest truncation/degradation flags. The shared
routes (``POST /v1/search``, ``POST /v1/graph:query``) stay untouched; this module is
a thin composition over the existing retrieval APIs (``Indexes.search`` /
``Indexes.query_graph``) and never re-implements authorization, visibility, memory
validity, or index recheck logic.

Guarantees and honest limits
----------------------------
* **Canonical authorization only.** Authentication uses the *current*
  ``app.state.authenticate_request`` (looked up per request, in the store lock), then
  ``Service.project_access``. Every result and every graph node/edge is materialized
  through ``Indexes`` canonical recheck, so revoked, deleted, expired or drifted
  resources never appear, and private intermediates are never traversed.
* **Read-only, never journaled.** The explore route never calls ``command`` /
  ``Service.mutate``: raw queries and returned context are not written into replayable
  command records. The request body (including the query text) is never echoed back,
  never logged, and never stored.
* **Bounded by construction.** Result limits, graph seeds, hops, node and edge budgets
  are server-capped (see ``LIMITS``). Graph traversal uses ``Indexes.query_graph``
  bounds; truncation at any budget is reported via ``truncated`` flags rather than
  silently dropped.
* **Truthful degradation.** ``lexical_fallback`` comes straight from the retrieval
  meta; ``graph_fallback`` from ``query_graph``; ``projections.stale`` is derived from
  canonical freshness watermarks (pending embedding/graph jobs). No semantic matches,
  graph edges, freshness or provenance are ever fabricated. When no encoder is bound,
  ``status.semantic_available`` is false and the lexical/canonical modes are the
  truthful alternatives -- this module never downloads models or starts services.
* **Authorized identifiers only.** Node ids in graph responses are canonical resource
  ids the principal may currently see. Counts of *hidden* (unauthorized or recheck-
  failed) candidates are never reported, so their existence is not leaked.
* **Retrieved text is evidence, not instructions.** Snippets are truncated plain text
  for display; nothing here executes, renders as HTML, or feeds retrieved content into
  any interpreter.

Integration contract (owned by the integration layer)
-----------------------------------------------------
Register by adding ``"context_explorer"`` to ``hivemind/capabilities.py``'s
``FEATURE_MODULES`` -- the standard ``install(app, service, principal, command,
envelope)`` seam, no other wiring. ``install`` also sets
``app.state.context_explorer`` so the discovery route may advertise the capability.
"""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import Request
from pydantic import Field

from . import models as m
from .storage import searchable

#: Server-fixed ceilings. Clients cannot widen any of them; the response echoes the
#: effective bounds under ``limits`` so the UI can render honest truncation notes.
MAX_RESULTS = 50
MAX_GRAPH_SEEDS = 8
MAX_HOPS = 3
MAX_NODES = 100
MAX_EDGES = 200
SNIPPET_CHARS = 280

RESOURCE_KINDS = ("agent", "board_post", "memory", "task", "work_claim", "artifact")

#: Stable, machine-readable labels the dashboard renders verbatim; no silent modes.
MODE_EFFECTIVE = ("semantic", "hybrid", "lexical", "lexical_fallback")
EVIDENCE_POLICY = "results_and_graph_include_only_currently_authorized_resources"


class ExploreBody(m.Model):
    """One explicit context-exploration request. ``project_id`` comes from the path,
    never the body, so a body cannot retarget another project."""

    query: str = Field(min_length=1, max_length=4096)
    mode: Literal["lexical", "semantic", "hybrid"] = "hybrid"
    limit: int = Field(default=10, ge=1, le=MAX_RESULTS)
    resource_kinds: list[Literal["agent", "board_post", "memory", "task", "work_claim",
                                 "artifact"]] = Field(default_factory=list, max_length=6)
    expand_graph: bool = True
    graph_seeds: int = Field(default=4, ge=0, le=MAX_GRAPH_SEEDS)
    max_hops: int = Field(default=1, ge=1, le=MAX_HOPS)
    max_nodes: int = Field(default=40, ge=1, le=MAX_NODES)
    max_edges: int = Field(default=80, ge=1, le=MAX_EDGES)


def _snippet(body):
    """Bounded plain-text excerpt of an authorized body. Never executed or rendered
    as markup; truncation is explicit."""
    text = " ".join(searchable(body).split())
    return text[:SNIPPET_CHARS]


def _label(body):
    for key in ("title", "display_name", "canonical_key", "intent"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:160]
    return None


def _projections(freshness):
    vector_pending = freshness.get("vector_pending_jobs", 0)
    graph_pending = freshness.get("graph_pending_jobs", 0)
    return {
        "vector_pending_jobs": vector_pending,
        "graph_pending_jobs": graph_pending,
        # Projections (derivative indexes) lag canonical state; results remain correct
        # because of canonical recheck, but ranking/graph recall may be incomplete.
        "stale": bool(vector_pending or graph_pending),
        "recheck": "every result and edge is rechecked against canonical state at "
                   "materialization; stale projections can reduce recall, never "
                   "resurrect revoked/deleted/superseded resources",
    }


def _scan_truncated(scan):
    if not isinstance(scan, dict):
        return False
    per_kind = scan.get("scan_truncated")
    return bool(scan.get("rank_truncated") or scan.get("time_budget_exceeded")
                or (isinstance(per_kind, dict) and any(per_kind.values())))


def explore(service, principal, project_id, body):
    """Compose one authorized search (+ optional graph expansion) response.

    Runs entirely on existing retrieval APIs. Raises ``Problem`` unchanged from the
    underlying seams (``not_found``, ``graph_limit`` ...); no error is swallowed into
    a fabricated empty result.
    """
    indexes = service.indexes
    search_request = m.SearchQuery(project_id=project_id, query=body.query,
                                   mode=body.mode, limit=body.limit,
                                   resource_kinds=list(body.resource_kinds))
    found = indexes.search(principal, search_request)

    freshness = found.get("freshness") or {}
    lexical_fallback = bool(found.get("lexical_fallback"))
    warnings = list(found.get("warnings") or [])

    results = []
    for item in found.get("results", []):
        resource = item.get("resource") or {}
        results.append({
            "resource_id": item["resource_id"],
            "kind": item["kind"],
            "revision": item["revision"],
            "score": item["score"],
            "trust": item.get("trust", "structured"),
            "title": _label(resource),
            "snippet": _snippet(resource),
            # Materialization rechecked canonical state; the freshness watermarks below
            # bound how current derivative projections were at ranking time.
            "current_at_snapshot": True,
        })

    truncated = _scan_truncated(found.get("scan"))
    if lexical_fallback and body.mode != "lexical":
        warnings = [*warnings, "semantic index unavailable; truthful lexical fallback"]

    graph = None
    if body.expand_graph and body.graph_seeds and results:
        seed_ids = [item["resource_id"] for item in results[:body.graph_seeds]]
        graph_result = indexes.query_graph(principal, m.GraphQuery(
            project_id=project_id, seed_ids=seed_ids, max_hops=body.max_hops,
            max_nodes=body.max_nodes, max_edges=body.max_edges))
        nodes = []
        for node in graph_result.get("nodes", []):
            nodes.append({"id": node["id"], "kind": node.get("kind"),
                          "revision": node.get("revision"), "title": _label(node)})
        edges = [{"source": edge["source"], "target": edge["target"], "kind": edge["kind"]}
                 for edge in graph_result.get("edges", [])]
        graph_truncated = (len(nodes) >= body.max_nodes or len(edges) >= body.max_edges)
        graph = {
            "seed_ids": seed_ids,
            "nodes": nodes,
            "edges": edges,
            "max_hops": body.max_hops,
            "graph_fallback": bool(graph_result.get("graph_fallback")),
            # Bounds were reached; more authorized relationships may exist.
            "truncated": graph_truncated,
            "warnings": list(graph_result.get("warnings") or []),
            # Only authorized, currently visible resources are included; hidden ones
            # are absent and their count is never disclosed.
            "scope": "authorized_resources_only",
        }
        truncated = truncated or graph_truncated
        warnings.extend(graph["warnings"])

    return {
        "project_id": project_id,
        "mode_requested": body.mode,
        "mode_effective": ("lexical_fallback" if lexical_fallback and body.mode != "lexical"
                           else body.mode),
        "results": results,
        "result_count": len(results),
        "graph": graph,
        "freshness": freshness,
        "projections": _projections(freshness),
        "embedding_profile": found.get("embedding_profile"),
        "semantic_available": indexes.encoder is not None,
        "graph_available": indexes.graph is not None,
        "truncated": truncated,
        "scan": found.get("scan"),
        "warnings": warnings,
        "limits": {"max_results": MAX_RESULTS, "max_graph_seeds": MAX_GRAPH_SEEDS,
                   "max_hops": MAX_HOPS, "max_nodes": MAX_NODES, "max_edges": MAX_EDGES,
                   "snippet_chars": SNIPPET_CHARS},
        "evidence_policy": EVIDENCE_POLICY,
        "hivemind_detached_work": True,
    }


def install(app, service, principal, command, envelope, **_options):
    """Register the read-only context-explorer routes on ``app``.

    ``command`` is accepted for signature parity and deliberately unused: exploration
    is read-only and must not journal raw queries or returned context into replayable
    command records.
    """

    def authenticate(request: Request):
        # Current authenticator, looked up per request and evaluated in the store lock
        # by the parent; this module never re-implements credential checks.
        return app.state.authenticate_request(request)

    @app.get("/v1/projects/{project_id}/context-explore:status")
    async def context_explore_status(project_id: str, request: Request):
        p = authenticate(request)
        service.project_access(p, project_id)
        indexes = service.indexes
        freshness = await asyncio.to_thread(service.store.freshness, project_id)
        return envelope({
            "project_id": project_id,
            "available": True,
            "semantic_available": indexes.encoder is not None,
            "graph_available": indexes.graph is not None,
            # Truthful alternatives when no encoder is bound; nothing is downloaded.
            "modes": (["lexical", "semantic", "hybrid"] if indexes.encoder is not None
                      else ["lexical"]),
            "freshness": freshness,
            "projections": _projections(freshness),
            "limits": {"max_results": MAX_RESULTS, "max_graph_seeds": MAX_GRAPH_SEEDS,
                       "max_hops": MAX_HOPS, "max_nodes": MAX_NODES,
                       "max_edges": MAX_EDGES},
            "evidence_policy": EVIDENCE_POLICY,
        })

    @app.post("/v1/projects/{project_id}/context-explore")
    async def context_explore(project_id: str, body: ExploreBody, request: Request):
        p = authenticate(request)
        service.project_access(p, project_id)
        # Indexes.search / query_graph are two-phase: short canonical snapshots and a
        # lock-free read-only ranking connection, so the handler is marked detached and
        # the parent middleware holds no global lock during compute.
        result = await asyncio.to_thread(explore, service, p, project_id, body)
        return envelope(result)

    context_explore.hivemind_detached_work = True

    app.state.context_explorer = {"available": True, "evidence_policy": EVIDENCE_POLICY}
    return app
