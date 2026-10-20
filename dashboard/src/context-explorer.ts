/*
 * Context explorer: pure contracts for the authorized semantic/hybrid exploration UI.
 *
 * Everything here is pure and network-free so the contracts can be verified
 * in-process. The component (ContextExplorer.tsx) owns fetching; this module owns
 * validation, bounded request shaping, and honest labeling of degradation,
 * truncation and evidence scope. No query, evidence or response is ever persisted
 * to browser storage or the URL from here.
 */

export type ExploreMode = "lexical" | "semantic" | "hybrid";

export type ExplorerAgent = {
  id: string;
  display_name: string;
  base_color_token?: string;
  effective_role?: string;
};

export type ExploreResultItem = {
  resource_id: string;
  kind: string;
  revision: number;
  score: number;
  trust: string;
  title: string | null;
  snippet: string;
  current_at_snapshot: boolean;
};

export type GraphNode = {id: string; kind: string | null; revision: number | null; title: string | null};
export type GraphEdge = {source: string; target: string; kind: string};

export type ExploreGraph = {
  seed_ids: string[];
  nodes: GraphNode[];
  edges: GraphEdge[];
  max_hops: number;
  graph_fallback: boolean;
  truncated: boolean;
  warnings: string[];
  scope: string;
};

export type Projections = {
  vector_pending_jobs: number;
  graph_pending_jobs: number;
  stale: boolean;
  recheck: string;
};

export type ExploreLimits = {
  max_results: number;
  max_graph_seeds: number;
  max_hops: number;
  max_nodes: number;
  max_edges: number;
  snippet_chars?: number;
};

export type ExploreResponse = {
  project_id: string;
  mode_requested: ExploreMode;
  mode_effective: string;
  results: ExploreResultItem[];
  result_count: number;
  graph: ExploreGraph | null;
  freshness: Record<string, number>;
  projections: Projections;
  embedding_profile: string | null;
  semantic_available: boolean;
  graph_available: boolean;
  truncated: boolean;
  scan: Record<string, unknown> | null;
  warnings: string[];
  limits: ExploreLimits;
  evidence_policy: string;
};

export type ExploreStatus = {
  project_id: string;
  available: boolean;
  semantic_available: boolean;
  graph_available: boolean;
  modes: ExploreMode[];
  freshness: Record<string, number>;
  projections: Projections;
  limits: Omit<ExploreLimits, "snippet_chars">;
  evidence_policy: string;
};

/* Server-mirrored bounds (hivemind/context_explorer.py). The server is authoritative;
 * these keep the client from ever issuing a request the schema would reject. */
export const MAX_QUERY_CHARS = 4096;
export const MAX_RESULTS = 50;
export const MAX_HOPS = 3;
export const MAX_GRAPH_SEEDS = 8;
export const DEFAULT_LIMIT = 10;
export const DEFAULT_HOPS = 1;

export type QueryValidation = {ok: true; query: string} | {ok: false; message: string};

/** Explicit-submit validation: trimmed, bounded, never auto-fired while typing. */
export function validateQuery(raw: string): QueryValidation {
  const query = raw.trim();
  if (!query) return {ok: false, message: "Enter a query to explore."};
  if (query.length > MAX_QUERY_CHARS) {
    return {ok: false, message: `Query is limited to ${MAX_QUERY_CHARS} characters.`};
  }
  return {ok: true, query};
}

export function clampLimit(value: number): number {
  if (!Number.isFinite(value)) return DEFAULT_LIMIT;
  return Math.max(1, Math.min(MAX_RESULTS, Math.trunc(value)));
}

export function clampHops(value: number): number {
  if (!Number.isFinite(value)) return DEFAULT_HOPS;
  return Math.max(1, Math.min(MAX_HOPS, Math.trunc(value)));
}

export function explorePath(project: string): string {
  return `projects/${encodeURIComponent(project)}/context-explore`;
}

export function statusPath(project: string): string {
  return `projects/${encodeURIComponent(project)}/context-explore:status`;
}

export type ExploreRequest = {
  query: string;
  mode: ExploreMode;
  limit: number;
  expand_graph: boolean;
  graph_seeds: number;
  max_hops: number;
};

/** Shape one bounded request. The query is transmitted, never stored or echoed. */
export function buildRequest(input: {
  query: string;
  mode: ExploreMode;
  limit?: number;
  expandGraph?: boolean;
  graphSeeds?: number;
  maxHops?: number;
}): ExploreRequest | {error: string} {
  const validation = validateQuery(input.query);
  if (!validation.ok) return {error: validation.message};
  return {
    query: validation.query,
    mode: input.mode,
    limit: clampLimit(input.limit ?? DEFAULT_LIMIT),
    expand_graph: input.expandGraph ?? true,
    graph_seeds: Math.max(0, Math.min(MAX_GRAPH_SEEDS, Math.trunc(input.graphSeeds ?? 4))),
    max_hops: clampHops(input.maxHops ?? DEFAULT_HOPS),
  };
}

/** Modes the UI may offer given the status probe; never promises semantic without an encoder. */
export function availableModes(status: ExploreStatus | null): ExploreMode[] {
  if (!status || !status.available) return ["lexical", "semantic", "hybrid"];
  return status.modes.length ? status.modes : ["lexical"];
}

/** Honest headline for the index state; "degraded" labels, never hides them. */
export function indexStateLabel(response: ExploreResponse): string {
  if (response.mode_effective === "lexical_fallback") {
    return "lexical fallback (semantic index unavailable)";
  }
  return response.mode_effective;
}

/** Human-readable degradation/truncation notes derived only from server flags. */
export function degradationNotes(response: ExploreResponse): string[] {
  const notes: string[] = [];
  if (response.mode_effective === "lexical_fallback") {
    notes.push("Semantic ranking is unavailable; results use the authorized lexical/canonical index.");
  }
  if (response.projections?.stale) {
    notes.push("Derivative indexes are still catching up; recall may be incomplete. " +
      "Every shown result was rechecked against current canonical state.");
  }
  if (response.graph?.graph_fallback) {
    notes.push("Graph database unavailable; relationships come from the canonical fallback store.");
  }
  if (response.truncated) {
    notes.push("Results are bounded; more authorized matches or relationships may exist.");
  }
  for (const warning of response.warnings ?? []) {
    if (!notes.some(note => note === warning)) notes.push(warning);
  }
  return notes;
}

/** Counts only what the server returned: authorized, visible nodes/edges. */
export function graphSummary(graph: ExploreGraph | null): string {
  if (!graph) return "";
  const base = `${graph.nodes.length} authorized resource${graph.nodes.length === 1 ? "" : "s"}, ` +
    `${graph.edges.length} relationship${graph.edges.length === 1 ? "" : "s"} (max ${graph.max_hops} hop${graph.max_hops === 1 ? "" : "s"})`;
  return graph.truncated ? `${base} — truncated at bounds` : base;
}

export type AuthFailure = {clearEvidence: boolean; message: string};

/**
 * Authorization/read failures must wipe in-memory evidence: revoked access means
 * previously shown snippets/graph are no longer known to be authorized.
 */
export function classifyFailure(error: unknown): AuthFailure {
  const status = (error as {status?: number} | null)?.status;
  if (status === 401 || status === 403) {
    return {clearEvidence: true, message: "Authorization changed; cleared results."};
  }
  const message = error instanceof Error ? error.message : String(error);
  return {clearEvidence: false, message};
}
