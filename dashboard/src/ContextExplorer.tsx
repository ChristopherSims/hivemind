/*
 * ContextExplorer — authorized semantic/hybrid context exploration panel.
 *
 * Route (wired by the dashboard owner in main.tsx): /projects/{id}/context
 *
 * Contract:
 * - Explicit search submit only; typing never fires requests and nothing is
 *   persisted to browser storage or the URL.
 * - No independent SSE: the shared `refreshVersion` resync re-runs the last
 *   in-memory query so stale/deleted/expired sources never survive a resync.
 * - Every request is fenced by a generation counter across project, permissionKey
 *   and unmount; late responses are dropped, and auth failures clear evidence.
 * - Retrieved text is rendered as plain text (React escaping), never as HTML and
 *   never executed as instructions. Source links are authorized resource ids.
 * - Degradation (lexical fallback, stale projections, graph fallback, truncation)
 *   is surfaced verbatim from server flags; nothing is fabricated.
 */
import React, {useCallback, useEffect, useRef, useState} from "react";
import {api} from "./api";
import {
  availableModes, buildRequest, classifyFailure, clampHops, clampLimit, degradationNotes,
  DEFAULT_HOPS, DEFAULT_LIMIT, graphSummary, indexStateLabel, explorePath, statusPath,
  type ExploreMode, type ExploreResponse, type ExploreStatus, type ExplorerAgent,
} from "./context-explorer";
import "./context-explorer.css";

export type ContextExplorerProps = {
  project: string;
  role: string;
  agents?: ExplorerAgent[];
  refreshVersion?: number;
  permissionKey?: string;
  onError?: (message: string) => void;
  onOpenAgent?: (id: string) => void;
};

export function ContextExplorer(props: ContextExplorerProps) {
  const {project, onError, onOpenAgent} = props;
  const agents = props.agents ?? [];
  const refreshVersion = props.refreshVersion ?? 0;
  const permissionKey = JSON.stringify([props.role, props.permissionKey ?? ""]);

  const [draft, setDraft] = useState("");
  const [mode, setMode] = useState<ExploreMode>("hybrid");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [hops, setHops] = useState(DEFAULT_HOPS);
  const [expandGraph, setExpandGraph] = useState(true);
  const [status, setStatus] = useState<ExploreStatus | null>(null);
  const [response, setResponse] = useState<ExploreResponse | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");

  // Fencing: every in-flight response must match the current generation, which
  // advances on new searches, project/permission changes, resyncs and unmount.
  const generation = useRef(0);
  // The last *submitted* query lives only in memory for the resync re-run.
  const lastRequest = useRef<ReturnType<typeof buildRequest> | null>(null);
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  const clearEvidence = useCallback(() => {
    setResponse(null);
    lastRequest.current = null;
  }, []);

  const fail = useCallback((error: unknown) => {
    const failure = classifyFailure(error);
    if (failure.clearEvidence) clearEvidence();
    setNotice(failure.message);
    onErrorRef.current?.(failure.message);
  }, [clearEvidence]);

  const runSearch = useCallback(async (request: Exclude<ReturnType<typeof buildRequest>, {error: string}>) => {
    const ticket = ++generation.current;
    setBusy(true);
    setNotice("");
    try {
      const result = await api<ExploreResponse>(explorePath(project), "POST", request);
      if (generation.current !== ticket) return; // fenced: stale response
      setResponse(result);
      lastRequest.current = request;
    } catch (error) {
      if (generation.current !== ticket) return;
      fail(error);
    } finally {
      if (generation.current === ticket) setBusy(false);
    }
  }, [project, fail]);

  // Status probe: which modes are truthfully available (no downloads, no fabrication).
  useEffect(() => {
    const ticket = ++generation.current;
    setStatus(null);
    clearEvidence();
    setNotice("");
    void (async () => {
      try {
        const result = await api<ExploreStatus>(statusPath(project));
        if (generation.current !== ticket) return;
        setStatus(result);
        setMode(current => result.modes.includes(current) ? current : (result.modes[0] ?? "lexical"));
      } catch (error) {
        if (generation.current !== ticket) return;
        fail(error);
      }
    })();
  }, [project, permissionKey, clearEvidence, fail]);

  // Shared resync: re-run the last in-memory query so stale/deleted/expired sources
  // cannot survive; never replays anything from storage.
  useEffect(() => {
    if (refreshVersion === 0) return;
    const pending = lastRequest.current;
    if (pending && !("error" in pending)) void runSearch(pending);
  }, [refreshVersion, runSearch]);

  // Unmount fence: late responses after unmount are dropped.
  useEffect(() => () => { generation.current += 1; }, []);

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    const request = buildRequest({query: draft, mode, limit, expandGraph, maxHops: hops});
    if ("error" in request) {
      setNotice(request.error);
      return;
    }
    void runSearch(request);
  };

  const agentName = (id: string) => agents.find(agent => agent.id === id)?.display_name;
  const modes = availableModes(status);
  const notes = response ? degradationNotes(response) : [];

  return <section className="context-explorer" data-testid="context-explorer"
    data-project={project} aria-label="Context explorer">
    <form className="context-explorer-form" onSubmit={submit} data-testid="context-explorer-form">
      <input type="search" data-testid="context-explorer-query" value={draft}
        maxLength={4096} placeholder="Explore project context…" aria-label="Context query"
        onChange={event => setDraft(event.target.value)} />
      <select data-testid="context-explorer-mode" value={mode} aria-label="Search mode"
        onChange={event => setMode(event.target.value as ExploreMode)}>
        {modes.map(option => <option key={option} value={option}>
          {option === "semantic" && status && !status.semantic_available
            ? "semantic (unavailable)" : option}
        </option>)}
      </select>
      <label className="context-explorer-field">Results
        <input type="number" data-testid="context-explorer-limit" min={1} max={50}
          value={limit} onChange={event => setLimit(clampLimit(Number(event.target.value)))} />
      </label>
      <label className="context-explorer-field">Hops
        <input type="number" data-testid="context-explorer-hops" min={1} max={3}
          value={hops} onChange={event => setHops(clampHops(Number(event.target.value)))} />
      </label>
      <label className="context-explorer-field context-explorer-checkbox">
        <input type="checkbox" data-testid="context-explorer-expand-graph"
          checked={expandGraph} onChange={event => setExpandGraph(event.target.checked)} />
        Graph
      </label>
      {/* Never disabled: a new explicit submit supersedes the in-flight one via the
          generation fence, so users are not stuck behind a slow request. */}
      <button type="submit" data-testid="context-explorer-submit">
        {busy ? "Exploring…" : "Explore"}
      </button>
    </form>

    {status && <p className="context-explorer-status" data-testid="context-explorer-status"
      data-semantic={status.semantic_available ? "true" : "false"}
      data-graph={status.graph_available ? "true" : "false"}
      data-stale={status.projections.stale ? "true" : "false"}>
      {status.semantic_available ? "Semantic index available" : "Lexical/canonical search only"}
      {status.graph_available ? " · graph index available" : " · canonical relationship fallback"}
      {status.projections.stale ? " · indexes catching up" : ""}
    </p>}

    {notice && <p role="alert" className="context-explorer-notice"
      data-testid="context-explorer-notice">{notice}</p>}

    {response && <div className="context-explorer-results" data-testid="context-explorer-results"
      data-mode={indexStateLabel(response)} data-truncated={response.truncated ? "true" : "false"}>
      <p className="context-explorer-meta" data-testid="context-explorer-meta">
        {response.result_count} result{response.result_count === 1 ? "" : "s"}
        {" · mode: "}{indexStateLabel(response)}
        {" · canonical through seq "}{response.freshness.canonical_through_ingestion_seq ?? "?"}
      </p>
      {notes.length > 0 && <ul className="context-explorer-notes" data-testid="context-explorer-notes">
        {notes.map(note => <li key={note}>{note}</li>)}
      </ul>}
      <ol className="context-explorer-list">
        {response.results.map(item => <li key={item.resource_id}
          data-testid="context-explorer-result" data-resource-id={item.resource_id}
          data-kind={item.kind} data-revision={item.revision}>
          <header>
            <span className="context-explorer-kind">{item.kind}</span>
            {item.title && <strong className="context-explorer-title">{item.title}</strong>}
            <span className="context-explorer-score">score {item.score.toFixed(4)}</span>
          </header>
          {item.snippet && <p className="context-explorer-snippet">{item.snippet}</p>}
          <footer className="context-explorer-evidence">
            evidence: <code>{item.resource_id}</code> rev {item.revision} · trust {item.trust}
            {item.kind === "agent" && onOpenAgent &&
              <button type="button" data-testid="context-explorer-open-agent"
                data-agent-id={item.resource_id}
                onClick={() => onOpenAgent(item.resource_id)}>
                Open {agentName(item.resource_id) ?? "agent"}
              </button>}
          </footer>
        </li>)}
      </ol>

      {response.graph && <div className="context-explorer-graph"
        data-testid="context-explorer-graph"
        data-fallback={response.graph.graph_fallback ? "true" : "false"}
        data-truncated={response.graph.truncated ? "true" : "false"}>
        <p className="context-explorer-meta" data-testid="context-explorer-graph-summary">
          {graphSummary(response.graph)}
        </p>
        <ul className="context-explorer-nodes">
          {response.graph.nodes.map(node => <li key={node.id}
            data-testid="context-explorer-node" data-node-id={node.id}>
            <code>{node.id}</code>
            {node.title ? ` — ${node.title}` : ""}
            {node.kind ? ` (${node.kind}${node.revision !== null ? ` rev ${node.revision}` : ""})` : ""}
            {node.kind === "agent" && onOpenAgent &&
              <button type="button" data-testid="context-explorer-open-agent"
                data-agent-id={node.id} onClick={() => onOpenAgent(node.id)}>
                Open {agentName(node.id) ?? "agent"}
              </button>}
          </li>)}
        </ul>
        <ul className="context-explorer-edges">
          {response.graph.edges.map(edge =>
            <li key={`${edge.source}:${edge.kind}:${edge.target}`}
              data-testid="context-explorer-edge">
              <code>{edge.source}</code> —{edge.kind}→ <code>{edge.target}</code>
            </li>)}
        </ul>
      </div>}
    </div>}
  </section>;
}

export default ContextExplorer;
