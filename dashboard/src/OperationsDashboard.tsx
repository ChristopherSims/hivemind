/**
 * Operations health dashboard (read-only).
 *
 * Renders exactly what GET /v1/projects/{id}/operations-summary measured:
 * canonical ingestion vs. semantic/graph projection watermarks, bounded job
 * and delivery counters, heartbeat presence vs. reported capture health, and
 * coarse degraded-service warnings. Unknown and unavailable sections render
 * as unknown -- never as healthy. The component owns no SSE stream; the
 * parent invalidates it through `refreshVersion`. Wired by the dashboard
 * owner at route /projects/{id}/operations.
 */
import React, {useCallback, useEffect, useRef, useState} from "react";
import {
  CAPTURE_LABELS, DELIVERY_LABELS, DELIVERY_STATES, fetchOperationsSummary, lagLabel,
  PRESENCE_LABELS, sampleTimeLabel, summarizeWarnings, warningTitle, windowNote,
  type OperationsSummary,
} from "./operations-dashboard";
import "./operations-dashboard.css";

export type OperationsDashboardAgent = {
  id: string;
  display_name: string;
  base_color_token?: string;
  effective_role?: string;
};

export type OperationsDashboardProps = {
  project: string;
  role: string;
  agents?: OperationsDashboardAgent[];
  /** Parent-driven invalidation; bump to refetch. No own SSE. */
  refreshVersion?: number;
  /** Changes with the permission context; fenced into the request scope. */
  permissionKey?: string;
  onError?: (message: string) => void;
  onOpenAgent?: (id: string) => void;
};

export function useOperationsSummary(options: {
  project: string; refreshVersion?: number; permissionKey?: string;
  onError?: (message: string) => void;
}) {
  const {project, refreshVersion = 0, permissionKey = "", onError} = options;
  const scope = JSON.stringify([project, permissionKey]);
  const activeScope = useRef(scope);
  activeScope.current = scope;
  const generation = useRef(0);
  const ticket = useRef(0);
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;
  const [summary, setSummary] = useState<OperationsSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    if (!project) { setSummary(null); setLoading(false); return; }
    const version = generation.current;
    const request = ++ticket.current;
    setLoading(true);
    try {
      const next = await fetchOperationsSummary(project);
      // Drop evidence for a superseded project/session/permission/unmount.
      if (version !== generation.current || request !== ticket.current
          || scope !== activeScope.current) return;
      setSummary(next);
      setError("");
    } catch (e) {
      if (version !== generation.current || request !== ticket.current
          || scope !== activeScope.current) return;
      // 401/403 already clear privileged UI via the shared authorization handler;
      // a failed read drops the previous evidence rather than going stale.
      setSummary(null);
      const message = String(e);
      setError(message);
      onErrorRef.current?.(message);
    } finally {
      if (version === generation.current) setLoading(false);
    }
  }, [project, scope]);

  useEffect(() => {
    generation.current++;
    setSummary(null);
    setError("");
    setLoading(true);
    return () => { generation.current++; };
  }, [scope]);

  useEffect(() => { void load(); }, [load, refreshVersion]);

  return {summary, loading, error, refresh: load};
}

function Section({title, note, available, reason, children, testId}: {
  title: string; note?: string; available: boolean; reason?: string;
  children?: React.ReactNode; testId: string;
}) {
  return <section className="operations-section" data-testid={testId}
                  data-available={available ? "true" : "false"}>
    <header className="operations-section-header">
      <h2>{title}</h2>
      {!available && <span className="operations-unknown" role="status">
        Unknown{reason ? ` — ${reason.replace(/_/g, " ")}` : ""}</span>}
    </header>
    {available && children}
    {available && note && <p className="operations-note">{note}</p>}
  </section>;
}

function CountRow({label, value, tone}: {label: string; value: number | null | undefined; tone?: string}) {
  return <div className={`operations-count ${tone ?? ""}`}>
    <span className="operations-count-value">{value ?? "Unknown"}</span>
    <span className="operations-count-label">{label}</span>
  </div>;
}

export function OperationsDashboard({project, role, agents = [], refreshVersion = 0,
  permissionKey = "", onError, onOpenAgent}: OperationsDashboardProps) {
  const {summary, loading, error, refresh} = useOperationsSummary({
    project, refreshVersion, permissionKey: JSON.stringify([role, permissionKey]), onError});
  const agentName = useCallback((id: string | null) =>
    (id && agents.find(agent => agent.id === id)?.display_name) || id || "Unknown agent",
  [agents]);

  return <div className="operations-dashboard" data-testid="operations-dashboard"
              data-project={project} data-role={role}>
    <header className="operations-header">
      <div>
        <p className="eyebrow">PROJECT OPERATIONS</p>
        <h1>Measured health, and what is unknown.</h1>
        {summary && <p className="operations-sample" data-testid="operations-generated-at">
          Sampled {sampleTimeLabel(summary.generated_at_unix_seconds)} · scope {summary.scope} ·
          role {summary.role}</p>}
      </div>
      <button type="button" data-testid="operations-refresh" disabled={loading}
              onClick={() => void refresh()}>
        {loading ? "Sampling…" : "Resample"}</button>
    </header>

    {error && <p className="operations-error" role="alert" data-testid="operations-error">{error}</p>}
    {!summary && !loading && !error &&
      <p className="operations-unknown" data-testid="operations-empty">No operations evidence.</p>}

    {summary && <>
      <section className="operations-warnings" data-testid="operations-warnings">
        <h2>Degraded-service warnings</h2>
        {summary.warnings.length === 0
          ? <p className="operations-note" data-testid="operations-no-warnings">
              No measured warnings in this sample.</p>
          : <ul>{summarizeWarnings(summary.warnings).map(warning =>
              <li key={warning.code} className="operations-warning" data-testid="operations-warning"
                  data-code={warning.code}>
                <strong>{warning.title}</strong>
                {warning.count !== null && <span className="operations-warning-count">
                  {warning.count}</span>}
                <span className="operations-warning-message">{warning.message}</span>
              </li>)}</ul>}
      </section>

      <Section title="Canonical ingestion and projections" testId="operations-ingestion"
               available={summary.ingestion.available} reason={summary.ingestion.reason}>
        {summary.ingestion.vector && summary.ingestion.graph && <>
          <dl className="operations-watermarks">
            <div data-testid="operations-canonical">
              <dt>Canonical ingestion</dt>
              <dd>through seq {summary.ingestion.canonical_through_ingestion_seq}</dd>
            </div>
            <div data-testid="operations-vector">
              <dt>Semantic / vector</dt>
              <dd>through seq {summary.ingestion.vector.through_ingestion_seq} ·{" "}
                {lagLabel(summary.ingestion.vector)}</dd>
            </div>
            <div data-testid="operations-graph">
              <dt>Graph projection</dt>
              <dd>through seq {summary.ingestion.graph.through_ingestion_seq} ·{" "}
                {lagLabel(summary.ingestion.graph)}</dd>
            </div>
          </dl>
          <p className="operations-note" data-testid="operations-read-path">
            Retrieval read path: {summary.ingestion.index_read_path === "dedicated"
              ? "dedicated index connection" : "canonical fallback"} · graph projection{" "}
            {summary.ingestion.graph_projection} · semantic encoder{" "}
            {summary.ingestion.semantic_encoder}.
          </p>
        </>}
      </Section>

      <Section title="Indexing queue" testId="operations-jobs"
               available={summary.jobs.available} reason={summary.jobs.reason}>
        {summary.jobs.project && <>
          <div className="operations-counts" data-testid="operations-jobs-project">
            <CountRow label="Pending" value={summary.jobs.project.total.pending}/>
            <CountRow label="Done" value={summary.jobs.project.total.done}/>
            <CountRow label="Dead" value={summary.jobs.project.total.dead}
                      tone={summary.jobs.project.total.dead ? "bad" : ""}/>
          </div>
          <p className="operations-note">Project scope.
            {Object.entries(summary.jobs.project.by_kind).map(([kind, counts]) =>
              <span key={kind} data-testid={`operations-jobs-kind-${kind}`}>
                {" "}{kind}: {counts.pending} pending / {counts.dead} dead.</span>)}
          </p>
        </>}
        {summary.jobs.global && <p className="operations-note" data-testid="operations-jobs-global">
          Global (all projects, administrator only):{" "}
          {Object.entries(summary.jobs.global.by_state)
            .map(([state, count]) => `${state} ${count}`).join(", ") || "no jobs"}.
        </p>}
      </Section>

      <Section title="Delivery queue" testId="operations-delivery"
               available={summary.delivery.available} reason={summary.delivery.reason}
               note={summary.delivery.window_note}>
        {summary.delivery.states && <>
          <div className="operations-counts" data-testid="operations-delivery-states">
            {DELIVERY_STATES.map(state =>
              <CountRow key={state} label={DELIVERY_LABELS[state]}
                        value={summary.delivery.states![state]}
                        tone={state === "dead_letter" && summary.delivery.states![state]
                          ? "bad" : ""}/>)}
          </div>
          {windowNote(summary.delivery.scanned, summary.delivery.limit,
                      summary.delivery.truncated) &&
            <p className="operations-note" data-testid="operations-delivery-truncated">
              {windowNote(summary.delivery.scanned, summary.delivery.limit,
                          summary.delivery.truncated)}</p>}
        </>}
      </Section>

      <Section title="Runs: presence and capture" testId="operations-runs"
               available={summary.runs.available} reason={summary.runs.reason}
               note={summary.runs.presence_note}>
        {summary.runs.presence && summary.runs.capture && <>
          <div className="operations-counts" data-testid="operations-presence">
            {(Object.keys(PRESENCE_LABELS) as (keyof typeof PRESENCE_LABELS)[]).map(state =>
              <CountRow key={state} label={PRESENCE_LABELS[state]}
                        value={summary.runs.presence![state]}/>)}
          </div>
          <div className="operations-counts" data-testid="operations-capture">
            {(Object.keys(CAPTURE_LABELS) as (keyof typeof CAPTURE_LABELS)[]).map(state =>
              <CountRow key={state} label={`Capture: ${CAPTURE_LABELS[state].toLowerCase()}`}
                        value={summary.runs.capture![state]}
                        tone={state === "gaps" && summary.runs.capture![state] ? "bad" : ""}/>)}
          </div>
          {summary.runs.sample && summary.runs.sample.length > 0 &&
            <table className="operations-runs-table" data-testid="operations-runs-sample">
              <thead><tr>
                <th>Run</th><th>Agent</th><th>Status</th><th>Presence</th><th>Capture</th><th>Last seen</th>
              </tr></thead>
              <tbody>{summary.runs.sample.map(run =>
                <tr key={run.id} data-testid="operations-run" data-run-id={run.id}
                    data-presence={run.presence} data-capture={run.capture_state}>
                  <td className="operations-mono">{run.id}</td>
                  <td>{onOpenAgent && run.agent_id
                    ? <button type="button" className="operations-agent-link"
                              onClick={() => run.agent_id && onOpenAgent(run.agent_id)}>
                        {agentName(run.agent_id)}</button>
                    : agentName(run.agent_id)}</td>
                  <td>{run.status ?? "unknown"}</td>
                  <td>{PRESENCE_LABELS[run.presence]}</td>
                  <td>{CAPTURE_LABELS[run.capture_state]}</td>
                  <td>{sampleTimeLabel(run.last_seen_at)}</td>
                </tr>)}</tbody>
            </table>}
          {summary.runs.sample_truncated &&
            <p className="operations-note" data-testid="operations-runs-truncated">
              Sample shows the newest {summary.runs.sample_limit} runs; buckets cover{" "}
              {summary.runs.scanned}.</p>}
        </>}
      </Section>

      {role === "admin" &&
        <Section title="Diagnostics (telemetry)" testId="operations-telemetry"
                 available={summary.telemetry.available} reason={summary.telemetry.reason}>
          {summary.telemetry.available && <div className="operations-counts">
            <CountRow label="Queued" value={summary.telemetry.queued}/>
            <CountRow label="Dropped (node-wide)" value={summary.telemetry.dropped}
                      tone={summary.telemetry.dropped ? "bad" : ""}/>
          </div>}
          {summary.telemetry.available && <p className="operations-note">
            {summary.telemetry.enabled ? "Enabled" : "Disabled"} · content capture off · delivery{" "}
            {summary.telemetry.delivery}. Diagnostics are not execution evidence.</p>}
        </Section>}
    </>}
  </div>;
}

export {warningTitle};
