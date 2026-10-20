import React, {useEffect, useRef, useState} from "react";
import {api, ApiError} from "./api";
import "./agent-details.css";

export type EvidenceRef = {kind: string; id: string; revision?: number};

export type AgentLedgerRun = {
  id: string; revision: number; status: string; presence: string;
  task_id?: string | null; last_seen_at?: number | null; started_at?: string | null;
  capture_health?: {state?: string} | null;
  summary?: {text?: string; origin?: string} | null; control_revision?: number | null;
};

export type AgentArtifact = {
  id: string; revision: number; title: string; run_id?: string | null; created_at?: string;
  content?: {media_type?: string; text?: string; truncated?: boolean} | null;
};

export type AgentMemoryEvidence = {
  id: string; revision: number; title: string; category?: string;
  scope_kind: string; scope_owner_id: string; status: string; effective_status: string;
  origin?: string; evidence_refs: EvidenceRef[];
  validity?: {valid_from?: string | null; valid_until?: string | null;
    repository_id?: string | null; branch?: string | null; observed_commit?: string | null} | null;
  expires_at?: string | null; created_at?: string;
  content?: {text?: string; truncated?: boolean} | null;
};

export type AgentCoordinationEvent = {
  event_id: string; type: string; occurred_at: string; run_id: string; ingestion_seq: number;
  source?: {source_id?: string; kind?: string; attribution?: string; sequence?: number} | null;
  data?: Record<string, unknown> | null;
};

export type EvidenceSection<T> = {items: T[]; next_before: number | null};

export type AgentEvidence = {
  snapshot_cursor?: string;
  agent: {id: string; display_name?: string; description?: string; project_id: string};
  generated_at: number; limit: number;
  ledger: EvidenceSection<AgentLedgerRun>;
  artifacts: EvidenceSection<AgentArtifact>;
  memories: EvidenceSection<AgentMemoryEvidence>;
  events: EvidenceSection<AgentCoordinationEvent>;
};

export type AgentDetailsDrawerProps = {
  project: string; agentId: string; agentName?: string;
  onClose: () => void; pageSize?: number; refreshVersion?: number;
};

const SECTIONS = ["ledger", "artifacts", "memories", "events"] as const;
type SectionName = (typeof SECTIONS)[number];
const DEFAULT_PAGE_SIZE = 20;

function identity(section: SectionName, item: unknown): string {
  return section === "events"
    ? (item as AgentCoordinationEvent).event_id
    : (item as {id: string}).id;
}

function merge<T>(section: SectionName, current: T[], incoming: T[]): T[] {
  const seen = new Set(current.map(item => identity(section, item)));
  const result = [...current];
  for (const item of incoming) {
    const key = identity(section, item);
    if (!seen.has(key)) { seen.add(key); result.push(item); }
  }
  return result;
}

function timeOf(value?: string | null): string {
  if (!value) return "unknown time";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function epochOf(value?: number | null): string {
  return value ? new Date(value * 1000).toLocaleString() : "unknown";
}

function permissionMessage(cause: unknown): string | null {
  if (cause instanceof ApiError && [401, 403, 404].includes(cause.status)) {
    return "Evidence is unavailable for this agent. Access may have been revoked or the project changed.";
  }
  return null;
}

function Empty({children}: {children: React.ReactNode}) {
  return <p className="agent-details-empty">{children}</p>;
}

function SectionBlock({title, testId, count, busy, hasMore, onLoadMore, children}: {
  title: string; testId: string; count: number; busy: boolean; hasMore: boolean;
  onLoadMore: () => void; children: React.ReactNode;
}) {
  return <section className="agent-details-section" data-testid={testId} aria-label={title}>
    <div className="agent-details-section-head">
      <h3>{title}</h3><span className="muted">{count} loaded</span>
    </div>
    {children}
    {hasMore && <button className="agent-details-more" disabled={busy} onClick={onLoadMore}>
      {busy ? "Loading…" : `Load more ${title.toLowerCase()}`}</button>}
  </section>;
}

export function AgentDetailsDrawer({project, agentId, agentName, onClose, pageSize = DEFAULT_PAGE_SIZE, refreshVersion}:
  AgentDetailsDrawerProps) {
  const [evidence, setEvidence] = useState<AgentEvidence | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<SectionName | "">("");
  const [reloadVersion, setReloadVersion] = useState(0);
  const closeRef = useRef<HTMLButtonElement>(null);
  const onCloseRef = useRef(onClose);
  const generation = useRef(0);
  const drawerRef = useRef<HTMLDivElement>(null);

  useEffect(() => { onCloseRef.current = onClose; }, [onClose]);

  useEffect(() => {
    const version = ++generation.current;
    let disposed = false;
    // Project or agent change clears stale evidence immediately; a failed reload
    // never leaves another project's records on screen.
    setEvidence(null); setError(""); setLoading(true); setBusy("");
    const params = new URLSearchParams({project_id: project, limit: String(pageSize)});
    api<AgentEvidence>(`agents/${encodeURIComponent(agentId)}/evidence?${params.toString()}`)
      .then(data => { if (!disposed && version === generation.current) { setEvidence(data); setLoading(false); } })
      .catch((cause: unknown) => {
        if (disposed || version !== generation.current) return;
        setEvidence(null); setLoading(false);
        setError(permissionMessage(cause) ?? String(cause));
      });
    return () => { disposed = true; generation.current++; };
  }, [project, agentId, pageSize, refreshVersion, reloadVersion]);

  useEffect(() => {
    closeRef.current?.focus();
    const handler = (event: KeyboardEvent) => {
      if (event.key === "Escape") { event.preventDefault(); onCloseRef.current(); }
      if (event.key !== "Tab" || !drawerRef.current) return;
      const controls = Array.from(drawerRef.current.querySelectorAll<HTMLElement>(
        "button:not(:disabled), a[href], input:not(:disabled), textarea:not(:disabled), select:not(:disabled), summary, [tabindex='0']",
      )).filter(element => element.getClientRects().length > 0);
      const first = controls[0], last = controls[controls.length - 1];
      if (event.shiftKey && (document.activeElement === first || !drawerRef.current.contains(document.activeElement))) {
        event.preventDefault(); last?.focus();
      } else if (!event.shiftKey && (document.activeElement === last || !drawerRef.current.contains(document.activeElement))) {
        event.preventDefault(); first?.focus();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  async function loadMore(section: SectionName) {
    const current = evidence;
    if (!current || busy) return;
    const cursor = current[section].next_before;
    if (cursor === null) return;
    const version = generation.current;
    setBusy(section);
    const params = new URLSearchParams({project_id: project, limit: String(pageSize)});
    if (current.snapshot_cursor) params.set("snapshot_cursor", current.snapshot_cursor);
    for (const name of SECTIONS) params.set(`${name}_before`, name === section ? String(cursor) : "0");
    try {
      const page = await api<AgentEvidence>(`agents/${encodeURIComponent(agentId)}/evidence?${params.toString()}`);
      if (version !== generation.current) return;
      setEvidence(previous => {
        if (!previous) return page;
        if (section === "ledger") return {...previous, ledger: {
          items: merge(section, previous.ledger.items, page.ledger.items),
          next_before: page.ledger.next_before}};
        if (section === "artifacts") return {...previous, artifacts: {
          items: merge(section, previous.artifacts.items, page.artifacts.items),
          next_before: page.artifacts.next_before}};
        if (section === "memories") return {...previous, memories: {
          items: merge(section, previous.memories.items, page.memories.items),
          next_before: page.memories.next_before}};
        return {...previous, events: {
          items: merge(section, previous.events.items, page.events.items),
          next_before: page.events.next_before}};
      });
      setError("");
    } catch (cause) {
      if (version !== generation.current) return;
      const denied = permissionMessage(cause);
      setEvidence(null); setError(denied ?? String(cause));
      if (cause instanceof ApiError && cause.code === "snapshot_changed") setReloadVersion(value => value + 1);
    } finally { if (version === generation.current) setBusy(""); }
  }

  const name = agentName || evidence?.agent.display_name || agentId;

  return <div className="agent-details-backdrop" onClick={onClose}>
    <div ref={drawerRef} className="agent-details-drawer" role="dialog" aria-modal="true"
      aria-label={`${name} evidence`} data-testid="agent-details-drawer"
      onClick={event => event.stopPropagation()}>
      <header className="agent-details-head">
        <div>
          <p className="eyebrow">AUTHORIZED EVIDENCE</p>
          <h2>{name}</h2>
          <small className="muted"><code>{agentId}</code></small>
        </div>
        <button ref={closeRef} className="agent-details-close"
          aria-label={`Close ${name} details`} onClick={onClose}>Close</button>
      </header>
      <p className="muted agent-details-note">Only records this account is authorized to read are shown.
        Absence here is not proof that no evidence exists.</p>
      {error && <div className="error" role="alert">{error}</div>}
      {loading && <p className="agent-details-loading" role="status">Loading evidence…</p>}
      {!loading && !evidence && !error && <Empty>No evidence available.</Empty>}
      {evidence && <div className="agent-details-sections">
        <SectionBlock title="Ledger" testId="agent-details-ledger" count={evidence.ledger.items.length}
          busy={busy === "ledger"} hasMore={evidence.ledger.next_before !== null}
          onLoadMore={() => void loadMore("ledger")}>
          {evidence.ledger.items.length === 0
            ? <Empty>No authorized run ledger entries recorded.</Empty>
            : evidence.ledger.items.map(run => <article className="agent-details-item"
                data-testid="agent-details-ledger-item" key={run.id}>
                <div className="agent-details-item-head">
                  <strong>{run.status ?? "unknown"}</strong>
                  <span className={`agent-details-presence ${run.presence}`}>{run.presence}</span>
                </div>
                <small><code>{run.id}</code> · revision {run.revision}
                  {run.task_id ? <> · task <code>{run.task_id}</code></> : null}</small>
                <small>Last seen {epochOf(run.last_seen_at)} · started {timeOf(run.started_at)}</small>
                {run.summary?.text && <p className="agent-details-preview">{run.summary.text}
                  {" "}<span className="muted">({run.summary.origin ?? "reported"})</span></p>}
                {run.capture_health?.state && run.capture_health.state !== "healthy"
                  && <small className="warning">Capture {run.capture_health.state}</small>}
              </article>)}
        </SectionBlock>
        <SectionBlock title="Artifacts" testId="agent-details-artifacts"
          count={evidence.artifacts.items.length} busy={busy === "artifacts"}
          hasMore={evidence.artifacts.next_before !== null} onLoadMore={() => void loadMore("artifacts")}>
          {evidence.artifacts.items.length === 0
            ? <Empty>No artifacts produced by this agent&apos;s runs.</Empty>
            : evidence.artifacts.items.map(item => <article className="agent-details-item"
                data-testid="agent-details-artifact-item" key={item.id}>
                <div className="agent-details-item-head">
                  <strong>{item.title}</strong><span className="muted">revision {item.revision}</span>
                </div>
                <small><code>{item.id}</code>
                  {item.run_id ? <> · produced by run <code>{item.run_id}</code></> : null}</small>
                <small>Created {timeOf(item.created_at)}</small>
                {item.content?.text && <p className="agent-details-preview">{item.content.text}
                  {item.content.truncated ? " …" : ""}</p>}
              </article>)}
        </SectionBlock>
        <SectionBlock title="Memory evidence" testId="agent-details-memories"
          count={evidence.memories.items.length} busy={busy === "memories"}
          hasMore={evidence.memories.next_before !== null} onLoadMore={() => void loadMore("memories")}>
          {evidence.memories.items.length === 0
            ? <Empty>No agent or run scoped memory evidence recorded.</Empty>
            : evidence.memories.items.map(memory => <article className="agent-details-item"
                data-testid="agent-details-memory-item" key={memory.id}>
                <div className="agent-details-item-head">
                  <strong>{memory.title}</strong>
                  <span className={`visibility ${memory.effective_status}`}>
                    {memory.effective_status.replaceAll("_", " ")}</span>
                </div>
                <small><code>{memory.id}</code> · revision {memory.revision}
                  {" · "}{memory.category ?? "semantic"} · {memory.scope_kind} scope</small>
                <small>Origin {memory.origin ?? "explicit"} · expires
                  {" "}{memory.expires_at ? timeOf(memory.expires_at) : "never"}</small>
                {memory.content?.text && <p className="agent-details-preview">{memory.content.text}
                  {memory.content.truncated ? " …" : ""}</p>}
                {memory.evidence_refs?.length > 0 && <ul className="agent-details-refs"
                  aria-label="Evidence references">{memory.evidence_refs.map((ref, index) =>
                  <li key={`${ref.kind}-${ref.id}-${index}`}>{ref.kind} · <code>{ref.id}</code>
                    {" "}· revision {ref.revision ?? "current"}</li>)}</ul>}
              </article>)}
        </SectionBlock>
        <SectionBlock title="Coordination events" testId="agent-details-events"
          count={evidence.events.items.length} busy={busy === "events"}
          hasMore={evidence.events.next_before !== null} onLoadMore={() => void loadMore("events")}>
          {evidence.events.items.length === 0
            ? <Empty>No authorized coordination events recorded.</Empty>
            : evidence.events.items.map(event => <article className="agent-details-item"
                data-testid="agent-details-event-item" key={event.event_id}>
                <div className="agent-details-item-head">
                  <strong>{event.type.replace("hivemind.context.", "")}</strong>
                  <time dateTime={event.occurred_at}>{timeOf(event.occurred_at)}</time>
                </div>
                <small>run <code>{event.run_id}</code> · seq {event.ingestion_seq}
                  {event.source ? <> · {event.source.kind ?? "source"} · {event.source.attribution ?? "reported"}</> : null}</small>
                <details><summary>Observed payload</summary>
                  <pre>{JSON.stringify(event.data ?? {}, null, 2)}</pre></details>
              </article>)}
        </SectionBlock>
      </div>}
    </div>
  </div>;
}

export default AgentDetailsDrawer;
