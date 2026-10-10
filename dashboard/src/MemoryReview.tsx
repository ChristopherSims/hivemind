import React, {useEffect, useState} from "react";
import {api} from "./api";

type Evidence = {kind: string; id: string; revision?: number};
type Memory = {
  id: string; title: string; content: {text?: string}; revision: number;
  status: string; effective_status: string; memory_version_id: string;
  evidence_refs: Evidence[]; expires_at?: string | null;
  validity?: {valid_from?: string | null; valid_until?: string | null; repository_id?: string | null;
    branch?: string | null; observed_commit?: string | null};
};
type Proposal = Memory & {memory_id: string; base_revision: number; review_reason?: string};
type Dispute = {id: string; status: string; revision: number; reason: string;
  left_memory_id: string; right_memory_id: string; resolution?: string; review_reason?: string;
  evidence_unavailable?: boolean};
type Queue = {memories: Memory[]; proposals: Proposal[]; disputes: Dispute[]};
type BoardItem = {id: string; revision: number; content: {text?: string}};

export function MemoryReview({project}: {project: string}) {
  const [queue, setQueue] = useState<Queue>({memories: [], proposals: [], disputes: []});
  const [evidence, setEvidence] = useState<BoardItem[]>([]);
  const [reason, setReason] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [history, setHistory] = useState<Memory[] | null>(null);
  const [historyId, setHistoryId] = useState("");
  const [filter, setFilter] = useState("all");
  const [editing, setEditing] = useState<Memory | null>(null);
  const [title, setTitle] = useState("");
  const [text, setText] = useState("");
  const [expires, setExpires] = useState("");
  const [validFrom, setValidFrom] = useState("");
  const [validUntil, setValidUntil] = useState("");
  const [source, setSource] = useState("");
  const [left, setLeft] = useState("");
  const [right, setRight] = useState("");
  const [disputeReason, setDisputeReason] = useState("");
  const [disputeSource, setDisputeSource] = useState("");

  async function refresh() {
    const data = await api<Queue>(`memories/review-queue?project_id=${encodeURIComponent(project)}`);
    setQueue(data);
  }
  useEffect(() => {
    let disposed = false;
    async function load() {
      try {
        const [data, boards] = await Promise.all([
          api<Queue>(`memories/review-queue?project_id=${encodeURIComponent(project)}`),
          api<{id: string}[]>(`boards?project_id=${encodeURIComponent(project)}`),
        ]);
        const posts = boards[0] ? await api<{items: BoardItem[]}>(`boards/${boards[0].id}/posts`) : {items: []};
        const versions = historyId ? await api<Memory[]>(`memories/${historyId}/versions`).catch(() => []) : null;
        if (!disposed) {
          setQueue(data); setEvidence(posts.items); setError("");
          if (versions) setHistory(versions);
          setEditing(previous => previous && data.memories.some(item =>
            item.id === previous.id && item.revision === previous.revision) ? previous : null);
        }
      } catch (e) {
        if (!disposed) {
          setError(String(e)); setQueue({memories: [], proposals: [], disputes: []});
          setEvidence([]); setHistory(null); setEditing(null);
        }
      }
    }
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => { disposed = true; window.clearInterval(timer); };
  }, [project, historyId]);

  async function change(path: string, body: unknown, requiresReason = true) {
    if (requiresReason && !reason.trim()) { setError("Enter a review reason before deciding."); return; }
    setBusy(true);
    try {
      await api(path, "POST", body);
      await refresh(); setHistory(null); setHistoryId(""); setEditing(null); setError("");
    } catch (e) { setError(`${String(e)}. Refresh before retrying if this record changed.`); }
    finally { setBusy(false); }
  }
  function evidenceList(refs: Evidence[]) {
    return <ul className="memory-evidence" aria-label="Evidence references">{refs.map((ref, index) =>
      <li key={index}>{ref.kind} · <code>{ref.id}</code> · revision {ref.revision ?? "current"}</li>)}</ul>;
  }
  function sourceOptions() {
    return <><option value="">Select an authorized board source</option>{evidence.map(post =>
      <option value={post.id} key={post.id}>{(post.content.text ?? post.id).slice(0, 90)}</option>)}</>;
  }
  const visible = queue.memories.filter(memory => filter === "all" || memory.effective_status === filter);
  const eligible = queue.memories.filter(memory => ["proposed", "verified"].includes(memory.effective_status));

  return <section className="memory-workspace" aria-label="Memory review">
    <div className="timeline-heading"><div><p className="eyebrow">EVIDENCE-BACKED MEMORY</p>
      <h1>Review facts, not assumptions.</h1></div><span className="count">{queue.memories.length} memories</span></div>
    <p className="muted">Proposals and reports are untrusted evidence. Human approval never widens the original scope.
      Expiry excludes retrieval immediately; it does not erase stored history.</p>
    {error && <div className="error" role="alert">{error}</div>}
    <div className="memory-toolbar">
      <label>Memory status <select aria-label="Memory status" value={filter} onChange={e => setFilter(e.target.value)}>
        {["all", "proposed", "verified", "disputed", "expired", "not_yet_valid", "rejected", "superseded"].map(value =>
          <option key={value} value={value}>{value.replaceAll("_", " ")}</option>)}</select></label>
      <label>Review reason <input aria-label="Review reason" value={reason} onChange={e => setReason(e.target.value)}/></label>
      <button disabled={busy} onClick={() => void refresh().catch(e => setError(String(e)))}>Refresh memories</button>
    </div>
    {visible.length === 0 && <p>No authorized memories match this status.</p>}
    <div className="memory-list">{visible.map(memory => <article className="memory-card" key={memory.id}
      data-testid="memory-card" data-memory-id={memory.id}>
      <div className="message-heading"><h2>{memory.title}</h2><span className="visibility">{memory.effective_status.replaceAll("_", " ")}</span></div>
      <div className="body">{memory.content.text}</div>
      <small>Version <code>{memory.memory_version_id}</code> · revision {memory.revision}</small>
      <p className="muted">Valid from: {memory.validity?.valid_from ?? "now"} · valid until: {memory.validity?.valid_until ?? "unbounded"}
        {" · "}expires: {memory.expires_at ?? "never"}</p>
      {evidenceList(memory.evidence_refs)}
      <div className="memory-actions">
        {["proposed", "verified"].includes(memory.effective_status) && <>
          <button disabled={busy || !reason.trim()} onClick={() => void change(`memories/${memory.id}:review`,
            {expected_revision: memory.revision, decision: "verify", reason})}>Verify memory</button>
          <button disabled={busy || !reason.trim()} onClick={() => void change(`memories/${memory.id}:review`,
            {expected_revision: memory.revision, decision: "reject", reason})}>Reject memory</button>
        </>}
        {["proposed", "verified", "expired", "not_yet_valid"].includes(memory.effective_status) &&
          <button disabled={busy} onClick={() => {
            setEditing(memory); setTitle(memory.title); setText(memory.content.text ?? "");
            setExpires(memory.expires_at ?? ""); setValidFrom(memory.validity?.valid_from ?? "");
            setValidUntil(memory.validity?.valid_until ?? ""); setSource(""); setHistory(null);
          }}>Propose new version</button>}
        <button disabled={busy} onClick={async () => {
          try { setHistory(await api<Memory[]>(`memories/${memory.id}/versions`)); setHistoryId(memory.id); }
          catch (e) { setError(String(e)); }
        }}>View version history</button>
      </div>
    </article>)}</div>
    {history && <section aria-label="Memory version history" className="memory-panel">
      <h2>Version history</h2><code>{historyId}</code><button onClick={() => {setHistory(null); setHistoryId("");}}>Close history</button>
      {history.map(version => <article key={version.memory_version_id}>
        <h3>{version.title} · {version.effective_status}</h3><code>{version.memory_version_id}</code>
        <div className="body">{version.content.text}</div>{evidenceList(version.evidence_refs)}</article>)}
    </section>}
    {editing && <form className="memory-panel" aria-label="New memory version" onSubmit={event => {
      event.preventDefault();
      const post = evidence.find(item => item.id === source);
      if (!post) { setError("Select authorized evidence for the new version."); return; }
      void change(`memories/${editing.id}/versions`, {
        expected_revision: editing.revision, title, content: {media_type: "text/plain", text},
        evidence_refs: [{kind: "board_post", id: post.id, revision: post.revision}],
        validity: {...editing.validity, valid_from: validFrom || null, valid_until: validUntil || null},
        expires_at: expires || null,
      }, false);
    }}>
      <h2>Propose a new version</h2><p>Current content stays active until a human approves this proposal.</p>
      <label>Version title <input aria-label="Version title" required value={title} onChange={e => setTitle(e.target.value)}/></label>
      <label>Version content <textarea aria-label="Version content" required value={text} onChange={e => setText(e.target.value)}/></label>
      <label>Version evidence <select aria-label="Version evidence" required value={source} onChange={e => setSource(e.target.value)}>{sourceOptions()}</select></label>
      <label>Valid from (UTC RFC3339) <input aria-label="Valid from" placeholder="2026-10-09T00:00:00Z" value={validFrom} onChange={e => setValidFrom(e.target.value)}/></label>
      <label>Valid until (UTC RFC3339) <input aria-label="Valid until" value={validUntil} onChange={e => setValidUntil(e.target.value)}/></label>
      <label>Expires at (UTC RFC3339) <input aria-label="Expires at" value={expires} onChange={e => setExpires(e.target.value)}/></label>
      <button className="primary" disabled={busy}>Submit version proposal</button>
      <button type="button" onClick={() => setEditing(null)}>Cancel version proposal</button>
    </form>}
    <section className="memory-panel" aria-label="Memory version proposals"><h2>Version proposals</h2>
      {!queue.proposals.length && <p>No proposals yet.</p>}
      {queue.proposals.map(proposal => {
        const current = queue.memories.find(memory => memory.id === proposal.memory_id);
        const stale = current?.revision !== proposal.base_revision;
        return <article key={proposal.id} data-testid="memory-proposal">
          <h3>{proposal.title} · {proposal.status}{proposal.status === "proposed" && stale ? " · stale" : ""}</h3>
          <div className="body">{proposal.content.text}</div>{evidenceList(proposal.evidence_refs)}
          {proposal.review_reason && <p>Review: {proposal.review_reason}</p>}
          {proposal.status === "proposed" && <div className="memory-actions">
            <button disabled={busy || stale || !reason.trim()} onClick={() => void change(`memory-proposals/${proposal.id}:review`,
              {expected_revision: proposal.revision, decision: "approve", reason})}>Approve version</button>
            <button disabled={busy || !reason.trim()} onClick={() => void change(`memory-proposals/${proposal.id}:review`,
              {expected_revision: proposal.revision, decision: "reject", reason})}>Reject version</button>
          </div>}
        </article>;
      })}
    </section>
    <form className="memory-panel" aria-label="Report memory contradiction" onSubmit={event => {
      event.preventDefault();
      const a = queue.memories.find(memory => memory.id === left);
      const b = queue.memories.find(memory => memory.id === right);
      const post = evidence.find(item => item.id === disputeSource);
      if (!a || !b || !post || a.id === b.id) { setError("Choose two distinct memories and evidence."); return; }
      void change("memory-disputes", {project_id: project, left_memory_id: a.id, right_memory_id: b.id,
        left_revision: a.revision, right_revision: b.revision, reason: disputeReason,
        evidence_refs: [{kind: "board_post", id: post.id, revision: post.revision}]}, false);
    }}>
      <h2>Report a contradiction</h2><p>Reporting alone does not revoke either fact. A human confirms quarantine.</p>
      <label>Left memory <select aria-label="Left memory" required value={left} onChange={e => setLeft(e.target.value)}>
        <option value="">Choose a memory</option>{eligible.map(memory => <option key={memory.id} value={memory.id}>{memory.title}</option>)}</select></label>
      <label>Right memory <select aria-label="Right memory" required value={right} onChange={e => setRight(e.target.value)}>
        <option value="">Choose a memory</option>{eligible.map(memory => <option key={memory.id} value={memory.id}>{memory.title}</option>)}</select></label>
      <label>Contradiction reason <textarea aria-label="Contradiction reason" required value={disputeReason} onChange={e => setDisputeReason(e.target.value)}/></label>
      <label>Contradiction evidence <select aria-label="Contradiction evidence" required value={disputeSource} onChange={e => setDisputeSource(e.target.value)}>
        {sourceOptions()}</select></label><button disabled={busy}>Report contradiction</button>
    </form>
    <section className="memory-panel" aria-label="Memory contradictions"><h2>Contradiction reviews</h2>
      {!queue.disputes.length && <p>No contradiction reports.</p>}
      {queue.disputes.map(dispute => {
        const a = queue.memories.find(memory => memory.id === dispute.left_memory_id);
        const b = queue.memories.find(memory => memory.id === dispute.right_memory_id);
        return <article key={dispute.id} data-testid="memory-dispute"><h3>{a?.title} ↔ {b?.title} · {dispute.status}</h3>
          <p className="body">{dispute.reason}</p>{dispute.review_reason && <p>Review: {dispute.review_reason} · {dispute.resolution}</p>}
          {a && b && ["reported", "confirmed"].includes(dispute.status) && <div className="memory-actions">
            {(dispute.status === "reported" ? ["confirm", "dismiss", "prefer_left", "prefer_right"] : ["dismiss", "prefer_left", "prefer_right"]).map(decision =>
              <button key={decision} disabled={busy || !reason.trim() || Boolean(dispute.evidence_unavailable && decision !== "dismiss")} onClick={() => void change(`memory-disputes/${dispute.id}:review`,
                {expected_revision: dispute.revision, left_expected_revision: a.revision,
                  right_expected_revision: b.revision, decision, reason})}>
                {({confirm: "Confirm contradiction", dismiss: "Dismiss contradiction",
                  prefer_left: "Keep left memory", prefer_right: "Keep right memory"} as Record<string,string>)[decision]}
              </button>)}
          </div>}
        </article>;
      })}
    </section>
  </section>;
}
