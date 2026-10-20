import React, {useCallback, useEffect, useMemo, useRef, useState} from "react";
import {ApiError} from "./api";
import {
  CONSOLE_PAGE_SIZE, controlEligibility, controlIsPending, controlPhaseLabel,
  fetchConsoleSnapshot, holdReasonLabel, issueControl, overlapsByClaim, pageSlice,
  prepareControlCommand, prepareHoldResolution, redactionNote, reportedStatusLabel,
  resolveHold,
  type ConsoleAction, type ConsoleAgent, type ConsoleControl, type ConsoleRun,
  type ConsoleSnapshot, type ControlAction, type PreparedControl,
  type PreparedHoldResolution, type SteerTarget,
} from "./conflict-control-console";
import "./conflict-control-console.css";

export * from "./conflict-control-console";

export type ConsoleAgentProp = {
  id: string; display_name: string; base_color_token?: string; effective_role?: string;
};

export type ConflictControlConsoleProps = {
  project: string;
  role: string;
  agents?: ConsoleAgentProp[];
  /** Shared live-invalidation counter owned by the host; a change refetches. */
  refreshVersion?: number;
  /** Changes on session/permission change; stale results are fenced out. */
  permissionKey?: string;
  onError?: (message: string) => void;
  onOpenAgent?: (id: string) => void;
};

type PendingIntent = {
  action: ControlAction;
  run: ConsoleRun;
  agent: ConsoleAgent | undefined;
  steerTarget: SteerTarget | null;
  reason: string;
  /** Set at the first submission: fields lock and retries replay this exactly. */
  prepared: PreparedControl | null;
};

type HoldIntent = {
  hold: ConsoleAction;
  resolution: "reassign" | "escalate" | "dead_letter" | "dismiss";
  reason: string;
  /** Set at the first submission: the frozen key+body replayed verbatim on retry. */
  prepared: PreparedHoldResolution | null;
};

/**
 * Conflict and control console for one project. Read path is the canonical
 * aggregate (hivemind/conflict_control_console.py); every write goes through
 * the canonical control/hold surfaces with frozen payloads and immutable
 * idempotency keys. Consequential actions require an explicit review-and-
 * confirm step; the confirm click mints the idempotency key exactly once.
 */
export function ConflictControlConsole({
  project, role, agents = [], refreshVersion = 0, permissionKey = "",
  onError, onOpenAgent,
}: ConflictControlConsoleProps) {
  // Role is part of the access scope: a role change must mask and refetch.
  const scope = JSON.stringify([project, role, permissionKey]);
  const activeScope = useRef(scope);
  activeScope.current = scope;
  const generation = useRef(0);
  // Latest-read ticket: overlapping same-scope reads apply strictly in issue
  // order, so a slow older read can never overwrite a newer snapshot.
  const readTicket = useRef(0);
  const [snapshot, setSnapshot] = useState<ConsoleSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [intent, setIntent] = useState<PendingIntent | null>(null);
  const [holdIntent, setHoldIntent] = useState<HoldIntent | null>(null);
  const [sending, setSending] = useState(false);
  const [receipt, setReceipt] = useState<{id: string; phase: string} | null>(null);
  const [claimsPage, setClaimsPage] = useState(1);
  const [actionsPage, setActionsPage] = useState(1);
  const [controlsPage, setControlsPage] = useState(1);
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;
  const intentRef = useRef(intent);
  intentRef.current = intent;
  const holdIntentRef = useRef(holdIntent);
  holdIntentRef.current = holdIntent;

  // Synchronous scope-change masking: a render under a new project/role/
  // permission scope never paints the previous scope's protected data, review
  // dialogs, or receipts, and an in-flight send's stale finally cannot strand
  // the new view in `sending`.
  const [renderedScope, setRenderedScope] = useState(scope);
  if (renderedScope !== scope) {
    setRenderedScope(scope);
    setSnapshot(null); setError(""); setIntent(null); setHoldIntent(null);
    setReceipt(null); setSending(false);
    setClaimsPage(1); setActionsPage(1); setControlsPage(1);
    setLoading(true);
  }

  const load = useCallback(async (silent = false) => {
    if (!project) { setSnapshot(null); setLoading(false); return; }
    const version = generation.current;
    const ticket = ++readTicket.current;
    if (!silent) setLoading(true);
    try {
      const next = await fetchConsoleSnapshot(project);
      if (version !== generation.current || ticket !== readTicket.current
          || scope !== activeScope.current) return;
      setSnapshot(next); setError("");
      // A healthy refresh keeps legitimate frozen retries intact; it only
      // drops review state whose protected references vanished or were
      // redacted out of the fresh snapshot.
      const pending = intentRef.current;
      if (pending && (!next.runs.some(r => r.id === pending.run.id)
          || (pending.steerTarget
              && !next.steer_targets.some(t => t.id === pending.steerTarget!.id)))) {
        setIntent(null);
      }
      const pendingHold = holdIntentRef.current;
      if (pendingHold && !next.holds.some(h => h.id === pendingHold.hold.id
          && h.revision === pendingHold.hold.revision)) {
        setHoldIntent(null);
      }
    } catch (e) {
      if (version !== generation.current || ticket !== readTicket.current
          || scope !== activeScope.current) return;
      // Authorization/read failures clear protected data, review dialogs, and
      // receipts: they retain hidden target/hold metadata otherwise.
      setSnapshot(null); setIntent(null); setHoldIntent(null); setReceipt(null);
      const message = e instanceof ApiError && (e.status === 401 || e.status === 403)
        ? "You are not authorized to read coordination state for this project."
        : String(e);
      setError(message); onErrorRef.current?.(message);
    } finally {
      if (!silent && version === generation.current && ticket === readTicket.current) {
        setLoading(false);
      }
    }
  }, [project, scope]);

  useEffect(() => {
    generation.current++;
    readTicket.current++;
    setSnapshot(null); setError(""); setIntent(null); setHoldIntent(null); setReceipt(null);
    setSending(false);
    setClaimsPage(1); setActionsPage(1); setControlsPage(1);
    setLoading(true);
    return () => { generation.current++; readTicket.current++; };
  }, [scope]);

  useEffect(() => { void load(false); }, [refreshVersion, load]);

  const overlapIndex = useMemo(
    () => overlapsByClaim(snapshot?.claim_overlaps ?? []), [snapshot]);
  const agentIndex = useMemo(() => {
    const map = new Map<string, ConsoleAgent>();
    for (const agent of snapshot?.agents ?? []) map.set(agent.id, agent);
    return map;
  }, [snapshot]);
  const runIndex = useMemo(() => {
    const map = new Map<string, ConsoleRun>();
    for (const run of snapshot?.runs ?? []) map.set(run.id, run);
    return map;
  }, [snapshot]);
  const nameOf = useCallback((id: string | null | undefined) => {
    if (!id) return "Unknown agent";
    return agents.find(a => a.id === id)?.display_name
      ?? agentIndex.get(id)?.display_name ?? id;
  }, [agents, agentIndex]);

  const canOperate = role === "admin" || role === "human";
  const nowSeconds = Date.now() / 1000;

  function openIntent(action: ControlAction, run: ConsoleRun) {
    const agent = agentIndex.get(run.agent_id);
    setReceipt(null);
    setIntent({action, run, agent, steerTarget: null, reason: "", prepared: null});
  }

  async function confirmIntent() {
    if (!intent || !snapshot || intent.prepared) return;
    // Freeze once at the first submission: fields lock and every retry replays
    // this exact key and body, so the displayed confirmation matches the wire.
    const current = prepareControlCommand({
      projectId: project, targetRunId: intent.run.id, action: intent.action,
      replacementTaskId: intent.steerTarget?.id ?? null,
      targetRunRevision: intent.run.control_revision,
      coordinatorEpoch: snapshot.lease!.coordinator_epoch, reason: intent.reason.trim(),
    });
    setIntent({...intent, prepared: current});
    const version = generation.current;
    setSending(true);
    try {
      const control = await issueControl(current);
      if (version !== generation.current || scope !== activeScope.current) return;
      setIntent(null);
      setReceipt({id: control.id, phase: controlPhaseLabel(control)});
      await load(true);
    } catch (e) {
      if (version !== generation.current) return;
      const message = String(e);
      setError(message); onErrorRef.current?.(message);
    } finally {
      if (version === generation.current) setSending(false);
    }
  }

  async function retryIntent() {
    const prepared = intent?.prepared;
    if (!intent || !prepared) return;
    const version = generation.current;
    setSending(true);
    try {
      // Legitimate retry: replay the frozen payload with its immutable key.
      const control = await issueControl(prepared);
      if (version !== generation.current || scope !== activeScope.current) return;
      setIntent(null);
      setReceipt({id: control.id, phase: controlPhaseLabel(control)});
      await load(true);
    } catch (e) {
      if (version !== generation.current) return;
      const message = String(e);
      setError(message); onErrorRef.current?.(message);
    } finally {
      if (version === generation.current) setSending(false);
    }
  }

  async function confirmHold() {
    if (!holdIntent) return;
    // Freeze the complete resolution (key and body) at the first submission.
    const current = holdIntent.prepared ?? prepareHoldResolution({
      actionId: holdIntent.hold.id, expectedRevision: holdIntent.hold.revision,
      resolution: holdIntent.resolution, reason: holdIntent.reason.trim(),
    });
    if (!holdIntent.prepared) setHoldIntent({...holdIntent, prepared: current});
    const version = generation.current;
    setSending(true);
    try {
      await resolveHold(current);
      if (version !== generation.current || scope !== activeScope.current) return;
      setHoldIntent(null);
      await load(true);
    } catch (e) {
      if (version !== generation.current) return;
      const message = String(e);
      setError(message); onErrorRef.current?.(message);
    } finally {
      if (version === generation.current) setSending(false);
    }
  }

  if (!project) return null;

  const claims = snapshot ? pageSlice(snapshot.claims, claimsPage) : {page: [], pages: 1};
  const controls = snapshot ? pageSlice(snapshot.controls, controlsPage) : {page: [], pages: 1};
  const actions = snapshot ? pageSlice(snapshot.actions, actionsPage) : {page: [], pages: 1};
  const redaction = snapshot ? redactionNote(snapshot.redactions) : null;

  return <section className="conflict-console" aria-label="Conflict and control console"
    data-testid="conflict-console" data-project={project}>
    <header className="conflict-console-header">
      <h1>Conflicts & control</h1>
      <p className="muted">Canonical work-claim overlaps, arbitration records, holds, and
        control requests. A request is not an applied outcome; presence is never an
        acknowledgment.</p>
      {snapshot?.lease
        ? <p className="conflict-console-lease" data-testid="console-lease">
            Coordination lease held by {nameOf(snapshot.lease.agent_id)} · epoch {snapshot.lease.coordinator_epoch}</p>
        : <p className="conflict-console-lease muted" data-testid="console-lease-empty">
            No active coordination lease visible. Control requests are disabled.</p>}
      {snapshot && !snapshot.issuer_hint.principal_may_issue
        && snapshot.issuer_hint.lease_visible
        && <p className="conflict-console-redaction" data-testid="console-issuer-hint">
          {snapshot.issuer_hint.explanation}</p>}
    </header>

    {error && <p className="conflict-console-error" role="alert" data-testid="console-error">{error}</p>}
    {loading && !snapshot && <p className="muted" data-testid="console-loading">Loading…</p>}
    {redaction && <p className="conflict-console-redaction" data-testid="console-redaction">{redaction}</p>}
    {receipt && <p className="conflict-console-receipt" role="status" data-testid="console-receipt">
      Control {receipt.id}: {receipt.phase}. Accepted for delivery is not an applied outcome.</p>}

    {snapshot && <>
      <section aria-label="Overlapping work claims" className="conflict-console-panel">
        <h2>Active claims {snapshot.truncated.claims && <small className="muted">
          (showing first {snapshot.limits.claims}; truncated)</small>}</h2>
        {snapshot.claims.length === 0 && <p className="muted">No active work claims.</p>}
        {claims.page.map(claim => {
          const overlaps = overlapIndex.get(claim.id) ?? [];
          const run = runIndex.get(claim.run_id);
          return <article key={claim.id} className="conflict-console-card"
            data-testid="console-claim" data-claim-id={claim.id}
            data-overlapping={overlaps.length ? "true" : "false"}>
            <header>
              <strong>{nameOf(run?.agent_id)}</strong>
              <span className="muted">{claim.mode ?? "claim"} · {claim.state}</span>
            </header>
            <p className="muted">
              {claim.scope_ids.length} visible scope{claim.scope_ids.length === 1 ? "" : "s"}
              {claim.hidden_scope_count > 0 &&
                ` · ${claim.hidden_scope_count} protected scope${claim.hidden_scope_count === 1 ? "" : "s"} redacted`}
              {claim.task_id && <> · task {claim.task_id}</>}
            </p>
            {overlaps.length > 0 && <p className="conflict-console-warning">
              Overlaps {overlaps.length} other active claim{overlaps.length === 1 ? "" : "s"}
              {" "}(canonical scope overlap).</p>}
            {run && <div className="conflict-console-controls">
              {(["pause", "resume", "steer"] as ControlAction[]).map(action => {
                const eligibility = controlEligibility({
                  action, run, agent: agentIndex.get(run.agent_id),
                  lease: snapshot.lease, issuerHint: snapshot.issuer_hint, nowSeconds,
                  steerTarget: snapshot.steer_targets[0] ?? null,
                });
                return <button key={action} type="button"
                  data-testid={`control-${action}`} data-run-id={run.id}
                  disabled={!canOperate || !eligibility.allowed || sending}
                  title={eligibility.allowed ? `${action} ${nameOf(run.agent_id)} — requires confirmation`
                    : eligibility.reason}
                  onClick={() => openIntent(action, run)}>
                  {action[0].toUpperCase() + action.slice(1)}</button>;
              })}
            </div>}
            <small className="muted">Presence: {run?.presence ?? "unknown"} (presence is not an
              acknowledgment of any control).</small>
          </article>;
        })}
        {claims.pages > 1 && <Pager page={claimsPage} pages={claims.pages} onPage={setClaimsPage}
          label="claims"/>}
      </section>

      <section aria-label="Arbitration decisions" className="conflict-console-panel">
        <h2>Arbitration decisions</h2>
        {snapshot.conflicts.length === 0 && <p className="muted">No claim conflicts recorded.</p>}
        {snapshot.conflicts.map(conflict => <article key={conflict.id}
          className="conflict-console-card" data-testid="console-conflict">
          <p><strong>{conflict.state}</strong> · enforcement: {conflict.enforcement ?? "policy"}</p>
          <p className="muted">Winner claim {conflict.winner_claim_id} · requesting run
            {" "}{conflict.requesting_run_id} · {conflict.scope_ids.length} visible scope
            {conflict.scope_ids.length === 1 ? "" : "s"}
            {conflict.hidden_scope_count > 0 && ` · ${conflict.hidden_scope_count} redacted`}</p>
        </article>)}
      </section>

      <section aria-label="Manual holds" className="conflict-console-panel">
        <h2>Manual holds</h2>
        {snapshot.holds.length === 0 && <p className="muted">No manual holds.</p>}
        {snapshot.holds.map(hold => <article key={hold.id} className="conflict-console-card"
          data-testid="console-hold" data-action-id={hold.id}>
          <p><strong>Held</strong> · {holdReasonLabel(hold.reason)}</p>
          <p className="muted">{hold.task_redacted
            ? "Task reference redacted (not visible to you)"
            : hold.task_id ? `Task ${hold.task_id}` : "No task reference"}</p>
          {canOperate && !hold.task_redacted && <div className="conflict-console-controls">
            {(["dismiss", "escalate"] as const).map(resolution =>
              <button key={resolution} type="button" data-testid={`hold-${resolution}`}
                disabled={sending}
                title={resolution === "dismiss"
                  ? "Close the hold without altering ownership"
                  : "Escalate for attention; never alters ownership"}
                onClick={() => setHoldIntent({hold, resolution, reason: "", prepared: null})}>
                {resolution[0].toUpperCase() + resolution.slice(1)}</button>)}
          </div>}
        </article>)}
      </section>

      <section aria-label="Control requests" className="conflict-console-panel">
        <h2>Control requests</h2>
        {snapshot.controls.length === 0 && <p className="muted">No control requests recorded.</p>}
        {controls.page.map(control => <article key={control.id}
          className="conflict-console-card" data-testid="console-control"
          data-control-id={control.id} data-state={control.state}>
          <p><strong>{control.action}</strong> → {control.target_redacted
            ? "redacted target" : nameOf(control.target_agent_id)}</p>
          <p data-testid="control-phase">{controlPhaseLabel(control)}</p>
          {reportedStatusLabel(control) &&
            <small className="muted">{reportedStatusLabel(control)}</small>}
          {control.reason && <p className="muted">{control.reason}</p>}
          {controlIsPending(control) &&
            <small className="muted">Pending; outcome is not yet known.</small>}
        </article>)}
        {controls.pages > 1 && <Pager page={controlsPage} pages={controls.pages}
          onPage={setControlsPage} label="controls"/>}
      </section>

      <section aria-label="Recent coordination actions" className="conflict-console-panel">
        <h2>Recent coordination actions</h2>
        {snapshot.actions.length === 0 && <p className="muted">No coordination actions.</p>}
        {actions.page.map(action => <article key={action.id} className="conflict-console-card"
          data-testid="console-action">
          <p><strong>{action.action}</strong> · {action.state}</p>
          <p className="muted">{action.task_redacted
            ? "Task reference redacted" : action.task_id ?? "No task"}</p>
        </article>)}
        {actions.pages > 1 && <Pager page={actionsPage} pages={actions.pages}
          onPage={setActionsPage} label="actions"/>}
      </section>
    </>}

    {intent && snapshot && <div className="conflict-console-dialog-backdrop">
      <div role="dialog" aria-modal="true" aria-label="Confirm control request"
        className="conflict-console-dialog" data-testid="control-dialog">
        <h2>Review control request</h2>
        <p>You are requesting <strong>{intent.action}</strong> for
          {" "}{nameOf(intent.run.agent_id)} (run {intent.run.id}).</p>
        <p className="muted">Submitting sends a control request through the canonical Nexus
          path. Queued means requested — not delivered, acknowledged, or applied. There is no
          atomic reassignment; steering redirects the run to the selected queued task only
          after the target runtime confirms.</p>
        {intent.prepared && <p className="muted" data-testid="control-locked">
          Submitted. Reason and target are locked; a retry replays the exact same
          request (idempotency key {intent.prepared.idempotencyKey.slice(0, 8)}…).</p>}
        {intent.action === "steer" && <label className="conflict-console-field">Target task
          <select data-testid="steer-target" value={intent.steerTarget?.id ?? ""}
            disabled={Boolean(intent.prepared)}
            onChange={e => setIntent({...intent,
              steerTarget: snapshot.steer_targets.find(t => t.id === e.target.value) ?? null})}>
            <option value="">Select a queued task…</option>
            {snapshot.steer_targets.map(target =>
              <option key={target.id} value={target.id}>{target.title ?? target.id}</option>)}
          </select></label>}
        <label className="conflict-console-field">Reason (recorded on the control record)
          <textarea data-testid="control-reason" maxLength={4096}
            value={intent.prepared ? intent.prepared.payload.reason : intent.reason}
            disabled={Boolean(intent.prepared)}
            onChange={e => setIntent({...intent, reason: e.target.value})}/></label>
        <div className="conflict-console-controls">
          {intent.prepared
            ? <button type="button" data-testid="control-retry" disabled={sending}
                onClick={() => void retryIntent()}>
                {sending ? "Retrying…" : "Retry the same request"}</button>
            : <button type="button" data-testid="control-confirm" disabled={sending
                || !intent.reason.trim()
                || ((intent.action === "steer" || intent.action === "assign_task")
                    && !intent.steerTarget)}
                onClick={() => void confirmIntent()}>
                {sending ? "Sending…" : `Confirm ${intent.action} request`}</button>}
          <button type="button" data-testid="control-cancel" disabled={sending}
            onClick={() => setIntent(null)}>Cancel</button>
        </div>
      </div>
    </div>}

    {holdIntent && <div className="conflict-console-dialog-backdrop">
      <div role="dialog" aria-modal="true" aria-label="Confirm hold resolution"
        className="conflict-console-dialog" data-testid="hold-dialog">
        <h2>Review hold resolution</h2>
        <p><strong>{holdIntent.resolution}</strong> — {holdIntent.resolution === "dismiss"
          ? "closes the hold without altering ownership."
          : "escalates for attention; ownership is unchanged."}</p>
        <p className="muted">Reassignment and dead-lettering of ambiguous executions stay with
          the canonical hold-resolution policy and are not offered here.</p>
        {holdIntent.prepared && <p className="muted" data-testid="hold-locked">
          Submitted. The reason is locked; a retry replays the exact same resolution.</p>}
        <label className="conflict-console-field">Reason
          <textarea data-testid="hold-reason" maxLength={4096}
            value={holdIntent.prepared ? holdIntent.prepared.payload.reason : holdIntent.reason}
            disabled={Boolean(holdIntent.prepared)}
            onChange={e => setHoldIntent({...holdIntent, reason: e.target.value})}/></label>
        <div className="conflict-console-controls">
          <button type="button" data-testid="hold-confirm"
            disabled={sending || (!holdIntent.prepared && !holdIntent.reason.trim())}
            onClick={() => void confirmHold()}>
            {holdIntent.prepared
              ? (sending ? "Retrying…" : "Retry the same resolution")
              : (sending ? "Sending…" : `Confirm ${holdIntent.resolution}`)}</button>
          <button type="button" data-testid="hold-cancel" disabled={sending}
            onClick={() => setHoldIntent(null)}>Cancel</button>
        </div>
      </div>
    </div>}
  </section>;
}

function Pager({page, pages, onPage, label}: {
  page: number; pages: number; onPage: (page: number) => void; label: string;
}) {
  return <nav className="conflict-console-pager" aria-label={`${label} pages`}
    data-testid={`pager-${label}`}>
    <button type="button" disabled={page <= 1} onClick={() => onPage(page - 1)}>Newer</button>
    <span>{page} / {pages}</span>
    <button type="button" disabled={page >= pages} onClick={() => onPage(page + 1)}>Older</button>
  </nav>;
}
