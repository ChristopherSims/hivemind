// Task/run workspace: assignments, dependencies, progress, blockers, and
// execution history for one project, with explicit operator actions routed
// through the canonical scheduling / blocked-task endpoints only.
//
// Refresh is driven ENTIRELY by the parent-owned SSE stream via
// `refreshVersion`; this component never opens its own stream. Reads are
// fenced by a read-generation counter; writes use an INDEPENDENT write-ticket
// counter plus a synchronous (project, role, permissionKey) scope guard, so a
// read reload can never strand or cancel an in-flight mutation and a scope
// change can never let a stale write commit UI state. Protected evidence
// (detail, checkpoint summaries, history) is cleared before every
// authoritative reload, on scope/selection loss, and on any authorization or
// snapshot failure. Mutation payloads are frozen in full on first submit —
// reason and target included — and a retry replays the exact frozen payload
// under the original idempotency key; a changed command is never silently
// sent. Revision conflicts are surfaced, never retried.
import React, {useCallback, useEffect, useMemo, useRef, useState} from "react";
import {api, ApiError} from "./api";
import {
  BlockedRequestView,
  HistoryEntry,
  TaskHistoryPage,
  WorkspaceAgent,
  WorkspacePage,
  WorkspaceTask,
  agentName,
  canHandoff,
  canReconcile,
  describeHistoryEntry,
  filterTasks,
  freezePayload,
  historyEntryKey,
  isTerminal,
  planTitle,
  progressLabel,
  sortTasks,
  unmetDependencies,
} from "./task-run-workspace";
import "./task-run-workspace.css";

export interface TaskRunWorkspaceProps {
  project: string;
  role: string;
  agents?: WorkspaceAgent[];
  refreshVersion?: number;
  permissionKey?: string;
  onError?: (message: string) => void;
  onOpenAgent?: (id: string) => void;
}

const TASK_PAGE_SIZE = 100;
const HISTORY_PAGE_SIZE = 50;

interface FrozenCommand {
  payload: Readonly<Record<string, unknown>>;
  key: string;
}

interface PendingAction {
  kind: "reconcile" | "handoff";
  task: WorkspaceTask;
  /** Frozen intent base (expected_revision + action) minted at dialog open. */
  base: Readonly<Record<string, unknown>>;
  /** Idempotency key minted once per explicit operator intent. */
  key: string;
  /** Complete frozen first-submitted command; retries replay this exactly. */
  submitted: FrozenCommand | null;
}

export function TaskRunWorkspace(props: TaskRunWorkspaceProps) {
  const {project, role, agents, refreshVersion = 0, permissionKey = "", onError, onOpenAgent} = props;
  const [page, setPage] = useState<WorkspacePage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<Record<string, unknown> | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [historyPage, setHistoryPage] = useState<TaskHistoryPage | null>(null);
  const [pending, setPending] = useState<PendingAction | null>(null);
  const [actionBusy, setActionBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  // Read fencing: bumped by every authorized read path (page load, selection).
  const generation = useRef(0);
  // Write fencing: independent of reads so load() can never strand a busy
  // flag or fence a mutation response; bumped on scope/selection loss.
  const writeTicket = useRef(0);
  const mounted = useRef(true);
  // Synchronous scope guard captured at write start; compared at write end.
  const scopeRef = useRef("");
  scopeRef.current = `${project}|${role}|${permissionKey}`;
  const selectedIdRef = useRef<string | null>(null);
  selectedIdRef.current = selectedId;
  const pendingRef = useRef<PendingAction | null>(null);
  pendingRef.current = pending;
  // Cursor of the currently displayed authorized page; binds history reads.
  const pageCursorRef = useRef<string | null>(null);

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);

  // Keep the (often inline) callback in a ref so load/effect identities are
  // stable; otherwise every parent render would retrigger a fenced reload.
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  const reportError = useCallback((cause: unknown) => {
    const message = cause instanceof ApiError
      ? (cause.status === 401 || cause.status === 403
        ? "You are not authorized to view this task workspace."
        : cause.message)
      : "The task workspace could not be loaded.";
    onErrorRef.current?.(message);
    return message;
  }, []);

  const clearProtectedDetail = useCallback(() => {
    setDetail(null);
    setHistory([]);
    setHistoryPage(null);
  }, []);

  // ---- bounded authorized reads --------------------------------------------

  /** Authoritative page reload; returns the page so callers can re-validate
   * selection/intent against the current authorized set. */
  const load = useCallback(async (): Promise<WorkspacePage | null> => {
    const ticket = ++generation.current;
    setLoading(true);
    setError(null);
    try {
      const result = await api<WorkspacePage>(
        `projects/${project}/task-workspace?limit=${TASK_PAGE_SIZE}`);
      if (!mounted.current || ticket !== generation.current) return null;
      pageCursorRef.current = result.snapshot_cursor;
      setPage(result);
      return result;
    } catch (cause) {
      if (!mounted.current || ticket !== generation.current) return null;
      // Authorization/read failure: drop all protected evidence immediately.
      pageCursorRef.current = null;
      setPage(null);
      clearProtectedDetail();
      setSelectedId(null);
      setError(reportError(cause));
      return null;
    } finally {
      if (mounted.current && ticket === generation.current) setLoading(false);
    }
  }, [project, reportError, clearProtectedDetail]);

  // Project or permission change: clear protected state, invalidate in-flight
  // writes for the old scope, then reload.
  useEffect(() => {
    writeTicket.current += 1;
    setPage(null);
    clearProtectedDetail();
    setSelectedId(null);
    setPending(null);
    setActionBusy(false);
    setActionError(null);
    void load();
  }, [load, permissionKey, clearProtectedDetail]);

  // ---- selection (fenced) ----------------------------------------------------

  const select = useCallback(async (taskId: string | null, cursor?: string | null) => {
    const ticket = ++generation.current;
    // Selection LOSS invalidates writes tied to the previous selection; a
    // re-read of the SAME selection (refresh, post-write) must not fence an
    // in-flight write or discard a still-valid frozen intent.
    if (taskId !== selectedIdRef.current) {
      writeTicket.current += 1;
      setActionBusy(false);
      setPending(null);
      setActionError(null);
    }
    setSelectedId(taskId);
    clearProtectedDetail();
    if (!taskId) return;
    const boundCursor = cursor ?? pageCursorRef.current;
    try {
      const historyQuery = boundCursor ? `&snapshot_cursor=${boundCursor}` : "";
      const [taskDetail, firstPage] = await Promise.all([
        api<Record<string, unknown>>(`tasks/${taskId}`),
        api<TaskHistoryPage>(
          `tasks/${taskId}/workspace-history?limit=${HISTORY_PAGE_SIZE}${historyQuery}`),
      ]);
      if (!mounted.current || ticket !== generation.current) return;
      setDetail(taskDetail);
      setHistory(firstPage.entries);
      setHistoryPage(firstPage);
    } catch (cause) {
      if (!mounted.current || ticket !== generation.current) return;
      // Never preserve protected detail/history after a failed read.
      clearProtectedDetail();
      setSelectedId(null);
      setError(reportError(cause));
    }
  }, [reportError, clearProtectedDetail]);

  const loadMoreHistory = useCallback(async () => {
    if (!selectedId || !historyPage?.has_more || historyPage.next_offset === null) return;
    const ticket = generation.current;
    try {
      // Fresh authorization + snapshot binding happen server-side per page.
      const next = await api<TaskHistoryPage>(
        `tasks/${selectedId}/workspace-history?limit=${HISTORY_PAGE_SIZE}` +
        `&offset=${historyPage.next_offset}&snapshot_cursor=${historyPage.snapshot_cursor}`);
      if (!mounted.current || ticket !== generation.current) return;
      if (next.revision !== historyPage.revision) {
        // The task changed mid-pagination: old entries are not evidence of
        // the current revision, so they are dropped rather than merged.
        clearProtectedDetail();
        setSelectedId(null);
        setError("The task changed while paging history; select it again to re-read.");
        return;
      }
      setHistory(current => [...current, ...next.entries]);
      setHistoryPage(next);
    } catch (cause) {
      if (!mounted.current || ticket !== generation.current) return;
      // Snapshot drift or authorization loss: clear protected history/detail.
      clearProtectedDetail();
      setSelectedId(null);
      setError(reportError(cause));
    }
  }, [selectedId, historyPage, reportError, clearProtectedDetail]);

  // Parent-owned SSE invalidation: clear protected evidence BEFORE the
  // authoritative reload so stale checkpoint summaries cannot survive a
  // source-access invalidation, then re-read the still-visible selection.
  const firstRefresh = useRef(true);
  useEffect(() => {
    if (firstRefresh.current) {
      firstRefresh.current = false;
      return;
    }
    void (async () => {
      clearProtectedDetail();
      const result = await load();
      if (!result) return;
      // Mask a pending intent whose task fell out of the authorized page.
      const currentIntent = pendingRef.current;
      if (currentIntent && !result.tasks.some(task => task.id === currentIntent.task.id)) {
        writeTicket.current += 1;
        setActionBusy(false);
        setPending(null);
        setActionError(
          "The task left the current authorized page; the pending action was discarded.");
      } // A still-visible frozen review survives the refresh untouched.
      const currentSelection = selectedIdRef.current;
      if (currentSelection && result.tasks.some(task => task.id === currentSelection)) {
        await select(currentSelection, result.snapshot_cursor);
      } else if (currentSelection) {
        setSelectedId(null);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshVersion]);

  // ---- explicit canonical mutations ------------------------------------------

  const openReconcile = useCallback((task: WorkspaceTask, action: string) => {
    // Freeze the intent base once; the key is minted once per intent.
    setActionError(null);
    setPending({
      kind: "reconcile",
      task,
      base: freezePayload({expected_revision: task.revision, action}),
      key: crypto.randomUUID(),
      submitted: null,
    });
  }, []);

  const openHandoff = useCallback((task: WorkspaceTask) => {
    setActionError(null);
    setPending({
      kind: "handoff",
      task,
      base: freezePayload({expected_revision: task.revision}),
      key: crypto.randomUUID(),
      submitted: null,
    });
  }, []);

  /** First submit freezes the complete payload (reason/target included);
   * retries replay the exact frozen command under the original key. */
  const submitPending = useCallback(async (draft: Record<string, unknown> | null) => {
    if (!pending || actionBusy) return;
    let command = pending.submitted;
    if (!command) {
      const payload = freezePayload({...pending.base, ...(draft ?? {})});
      if (pending.kind === "handoff" && !payload.target_agent_id) {
        setActionError("Choose a target agent for the handoff.");
        return;
      }
      if (pending.kind === "reconcile" && !String(payload.reason ?? "").trim()) {
        setActionError("A reason is required so the reconcile is auditable.");
        return;
      }
      command = {payload, key: pending.key};
      setPending({...pending, submitted: command});
    }
    const ticket = ++writeTicket.current;
    const scope = scopeRef.current;
    const valid = () => mounted.current && ticket === writeTicket.current
      && scopeRef.current === scope;
    setActionBusy(true);
    setActionError(null);
    try {
      const path = pending.kind === "reconcile"
        ? `tasks/${pending.task.id}:reconcile`
        : `tasks/${pending.task.id}:handoff`;
      await api(path, "POST", command.payload, undefined, command.key);
      if (!valid()) return;
      setPending(null);
      setActionBusy(false);
      const result = await load();
      const currentSelection = selectedIdRef.current;
      if (result && currentSelection
          && result.tasks.some(task => task.id === currentSelection)) {
        await select(currentSelection, result.snapshot_cursor);
      }
    } catch (cause) {
      if (!valid()) return;
      // Revision conflicts and policy rejections are surfaced, never retried
      // with a changed command; the frozen payload stays for explicit replay.
      const message = cause instanceof ApiError ? cause.message : "The action could not be completed.";
      setActionError(message);
      onErrorRef.current?.(message);
      setActionBusy(false);
    }
  }, [pending, actionBusy, load, select]);

  const triageRequest = useCallback(async (request: BlockedRequestView, outcome: "acknowledge" | "resolved" | "declined") => {
    if (actionBusy) return;
    // Frozen exact payload and key minted at click time; never recomputed.
    const command: FrozenCommand = {
      payload: freezePayload(outcome === "acknowledge" ? {note: ""} : {outcome, note: ""}),
      key: crypto.randomUUID(),
    };
    const ticket = ++writeTicket.current;
    const scope = scopeRef.current;
    const valid = () => mounted.current && ticket === writeTicket.current
      && scopeRef.current === scope;
    setActionBusy(true);
    setActionError(null);
    try {
      const path = outcome === "acknowledge"
        ? `blocked-task-requests/${request.id}:acknowledge`
        : `blocked-task-requests/${request.id}:resolve`;
      await api(path, "POST", command.payload, undefined, command.key);
      if (!valid()) return;
      setActionBusy(false);
      await load();
    } catch (cause) {
      if (!valid()) return;
      const message = cause instanceof ApiError ? cause.message : "The triage action could not be completed.";
      setActionError(message);
      onErrorRef.current?.(message);
      setActionBusy(false);
    }
  }, [actionBusy, load]);

  const cancelPending = useCallback(() => {
    writeTicket.current += 1;
    setActionBusy(false);
    setPending(null);
    setActionError(null);
  }, []);

  // ---- derived view state ------------------------------------------------------

  const tasks = useMemo(() => page?.tasks ?? [], [page]);
  const tasksById = useMemo(() => new Map(tasks.map(task => [task.id, task])), [tasks]);
  const visible = useMemo(
    () => sortTasks(filterTasks(tasks, query, statusFilter)),
    [tasks, query, statusFilter]);
  const selected = selectedId ? tasksById.get(selectedId) ?? null : null;
  const statuses = useMemo(() => Object.keys(page?.counts.by_status ?? {}).sort(), [page]);
  const canOperate = role === "admin" || role === "human";

  if (loading && !page) {
    return <section className="trw" data-testid="task-run-workspace">
      <p className="trw-status">Loading task workspace…</p>
    </section>;
  }
  if (error && !page) {
    return <section className="trw" data-testid="task-run-workspace">
      <p className="trw-error" role="alert">{error}</p>
    </section>;
  }
  if (!page) return null;

  return <section className="trw" data-testid="task-run-workspace">
    <header className="trw-header">
      <h2>Tasks &amp; runs</h2>
      <dl className="trw-counts" data-testid="trw-counts">
        <div><dt>Total</dt><dd>{page.counts.total}{page.counts.truncated ? "+" : ""}</dd></div>
        {statuses.map(status => <div key={status}>
          <dt>{status}</dt><dd>{page.counts.by_status[status]}</dd>
        </div>)}
      </dl>
      {page.counts.truncated && <p className="trw-note">
        This project exceeds the bounded read; counts are partial. Filters apply only to loaded tasks.</p>}
    </header>

    <div className="trw-filters">
      <input type="search" placeholder="Filter by title or ID" value={query}
        aria-label="Filter tasks" onChange={event => setQuery(event.target.value)}/>
      <select value={statusFilter} aria-label="Filter by status"
        onChange={event => setStatusFilter(event.target.value)}>
        <option value="">All statuses</option>
        {statuses.map(status => <option key={status} value={status}>{status}</option>)}
      </select>
    </div>

    {error && <p className="trw-error" role="alert" data-testid="trw-read-error">{error}</p>}
    {actionError && !pending &&
      <p className="trw-error" role="alert" data-testid="trw-action-error">{actionError}</p>}

    <div className="trw-body">
      <ul className="trw-list" data-testid="trw-task-list">
        {visible.map(task => {
          const dependencies = unmetDependencies(task, tasksById);
          const assigned = agentName(agents, task.scheduling.assigned_agent_id
            ?? task.execution?.agent_id ?? null);
          return <li key={task.id}>
            <button type="button"
              className={`trw-task${task.id === selectedId ? " trw-task--selected" : ""}`}
              data-testid={`trw-task-${task.id}`}
              onClick={() => void select(task.id === selectedId ? null : task.id)}>
              <span className={`trw-badge trw-status-${task.status}`}>{progressLabel(task)}</span>
              <span className="trw-title">{task.title}</span>
              {task.plan_id && <span className="trw-plan">
                {planTitle(task, page.plans) ?? "Plan"}{task.plan_key ? ` · ${task.plan_key}` : ""}
              </span>}
              {task.priority > 0 && <span className="trw-priority">P{task.priority}</span>}
              {assigned && <span className="trw-agent">{assigned}</span>}
              {dependencies.unresolved > 0 && !isTerminal(task.status) &&
                <span className="trw-deps" data-testid={`trw-deps-${task.id}`}>
                  Blocked by {dependencies.unresolved} visible {dependencies.unresolved === 1 ? "dependency" : "dependencies"}
                </span>}
              {task.blocked && task.blocked.readiness === "blocked" &&
                <span className="trw-blocked" data-testid={`trw-blocked-${task.id}`}>
                  {task.blocked.blocker_count} {task.blocked.blocker_count === 1 ? "blocker" : "blockers"}
                </span>}
              {task.scheduling.escalated && <span className="trw-escalated">Escalated</span>}
            </button>
            {assigned && onOpenAgent && <button type="button" className="trw-agent-link"
              aria-label={`Open agent ${assigned}`} onClick={() => {
                const id = task.scheduling.assigned_agent_id ?? task.execution?.agent_id;
                if (id) onOpenAgent(id);
              }}>Open agent</button>}
          </li>;
        })}
        {visible.length === 0 && <li className="trw-empty">No tasks match the current filters.</li>}
      </ul>

      {page.tasks_has_more && <p className="trw-note" data-testid="trw-tasks-truncated">
        Showing the first {page.task_count} tasks; the project has more. Narrow with filters.</p>}

      {selected && <aside className="trw-detail" data-testid="trw-detail">
        <header>
          <h3>{selected.title}</h3>
          <button type="button" onClick={() => void select(null)} aria-label="Close detail">×</button>
        </header>
        <dl className="trw-facts">
          <div><dt>Status</dt><dd>{selected.status}</dd></div>
          <div><dt>Revision</dt><dd>{selected.revision}</dd></div>
          <div><dt>Scheduling</dt><dd>{selected.scheduling.state}</dd></div>
          <div><dt>Attempts</dt><dd>{selected.scheduling.attempts} / {selected.max_attempts}</dd></div>
          {selected.required_capabilities.length > 0 && <div>
            <dt>Capabilities</dt><dd>{selected.required_capabilities.join(", ")}</dd>
          </div>}
          {selected.execution && <div>
            <dt>Current run</dt>
            <dd>{agentName(agents, selected.execution.agent_id) ?? "—"}
              {selected.execution.progress !== null &&
                ` — ${selected.execution.progress}% self-reported`}</dd>
          </div>}
        </dl>

        {selected.blocked && <section className="trw-blockers" data-testid="trw-blockers">
          <h4>Readiness: {selected.blocked.readiness}</h4>
          {selected.blocked.blockers.length > 0
            ? <ul>{selected.blocked.blockers.map((blocker, index) =>
                <li key={`${blocker.code ?? "blocker"}-${index}`}>
                  <code>{blocker.code}</code>{blocker.message ? ` — ${blocker.message}` : ""}
                </li>)}</ul>
            : <p>No current blockers.</p>}
          <p className="trw-note">Explanation only — {selected.blocked.authority?.replace(/_/g, " ")}.</p>
        </section>}

        <section className="trw-history" data-testid="trw-history">
          <h4>Execution &amp; attempt history
            ({historyPage?.total ?? selected.execution_history_count})</h4>
          {history.length === 0 && <p>No recorded executions or attempts yet.</p>}
          <ul>
            {history.map((entry, index) =>
              <li key={historyEntryKey(entry, index)}>{describeHistoryEntry(entry)}</li>)}
          </ul>
          {historyPage?.has_more &&
            <button type="button" onClick={() => void loadMoreHistory()}
              data-testid="trw-history-more">Load more history</button>}
          <p className="trw-note">History entries are agent self-reports and operator actions,
            not verification.</p>
        </section>

        {canOperate && (canReconcile(selected, role) || canHandoff(selected, role)) &&
          <section className="trw-actions" data-testid="trw-actions">
            <h4>Operator actions</h4>
            {canReconcile(selected, role) && <>
              <button type="button" disabled={actionBusy}
                onClick={() => openReconcile(selected, "requeue")}>Requeue</button>
              <button type="button" disabled={actionBusy}
                onClick={() => openReconcile(selected, "reassign")}>Clear assignment</button>
              <button type="button" disabled={actionBusy}
                onClick={() => openReconcile(selected, "escalate")}>Escalate</button>
            </>}
            {canHandoff(selected, role) &&
              <button type="button" disabled={actionBusy}
                onClick={() => openHandoff(selected)}>Hand off…</button>}
          </section>}
      </aside>}
    </div>

    {page.blocked_requests && page.blocked_requests.length > 0 &&
      <section className="trw-requests" data-testid="trw-requests">
        <h3>Blocked-task requests</h3>
        <ul>
          {page.blocked_requests.map(request => <li key={request.id}>
            <span className="trw-title">{tasksById.get(request.task_id)?.title ?? "A visible task"}</span>
            <span className="trw-badge">{request.state}</span>
            {request.action && <code>{request.action}</code>}
            {canOperate && (request.state === "open" || request.state === "acknowledged") && <>
              {request.state === "open" && <button type="button" disabled={actionBusy}
                data-testid={`trw-ack-${request.id}`}
                onClick={() => void triageRequest(request, "acknowledge")}>Acknowledge</button>}
              <button type="button" disabled={actionBusy}
                data-testid={`trw-resolve-${request.id}`}
                onClick={() => void triageRequest(request, "resolved")}>Resolve</button>
              <button type="button" disabled={actionBusy}
                onClick={() => void triageRequest(request, "declined")}>Decline</button>
            </>}
          </li>)}
        </ul>
      </section>}

    {pending && <div className="trw-dialog" role="dialog" aria-modal="true"
      data-testid="trw-confirm-dialog">
      <PendingDialog pending={pending} agents={agents} busy={actionBusy} error={actionError}
        onCancel={cancelPending} onSubmit={draft => void submitPending(draft)}/>
    </div>}
  </section>;
}

function PendingDialog(props: {
  pending: PendingAction;
  agents?: WorkspaceAgent[];
  busy: boolean;
  error: string | null;
  onCancel: () => void;
  onSubmit: (draft: Record<string, unknown> | null) => void;
}) {
  const {pending, agents, busy, error, onCancel, onSubmit} = props;
  const [reason, setReason] = useState("");
  const [target, setTarget] = useState("");
  // After the first submit the command is frozen: fields lock and the
  // confirm button replays the exact payload under the original key.
  const frozen = pending.submitted !== null;
  const title = pending.kind === "reconcile"
    ? `${String(pending.base.action)} “${pending.task.title}”`
    : `Hand off “${pending.task.title}”`;
  return <form className="trw-dialog-card" onSubmit={event => {
    event.preventDefault();
    onSubmit(frozen ? null
      : pending.kind === "reconcile" ? {reason} : {reason, target_agent_id: target});
  }}>
    <h3>{title}</h3>
    <p className="trw-note">This runs the canonical scheduling command at revision{" "}
      {pending.task.revision}. If the task changed since, the command is rejected and
      nothing is retried automatically.</p>
    {frozen && <p className="trw-note" data-testid="trw-frozen-note">
      The command is frozen. Confirm replays the exact same payload and idempotency key.</p>}
    {pending.kind === "handoff" &&
      <label>Target agent
        <select value={frozen ? String(pending.submitted!.payload.target_agent_id) : target}
          required disabled={busy || frozen}
          onChange={event => setTarget(event.target.value)}
          data-testid="trw-handoff-target">
          <option value="">Choose an agent…</option>
          {(agents ?? [])
            .filter(agent => agent.id !== pending.task.execution?.agent_id)
            .map(agent => <option key={agent.id} value={agent.id}>{agent.display_name}</option>)}
        </select>
      </label>}
    <label>Reason
      <textarea
        value={frozen ? String(pending.submitted!.payload.reason ?? "") : reason}
        required={pending.kind === "reconcile"} rows={3} disabled={busy || frozen}
        data-testid="trw-action-reason"
        onChange={event => setReason(event.target.value)}/>
    </label>
    {error && <p className="trw-error" role="alert">{error}</p>}
    <div className="trw-dialog-actions">
      <button type="submit" disabled={busy} data-testid="trw-action-confirm">
        {busy ? "Working…" : frozen ? "Retry exact command" : "Confirm"}
      </button>
      <button type="button" disabled={busy} onClick={onCancel}>Cancel</button>
    </div>
  </form>;
}
