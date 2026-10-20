// Types and pure logic for the task/run workspace. Nothing here performs I/O;
// every function is deterministic so the component and the tests share one
// source of truth. Progress and completion are NEVER inferred from the
// presence of executions, history, or assignments — only the canonical
// task.status (and explicitly labelled agent self-reports) are displayed.

export interface WorkspaceAgent {
  id: string;
  display_name: string;
  base_color_token?: string;
  effective_role?: string;
}

export interface SchedulingView {
  state: string;
  attempts: number;
  dispatches: number;
  reassignments: number;
  assigned_agent_id: string | null;
  escalated: boolean;
  escalation: {reason?: string; detail?: string; at?: string; agent_id?: string | null} | null;
  retry_after: number | null;
  last_outcome: {outcome?: string; summary?: string; at?: string} | null;
  last_reconcile: {action?: string; reason?: string; at?: string; by?: string} | null;
  attempt_history_count: number;
}

export interface ExecutionSummary {
  id: string | null;
  run_id: string | null;
  agent_id: string | null;
  started_at: number | null;
  progress: number | null;
  progress_basis: string;
  checkpoint?: {id: string | null; summary?: string; evidence_unavailable: boolean};
}

export interface BlockedView {
  readiness: string;
  readiness_scope: string;
  blocker_count: number;
  blockers: {code?: string; action?: string; source?: string; message?: string}[];
  suggested_actions: string[];
  authority: string;
}

export interface WorkspaceTask {
  id: string;
  revision: number;
  title: string;
  status: string;
  created_at: string | null;
  priority: number;
  max_attempts: number;
  required_capabilities: string[];
  plan_id: string | null;
  plan_key: string | null;
  plan_order: number;
  depends_on: string[];
  external_dependency_ids: string[];
  scheduling: SchedulingView;
  execution: ExecutionSummary | null;
  execution_history_count: number;
  latest_execution: ExecutionSummary | null;
  blocked?: BlockedView | null;
}

export interface WorkspacePlan {
  id: string;
  revision: number;
  title: string;
  task_ids: string[];
  task_count: number;
}

export interface BlockedRequestView {
  id: string;
  task_id: string;
  state: string;
  action?: string;
  note?: string;
  close_note?: string | null;
  created_at?: string;
  closed_at?: string | null;
  [key: string]: unknown;
}

export interface WorkspaceCounts {
  total: number;
  by_status: Record<string, number>;
  truncated: boolean;
}

export interface WorkspacePage {
  project_id: string;
  snapshot_cursor: string;
  tasks: WorkspaceTask[];
  task_count: number;
  tasks_has_more: boolean;
  next_offset: number | null;
  counts: WorkspaceCounts;
  plans: WorkspacePlan[];
  plans_truncated: boolean;
  blocked_requests: BlockedRequestView[] | null;
}

export interface HistoryEntry {
  kind: "execution" | "attempt";
  at?: string | number;
  started_at?: string | number;
  [key: string]: unknown;
}

export interface TaskHistoryPage {
  task_id: string;
  revision: number;
  /** Checked snapshot binding for this page; required on follow-up pages. */
  snapshot_cursor: string;
  entries: HistoryEntry[];
  count: number;
  total: number;
  has_more: boolean;
  next_offset: number | null;
  basis: string;
}

// ---- status semantics ------------------------------------------------------

export const TERMINAL_STATUSES = new Set(["completed", "cancelled"]);

/** Display order for statuses; unknown statuses sort after known ones. */
const STATUS_ORDER = ["failed", "running", "paused", "queued", "completed", "cancelled"];

export function isTerminal(status: string): boolean {
  return TERMINAL_STATUSES.has(status);
}

// ---- pure derivations ------------------------------------------------------

/** Canonical actions exist only for non-terminal tasks and operator roles. */
export function canReconcile(task: WorkspaceTask, role: string): boolean {
  return (role === "admin" || role === "human") && !isTerminal(task.status);
}

/** Handoff additionally requires a current execution (canonical requirement). */
export function canHandoff(task: WorkspaceTask, role: string): boolean {
  return canReconcile(task, role) && task.execution !== null;
}

export function sortTasks(tasks: WorkspaceTask[]): WorkspaceTask[] {
  return [...tasks].sort((a, b) => {
    const orderA = STATUS_ORDER.indexOf(a.status);
    const orderB = STATUS_ORDER.indexOf(b.status);
    const rankA = orderA === -1 ? STATUS_ORDER.length : orderA;
    const rankB = orderB === -1 ? STATUS_ORDER.length : orderB;
    if (rankA !== rankB) return rankA - rankB;
    if (a.priority !== b.priority) return b.priority - a.priority;
    return (a.created_at ?? "").localeCompare(b.created_at ?? "");
  });
}

export function filterTasks(tasks: WorkspaceTask[], query: string, status: string): WorkspaceTask[] {
  const needle = query.trim().toLowerCase();
  return tasks.filter(task => {
    if (status && task.status !== status) return false;
    if (!needle) return true;
    return task.title.toLowerCase().includes(needle) || task.id.toLowerCase().includes(needle);
  });
}

/**
 * Visible unresolved dependencies. The server already dropped dependencies
 * the caller may not read, so this never reveals hidden task IDs; a
 * dependency missing from the visible page is treated as unresolved only
 * when it is listed but absent (e.g. on another page) — never guessed.
 */
export function unmetDependencies(task: WorkspaceTask, byId: Map<string, WorkspaceTask>): {
  visible: WorkspaceTask[];
  unresolved: number;
} {
  const ids = [...task.depends_on, ...task.external_dependency_ids];
  const visible: WorkspaceTask[] = [];
  let unresolved = 0;
  for (const id of new Set(ids)) {
    const dependency = byId.get(id);
    if (dependency) {
      visible.push(dependency);
      if (dependency.status !== "completed") unresolved += 1;
    } else {
      unresolved += 1; // Listed dependency outside the current page: not known-met.
    }
  }
  return {visible, unresolved};
}

export function planTitle(task: WorkspaceTask, plans: WorkspacePlan[]): string | null {
  if (!task.plan_id) return null;
  return plans.find(plan => plan.id === task.plan_id)?.title ?? null;
}

/** Explicit status label; progress is attributed to the self-report, never implied. */
export function progressLabel(task: WorkspaceTask): string {
  if (task.status === "running" && task.execution && task.execution.progress !== null) {
    return `Running — ${task.execution.progress}% self-reported`;
  }
  return task.status.charAt(0).toUpperCase() + task.status.slice(1);
}

export function agentName(agents: WorkspaceAgent[] | undefined, id: string | null): string | null {
  if (!id) return null;
  return agents?.find(agent => agent.id === id)?.display_name ?? id;
}

/** Stable frozen mutation payloads; created once per explicit user action. */
export function freezePayload<T extends Record<string, unknown>>(payload: T): Readonly<T> {
  return Object.freeze({...payload});
}

export function historyEntryKey(entry: HistoryEntry, index: number): string {
  return `${entry.kind}:${String(entry.id ?? entry.execution_id ?? index)}:${String(entry.at ?? entry.started_at ?? "")}`;
}

export function describeHistoryEntry(entry: HistoryEntry): string {
  if (entry.kind === "attempt") {
    const outcome = typeof entry.outcome === "string" ? entry.outcome : "recorded";
    const summary = typeof entry.summary === "string" ? entry.summary : "";
    return `Attempt ${outcome}${summary ? ` — ${summary}` : ""}`;
  }
  const progress = typeof entry.progress === "number" ? ` (${entry.progress}% self-reported)` : "";
  const summary = typeof entry.summary === "string" ? entry.summary
    : typeof entry.outcome === "string" ? entry.outcome : "Execution record";
  return `${summary}${progress}`;
}
