/**
 * Conflict/control console: pure contracts, wire types, and honest lifecycle
 * labeling for the canonical coordination records.
 *
 * Rules encoded here:
 * - A control is never "paused"/"steered"/"resumed" because the POST was
 *   accepted. Canonical control states map to requested (queued) → delivered
 *   (acknowledged) → applying → applied/failed/outcome_unknown, and terminal
 *   administrative states expired/superseded. The runtime-reported status is
 *   shown only as attributed receipt data from the control result.
 * - Run presence (online/stale/offline) is volatile presence only; it is
 *   never an acknowledgment of a control.
 * - Redacted fields are counts, never enumerated ids.
 * - Idempotency keys are minted once per operator intent and reused verbatim
 *   with the frozen payload on retry, so a retry is a replay, not a new
 *   command.
 */
import {api} from "./api";

// ---- wire types (mirror hivemind/conflict_control_console.py) --------------

export type ConsoleClaim = {
  id: string; run_id: string; task_id: string | null; mode: string | null;
  state: string | null; scope_ids: string[]; hidden_scope_count: number;
  expires_at: number | null; ownership_epoch: number | null; revision: number;
};

export type ClaimOverlap = {
  kind: string; claim_ids: [string, string] | string[]; run_ids: string[]; modes: string[];
};

export type ConsoleConflict = {
  id: string; winner_claim_id: string; requesting_run_id: string;
  scope_ids: string[]; hidden_scope_count: number; state: string;
  enforcement?: string; policy_version?: number; revision: number;
};

export type ConsoleAction = {
  id: string; task_id: string | null; task_redacted?: boolean; action: string;
  state: string; reason?: string; evidence?: Record<string, unknown>;
  outcome?: Record<string, unknown> | null; revision: number; created_at?: string;
  finished_at?: string; attempt?: number;
};

export type ConsoleControl = {
  id: string; action: string; state: string; reason?: string;
  target_run_id: string | null; target_redacted?: boolean;
  target_agent_id?: string; issuer_agent_id?: string;
  coordinator_epoch?: number; replacement_task_id?: string | null;
  expires_at?: number; revision: number; created_at?: string;
  result?: {outcome?: string; observed_status?: string | null; checkpoint_id?: string | null} | null;
  outcome?: Record<string, unknown> | null;
  delegation?: {grant_id: string; revision: number} | null;
  reserved_claim_id?: string | null;
};

export type ConsoleRun = {
  id: string; agent_id: string; status: string; control_revision: number;
  task_id?: string | null; presence: string; presence_is_acknowledgment: boolean;
  revision: number;
};

export type ConsoleAgent = {
  id: string; display_name?: string; supported_actions: string[];
  scope_permit_enforcement: boolean;
};

export type ConsoleLease = {
  id: string; agent_id: string; coordinator_epoch: number; expires_at: number; state: string;
};

/**
 * Truthful, read-only issuance hint from the aggregate. A merely visible lease
 * never confers issuance: the canonical path requires representing the
 * lease-holding agent under a coordinator grant (admin bypasses). Advisory
 * only — the server re-authorizes on issue.
 */
export type IssuerHint = {
  lease_visible: boolean;
  advisory: boolean;
  principal_may_issue: boolean;
  explanation: string;
};

export type SteerTarget = {id: string; title?: string; revision: number};

export type ConsoleSnapshot = {
  project_id: string; generated_at: number; snapshot_cursor: string;
  policy: Record<string, unknown>;
  counts: Record<string, number> & {
    claims: number; overlaps: number; conflicts: number; holds: number;
    actions: number; controls: number; runs: number; pending_controls: number;
  };
  claims: ConsoleClaim[]; claim_overlaps: ClaimOverlap[]; conflicts: ConsoleConflict[];
  holds: ConsoleAction[]; actions: ConsoleAction[]; controls: ConsoleControl[];
  lease: ConsoleLease | null; issuer_hint: IssuerHint; runs: ConsoleRun[]; agents: ConsoleAgent[];
  steer_targets: SteerTarget[];
  redactions: Record<string, number>;
  truncated: Record<string, boolean>;
  limits: Record<string, number>;
  honesty: {
    presence_is_acknowledgment: boolean;
    control_acceptance_is_applied: boolean;
    aggregate_is_read_only: boolean;
    command_surfaces: string[];
  };
};

// ---- control lifecycle: honest labels only ---------------------------------

export type ControlPhase =
  | "requested" | "delivered" | "applying" | "applied" | "failed"
  | "outcome_unknown" | "expired" | "superseded" | "unknown";

/** Map the canonical control state to an honest lifecycle phase. */
export function controlPhase(control: Pick<ConsoleControl, "state">): ControlPhase {
  switch (control.state) {
    case "queued": return "requested";
    case "acknowledged": return "delivered";
    case "applying": return "applying";
    case "applied": return "applied";
    case "failed": return "failed";
    case "outcome_unknown": return "outcome_unknown";
    case "expired": return "expired";
    case "superseded": return "superseded";
    default: return "unknown";
  }
}

export const CONTROL_PHASE_LABEL: Record<ControlPhase, string> = {
  requested: "Requested (queued for delivery)",
  delivered: "Delivered (acknowledged by target runtime)",
  applying: "Applying (in progress at target runtime)",
  applied: "Applied (per target runtime result receipt)",
  failed: "Failed (per target runtime result receipt)",
  outcome_unknown: "Outcome unknown (interrupted while applying)",
  expired: "Expired before completion",
  superseded: "Superseded (authorization lapsed before completion)",
  unknown: "Unknown state",
};

/** A queued/acknowledged control is never a paused or steered agent. */
export function controlPhaseLabel(control: ConsoleControl): string {
  return CONTROL_PHASE_LABEL[controlPhase(control)];
}

/** The runtime-reported status, attributed and only from a result receipt. */
export function reportedStatusLabel(control: ConsoleControl): string | null {
  const observed = control.result?.observed_status;
  if (!observed) return null;
  return `Target runtime reported: ${observed} (receipt, not a guarantee of current state)`;
}

export function controlIsPending(control: ConsoleControl): boolean {
  return ["queued", "acknowledged", "applying"].includes(control.state);
}

// ---- action/hold labeling ---------------------------------------------------

export function holdReasonLabel(reason: string | undefined): string {
  switch (reason) {
    case "execution_liveness_unknown":
      return "Attempt deadline passed and the runtime is silent; held for operator review";
    case "escalated_execution_ambiguous":
      return "Escalation budget reached with an ambiguous execution; held";
    case "execution_record_missing":
      return "Dispatched without an execution record; held";
    default: return reason ?? "Held";
  }
}

// ---- overlap/claim helpers ----------------------------------------------------

/** Index overlaps per claim id for rendering. */
export function overlapsByClaim(overlaps: ClaimOverlap[]): Map<string, ClaimOverlap[]> {
  const map = new Map<string, ClaimOverlap[]>();
  for (const overlap of overlaps) {
    for (const id of overlap.claim_ids) {
      map.set(id, [...(map.get(id) ?? []), overlap]);
    }
  }
  return map;
}

// ---- command preparation: frozen payload, immutable idempotency key ---------

export type ControlAction = "steer" | "pause" | "resume" | "cancel" | "assign_task";

export type PreparedControl = {
  /** Minted once; reused verbatim on every retry of this exact intent. */
  idempotencyKey: string;
  /** Frozen at preparation time; the wire body never drifts from the key. */
  payload: Readonly<{
    project_id: string; target_run_id: string; action: ControlAction;
    replacement_task_id: string | null; target_run_revision: number;
    coordinator_epoch: number; reason: string;
  }>;
};

export function prepareControlCommand(input: {
  projectId: string; targetRunId: string; action: ControlAction;
  replacementTaskId?: string | null; targetRunRevision: number;
  coordinatorEpoch: number; reason: string;
}): PreparedControl {
  const payload = Object.freeze({
    project_id: input.projectId,
    target_run_id: input.targetRunId,
    action: input.action,
    replacement_task_id: input.action === "steer" || input.action === "assign_task"
      ? input.replacementTaskId ?? null : null,
    target_run_revision: input.targetRunRevision,
    coordinator_epoch: input.coordinatorEpoch,
    reason: input.reason,
  });
  return {idempotencyKey: crypto.randomUUID(), payload};
}

/** Issue a prepared control through the canonical surface; replay-safe. */
export function issueControl(prepared: PreparedControl): Promise<ConsoleControl> {
  return api<ConsoleControl>("controls", "POST", prepared.payload, undefined,
    prepared.idempotencyKey);
}

export type HoldResolution = "reassign" | "escalate" | "dead_letter" | "dismiss";

export type PreparedHoldResolution = {
  /** Minted once; reused verbatim on every retry of this exact resolution. */
  idempotencyKey: string;
  actionId: string;
  /** Frozen at preparation time; a retry replays this exact body. */
  payload: Readonly<{
    expected_revision: number; action: HoldResolution; reason: string;
  }>;
};

/** Freeze the complete hold-resolution intent: key and body are bound together. */
export function prepareHoldResolution(input: {
  actionId: string; expectedRevision: number; resolution: HoldResolution; reason: string;
}): PreparedHoldResolution {
  return {
    idempotencyKey: crypto.randomUUID(),
    actionId: input.actionId,
    payload: Object.freeze({
      expected_revision: input.expectedRevision,
      action: input.resolution,
      reason: input.reason,
    }),
  };
}

/** Resolve a manual hold through the canonical coordination surface. */
export function resolveHold(prepared: PreparedHoldResolution): Promise<ConsoleAction> {
  return api<ConsoleAction>(
    `coordination-actions/${encodeURIComponent(prepared.actionId)}:resolve`,
    "POST", prepared.payload, undefined, prepared.idempotencyKey);
}

// ---- snapshot fetch ------------------------------------------------------------

export function fetchConsoleSnapshot(projectId: string, options: {
  snapshotCursor?: string | null;
} = {}): Promise<ConsoleSnapshot> {
  const params = new URLSearchParams();
  const cursor = options.snapshotCursor;
  if (cursor && /^[0-9a-f]{64}$/.test(cursor)) params.set("snapshot_cursor", cursor);
  const query = params.size ? `?${params}` : "";
  return api<ConsoleSnapshot>(
    `projects/${encodeURIComponent(projectId)}/conflict-console${query}`);
}

// ---- eligibility: accurate disabled explanations, never faux-enabling --------

export type ControlEligibility = {
  allowed: boolean;
  /** Accurate, operator-facing explanation when not allowed. */
  reason: string;
};

const CONTROL_ACTIONS: ControlAction[] = ["pause", "resume", "cancel", "steer"];

/**
 * Static, client-side pre-check only. The canonical control path re-authorizes
 * everything (lease epoch, delegation, team access, target revision); this
 * exists so unsupported boundaries are disabled with a true explanation rather
 * than failing after the fact.
 */
export function controlEligibility(input: {
  action: ControlAction;
  run: ConsoleRun | undefined;
  agent: ConsoleAgent | undefined;
  lease: ConsoleLease | null;
  issuerHint?: IssuerHint | null;
  nowSeconds: number;
  steerTarget?: SteerTarget | null;
}): ControlEligibility {
  const {action, run, agent, lease, issuerHint, nowSeconds} = input;
  if (!CONTROL_ACTIONS.includes(action)) {
    return {allowed: false, reason: `Control action '${action}' is not supported here.`};
  }
  if (!run) {
    return {allowed: false, reason: "The target run is not visible to you (redacted or absent)."};
  }
  if (["completed", "failed", "cancelled"].includes(run.status)) {
    return {allowed: false, reason: `The target run is already ${run.status}; terminal runs cannot be controlled.`};
  }
  if (!lease) {
    return {allowed: false,
      reason: "No active Nexus coordination lease is visible. Controls are issued by the lease-holding coordinator; this console never impersonates Nexus."};
  }
  if (lease.expires_at <= nowSeconds) {
    return {allowed: false, reason: "The visible Nexus lease has expired; a renewed lease is required."};
  }
  if (issuerHint && !issuerHint.principal_may_issue) {
    return {allowed: false,
      reason: issuerHint.explanation
        || "Your credentials cannot issue Nexus controls for this lease."};
  }
  if (!agent) {
    return {allowed: false, reason: "The target agent is not visible to you."};
  }
  if (!agent.scope_permit_enforcement) {
    return {allowed: false,
      reason: `${agent.display_name ?? agent.id} does not enforce scope permits; the control boundary is unsupported.`};
  }
  if (!agent.supported_actions.includes(action)) {
    return {allowed: false,
      reason: `${agent.display_name ?? agent.id} does not advertise support for '${action}'; the request would be refused as unsupported.`};
  }
  if ((action === "steer" || action === "assign_task") && !input.steerTarget) {
    return {allowed: false,
      reason: "Steering requires selecting an actual queued, scoped task from the authorized list."};
  }
  return {allowed: true, reason: ""};
}

// ---- display helpers -------------------------------------------------------------

export const CONSOLE_PAGE_SIZE = 25;

export function pageSlice<T>(items: T[], page: number): {page: T[]; pages: number} {
  const pages = Math.max(1, Math.ceil(items.length / CONSOLE_PAGE_SIZE));
  const current = Math.max(1, Math.min(page, pages));
  return {page: items.slice((current - 1) * CONSOLE_PAGE_SIZE, current * CONSOLE_PAGE_SIZE),
    pages};
}

export function redactionNote(redactions: Record<string, number>): string | null {
  const total = Object.values(redactions).reduce((sum, value) => sum + value, 0);
  if (!total) return null;
  return `${total} protected reference${total === 1 ? "" : "s"} redacted; hidden ids are never enumerated.`;
}
