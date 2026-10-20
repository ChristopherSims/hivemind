/**
 * Operations dashboard contracts: the exact shape of
 * GET /v1/projects/{project_id}/operations-summary plus the pure derivations
 * the view is allowed to make. Nothing here invents measurements: an
 * unavailable section is rendered as unknown, an empty backlog is a measured
 * zero only when the section reported available, and presence is never
 * presented as capture verification.
 */
import {api} from "./api";

export type SectionAvailability = {available: boolean; reason?: string};

export type OperationsWatermark = {
  available: boolean;
  through_ingestion_seq: number;
  pending_jobs: number;
  lag_ingestion_seq: number;
};

export type OperationsIngestion = SectionAvailability & {
  canonical_through_ingestion_seq?: number;
  vector?: OperationsWatermark;
  graph?: OperationsWatermark;
  index_read_path?: "dedicated" | "canonical_fallback";
  graph_projection?: "available" | "unavailable";
  semantic_encoder?: "configured" | "absent";
};

export type JobStateCounts = {pending: number; done: number; dead: number; other: number};

export type OperationsJobs = SectionAvailability & {
  scope?: "project";
  project?: {total: JobStateCounts; by_kind: Record<string, JobStateCounts>};
  /** Cross-project counters; administrator only, never a project count. */
  global?: {scope: "global_all_projects"; by_state: Record<string, number>} | null;
};

export const DELIVERY_STATES = ["available", "leased", "retry_wait", "dead_letter", "expired", "acked"] as const;
export type DeliveryState = (typeof DELIVERY_STATES)[number];

export type OperationsDelivery = SectionAvailability & {
  scope?: "authorized_principal";
  states?: Record<DeliveryState, number>;
  scanned?: number;
  limit?: number;
  truncated?: boolean;
  window_note?: string;
};

export type PresenceState = "online" | "stale" | "offline" | "unknown";
export type CaptureState = "healthy" | "gaps" | "unknown";

export type OperationsRunSample = {
  id: string;
  agent_id: string | null;
  status: string | null;
  presence: PresenceState;
  capture_state: CaptureState;
  last_seen_at: number | null;
};

export type OperationsRuns = SectionAvailability & {
  scanned?: number;
  limit?: number;
  truncated?: boolean;
  presence?: Record<PresenceState, number>;
  capture?: Record<CaptureState | "other", number>;
  presence_thresholds_seconds?: {online_within: number; stale_within: number};
  presence_basis?: string;
  presence_note?: string;
  sample?: OperationsRunSample[];
  sample_limit?: number;
  sample_truncated?: boolean;
};

export type OperationsTelemetry = SectionAvailability & {
  scope?: "project";
  enabled?: boolean;
  export_enabled?: boolean;
  queued?: number;
  dropped?: number | null;
  dropped_scope?: "global_all_projects";
  delivery?: string;
  content_capture?: boolean;
};

export type OperationsWarning = {
  code: string;
  scope: string;
  count: number | null;
  message: string;
};

export type OperationsSummary = {
  project_id: string;
  scope: "project";
  role: string;
  generated_at: number;
  generated_at_unix_seconds: number;
  generated_at_note?: string;
  ingestion: OperationsIngestion;
  jobs: OperationsJobs;
  delivery: OperationsDelivery;
  runs: OperationsRuns;
  telemetry: OperationsTelemetry;
  warnings: OperationsWarning[];
};

/** Read-only point-in-time operations summary for one authorized project. */
export async function fetchOperationsSummary(projectId: string): Promise<OperationsSummary> {
  return api<OperationsSummary>(`projects/${encodeURIComponent(projectId)}/operations-summary`);
}

/** Stable, user-facing titles for the coarse server warning codes. */
export const WARNING_TITLES: Record<string, string> = {
  dead_letter_messages: "Undeliverable messages",
  delivery_window_truncated: "Delivery window truncated",
  index_jobs_dead: "Indexing jobs exhausted retries",
  index_read_path_fallback: "Index fallback read path",
  graph_projection_unavailable: "Graph projection unavailable",
  semantic_encoder_absent: "Semantic encoder absent",
  capture_gaps_reported: "Measured capture gaps",
  runs_window_truncated: "Run window truncated",
  telemetry_spans_dropped: "Diagnostic spans dropped",
};

export function warningTitle(warning: OperationsWarning): string {
  return WARNING_TITLES[warning.code] ?? "Operational warning";
}

/** Group identical warning codes; the total count is a sum only when every
 * entry carried a measured count, otherwise it is explicitly unknown. */
export function summarizeWarnings(warnings: OperationsWarning[]):
    {code: string; title: string; count: number | null; occurrences: number; message: string}[] {
  const groups = new Map<string, {count: number | null; occurrences: number; message: string}>();
  for (const warning of warnings) {
    const group = groups.get(warning.code) ?? {count: 0, occurrences: 0, message: warning.message};
    group.occurrences += 1;
    group.count = group.count === null || warning.count === null ? null : group.count + warning.count;
    group.message = warning.message;
    groups.set(warning.code, group);
  }
  return [...groups.entries()].map(([code, group]) => ({
    code, title: WARNING_TITLES[code] ?? "Operational warning", ...group,
  }));
}

export const PRESENCE_LABELS: Record<PresenceState, string> = {
  online: "Online", stale: "Stale", offline: "Offline", unknown: "Unknown",
};
export const CAPTURE_LABELS: Record<CaptureState | "other", string> = {
  healthy: "Reported healthy", gaps: "Measured gaps", unknown: "Unknown", other: "Unrecognized",
};
export const DELIVERY_LABELS: Record<DeliveryState, string> = {
  available: "Available", leased: "Leased", retry_wait: "Retry wait",
  dead_letter: "Dead letter", expired: "Expired", acked: "Acked",
};

/** A watermark lag is shown only when the section actually measured one. */
export function lagLabel(watermark: OperationsWatermark | undefined): string {
  if (!watermark || watermark.available !== true) return "Unknown";
  return watermark.lag_ingestion_seq === 0
    ? "Caught up"
    : `${watermark.lag_ingestion_seq} seq behind (${watermark.pending_jobs} pending)`;
}

/** Render a sampled unix timestamp; a missing sample is unknown, not "now". */
export function sampleTimeLabel(unixSeconds: number | null | undefined): string {
  if (typeof unixSeconds !== "number" || !Number.isFinite(unixSeconds)) return "Unknown";
  return new Date(unixSeconds * 1000).toLocaleString();
}

/** Truncation note for a bounded scan window; absent when nothing was cut. */
export function windowNote(scanned: number | undefined, limit: number | undefined,
                           truncated: boolean | undefined): string {
  if (!truncated || scanned === undefined || limit === undefined) return "";
  return `Newest ${scanned} of a ${limit}-row window; older rows are not counted here.`;
}
