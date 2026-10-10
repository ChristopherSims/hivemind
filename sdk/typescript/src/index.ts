export type AgentSelector = {
  agent_ids?: string[];
  semantic_query?: string;
  match?: "union" | "intersection";
  active_only?: boolean;
  max_agents?: number;
};
export type ContextRequest = {
  project_id: string; agent_selector: AgentSelector; query?: string;
  token_budget?: number; require_verified_memory?: boolean;
  requester_run_id?: string; target_tokenizer?: string; min_ingestion_seq?: number;
};
export type Envelope<T> = {protocol: "hivemind"; protocol_version: "0.1.0"; data: T};
export type MemoryVersionProposal = {
  expected_revision: number; title: string;
  content: {media_type: "text/plain" | "text/markdown"; text: string};
  evidence_refs: {kind: string; id: string; revision?: number}[];
  validity?: {valid_from?: string | null; valid_until?: string | null;
    repository_id?: string | null; branch?: string | null; observed_commit?: string | null};
  expires_at?: string | null;
};
export type MemoryReview = {expected_revision: number; decision: "verify" | "reject"; reason: string};
export type DeliveryStatus = "available" | "leased" | "retry_wait" | "dead_letter" | "expired" | "acked";
export type DeliveryReceipt = {id: string; revision: number; delivery_id: string;
  delivery_status: DeliveryStatus; attempt_count: number; claim_until: number};
export type DeliverySummary = {unread: number; count: number; limit: number;
  states: Record<DeliveryStatus, number>;
  by_agent: Record<string, {pending: number; dead_letter: number; unread: number}>};
export type TimelineOptions = {query?: string; before?: number; limit?: number; agent_id?: string};
export type TimelinePage<T = unknown> = {items: T[]; next_before: number | null; snapshot_cursor: string};
export type MemoryExtractRequest = {project_id: string; agent_id: string; requester_run_id?: string | null;
  source_refs: {kind: "board_post" | "artifact"; id: string; revision?: number}[];
  scope_kind?: "agent" | "project" | "run" | "task"; scope_owner_id?: string | null;
  max_candidates?: number; max_source_bytes?: number};
export type AgentNotification = {id: string; type: "memory_review" | "board_mention"; state: string;
  subject_id?: string; subject_kind?: string; subject_revision?: number;
  memory_id?: string; memory_version_id?: string; decision?: string; current_memory_status?: string};
export type MemoryExtractionResult = {method: "extractive-v1";
  memories: {id: string; revision: number; status: string; origin: string; memory_version_id: string;
    content: {media_type: string; text?: string}; evidence_refs: {kind: string; id: string; revision: number}[]}[];
  skipped: {source_id: string; reason: string}[]};
export type TaskCheckpoint = {expected_revision: number; run_id: string; execution_id: string;
  checkpoint_id: string; status: "running" | "paused" | "completed" | "failed";
  progress: number; summary: string; evidence_refs?: {kind: "board_post" | "artifact"; id: string; revision?: number}[]};

export class Hivemind {
  constructor(private baseURL: string, private token: string) {}
  async request<T>(method: string, path: string, body?: unknown, key?: string): Promise<T> {
    const response = await fetch(`${this.baseURL.replace(/\/$/, "")}/v0.1/${path}`, {
      method,
      headers: {Authorization: `Bearer ${this.token}`, "Content-Type": "application/json",
        ...(method !== "GET" ? {"Idempotency-Key": key ?? crypto.randomUUID()} : {})},
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) throw new Error(`Hivemind request rejected (${response.status})`);
    return ((await response.json()) as Envelope<T>).data;
  }
  context(request: ContextRequest) { return this.request("POST", "context:query", request); }
  assembleContext(request: ContextRequest) { return this.request("POST", "context:assemble", request); }
  createTaskPlan(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "task-plans", request, key);
  }
  initializeGitRepository(project: string, key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/git:initialize`, {}, key);
  }
  gitRepository(project: string) {
    return this.request("GET", `projects/${encodeURIComponent(project)}/git`);
  }
  createGitWorkspace(project: string, agent: string, key?: string) {
    return this.request("POST", "git-workspaces", {project_id: project, agent_id: agent}, key);
  }
  gitWorkspaceAction(id: string, action: "commit" | "push" | "reconcile" | "fetch",
                     request: Record<string, unknown>, key?: string) {
    return this.request("POST", `git-workspaces/${encodeURIComponent(id)}:${action}`, request, key);
  }
  submitMergeRequest(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "merge-requests", request, key);
  }
  mergeRequestAction(id: string, action: "review" | "check" | "merge" | "reconcile",
                     request: Record<string, unknown>, key?: string) {
    return this.request("POST", `merge-requests/${encodeURIComponent(id)}:${action}`, request, key);
  }
  taskPlan(id: string) { return this.request("GET", `task-plans/${encodeURIComponent(id)}`); }
  handoffTask(id: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `tasks/${encodeURIComponent(id)}:handoff`, request, key);
  }
  reviewAutomationTick(project: string, limit = 20, key?: string) {
    return this.request("POST", "review-automation:tick", {project_id: project, limit}, key);
  }
  beginCorrection(id: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `verifications/${encodeURIComponent(id)}:begin-correction`, request, key);
  }
  dispatchTask(project: string, agent: string, run: string, key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/tasks:dispatch`,
      {agent_id: agent, run_id: run}, key);
  }
  reportDispatchOutcome(id: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `tasks/${encodeURIComponent(id)}:dispatch-outcome`, request, key);
  }
  setCapabilities(agent: string, capabilities: string[], key?: string) {
    return this.request("POST", `agents/${encodeURIComponent(agent)}/capabilities`, {capabilities}, key);
  }
  submitVerification(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "verifications", request, key);
  }
  verification(id: string) { return this.request("GET", `verifications/${encodeURIComponent(id)}`); }
  reviewVerification(id: string, action: "update" | "assign-reviewer" | "check" | "decide",
                     request: Record<string, unknown>, key?: string) {
    return this.request("POST", `verifications/${encodeURIComponent(id)}:${action}`, request, key);
  }
  conversationRequest(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "conversation-requests", request, key);
  }
  conversationAction(id: string, action: "respond" | "reconcile" | "cancel" | "escalate",
                     request: Record<string, unknown>, key?: string) {
    return this.request("POST", `conversation-requests/${encodeURIComponent(id)}:${action}`, request, key);
  }
  renewMessageLease(id: string, run: string, delivery: string, ttl = 30, key?: string) {
    return this.request("POST", `messages/${encodeURIComponent(id)}:renew`,
      {run_id: run, delivery_id: delivery, ttl_seconds: ttl}, key);
  }
  memoryPipeline(action: "enqueue" | "process" | "tick" | "freshness" | "retrieve",
                 request: Record<string, unknown>, key?: string) {
    return this.request("POST", `memory-pipeline:${action}`, request, key);
  }
  reconcileRuntime(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "runtime:reconcile", request, key);
  }
  task(id: string) { return this.request("GET", `tasks/${encodeURIComponent(id)}`); }
  startTask(id: string, run: string, revision: number, recover = false, key?: string) {
    return this.request("POST", `tasks/${encodeURIComponent(id)}:start`,
      {run_id: run, expected_revision: revision, recover}, key);
  }
  checkpointTask(id: string, checkpoint: TaskCheckpoint, key?: string) {
    return this.request("POST", `tasks/${encodeURIComponent(id)}:checkpoint`, checkpoint, key);
  }
  ledger(project: string) { return this.request("GET", `ledger?project_id=${encodeURIComponent(project)}`); }
  inbox(agent: string, project: string) {
    return this.request("GET", `agents/${agent}/inbox?project_id=${encodeURIComponent(project)}`);
  }
  boardPosts(board: string, options: TimelineOptions = {}) {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(options)) if (value !== undefined) params.set(key, String(value));
    return this.request<TimelinePage>("GET", `boards/${encodeURIComponent(board)}/posts?${params}`);
  }
  conversations(project: string, options: TimelineOptions = {}) {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(options)) if (key !== "agent_id" && value !== undefined) params.set(key, String(value));
    const path = options.agent_id ? `agents/${encodeURIComponent(options.agent_id)}/conversation`
      : `projects/${encodeURIComponent(project)}/conversations`;
    return this.request<TimelinePage>("GET", `${path}?${params}`);
  }
  claimMessage(id: string, run: string, key?: string) {
    return this.request<DeliveryReceipt>("POST", `messages/${encodeURIComponent(id)}:claim`, {run_id: run}, key);
  }
  ackMessage(id: string, run: string, delivery: string, key?: string) {
    return this.request<DeliveryReceipt>("POST", `messages/${encodeURIComponent(id)}:ack`,
      {run_id: run, delivery_id: delivery}, key);
  }
  failMessage(id: string, run: string, delivery: string,
    reason: "handler_failed" | "interrupted" | "temporary_unavailable" = "handler_failed", key?: string) {
    return this.request<DeliveryReceipt>("POST", `messages/${encodeURIComponent(id)}:fail`,
      {run_id: run, delivery_id: delivery, reason}, key);
  }
  requeueMessage(id: string, revision: number, key?: string) {
    return this.request("POST", `messages/${encodeURIComponent(id)}:requeue`, {expected_revision: revision}, key);
  }
  deliverySummary(project: string) {
    return this.request<DeliverySummary>("GET", `projects/${encodeURIComponent(project)}/delivery-summary`);
  }
  markConversationsRead(project: string, ids: string[], key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/conversations:read`, {message_ids: ids}, key);
  }
  memoryVersions(id: string) { return this.request("GET", `memories/${encodeURIComponent(id)}/versions`); }
  extractMemories(request: MemoryExtractRequest, key?: string) {
    return this.request<MemoryExtractionResult>("POST", "memories:extract", request, key);
  }
  notifications(agent: string, project: string) {
    return this.request<AgentNotification[]>("GET", `agents/${encodeURIComponent(agent)}/notifications?project_id=${encodeURIComponent(project)}`);
  }
  ackNotification(id: string, key?: string) {
    return this.request("POST", `notifications/${encodeURIComponent(id)}:ack`, {}, key);
  }
  memoryProposal(id: string) { return this.request("GET", `memory-proposals/${encodeURIComponent(id)}`); }
  memoryDispute(id: string) { return this.request("GET", `memory-disputes/${encodeURIComponent(id)}`); }
  proposeMemoryVersion(id: string, proposal: MemoryVersionProposal, key?: string) {
    return this.request("POST", `memories/${encodeURIComponent(id)}/versions`, proposal, key);
  }
  reviewMemory(id: string, review: MemoryReview, key?: string) {
    return this.request("POST", `memories/${encodeURIComponent(id)}:review`, review, key);
  }
  reviewMemoryProposal(id: string, review: {expected_revision: number; decision: "approve" | "reject"; reason: string}, key?: string) {
    return this.request("POST", `memory-proposals/${encodeURIComponent(id)}:review`, review, key);
  }
  reportMemoryDispute(dispute: {project_id: string; left_memory_id: string; right_memory_id: string;
    left_revision: number; right_revision: number; reason: string;
    evidence_refs: {kind: string; id: string; revision?: number}[]}, key?: string) {
    return this.request("POST", "memory-disputes", dispute, key);
  }
  reviewMemoryDispute(id: string, review: {expected_revision: number; left_expected_revision: number;
    right_expected_revision: number; decision: "confirm" | "dismiss" | "prefer_left" | "prefer_right"; reason: string}, key?: string) {
    return this.request("POST", `memory-disputes/${encodeURIComponent(id)}:review`, review, key);
  }
}
