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
export type ActivityFrame = {id?: string; event: "snapshot" | "activity" | "resync" | "shutdown";
  data: Record<string, unknown>};
export type OperationDescription = {
  id: string; method: string; path: string; summary: string; description: string;
  context: string; authority: string;
  inputs: {path_parameters: string[]; query_parameters: string[];
    body_schema: string | null; schema_document: string};
  outputs: string; side_effects: string; retry_safety: string; limits: string; untrusted_data: string;
};
export type OperationContext = {schema_version: number; digest: string;
  operations: OperationDescription[]; feature_descriptions: Record<string, unknown>;
  runtime_descriptions: Record<string, unknown>; rules: string[]};
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

export type AccessRules = {public_read?: boolean; public_contribute?: boolean;
  reader_agent_ids?: string[]; contributor_agent_ids?: string[];
  reader_team_ids?: string[]; contributor_team_ids?: string[]};
export type TeamCreate = {project_id: string; display_name: string; member_agent_ids?: string[]};
export type MemoryProvider = "mem0" | "langmem" | "letta";
export type ProviderImport = {result_id: string; item_index: number; content_sha256: string; title: string;
  approve_import: true; evidence_refs: {kind: string; id: string; revision?: number}[]};

export class Hivemind {
  constructor(private baseURL: string, private token: string) {}
  retrieveProviderMemories(projectId: string, request: {adapter: MemoryProvider; query?: string; limit?: number}, key: string) {
    return this.request("POST", `projects/${encodeURIComponent(projectId)}/memory-providers:retrieve`, request, key);
  }
  providerMemoryResult(resultId: string) {
    return this.request("GET", `memory-provider-results/${encodeURIComponent(resultId)}`);
  }
  importProviderMemory(projectId: string, request: ProviderImport, key: string) {
    return this.request("POST", `projects/${encodeURIComponent(projectId)}/memory-providers:import`, request, key);
  }
  reconcileProviderMemories(projectId: string, adapter: MemoryProvider) {
    return this.request("POST", `projects/${encodeURIComponent(projectId)}/memory-providers:reconcile`, {adapter});
  }
  providerMemoryStatus(projectId: string) {
    return this.request("GET", `projects/${encodeURIComponent(projectId)}/memory-providers:status`);
  }
  createTeam(request: TeamCreate, key: string) {
    return this.request("POST", "teams", request, key);
  }
  teams(projectId: string) {
    return this.request("GET", `projects/${encodeURIComponent(projectId)}/teams`);
  }
  updateTeam(teamId: string, request: TeamCreate & {expected_revision: number; active?: boolean}, key: string) {
    return this.request("PATCH", `teams/${encodeURIComponent(teamId)}`, request, key);
  }
  createBoard(request: {project_id: string; title: string; access?: AccessRules}, key: string) {
    return this.request("POST", "boards", request, key);
  }
  resourceAccess(resourceId: string) {
    return this.request("GET", `resources/${encodeURIComponent(resourceId)}/access`);
  }
  setResourceAccess(resourceId: string, request: AccessRules & {expected_revision?: number | null}, key: string) {
    return this.request("PUT", `resources/${encodeURIComponent(resourceId)}/access`, request, key);
  }
  createNexusGrant(request: {project_id: string; nexus_agent_id: string;
    target_agent_ids: string[]; actions: ("steer" | "pause" | "resume" | "cancel" | "assign_task" | "monitor")[];
    ttl_seconds?: number}, key: string) {
    return this.request("POST", "nexus-grants", request, key);
  }
  nexusGrants(projectId: string) {
    return this.request("GET", `projects/${encodeURIComponent(projectId)}/nexus-grants`);
  }
  revokeNexusGrant(grantId: string, expectedRevision: number, key: string) {
    return this.request("POST", `nexus-grants/${encodeURIComponent(grantId)}:revoke`,
      {expected_revision: expectedRevision}, key);
  }
  mirrorMemory(memoryId: string, adapter: "mem0" | "langmem" | "letta", expectedRevision: number, key: string) {
    return this.request("POST", `memories/${encodeURIComponent(memoryId)}:mirror`,
      {adapter, expected_revision: expectedRevision}, key);
  }
  createWorker(projectId: string, request: Record<string, unknown>, key: string) {
    return this.request("POST", `projects/${encodeURIComponent(projectId)}/workers`, request, key);
  }
  workerDefaults(projectId: string) {
    return this.request("GET", `projects/${encodeURIComponent(projectId)}/workers:defaults`);
  }
  workerStatus(workerId: string) {
    return this.request("GET", `workers/${encodeURIComponent(workerId)}`);
  }
  startWorker(workerId: string, request: Record<string, unknown>) {
    return this.request("POST", `workers/${encodeURIComponent(workerId)}:start`, request);
  }
  stopWorker(workerId: string, request: Record<string, unknown>) {
    return this.request("POST", `workers/${encodeURIComponent(workerId)}:stop`, request);
  }
  /** A readiness snapshot is not an execution permit. */
  blockedTaskStatus(projectId: string, taskId: string, runId?: string, agentId?: string) {
    const query = new URLSearchParams();
    if (runId !== undefined) query.set("run_id", runId);
    if (agentId !== undefined) query.set("agent_id", agentId);
    return this.request("GET", `projects/${encodeURIComponent(projectId)}/tasks/${
      encodeURIComponent(taskId)}/blocked-status${query.size ? `?${query}` : ""}`);
  }
  requestTaskResolution(projectId: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(projectId)}/blocked-tasks:request`,
      request, key);
  }
  taskResolutionRequests(projectId: string, taskId?: string) {
    return this.request("GET", `projects/${encodeURIComponent(projectId)}/blocked-task-requests${
      taskId === undefined ? "" : `?task_id=${encodeURIComponent(taskId)}`}`);
  }
  taskResolutionRequest(requestId: string) {
    return this.request("GET", `blocked-task-requests/${encodeURIComponent(requestId)}`);
  }
  acknowledgeTaskResolution(requestId: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `blocked-task-requests/${encodeURIComponent(requestId)}:acknowledge`,
      request, key);
  }
  closeTaskResolution(requestId: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `blocked-task-requests/${encodeURIComponent(requestId)}:resolve`,
      request, key);
  }
  triggerMemoryEvents(projectId: string, bindingKey?: string, key?: string) {
    return this.request("POST", "memory-events:trigger",
      {project_id: projectId, binding_key: bindingKey}, key);
  }
  memoryEventStatus(projectId: string) {
    return this.request("GET", `memory-events:status?project_id=${encodeURIComponent(projectId)}`);
  }
  memoryEventReviews(projectId: string) {
    return this.request("GET", `memory-events:reviews?project_id=${encodeURIComponent(projectId)}`);
  }
  /** Skip a held intent; this does not permit uncertain computation replay. */
  releaseMemoryEventHold(projectId: string, fingerprint?: string, key?: string) {
    return this.request("POST", "memory-events:release", {project_id: projectId, fingerprint}, key);
  }
  /** Bounded, cancellable authorized SSE. Persist yielded ids to resume with `after`. */
  async *subscribeAgentActivity(projectId: string, options: {
    after?: string; signal?: AbortSignal; readTimeoutMs?: number
  } = {}): AsyncGenerator<ActivityFrame> {
    const readTimeout = options.readTimeoutMs ?? 10000;
    if (!Number.isFinite(readTimeout) || readTimeout < 1000 || readTimeout > 120000)
      throw new Error("Invalid activity read timeout");
    const controller = new AbortController();
    const abort = () => controller.abort();
    options.signal?.addEventListener("abort", abort, {once: true});
    if (options.signal?.aborted) abort();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const query = options.after ? `?after=${encodeURIComponent(options.after)}` : "";
      timer = setTimeout(abort, readTimeout);
      const response = await fetch(`${this.baseURL.replace(/\/$/, "")}/v1/projects/${
        encodeURIComponent(projectId)}/agents:stream${query}`, {
        headers: {Authorization: `Bearer ${this.token}`, Accept: "text/event-stream"},
        signal: controller.signal, redirect: "error"});
      clearTimeout(timer);
      if (!response.ok || !response.headers.get("content-type")?.startsWith("text/event-stream")
          || !response.body) throw new Error("Activity subscription rejected");
      reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8", {fatal: true}), encoder = new TextEncoder();
      let buffer = "", event = "", id: string | undefined, data: string[] = [], bytes = 0, frames = 0;
      while (!controller.signal.aborted) {
        timer = setTimeout(abort, readTimeout);
        const item = await reader.read();
        clearTimeout(timer);
        if (item.done) return;
        for (let offset = 0; offset < item.value.length; offset += 8192) {
          buffer += decoder.decode(item.value.subarray(offset, offset + 8192), {stream: true});
          let end: number;
          while ((end = buffer.indexOf("\n")) >= 0) {
            const line = buffer.slice(0, end).replace(/\r$/, "");
            buffer = buffer.slice(end + 1);
            bytes += encoder.encode(line).length + 1;
            if (bytes > 16384) throw new Error("Activity frame exceeds limit");
            if (!line) {
              if (data.length) {
                if (!["snapshot", "activity", "resync", "shutdown"].includes(event) || ++frames > 10000)
                  throw new Error("Invalid activity frame");
                const payload: unknown = JSON.parse(data.join("\n"));
                if (!payload || typeof payload !== "object" || Array.isArray(payload))
                  throw new Error("Invalid activity frame");
                if (!Object.keys(payload).length || (["snapshot", "activity"].includes(event)
                    && (payload as Record<string, unknown>).type !== event))
                  throw new Error("Invalid activity frame");
                if (id && !/^(g\d+|s\d+(\.a\d+)?)$/.test(id)) throw new Error("Invalid activity cursor");
                yield {id, event: event as ActivityFrame["event"], data: payload as Record<string, unknown>};
              }
              event = ""; id = undefined; data = []; bytes = 0;
            } else if (line.startsWith("event:")) event = line.slice(6).trimStart();
            else if (line.startsWith("id:")) id = line.slice(3).trimStart();
            else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
          }
          if (encoder.encode(buffer).length + bytes > 16384) throw new Error("Activity frame exceeds limit");
        }
      }
    } catch {
      if (!options.signal?.aborted) throw new Error("Activity subscription interrupted");
    } finally {
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", abort);
      controller.abort();
      if (reader) { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
    }
  }
  async request<T>(method: string, path: string, body?: unknown, key?: string): Promise<T> {
    const response = await fetch(`${this.baseURL.replace(/\/$/, "")}/v1/${path}`, {
      method,
      headers: {Authorization: `Bearer ${this.token}`, "Content-Type": "application/json",
        ...(method !== "GET" ? {"Idempotency-Key": key ?? crypto.randomUUID()} : {})},
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) throw new Error(`Hivemind request rejected (${response.status})`);
    return ((await response.json()) as Envelope<T>).data;
  }
  context(request: ContextRequest) { return this.request("POST", "context:query", request); }
  /** Describe purpose/schema/authority/side effects/retry rules. Context is not a permission grant. */
  operationContext(operationId?: string) {
    const query = operationId === undefined ? "" : `?operation_id=${encodeURIComponent(operationId)}`;
    return this.request<OperationContext>("GET", `tool-context${query}`);
  }
  assembleContext(request: ContextRequest) { return this.request("POST", "context:assemble", request); }
  searchPage(request: Record<string, unknown>, cursor?: string) {
    const body = cursor === undefined ? {...request} : {...request, cursor};
    return this.request("POST", "retrieval:search-page", body);
  }
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
  createGoal(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "goals", request, key);
  }
  goal(id: string) { return this.request("GET", `goals/${encodeURIComponent(id)}`); }
  reviseGoal(id: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `goals/${encodeURIComponent(id)}:revise`, request, key);
  }
  tickGoal(id: string, request: Record<string, unknown> = {}, key?: string) {
    return this.request("POST", `goals/${encodeURIComponent(id)}:tick`, request, key);
  }
  proposeGoalPlan(id: string, request: Record<string, unknown> = {}, key?: string) {
    return this.request("POST", `goals/${encodeURIComponent(id)}:propose`, request, key);
  }
  handoffTask(id: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `tasks/${encodeURIComponent(id)}:handoff`, request, key);
  }
  offerStructuredHandoff(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "structured-handoffs", request, key);
  }
  structuredHandoff(id: string) {
    return this.request("GET", `structured-handoffs/${encodeURIComponent(id)}`);
  }
  decideStructuredHandoff(id: string, action: "accept" | "reject" | "revoke",
      request: Record<string, unknown>, key?: string) {
    return this.request("POST", `structured-handoffs/${encodeURIComponent(id)}:${action}`, request, key);
  }
  consumeHandoffContext(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "structured-handoffs:consume", request, key);
  }
  setRoutingDeclaration(id: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `agents/${encodeURIComponent(id)}/routing-declaration`, request, key);
  }
  queryRouting(project: string, request: Record<string, unknown>) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/routing:query`, request);
  }
  approveRouting(project: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/routing:dispatch`, request, key);
  }
  createArtifactContract(request: Record<string, unknown>, key?: string) {
    return this.request("POST", "artifact-contracts", request, key);
  }
  artifactContract(id: string) {
    return this.request("GET", `artifact-contracts/${encodeURIComponent(id)}`);
  }
  artifactContractAction(id: string, action: "revise" | "revoke" | "submit",
      request: Record<string, unknown>, key?: string) {
    return this.request("POST", `artifact-contracts/${encodeURIComponent(id)}:${action}`, request, key);
  }
  setTaskBudget(project: string, request: Record<string, unknown>, key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/budgets`, request, key);
  }
  taskBudget(project: string, task: string) {
    return this.request("GET", `projects/${encodeURIComponent(project)}/budgets/${encodeURIComponent(task)}`);
  }
  taskBudgetAction(project: string, action: "reserve" | "settle" | "checkpoint",
      request: Record<string, unknown>, key?: string) {
    return this.request("POST", `projects/${encodeURIComponent(project)}/budgets:${action}`, request, key);
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
