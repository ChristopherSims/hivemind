import {test, expect, Page} from "@playwright/test";
import {spawn, ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";

// The workspace is exercised in an isolated vite dev server so it can be
// verified independently of the app shell. Every network call is stubbed
// with page.route; no real Hivemind store is touched.
const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
// 5210 is reserved for this harness (clear of 5197 thread pane, 5199
// timeline, 8766 python fixture); cleaned up in afterAll.
const HARNESS_PORT = 5210;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/task-run-workspace.html`;
let harness: ChildProcess | null = null;

async function waitForHarness() {
  for (let attempt = 0; attempt < 80; attempt++) {
    try {
      const response = await fetch(HARNESS);
      if (response.ok) return;
    } catch {
      // The dev server is still starting.
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Harness server did not start at ${HARNESS}`);
}

type MockTask = {
  id: string; revision: number; title: string; status: string;
  created_at: string; priority: number; max_attempts: number;
  required_capabilities: string[]; plan_id: string | null; plan_key: string | null;
  plan_order: number; depends_on: string[]; external_dependency_ids: string[];
  scheduling: Record<string, unknown>; execution: Record<string, unknown> | null;
  execution_history_count: number; latest_execution: Record<string, unknown> | null;
  blocked?: Record<string, unknown> | null;
};

function scheduling(state = "queued", extra: Record<string, unknown> = {}) {
  return {state, attempts: 0, dispatches: 0, reassignments: 0, assigned_agent_id: null,
    escalated: false, escalation: null, retry_after: null, last_outcome: null,
    last_reconcile: null, attempt_history_count: 0, ...extra};
}

function mockTask(id: string, title: string, status: string, extra: Partial<MockTask> = {}): MockTask {
  return {
    id, revision: 3, title, status, created_at: "2026-01-01T00:00:00Z",
    priority: 0, max_attempts: 3, required_capabilities: [], plan_id: null,
    plan_key: null, plan_order: 0, depends_on: [], external_dependency_ids: [],
    scheduling: scheduling(), execution: null, execution_history_count: 0,
    latest_execution: null, ...extra,
  };
}

function fixtureTasks(): MockTask[] {
  return [
    mockTask("task-run-1", "Execute the migration", "running", {
      plan_id: "plan-1", plan_key: "migrate", priority: 10,
      scheduling: scheduling("dispatched", {assigned_agent_id: "agent-droid0001", attempts: 1}),
      execution: {id: "exec-1", run_id: "run-1", agent_id: "agent-droid0001",
        started_at: 1767225600, progress: 40, progress_basis: "agent_self_report"},
    }),
    mockTask("task-blocked-1", "Provision credentials", "queued", {
      depends_on: ["task-run-1"],
      blocked: {readiness: "blocked", readiness_scope: "task_only", blocker_count: 1,
        blockers: [{code: "dependency_incomplete", action: "wait", source: "scheduling"}],
        suggested_actions: ["wait"], authority: "explanation_snapshot_not_execution_permission"},
    }),
    mockTask("task-done-1", "Draft the plan", "completed", {
      execution_history_count: 3,
      latest_execution: {id: "exec-0", run_id: "run-0", agent_id: "agent-pi000002",
        started_at: 1767220000, progress: 100, progress_basis: "agent_self_report"},
    }),
    mockTask("task-failed-1", "Run the verifier", "failed", {
      scheduling: scheduling("queued", {attempts: 3, escalated: true,
        escalation: {reason: "review_requested", detail: "attempt budget exhausted"}}),
    }),
  ];
}

function workspacePage(tasks: MockTask[], project = "proj-1", extra: Record<string, unknown> = {}) {
  const byStatus: Record<string, number> = {};
  for (const task of tasks) byStatus[task.status] = (byStatus[task.status] ?? 0) + 1;
  return {
    project_id: project, snapshot_cursor: "a".repeat(64),
    tasks, task_count: tasks.length, tasks_has_more: false, next_offset: null,
    counts: {total: tasks.length, by_status: byStatus, truncated: false},
    plans: [{id: "plan-1", revision: 1, title: "Release plan",
      task_ids: ["task-run-1", "task-blocked-1"], task_count: 2}],
    plans_truncated: false, blocked_requests: null,
    ...extra,
  };
}

type MockControls = {
  failWorkspace: number;
  failFirstMutation: boolean;
  deferFirstMutation: boolean;
  failHistoryMore: boolean;
};

type MockHandles = {
  workspaceCalls: () => string[];
  mutations: () => {path: string; body: Record<string, unknown>; key: string | null}[];
  detailReads: () => number;
  historyReads: () => string[];
  releaseMutation: () => void;
};

async function installMocks(page: Page, options: {
  tasks?: MockTask[];
  blockedRequests?: Record<string, unknown>[];
  controls?: Partial<MockControls>;
} = {}): Promise<MockHandles> {
  const tasks = options.tasks ?? fixtureTasks();
  const controls: MockControls = {
    failWorkspace: 0, failFirstMutation: false, deferFirstMutation: false,
    failHistoryMore: false, ...options.controls,
  };
  const workspaceCalls: string[] = [];
  const mutations: {path: string; body: Record<string, unknown>; key: string | null}[] = [];
  const historyReads: string[] = [];
  let detailReads = 0;
  let mutationGate: Promise<void> | null = null;
  let releaseGate: (() => void) | null = null;
  await page.route("**/v1/streams**", route => route.abort());
  await page.route("**/v1/projects/*/task-workspace**", async route => {
    const url = route.request().url();
    workspaceCalls.push(url);
    if (controls.failWorkspace) {
      await route.fulfill({status: controls.failWorkspace,
        json: {error: {message: "forbidden", code: "forbidden"}}});
      return;
    }
    const project = url.includes("proj-2") ? "proj-2" : "proj-1";
    const visible = project === "proj-2"
      ? [mockTask("task-other-1", "Other project task", "queued")]
      : tasks;
    await route.fulfill({json: {data: workspacePage(visible, project, {
      blocked_requests: options.blockedRequests ?? null})}});
  });
  await page.route("**/v1/tasks/*/workspace-history**", async route => {
    const url = new URL(route.request().url());
    historyReads.push(url.toString());
    const offset = Number(url.searchParams.get("offset") ?? "0");
    if (controls.failHistoryMore && offset > 0) {
      await route.fulfill({status: 409,
        json: {error: {message: "snapshot_changed", code: "snapshot_changed"}}});
      return;
    }
    const entries = [
      {kind: "execution", id: "exec-2", outcome: "failed", progress: 80, started_at: 1767230000},
      {kind: "attempt", outcome: "failed", summary: "Verifier timed out", at: "2026-01-02T00:00:00Z"},
      {kind: "execution", id: "exec-1", outcome: "failed", progress: 40, started_at: 1767225000},
    ];
    const slice = entries.slice(offset, offset + 2);
    await route.fulfill({json: {data: {
      task_id: "task-done-1", revision: 3, entries: slice, count: slice.length,
      total: entries.length, has_more: offset + 2 < entries.length,
      next_offset: offset + 2 < entries.length ? offset + 2 : null,
      snapshot_cursor: "b".repeat(64),
      basis: "agent_self_reports_and_operator_actions",
    }}});
  });
  await page.route("**/v1/tasks/*", async route => {
    const url = route.request().url();
    if (url.includes(":reconcile") || url.includes(":handoff")) {
      if (controls.deferFirstMutation && mutations.length === 0) {
        mutationGate = new Promise<void>(resolve => { releaseGate = resolve; });
        await mutationGate;
      }
      if (controls.failFirstMutation && mutations.length === 0) {
        mutations.push({path: url, body: route.request().postDataJSON(),
          key: route.request().headers()["idempotency-key"] ?? null});
        await route.fulfill({status: 412,
          json: {error: {message: "revision_conflict", code: "revision_conflict"}}});
        return;
      }
      mutations.push({path: url, body: route.request().postDataJSON(),
        key: route.request().headers()["idempotency-key"] ?? null});
      await route.fulfill({json: {data: tasks[0]}});
      return;
    }
    detailReads += 1;
    const id = url.split("/v1/tasks/")[1];
    const task = tasks.find(item => item.id === id) ?? tasks[0];
    await route.fulfill({json: {data: {...task, description: "Detail body"}}});
  });
  await page.route("**/v1/blocked-task-requests/*", async route => {
    const url = route.request().url();
    if (url.includes(":acknowledge") || url.includes(":resolve")) {
      mutations.push({path: url, body: route.request().postDataJSON(),
        key: route.request().headers()["idempotency-key"] ?? null});
    }
    await route.fulfill({json: {data: {id: "req-1", state: "acknowledged"}}});
  });
  return {
    workspaceCalls: () => workspaceCalls,
    mutations: () => mutations,
    detailReads: () => detailReads,
    historyReads: () => historyReads,
    releaseMutation: () => releaseGate?.(),
  };
}

test.describe("task run workspace (isolated harness)", () => {
  test.beforeAll(async () => {
    harness = spawn(process.execPath,
      ["node_modules/vite/bin/vite.js", "--port", String(HARNESS_PORT), "--strictPort",
        "--host", "127.0.0.1"],
      {cwd: DASHBOARD, stdio: "ignore"});
    await waitForHarness();
  });

  test.afterAll(() => {
    harness?.kill();
    harness = null;
  });

  test("renders assignments, dependencies, blockers, and bounded counts", async ({page}) => {
    await installMocks(page);
    await page.goto(HARNESS);
    const workspace = page.getByTestId("task-run-workspace");
    await expect(workspace).toBeVisible();
    await expect(page.getByTestId("trw-counts")).toContainText("Total");
    await expect(page.getByTestId("trw-counts")).toContainText("4");
    await expect(page.getByTestId("trw-task-task-run-1")).toContainText("Execute the migration");
    await expect(page.getByTestId("trw-task-task-run-1")).toContainText("self-reported");
    await expect(page.getByTestId("trw-task-task-run-1")).toContainText("Droid worker");
    await expect(page.getByTestId("trw-task-task-run-1")).toContainText("Release plan");
    await expect(page.getByTestId("trw-deps-task-blocked-1"))
      .toContainText("Blocked by 1 visible dependency");
    await expect(page.getByTestId("trw-blocked-task-blocked-1")).toContainText("1 blocker");
    await expect(page.getByTestId("trw-task-task-failed-1")).toContainText("Escalated");
  });

  test("selection loads fenced detail and paged history bound to the snapshot", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-done-1").click();
    const detail = page.getByTestId("trw-detail");
    await expect(detail).toBeVisible();
    await expect(detail).toContainText("Draft the plan");
    const history = page.getByTestId("trw-history");
    await expect(history).toContainText("self-reports");
    await expect(history.locator("li")).toHaveCount(2);
    // The first history read is bound to the page's snapshot cursor.
    expect(mocks.historyReads()[0]).toContain(`snapshot_cursor=${"a".repeat(64)}`);
    await page.getByTestId("trw-history-more").click();
    await expect(history.locator("li")).toHaveCount(3);
    // Follow-up pages stay bound to the cursor returned with the first page.
    expect(mocks.historyReads()[1]).toContain(`snapshot_cursor=${"b".repeat(64)}`);
    await page.getByTestId("trw-task-task-done-1").click();
    await expect(page.getByTestId("trw-detail")).toHaveCount(0);
  });

  test("parent-driven refreshVersion re-reads; no stream is opened", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await expect(page.getByTestId("trw-task-list")).toBeVisible();
    expect(mocks.workspaceCalls().length).toBe(1);
    await page.getByText("Push refresh").click();
    await expect.poll(() => mocks.workspaceCalls().length).toBe(2);
    expect(mocks.workspaceCalls()[1]).toContain("proj-1");
  });

  test("refresh clears then re-reads the still-visible selection", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-done-1").click();
    await expect(page.getByTestId("trw-detail")).toBeVisible();
    await expect(page.getByTestId("trw-history").locator("li")).toHaveCount(2);
    const detailsBefore = mocks.detailReads();
    const historiesBefore = mocks.historyReads().length;
    await page.getByText("Push refresh").click();
    // The selection survives (task still visible) but detail and history are
    // re-read from scratch, never kept from the previous snapshot.
    await expect.poll(() => mocks.detailReads()).toBe(detailsBefore + 1);
    await expect.poll(() => mocks.historyReads().length).toBe(historiesBefore + 1);
    await expect(page.getByTestId("trw-detail")).toContainText("Draft the plan");
    await expect(page.getByTestId("trw-history").locator("li")).toHaveCount(2);
  });

  test("project and permission switches clear protected evidence and reload", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-done-1").click();
    await expect(page.getByTestId("trw-detail")).toBeVisible();
    await page.getByText("Switch project").click();
    await expect(page.getByTestId("trw-task-task-other-1")).toBeVisible();
    // The previous project's selection and detail are gone.
    await expect(page.getByTestId("trw-detail")).toHaveCount(0);
    await expect(page.getByTestId("trw-task-task-done-1")).toHaveCount(0);
    await page.getByText("Rotate permissions").click();
    await expect.poll(() => mocks.workspaceCalls().length).toBe(3);
  });

  test("authorization failure clears evidence and reports via onError", async ({page}) => {
    await installMocks(page, {controls: {failWorkspace: 403}});
    await page.goto(HARNESS);
    await expect(page.getByRole("alert"))
      .toContainText("not authorized to view this task workspace");
    await expect(page.getByTestId("errors")).toContainText("not authorized");
    await expect(page.getByTestId("trw-task-list")).toHaveCount(0);
  });

  test("history paging failure clears protected history and detail", async ({page}) => {
    await installMocks(page, {controls: {failHistoryMore: true}});
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-done-1").click();
    await expect(page.getByTestId("trw-history").locator("li")).toHaveCount(2);
    await page.getByTestId("trw-history-more").click();
    // The 409 snapshot drift drops the selection and all protected content.
    await expect(page.getByTestId("trw-detail")).toHaveCount(0);
    await expect(page.getByRole("alert")).toContainText("snapshot");
  });

  test("reconcile sends one frozen revision-fenced command with a stable key", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-failed-1").click();
    const actions = page.getByTestId("trw-actions");
    await expect(actions).toBeVisible();
    await actions.getByText("Requeue").click();
    const dialog = page.getByTestId("trw-confirm-dialog");
    await expect(dialog).toContainText("revision 3");
    await page.getByTestId("trw-action-reason").fill("Worker host lost power");
    await page.getByTestId("trw-action-confirm").click();
    await expect(dialog).toHaveCount(0);
    expect(mocks.mutations().length).toBe(1);
    const sent = mocks.mutations()[0];
    expect(sent.path).toContain("/v1/tasks/task-failed-1:reconcile");
    expect(sent.body).toEqual({expected_revision: 3, action: "requeue",
      reason: "Worker host lost power"});
    expect(sent.key).toBeTruthy();
  });

  test("a second action works after a successful one (busy never strands)", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-failed-1").click();
    await page.getByTestId("trw-actions").getByText("Requeue").click();
    await page.getByTestId("trw-action-reason").fill("First recovery");
    await page.getByTestId("trw-action-confirm").click();
    await expect(page.getByTestId("trw-confirm-dialog")).toHaveCount(0);
    await expect.poll(() => mocks.mutations().length).toBe(1);
    // The reload/re-read after success must leave actions usable.
    const actions = page.getByTestId("trw-actions");
    await expect(actions).toBeVisible();
    await actions.getByText("Escalate").click();
    await page.getByTestId("trw-action-reason").fill("Second recovery");
    await page.getByTestId("trw-action-confirm").click();
    await expect(page.getByTestId("trw-confirm-dialog")).toHaveCount(0);
    await expect.poll(() => mocks.mutations().length).toBe(2);
    expect(mocks.mutations()[1].body).toEqual({expected_revision: 3, action: "escalate",
      reason: "Second recovery"});
  });

  test("a parent refresh while an action is in flight does not strand the write", async ({page}) => {
    const mocks = await installMocks(page, {controls: {deferFirstMutation: true}});
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-failed-1").click();
    await page.getByTestId("trw-actions").getByText("Requeue").click();
    await page.getByTestId("trw-action-reason").fill("In-flight recovery");
    await page.getByTestId("trw-action-confirm").click();
    await expect(page.getByTestId("trw-action-confirm")).toContainText("Working…");
    // The parent SSE bump reloads the page mid-write (dispatched directly:
    // the modal dialog overlays the shell button).
    await page.getByText("Push refresh").dispatchEvent("click");
    mocks.releaseMutation();
    await expect(page.getByTestId("trw-confirm-dialog")).toHaveCount(0);
    await expect.poll(() => mocks.mutations().length).toBe(1);
    // The workspace is usable immediately afterwards; the selection was
    // re-read after the write, so the detail actions are already visible.
    const actions = page.getByTestId("trw-actions");
    await expect(actions).toBeVisible();
    await actions.getByText("Escalate").click();
    await page.getByTestId("trw-action-reason").fill("Post-refresh action");
    await page.getByTestId("trw-action-confirm").click();
    await expect.poll(() => mocks.mutations().length).toBe(2);
  });

  test("a failed action replays the exact frozen payload and key on retry", async ({page}) => {
    const mocks = await installMocks(page, {controls: {failFirstMutation: true}});
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-failed-1").click();
    await page.getByTestId("trw-actions").getByText("Requeue").click();
    await page.getByTestId("trw-action-reason").fill("Original reason");
    await page.getByTestId("trw-action-confirm").click();
    const dialog = page.getByTestId("trw-confirm-dialog");
    await expect(dialog).toContainText("revision_conflict");
    await expect(page.getByTestId("trw-frozen-note")).toBeVisible();
    // Fields are locked: no changed command can be produced for this key.
    await expect(page.getByTestId("trw-action-reason")).toBeDisabled();
    await expect(page.getByTestId("trw-action-confirm")).toContainText("Retry exact command");
    await page.getByTestId("trw-action-confirm").click();
    await expect(dialog).toHaveCount(0);
    await expect.poll(() => mocks.mutations().length).toBe(2);
    const [first, second] = mocks.mutations();
    expect(first.body).toEqual({expected_revision: 3, action: "requeue",
      reason: "Original reason"});
    expect(second.body).toEqual(first.body);
    expect(second.key).toBe(first.key);
  });

  test("handoff freezes the chosen target agent into the command", async ({page}) => {
    const mocks = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByTestId("trw-task-task-run-1").click();
    await page.getByTestId("trw-actions").getByText("Hand off…").click();
    await page.getByTestId("trw-handoff-target").selectOption("agent-pi000002");
    await page.getByTestId("trw-action-confirm").click();
    const sent = mocks.mutations()[0];
    expect(sent.path).toContain("/v1/tasks/task-run-1:handoff");
    expect(sent.body).toMatchObject({expected_revision: 3,
      target_agent_id: "agent-pi000002"});
  });

  test("non-operator roles see no mutation affordances", async ({page}) => {
    await installMocks(page);
    await page.goto(`${HARNESS}?role=agent`);
    await page.getByTestId("trw-task-task-failed-1").click();
    await expect(page.getByTestId("trw-detail")).toBeVisible();
    await expect(page.getByTestId("trw-actions")).toHaveCount(0);
  });

  test("blocked-task requests can be acknowledged through the canonical route", async ({page}) => {
    const mocks = await installMocks(page, {blockedRequests: [
      {id: "req-1", task_id: "task-blocked-1", state: "open", action: "retry_with_credentials"},
    ]});
    await page.goto(HARNESS);
    const requests = page.getByTestId("trw-requests");
    await expect(requests).toContainText("Provision credentials");
    await page.getByTestId("trw-ack-req-1").click();
    await expect.poll(() => mocks.mutations().length).toBe(1);
    expect(mocks.mutations()[0].path).toContain("/v1/blocked-task-requests/req-1:acknowledge");
  });
});
