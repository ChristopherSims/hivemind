import {test, expect, type Page} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";
import {
  controlEligibility, controlPhase, controlPhaseLabel, overlapsByClaim, pageSlice,
  prepareControlCommand, prepareHoldResolution, redactionNote, reportedStatusLabel,
  CONSOLE_PAGE_SIZE,
  type ConsoleSnapshot, type ConsoleControl, type ConsoleRun, type ConsoleAgent,
} from "../src/conflict-control-console";

/*
 * Conflict/control console.
 *
 * Part 1 covers the pure contracts in-process (no network, no harness). Part 2
 * mounts the component in an isolated vite dev server on reserved port 5211,
 * clear of the other harnesses (5197-5205) and the python fixture (8766).
 * Every request is stubbed by an in-spec fake server that mirrors the
 * canonical semantics of hivemind/conflict_control_console.py and
 * service.control; no real Hivemind store is touched and this spec runs
 * before the dashboard owner wires the route into main.tsx.
 */

const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5211;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/conflict-control-console.html`;
const CURSOR = "c".repeat(64);
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

// ---------------------------------------------------------------------------
// Part 1: pure contracts
// ---------------------------------------------------------------------------

function run(overrides: Partial<ConsoleRun> = {}): ConsoleRun {
  return {id: "run-1", agent_id: "agent-1", status: "running", control_revision: 3,
    presence: "online", presence_is_acknowledgment: false, revision: 1, ...overrides};
}

function agent(overrides: Partial<ConsoleAgent> = {}): ConsoleAgent {
  return {id: "agent-1", display_name: "Worker", supported_actions: ["pause", "resume", "steer"],
    scope_permit_enforcement: true, ...overrides};
}

const LEASE = {id: "lease-1", agent_id: "nexus-1", coordinator_epoch: 7,
  expires_at: 4_000_000_000, state: "active"};

test.describe("conflict console contracts", () => {
  test("control phases never claim an applied outcome from acceptance", () => {
    expect(controlPhase({state: "queued"})).toBe("requested");
    expect(controlPhase({state: "acknowledged"})).toBe("delivered");
    expect(controlPhase({state: "applying"})).toBe("applying");
    expect(controlPhase({state: "applied"})).toBe("applied");
    expect(controlPhase({state: "failed"})).toBe("failed");
    expect(controlPhase({state: "outcome_unknown"})).toBe("outcome_unknown");
    expect(controlPhase({state: "expired"})).toBe("expired");
    expect(controlPhase({state: "superseded"})).toBe("superseded");
    expect(controlPhase({state: "something-else"})).toBe("unknown");
    const queued = controlPhaseLabel({state: "queued"} as ConsoleControl);
    expect(queued).toContain("Requested");
    expect(queued).not.toContain("paused");
    expect(queued).not.toContain("Applied");
  });

  test("reported status is attributed receipt data, never current truth", () => {
    const control = {state: "applied",
      result: {outcome: "applied", observed_status: "paused"}} as ConsoleControl;
    const label = reportedStatusLabel(control)!;
    expect(label).toContain("paused");
    expect(label).toContain("reported");
    expect(reportedStatusLabel({state: "queued"} as ConsoleControl)).toBeNull();
  });

  test("prepared commands freeze the payload and mint one immutable key", () => {
    const prepared = prepareControlCommand({
      projectId: "proj-1", targetRunId: "run-1", action: "pause",
      targetRunRevision: 3, coordinatorEpoch: 7, reason: "Review"});
    expect(Object.isFrozen(prepared.payload)).toBe(true);
    expect(prepared.payload.replacement_task_id).toBeNull();
    expect(prepared.idempotencyKey).toMatch(/^[0-9a-f-]{36}$/);
    const steer = prepareControlCommand({
      projectId: "proj-1", targetRunId: "run-1", action: "steer",
      replacementTaskId: "task-9", targetRunRevision: 3, coordinatorEpoch: 7,
      reason: "Redirect"});
    expect(steer.payload.replacement_task_id).toBe("task-9");
    // A resume must not silently carry a replacement task.
    const resume = prepareControlCommand({
      projectId: "proj-1", targetRunId: "run-1", action: "resume",
      replacementTaskId: "task-9", targetRunRevision: 3, coordinatorEpoch: 7,
      reason: "Continue"});
    expect(resume.payload.replacement_task_id).toBeNull();
  });

  test("eligibility explains the true boundary instead of faux-enabling", () => {
    const now = 1_000;
    const ok = controlEligibility({action: "pause", run: run(), agent: agent(),
      lease: {...LEASE, expires_at: now + 60}, nowSeconds: now});
    expect(ok.allowed).toBe(true);
    expect(controlEligibility({action: "pause", run: run(), agent: agent(),
      lease: null, nowSeconds: now}).reason).toContain("lease");
    expect(controlEligibility({action: "pause", run: run({status: "completed"}),
      agent: agent(), lease: LEASE, nowSeconds: now}).reason).toContain("completed");
    expect(controlEligibility({action: "cancel", run: run(), agent: agent(),
      lease: LEASE, nowSeconds: now}).reason).toContain("cancel");
    expect(controlEligibility({action: "pause", run: run(),
      agent: agent({scope_permit_enforcement: false}), lease: LEASE,
      nowSeconds: now}).reason).toContain("scope permits");
    expect(controlEligibility({action: "pause", run: run(), agent: agent(),
      lease: {...LEASE, expires_at: now - 1}, nowSeconds: now}).reason).toContain("expired");
    expect(controlEligibility({action: "steer", run: run(), agent: agent(),
      lease: LEASE, nowSeconds: now, steerTarget: null}).reason).toContain("queued");
    expect(controlEligibility({action: "steer", run: run(), agent: agent(),
      lease: LEASE, nowSeconds: now,
      steerTarget: {id: "t1", revision: 1}}).allowed).toBe(true);
    // A visible lease alone never confers issuance on a normal operator.
    const human = controlEligibility({action: "pause", run: run(), agent: agent(),
      lease: LEASE, nowSeconds: now,
      issuerHint: {lease_visible: true, advisory: true, principal_may_issue: false,
        explanation: "Controls are issued by the lease-holding coordinator agent."}});
    expect(human.allowed).toBe(false);
    expect(human.reason).toContain("lease-holding coordinator");
    const issuer = controlEligibility({action: "pause", run: run(), agent: agent(),
      lease: LEASE, nowSeconds: now,
      issuerHint: {lease_visible: true, advisory: true, principal_may_issue: true,
        explanation: ""}});
    expect(issuer.allowed).toBe(true);
  });

  test("hold resolutions freeze key and body together at preparation", () => {
    const prepared = prepareHoldResolution({
      actionId: "hold-1", expectedRevision: 2, resolution: "dismiss", reason: "Reviewed"});
    expect(Object.isFrozen(prepared.payload)).toBe(true);
    expect(prepared.payload).toEqual({expected_revision: 2, action: "dismiss",
      reason: "Reviewed"});
    expect(prepared.idempotencyKey).toMatch(/^[0-9a-f-]{36}$/);
    expect(prepared.actionId).toBe("hold-1");
  });

  test("overlap indexing and pagination never fabricate entries", () => {
    const map = overlapsByClaim([
      {kind: "scope_overlap", claim_ids: ["c1", "c2"], run_ids: ["r1", "r2"],
        modes: ["exclusive_owner"]},
    ]);
    expect(map.get("c1")).toHaveLength(1);
    expect(map.get("c2")).toHaveLength(1);
    expect(map.get("c3")).toBeUndefined();
    const items = Array.from({length: CONSOLE_PAGE_SIZE + 3}, (_, i) => i);
    expect(pageSlice(items, 1).page).toHaveLength(CONSOLE_PAGE_SIZE);
    expect(pageSlice(items, 2).page).toHaveLength(3);
    expect(pageSlice(items, 99).pages).toBe(2);
    expect(pageSlice([], 1).pages).toBe(1);
  });

  test("redaction notes count protected references without enumerating them", () => {
    expect(redactionNote({hidden_claim_scopes: 0, hidden_conflict_scopes: 0})).toBeNull();
    const note = redactionNote({hidden_claim_scopes: 2, hidden_control_fields: 1})!;
    expect(note).toContain("3");
    expect(note).not.toContain("scope-");
  });
});

// ---------------------------------------------------------------------------
// Part 2: component against a fake canonical server
// ---------------------------------------------------------------------------

type Fake = {
  snapshot: ConsoleSnapshot;
  controlRequests: {body: Record<string, unknown>; key: string | null}[];
  holdRequests: {path: string; body: Record<string, unknown>; key: string | null}[];
  failConsoleWith: number | null;
  failNextControl: boolean;
  gets: number;
};

function snapshot(overrides: Partial<ConsoleSnapshot> = {}): ConsoleSnapshot {
  return {
    project_id: "proj-1", generated_at: 1_000, snapshot_cursor: CURSOR,
    policy: {enabled: true},
    counts: {claims: 1, overlaps: 0, conflicts: 0, holds: 0, actions: 0, controls: 0,
      runs: 1, pending_controls: 0},
    claims: [{id: "c1", run_id: "run-1", task_id: null, mode: "exclusive_owner",
      state: "active", scope_ids: ["scope-1"], hidden_scope_count: 0,
      expires_at: 9_999, ownership_epoch: 4, revision: 1}],
    claim_overlaps: [], conflicts: [], holds: [], actions: [], controls: [],
    lease: LEASE,
    issuer_hint: {lease_visible: true, advisory: true, principal_may_issue: true,
      explanation: ""},
    runs: [run()], agents: [agent(), {id: "nexus-1", display_name: "Nexus",
      supported_actions: [], scope_permit_enforcement: false}],
    steer_targets: [{id: "task-1", title: "Queued task", revision: 1}],
    redactions: {hidden_claim_scopes: 0, hidden_conflict_scopes: 0, hidden_control_fields: 0,
      hidden_steer_targets: 0},
    truncated: {claims: false, overlaps: false, conflicts: false, actions: false,
      controls: false, runs: false, agents: false, steer_targets: false},
    limits: {claims: 200, overlaps: 200, conflicts: 200, actions: 100, controls: 200,
      runs: 200, agents: 200, steer_targets: 100},
    honesty: {presence_is_acknowledgment: false, control_acceptance_is_applied: false,
      aggregate_is_read_only: true, command_surfaces: ["/v1/controls"]},
    ...overrides,
  };
}

function fake(overrides: Partial<ConsoleSnapshot> = {}): Fake {
  return {snapshot: snapshot(overrides), controlRequests: [], holdRequests: [],
    failConsoleWith: null, failNextControl: false, gets: 0};
}

async function installFake(page: Page, server: Fake) {
  await page.route(/\/v1\/streams/, route => route.abort());
  await page.route(/\/v1\/projects\/[^/]+\/conflict-console/, async route => {
    server.gets++;
    if (server.failConsoleWith) {
      await route.fulfill({status: server.failConsoleWith,
        json: {error: {code: "forbidden", message: "forbidden", retryable: false}}});
      return;
    }
    await route.fulfill({json: {data: server.snapshot}});
  });
  await page.route(/\/v1\/controls$/, async route => {
    const body = route.request().postDataJSON() as Record<string, unknown>;
    server.controlRequests.push({body, key: route.request().headers()["idempotency-key"] ?? null});
    if (server.failNextControl) {
      server.failNextControl = false;
      await route.fulfill({status: 503,
        json: {error: {code: "unavailable", message: "unavailable", retryable: true}}});
      return;
    }
    const control: ConsoleControl = {
      id: `control-${server.controlRequests.length}`,
      action: String(body.action), state: "queued", reason: String(body.reason),
      target_run_id: String(body.target_run_id), target_agent_id: "agent-1",
      coordinator_epoch: Number(body.coordinator_epoch), revision: 1,
    };
    server.snapshot = {...server.snapshot,
      controls: [control, ...server.snapshot.controls],
      counts: {...server.snapshot.counts, controls: server.snapshot.controls.length + 1,
        pending_controls: server.snapshot.counts.pending_controls + 1}};
    await route.fulfill({json: {data: control}});
  });
  await page.route(/\/v1\/coordination-actions\/[^/]+:resolve$/, async route => {
    const body = route.request().postDataJSON() as Record<string, unknown>;
    server.holdRequests.push({path: route.request().url(), body,
      key: route.request().headers()["idempotency-key"] ?? null});
    const hold = server.snapshot.holds.find(item =>
      route.request().url().includes(item.id));
    if (hold) {
      server.snapshot = {...server.snapshot,
        holds: server.snapshot.holds.filter(item => item.id !== hold.id),
        actions: server.snapshot.actions.map(item => item.id === hold.id
          ? {...item, state: body.action === "dismiss" ? "dismissed" : "resolved"} : item)};
    }
    await route.fulfill({json: {data: {...hold, state: "dismissed"}}});
  });
}

test.describe("conflict control console (isolated harness)", () => {
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

  test("renders canonical claims, overlaps, conflicts and holds with redaction counts",
    async ({page}) => {
      const server = fake({
        claims: [
          {id: "c1", run_id: "run-1", task_id: "task-9", mode: "exclusive_owner",
            state: "active", scope_ids: ["scope-1"], hidden_scope_count: 2,
            expires_at: 9_999, ownership_epoch: 4, revision: 1},
          {id: "c2", run_id: "run-2", task_id: null, mode: "exclusive_owner",
            state: "active", scope_ids: ["scope-1"], hidden_scope_count: 0,
            expires_at: 9_999, ownership_epoch: 5, revision: 1},
        ],
        claim_overlaps: [{kind: "scope_overlap", claim_ids: ["c1", "c2"],
          run_ids: ["run-1", "run-2"], modes: ["exclusive_owner"]}],
        conflicts: [{id: "conflict-1", winner_claim_id: "c1", requesting_run_id: "run-2",
          scope_ids: ["scope-1"], hidden_scope_count: 1, state: "denied",
          enforcement: "new_claim_denied", revision: 1}],
        holds: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
          state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
        actions: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
          state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
        runs: [run(), run({id: "run-2", agent_id: "agent-2"})],
        counts: {claims: 2, overlaps: 1, conflicts: 1, holds: 1, actions: 1, controls: 0,
          runs: 2, pending_controls: 0},
        redactions: {hidden_claim_scopes: 2, hidden_conflict_scopes: 1,
          hidden_control_fields: 0, hidden_steer_targets: 0},
      });
      await installFake(page, server);
      await page.goto(HARNESS);
      await expect(page.getByTestId("console-lease")).toContainText("epoch 7");
      const claims = page.getByTestId("console-claim");
      await expect(claims).toHaveCount(2);
      await expect(claims.first()).toHaveAttribute("data-overlapping", "true");
      await expect(claims.first()).toContainText("2 protected scopes redacted");
      await expect(page.getByTestId("console-conflict")).toContainText("denied");
      await expect(page.getByTestId("console-conflict")).toContainText("1 redacted");
      await expect(page.getByTestId("console-hold")).toContainText("held for operator review");
      await expect(page.getByTestId("console-redaction")).toContainText("3 protected references");
    });

  test("a queued control reads as requested, never as paused or applied", async ({page}) => {
    const server = fake({
      controls: [
        {id: "ctl-1", action: "pause", state: "queued", reason: "Review",
          target_run_id: "run-1", target_agent_id: "agent-1", revision: 1},
        {id: "ctl-2", action: "pause", state: "applied", reason: "Review",
          target_run_id: "run-1", target_agent_id: "agent-1", revision: 2,
          result: {outcome: "applied", observed_status: "paused"}},
      ],
      counts: {claims: 0, overlaps: 0, conflicts: 0, holds: 0, actions: 0, controls: 2,
        runs: 1, pending_controls: 1},
    });
    await installFake(page, server);
    await page.goto(HARNESS);
    const controls = page.getByTestId("console-control");
    await expect(controls).toHaveCount(2);
    const queued = controls.filter({hasText: "Requested"});
    await expect(queued).toHaveCount(1);
    await expect(queued).toContainText("queued for delivery");
    const applied = controls.filter({hasText: "Applied (per target runtime result receipt)"});
    await expect(applied).toContainText("Target runtime reported: paused");
    await expect(applied).toContainText("not a guarantee");
  });

  test("pause requires confirmation and issues one canonical control with a stable key",
    async ({page}) => {
      const server = fake();
      await installFake(page, server);
      await page.goto(HARNESS);
      const pause = page.getByTestId("control-pause").first();
      await expect(pause).toBeEnabled();
      await pause.click();
      const dialog = page.getByTestId("control-dialog");
      await expect(dialog).toContainText("Queued means requested");
      await expect(dialog.getByTestId("control-confirm")).toBeDisabled();
      await dialog.getByTestId("control-reason").fill("Operator review");
      await dialog.getByTestId("control-confirm").click();
      await expect(page.getByTestId("console-receipt")).toContainText("Requested");
      await expect(page.getByTestId("console-receipt")).toContainText("not an applied outcome");
      // The displayed receipt carries the canonical creation response's id.
      await expect(page.getByTestId("console-receipt")).toContainText("control-1");
      expect(server.controlRequests).toHaveLength(1);
      expect(server.controlRequests[0].key).toBeTruthy();
      expect(server.controlRequests[0].body).toMatchObject({
        project_id: "proj-1", target_run_id: "run-1", action: "pause",
        target_run_revision: 3, coordinator_epoch: 7, replacement_task_id: null});
      await expect(page.getByTestId("console-control")).toHaveCount(1);
    });

  test("a failed send locks fields and retries replay the frozen payload and key",
    async ({page}) => {
      const server = fake();
      server.failNextControl = true;
      await installFake(page, server);
      await page.goto(HARNESS);
      await page.getByTestId("control-pause").first().click();
      const dialog = page.getByTestId("control-dialog");
      await dialog.getByTestId("control-reason").fill("Operator review");
      await dialog.getByTestId("control-confirm").click();
      await expect(dialog).toBeVisible();
      // After the first submission the reason is locked to the frozen payload:
      // the displayed confirmation is exactly what a retry replays.
      await expect(dialog.getByTestId("control-locked")).toBeVisible();
      await expect(dialog.getByTestId("control-reason")).toBeDisabled();
      await expect(dialog.getByTestId("control-reason")).toHaveValue("Operator review");
      await expect(dialog.getByTestId("control-confirm")).toHaveCount(0);
      await dialog.getByTestId("control-retry").click();
      await expect(page.getByTestId("console-receipt")).toBeVisible();
      expect(server.controlRequests).toHaveLength(2);
      expect(server.controlRequests[1].key).toBe(server.controlRequests[0].key);
      expect(server.controlRequests[1].body).toEqual(server.controlRequests[0].body);
    });

  test("steer targets only an actual authorized queued task", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("control-steer").first().click();
    const dialog = page.getByTestId("control-dialog");
    await dialog.getByTestId("control-reason").fill("Redirect to queued work");
    await expect(dialog.getByTestId("control-confirm")).toBeDisabled();
    await dialog.getByTestId("steer-target").selectOption("task-1");
    await dialog.getByTestId("control-confirm").click();
    await expect(page.getByTestId("console-receipt")).toBeVisible();
    expect(server.controlRequests[0].body).toMatchObject({
      action: "steer", replacement_task_id: "task-1"});
  });

  test("unsupported boundaries are disabled with an accurate explanation", async ({page}) => {
    const server = fake({
      runs: [run()],
      agents: [agent({supported_actions: [], scope_permit_enforcement: false})],
    });
    await installFake(page, server);
    await page.goto(HARNESS);
    const pause = page.getByTestId("control-pause").first();
    await expect(pause).toBeDisabled();
    await expect(pause).toHaveAttribute("title", /scope permits/);
  });

  test("without a visible lease every control is disabled", async ({page}) => {
    const server = fake({lease: null,
      issuer_hint: {lease_visible: false, advisory: true, principal_may_issue: false,
        explanation: "No active coordination lease is visible; controls cannot be issued."}});
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("console-lease-empty"))
      .toContainText("No active coordination lease");
    const pause = page.getByTestId("control-pause").first();
    await expect(pause).toBeDisabled();
    await expect(pause).toHaveAttribute("title", /lease/);
    await expect(page.getByTestId("control-steer").first()).toBeDisabled();
  });

  test("hold dismissal is explicit and posts the canonical resolution", async ({page}) => {
    const server = fake({
      holds: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
        state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
      actions: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
        state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
      counts: {claims: 0, overlaps: 0, conflicts: 0, holds: 1, actions: 1, controls: 0,
        runs: 1, pending_controls: 0},
    });
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("hold-dismiss").click();
    const dialog = page.getByTestId("hold-dialog");
    await expect(dialog).toContainText("without altering ownership");
    await dialog.getByTestId("hold-reason").fill("Reviewed; work resumed normally");
    await dialog.getByTestId("hold-confirm").click();
    await expect(page.getByTestId("console-hold")).toHaveCount(0);
    expect(server.holdRequests).toHaveLength(1);
    expect(server.holdRequests[0].body).toMatchObject({
      expected_revision: 2, action: "dismiss"});
    expect(server.holdRequests[0].key).toBeTruthy();
  });

  test("an authorization failure clears protected data", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("console-claim")).toHaveCount(1);
    await expect(page.getByTestId("console-lease")).toBeVisible();
    server.failConsoleWith = 403;
    await page.getByTestId("simulate-refresh").click();
    await expect(page.getByTestId("console-error")).toContainText("not authorized");
    await expect(page.getByTestId("console-claim")).toHaveCount(0);
    await expect(page.getByTestId("console-lease")).toHaveCount(0);
    await expect(page.getByTestId("console-control")).toHaveCount(0);
  });

  test("switching projects fences stale results", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.route(/\/v1\/projects\/proj-2\/conflict-console/, async route => {
      await new Promise(resolve => setTimeout(resolve, 600));
      await route.fulfill({json: {data: {...server.snapshot, project_id: "proj-2",
        controls: [{id: "ctl-p2", action: "cancel", state: "queued",
          target_run_id: "run-9", revision: 1}],
        counts: {...server.snapshot.counts, controls: 1, pending_controls: 1}}}});
    });
    await page.goto(HARNESS);
    await expect(page.getByTestId("console-lease")).toBeVisible();
    await page.getByTestId("switch-project").click();
    await expect(page.getByTestId("harness-scope")).toHaveAttribute("data-project", "proj-2");
    // The proj-1 lease display must clear immediately, before proj-2 resolves.
    await expect(page.getByTestId("console-lease")).toHaveCount(0);
    await expect(page.getByTestId("console-lease")).toBeVisible();
    await expect(page.getByTestId("console-control")).toHaveCount(1);
    expect(server.gets).toBeGreaterThanOrEqual(1);
  });

  test("live invalidation refetches without duplicating writes", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("console-lease")).toBeVisible();
    const before = server.gets;
    await page.getByTestId("simulate-refresh").click();
    await expect.poll(() => server.gets).toBeGreaterThan(before);
    expect(server.controlRequests).toHaveLength(0);
  });

  test("a normal human sees issuance disabled with the truthful hint", async ({page}) => {
    const server = fake({
      issuer_hint: {lease_visible: true, advisory: true, principal_may_issue: false,
        explanation: "Controls are issued by the lease-holding coordinator agent under a "
          + "coordinator grant; your credentials do not represent it, so issuance would be "
          + "refused by the canonical path."},
    });
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("console-issuer-hint"))
      .toContainText("lease-holding coordinator");
    const pause = page.getByTestId("control-pause").first();
    await expect(pause).toBeDisabled();
    await expect(pause).toHaveAttribute("title", /do not represent it/);
    // Read-only operator capabilities remain: no dialog ever opens.
    await pause.click({force: true}).catch(() => {});
    await expect(page.getByTestId("control-dialog")).toHaveCount(0);
    expect(server.controlRequests).toHaveLength(0);
  });

  test("overlapping same-scope reads apply in issue order (latest read wins)",
    async ({page}) => {
      const server = fake();
      let call = 0;
      await page.route(/\/v1\/streams/, route => route.abort());
      await page.route(/\/v1\/projects\/[^/]+\/conflict-console/, async route => {
        call++;
        server.gets++;
        if (call === 1) {
          // The first (older) read is slow and carries a conflict.
          await new Promise(resolve => setTimeout(resolve, 700));
          await route.fulfill({json: {data: {...server.snapshot,
            conflicts: [{id: "conflict-old", winner_claim_id: "c1",
              requesting_run_id: "run-2", scope_ids: [], hidden_scope_count: 0,
              state: "denied", revision: 1}],
            counts: {...server.snapshot.counts, conflicts: 1}}}});
          return;
        }
        await route.fulfill({json: {data: server.snapshot}});
      });
      await page.route(/\/v1\/controls$/, route => route.fulfill({status: 500, json: {}}));
      await page.goto(HARNESS);
      // Trigger the newer read before the older one resolves.
      await page.getByTestId("simulate-refresh").click();
      await expect(page.getByTestId("console-lease")).toBeVisible();
      // The slow older read resolves last but must not overwrite the newer one.
      await page.waitForTimeout(900);
      await expect(page.getByTestId("console-conflict")).toHaveCount(0);
      await expect(page.getByTestId("console-lease")).toBeVisible();
    });

  test("a scope change during a send drops the receipt and never strands sending",
    async ({page}) => {
      const server = fake();
      await installFake(page, server);
      await page.route(/\/v1\/projects\/proj-2\/conflict-console/, async route => {
        await route.fulfill({json: {data: {...server.snapshot, project_id: "proj-2"}}});
      });
      await page.route(/\/v1\/controls$/, async route => {
        server.controlRequests.push({body: route.request().postDataJSON(),
          key: route.request().headers()["idempotency-key"] ?? null});
        // Slow enough that the scope change lands first.
        await new Promise(resolve => setTimeout(resolve, 500));
        await route.fulfill({json: {data: {id: "control-late", action: "pause",
          state: "queued", target_run_id: "run-1", revision: 1}}});
      });
      await page.goto(HARNESS);
      await page.getByTestId("control-pause").first().click();
      const dialog = page.getByTestId("control-dialog");
      await dialog.getByTestId("control-reason").fill("Operator review");
      await dialog.getByTestId("control-confirm").click();
      // The modal backdrop covers the harness buttons; switch directly.
      await page.evaluate(() =>
        document.querySelector<HTMLButtonElement>('[data-testid="switch-project"]')!.click());
      await expect(dialog).toHaveCount(0);
      await expect(page.getByTestId("harness-scope")).toHaveAttribute("data-project", "proj-2");
      await expect(page.getByTestId("console-lease")).toBeVisible();
      // The late response is fenced out: no receipt, and the new view can send.
      await page.waitForTimeout(700);
      await expect(page.getByTestId("console-receipt")).toHaveCount(0);
      await expect(page.getByTestId("control-pause").first()).toBeEnabled();
    });

  test("a failed read clears an open review dialog and receipt", async ({page}) => {
    const server = fake({
      holds: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
        state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
      actions: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
        state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
      counts: {claims: 1, overlaps: 0, conflicts: 0, holds: 1, actions: 1, controls: 0,
        runs: 1, pending_controls: 0},
    });
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("hold-dismiss").click();
    await expect(page.getByTestId("hold-dialog")).toBeVisible();
    server.failConsoleWith = 403;
    // The modal backdrop covers the harness buttons; fire the refresh directly.
    await page.evaluate(() =>
      document.querySelector<HTMLButtonElement>('[data-testid="simulate-refresh"]')!.click());
    await expect(page.getByTestId("console-error")).toContainText("not authorized");
    // The dialog's hidden hold metadata is gone with the failed read.
    await expect(page.getByTestId("hold-dialog")).toHaveCount(0);
    await expect(page.getByTestId("console-hold")).toHaveCount(0);
  });

  test("a healthy refresh removing the held reference clears its review dialog",
    async ({page}) => {
      const server = fake({
        holds: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
          state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
        actions: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
          state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
        counts: {claims: 1, overlaps: 0, conflicts: 0, holds: 1, actions: 1, controls: 0,
          runs: 1, pending_controls: 0},
      });
      await installFake(page, server);
      await page.goto(HARNESS);
      await page.getByTestId("hold-dismiss").click();
      await expect(page.getByTestId("hold-dialog")).toBeVisible();
      // The hold is resolved elsewhere; the fresh snapshot no longer carries it.
      server.snapshot = {...server.snapshot, holds: [],
        actions: server.snapshot.actions.map(item =>
          ({...item, state: "dismissed"})),
        counts: {...server.snapshot.counts, holds: 0}};
      await page.evaluate(() =>
        document.querySelector<HTMLButtonElement>('[data-testid="simulate-refresh"]')!.click());
      await expect(page.getByTestId("hold-dialog")).toHaveCount(0);
      await expect(page.getByTestId("console-error")).toHaveCount(0);
      await expect(page.getByTestId("console-hold")).toHaveCount(0);
    });

  test("a frozen hold retry replays the first-submitted resolution exactly",
    async ({page}) => {
      const server = fake({
        holds: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
          state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
        actions: [{id: "hold-1", task_id: "task-9", action: "manual_hold",
          state: "manual_hold", reason: "execution_liveness_unknown", revision: 2}],
        counts: {claims: 1, overlaps: 0, conflicts: 0, holds: 1, actions: 1, controls: 0,
          runs: 1, pending_controls: 0},
      });
      let failOnce = true;
      await installFake(page, server);
      await page.route(/\/v1\/coordination-actions\/[^/]+:resolve$/, async route => {
        server.holdRequests.push({path: route.request().url(),
          body: route.request().postDataJSON(),
          key: route.request().headers()["idempotency-key"] ?? null});
        if (failOnce) {
          failOnce = false;
          await route.fulfill({status: 503,
            json: {error: {code: "unavailable", message: "unavailable", retryable: true}}});
          return;
        }
        server.snapshot = {...server.snapshot, holds: [],
          counts: {...server.snapshot.counts, holds: 0}};
        await route.fulfill({json: {data: {id: "hold-1", state: "dismissed"}}});
      });
      await page.goto(HARNESS);
      await page.getByTestId("hold-dismiss").click();
      const dialog = page.getByTestId("hold-dialog");
      await dialog.getByTestId("hold-reason").fill("Reviewed; resumed normally");
      await dialog.getByTestId("hold-confirm").click();
      await expect(dialog).toBeVisible();
      // Locked: editing is impossible, so the retry cannot diverge.
      await expect(dialog.getByTestId("hold-locked")).toBeVisible();
      await expect(dialog.getByTestId("hold-reason")).toBeDisabled();
      await expect(dialog.getByTestId("hold-reason")).toHaveValue("Reviewed; resumed normally");
      await dialog.getByTestId("hold-confirm").click();
      await expect(page.getByTestId("console-hold")).toHaveCount(0);
      expect(server.holdRequests).toHaveLength(2);
      expect(server.holdRequests[1].key).toBe(server.holdRequests[0].key);
      expect(server.holdRequests[1].body).toEqual(server.holdRequests[0].body);
      expect(server.holdRequests[0].body).toMatchObject({
        expected_revision: 2, action: "dismiss", reason: "Reviewed; resumed normally"});
    });
});
