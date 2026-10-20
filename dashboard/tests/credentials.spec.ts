import {test, expect, Page, Route} from "@playwright/test";
import {spawn, ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";

// The credential screen is exercised in an isolated vite dev server so it can be
// verified before the owner wires its Administration section into
// dashboard/src/main.tsx. Every network call is stubbed with page.route; no real
// Hivemind store, credential or session is touched.
//
// The mock token below is a literal fixture value, not a credential. Tests assert
// only on its presence/absence booleans so a failure cannot print a secret.
const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
// 5203 stays clear of the thread-pane harness (5197), the timeline harness (5199)
// and the python browser fixture (8766).
const HARNESS_PORT = 5203;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/credentials.html`;
const MOCK_TOKEN = "mock-once-token-abcdefghijklmnopqrstuvwxyz-0123456789";
const ADMIN_ID = "principal_admin00001";
const AGENT_ID = "principal_agent00002";
const STALE_ID = "principal_agent00003";
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

type Credential = {
  credential_id: string; project_id: string; role: string; agent_id: string | null;
  agent_display_name: string | null; coordinator: boolean; capture_authority: boolean;
  revision: number; state: string; usable: boolean; active_sessions: number;
  rotated_at: string | null; revoked_at: string | null;
  rotation_count: number; self: boolean;
};

function credential(overrides: Partial<Credential>): Credential {
  return {
    credential_id: ADMIN_ID, project_id: "proj-1", role: "admin", agent_id: null,
    agent_display_name: null, coordinator: false, capture_authority: true, revision: 3,
    state: "active", usable: true, active_sessions: 1, rotated_at: null,
    revoked_at: null, rotation_count: 0, self: true, ...overrides,
  };
}

function listing(project = "proj-1") {
  return {
    project_id: project,
    credentials: [
      credential({}),
      credential({credential_id: AGENT_ID, role: "agent", agent_id: "agent-droid0001",
        agent_display_name: "Droid worker", capture_authority: false, revision: 1, self: false}),
      credential({credential_id: STALE_ID, role: "agent", agent_id: "agent-pi000002",
        agent_display_name: "Oh my Pi worker", capture_authority: false, revision: 4, self: false}),
    ],
    count: 3, limit: 100, has_more: false,
    summary: {usable_admins: 2, project_admins: 2, revoked_credentials: 0},
    policy: {listing: "Credential metadata only: raw tokens and stored token hashes are never listed.",
      rotation: "A rotation returns one new token in its immediate response only."},
    token_material_returned: false,
  };
}

function impact(credentialId: string, operation: "rotate" | "revoke") {
  const self = credentialId === ADMIN_ID;
  return {
    operation, credential_id: credentialId, revision: credentialId === AGENT_ID ? 1 : 3,
    state: "active", role: "agent",
    active_sessions: self ? 1 : 0,
    active_runs: credentialId === AGENT_ID
      ? [{run_id: "run_droid000000001", presence: "online", last_seen_at: 1}] : [],
    active_run_ids: credentialId === AGENT_ID ? ["run_droid000000001"] : [],
    owned_workers: [], runs_truncated: false, workers_truncated: false,
    self_affected: self, last_usable_admin: false, usable_admins: 2, project_admins: 2,
    fingerprint: "f".repeat(64),
    warnings: [
      operation === "rotate"
        ? "Rotation replaces the stored token hash for exactly this principal."
        : "Refused: revoking the last usable administrator credential would leave this deployment without an administrator.",
      self ? "This is your own credential. The bearer token and browser session used for this request stop working immediately." : "No active session.",
      credentialId === AGENT_ID ? "1 active run(s) for this agent lose API access immediately." : "No active run.",
    ],
  };
}

type Recorder = {
  listCalls: string[]; impactCalls: string[]; rotateCalls: {id: string; key: string; body: Record<string, unknown>}[];
  revokeCalls: {id: string; key: string; body: Record<string, unknown>}[];
  setListStatus: (status: number) => void;
  setRotateStatus: (status: number) => void;
  alreadyApplied: (id: string) => void;
  revokeAlreadyApplied: (id: string) => void;
};

async function installMocks(page: Page): Promise<Recorder> {
  const state = {
    listStatus: 200, rotateStatus: 200,
    already: new Set<string>(), revokeAlready: new Set<string>(),
  };
  const recorder: Recorder = {
    listCalls: [], impactCalls: [], rotateCalls: [], revokeCalls: [],
    setListStatus: status => { state.listStatus = status; },
    setRotateStatus: status => { state.rotateStatus = status; },
    alreadyApplied: id => state.already.add(id),
    revokeAlreadyApplied: id => state.revokeAlready.add(id),
  };
  await page.route("**/v1/streams**", route => route.abort());
  await page.route("**/v1/credentials/*:rotate", async (route: Route) => {
    const url = new URL(route.request().url());
    const credentialId = url.pathname.split("/").pop()!.replace(":rotate", "");
    const key = route.request().headers()["idempotency-key"] ?? "";
    recorder.rotateCalls.push({id: credentialId, key,
      body: route.request().postDataJSON() as Record<string, unknown>});
    if (state.rotateStatus !== 200) {
      await route.fulfill({status: state.rotateStatus,
        json: {error: {code: "revision_conflict", message: "The resource changed. Reload before applying this update."}}});
      return;
    }
    if (state.already.has(credentialId)) {
      await route.fulfill({json: {data: {status: "already_applied", token_available: false,
        token_notice: "This rotation was already applied. The replacement token was returned once and cannot "
          + "be shown again; rotate again with the current revision if you need a new token.",
        credential: credential({revision: 4, rotation_count: 1, rotated_at: "2026-01-01T00:00:00Z"}),
        sessions_invalidated: 0, self_affected: false, old_token_invalidated: true}}});
      return;
    }
    await route.fulfill({json: {data: {status: "rotated", token: MOCK_TOKEN, token_available: true,
      token_notice: "Shown once.", old_token_invalidated: true, sessions_invalidated: 1,
      self_affected: credentialId === ADMIN_ID,
      credential: credential({credential_id: credentialId, revision: 4, rotation_count: 1,
        rotated_at: "2026-01-01T00:00:00Z", self: credentialId === ADMIN_ID})}}});
  });
  await page.route("**/v1/credentials/*:revoke", async (route: Route) => {
    const url = new URL(route.request().url());
    const credentialId = url.pathname.split("/").pop()!.replace(":revoke", "");
    recorder.revokeCalls.push({id: credentialId,
      key: route.request().headers()["idempotency-key"] ?? "",
      body: route.request().postDataJSON() as Record<string, unknown>});
    if (state.revokeAlready.has(credentialId)) {
      await route.fulfill({json: {data: {status: "already_applied", revoked: true,
        token_available: false, effective_state: "revoked",
        token_notice: "This revocation was already applied. The credential is terminated and cannot be "
          + "restored; issue a new credential out of band if this principal must act again.",
        credential: credential({credential_id: credentialId, revision: 4, state: "revoked",
          usable: false, revoked_at: "2026-01-01T00:00:00Z", self: credentialId === ADMIN_ID}),
        sessions_invalidated: 0, self_affected: false, old_token_invalidated: true}}});
      return;
    }
    await route.fulfill({json: {data: {status: "revoked", revoked: true, token_available: false,
      effective_state: "revoked", principal_preserved: true, sessions_invalidated: 1,
      self_affected: credentialId === ADMIN_ID,
      credential: credential({credential_id: credentialId, revision: 4, state: "revoked",
        usable: false, revoked_at: "2026-01-01T00:00:00Z", self: credentialId === ADMIN_ID})}}});
  });
  await page.route("**/v1/credentials/*/impact*", async (route: Route) => {
    const url = new URL(route.request().url());
    const credentialId = url.pathname.split("/")[3];
    const operation = url.searchParams.get("operation") === "rotate" ? "rotate" : "revoke";
    recorder.impactCalls.push(`${credentialId}:${operation}`);
    await route.fulfill({json: {data: impact(credentialId, operation)}});
  });
  await page.route("**/v1/projects/*/credentials*", async (route: Route) => {
    const url = new URL(route.request().url());
    recorder.listCalls.push(url.pathname.split("/")[3]);
    if (state.listStatus !== 200) {
      await route.fulfill({status: state.listStatus,
        json: {error: {code: state.listStatus === 403 ? "administrator_required" : "authentication_required",
          message: "not authorized"}}});
      return;
    }
    await route.fulfill({json: {data: listing(url.pathname.split("/")[3])}});
  });
  return recorder;
}

async function open(page: Page, query = "") {
  await page.goto(`${HARNESS}${query}`);
  await expect(page.getByTestId("shell-marker")).toBeVisible();
  await expect(page.getByRole("heading", {name: "Credential rotation & revocation."})).toBeVisible();
}

async function reviewRotation(page: Page, row: ReturnType<Page["getByTestId"]>) {
  await row.getByRole("button", {name: "Review rotation"}).click();
  await expect(row.getByTestId("credential-impact")).toBeVisible();
}

async function confirmRotation(page: Page, row: ReturnType<Page["getByTestId"]>) {
  await row.getByRole("button", {name: "Rotate credential"}).click();
  await row.getByRole("button", {name: "Confirm rotation"}).click();
}

test.describe("credential management screen (isolated harness)", () => {
  test.beforeAll(async () => {
    harness = spawn(process.execPath,
      ["node_modules/vite/bin/vite.js", "--port", String(HARNESS_PORT), "--strictPort", "--host", "127.0.0.1"],
      {cwd: DASHBOARD, stdio: "ignore"});
    await waitForHarness();
  });

  test.afterAll(() => {
    harness?.kill();
    harness = null;
  });

  test("lists credential metadata, masks identifiers and stores no secrets", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    expect(recorder.listCalls).toEqual(["proj-1"]);
    const rows = page.getByTestId("credential-row");
    await expect(rows).toHaveCount(3);
    await expect(rows.nth(0)).toContainText("Administrator (this credential)");
    await expect(rows.nth(1)).toContainText("Droid worker");
    await expect(rows.nth(1)).toContainText("Active");
    await expect(page.getByText(ADMIN_ID, {exact: false})).toHaveCount(0);
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
    expect(await page.evaluate(() => [Object.keys(localStorage).length, Object.keys(sessionStorage).length]))
      .toEqual([0, 0]);
    await expect(page.getByText("Usable administrators")).toBeVisible();
  });

  test("requires an impact review and an explicit confirmation before rotating", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    const row = page.getByTestId("credential-row").filter({hasText: "Droid worker"});
    expect(recorder.rotateCalls).toHaveLength(0);
    await reviewRotation(page, row);
    expect(recorder.impactCalls).toEqual([`${AGENT_ID}:rotate`]);
    await expect(row.getByTestId("credential-impact")).toContainText("Active runs");
    await expect(row.getByTestId("credential-impact")).toContainText("1 active run(s)");
    expect(recorder.rotateCalls).toHaveLength(0);
    await row.getByRole("button", {name: "Rotate credential"}).click();
    expect(recorder.rotateCalls).toHaveLength(0);
    await expect(row.getByTestId("credential-confirm")).toContainText("shown once");
    await row.getByRole("button", {name: "Confirm rotation"}).click();
    await expect.poll(() => recorder.rotateCalls.length).toBe(1);
    expect(recorder.rotateCalls[0].id).toBe(AGENT_ID);
    expect(recorder.rotateCalls[0].key.length).toBeGreaterThan(8);
    expect(recorder.rotateCalls[0].body).toEqual({expected_revision: 1, acknowledged_impact: "f".repeat(64)});
  });

  test("reveals a rotation token once, clears it on dismissal and never repeats it", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    const row = page.getByTestId("credential-row").filter({hasText: "Administrator (this credential)"});
    await reviewRotation(page, row);
    await confirmRotation(page, row);
    const token = page.getByTestId("credential-token");
    await expect(token).toBeVisible();
    // Masked until an explicit reveal, and the shell is handed the token once.
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
    await expect(page.getByTestId("self-rotation")).toHaveText("received");
    await token.getByRole("button", {name: "Reveal token"}).click();
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeTruthy();
    await expect(token).toContainText("cannot be retrieved again");
    await token.getByRole("button", {name: "Hide token"}).click();
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
    await token.getByRole("button", {name: "Reveal token"}).click();
    await token.getByRole("button", {name: "Dismiss token"}).click();
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
    expect(await page.evaluate(() => [Object.keys(localStorage).length, Object.keys(sessionStorage).length]))
      .toEqual([0, 0]);

    // A credential whose rotation was already applied is reported honestly: no token.
    const stale = page.getByTestId("credential-row").filter({hasText: "Oh my Pi worker"});
    recorder.alreadyApplied(STALE_ID);
    await reviewRotation(page, stale);
    await confirmRotation(page, stale);
    await expect(page.getByTestId("credential-notice")).toContainText("already applied");
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    expect(recorder.rotateCalls.map(call => call.id)).toEqual([ADMIN_ID, STALE_ID]);
    expect(recorder.rotateCalls[0].key).not.toBe(recorder.rotateCalls[1].key);
  });

  test("clears a stale reviewed impact and reloads canonical state", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    const row = page.getByTestId("credential-row").filter({hasText: "Droid worker"});
    await reviewRotation(page, row);
    recorder.setRotateStatus(412);
    await confirmRotation(page, row);
    await expect(page.getByTestId("credential-error")).toContainText("412");
    await expect(row.getByTestId("credential-impact")).toHaveCount(0);
    await expect.poll(() => recorder.listCalls.length).toBeGreaterThan(1);
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
  });

  test("drops revealed secrets on project switch and on authorization loss", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    const row = page.getByTestId("credential-row").filter({hasText: "Administrator (this credential)"});
    await reviewRotation(page, row);
    await confirmRotation(page, row);
    await page.getByTestId("credential-token").getByRole("button", {name: "Reveal token"}).click();
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeTruthy();
    await page.getByRole("button", {name: "Switch project"}).click();
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    expect(recorder.listCalls).toContain("proj-2");
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();

    // A fresh rotation, then an authorization failure on refresh: no secret survives it.
    const second = page.getByTestId("credential-row").filter({hasText: "Administrator (this credential)"});
    await reviewRotation(page, second);
    await confirmRotation(page, second);
    await page.getByTestId("credential-token").getByRole("button", {name: "Reveal token"}).click();
    recorder.setListStatus(403);
    await page.getByRole("button", {name: "Refresh credentials"}).click();
    await expect(page.getByTestId("credential-error")).toContainText("403");
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    await expect(page.getByTestId("errors")).toContainText("403");
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
  });

  test("revokes with impact confirmation, tells the shell to sign out, and stays responsive", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    const row = page.getByTestId("credential-row").filter({hasText: "Administrator (this credential)"});
    await row.getByRole("button", {name: "Review revocation"}).click();
    await expect(row.getByTestId("credential-impact")).toContainText("Revocation impact");
    await row.getByRole("button", {name: "Revoke credential"}).click();
    await expect(row.getByTestId("credential-confirm")).toContainText("cannot be undone");
    await row.getByRole("button", {name: "Confirm revocation"}).click();
    await expect.poll(() => recorder.revokeCalls.length).toBe(1);
    expect(recorder.revokeCalls[0].body).toEqual({expected_revision: 3, acknowledged_impact: "f".repeat(64)});
    await expect(page.getByTestId("self-revocation")).toHaveText("received");
    await expect(page.getByTestId("credential-notice")).toContainText("signed out");
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    await expect(page.getByTestId("shell-marker")).toBeVisible();
    await page.setViewportSize({width: 390, height: 844});
    await page.screenshot({path: fileURLToPath(new URL("../.hivemind-test/ui-credentials-mobile.png", import.meta.url)),
      fullPage: true, animations: "disabled"});
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
  });

  test("non-administrators load no credential metadata", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page, "?role=observer");
    await expect(page.getByText("Credential management requires an administrator credential")).toBeVisible();
    expect(recorder.listCalls).toEqual([]);
    await page.getByRole("button", {name: "Switch role"}).click();
    await expect(page.getByTestId("credential-row")).toHaveCount(3);
    expect(recorder.listCalls).toEqual(["proj-1"]);
  });

  test("an already-applied revocation reports honestly without signing the shell out", async ({page}) => {
    const recorder = await installMocks(page);
    await open(page);
    const row = page.getByTestId("credential-row").filter({hasText: "Droid worker"});
    recorder.revokeAlreadyApplied(AGENT_ID);
    await row.getByRole("button", {name: "Review revocation"}).click();
    await row.getByRole("button", {name: "Revoke credential"}).click();
    await row.getByRole("button", {name: "Confirm revocation"}).click();
    await expect.poll(() => recorder.revokeCalls.length).toBe(1);
    await expect(page.getByTestId("credential-notice")).toContainText("already applied");
    await expect(page.getByTestId("credential-notice")).toContainText("cannot be restored");
    // No token, no self-sign-out callback, and the reviewed impact is consumed.
    await expect(page.getByTestId("credential-token")).toHaveCount(0);
    await expect(page.getByTestId("self-revocation")).toHaveText("");
    await expect(row.getByTestId("credential-impact")).toHaveCount(0);
    expect(await page.evaluate(() => document.body.innerHTML.includes("mock-once-token"))).toBeFalsy();
  });
});
