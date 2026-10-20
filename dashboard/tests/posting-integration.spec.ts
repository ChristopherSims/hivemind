import {test, expect, type Page, type Route} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {resolve} from "node:path";
import {fileURLToPath} from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const url = "http://127.0.0.1:5208";
const cursor = "a".repeat(64);
let server: ChildProcess | undefined;

test.beforeAll(async () => {
  server = spawn(process.execPath, [resolve(root, "node_modules/vite/bin/vite.js"),
    "--host", "127.0.0.1", "--port", "5208", "--strictPort"], {cwd: root, stdio: "ignore"});
  for (let attempt = 0; attempt < 80; attempt++) {
    try { if ((await fetch(url)).ok) return; } catch { /* Waiting for our isolated server. */ }
    await new Promise(done => setTimeout(done, 100));
  }
  throw new Error("Isolated dashboard server did not start");
});
test.afterAll(() => { server?.kill(); });

async function setup(page: Page, post: (route: Route) => Promise<void>, recipients: unknown[] = [],
  role = "human", extra?: (route: Route) => Promise<boolean>) {
  await page.addInitScript(() => {
    class QuietEventSource extends EventTarget {
      static CONNECTING = 0;
      static OPEN = 1;
      static CLOSED = 2;
      readyState = 1;
      onopen: ((event: Event) => void) | null = null;
      constructor(readonly url: string) {
        super();
        setTimeout(() => this.onopen?.(new Event("open")), 0);
      }
      close() { this.readyState = 2; }
    }
    window.EventSource = QuietEventSource as unknown as typeof EventSource;
  });
  await page.route("**/v1/**", async route => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const reply = (value: unknown) => route.fulfill({json: {data: value}});
    if (extra && await extra(route)) return;
    if (path === "/v1/session") return reply({csrf_token: "synthetic-csrf", role});
    if (path === "/v1/projects") return reply([{id: "project-1", title: "Synthetic workspace"}]);
    if (path.endsWith("/dashboard-snapshot")) return reply({
      agents: [{id: "agent-1", display_name: "Synthetic worker", base_color_token: "teal",
        effective_role: "participant", description: "Synthetic agent"}],
      runs: [], boards: [{id: "board-1", title: "Main board"}],
      delivery: null, snapshot_cursor: cursor,
    });
    if (path === "/v1/boards/board-1/posts") {
      if (request.method() === "POST") return post(route);
      return reply({items: [], next_before: null, snapshot_cursor: cursor});
    }
    if (path.endsWith("/mention-options")) return reply([{id: "agent-1",
      display_name: "Synthetic worker", base_color_token: "teal"}]);
    if (path.endsWith("/mention-receipts")) return reply({
      post_id: "post-1", recipients, queued_count: recipients.length, generated_at: "2026-01-01T00:00:00Z",
    });
    if (path.endsWith("/teams") || path.endsWith("/nexus-grants")) return reply([]);
    if (path.endsWith("/workers")) return reply({workers: [], has_more: false});
    return route.fulfill({status: 404, json: {detail: {code: "not_found", message: "No synthetic route"}}});
  });
  await page.goto(url);
  await expect(page.getByRole("button", {name: "Post to board", exact: true})).toBeVisible();
  await expect(page.locator(".timeline-search")).toContainText("0 loaded");
}

test("main composer shows the pending post without wiping a newer draft on confirmation", async ({page}) => {
  let held: Route | undefined;
  await setup(page, async route => { held = route; });
  const composer = page.locator("form.composer");
  const textarea = composer.getByRole("textbox");
  await textarea.fill("First synthetic post");
  await composer.getByRole("button", {name: "Post to board", exact: true}).click();
  await expect(page.getByText("First synthetic post", {exact: true})).toBeVisible();
  await expect(textarea).toHaveValue("");
  await textarea.fill("A newer unsent draft");
  await expect.poll(() => Boolean(held)).toBe(true);
  const payload = held!.request().postDataJSON();
  await held!.fulfill({json: {data: {id: "post-1", revision: 1, created_at: "2026-01-01T00:00:00Z",
    created_seq: 1, thread_id: "thread-1", author: {kind: "human"},
    content: payload.content, client_post_id: payload.client_post_id}}});
  await expect(textarea).toHaveValue("A newer unsent draft");
  await expect(page.getByRole("button", {name: "Retry", exact: true})).toHaveCount(0);
});

test("main composer retries an ambiguous post with the original body and idempotency key", async ({page}) => {
  const requests: {body: unknown; key: string | undefined}[] = [];
  await setup(page, async route => {
    requests.push({body: route.request().postDataJSON(), key: route.request().headers()["idempotency-key"]});
    await route.abort("failed");
  });
  const composer = page.locator("form.composer");
  await composer.getByRole("textbox").fill("Ambiguous synthetic post");
  await composer.getByRole("button", {name: "Post to board", exact: true}).click();
  await expect(page.getByRole("button", {name: "Retry", exact: true})).toBeVisible();
  await composer.getByRole("textbox").fill("Keep this newer draft");
  await page.getByRole("button", {name: "Retry", exact: true}).click();
  await expect.poll(() => requests.length).toBe(2);
  expect(requests[1]).toEqual(requests[0]);
  expect(requests[0].key).toBeTruthy();
  await expect(composer.getByRole("textbox")).toHaveValue("Keep this newer draft");
});

test("agent tab prefill remains optional and preserves the shared audience and draft", async ({page}) => {
  let writes = 0;
  await setup(page, async route => { writes++; await route.abort(); });
  await page.locator(".roster-card .agent-name").click();
  const composer = page.locator("form.composer");
  await composer.getByRole("textbox").fill("Unsent synthetic draft");
  const prefill = composer.getByRole("group", {name: "Agent mention prefill for Synthetic worker"});
  await expect(prefill.getByRole("checkbox")).not.toBeChecked();
  await prefill.getByRole("checkbox").check();
  await prefill.getByRole("button", {name: "Add @Synthetic worker to this reply"}).click();
  await expect(composer.locator(".chips")).toContainText("@Synthetic worker");
  await expect(composer.getByRole("textbox")).toHaveValue("Unsent synthetic draft");
  await expect(prefill).toContainText("Visible to everyone permitted on this board");
  expect(writes).toBe(0);
});

test("unresolved text requires confirmation again after the draft changes", async ({page}) => {
  let writes = 0;
  await setup(page, async route => { writes++; await route.abort(); });
  const composer = page.locator("form.composer");
  const textarea = composer.getByRole("textbox");
  await textarea.fill("@Unknown original suffix");
  await composer.getByRole("button", {name: "Post to board", exact: true}).click();
  expect(writes).toBe(0);
  await composer.getByRole("button", {name: "Send as plain text anyway"}).click();
  await textarea.fill("@Unknown changed suffix");
  await composer.getByRole("button", {name: "Post to board", exact: true}).click();
  expect(writes).toBe(0);
  await composer.getByRole("button", {name: "Send as plain text anyway"}).click();
  await composer.getByRole("button", {name: "Post to board", exact: true}).click();
  await expect.poll(() => writes).toBe(1);
});

test("offline feedback uses the accepted post's canonical notification receipt", async ({page}) => {
  await setup(page, async route => {
    const payload = route.request().postDataJSON();
    expect(payload.content.segments).toContainEqual({kind: "agent_mention", agent_id: "agent-1"});
    await route.fulfill({json: {data: {id: "post-1", revision: 1, created_at: "2026-01-01T00:00:00Z",
      created_seq: 1, thread_id: "thread-1", author: {kind: "human"},
      content: payload.content, client_post_id: payload.client_post_id}}});
  }, [{agent_id: "agent-1", agent_display_name: "Synthetic worker", notification_id: "notification-1",
    state: "queued", presence: "offline", created_at: "2026-01-01T00:00:00Z", expires_at: null}]);
  await page.locator(".roster-card .agent-name").click();
  const composer = page.locator("form.composer");
  const prefill = composer.getByRole("group", {name: "Agent mention prefill for Synthetic worker"});
  await prefill.getByRole("checkbox").check();
  await prefill.getByRole("button", {name: "Add @Synthetic worker to this reply"}).click();
  await composer.getByRole("textbox").fill("Ping synthetic worker");
  await composer.getByRole("button", {name: "Post to board", exact: true}).click();
  await expect(composer).toContainText("notification is queued");
  await expect(composer).toContainText("offline");
  await expect(composer).toContainText("not that an agent has replied");
});

test("self-rotation replaces the browser session without losing the masked one-time token", async ({page}) => {
  const record = {credential_id: "principal-self", project_id: "project-1", role: "admin",
    agent_id: null, agent_display_name: null, coordinator: false, capture_authority: false,
    revision: 1, state: "active", usable: true, active_sessions: 1, rotated_at: null,
    revoked_at: null, rotation_count: 0, self: true};
  let renewed = false;
  await setup(page, async route => { await route.abort(); }, [], "admin", async route => {
    const request = route.request(), path = new URL(request.url()).pathname;
    const reply = async (data: unknown) => { await route.fulfill({json: {data}}); return true; };
    if (path === "/v1/projects/project-1/credentials") return reply({
      project_id: "project-1", credentials: [record], count: 1, limit: 100, has_more: false,
      summary: {usable_admins: 1, project_admins: 1, revoked_credentials: 0},
      policy: {}, token_material_returned: false,
    });
    if (path.endsWith("/impact")) return reply({...record, operation: "rotate",
      fingerprint: "synthetic-impact", self_affected: true, last_usable_admin: true,
      usable_admins: 1, project_admins: 1, active_runs: [], owned_workers: [],
      active_run_ids: [], owned_worker_ids: [], warnings: [], policy: {},
      runs_truncated: false, workers_truncated: false, token_material_returned: false});
    if (path.endsWith(":rotate")) {
      record.revision = 2; record.rotation_count = 1;
      return reply({status: "rotated", token: "synthetic-replacement-token", token_available: true,
        credential: record, sessions_invalidated: 1, self_affected: true, old_token_invalidated: true});
    }
    if (path === "/v1/session" && request.method() === "POST") {
      expect(request.headers().authorization).toBe("Bearer synthetic-replacement-token");
      renewed = true;
      return reply({csrf_token: "synthetic-renewed-csrf", role: "admin"});
    }
    return false;
  });
  await page.getByRole("button", {name: "Administration", exact: true}).click();
  await page.getByRole("button", {name: "Credentials", exact: true}).click();
  const credentials = page.getByTestId("credential-management");
  await credentials.getByRole("button", {name: "Review rotation"}).click();
  await credentials.getByRole("button", {name: "Rotate credential", exact: true}).click();
  await credentials.getByRole("button", {name: "Confirm rotation", exact: true}).click();
  await expect.poll(() => renewed).toBe(true);
  await expect(page.getByTestId("credential-token")).toBeVisible();
  await expect(page.getByTestId("credential-token").locator("code")).toHaveText("•".repeat(24));
  await expect(credentials.getByRole("button", {name: "Dismiss token"})).toBeEnabled();
  await expect(page.getByLabel("Local human credential")).toHaveCount(0);
});
