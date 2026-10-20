import {test, expect, Page} from "@playwright/test";
import {spawn, ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";
import {
  describeUnresolved,
  findUnresolvedMentions,
  normalizeMentionReceipts,
  receiptFeedback,
} from "../src/mention-feedback";

// ---------------------------------------------------------------------------
// Pure detection / receipt logic. No browser, no fixture, no shared store.
// ---------------------------------------------------------------------------

test("plain-text mentions are detected and only a selected chip resolves them", () => {
  expect(findUnresolvedMentions("@Droid")).toEqual([{text: "Droid", start: 0, end: 6}]);
  expect(findUnresolvedMentions("Hello @Droid world")).toEqual([{text: "Droid", start: 6, end: 12}]);
  expect(findUnresolvedMentions("(@Droid)")).toEqual([{text: "Droid", start: 1, end: 7}]);
  expect(findUnresolvedMentions("@Droid and @Pi", [{id: "a", display_name: "Droid"}]))
    .toEqual([{text: "Pi", start: 11, end: 14}]);
  // A selected display name containing a space covers the whole token it names.
  expect(findUnresolvedMentions("ping @Droid worker now", [{id: "a", display_name: "Droid worker"}]))
    .toEqual([]);
  // A different agent's chip must never silently resolve the token.
  expect(findUnresolvedMentions("ping @Droid", [{id: "a", display_name: "Droid worker"}]))
    .toEqual([{text: "Droid", start: 5, end: 11}]);
  expect(findUnresolvedMentions("@Droid!", [{id: "a", display_name: "Droid"}])).toEqual([]);
});

test("e-mails, identifiers, escaped markers and code are never flagged", () => {
  const clean = [
    "mail user@example.com", "see a@b and x@y", "\\@Droid", "`@Droid`",
    "```\n@Droid worker\n```", "cost is 5 @ 3", "hi @", "@", "no mention here",
  ];
  for (const text of clean) expect(findUnresolvedMentions(text), text).toEqual([]);
});

test("the warning copy says plain text and never promises delivery", () => {
  expect(describeUnresolved([{text: "Droid", start: 0, end: 6}]))
    .toBe("@Droid is plain text and will not notify an agent.");
  expect(describeUnresolved([{text: "Droid", start: 0, end: 6}, {text: "Pi", start: 9, end: 12}]))
    .toBe("@Droid and @Pi are plain text and will not notify any agent.");
  expect(describeUnresolved([])).toBe("");
});

test("receipt normalization keeps only canonical records and their minimal fields", () => {
  const receipts = normalizeMentionReceipts({
    post_id: "post-1",
    recipients: [
      {agent_id: "agent-1", agent_display_name: "Droid worker", notification_id: "notif-1",
       state: "queued", created_at: "2026-01-01T00:00:00Z", expires_at: 1767225600, presence: "offline",
       body: "recipient private body", runs: [{id: "run-1"}], summary: {text: "private summary"}},
      {agent_id: "agent-2", state: "queued"},        // no canonical notification id -> dropped
      {notification_id: "notif-3", state: "queued"},  // no recipient -> dropped
      null, "nope",
    ],
  });
  expect(receipts.queued_count).toBe(1);
  expect(receipts.recipients).toHaveLength(1);
  expect(Object.keys(receipts.recipients[0]).sort()).toEqual(
    ["agent_id", "created_at", "display_name", "expires_at", "notification_id", "presence", "state"]);
  // Anything that is not "queued" is reported as closed, never upgraded.
  expect(normalizeMentionReceipts({recipients: [
    {agent_id: "a", notification_id: "n", state: "delivered"}]}).recipients[0].state).toBe("closed");
});

test("feedback only claims queued from a receipt and never promises a reply", () => {
  expect(receiptFeedback(null, {pending: true}).status).toBe("pending");
  const unavailable = receiptFeedback(null, {error: new Error("500")});
  expect(unavailable.status).toBe("unavailable");
  expect(unavailable.message).toMatch(/could not be read/);
  expect(unavailable.message).not.toMatch(/is queued|were queued|notification was recorded/);

  const queued = receiptFeedback({post_id: "p", queued_count: 1, recipients: [
    {agent_id: "a", display_name: "Droid worker", notification_id: "n", state: "queued", presence: "offline"}]});
  expect(queued.status).toBe("queued");
  expect(queued.message).toContain("offline");
  expect(queued.message).toContain("reconnecting");
  expect(queued.message).toContain("not that an agent has replied");
  expect(queued.message).not.toMatch(/will reply|will respond|immediately/);

  expect(receiptFeedback({post_id: "p", queued_count: 0, recipients: []}).status).toBe("none");
  expect(receiptFeedback({post_id: "p", queued_count: 0, recipients: [
    {agent_id: "a", notification_id: "n", state: "closed"}]}).status).toBe("closed");
});

// ---------------------------------------------------------------------------
// Isolated harness. A local vite dev server mounts the composer, the shared
// guard and the receipt feedback exactly as main.tsx / ThreadPane.tsx should
// wire them, so the contracts are verified before the shared hosts change.
// 5202 stays clear of the thread-pane harness (5197), the timeline harness
// (5199) and the python fixture (8766); every network call is stubbed.
// ---------------------------------------------------------------------------
const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5202;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/mention-feedback.html`;
let harness: ChildProcess | null = null;

async function waitForHarness() {
  for (let attempt = 0; attempt < 80; attempt++) {
    try {
      if ((await fetch(HARNESS)).ok) return;
    } catch {
      // The dev server is still starting.
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Harness server did not start at ${HARNESS}`);
}

const DROID = {id: "agent-droid0001", display_name: "Droid worker", base_color_token: "teal"};

type ReceiptReply = {status: number; json: unknown};

type MockOptions = {
  mentionOptions?: unknown[];
  /** Evaluated per request so a test can flip the server's answer. */
  receipt?: () => ReceiptReply;
  /** Held open until the test releases it, to observe the optimistic stage. */
  gate?: {promise: Promise<void>};
};

async function installMocks(page: Page, options: MockOptions = {}) {
  const posts: Record<string, any>[] = [];
  await page.route("**/v1/streams**", route => route.abort());
  await page.route("**/v1/projects/*/mention-options**", route =>
    route.fulfill({json: {data: options.mentionOptions ?? []}}));
  await page.route("**/v1/boards/*/posts", async route => {
    posts.push(route.request().postDataJSON() as Record<string, any>);
    await route.fulfill({json: {data: {id: `post-${posts.length}`, revision: 1,
      created_at: new Date(Date.UTC(2026, 0, 1)).toISOString()}}});
  });
  await page.route("**/v1/posts/*/mention-receipts", async route => {
    await options.gate?.promise;
    const reply = options.receipt?.() ?? {status: 500, json: {error: {message: "receipt_unavailable"}}};
    await route.fulfill({status: reply.status, json: reply.json});
  });
  return {posts};
}

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return {promise, release};
}

const receipts = (recipients: unknown[]) => () => ({status: 200, json: {data: {
  post_id: "post-1", recipients, queued_count: (recipients as {state?: string}[])
    .filter(item => item?.state === "queued").length, generated_at: 1767225600,
}}});

async function postAsPlainText(page: Page, text: string) {
  await page.getByLabel("Shared board message").fill(text);
  await page.getByRole("button", {name: "Send as plain text anyway"}).click();
  await page.getByRole("button", {name: "Post to board"}).click();
  await expect(page.getByTestId("log")).toContainText("posted:post-1");
}

test.describe("mention feedback (isolated harness)", () => {
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

  test("a typed mention that was never selected warns and needs explicit confirmation", async ({page}) => {
    const mocks = await installMocks(page, {mentionOptions: [DROID]});
    await page.goto(HARNESS);
    const field = page.getByLabel("Shared board message");
    await field.fill("@Droid worker");

    const warning = page.locator('[data-mention-unresolved="true"]');
    await expect(warning).toBeVisible();
    await expect(warning).toContainText("plain text and will not notify an agent");
    await expect(warning.locator("code")).toHaveText("@Droid");

    // The gate holds: a submit attempt sends nothing until the author confirms.
    await page.getByRole("button", {name: "Post to board"}).click();
    expect(mocks.posts).toHaveLength(0);
    await expect(page.getByTestId("log")).toHaveText("");

    await page.getByRole("button", {name: "Send as plain text anyway"}).click();
    await expect(warning).toContainText("Confirmed: this post sends them as plain text.");
    await page.getByRole("button", {name: "Post to board"}).click();
    await expect(page.getByTestId("log")).toContainText("posted:post-1");
    expect(mocks.posts).toHaveLength(1);
    expect(mocks.posts[0].content.segments).toEqual([{kind: "text", text: "@Droid worker"}]);
    await expect(field).toHaveValue("");
    await expect(page.locator('[data-mention-unresolved="true"]')).toHaveCount(0);
  });

  test("editing the draft invalidates a stale plain-text confirmation", async ({page}) => {
    const mocks = await installMocks(page, {mentionOptions: [DROID]});
    await page.goto(HARNESS);
    const field = page.getByLabel("Shared board message");
    await field.fill("@Droid worker");
    await page.getByRole("button", {name: "Send as plain text anyway"}).click();
    await expect(page.locator('[data-mention-unresolved="true"]')).toContainText("Confirmed");

    await field.fill("@Droid worker and @Pi");
    const warning = page.locator('[data-mention-unresolved="true"]');
    await expect(warning).toContainText("@Pi");
    await expect(warning).not.toContainText("Confirmed");
    await page.getByRole("button", {name: "Post to board"}).click();
    expect(mocks.posts).toHaveLength(0);
  });

  test("a chip selected from the list resolves the mention and sends without a warning", async ({page}) => {
    const mocks = await installMocks(page, {mentionOptions: [DROID]});
    await page.goto(HARNESS);
    const field = page.getByLabel("Shared board message");
    await field.fill("@Dro");
    await page.getByRole("option", {name: /Droid worker/}).click();
    await expect(page.locator(".chips")).toContainText("@Droid worker");

    await field.fill("please confirm the breakpoint");
    await expect(page.locator('[data-mention-unresolved="true"]')).toHaveCount(0);
    await page.getByRole("button", {name: "Post to board"}).click();
    await expect(page.getByTestId("log")).toContainText("posted:post-1");
    expect(mocks.posts[0].content.segments).toEqual([
      {kind: "agent_mention", agent_id: "agent-droid0001"},
      {kind: "text", text: "please confirm the breakpoint"},
    ]);
  });

  test("e-mails, identifiers and code spans are not treated as mentions", async ({page}) => {
    const mocks = await installMocks(page, {mentionOptions: [DROID]});
    await page.goto(`${HARNESS}?inline=1`);
    await page.getByLabel("Shared board message").fill("mail user@example.com and `@Droid`");
    await expect(page.locator('[data-mention-unresolved="true"]')).toHaveCount(0);
    await page.getByRole("button", {name: "Post to board"}).click();
    await expect(page.getByTestId("log")).toContainText("posted:post-1");
    expect(mocks.posts).toHaveLength(1);
  });

  test("the composer warns inline when the host does not render the shared guard", async ({page}) => {
    await installMocks(page, {mentionOptions: [DROID]});
    await page.goto(`${HARNESS}?inline=1`);
    await page.getByLabel("Shared board message").fill("@Droid worker");
    const warning = page.locator('[data-mention-unresolved="true"]');
    await expect(warning).toBeVisible();
    await expect(page.getByLabel("Shared board message")).toHaveAttribute("aria-describedby", /-unresolved$/);
    // The fallback notice never duplicates the confirmation control.
    await expect(warning.getByRole("button")).toHaveCount(0);
  });

  test("queued feedback comes from the canonical receipt, with honest offline copy", async ({page}) => {
    const gate = deferred();
    await installMocks(page, {mentionOptions: [DROID], gate, receipt: receipts([
      {agent_id: "agent-droid0001", agent_display_name: "Droid worker", notification_id: "notif-1",
       state: "queued", created_at: "2026-01-01T00:00:00Z", expires_at: 1767225600, presence: "offline"}]),
    });
    await page.goto(HARNESS);
    await page.getByLabel("Shared board message").fill("ping @Droid worker");
    await page.getByRole("button", {name: "Send as plain text anyway"}).click();
    await page.getByRole("button", {name: "Post to board"}).click();
    await expect(page.getByTestId("log")).toContainText("posted:post-1");

    // Optimistic stage: the canonical post is accepted, the receipt is not in yet.
    await expect(page.locator('[data-mention-receipts="pending"]')).toBeVisible();
    gate.release();

    const queued = page.locator('[data-mention-receipts="queued"]');
    await expect(queued).toBeVisible();
    await expect(queued).toContainText("Droid worker");
    await expect(queued).toContainText("notification queued");
    await expect(queued).toContainText("offline");
    await expect(queued).toContainText("reconnecting");
    await expect(queued).toContainText("not that an agent has replied");
    await expect(page.locator('[data-mention-receipts="pending"]')).toHaveCount(0);
  });

  test("presence never upgrades a closed notification into a queued claim", async ({page}) => {
    await installMocks(page, {mentionOptions: [DROID], receipt: receipts([
      {agent_id: "agent-droid0001", agent_display_name: "Droid worker", notification_id: "notif-1",
       state: "closed", presence: "offline"}])});
    await page.goto(HARNESS);
    await postAsPlainText(page, "ping @Droid worker");
    const closed = page.locator('[data-mention-receipts="closed"]');
    await expect(closed).toBeVisible();
    await expect(closed).toContainText("no longer queued");
    await expect(page.locator('[data-mention-receipts="queued"]')).toHaveCount(0);
  });

  test("an unreadable receipt is unavailable, never queued, and can be re-checked", async ({page}) => {
    let mode = "missing";
    await installMocks(page, {mentionOptions: [DROID], receipt: () =>
      mode === "missing"
        ? {status: 404, json: {error: {code: "not_found", message: "deleted"}}}
        : receipts([{agent_id: "agent-droid0001", agent_display_name: "Droid worker",
                     notification_id: "notif-1", state: "queued", presence: "stale"}])()});
    await page.goto(HARNESS);
    await postAsPlainText(page, "ping @Droid worker");

    const unavailable = page.locator('[data-mention-receipts="unavailable"]');
    await expect(unavailable).toBeVisible();
    await expect(unavailable).toContainText("could not be read");
    await expect(page.locator('[data-mention-receipts="queued"]')).toHaveCount(0);

    mode = "ok";
    await unavailable.getByRole("button", {name: "Check receipts again"}).click();
    const queued = page.locator('[data-mention-receipts="queued"]');
    await expect(queued).toBeVisible();
    await expect(queued).toContainText("stale");
  });

  test("the receipt region shows only minimal facts and persists nothing", async ({page}) => {
    await installMocks(page, {mentionOptions: [DROID], receipt: receipts([
      {agent_id: "agent-droid0001", agent_display_name: "Droid worker", notification_id: "notif-secret-1",
       state: "queued", presence: "online", body: "private recipient body", runs: [{id: "run-secret"}],
       summary: {text: "recipient private summary"}, task: "private task"}])});
    await page.goto(HARNESS);
    await postAsPlainText(page, "ping @Droid worker");

    const queued = page.locator('[data-mention-receipts="queued"]');
    await expect(queued).toBeVisible();
    const rendered = await queued.innerText();
    expect(rendered).toContain("Droid worker");
    for (const secret of ["notif-secret-1", "private recipient body", "run-secret",
                          "recipient private summary", "private task"]) {
      expect(rendered).not.toContain(secret);
    }
    // The guard stores nothing: no draft, no receipt, no credential.
    expect(await page.evaluate(() =>
      Object.keys(localStorage).length + Object.keys(sessionStorage).length)).toBe(0);
    expect(await page.evaluate(() => document.cookie)).toBe("");
  });

  test("changing the access context clears the queued feedback", async ({page}) => {
    await installMocks(page, {mentionOptions: [DROID], receipt: receipts([
      {agent_id: "agent-droid0001", agent_display_name: "Droid worker",
       notification_id: "notif-1", state: "queued", presence: "offline"}])});
    await page.goto(HARNESS);
    await postAsPlainText(page, "ping @Droid worker");
    await expect(page.locator('[data-mention-receipts="queued"]')).toBeVisible();

    await page.getByTestId("switch-context").click();
    await expect(page.locator('[data-mention-receipts]')).toHaveCount(0);
  });
});
