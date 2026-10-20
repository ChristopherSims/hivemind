import {test, expect, Page} from "@playwright/test";
import {spawn, ChildProcess} from "node:child_process";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";
import {fileURLToPath} from "node:url";

// The focused pane is exercised in an isolated vite dev server so it can be
// verified before dashboard/src/main.tsx wires it into the app shell. Every
// network call is stubbed with page.route; no real Hivemind store is touched.
const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
// 5197 stays clear of the timeline harness (5199) and the python fixture (8766).
const HARNESS_PORT = 5197;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/thread-pane.html`;
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

type MockPost = {
  id: string; revision: number; created_at: string; created_seq: number;
  board_id: string; thread_id: string; parent_post_id: string | null; kind: string;
  author: {kind: string; agent_id?: string; principal_id?: string};
  content: {media_type: string; text?: string;
    segments?: {kind: string; text?: string; agent_id?: string}[]};
  visibility?: string;
};

function buildThread(count: number, threadId = "thread-1", boardId = "board-1", label = "thread-1") {
  const posts: MockPost[] = [];
  for (let index = 0; index < count; index++) {
    posts.push({
      id: `post-${String(index).padStart(4, "0")}`, revision: 1,
      created_at: new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString(),
      created_seq: 1000 + index, board_id: boardId, thread_id: threadId,
      parent_post_id: index === 0 ? null : "post-0000",
      kind: index === 0 ? "question" : "answer",
      author: index % 2 === 0
        ? {kind: "human", principal_id: "principal-1"}
        : {kind: "agent", agent_id: "agent-droid0001"},
      content: {media_type: "text/plain",
        text: index === 0 ? `Root ${label}: confirm the navigation breakpoint` : `Reply ${index}`},
      visibility: "Shared board thread",
    });
  }
  return posts;
}

function threadPage(posts: MockPost[], url: URL) {
  const limit = Number(url.searchParams.get("limit") ?? "100");
  const before = url.searchParams.get("before");
  const descending = [...posts].sort((a, b) => b.created_seq - a.created_seq)
    .filter(post => before === null || post.created_seq < Number(before));
  const window = descending.slice(0, limit);
  const hasMore = descending.length > limit;
  const items = window.reverse();
  return {
    thread_id: items[0]?.thread_id ?? "thread-1", board_id: items[0]?.board_id ?? "board-1",
    project_id: "proj-1", root_post_id: items.find(post => !post.parent_post_id)?.id ?? null,
    items, count: items.length, has_more: hasMore,
    next_before: hasMore && items.length ? items[0].created_seq : null,
    snapshot_cursor: "a".repeat(64),
  };
}

type MockOptions = {
  posts?: MockPost[];
  mentionOptions?: unknown[];
  failThread?: boolean;
  onThreadRequest?: (url: string) => void;
};

async function installMocks(page: Page, options: MockOptions = {}) {
  let posts = options.posts ?? [];
  let lastRequest: Record<string, unknown> | null = null;
  let lastCreated: MockPost | null = null;
  await page.route("**/v1/streams**", route => route.abort());
  await page.route("**/v1/projects/*/mention-options**", route =>
    route.fulfill({json: {data: options.mentionOptions ?? []}}));
  await page.route("**/v1/boards/*/threads/*/posts**", async route => {
    options.onThreadRequest?.(route.request().url());
    if (options.failThread) {
      await route.fulfill({status: 500, json: {error: {message: "thread_unavailable"}}});
      return;
    }
    await route.fulfill({json: {data: threadPage(posts, new URL(route.request().url()))}});
  });
  await page.route("**/v1/boards/*/posts", async route => {
    const body = route.request().postDataJSON() as Record<string, unknown>;
    lastRequest = body;
    const created: MockPost = {
      id: `post-${String(posts.length).padStart(4, "0")}`, revision: 1,
      created_at: new Date(Date.UTC(2026, 0, 2)).toISOString(),
      created_seq: 1000 + posts.length,
      board_id: posts[0]?.board_id ?? "board-1", thread_id: String(body.thread_id),
      parent_post_id: (body.parent_post_id as string) ?? null, kind: String(body.kind),
      author: {kind: "human", principal_id: "principal-1"},
      content: body.content as MockPost["content"], visibility: "Shared board thread",
    };
    posts = [...posts, created];
    lastCreated = created;
    await route.fulfill({json: {data: created}});
  });
  return {lastPost: () => lastRequest as Record<string, any>,
    lastCreated: () => lastCreated as unknown as MockPost};
}

test.describe("focused thread pane (isolated harness)", () => {
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

  test("loads the whole authorized thread, not only the loaded timeline page", async ({page}) => {
    const requests: string[] = [];
    await installMocks(page, {posts: buildThread(250), onThreadRequest: url => requests.push(url)});
    await page.goto(`${HARNESS}?selected=post-0000`);
    const pane = page.getByTestId("thread-pane");
    await expect(pane).toBeVisible();
    await expect(pane.getByTestId("thread-post")).toHaveCount(250);
    // The root lives on the second page: the newest 200 posts fill page one.
    await expect(pane.locator('[data-post-id="post-0000"]')).toBeVisible();
    await expect(pane.locator('[data-post-id="post-0000"]')).toHaveAttribute("data-selected", "true");
    expect(requests.length).toBe(2);
    expect(requests[1]).toContain("before=");
    // The pane never replaces the main timeline.
    await expect(page.getByTestId("timeline-marker")).toBeVisible();
  });

  test("replies through the canonical board endpoint with thread and parent fences", async ({page}) => {
    const mocks = await installMocks(page, {posts: buildThread(3)});
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    await expect(pane.getByTestId("thread-post")).toHaveCount(3);
    await pane.locator('[data-post-id="post-0001"] [data-testid="thread-reply-target"]').click();
    await expect(pane.locator(".thread-reply-context")).toContainText("Replying to");
    await pane.getByLabel("Reply in thread").fill("Confirmed on mobile widths");
    await pane.getByRole("button", {name: "Post reply"}).click();
    await expect(pane.locator(".body").filter({hasText: "Confirmed on mobile widths"})).toBeVisible();
    const body = mocks.lastPost();
    expect(body.kind).toBe("answer");
    expect(body.thread_id).toBe("thread-1");
    expect(body.parent_post_id).toBe("post-0001");
    expect(body.content.segments).toEqual([{kind: "text", text: "Confirmed on mobile widths"}]);
    expect(String(body.client_post_id)).toMatch(/^[0-9a-f-]{36}$/);
    await expect(page.getByTestId("posted")).toContainText(mocks.lastCreated().id);
    await expect(pane.getByLabel("Reply in thread")).toHaveValue("");
  });

  test("mention chips reuse the canonical mention options and segments", async ({page}) => {
    const mocks = await installMocks(page, {
      posts: buildThread(1),
      mentionOptions: [{id: "agent-droid0001", display_name: "Droid worker", base_color_token: "teal"}],
    });
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    await pane.getByLabel("Reply in thread").fill("@Dro");
    // Shared MentionComposer contract: the pane must not fork the mention UI.
    await expect(pane.getByRole("listbox", {name: "Agent mentions"})).toBeVisible();
    await pane.getByRole("option", {name: /Droid worker/}).click();
    await expect(pane.locator(".chips")).toContainText("@Droid worker");
    await pane.getByLabel("Reply in thread").fill("please confirm");
    await pane.getByRole("button", {name: "Post reply"}).click();
    expect(mocks.lastPost().content.segments).toEqual([
      {kind: "agent_mention", agent_id: "agent-droid0001"}, {kind: "text", text: "please confirm"}]);
  });

  test("an ambiguous reply retries the frozen thread and parent without erasing a new draft", async ({page}) => {
    await installMocks(page, {posts: buildThread(3)});
    const attempts: {body: any; key: string | undefined}[] = [];
    await page.route("**/v1/boards/*/posts", async route => {
      attempts.push({body: route.request().postDataJSON(), key: route.request().headers()["idempotency-key"]});
      await route.abort("failed");
    });
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    await expect(pane.getByTestId("thread-post")).toHaveCount(3);
    await pane.getByLabel("Reply to post 2").click();
    await pane.getByLabel("Reply in thread").fill("Original failed reply");
    await pane.getByRole("button", {name: "Post reply", exact: true}).click();
    await expect(pane.getByRole("button", {name: "Retry", exact: true})).toBeVisible();
    await pane.getByLabel("Reply to post 3").click();
    await pane.getByLabel("Reply in thread").fill("New unsent reply");
    await pane.getByRole("button", {name: "Retry", exact: true}).click();
    await expect.poll(() => attempts.length).toBe(2);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(attempts[0].body).toMatchObject({thread_id: "thread-1", parent_post_id: "post-0001"});
    expect(attempts[0].key).toBe(attempts[0].body.client_post_id);
    await expect(pane.getByLabel("Reply in thread")).toHaveValue("New unsent reply");
    await expect(pane.getByTestId("thread-post")).toHaveCount(3);
  });

  test("unresolved reply text requires explicit plain-text confirmation", async ({page}) => {
    const mocks = await installMocks(page, {posts: buildThread(1)});
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    await expect(pane.getByTestId("thread-post")).toHaveCount(1);
    await pane.getByLabel("Reply in thread").fill("@Unselected reply");
    await pane.getByRole("button", {name: "Post reply", exact: true}).click();
    expect(mocks.lastPost()).toBeNull();
    await pane.getByRole("button", {name: "Send as plain text anyway"}).click();
    await pane.getByRole("button", {name: "Post reply", exact: true}).click();
    await expect.poll(() => mocks.lastPost()).not.toBeNull();
    expect(mocks.lastPost().content.segments).toEqual([{kind: "text", text: "@Unselected reply"}]);
  });

  test("highlight, escape and close keep the timeline mounted", async ({page}) => {
    await installMocks(page, {posts: buildThread(3)});
    await page.goto(`${HARNESS}?selected=post-0002`);
    const pane = page.getByTestId("thread-pane");
    await expect(pane.locator('[data-post-id="post-0002"]')).toHaveAttribute("data-selected", "true");
    await pane.getByLabel("Reply in thread").click();
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("thread-pane")).toHaveCount(0);
    await expect(page.getByTestId("timeline-marker")).toBeVisible();
    await page.getByRole("button", {name: "Open thread"}).click();
    await expect(page.getByTestId("thread-pane")).toBeVisible();
    await page.getByRole("button", {name: "Close thread pane"}).click();
    await expect(page.getByTestId("thread-pane")).toHaveCount(0);
    await expect(page.getByTestId("timeline-marker")).toBeVisible();
  });

  test("Escape dismisses mention suggestions before closing the thread", async ({page}) => {
    await installMocks(page, {posts: buildThread(1),
      mentionOptions: [{id: "agent-droid0001", display_name: "Droid worker"}]});
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    const composer = pane.getByLabel("Reply in thread");
    await composer.fill("@Droid");
    await expect(pane.getByRole("listbox", {name: "Agent mentions"})).toBeVisible();
    await composer.press("Escape");
    await expect(pane.getByRole("listbox", {name: "Agent mentions"})).toHaveCount(0);
    await expect(pane).toBeVisible();
    await composer.press("Escape");
    await expect(pane).toHaveCount(0);
  });

  test("permission loss on refresh clears thread evidence and the draft", async ({page}) => {
    await installMocks(page, {posts: buildThread(3)});
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    await expect(pane.getByTestId("thread-post")).toHaveCount(3);
    await pane.getByLabel("Reply in thread").fill("Draft from the authorized thread");
    await page.route("**/v1/boards/*/threads/*/posts**", route =>
      route.fulfill({status: 403, json: {error: {message: "permission_revoked"}}}));
    await pane.getByRole("button", {name: "Refresh thread"}).click();
    await expect(pane.getByTestId("thread-post")).toHaveCount(0);
    await expect(pane.getByLabel("Reply in thread")).toHaveValue("");
    await expect(pane.getByRole("button", {name: "Post reply"})).toBeDisabled();
  });

  test("a failed load clears stale content and project switching resets the pane", async ({page}) => {
    await installMocks(page, {failThread: true});
    await page.goto(HARNESS);
    const pane = page.getByTestId("thread-pane");
    await expect(pane.getByRole("alert")).toContainText("thread_unavailable");
    await expect(pane.getByTestId("thread-post")).toHaveCount(0);
    await pane.getByLabel("Reply in thread").fill("leftover draft from the failed thread");
    await page.unroute("**/v1/boards/*/threads/*/posts**");
    const second = buildThread(2, "thread-2", "board-proj-2", "thread-2");
    await page.route("**/v1/boards/*/threads/*/posts**", route =>
      route.fulfill({json: {data: threadPage(second, new URL(route.request().url()))}}));
    await page.getByRole("button", {name: "Switch project"}).click();
    await expect(pane.getByRole("alert")).toHaveCount(0);
    await expect(pane.getByLabel("Reply in thread")).toHaveValue("");
    await expect(pane.getByTestId("thread-post")).toHaveCount(2);
    await expect(pane.locator('[data-post-id="post-0000"]')).toContainText("Root thread-2");
  });
});

// ---------------------------------------------------------------------------
// Integration verifies the component and endpoint in the real app shell.
// ---------------------------------------------------------------------------
function fixture() {
  return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
}

async function login(page: Page) {
  const data = fixture();
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.getByRole("button", {name: "Main board", exact: true})).toBeVisible();
  await expect(page.locator("header .connection")).toHaveText("Live");
  return data;
}

test("integrated thread pane opens, replies and closes without leaving the board", async ({page, request}) => {
  const data = await login(page);
  const headers = {Authorization: `Bearer ${data.token}`, "Idempotency-Key": crypto.randomUUID()};
  const root = (await (await request.post(`/v1/boards/${data.board_id}/posts`, {headers,
    data: {client_post_id: crypto.randomUUID(), kind: "question",
      content: {media_type: "text/plain", text: "Integrated thread root"}}})).json()).data;
  await request.post(`/v1/boards/${data.board_id}/posts`, {headers: {...headers, "Idempotency-Key": crypto.randomUUID()},
    data: {client_post_id: crypto.randomUUID(), kind: "answer", parent_post_id: root.id, thread_id: root.thread_id,
      content: {media_type: "text/plain", text: "Integrated thread reply"}}});
  const probe = await request.get(`/v1/boards/${data.board_id}/threads/${root.thread_id}/posts`, {headers});
  expect(probe.ok()).toBeTruthy();
  await page.reload();
  const post = page.locator(".message").filter({hasText: "Integrated thread root"}).first();
  const trigger = post.locator("[data-thread-pane-trigger]").first();
  await expect(trigger).toBeVisible();
  await trigger.click();
  const pane = page.locator('[data-testid="thread-pane"]');
  await expect(pane).toBeVisible();
  await expect(pane.locator('[data-testid="thread-post"]')).toHaveCount(2);
  await expect(page.getByRole("button", {name: "Main board", exact: true})).toBeVisible();
  await pane.getByLabel("Reply in thread").fill("Reply from the focused pane");
  await pane.getByRole("button", {name: "Post reply"}).click();
  await expect(pane.locator(".body").filter({hasText: "Reply from the focused pane"})).toBeVisible();
  await page.getByRole("button", {name: "Close thread pane"}).click();
  await expect(pane).toHaveCount(0);
  await expect(page.getByRole("button", {name: "Main board", exact: true})).toBeVisible();
});
