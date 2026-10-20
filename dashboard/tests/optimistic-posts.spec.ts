import {test, expect, type Page, type Route} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {
  applyOptimisticConfirmation, applyOptimisticFailure, classifyOptimisticError, freezePayload,
  isOptimisticPost, mergeOptimistic, OPTIMISTIC_MEDIA_TYPE, optimisticKeyOf, reconcileRecords,
  syntheticRow, type OptimisticPhase, type OptimisticRecord, type PostItem,
} from "../src/useOptimisticPosts";
import {ApiError} from "../src/api";

/*
 * Optimistic board posting.
 *
 * Part 1 covers the pure merge/reconcile/classify helpers in-process. Part 2
 * mounts the hook and row component in an isolated vite dev server on port 5201
 * (clear of the timeline harness 5199, the thread-pane harness 5197 and the
 * python fixture on 8766) and stubs every `/v1/boards/**` call with page.route,
 * so no Hivemind store is touched.
 */

const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5201;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/optimistic-posts.html`;
const CONTEXT = "proj-1\u0000board-1\u0000session-1";

let harness: ChildProcess | null = null;

type HarnessRow = {kind: "canonical" | "optimistic"; id: string; key: string;
  phase?: string; client_post_id?: string; text?: string; ambiguous?: boolean};
type HarnessState = {rows: HarnessRow[]; draft: string; pendingCount: number; failedCount: number;
  authCalls: number; project: string; board: string};
type HarnessControls = {
  send: (text: string, mentions?: string[]) => string | null;
  retry: (client_post_id: string) => void;
  cancel: (client_post_id: string) => void;
  setBoard: (board: string) => void;
  setSession: (key: string) => void;
  addCanonical: (post: PostItem) => void;
  clearCanonical: () => void;
  setDraft: (text: string) => void;
  state: () => HarnessState;
};

function item(id: string, created_seq: number, text: string, client_post_id?: string): PostItem {
  return {id, revision: 1, created_at: "2026-01-01T00:00:00.000Z", created_seq,
    thread_id: `thread-${id}`, content: {text}, author: {kind: "human"},
    ...(client_post_id ? {client_post_id} : {})};
}

function record(client_post_id: string, order: number, text: string,
                phase: OptimisticPhase | "confirmed" = "pending",
                contextKey = CONTEXT): OptimisticRecord<PostItem> {
  return {client_post_id, contextKey, project: "proj-1", board: "board-1", order, phase,
    payload: freezePayload(client_post_id, {content: {segments: [{kind: "text", text}]}}),
    created_at: "2026-01-01T00:00:00.000Z", attempt: 1};
}

test.describe("optimistic posting helpers", () => {
  test("freezePayload builds the canonical body and freezes it", () => {
    const payload = freezePayload("cpid-1", {content: {segments: [{kind: "text", text: "hi"}]}});
    expect(payload).toMatchObject({client_post_id: "cpid-1", kind: "message",
      content: {media_type: OPTIMISTIC_MEDIA_TYPE, segments: [{kind: "text", text: "hi"}]}});
    expect(Object.isFrozen(payload)).toBe(true);
    expect(Object.isFrozen(payload.content)).toBe(true);
  });

  test("retry payload owns a deep-frozen copy of the draft segments", () => {
    const segments: {kind: "text"; text: string}[] = [{kind: "text", text: "original"}];
    const payload = freezePayload("cpid-copy", {content: {segments}});
    segments[0].text = "edited";
    segments.push({kind: "text", text: "extra"});
    expect(payload.content.segments).toEqual([{kind: "text", text: "original"}]);
    expect(Object.isFrozen(payload.content.segments)).toBe(true);
    expect(Object.isFrozen(payload.content.segments![0])).toBe(true);
  });

  test("a fresh authoritative snapshot cannot resurrect a hidden confirmed post", () => {
    const confirmed = applyOptimisticConfirmation(
      [{...record("cpid-x", 1, "hidden"), snapshotVersion: 1}],
      "cpid-x", item("post-10", 21, "hidden", "cpid-x"), "http",
    );
    expect(mergeOptimistic([], confirmed, CONTEXT, 1)).toHaveLength(1);
    expect(mergeOptimistic([], confirmed, CONTEXT, 2)).toEqual([]);
    const updated = item("post-10", 21, "current revision", "cpid-x");
    expect(mergeOptimistic([updated], confirmed, CONTEXT, 2)).toEqual([updated]);
  });

  test("classifies failures: 4xx definitive, 5xx and network ambiguous", () => {
    expect(classifyOptimisticError(new ApiError(403, "forbidden", "forbidden")))
      .toMatchObject({status: 403, code: "forbidden", ambiguous: false});
    expect(classifyOptimisticError(new ApiError(409, "conflict", "snapshot_changed")).ambiguous).toBe(false);
    expect(classifyOptimisticError(new ApiError(500, "boom", "server_error")).ambiguous).toBe(true);
    expect(classifyOptimisticError(new TypeError("Failed to fetch")))
      .toMatchObject({ambiguous: true, code: "network_error"});
  });

  test("appends synthetic rows after canonical rows with distinct send-ordered sequences", () => {
    const canonical = [item("post-1", 10, "first"), item("post-2", 11, "second")];
    const merged = mergeOptimistic(canonical, [record("cpid-a", 1, "a"), record("cpid-b", 2, "b")], CONTEXT);
    expect(merged.map(row => row.id))
      .toEqual(["post-1", "post-2", "optimistic:cpid-a", "optimistic:cpid-b"]);
    // max(canonical.created_seq) + record.order => 12, 13: same-tick sends still sort.
    expect(merged.slice(2).map(row => row.created_seq)).toEqual([12, 13]);
    expect(merged.slice(2).every(isOptimisticPost)).toBe(true);
  });

  test("a canonical row carrying the client_post_id replaces the pending row exactly once", () => {
    const canonical = [item("post-9", 20, "other"), item("post-10", 21, "hello", "cpid-x")];
    const records = [record("cpid-x", 1, "hello")];
    const merged = mergeOptimistic(canonical, records, CONTEXT);
    expect(merged.map(row => row.id)).toEqual(["post-9", "post-10"]);
    // One row for the send, and the key is unchanged from the pending row's key.
    expect(merged.filter(row => (row as PostItem).client_post_id === "cpid-x").length).toBe(1);
    expect(optimisticKeyOf(merged[1], records, CONTEXT)).toBe("optimistic:cpid-x");
    expect(optimisticKeyOf(syntheticRow(records[0], 22), records, CONTEXT)).toBe("optimistic:cpid-x");
    expect(syntheticRow(records[0], 22).id).toBe("optimistic:cpid-x");
  });

  test("reconciles a snapshot confirmation without mutating the frozen payload", () => {
    const records = [record("cpid-x", 1, "hello")];
    const next = reconcileRecords(records, [item("post-10", 21, "hello", "cpid-x")], CONTEXT);
    expect(next[0].phase).toBe("confirmed");
    expect(next[0].canonicalId).toBe("post-10");
    expect(next[0].confirmSource).toBe("stream");
    expect(next[0].payload.client_post_id).toBe("cpid-x");
    expect(Object.isFrozen(next[0].payload)).toBe(true);
    // Identity-stable when nothing changes, so it is safe inside an effect.
    expect(reconcileRecords(next, [item("post-10", 21, "hello", "cpid-x")], CONTEXT)).toBe(next);
  });

  test("confirmation wins over a late ambiguous failure; cancel is identity-stable", () => {
    const confirmed = applyOptimisticConfirmation([record("cpid-x", 1, "hello")], "cpid-x",
      item("post-10", 21, "hello", "cpid-x"), "http");
    expect(confirmed[0].phase).toBe("confirmed");
    expect(applyOptimisticFailure(confirmed, "cpid-x",
      {message: "Failed to fetch", ambiguous: true})).toBe(confirmed);
    const pending = [record("cpid-y", 1, "y")];
    expect(applyOptimisticConfirmation(pending, "missing", item("post-1", 1, "z"), "http")).toBe(pending);
    expect(applyOptimisticFailure(pending, "missing",
      {message: "x", ambiguous: true})).toBe(pending);
  });

  test("rows from another project/board/session are never merged or keyed", () => {
    const canonical = [item("post-1", 10, "one", "cpid-o")];
    const foreign = [record("cpid-o", 1, "one", "pending", "proj-2\u0000board-9\u0000session-1")];
    expect(mergeOptimistic(canonical, foreign, CONTEXT)).toEqual(canonical);
    expect(optimisticKeyOf(canonical[0], foreign, CONTEXT)).toBe("post-1");
  });
});

/* ------------------------------------------------------------------ */
/* Harness wiring                                                      */
/* ------------------------------------------------------------------ */

async function waitForHarness() {
  for (let attempt = 0; attempt < 80; attempt++) {
    try {
      if ((await fetch(HARNESS)).ok) return;
    } catch {
      // Dev server still starting.
    }
    await new Promise(ready => setTimeout(ready, 500));
  }
  throw new Error(`Harness server did not start at ${HARNESS}`);
}

type PostBehaviour = "hold" | "accept" | "abort" | "error500" | "forbidden";

/** Stubs POST boards/{id}/posts with a controllable outcome and records what was sent. */
async function installPostRouter(page: Page) {
  const captured: {body: Record<string, unknown>; idempotencyKey: string | undefined}[] = [];
  const held: {route: Route; body: Record<string, unknown>}[] = [];
  let behaviour: PostBehaviour = "accept";
  let sequence = 0;

  const canonicalize = (body: Record<string, unknown>) => ({
    id: `post-${++sequence}`, revision: 1,
    created_at: new Date(Date.UTC(2026, 0, 1, 0, 0, sequence)).toISOString(),
    created_seq: 100 + sequence, thread_id: `thread-${sequence}`,
    kind: body.kind ?? "message", author: {kind: "human", principal_id: "principal-1"},
    content: body.content, client_post_id: body.client_post_id,
  });

  await page.route("**/v1/boards/**/posts", async route => {
    if (route.request().method() !== "POST") return route.continue();
    const body = route.request().postDataJSON() as Record<string, unknown>;
    captured.push({body, idempotencyKey: route.request().headers()["idempotency-key"]});
    if (behaviour === "hold") { held.push({route, body}); return; }
    if (behaviour === "abort") return route.abort("failed");
    if (behaviour === "error500") return route.fulfill({status: 500, contentType: "application/json",
      body: JSON.stringify({error: {code: "server_error", message: "boom"}})});
    if (behaviour === "forbidden") return route.fulfill({status: 403, contentType: "application/json",
      body: JSON.stringify({error: {code: "forbidden", message: "forbidden"}})});
    return route.fulfill({json: {data: canonicalize(body)}});
  });

  return {
    captured,
    setBehaviour: (next: PostBehaviour) => { behaviour = next; },
    release: async () => {
      for (const entry of held.splice(0)) {
        await entry.route.fulfill({json: {data: canonicalize(entry.body)}});
      }
    },
  };
}

function sendPost(page: Page, text: string): Promise<string | null> {
  return page.waitForFunction(() => Boolean((window as unknown as {optimisticHarness?: unknown}).optimisticHarness))
    .then(() => page.evaluate(value =>
      (window as unknown as {optimisticHarness: HarnessControls}).optimisticHarness.send(value), text));
}

async function harnessState(page: Page): Promise<HarnessState> {
  await page.waitForFunction(() => Boolean((window as unknown as {optimisticHarness?: unknown}).optimisticHarness));
  return page.evaluate(() =>
    (window as unknown as {optimisticHarness: HarnessControls}).optimisticHarness.state());
}

function renderedKeys(page: Page): Promise<(string | null)[]> {
  return page.locator(".virtual-timeline-item")
    .evaluateAll(nodes => nodes.map(node => node.getAttribute("data-vt-key")));
}

test.describe("optimistic posting (isolated harness)", () => {
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

  test("shows a pending row immediately, with the frozen idempotency key and no canonical actions", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("hold");
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Confirm the navigation breakpoint");
    expect(client_post_id).toBeTruthy();

    const row = page.locator(`[data-client-post-id="${client_post_id}"]`);
    await expect(row).toBeVisible();
    await expect(row).toHaveAttribute("data-optimistic-phase", "pending");
    await expect(row).toContainText("Sending…");
    // The key is the synthetic row id, and it is the only row so far.
    await expect(page.locator(`[data-vt-key="optimistic:${client_post_id}"]`)).toHaveCount(1);
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(1);
    // A pending row is not a canonical post: no reply/read controls, no unread bookkeeping.
    await expect(page.locator(".reply-button")).toHaveCount(0);
    await expect(page.locator("[data-optimistic-phase] .delivery-detail")).toHaveCount(0);
    const state = await harnessState(page);
    expect(state.pendingCount).toBe(1);
    expect(state.failedCount).toBe(0);
    expect(state.draft).toBe("Confirm the navigation breakpoint");

    // The body and header carry the same stable client_post_id.
    expect(router.captured).toHaveLength(1);
    expect(router.captured[0].body.client_post_id).toBe(client_post_id);
    expect(router.captured[0].idempotencyKey).toBe(client_post_id);
    // No persistence of the draft anywhere.
    expect(await page.evaluate(() => [localStorage.length, sessionStorage.length])).toEqual([0, 0]);

    // Server acceptance replaces the row without changing its key.
    await router.release();
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.locator(`[data-vt-key="optimistic:${client_post_id}"]`)).toHaveCount(1);
    await expect(page.locator(".canonical-post")).toContainText("Confirm the navigation breakpoint");
    expect((await harnessState(page)).draft).toBe("");
  });

  test("a snapshot landing before the HTTP response never produces a duplicate", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("hold");
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Racing snapshot");
    await expect(page.locator(`[data-client-post-id="${client_post_id}"]`)).toBeVisible();

    // SSE/snapshot arrives first, already carrying the same client_post_id.
    await page.evaluate(id => (window as unknown as {optimisticHarness: HarnessControls})
      .optimisticHarness.addCanonical({id: "post-sse", revision: 1,
        created_at: "2026-01-02T00:00:00.000Z", created_seq: 200, thread_id: "thread-sse",
        content: {segments: [{kind: "text", text: "Racing snapshot"}]},
        author: {kind: "human"}, client_post_id: id} as PostItem), client_post_id);

    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.locator(`[data-vt-key="optimistic:${client_post_id}"]`)).toHaveCount(1);
    await expect(page.getByText("Racing snapshot", {exact: true})).toHaveCount(1);

    // The HTTP response lands afterwards; still exactly one row.
    await router.release();
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    await expect(page.getByText("Racing snapshot", {exact: true})).toHaveCount(1);
  });

  test("a network failure is ambiguous, never auto-resends, and retries the same frozen payload", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("abort");
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Maybe committed");
    const row = page.locator(`[data-client-post-id="${client_post_id}"]`);
    await expect(row).toHaveAttribute("data-optimistic-phase", "failed");
    await expect(row).toContainText("may already have this post");
    await expect(row.getByRole("button", {name: "Retry"})).toBeVisible();
    await expect(row.getByRole("button", {name: "Cancel"})).toBeVisible();
    // No automatic resend: the failure sits until the reader acts.
    await page.waitForTimeout(400);
    expect(router.captured).toHaveLength(1);
    expect((await harnessState(page)).rows[0].ambiguous).toBe(true);

    router.setBehaviour("accept");
    await row.getByRole("button", {name: "Retry"}).click();
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.locator(`[data-vt-key="optimistic:${client_post_id}"]`)).toHaveCount(1);
    expect(router.captured).toHaveLength(2);
    expect(router.captured[1].body).toEqual(router.captured[0].body);
    expect(router.captured[1].body.client_post_id).toBe(client_post_id);
    expect(router.captured[1].idempotencyKey).toBe(client_post_id);
  });

  test("a snapshot that includes the confirmed post does not add a second row", async ({page}) => {
    const router = await installPostRouter(page);
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Posted once");
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    // The route stub returns the post under a stable id for the first accepted send.
    await page.evaluate(([id, cpid]) => (window as unknown as {optimisticHarness: HarnessControls})
      .optimisticHarness.addCanonical({id: id as string, revision: 1,
        created_at: "2026-01-03T00:00:00.000Z", created_seq: 300, thread_id: "thread-sse",
        content: {segments: [{kind: "text", text: "Posted once"}]},
        author: {kind: "human"}, client_post_id: cpid as string} as PostItem),
      ["post-1", client_post_id]);
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.getByText("Posted once", {exact: true})).toHaveCount(1);
    await expect(page.locator(`[data-vt-key="optimistic:${client_post_id}"]`)).toHaveCount(1);
  });

  test("switching board while a send is in flight fences the stale response", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("hold");
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Board one only");
    await expect(page.locator(`[data-client-post-id="${client_post_id}"]`)).toHaveCount(1);

    await page.evaluate(() => (window as unknown as {optimisticHarness: HarnessControls})
      .optimisticHarness.setBoard("board-2"));
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    await expect(page.getByText("Board one only", {exact: true})).toHaveCount(0);
    await page.evaluate(() => (window as unknown as {optimisticHarness: HarnessControls})
      .optimisticHarness.setDraft("New draft on board two"));

    await router.release();
    await page.waitForTimeout(300);
    const state = await harnessState(page);
    expect(state.board).toBe("board-2");
    expect(state.rows).toHaveLength(0);
    // The stale confirmation can neither commit nor wipe the newer draft.
    expect(state.draft).toBe("New draft on board two");
  });

  test("permission loss fails the row through the shared authorization handler", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("forbidden");
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Not permitted");
    const row = page.locator(`[data-client-post-id="${client_post_id}"]`);
    await expect(row).toHaveAttribute("data-optimistic-phase", "failed");
    await expect(row).toContainText("Not posted");
    await expect(row).not.toContainText("may already have this post");
    const state = await harnessState(page);
    expect(state.authCalls).toBe(1);
    expect(state.failedCount).toBe(1);
  });

  test("a silent request becomes an unconfirmed failure and still reconciles when it lands", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("hold");
    await page.goto(`${HARNESS}?timeout=800`);
    const client_post_id = await sendPost(page, "Slow board");
    const row = page.locator(`[data-client-post-id="${client_post_id}"]`);
    await expect(row).toHaveAttribute("data-optimistic-phase", "failed", {timeout: 5000});
    await expect(row).toContainText("may already have this post");
    // The request is still in flight; the late response still reconciles the row.
    await router.release();
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    await expect(page.locator(".canonical-post")).toHaveCount(1);
    await expect(page.locator(`[data-vt-key="optimistic:${client_post_id}"]`)).toHaveCount(1);
  });

  test("cancel drops the failed row locally without touching the board", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("abort");
    await page.goto(HARNESS);
    const client_post_id = await sendPost(page, "Cancel me");
    const row = page.locator(`[data-client-post-id="${client_post_id}"]`);
    await expect(row).toHaveAttribute("data-optimistic-phase", "failed");
    await row.getByRole("button", {name: "Cancel"}).click();
    await expect(page.locator(`[data-client-post-id="${client_post_id}"]`)).toHaveCount(0);
    const state = await harnessState(page);
    expect(state.rows).toHaveLength(0);
    expect(state.failedCount).toBe(0);
    expect(router.captured).toHaveLength(1);
  });

  test("multiple sends stay ordered while pending and keep their keys once confirmed", async ({page}) => {
    const router = await installPostRouter(page);
    router.setBehaviour("hold");
    await page.goto(HARNESS);
    const first = await sendPost(page, "First pending");
    const second = await sendPost(page, "Second pending");
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(2);
    expect(await renderedKeys(page)).toEqual([`optimistic:${first}`, `optimistic:${second}`]);
    expect((await harnessState(page)).pendingCount).toBe(2);

    await router.release();
    await expect(page.locator(".canonical-post")).toHaveCount(2);
    await expect(page.locator("[data-optimistic-phase]")).toHaveCount(0);
    expect(await renderedKeys(page)).toEqual([`optimistic:${first}`, `optimistic:${second}`]);
    const state = await harnessState(page);
    expect(state.pendingCount).toBe(0);
    expect(state.rows.map(row => row.text)).toEqual(["First pending", "Second pending"]);
  });
});
