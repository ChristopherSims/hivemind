import {test, expect, type Page} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";
import {
  boardBadgeLabel, boardBadgeTitle, boardUnreadQuery, BATCH_LIMIT, decorateBoardItems,
  markingBatches, MARKER_LIMIT, mergeSnapshotBoardUnread, SELF_POLICY, unreadAmongItems,
  type BoardUnreadState,
} from "../src/board-unread";

/*
 * Shared-board unread.
 *
 * Part 1 covers the pure contracts in-process (no network, no harness). Part 2
 * mounts the hook and components in an isolated vite dev server on port 5205,
 * clear of the thread-pane harness (5197), the timeline harness (5199), the
 * optimistic-posts harness (5201) and the python fixture (8766). Every request
 * is stubbed by an in-spec fake server that mirrors the canonical semantics of
 * hivemind/board_reads.py, so no real Hivemind store is touched and this spec
 * can run before the dashboard owner wires the feature into main.tsx.
 */

const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5205;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/board-unread.html`;
const CURSOR = "a".repeat(64);
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

function state(overrides: Partial<BoardUnreadState> = {}): BoardUnreadState {
  return {
    project_id: "proj-1", board_id: "board-1", board_title: "Main board",
    available: true, revision_aware: true, self_policy: SELF_POLICY, include_own: false,
    unread_count: 0, unread_post_ids: [], unread_markers_truncated: false, visible_count: 0,
    read_count: 0, authored_count: 0, own_excluded_count: 0, counts_complete: true,
    truncated: false, examined_count: 0, scan_limit: 2000, marker_limit: MARKER_LIMIT,
    read_digest: "d".repeat(64), generated_at: 1, snapshot_cursor: CURSOR,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Part 1: pure contracts
// ---------------------------------------------------------------------------

test.describe("board unread contracts", () => {
  test("a truncated count is never rendered as an exact number", () => {
    expect(boardBadgeLabel(0)).toBe("");
    expect(boardBadgeLabel(3)).toBe("3");
    expect(boardBadgeLabel(3, true)).toBe("3+");
    expect(boardBadgeTitle(3, false, true, "Main board")).toContain("3 unread posts in Main board");
    expect(boardBadgeTitle(3, true, true, "Main board")).toContain("More posts exist");
    expect(boardBadgeTitle(0, false, true, "Main board")).toContain("No unread posts");
    expect(boardBadgeTitle(5, false, false)).toContain("unavailable");
  });

  test("decoration never invents an unread marker while the state is unknown", () => {
    const items = [{id: "p1"}, {id: "p2"}];
    const unknown = decorateBoardItems(items, state({available: false, unread_count: 2,
      unread_post_ids: ["p1", "p2"]}));
    expect(unknown.map(item => item.board_unread)).toEqual([undefined, undefined]);
    expect(unknown.map(item => item.is_read)).toEqual([undefined, undefined]);

    const known = decorateBoardItems(items, state({unread_post_ids: ["p2"], unread_count: 1}));
    expect(known).toEqual([
      {id: "p1", is_read: true, board_unread: false},
      {id: "p2", is_read: false, board_unread: true},
    ]);
    // A bare id set (a host may already hold one) decorates the same way.
    expect(decorateBoardItems(items, new Set(["p1"]))[0].board_unread).toBe(true);
    expect(unreadAmongItems(items, state({available: false}))).toEqual([]);
    expect(unreadAmongItems(items, state({unread_post_ids: ["p2"]}))).toEqual(["p2"]);
  });

  test("marking batches cap at the server limit and never duplicate an id", () => {
    const ids = Array.from({length: 250}, (_, index) => `p${index}`);
    const batches = markingBatches([...ids, "p0"]);
    expect(batches.map(batch => batch.length)).toEqual([BATCH_LIMIT, BATCH_LIMIT, 50]);
    expect(batches.flat()).toEqual(ids);
    expect(markingBatches([])).toEqual([]);
  });

  test("only a valid snapshot cursor is ever bound to a request", () => {
    expect(boardUnreadQuery({})).toBe("");
    expect(boardUnreadQuery({cursor: "not-a-cursor"})).toBe("");
    expect(boardUnreadQuery({cursor: CURSOR})).toBe(`?snapshot_cursor=${CURSOR}`);
    expect(boardUnreadQuery({boardId: "board-1", includeOwn: true, markerLimit: 5}))
      .toBe("?board_id=board-1&include_own=true&marker_limit=5");
  });

  test("snapshot board badges never fabricate a zero for an unknown board", () => {
    const boards = [{id: "board-1", title: "Main board"}, {id: "board-2", title: "Second board"}];
    const summary = state({board_id: null, unread_count: 4, boards: [
      {board_id: "board-1", title: "Main board", unread_count: 4, visible_count: 9, counts_complete: false},
    ]});
    const merged = mergeSnapshotBoardUnread(boards, summary);
    expect(merged[0]).toMatchObject({id: "board-1", unread_count: 4, unread_truncated: true});
    expect(merged[1]).toMatchObject({id: "board-2", unread_count: undefined, unread_truncated: false});
    // An unavailable module leaves every board unknown rather than zero.
    const unavailable = mergeSnapshotBoardUnread(boards, state({available: false, boards: []}));
    expect(unavailable.every(board => board.unread_count === undefined)).toBe(true);
    expect(mergeSnapshotBoardUnread(boards, null).every(board => board.unread_count === undefined))
      .toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Part 2: hook and components against a fake canonical server
// ---------------------------------------------------------------------------

type FakePost = {id: string; revision: number; created_seq: number; text: string};

type Fake = {
  posts: FakePost[];
  read: Map<string, number>;
  /** One forced 409 before the next marking request succeeds. */
  driftOnce: boolean;
  unavailable: boolean;
  truncated: boolean;
  markerLimit: number;
  ownAuthored: string[];
  markRequests: {path: string; body: Record<string, unknown>}[],
  unreadGets: string[];
};

function fake(): Fake {
  return {posts: [], read: new Map(), driftOnce: false, unavailable: false, truncated: false,
    markerLimit: MARKER_LIMIT, ownAuthored: [], markRequests: [], unreadGets: []};
}

function buildPosts(fake: Fake, count: number, boardId = "board-1") {
  fake.posts = Array.from({length: count}, (_, index) => ({
    id: `${boardId}-post-${String(index).padStart(4, "0")}`, revision: 1,
    created_seq: 1000 + index, text: `Post ${index}`,
  }));
}

function unreadIds(fake: Fake, boardId: string) {
  return fake.posts.filter(post => post.id.startsWith(`${boardId}-`) && fake.read.get(post.id) !== post.revision)
    .sort((left, right) => right.created_seq - left.created_seq).map(post => post.id);
}

function boardState(fake: Fake, boardId: string): BoardUnreadState {
  const visible = fake.posts.filter(post => post.id.startsWith(`${boardId}-`));
  if (fake.unavailable) {
    // The canonical module reports availability, not a fabricated zero.
    return state({board_id: boardId, available: false, counts_complete: false,
      visible_count: visible.length, marker_limit: fake.markerLimit});
  }
  const unread = unreadIds(fake, boardId);
  const own = unread.filter(id => fake.ownAuthored.includes(id));
  const listed = own.length ? unread.filter(id => !own.includes(id)) : unread;
  const total = listed.length;
  const returned = listed.slice(0, fake.markerLimit);
  return state({
    board_id: boardId, available: !fake.unavailable, unread_count: total,
    unread_post_ids: returned, unread_markers_truncated: total > returned.length,
    visible_count: visible.length, read_count: visible.length - total,
    authored_count: own.length, own_excluded_count: own.length,
    counts_complete: !fake.unavailable && !fake.truncated, truncated: fake.truncated,
    examined_count: fake.truncated ? 2 : visible.length, marker_limit: fake.markerLimit,
  });
}

function projectState(fake: Fake, boardIds: string[]): BoardUnreadState {
  const perBoard = boardIds.map(boardId => boardState(fake, boardId));
  return state({
    board_id: null, board_title: null, available: !fake.unavailable,
    unread_count: perBoard.reduce((sum, item) => sum + item.unread_count, 0),
    unread_post_ids: perBoard.flatMap(item => item.unread_post_ids)
      .sort((left, right) => right.localeCompare(left)),
    visible_count: perBoard.reduce((sum, item) => sum + item.visible_count, 0),
    boards: perBoard.map(item => ({board_id: item.board_id!, title: item.board_id,
      unread_count: item.unread_count, visible_count: item.visible_count,
      counts_complete: item.counts_complete})),
    counts_complete: !fake.unavailable && !fake.truncated, truncated: fake.truncated,
  });
}

function markResult(fake: Fake, boardId: string, ids: string[]) {
  const marked: {post_id: string; revision: number}[] = [];
  const skipped: {post_id: string; reason: string}[] = [];
  let already = 0;
  for (const id of ids) {
    const post = fake.posts.find(candidate => candidate.id === id);
    if (!post || !id.startsWith(`${boardId}-`)) { skipped.push({post_id: id, reason: "not_found"}); continue; }
    if (fake.read.get(id) === post.revision) { already += 1; continue; }
    fake.read.set(id, post.revision);
    marked.push({post_id: id, revision: post.revision});
  }
  return {marked_count: marked.length, already_read_count: already, marked, skipped,
    project_id: "proj-1", board_id: boardId, state: boardState(fake, boardId),
    snapshot_cursor: CURSOR};
}

async function installFake(page: Page, fake: Fake) {
  await page.route(/\/v1\/streams/, route => route.abort());
  await page.route(/\/v1\/boards\?project_id=/, route => route.fulfill({json: {data: [
    {id: "board-1", title: "Main board"}, {id: "board-2", title: "Second board"},
  ]}}));
  await page.route(/\/v1\/boards\/[^/]+\/posts/, route => route.fulfill({json: {data: {
    items: fake.posts.map(post => ({id: post.id, revision: post.revision,
      created_seq: post.created_seq, content: {text: post.text}})),
    next_before: null, snapshot_cursor: CURSOR,
  }}}));
  await page.route(/\/v1\/boards\/[^/]+\/unread:read$/, async route => {
    const body = route.request().postDataJSON() as Record<string, unknown>;
    fake.markRequests.push({path: route.request().url(), body});
    if (fake.driftOnce) {
      fake.driftOnce = false;
      await route.fulfill({status: 409, json: {error: {code: "snapshot_changed",
        message: "snapshot changed", retryable: false}}});
      return;
    }
    const boardId = route.request().url().split("/v1/boards/")[1].split("/")[0];
    await route.fulfill({json: {data: markResult(fake, boardId, body.post_ids as string[])}});
  });
  await page.route(/\/v1\/projects\/[^/]+\/board-unread:read$/, async route => {
    const body = route.request().postDataJSON() as Record<string, unknown>;
    fake.markRequests.push({path: route.request().url(), body});
    const result = markResult(fake, "board-1", body.post_ids as string[]);
    await route.fulfill({json: {data: {...result, board_id: null}}});
  });
  await page.route(/\/v1\/boards\/[^/]+\/unread(\?|$)/, route => {
    fake.unreadGets.push(route.request().url());
    const boardId = route.request().url().split("/v1/boards/")[1].split("/")[0];
    return route.fulfill({json: {data: boardState(fake, boardId)}});
  });
  await page.route(/\/v1\/projects\/[^/]+\/board-unread(\?|$)/, route => {
    fake.unreadGets.push(route.request().url());
    return route.fulfill({json: {data: projectState(fake, ["board-1", "board-2"])}});
  });
}

function row(page: Page, postId: string) {
  return page.locator(`[data-testid="harness-post"][data-post-id="${postId}"]`);
}

function unreadRows(page: Page) {
  return page.locator('[data-testid="harness-post"][data-unread="true"]');
}

test.describe("shared board unread (isolated harness)", () => {
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

  test("viewing a board marks nothing read; only the explicit control writes", async ({page}) => {
    const server = fake();
    buildPosts(server, 3);
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-count")).toHaveText("3 unread");
    await expect(unreadRows(page)).toHaveCount(3);
    await expect(page.getByTestId("board-unread-mark")).toBeEnabled();
    expect(server.markRequests).toEqual([]);
    expect(server.read.size).toBe(0);
    const expected = unreadIds(server, "board-1");

    await page.getByTestId("board-unread-mark").click();
    await expect(page.getByTestId("board-unread-count")).toHaveText("0 unread");
    await expect(unreadRows(page)).toHaveCount(0);
    expect(server.markRequests.length).toBe(1);
    expect([...(server.markRequests[0].body.post_ids as string[])].sort()).toEqual([...expected].sort());
    // The marking request binds the snapshot the counts came from.
    expect(server.markRequests[0].body.snapshot_cursor).toBe(CURSOR);
    await expect(page.getByTestId("harness-marked")).toHaveText("3/0");
  });

  test("marking the loaded window is batched to 100 ids per request", async ({page}) => {
    const server = fake();
    buildPosts(server, 250);
    server.markerLimit = 250;
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-count")).toHaveText("250 unread");
    await expect(unreadRows(page)).toHaveCount(250);

    await page.getByTestId("board-unread-mark").click();
    await expect(page.getByTestId("board-unread-count")).toHaveText("0 unread");
    expect(server.markRequests.length).toBe(3);
    expect(server.markRequests.map(request => (request.body.post_ids as string[]).length))
      .toEqual([100, 100, 50]);
    expect(server.read.size).toBe(250);
  });

  test("a truncated count is shown as a lower bound, never as an exact number", async ({page}) => {
    const server = fake();
    buildPosts(server, 40);
    server.truncated = true;
    server.markerLimit = 5;
    await installFake(page, server);
    await page.goto(HARNESS);
    const count = page.getByTestId("board-unread-count");
    await expect(count).toHaveText("40+ unread");
    await expect(count).toHaveAttribute("data-complete", "false");
    await expect(page.getByTestId("board-unread-truncated")).toContainText("lower bound");
    // Only the listed ids are markable: the window is honest about its bound.
    await expect(page.getByTestId("board-unread-mark")).toContainText("(5)");
    await expect(unreadRows(page)).toHaveCount(5);
  });

  test("an unavailable backend reports unavailable instead of a false zero", async ({page}) => {
    const server = fake();
    buildPosts(server, 2);
    server.unavailable = true;
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-count")).toHaveText("Unread state unavailable");
    await expect(page.getByTestId("board-unread-count")).toHaveAttribute("data-complete", "false");
    await expect(unreadRows(page)).toHaveCount(0);
    await expect(page.getByTestId("board-unread-mark")).toBeDisabled();
    await expect(page.getByTestId("harness-unread-ids")).toHaveText("");
  });

  test("snapshot drift is retried once after a refresh, never silently ignored", async ({page}) => {
    const server = fake();
    buildPosts(server, 2);
    server.driftOnce = true;
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-count")).toHaveText("2 unread");
    const gets = server.unreadGets.length;

    await page.getByTestId("board-unread-mark").click();
    await expect(page.getByTestId("board-unread-count")).toHaveText("0 unread");
    // One refused attempt, one refetch of the state, one successful attempt.
    expect(server.markRequests.length).toBe(2);
    expect(server.unreadGets.length).toBeGreaterThan(gets);
    expect(server.read.size).toBe(2);
  });

  test("an edit makes a read post unread again for the same principal", async ({page}) => {
    const server = fake();
    buildPosts(server, 2);
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("board-unread-mark").click();
    await expect(page.getByTestId("board-unread-count")).toHaveText("0 unread");

    // A later revision of the same post invalidates the marker (revision-aware).
    const edited = server.posts[1];
    edited.revision = 2;
    await page.getByTestId("simulate-resync").click();
    await expect(page.getByTestId("board-unread-count")).toHaveText("1 unread");
    await expect(row(page, edited.id)).toHaveAttribute("data-unread", "true");
    expect(server.read.get(edited.id)).toBe(1);
  });

  test("the per-post control marks exactly one post", async ({page}) => {
    const server = fake();
    buildPosts(server, 3);
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(unreadRows(page)).toHaveCount(3);
    const target = server.posts[2].id;

    await row(page, target).getByTestId("board-post-read").click();
    await expect(row(page, target)).toHaveAttribute("data-unread", "false");
    await expect(unreadRows(page)).toHaveCount(2);
    expect(server.markRequests.length).toBe(1);
    expect(server.markRequests[0].body.post_ids).toEqual([target]);
    await expect(page.getByTestId("harness-marked")).toHaveText(`single:${target}:1`);
  });

  test("switching project or board never keeps the previous scope's counts", async ({page}) => {
    const server = fake();
    buildPosts(server, 3, "board-1");
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-count")).toHaveText("3 unread");

    await page.getByTestId("switch-board").click();
    await expect(page.getByTestId("harness-scope")).toHaveAttribute("data-board", "board-2");
    await expect(page.getByTestId("board-unread-count")).toHaveText("0 unread");
    await expect(unreadRows(page)).toHaveCount(0);

    await page.getByTestId("switch-project").click();
    await expect(page.getByTestId("harness-scope")).toHaveAttribute("data-project", "proj-2");
    await expect(page.getByTestId("board-unread-count")).toHaveText("0 unread");
  });

  test("a shared resync invalidation refetches counts without a second stream", async ({page}) => {
    const server = fake();
    buildPosts(server, 1);
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-count")).toHaveText("1 unread");
    const gets = server.unreadGets.length;

    buildPosts(server, 4);
    await page.getByTestId("simulate-resync").click();
    await expect(page.getByTestId("board-unread-count")).toHaveText("4 unread");
    expect(server.unreadGets.length).toBeGreaterThan(gets);
    // Only the stubbed stream route was ever contacted; no second live channel.
    expect(server.unreadGets.every(url => !url.includes("streams"))).toBe(true);
  });

  test("project badges come from the snapshot merge and stay unknown when absent", async ({page}) => {
    const server = fake();
    buildPosts(server, 2, "board-1");
    const firstBoard = server.posts;
    buildPosts(server, 2, "board-2");
    server.posts = [...firstBoard, ...server.posts];
    await installFake(page, server);
    await page.goto(HARNESS);
    const boardOne = page.locator('[data-testid="harness-board"][data-board-id="board-1"]');
    const boardTwo = page.locator('[data-testid="harness-board"][data-board-id="board-2"]');
    await expect(boardOne).toHaveAttribute("data-unread-count", "2");
    await expect(boardOne.getByTestId("board-unread-badge")).toHaveText("2");
    await expect(boardTwo).toHaveAttribute("data-unread-count", "2");
    await expect(boardTwo.getByTestId("board-unread-badge")).toHaveText("2");
  });

  test("the self-policy note states the explicit author rule", async ({page}) => {
    const server = fake();
    buildPosts(server, 2);
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("board-unread-self-policy")).toHaveText(
      "Your own posts are never counted as unread.");
    // An own-authored post is excluded from the count but reported honestly.
    server.ownAuthored = [server.posts[0].id];
    await page.getByTestId("board-unread-refresh").click();
    await expect(page.getByTestId("board-unread-self-policy")).toContainText("1 of your own post");
    await expect(page.getByTestId("board-unread-count")).toHaveText("1 unread");
  });
});
