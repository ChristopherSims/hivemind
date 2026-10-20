import {test, expect, type Page} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";
import {
  anchorOffset, buildLayout, indexAtOffset, restoredScrollTop, visibleRange,
} from "../src/virtual-timeline-math";

/*
 * VirtualTimeline coverage.
 *
 * The windowing rules are exercised against dashboard/tests/harness/… on a Vite
 * dev server started by this spec: a 1200+ row, variable-height dataset with
 * controls for prepending an "earlier page", appending live rows, clearing the
 * list, toggling loading, and changing the measured head slot. A final test
 * covers the real dashboard timeline once main.tsx renders it through
 * VirtualTimeline (it skips while dashboard/dist is not built from such a
 * main.tsx, because the fixture server serves the prebuilt bundle).
 */

const HARNESS_PORT = 5199;
const HARNESS_URL = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/virtual-timeline-harness.html`;
const CONTAINER = ".harness-timeline";
const ITEM = ".virtual-timeline-item";
const TALL_EVERY = 37;

type Controls = {
  prepend: (count: number) => void;
  append: (count: number) => void;
  clear: () => void;
  setLoading: (value: boolean) => void;
  setHeaderExtra: (value: boolean) => void;
};

let harness: ChildProcess | undefined;

async function waitForServer(url: string, timeout: number) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch {
      // Server not listening yet.
    }
    await new Promise(ready => setTimeout(ready, 250));
  }
  throw new Error(`harness server did not start: ${url}`);
}

async function useControls(page: Page, action: (controls: Controls) => void) {
  await page.evaluate(source => {
    const controls = (window as unknown as {harness?: Controls}).harness;
    if (!controls) throw new Error("harness controls are not installed");
    (new Function("controls", `return (${source})(controls)`) as (value: Controls) => void)(controls);
  }, action.toString());
}

/**
 * Scrolls the window and waits until the rendered window covers the new offset.
 * Scrolling is asynchronous in the component (the scroll event drives the
 * window), so reading the DOM before the window catches up would compare
 * against stale nodes.
 */
async function scrollTo(page: Page, offset: number | "bottom") {
  await page.locator(CONTAINER).evaluate((node, value) => {
    node.scrollTo(0, value === "bottom" ? node.scrollHeight : value);
  }, offset);
  await expect.poll(() => page.evaluate(selector => {
    const container = document.querySelector(selector) as HTMLElement;
    const items = Array.from(container.querySelectorAll<HTMLElement>(".virtual-timeline-item"));
    if (items.length === 0) return false;
    // Window start of 0 means the head slot (if any) is the content above the first item.
    if (Number(container.dataset.vtStart) === 0) return true;
    return items[0].getBoundingClientRect().top <= container.getBoundingClientRect().top + 1;
  }, CONTAINER)).toBe(true);
}

/** Scroll offset of the container plus the item sitting at the viewport top. */
async function anchor(page: Page, key?: string | null) {
  return page.evaluate(([selector, wanted]) => {
    const container = document.querySelector(selector) as HTMLElement;
    const containerTop = container.getBoundingClientRect().top;
    const items = Array.from(document.querySelectorAll<HTMLElement>(".virtual-timeline-item"));
    const first = items.find(node => node.getBoundingClientRect().bottom > containerTop + 1) ?? items[0];
    const current = (wanted ? items.find(node => node.dataset.vtKey === wanted) : undefined) ?? first;
    return {
      key: current?.dataset.vtKey ?? null,
      offset: current ? current.getBoundingClientRect().top - containerTop : 0,
      scrollTop: container.scrollTop,
    };
  }, [CONTAINER, key ?? null] as const);
}

async function scrollMetrics(page: Page) {
  return page.evaluate(selector => {
    const container = document.querySelector(selector) as HTMLElement;
    return {top: container.scrollTop, height: container.scrollHeight, client: container.clientHeight};
  }, CONTAINER);
}

test.describe("VirtualTimeline windowing", () => {
  test.slow();

  test.beforeAll(async () => {
    harness = spawn(process.execPath, [
      resolve("node_modules/vite/bin/vite.js"),
      "--host", "127.0.0.1", "--port", String(HARNESS_PORT), "--strictPort", "--clearScreen", "false",
    ], {cwd: resolve("."), stdio: "ignore"});
    await waitForServer(HARNESS_URL, 90000);
  });

  test.afterAll(() => {
    harness?.kill();
    harness = undefined;
  });

  test.beforeEach(async ({page}) => {
    await page.goto(HARNESS_URL);
    await expect(page.locator("[data-vt-total]")).toHaveAttribute("data-vt-total", "1200");
  });

  test("keeps the DOM bounded for 1200 variable-height rows and opens at the latest row", async ({page}) => {
    expect(await page.locator(ITEM).count()).toBeLessThan(40);
    expect(await page.locator(ITEM).count()).toBeGreaterThan(0);
    // The reader starts at the latest row, matching the previous scroll-to-latest behavior.
    const metrics = await scrollMetrics(page);
    expect(metrics.top).toBeGreaterThan(metrics.height - metrics.client - 5);
    await expect(page.locator('[data-vt-key="row-1199"]')).toBeVisible();
    await expect(page.locator('[data-vt-key="row-0"]')).toHaveCount(0);

    // Reading back to the start renders the first row and still bounds the DOM.
    await scrollTo(page, 0);
    await expect(page.locator('[data-vt-key="row-0"]')).toBeVisible();
    expect(await page.locator(ITEM).count()).toBeLessThan(40);
    await expect(page.locator('[data-vt-key="row-1199"]')).toHaveCount(0);
  });

  test("renders only unique keys while scrolling the whole history", async ({page}) => {
    for (const fraction of [0, 0.25, 0.5, 0.75, 1]) {
      const metrics = await scrollMetrics(page);
      await scrollTo(page, Math.round((metrics.height - metrics.client) * fraction));
      const rendered = await page.locator(ITEM).count();
      expect(rendered).toBeGreaterThan(0);
      expect(rendered).toBeLessThan(40);
      const keys = await page.locator(ITEM).evaluateAll(nodes => nodes.map(node => node.getAttribute("data-vt-key")));
      expect(new Set(keys).size).toBe(keys.length);
    }
  });

  test("measures variable item heights instead of trusting the estimate", async ({page}) => {
    await scrollTo(page, 0);
    await expect(page.locator('[data-vt-key="row-0"]')).toBeVisible();
    const heights = await page.locator(ITEM).evaluateAll(nodes => nodes.map(node => ({
      key: node.getAttribute("data-vt-key") ?? "",
      height: node.getBoundingClientRect().height,
    })));
    const tall = heights.filter(entry => Number(entry.key.split("-")[1]) % TALL_EVERY === 0);
    const short = heights.filter(entry => Number(entry.key.split("-")[1]) % TALL_EVERY !== 0);
    expect(tall.length).toBeGreaterThan(0);
    expect(short.length).toBeGreaterThan(0);
    expect(tall[0].height).toBeGreaterThan(280);
    expect(short[0].height).toBeLessThan(140);

    // Every rendered item sits exactly after the measured height of its predecessor,
    // which can only hold if measured heights (not the 140px estimate) drive offsets.
    await expect.poll(() => page.evaluate(() => {
      const items = Array.from(document.querySelectorAll<HTMLElement>(".virtual-timeline-item"));
      if (items.length < 4) return false;
      for (let index = 1; index < items.length; index++) {
        const previous = items[index - 1].getBoundingClientRect();
        const current = items[index].getBoundingClientRect();
        if (Math.abs(current.top - previous.top - previous.height) > 1.5) return false;
      }
      return true;
    })).toBe(true);
  });

  test("holds the reader's place when an earlier page is prepended", async ({page}) => {
    await scrollTo(page, 4000);
    const before = await anchor(page);
    expect(before.key).not.toBeNull();

    await useControls(page, controls => controls.prepend(300));
    await expect(page.locator("[data-vt-total]")).toHaveAttribute("data-vt-total", "1500");

    const after = await anchor(page, before.key);
    expect(after.key).toBe(before.key);
    expect(Math.abs(after.offset - before.offset)).toBeLessThan(2);
    // 300 rows were inserted above the anchor, so the scroll offset grew with them.
    expect(after.scrollTop - before.scrollTop).toBeGreaterThan(20000);
    expect(await page.locator(ITEM).count()).toBeLessThan(40);

    // The earlier page is reachable by scrolling back.
    await scrollTo(page, 0);
    await expect(page.locator('[data-vt-key="older-0"]')).toBeVisible();
  });

  test("follows live rows at the bottom and stays put when reading older rows", async ({page}) => {
    await scrollTo(page, "bottom");
    await expect(page.locator('[data-vt-key="row-1199"]')).toBeVisible();
    await useControls(page, controls => controls.append(5));
    await expect(page.locator("[data-vt-total]")).toHaveAttribute("data-vt-total", "1205");
    await expect(page.locator('[data-vt-key="newer-1204"]')).toBeVisible();
    await expect.poll(async () => {
      const metrics = await scrollMetrics(page);
      return metrics.height - metrics.client - metrics.top;
    }).toBeLessThanOrEqual(80);

    // Scrolled away from the bottom, a live update must not move the reader.
    await scrollTo(page, 5000);
    const before = await anchor(page);
    await useControls(page, controls => controls.append(5));
    await expect(page.locator("[data-vt-total]")).toHaveAttribute("data-vt-total", "1210");
    const after = await anchor(page, before.key);
    expect(Math.abs(after.offset - before.offset)).toBeLessThan(2);
    // No jump to the bottom, and the appended rows are not rendered while reading.
    expect(Math.abs(after.scrollTop - before.scrollTop)).toBeLessThan(200);
    await expect(page.locator('[data-vt-key="newer-1204"]')).toHaveCount(0);
  });

  test("re-windows on viewport resize without losing the anchor", async ({page}) => {
    await scrollTo(page, 6000);
    const before = await anchor(page);
    await page.setViewportSize({width: 900, height: 420});
    const small = (await scrollMetrics(page)).client;
    expect(small).toBeLessThan(520);
    await page.setViewportSize({width: 900, height: 1100});
    await expect.poll(async () => (await scrollMetrics(page)).client).toBeGreaterThan(small + 400);
    expect(await page.locator(ITEM).count()).toBeLessThan(60);
    const after = await anchor(page, before.key);
    expect(Math.abs(after.offset - before.offset)).toBeLessThan(4);
  });

  test("keeps the anchor when the measured head slot changes height", async ({page}) => {
    await scrollTo(page, 3000);
    const before = await anchor(page);
    await useControls(page, controls => controls.setHeaderExtra(true));
    await expect(page.locator(".header-extra")).toHaveText(/Expanded head slot/);
    const after = await anchor(page, before.key);
    expect(Math.abs(after.offset - before.offset)).toBeLessThan(2);
  });

  test("shows the empty and loading slots and recovers when items return", async ({page}) => {
    await useControls(page, controls => controls.clear());
    await expect(page.locator("[data-vt-total]")).toHaveAttribute("data-vt-total", "0");
    await expect(page.locator(ITEM)).toHaveCount(0);
    await expect(page.locator(".empty")).toHaveText("No rows loaded.");

    await useControls(page, controls => controls.setLoading(true));
    await expect(page.locator(".loading")).toBeVisible();
    await expect(page.locator(".empty")).toHaveCount(0);

    await useControls(page, controls => controls.setLoading(false));
    await useControls(page, controls => controls.append(40));
    await expect(page.locator("[data-vt-total]")).toHaveAttribute("data-vt-total", "40");
    // Recovering from an empty list follows the latest row, like the previous list.
    await expect(page.locator('[data-vt-key="newer-1239"]')).toBeVisible();
    expect(await page.locator(ITEM).count()).toBeLessThan(40);
    await expect(page.locator(".empty")).toHaveCount(0);
  });

  test("exposes list position and recovers focus instead of losing it", async ({page}) => {
    const list = page.locator(".virtual-timeline-items");
    await expect(list).toHaveAttribute("role", "list");
    const first = page.locator('[data-vt-key="row-1199"]');
    await expect(first).toHaveAttribute("role", "listitem");
    await expect(first).toHaveAttribute("aria-posinset", "1200");
    await expect(first).toHaveAttribute("aria-setsize", "1200");

    // A control inside the last row is reachable, then scrolls out of the window.
    await first.getByRole("button", {name: "Action row-1199"}).focus();
    expect(await page.evaluate(() => document.activeElement?.textContent)).toBe("Action row-1199");
    await scrollTo(page, 0);
    await expect(page.locator('[data-vt-key="row-1199"]')).toHaveCount(0);
    await expect.poll(() => page.evaluate(() => document.activeElement?.classList.contains("virtual-timeline") ?? false)).toBe(true);
  });
});

test.describe("VirtualTimeline math", () => {
  test("builds cumulative offsets from measured and estimated heights", () => {
    const layout = buildLayout(["a", "b", "c"], new Map([["b", 50]]), 20);
    expect(layout.offsets).toEqual([0, 20, 70]);
    expect(layout.heights).toEqual([20, 50, 20]);
    expect(layout.total).toBe(90);
  });

  test("windows only the visible range plus overscan", () => {
    const keys = Array.from({length: 1000}, (_, index) => `k${index}`);
    const layout = buildLayout(keys, new Map(), 100);
    const range = visibleRange(layout.offsets, layout.heights, 50_000, 700, 200);
    expect(range.start).toBeGreaterThan(400);
    expect(range.end - range.start).toBeLessThan(30);
    expect(layout.offsets[range.start]).toBeLessThanOrEqual(50_000);
    expect(layout.offsets[range.end - 1] + layout.heights[range.end - 1]).toBeGreaterThanOrEqual(50_700);
    expect(visibleRange([], [], 0, 100, 10)).toEqual({start: 0, end: 0});
  });

  test("restores the anchor offset when earlier items are prepended", () => {
    const base = buildLayout(["a", "b", "c"], new Map(), 100);
    const anchor = anchorOffset(base, 0, 250);
    expect(anchor).toEqual({key: "c", delta: 50});
    const next = buildLayout(["older", "a", "b", "c"], new Map(), 100);
    expect(restoredScrollTop(next, 0, anchor!, indexAtOffset(base.offsets, 250))).toBe(350);
    // A vanished anchor falls back to the nearest surviving index.
    expect(restoredScrollTop(next, 0, {key: "gone", delta: 10}, 0)).toBe(10);
    expect(restoredScrollTop(next, 30, {key: "b", delta: 0}, 0)).toBe(230);
  });

  test("clamps indexes for empty and out-of-range positions", () => {
    expect(indexAtOffset([], 10)).toBe(-1);
    expect(indexAtOffset([0, 100, 200], -50)).toBe(0);
    expect(indexAtOffset([0, 100, 200], 500)).toBe(2);
    expect(anchorOffset(buildLayout([], new Map(), 10), 0, 0)).toBeNull();
  });
});

/* The fixture server serves dashboard/dist, so this runs only after the app is
   rebuilt with main.tsx rendering the timeline through VirtualTimeline. */
function fixture() {
  return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
}

test.describe("dashboard timeline integration", () => {
  test("the main timeline windows loaded history, keeps the latest row, and reads earlier pages", async ({page, request}) => {
    const data = fixture();
    const headers = {Authorization: `Bearer ${data.token}`};
    for (let index = 0; index < 120; index++) {
      const response = await request.post(`/v1/boards/${data.board_id}/posts`, {headers, data: {
        client_post_id: crypto.randomUUID(),
        content: {media_type: "text/plain", text: `Virtual timeline row ${String(index).padStart(4, "0")}`},
      }});
      expect(response.ok()).toBeTruthy();
    }

    await page.goto("/");
    await page.getByLabel("Local human credential").fill(data.token);
    await page.getByRole("button", {name: "Open workspace"}).click();
    await expect(page.locator("header .connection")).toHaveText("Live");

    const timeline = page.locator(".timeline");
    await expect(timeline).toHaveAttribute("data-vt-total", /\d+/);
    await page.getByLabel("Search this view", {exact: true}).fill("Virtual timeline row");
    await page.getByRole("button", {name: "Search", exact: true}).click();
    await expect(timeline).toHaveAttribute("data-vt-total", "100");

    await expect(page.getByText("Virtual timeline row 0119", {exact: true})).toBeVisible();
    expect(await page.locator(".timeline .message").count()).toBeLessThan(60);

    await page.getByRole("button", {name: "Load earlier posts", exact: true}).click();
    await expect.poll(async () => Number(await timeline.getAttribute("data-vt-total"))).toBeGreaterThan(100);
    expect(await page.locator(".timeline .message").count()).toBeLessThan(60);
    await timeline.evaluate(node => node.scrollTo(0, node.scrollHeight));
    await expect(page.getByText("Virtual timeline row 0119", {exact: true})).toBeVisible();

    await timeline.evaluate(node => node.scrollTo(0, 0));
    await expect(page.getByText("Virtual timeline row 0000", {exact: true})).toBeVisible();
    expect(await page.locator(".timeline .message").count()).toBeLessThan(60);

    await page.setViewportSize({width: 390, height: 844});
    await expect(page.getByText("Virtual timeline row 0000", {exact: true})).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
  });
});
