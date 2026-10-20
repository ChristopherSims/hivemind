import {test, expect, Page} from "@playwright/test";
import {spawn, ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";

// The optional agent-tab prefill control is exercised in an isolated vite dev
// server so it can be verified before dashboard/src/main.tsx wires it into the
// shared-board composer. Every network call is stubbed with page.route, and the
// host side (draft, chips, openAgent) is emulated by the harness.
const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
// 5204 stays clear of the thread-pane harness (5197), the timeline harness
// (5199) and the python fixture (8766). Playwright runs these specs serially
// (workers: 1), so no fixture server is ever started twice at once.
const HARNESS_PORT = 5204;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/agent-prefill.html`;
const MENTION_OPTIONS = "**/v1/projects/*/mention-options*";
const AGENT_ID = "agent-droid0001";
const AGENT_NAME = "Droid worker";
const INITIAL_DRAFT = "My own notes stay untouched";

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

type OptionRow = {id: string; display_name: string; base_color_token?: string};

type StubOptions = {
  rows?: OptionRow[] | ((url: URL) => OptionRow[] | Promise<OptionRow[]>);
  status?: number;
  delayMs?: number;
};

type Stub = {
  /** Every GET the control made to the mention-options endpoint. */
  requests: {url: string; method: string; query: string; board: string}[];
  /** Any other /v1/ traffic, which must always stay empty. */
  other: {url: string; method: string}[];
};

function row(id = AGENT_ID, display_name = AGENT_NAME): OptionRow {
  return {id, display_name, base_color_token: "teal"};
}

async function installStub(page: Page, options: StubOptions = {}): Promise<Stub> {
  const requests: Stub["requests"] = [];
  const other: Stub["other"] = [];
  page.on("request", request => {
    const url = new URL(request.url());
    if (!url.pathname.startsWith("/v1/")) return;
    if (url.pathname.endsWith("/mention-options")) return;
    other.push({url: request.url(), method: request.method()});
  });
  await page.route(MENTION_OPTIONS, async route => {
    const url = new URL(route.request().url());
    requests.push({
      url: route.request().url(),
      method: route.request().method(),
      query: url.searchParams.get("query") ?? "",
      board: url.searchParams.get("board_id") ?? "",
    });
    if (options.delayMs) await new Promise(resolve => setTimeout(resolve, options.delayMs));
    if (options.status && options.status >= 400) {
      await route.fulfill({status: options.status, json: {error: {code: "forbidden", message: "no"}}});
      return;
    }
    const rows = typeof options.rows === "function" ? await options.rows(url) : options.rows ?? [row()];
    await route.fulfill({status: 200, json: {data: rows}});
  });
  return {requests, other};
}

const prefill = (page: Page) => page.locator("[data-agent-prefill]");
const toggle = (page: Page) => page.locator("[data-agent-prefill-toggle]");
const addButton = (page: Page) => page.locator("[data-agent-prefill-add]");
const note = (page: Page) => page.locator("[data-agent-prefill-note]");
const chips = (page: Page) => page.locator("[data-chip-id]");

test.describe("agent tab mention prefill (isolated harness)", () => {
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

  test("prefill is off by default and viewing or switching context never notifies", async ({page}) => {
    const stub = await installStub(page);
    await page.goto(HARNESS);

    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "off");
    await expect(toggle(page)).not.toBeChecked();
    await expect(addButton(page)).toHaveCount(0);
    await expect(note(page)).toContainText("Viewing or switching tabs never notifies anyone");

    await page.getByTestId("switch-board").click();
    await page.getByTestId("switch-project").click();
    await page.getByTestId("switch-view").click();
    await page.getByTestId("bump-permission").click();
    await page.getByTestId("toggle-view-active").click();
    await page.waitForTimeout(300);

    expect(stub.requests.length).toBe(0);
    expect(stub.other).toEqual([]);
    await expect(chips(page)).toHaveCount(0);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");
    await expect(toggle(page)).not.toBeChecked();
  });

  test("opting in alone adds nothing, and the explicit add leaves the draft and chips alone", async ({page}) => {
    const stub = await installStub(page);
    await page.goto(HARNESS);
    await page.getByTestId("draft").fill(`${INITIAL_DRAFT} and I am still typing`);

    await toggle(page).check();
    await expect(toggle(page)).toBeChecked();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "armed");
    await expect(addButton(page)).toBeEnabled();
    await page.waitForTimeout(250);

    // Opting in is a local preference: it must not call out or add a chip.
    expect(stub.requests.length).toBe(0);
    expect(stub.other).toEqual([]);
    await expect(chips(page)).toHaveCount(0);
    await expect(note(page)).toContainText("no message text is copied");

    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "added");
    await expect(chips(page)).toHaveCount(1);
    await expect(chips(page)).toContainText(`@${AGENT_NAME}`);
    await expect(chips(page)).toHaveAttribute("data-chip-id", AGENT_ID);
    await expect(page.getByTestId("mention-calls")).toHaveText("1");
    // The chip carries only the immutable id: the draft is untouched and the
    // surrounding composer form was never submitted.
    await expect(page.getByTestId("draft")).toHaveValue(`${INITIAL_DRAFT} and I am still typing`);
    await expect(page.getByTestId("posted")).toHaveText("not posted");
    expect(stub.other).toEqual([]);
    expect(stub.requests.length).toBe(1);
  });

  test("the add action rechecks this board's mention options for the exact id", async ({page}) => {
    const stub = await installStub(page);
    await page.goto(HARNESS);
    await toggle(page).check();
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "added");

    expect(stub.requests.length).toBe(1);
    expect(stub.requests[0].method).toBe("GET");
    const url = new URL(stub.requests[0].url);
    expect(url.pathname).toBe("/v1/projects/proj-1/mention-options");
    expect(stub.requests[0].query).toBe(AGENT_NAME);
    expect(stub.requests[0].board).toBe("board-1");
  });

  test("a lookalike name, an empty option list, or a failing endpoint never adds a chip", async ({page}) => {
    // Same display name, different id: the exact-id match fails closed.
    await installStub(page, {rows: [row("agent-imposter99", AGENT_NAME)]});
    await page.goto(HARNESS);
    await toggle(page).check();
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "blocked");
    await expect(note(page)).toContainText("Nothing was added");
    await expect(chips(page)).toHaveCount(0);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");
    await expect(page.getByTestId("draft")).toHaveValue(INITIAL_DRAFT);

    // The agent is not visible on this board at all.
    await page.unroute(MENTION_OPTIONS);
    await installStub(page, {rows: []});
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "blocked");
    await expect(chips(page)).toHaveCount(0);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");

    // Access changed mid-session: the check errors out and adds nothing.
    await page.unroute(MENTION_OPTIONS);
    await installStub(page, {status: 403});
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "error");
    await expect(note(page)).toContainText("Nothing was added");
    await expect(chips(page)).toHaveCount(0);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");
  });

  test("existing chips are never duplicated, and removing one re-enables an explicit add", async ({page}) => {
    const stub = await installStub(page);
    await page.goto(HARNESS);
    await toggle(page).check();
    await addButton(page).click();
    await expect(chips(page)).toHaveCount(1);
    await expect(page.getByTestId("mention-calls")).toHaveText("1");

    // Once the host holds the chip there is no second control to add it again.
    await expect(addButton(page)).toHaveCount(0);
    await expect(note(page)).toContainText("already addressed on this reply");

    await chips(page).click();
    await expect(chips(page)).toHaveCount(0);
    await expect(addButton(page)).toBeVisible();
    await expect(page.getByTestId("mention-calls")).toHaveText("1");

    await addButton(page).click();
    await expect(chips(page)).toHaveCount(1);
    await expect(page.getByTestId("mention-calls")).toHaveText("2");
    expect(stub.requests.length).toBe(2);
  });

  test("a stale response for a previous board or view is discarded", async ({page}) => {
    const seenBoards: string[] = [];
    const stub = await installStub(page, {
      delayMs: 700,
      rows: url => {
        seenBoards.push(url.searchParams.get("board_id") ?? "");
        return [row()];
      },
    });
    await page.goto(HARNESS);
    await toggle(page).check();
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "checking");
    await expect(addButton(page)).toBeDisabled();

    await page.getByTestId("switch-board").click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "off");
    await expect(toggle(page)).not.toBeChecked();
    await page.waitForTimeout(1200);

    expect(stub.requests.length).toBe(1);
    expect(seenBoards).toEqual(["board-1"]);
    await expect(chips(page)).toHaveCount(0);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");

    // Same guard for a view change while the check is in flight.
    await toggle(page).check();
    await addButton(page).click();
    await page.getByTestId("switch-view").click();
    await page.waitForTimeout(1200);
    await expect(chips(page)).toHaveCount(0);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");
    await expect(toggle(page)).not.toBeChecked();
  });

  test("the audience is always stated and the controls are labelled", async ({page}) => {
    await installStub(page);
    await page.goto(HARNESS);

    const audience = page.locator("[data-agent-prefill-audience]");
    await expect(audience).toHaveText("Visible to everyone permitted on this board");
    await expect(prefill(page)).toHaveAttribute("data-audience-scope", "shared-board");
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-target", AGENT_ID);
    await expect(prefill(page)).toHaveAccessibleName(`Agent mention prefill for ${AGENT_NAME}`);
    await expect(toggle(page)).toHaveAccessibleName(`Opt in to addressing ${AGENT_NAME} on the shared board`);
    await expect(note(page)).toHaveAttribute("role", "status");

    // The control is described by the audience sentence and the live status note.
    const describedBy = (await toggle(page).getAttribute("aria-describedby"))?.split(/\s+/) ?? [];
    expect(describedBy.length).toBeGreaterThanOrEqual(2);
    for (const id of describedBy) await expect(page.locator(`[id="${id}"]`)).toHaveCount(1);
    const audienceId = await audience.getAttribute("id");
    expect(describedBy).toContain(audienceId);

    await toggle(page).check();
    await expect(addButton(page)).toHaveAccessibleName(`Add @${AGENT_NAME} to this reply on the shared board. Nothing is posted.`);
    await expect(addButton(page)).toHaveAttribute("type", "button");
    // The add action is described by the same stated audience and live status.
    const addDescribedBy = await addButton(page).getAttribute("aria-describedby");
    expect(addDescribedBy).toContain(audienceId);

    // No private/direct audience wording is introduced anywhere in the control.
    await expect(prefill(page)).not.toContainText(/direct message|private/i);
  });

  test("opening the agent tab goes through the host callback and notifies nobody", async ({page}) => {
    const stub = await installStub(page);
    await page.goto(`${HARNESS}?viewActive=false`);

    const open = page.locator("[data-agent-prefill-open]");
    await expect(open).toBeVisible();
    await expect(open).toHaveAttribute("type", "button");
    await open.click();

    await expect(page.getByTestId("opened")).toHaveText(AGENT_ID);
    await expect(page.getByTestId("mention-calls")).toHaveText("0");
    await expect(chips(page)).toHaveCount(0);
    await expect(toggle(page)).not.toBeChecked();
    expect(stub.requests.length).toBe(0);
    expect(stub.other).toEqual([]);

    // The host-owned view switch hides the open button and still adds nothing.
    await expect(open).toHaveCount(0);
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "off");
  });

  test("a refused check can be retried once the board's mention options recover", async ({page}) => {
    await installStub(page, {status: 500});
    await page.goto(HARNESS);
    await toggle(page).check();
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "error");
    await expect(chips(page)).toHaveCount(0);

    await page.unroute(MENTION_OPTIONS);
    const stub = await installStub(page);
    await addButton(page).click();
    await expect(prefill(page)).toHaveAttribute("data-agent-prefill-status", "added");
    await expect(chips(page)).toHaveCount(1);
    await expect(page.getByTestId("mention-calls")).toHaveText("1");
    expect(stub.requests.length).toBe(1);
    await expect(page.getByTestId("posted")).toHaveText("not posted");
  });
});
