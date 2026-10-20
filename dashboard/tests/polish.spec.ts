import {test, expect, Page, APIRequestContext} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";
import {readViewState, viewPath, writeViewState} from "../src/view-state";

function fixture() {
  return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
}

async function login(page: Page, human = false, path = "/") {
  const data = fixture();
  await page.goto(path);
  await page.getByLabel("Local human credential").fill(human ? data.human_token : data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.locator("header .connection")).toHaveText("Live");
  await expect(page.locator(".timeline")).toBeVisible();
  return data;
}

async function command(request: APIRequestContext, path: string, body: unknown) {
  const response = await request.post(`/v1/${path}`, {data: body,
    headers: {Authorization: `Bearer ${fixture().token}`, "Idempotency-Key": crypto.randomUUID()}});
  expect(response.ok()).toBeTruthy();
  return (await response.json()).data;
}

test("view-state fragments round-trip bounded navigation settings only", () => {
  const state = {board: "board_one", query: "Unicode ✓ & literal + query", pages: 3};
  expect(readViewState(writeViewState(state))).toEqual(state);
  expect(readViewState("#q=" + "x".repeat(300) + "&pages=999").query).toHaveLength(256);
  expect(readViewState("#pages=999").pages).toBe(20);
  expect(readViewState("#pages=-1").pages).toBe(1);
  expect(readViewState("#pages=2.5").pages).toBe(1);
  expect(readViewState("#board=" + "x".repeat(129)).board).toBe("");
  expect(writeViewState({board: "", query: "", pages: Number.NaN})).toBe("");
  expect(viewPath("project_one", "agent/one")).toBe("/projects/project_one/agents/agent%2Fone");
});

test("agent tabs keep their color and follow the authoritative Nexus lease", async ({page, request}) => {
  const data = await login(page);
  const agent = data.agents[2];
  await page.locator(".roster-card").getByRole("button", {name: agent.display_name, exact: true}).click();
  const tab = page.locator(".tabs .agent-tab");
  await expect(tab).toHaveClass(/teal/);
  const baseColor = await tab.evaluate(element => getComputedStyle(element).color);
  const ledger = (await (await request.get(`/v1/ledger?project_id=${data.project_id}`,
    {headers: {Authorization: `Bearer ${data.token}`}})).json()).data;
  const run = ledger.runs.find((item: {agent_id: string}) => item.agent_id === agent.id);
  await command(request, `projects/${data.project_id}/nexus:acquire`, {agent_id: agent.id, run_id: run.id});
  try {
    await expect(tab).toHaveClass(/nexus/);
    await expect(tab).toHaveAttribute("aria-label", `${agent.display_name}, Nexus, main coordinator`);
    await expect(tab.locator("small")).toHaveText("Nexus");
    await expect(tab.locator("svg")).toHaveCount(1);
    expect(await tab.evaluate(element => getComputedStyle(element).color)).not.toBe(baseColor);
    await page.reload();
    await expect(page.locator(".tabs .agent-tab.nexus")).toBeVisible();
  } finally {
    await command(request, `projects/${data.project_id}/nexus:release`, {});
  }
  const restored = page.locator(".tabs .agent-tab");
  await expect(restored).toHaveClass(/teal/);
  await expect(restored.locator("small, svg")).toHaveCount(0);
  await expect.poll(() => restored.evaluate(element => getComputedStyle(element).color)).toBe(baseColor);
});

test("mobile roster opens details, restores focus, and switches agent tabs", async ({page}) => {
  await page.setViewportSize({width: 390, height: 844});
  const data = await login(page);
  const toggle = page.getByRole("button", {name: "Open agent roster"});
  await toggle.click();
  const roster = page.getByRole("dialog", {name: "Agent roster", exact: true});
  await expect(roster).toBeVisible();
  await expect(page.getByRole("button", {name: "Close agent roster"})).toBeFocused();
  await page.keyboard.press("Shift+Tab");
  await expect(roster.locator(".roster-details-button").last()).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", {name: "Close agent roster"})).toBeFocused();
  const details = roster.getByRole("button", {name: `Details for ${data.agents[0].display_name}`, exact: true});
  await details.click();
  await expect(roster).toHaveCount(0);
  await expect(page.getByTestId("agent-details-drawer")).toBeVisible();
  await page.getByRole("button", {name: `Close ${data.agents[0].display_name} details`}).click();
  await expect(roster).toBeVisible();
  await expect(details).toBeFocused();
  await roster.getByRole("button", {name: data.agents[0].display_name, exact: true}).click();
  await expect(roster).toHaveCount(0);
  await expect(page.locator(".tabs .agent-tab[aria-current='page']")).toBeFocused();
  await expect(page.locator(".timeline")).toBeVisible();
  await toggle.click();
  await page.keyboard.press("Escape");
  await expect(roster).toHaveCount(0);
  await expect(toggle).toBeFocused();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});

test("applied search, selected board, and loaded page depth survive reload", async ({page, request}) => {
  const data = fixture();
  const board = await command(request, "boards", {project_id: data.project_id,
    title: `Reload board ${crypto.randomUUID().slice(0, 8)}`});
  const query = `Polish archive ${crypto.randomUUID().slice(0, 8)}`;
  for (let index = 0; index < 120; index++) {
    await command(request, `boards/${board.id}/posts`, {client_post_id: crypto.randomUUID(),
      content: {media_type: "text/plain", text: `${query} ${String(index).padStart(3, "0")}`}});
  }
  await login(page);
  await page.getByLabel("Active board").selectOption(board.id);
  await page.getByLabel("Search this view", {exact: true}).fill(query);
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.locator(".timeline")).toHaveAttribute("data-vt-total", "100");
  await page.getByRole("button", {name: "Load earlier posts", exact: true}).click();
  await expect(page.locator(".timeline")).toHaveAttribute("data-vt-total", "120");
  await expect.poll(() => readViewState(new URL(page.url()).hash).pages).toBe(2);
  await page.getByLabel("Shared board message").fill("An unsent draft must not be stored");
  await page.reload();
  await expect(page.getByLabel("Active board")).toHaveValue(board.id);
  await expect(page.getByLabel("Search this view", {exact: true})).toHaveValue(query);
  await expect(page.locator(".timeline")).toHaveAttribute("data-vt-total", "120");
  await expect(page.getByLabel("Shared board message")).toHaveValue("");
  expect(new URL(page.url()).search).toBe("");
  expect(readViewState(new URL(page.url()).hash)).toEqual({board: board.id, query, pages: 2});
  expect(await page.evaluate(() => [localStorage.length, sessionStorage.length])).toEqual([0, 0]);
  await page.getByRole("button", {name: "Sign out"}).click();
  await expect(page.getByLabel("Local human credential")).toBeVisible();
  expect(new URL(page.url()).hash).toBe("");
});

test("agent-tab searches survive reload but changing tabs resets the filter", async ({page}) => {
  const data = await login(page);
  await page.locator(".roster-card").getByRole("button", {name: data.agents[0].display_name, exact: true}).click();
  await page.getByLabel("Search this view", {exact: true}).fill("navigation breakpoint");
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.locator(".timeline")).toHaveAttribute("data-vt-total", "1");
  await page.reload();
  await expect(page.locator(".tabs .agent-tab[aria-current='page']")).toHaveText(data.agents[0].display_name);
  await expect(page.getByLabel("Search this view", {exact: true})).toHaveValue("navigation breakpoint");
  await expect(page.locator(".timeline")).toHaveAttribute("data-vt-total", "1");
  await page.getByRole("button", {name: "Main board", exact: true}).click();
  await expect(page.getByLabel("Search this view", {exact: true})).toHaveValue("");
  expect(readViewState(new URL(page.url()).hash).query).toBe("");
  expect(new URL(page.url()).pathname).toBe(`/projects/${data.project_id}/board`);
});

test("inaccessible saved views and boards are discarded before restoring search", async ({page}) => {
  const data = fixture();
  await login(page, true, `/projects/${data.project_id}/conversations#q=private+filter&pages=20`);
  await expect(page.getByLabel("Search this view", {exact: true})).toHaveValue("");
  await expect(page.getByRole("button", {name: "Agent conversations", exact: true})).toHaveCount(0);
  expect(readViewState(new URL(page.url()).hash).pages).toBe(1);
  await page.goto(`/projects/${data.project_id}/board#board=board_hidden&q=hidden+filter&pages=20`);
  await page.reload();
  await expect(page.getByLabel("Search this view", {exact: true})).toHaveValue("");
  await expect.poll(() => readViewState(new URL(page.url()).hash).board).toBe(data.board_id);
  expect(readViewState(new URL(page.url()).hash).pages).toBe(1);
});
