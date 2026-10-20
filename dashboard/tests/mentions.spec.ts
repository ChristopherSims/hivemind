import {test, expect, Page, APIRequestContext} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";
import {applyMentionRemoval, findMentionToken} from "../src/mentionCaret";

function fixture() { return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8")); }

async function call(request: APIRequestContext, path: string, method: string, body?: unknown) {
  const data = fixture();
  const response = await request.fetch(`/v1/${path}`, {method, data: body, headers: {
    Authorization: `Bearer ${data.token}`, "Idempotency-Key": crypto.randomUUID(),
  }});
  expect(response.ok(), `API ${method} ${path} -> ${response.status()}`).toBeTruthy();
  return (await response.json()).data;
}

async function login(page: Page, token = fixture().token) {
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.getByRole("button", {name: "Main board", exact: true})).toBeVisible();
}

async function composerField(page: Page) {
  await expect(page.locator("[data-mention-composer]")).toBeVisible();
  return page.getByLabel("Shared board message");
}

test("mention token detection is scoped to the caret", () => {
  expect(findMentionToken("Hello @Dro world", 10)).toEqual({start: 6, end: 10, query: "Dro"});
  expect(findMentionToken("Hello @Dro world", 8)).toEqual({start: 6, end: 8, query: "D"});
  expect(findMentionToken("Hello @Dro world", 7)).toEqual({start: 6, end: 7, query: ""});
  expect(findMentionToken("@Dr", 3)).toEqual({start: 0, end: 3, query: "Dr"});
  expect(findMentionToken("hi @", 4)).toEqual({start: 3, end: 4, query: ""});
  expect(findMentionToken("hi @b", 5)).toEqual({start: 3, end: 5, query: "b"});
  expect(findMentionToken("@abc", 2)).toEqual({start: 0, end: 2, query: "a"});
  expect(findMentionToken("user@example", 12)).toBeNull();
  expect(findMentionToken("hi @a@b", 7)).toBeNull();
  expect(findMentionToken("no mention here", 15)).toBeNull();
  expect(findMentionToken("x", 99)).toBeNull();
});

test("mention removal handles mid-draft tokens and collapses the gap", () => {
  expect(applyMentionRemoval("Hello @Dro world", {start: 6, end: 10, query: "Dro"}))
    .toEqual({value: "Hello world", caret: 6});
  expect(applyMentionRemoval("@Droid", {start: 0, end: 6, query: "Droid"})).toEqual({value: "", caret: 0});
  expect(applyMentionRemoval("Hi @Dr there", {start: 3, end: 6, query: "Dr"})).toEqual({value: "Hi there", caret: 3});
  expect(applyMentionRemoval("a @b", {start: 2, end: 4, query: "b"})).toEqual({value: "a ", caret: 2});
  expect(applyMentionRemoval("keep @a trailing", {start: 5, end: 7, query: "a"}))
    .toEqual({value: "keep trailing", caret: 5});
});

test("options are enriched with presence, current task, color and Nexus badge", async ({page, request}) => {
  const data = fixture();
  const taskTitle = `Header progress ${crypto.randomUUID().slice(0, 8)}`;
  const runs = (await call(request, `ledger?project_id=${data.project_id}`, "GET")).runs;
  const droid = data.agents[0], coordinator = data.agents[2];
  await page.route("**/dashboard-snapshot", async route => {
    const response = await route.fetch();
    const body = await response.json();
    const person = body.data.agents.find((agent: {id: string}) => agent.id === droid.id);
    person.current_tasks = [{id: "mention_task", title: taskTitle, status: "running"}];
    await route.fulfill({response, json: body});
  });
  const coordinatorRun = runs.find((run: {agent_id: string}) => run.agent_id === coordinator.id);
  await call(request, `projects/${data.project_id}/nexus:acquire`, "POST", {agent_id: coordinator.id, run_id: coordinatorRun.id});

  try {
  await login(page);
  const composer = await composerField(page);
  await composer.fill("@");
  const menu = page.getByRole("listbox", {name: "Agent mentions"});
  await expect(menu).toBeVisible();

  const droidOption = page.getByRole("option").filter({hasText: "Droid worker"});
  await expect(droidOption).toBeVisible();
  await expect(droidOption).toContainText(taskTitle);
  await expect(droidOption).toContainText(/online|stale|offline/);
  await expect(droidOption.locator(".mention-swatch")).toHaveClass(/mention-color-teal/);

  const coordinatorOption = menu.getByRole("option").filter({has: page.locator(".mention-nexus")});
  await expect(coordinatorOption).toContainText("Coordinator");
  await expect(coordinatorOption).toContainText("Nexus");

  await composer.press("Escape");
  await expect(menu).toHaveCount(0);
  } finally {
    await call(request, `projects/${data.project_id}/nexus:release`, "POST", {});
  }
});

test("a mid-draft mention anchors to the caret and never submits", async ({page, request}) => {
  const data = fixture();
  await login(page);
  const composer = await composerField(page);
  const postsBefore = (await call(request, `boards/${data.board_id}/posts`, "GET")).items.length;

  await composer.fill("Hello @Dro world");
  for (let index = 0; index < 6; index += 1) await composer.press("ArrowLeft");
  const menu = page.getByRole("listbox", {name: "Agent mentions"});
  await expect(menu).toBeVisible();
  await expect(menu).toHaveAttribute("data-anchored", "caret");
  // Caret anchoring: the menu starts after "Hello ", not at the field's left edge.
  await expect.poll(() => menu.evaluate(element => parseFloat((element as HTMLElement).style.left))).toBeGreaterThan(0);

  await composer.press("Enter");
  await expect(menu).toHaveCount(0);
  await expect(page.locator(".chips")).toContainText("@Droid worker");
  await expect(composer).toHaveValue("Hello world");
  await expect.poll(() => composer.evaluate(element => element.selectionStart)).toBe(6);

  const postsAfter = (await call(request, `boards/${data.board_id}/posts`, "GET")).items.length;
  expect(postsAfter).toBe(postsBefore);
});

test("keyboard navigation wraps, Escape cancels, and Tab selects without submitting", async ({page, request}) => {
  const data = fixture();
  await login(page);
  const composer = await composerField(page);
  const postsBefore = (await call(request, `boards/${data.board_id}/posts`, "GET")).items.length;

  await composer.fill("@");
  const menu = page.getByRole("listbox", {name: "Agent mentions"});
  await expect(menu).toBeVisible();
  const options = menu.getByRole("option");
  await expect.poll(() => options.count()).toBeGreaterThanOrEqual(3);
  await expect(options.first()).toHaveAttribute("aria-selected", "true");

  await composer.press("ArrowUp");
  await expect(options.last()).toHaveAttribute("aria-selected", "true");
  await composer.press("ArrowDown");
  await expect(options.first()).toHaveAttribute("aria-selected", "true");

  await composer.press("Escape");
  await expect(menu).toHaveCount(0);
  await expect(composer).toHaveValue("@");

  await composer.pressSequentially("Droid");
  await expect(menu).toBeVisible();
  await composer.press("Tab");
  await expect(menu).toHaveCount(0);
  await expect(page.locator(".chips")).toContainText("@Droid worker");

  const postsAfter = (await call(request, `boards/${data.board_id}/posts`, "GET")).items.length;
  expect(postsAfter).toBe(postsBefore);
});

test("mention tokens respect boundaries and clear when the board context changes", async ({page, request}) => {
  const data = fixture();
  const otherBoard = await call(request, "boards", "POST", {project_id: data.project_id, title: `Mention board ${crypto.randomUUID().slice(0, 8)}`});
  await login(page);
  const composer = await composerField(page);
  const menu = page.getByRole("listbox", {name: "Agent mentions"});

  await composer.fill("user@example");
  await page.waitForTimeout(350);
  await expect(menu).toHaveCount(0);

  await composer.fill("hi @");
  await expect(menu).toBeVisible();

  await page.getByLabel("Active board").selectOption(otherBoard.id);
  await expect(menu).toHaveCount(0);
  await expect(composer).toHaveValue("");
  await expect(page.locator(".chips button")).toHaveCount(0);
});
