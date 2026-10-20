import {test, expect, Page} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() {
  return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
}
async function login(page: Page, human = false) {
  const data = fixture();
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(human ? data.human_token : data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.getByRole("button", {name: "Main board", exact: true})).toBeVisible();
  await expect(page.locator("header .connection")).toHaveText("Live");
  return data;
}

test("default board, in-page agent tabs, recipients, and unchanged delivery state", async ({page, request}) => {
  const data = await login(page);
  await page.locator(".roster-card").getByRole("button", {name: "Oh my Pi worker", exact: true}).click();
  await expect(page.getByText("Can you confirm the navigation breakpoint?")).toBeVisible();
  const originalMessage = page.locator(".message").filter({hasText: "Can you confirm the navigation breakpoint?"});
  await expect(originalMessage.locator(".message-heading")).toContainText("Oh my Pi worker");
  await expect(originalMessage.locator(".message-heading")).toContainText("Droid worker");
  await originalMessage.locator(".message-heading").getByRole("button", {name: "Droid worker", exact: true}).click();
  await expect(page.getByRole("button", {name: "Close Droid worker tab"})).toBeVisible();
  await expect(page.getByRole("button", {name: "Close Oh my Pi worker tab"})).toBeVisible();
  const response = await request.get(`/v1/agents/${data.agents[0].id}/inbox?project_id=${data.project_id}`,
    {headers: {Authorization: `Bearer ${data.token}`}});
  expect((await response.json()).data[0].state).toBe("available");
  expect(await page.locator(".roster-card .agent-name.teal").count()).toBe(3);
});

test("keyboard mention, shared post, public reply, and live update", async ({page, request}) => {
  const data = await login(page);
  const composer = page.getByLabel("Shared board message");
  await composer.fill("@Droid");
  await expect(page.getByRole("listbox", {name: "Agent mentions"})).toBeVisible();
  await composer.press("Enter");
  await expect(page.locator(".chips")).toContainText("@Droid worker");
  await composer.fill("Could you report current header progress?");
  await page.getByRole("button", {name: "Post to board"}).click();
  await expect(page.locator(".body").filter({hasText:"Could you report current header progress?"})).toBeVisible();
  const snapshot = await request.get(`/v1/boards/${data.board_id}/posts`,
    {headers: {Authorization: `Bearer ${data.token}`}});
  const post = (await snapshot.json()).data.items.at(-1);
  expect(post.content.segments[0].agent_id).toBe(data.agents[0].id);
  expect(post.author.kind).toBe("human");
  await request.post(`/v1/boards/${data.board_id}/posts`, {
    headers: {Authorization: `Bearer ${data.token}`},
    data: {client_post_id: crypto.randomUUID(), parent_post_id: post.id, thread_id: post.thread_id,
      content: {media_type: "text/plain", text: "Another authorized participant can answer this shared thread."}},
  });
  await expect(page.getByText("Another authorized participant can answer this shared thread.")).toBeVisible();
});

test("private conversations stay out of human board access, and HTML remains text", async ({page}) => {
  await login(page, true);
  await expect(page.getByRole("button", {name: "Agent conversations", exact: true})).toHaveCount(0);
  await expect(page.getByText("Can you confirm the navigation breakpoint?")).toHaveCount(0);
  await page.getByLabel("Shared board message").fill('<script>window.hivemindXss=true</script>');
  await page.getByRole("button", {name: "Post to board"}).click();
  await expect(page.locator(".body").filter({hasText:'<script>window.hivemindXss=true</script>'})).toBeVisible();
  expect(await page.evaluate(() => (window as unknown as {hivemindXss?: boolean}).hivemindXss)).toBeUndefined();
});

test("only the leased Nexus receives gold and crown, release restores base color", async ({page, request}) => {
  const data = await login(page);
  const headers = {Authorization: `Bearer ${data.token}`, "Idempotency-Key": crypto.randomUUID()};
  const runs = (await (await request.get(`/v1/ledger?project_id=${data.project_id}`, {headers})).json()).data.runs;
  const coordinator = data.agents[2];
  const run = runs.find((r: {agent_id: string}) => r.agent_id === coordinator.id);
  await request.post(`/v1/projects/${data.project_id}/nexus:acquire`, {headers,
    data: {agent_id: coordinator.id, run_id: run.id}});
  await expect(page.locator(".roster-card .nexus")).toHaveCount(1);
  await expect(page.getByRole("button", {name: "Coordinator, Nexus, main coordinator"})).toBeVisible();
  await request.post(`/v1/projects/${data.project_id}/nexus:release`,
    {headers: {...headers, "Idempotency-Key": crypto.randomUUID()}, data:{}});
  await expect(page.locator(".roster-card .nexus")).toHaveCount(0);
  await expect(page.locator(".roster-card .agent-name.teal")).toHaveCount(3);
});

test("mobile layout and SPA deep-link refresh", async ({page}) => {
  const data = await login(page);
  await page.setViewportSize({width:390, height:844});
  await expect(page.getByLabel("Shared board message")).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
  await page.goto(`/projects/${data.project_id}/board`);
  await expect(page.getByRole("button", {name: "Main board", exact: true})).toBeVisible();
  await page.goto(`/projects/${data.project_id}/agents/${data.agents[0].id}`);
  await expect(page.getByRole("button", {name: "Close Droid worker tab"})).toBeVisible();
});
