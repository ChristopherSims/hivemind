import {test, expect, Page} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() { return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8")); }
async function login(page: Page) {
  const data = fixture();
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.getByRole("status")).toHaveText("Live");
  return data;
}

test("older board pages survive live refresh and searches/deletions recheck canonical evidence", async ({page, request}) => {
  const data = fixture(), headers = {Authorization: `Bearer ${data.token}`};
  let first = "";
  for (let index = 0; index < 121; index++) {
    const response = await request.post(`/v0.1/boards/${data.board_id}/posts`, {headers, data: {
      client_post_id: crypto.randomUUID(), content: {media_type: "text/plain", text: `Board archive ${String(index).padStart(3, "0")}`}}});
    expect(response.ok()).toBeTruthy();
    if (index === 0) first = (await response.json()).data.id;
  }
  await login(page);
  await expect(page.getByText("Board archive 120", {exact: true})).toBeVisible();
  await expect(page.getByText("Board archive 000", {exact: true})).toHaveCount(0);
  await page.getByRole("button", {name: "Load earlier posts", exact: true}).click();
  await expect(page.getByText("Board archive 000", {exact: true})).toBeVisible();
  await request.post(`/v0.1/boards/${data.board_id}/posts`, {headers, data: {
    client_post_id: crypto.randomUUID(), content: {media_type: "text/plain", text: "Board live refresh canary"}}});
  await expect(page.getByText("Board live refresh canary", {exact: true})).toBeVisible();
  await expect(page.getByText("Board archive 000", {exact: true})).toBeVisible();
  await page.getByLabel("Search this view", {exact: true}).fill("Board archive 000");
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.locator(".timeline .message")).toHaveCount(1);
  const deleted = await request.delete(`/v0.1/posts/${first}`, {headers, data: {expected_revision: 1}});
  expect(deleted.ok()).toBeTruthy();
  await expect(page.getByText("Board archive 000", {exact: true})).toHaveCount(0);
  await page.getByRole("button", {name: "Clear search"}).click();
  await expect(page.getByText("Board live refresh canary", {exact: true})).toBeVisible();
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});

test("conversation and agent pages search old history while read marking covers displayed pages only", async ({page, request}) => {
  const data = fixture(), headers = {Authorization: `Bearer ${data.token}`};
  for (let index = 0; index < 121; index++) {
    const response = await request.post("/v0.1/messages", {headers, data: {
      message_id: `msg_${crypto.randomUUID()}`, project_id: data.project_id, from: data.agents[2].id,
      to: data.agents[0].id, type: "inform", thread_id: `thread_${crypto.randomUUID()}`,
      content: {media_type: "text/plain", text: `Conversation archive ${String(index).padStart(3, "0")}`}}});
    expect(response.ok()).toBeTruthy();
  }
  await login(page);
  await page.getByRole("button", {name: "Agent conversations", exact: true}).click();
  await expect(page.getByText("Conversation archive 120", {exact: true})).toBeVisible();
  await expect(page.getByText("Conversation archive 000", {exact: true})).toHaveCount(0);
  await page.getByRole("button", {name: "Load earlier conversations"}).click();
  await expect(page.getByText("Conversation archive 000", {exact: true})).toBeVisible();
  await page.getByRole("button", {name: "Mark displayed conversations read"}).click();
  await expect(page.getByLabel("Unread conversations")).toHaveText("0 unread");
  const inbox = (await (await request.get(`/v0.1/agents/${data.agents[0].id}/inbox?project_id=${data.project_id}`, {headers})).json()).data;
  expect(inbox.filter((item: {content: {text: string}}) => item.content.text.startsWith("Conversation archive")).length).toBe(121);
  await page.getByLabel("Search this view", {exact: true}).fill("Conversation archive 000");
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.locator(".timeline .message")).toHaveCount(1);
  await page.locator(".roster-card").getByRole("button", {name: "Droid worker", exact: true}).click();
  await expect(page.getByRole("button", {name: "Load earlier activity"})).toBeVisible();
  await page.getByRole("button", {name: "Load earlier activity"}).click();
  await expect(page.getByText("Conversation archive 000", {exact: true})).toBeVisible();
  await page.getByLabel("Search this view", {exact: true}).fill("Conversation archive 000");
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.locator(".timeline .message")).toHaveCount(1);
});
