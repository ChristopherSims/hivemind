import {test, expect, Page} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() { return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8")); }
async function login(page: Page, observer = false) {
  const data = fixture();
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(observer ? data.observer_token : data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await page.getByRole("button", {name: "Agent conversations", exact: true}).click();
  await expect(page.getByRole("button", {name: "Mark displayed conversations read"})).toBeVisible();
  return data;
}

test("explicit reader accounting survives reload without acknowledging agent delivery", async ({page, request}) => {
  const data = await login(page);
  await expect(page.getByLabel("Unread conversations")).toHaveText("1 unread");
  await page.getByRole("button", {name: "Mark displayed conversations read"}).click();
  await expect(page.getByLabel("Unread conversations")).toHaveText("0 unread");
  await expect(page.locator(".delivery-detail")).toContainText("Read by you");
  await page.reload();
  await expect(page.getByLabel("Unread conversations")).toHaveText("0 unread");
  const inbox = await request.get(`/v1/agents/${data.agents[0].id}/inbox?project_id=${data.project_id}`,
    {headers: {Authorization: `Bearer ${data.token}`}});
  expect((await inbox.json()).data[0].state).toBe("available");
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});

test("dead-letter retry is explicit, keeps the same envelope, and never impersonates processing", async ({page, request}) => {
  const data = fixture(), headers = {Authorization: `Bearer ${data.token}`, "Idempotency-Key": crypto.randomUUID()};
  const messageId = `msg_${crypto.randomUUID()}`;
  const created = await request.post("/v1/messages", {headers, data: {
    message_id: messageId, project_id: data.project_id, from: data.agents[1].id, to: data.agents[0].id,
    type: "inform", thread_id: `thread_${crypto.randomUUID()}`,
    content: {media_type: "text/plain", text: "Browser dead-letter canary"},
    retry_policy: {max_attempts: 1},
  }});
  expect(created.ok(), await created.text()).toBeTruthy();
  const ledger = (await (await request.get(`/v1/ledger?project_id=${data.project_id}`, {headers})).json()).data;
  const run = ledger.runs.find((item: {agent_id: string}) => item.agent_id === data.agents[0].id);
  const claimed = await request.post(`/v1/messages/${messageId}:claim`, {headers, data: {run_id: run.id}});
  expect(claimed.ok()).toBeTruthy();
  const receipt = (await claimed.json()).data;
  const failed = await request.post(`/v1/messages/${messageId}:fail`, {headers,
    data: {run_id: run.id, delivery_id: receipt.delivery_id, reason: "handler_failed"}});
  expect(failed.ok()).toBeTruthy();
  await login(page, true);
  const row = page.locator(".message").filter({hasText: "Browser dead-letter canary"});
  await expect(row).toContainText("Delivery: dead_letter");
  await expect(row.getByRole("button", {name: "Retry dead-letter message"})).toHaveCount(0);
  await page.getByRole("button", {name: "Mark displayed conversations read"}).click();
  await expect(page.getByLabel("Unread conversations")).toHaveText("0 unread");
  await page.getByRole("button", {name: "Sign out"}).click();
  await login(page);
  await expect(page.getByLabel("Unread conversations")).toHaveText("1 unread");
  await row.getByRole("button", {name: "Retry dead-letter message"}).click();
  await expect(row).toContainText("Delivery: available");
  await expect(row).toContainText("attempt 0");
  await row.getByText("Delivery history", {exact: true}).click();
  await expect(row).toContainText("requeued");
  const detail = (await (await request.get(`/v1/messages/${messageId}`, {headers})).json()).data;
  expect(detail.id).toBe(messageId);
  expect(detail.total_attempts).toBe(1);
  expect(detail.state).toBe("available");
});
