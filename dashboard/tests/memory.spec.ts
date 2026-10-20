import {test, expect, Page, APIRequestContext} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() { return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8")); }
async function login(page: Page) {
  const data = fixture();
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await page.getByRole("button", {name: "Memory review", exact: true}).click();
  await expect(page.getByRole("heading", {name: "Review facts, not assumptions."})).toBeVisible();
  return data;
}
async function seed(request: APIRequestContext, title: string, text: string, fields = {}) {
  const data = fixture(), headers = {Authorization: `Bearer ${data.token}`, "Idempotency-Key": crypto.randomUUID()};
  const postResponse = await request.post(`/v1/boards/${data.board_id}/posts`, {headers, data: {
    client_post_id: crypto.randomUUID(), content: {media_type: "text/plain", text}}});
  expect(postResponse.ok()).toBeTruthy();
  const post = (await postResponse.json()).data;
  const response = await request.post("/v1/memories", {headers, data: {
    project_id: data.project_id, title, content: {media_type: "text/plain", text},
    scope_owner_id: data.project_id, evidence_refs: [{kind: "board_post", id: post.id, revision: post.revision}],
    ...fields}});
  expect(response.ok(), await response.text()).toBeTruthy();
  return {memory: (await response.json()).data, post};
}

test("human verifies, proposes, approves, and reviews immutable version history", async ({page, request}) => {
  const {memory, post} = await seed(request, "Browser version lifecycle", "Initial browser memory");
  const data = await login(page);
  const card = page.locator(`[data-memory-id="${memory.id}"]`);
  await expect(card).toContainText("proposed");
  await expect(card.getByRole("button", {name: "Verify memory"})).toBeDisabled();
  await page.getByLabel("Review reason", {exact: true}).fill("Reviewed the source");
  await card.getByRole("button", {name: "Verify memory"}).click();
  await expect(card).toContainText("verified");
  await card.getByRole("button", {name: "Propose new version"}).click();
  await page.getByLabel("Version content", {exact: true}).fill("<script>window.memoryXss=true</script> Revised format");
  await page.getByLabel("Version evidence", {exact: true}).selectOption(post.id);
  await page.getByRole("button", {name: "Submit version proposal"}).click();
  await expect(card).toContainText("Initial browser memory");
  const proposal = page.getByTestId("memory-proposal").filter({hasText: "Browser version lifecycle"});
  await proposal.getByRole("button", {name: "Approve version"}).click();
  await expect(card).toContainText("Revised format");
  await expect(proposal).toContainText("approved");
  expect(await page.evaluate(() => (window as unknown as {memoryXss?: boolean}).memoryXss)).toBeUndefined();
  await card.getByRole("button", {name: "View version history"}).click();
  const history = page.getByRole("region", {name: "Memory version history"});
  await expect(history).toContainText("Initial browser memory");
  await expect(history).toContainText("superseded");
  await page.goto(`/projects/${data.project_id}/memories`);
  await expect(page.getByRole("heading", {name: "Review facts, not assumptions."})).toBeVisible();
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});

test("expiry is visible for review, and invalid dates never create a version", async ({page, request}) => {
  const {memory, post} = await seed(request, "Browser expiry lifecycle", "An expired fact",
    {expires_at: "2000-01-01T00:00:00Z"});
  await login(page);
  await page.getByLabel("Memory status", {exact: true}).selectOption("expired");
  const card = page.locator(`[data-memory-id="${memory.id}"]`);
  await expect(card).toContainText("expired");
  await expect(card.getByRole("button", {name: "Verify memory"})).toHaveCount(0);
  await card.getByRole("button", {name: "Propose new version"}).click();
  await page.getByLabel("Version evidence", {exact: true}).selectOption(post.id);
  await page.getByLabel("Expires at", {exact: true}).fill("2026-02-30T00:00:00Z");
  await page.getByRole("button", {name: "Submit version proposal"}).click();
  await expect(page.getByRole("alert")).toContainText("422");
  await page.getByLabel("Expires at", {exact: true}).fill("");
  await page.getByRole("button", {name: "Submit version proposal"}).click();
  await expect(page.getByTestId("memory-proposal").filter({hasText: "Browser expiry lifecycle"})).toBeVisible();
});

test("reporting leaves facts active until human confirmation and resolution", async ({page, request}) => {
  const left = await seed(request, "Browser contradiction left", "Format A");
  const right = await seed(request, "Browser contradiction right", "Format B");
  await login(page);
  await page.getByLabel("Review reason", {exact: true}).fill("Independent evidence checked");
  for (const item of [left, right]) {
    const card = page.locator(`[data-memory-id="${item.memory.id}"]`);
    await card.getByRole("button", {name: "Verify memory"}).click();
    await expect(card).toContainText("verified");
  }
  await page.getByLabel("Left memory", {exact: true}).selectOption(left.memory.id);
  await page.getByLabel("Right memory", {exact: true}).selectOption(right.memory.id);
  await page.getByLabel("Contradiction reason", {exact: true}).fill("These formats cannot both be canonical");
  await page.getByLabel("Contradiction evidence", {exact: true}).selectOption(left.post.id);
  await page.getByRole("button", {name: "Report contradiction", exact: true}).click();
  const dispute = page.getByTestId("memory-dispute").filter({hasText: "Browser contradiction left"});
  await expect(dispute).toContainText("reported");
  const a = page.locator(`[data-memory-id="${left.memory.id}"]`);
  const b = page.locator(`[data-memory-id="${right.memory.id}"]`);
  await expect(a).toContainText("verified");
  await dispute.getByRole("button", {name: "Confirm contradiction"}).click();
  await expect(a).toContainText("disputed");
  await expect(b).toContainText("disputed");
  await dispute.getByRole("button", {name: "Keep left memory"}).click();
  await expect(a).toContainText("verified");
  await expect(b).toContainText("superseded");
  await expect(dispute).toContainText("resolved");
});
