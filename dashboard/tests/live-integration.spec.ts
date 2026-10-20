import {test, expect, Page} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() {
  return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
}

async function login(page: Page) {
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(fixture().token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.locator("header .connection")).toHaveText("Live");
  await expect(page.locator(".roster-card").first()).toBeVisible();
}

test("real EventSource binds the cursor of the displayed snapshot", async ({page}) => {
  const streams: string[] = [];
  page.on("request", request => {
    if (new URL(request.url()).pathname === "/v1/streams") streams.push(request.url());
  });
  await login(page);
  await expect.poll(() => streams.some(url => /^[0-9a-f]{64}$/.test(
    new URL(url).searchParams.get("cursor") ?? "",
  ))).toBeTruthy();
  const data = fixture();
  const response = await page.request.get(`/v1/boards/${data.board_id}/posts`);
  const snapshot = (await response.json()).data.snapshot_cursor;
  await expect.poll(() => streams.some(url => new URL(url).searchParams.get("cursor") === snapshot)).toBeTruthy();
});

test("revoked real session clears private conversations and the evidence drawer", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "Agent conversations", exact: true}).click();
  await expect(page.getByText("Can you confirm the navigation breakpoint?")).toBeVisible();
  await page.getByRole("button", {name: "Details for Droid worker", exact: true}).click();
  await expect(page.getByTestId("agent-details-drawer")).toBeVisible();
  const session = (await (await page.request.get("/v1/session")).json()).data;
  const response = await page.request.delete("/v1/session", {
    headers: {"X-CSRF-Token": session.csrf_token, "Idempotency-Key": crypto.randomUUID()},
  });
  expect(response.ok()).toBeTruthy();
  await expect(page.getByLabel("Local human credential")).toBeVisible();
  await expect(page.getByText("Can you confirm the navigation breakpoint?")).toHaveCount(0);
  await expect(page.getByTestId("agent-details-drawer")).toHaveCount(0);
  await expect(page.locator(".roster-card")).toHaveCount(0);
});

test("permission failure clears all views and access recovery is explicit", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "Details for Droid worker", exact: true}).click();
  await expect(page.getByTestId("agent-details-drawer")).toBeVisible();
  await page.getByRole("button", {name: "Close Droid worker details"}).click();
  await page.getByLabel("Shared board message").fill("Draft before access changed");
  await page.route("**/v1/boards/*/posts?**", route =>
    route.fulfill({status: 403, json: {error: {code: "permission_revoked", message: "permission revoked"}}}));
  await page.getByLabel("Search this view", {exact: true}).fill("Force a fresh authorized read");
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.getByRole("button", {name: "Recheck access"})).toBeVisible();
  await expect(page.locator(".message, .roster-card")).toHaveCount(0);
  await expect(page.getByLabel("Shared board message")).toHaveCount(0);
  await page.unroute("**/v1/boards/*/posts?**");
  await page.getByRole("button", {name: "Recheck access"}).click();
  await expect(page.locator(".roster-card").first()).toBeVisible();
  await expect(page.getByLabel("Shared board message")).toHaveValue("");
  await expect(page.locator("header .connection")).toHaveText("Live");
});
