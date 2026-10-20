import {test, expect, Page} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

async function login(page: Page, human = false) {
  const data = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(human ? data.human_token : data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.locator(".roster-card").first()).toBeVisible();
  return data;
}

test("roster shows tasks, ownership, blockers, and authorized unread counts", async ({page}) => {
  await page.route("**/dashboard-snapshot", async route => {
    const response = await route.fetch();
    const body = await response.json();
    body.data.agents[0] = {...body.data.agents[0],
      current_tasks: [{id: "task_roster", title: "Build the agent roster", status: "running"}],
      owned_scopes: [{id: "scope_roster", display_name: "dashboard/roster"}],
      blockers: [{code: "run_waiting_input", label: "Waiting for input"}],
    };
    for (const agent of Object.values(body.data.delivery.by_agent) as {unread: number}[]) agent.unread = 7;
    await route.fulfill({response, json: body});
  });
  await login(page);
  const card = page.locator(".roster-card").first();
  await expect(card).toContainText("Build the agent roster");
  await expect(card).toContainText("dashboard/roster");
  await expect(card).toContainText("Waiting for input");
  await expect(page.locator(".roster-unread").filter({hasText: "7 unread conversations"}).first()).toBeVisible();
  await card.locator(".agent-name").click();
  await expect(page.getByRole("button", {name: /Close .* tab/})).toBeVisible();
});

test("human roster has empty states but no conversation counts", async ({page}) => {
  await login(page, true);
  const card = page.locator(".roster-card").first();
  await expect(card).toContainText("Current task");
  await expect(card).toContainText("No current task");
  await expect(card).toContainText("No owned scopes");
  await expect(card).toContainText("No reported blockers");
  await expect(page.locator(".roster-unread")).toHaveCount(0);
});
