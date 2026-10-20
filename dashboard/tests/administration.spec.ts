import {test, expect, Page, APIRequestContext} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() { return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8")); }
async function call(request: APIRequestContext, path: string, method: string, body?: unknown) {
  const data = fixture();
  const response = await request.fetch(`/v1/${path}`, {method, data: body, headers: {
    Authorization: `Bearer ${data.token}`, "Idempotency-Key": crypto.randomUUID(),
  }});
  expect(response.ok(), `Administrative API status ${response.status()}`).toBeTruthy();
  return (await response.json()).data;
}
async function login(page: Page, token = fixture().token) {
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await page.getByRole("button", {name: "Administration", exact: true}).click();
  await expect(page.getByRole("heading", {name: "People, permissions & peace of mind."})).toBeVisible();
  await expect(page.getByRole("button", {name: "Refresh administration"})).toBeEnabled();
}

test("operator manages membership, handles stale revisions, and deactivates explicitly", async ({page, request}) => {
  const data = fixture(), title = `Navigation team ${crypto.randomUUID().slice(0, 8)}`;
  await login(page);
  await page.getByLabel("Team name", {exact: true}).fill(title);
  await page.getByRole("group", {name: /Member agents/}).getByLabel(data.agents[0].display_name, {exact: true}).check();
  await page.getByRole("button", {name: "Create team", exact: true}).click();
  const row = page.locator(".entity-row").filter({hasText: title});
  await expect(row).toContainText("1 members");
  await row.click();
  const teamId = await row.getAttribute("data-team-id");
  const teams = await call(request, `projects/${data.project_id}/teams`, "GET");
  const team = teams.find((item: {id: string}) => item.id === teamId);
  await call(request, `teams/${team.id}`, "PATCH", {project_id: data.project_id,
    display_name: title, member_agent_ids: [], active: true, expected_revision: team.revision});
  await page.getByRole("button", {name: "Save team", exact: true}).click();
  await expect(page.getByRole("alert")).toContainText("412");
  await page.getByRole("button", {name: "Refresh administration"}).click();
  await expect(row).toContainText("0 members");
  await row.click();
  await page.getByLabel("Team is active", {exact: true}).uncheck();
  await page.getByRole("button", {name: "Save team", exact: true}).click();
  await expect(page.getByRole("alert")).toContainText("immediately removes");
  await page.getByRole("button", {name: "Confirm team deactivation"}).click();
  await expect(row).toContainText("Inactive");
});

test("resource policies require confirmation and persist exact grants", async ({page, request}) => {
  const data = fixture();
  const board = await call(request, "boards", "POST", {project_id: data.project_id, title: `Policy board ${crypto.randomUUID()}`});
  const team = await call(request, "teams", "POST", {project_id: data.project_id,
    display_name: `Policy readers ${crypto.randomUUID()}`, member_agent_ids: [data.agents[0].id]});
  await login(page);
  await page.getByRole("button", {name: "Resource access", exact: true}).click();
  await page.getByLabel("Resource", {exact: true}).selectOption(board.id);
  await expect(page.locator(".policy-caption")).toContainText("Legacy policy");
  await page.getByLabel("Public read", {exact: true}).uncheck();
  await page.getByLabel("Public contribute", {exact: true}).uncheck();
  await page.getByRole("group", {name: /Reader teams/}).getByLabel(team.display_name, {exact: true}).check();
  await page.getByRole("button", {name: "Review access changes"}).click();
  let policy = await call(request, `resources/${board.id}/access`, "GET");
  expect(policy.revision).toBeNull();
  await page.getByRole("button", {name: "Confirm access replacement"}).click();
  await expect(page.locator(".policy-caption")).toContainText("Explicit policy");
  policy = await call(request, `resources/${board.id}/access`, "GET");
  expect(policy.reader_team_ids).toEqual([team.id]);
  expect(policy.public_read).toBe(false);
  expect(policy.public_contribute).toBe(false);
  await page.setViewportSize({width: 390, height: 844});
  await expect(page.getByRole("button", {name: "Review access changes"})).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
});

test("Nexus delegation warns about sticky authority and revokes by revision", async ({page, request}) => {
  const data = fixture();
  const agent = await call(request, "agents", "POST", {project_id: data.project_id,
    display_name: `Scoped coordinator ${crypto.randomUUID().slice(0, 8)}`});
  await login(page);
  await page.getByRole("button", {name: "Nexus grants", exact: true}).click();
  await page.getByLabel("Coordinator agent", {exact: true}).selectOption(agent.id);
  await page.getByRole("group", {name: /Target agents/}).getByLabel(data.agents[0].display_name, {exact: true}).check();
  await page.getByLabel("Lifetime in seconds", {exact: true}).fill("600");
  await page.getByRole("button", {name: "Review scoped grant"}).click();
  await expect(page.getByRole("alert")).toContainText("permanently switches");
  await page.getByRole("button", {name: "Confirm scoped grant"}).click();
  const card = page.locator(".grant-card").filter({hasText: agent.display_name});
  await expect(card).toContainText("monitor");
  await card.getByRole("button", {name: "Revoke grant", exact: true}).click();
  await expect(card.getByRole("alert")).toContainText("Broad authority will not return");
  await card.getByRole("button", {name: "Confirm revoke"}).click();
  await expect(card).toContainText("revoked");
});

test("human operators lack worker controls; observers lack administration", async ({page}) => {
  const data = fixture();
  await login(page, data.human_token);
  await expect(page.getByRole("button", {name: "Worker incidents", exact: true})).toHaveCount(0);
  await page.getByRole("button", {name: "Sign out"}).click();
  await page.getByLabel("Local human credential").fill(data.observer_token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.getByRole("button", {name: "Memory review", exact: true})).toBeVisible();
  await expect(page.getByRole("button", {name: "Administration", exact: true})).toHaveCount(0);
});

test("admin worker view states monitoring limitations; deep links and mobile layout work", async ({page}) => {
  const data = fixture();
  await login(page);
  await page.getByRole("button", {name: "Worker incidents", exact: true}).click();
  await expect(page.getByRole("heading", {name: "Owned workers & incidents"})).toBeVisible();
  await expect(page.getByText(/Worker administration is unavailable/)).toBeVisible();
  await expect(page.getByText(/Denial monitoring is not universal/)).toBeVisible();
  await page.goto(`/projects/${data.project_id}/administration`);
  await expect(page.getByRole("heading", {name: "People, permissions & peace of mind."})).toBeVisible();
  await expect(page.getByRole("button", {name: "Refresh administration"})).toBeEnabled();
  await page.screenshot({path: resolve(`../.hivemind-test/ui-administration-desktop-${Date.now()}.png`), fullPage: true, animations: "disabled"});
  await page.setViewportSize({width: 390, height: 844});
  await page.getByRole("button", {name: "Nexus grants", exact: true}).click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await expect(page.getByLabel("Coordinator agent", {exact: true})).toBeVisible();
  await page.screenshot({path: resolve(`../.hivemind-test/ui-administration-mobile-${Date.now()}.png`), fullPage: true, animations: "disabled"});
});
