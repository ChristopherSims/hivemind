import {expect, test} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

const workspaces = [
  {path: "tasks", label: "Tasks & runs", selector: '[data-testid="task-run-workspace"]'},
  {path: "coordination", label: "Coordination", selector: '[data-testid="conflict-console"]'},
  {path: "context", label: "Context explorer", selector: '[data-testid="context-explorer"]'},
  {path: "operations", label: "Operations", selector: '[data-testid="operations-dashboard"]'},
  {path: "privacy", label: "Privacy & audit", selector: '[data-testid="pa-permissions"]'},
];

test("a late initial snapshot cannot replace explicit workspace navigation", async ({page}) => {
  const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  await page.route("**/v1/projects/*/dashboard-snapshot", async route => {
    await gate;
    await route.continue();
  });
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(fixture.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await page.getByRole("button", {name: "Tasks & runs", exact: true}).click();
  await expect(page.getByTestId("task-run-workspace")).toBeVisible();
  release();
  await expect(page.locator(".roster-card").first()).toBeVisible();
  await expect(page.getByRole("button", {name: "Tasks & runs", exact: true}))
    .toHaveAttribute("aria-current", "page");
  await expect(page.getByTestId("task-run-workspace")).toBeVisible();
  await expect(page.getByLabel("Shared board message")).toHaveCount(0);
});

test("metadata export downloads through the application with live invalidation enabled", async ({page}) => {
  const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
  await page.goto(`/projects/${fixture.project_id}/privacy`);
  await page.getByLabel("Local human credential").fill(fixture.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.getByTestId("pa-permissions")).toBeVisible();
  await expect(page.locator("header .connection")).toHaveText("Live");
  await page.getByRole("tab", {name: "Exports", exact: true}).click();
  await page.getByLabel("Export confirmation").fill(`EXPORT ${fixture.project_id}`);
  await page.getByRole("button", {name: "Create export descriptor"}).click();
  const download = page.getByRole("button", {name: "Download (re-authorized)"}).first();
  await expect(download).toBeEnabled();
  const [file] = await Promise.all([page.waitForEvent("download"), download.click()]);
  expect(file.suggestedFilename()).toMatch(/^privacy-audit-export-.*\.json$/);
  expect(await file.failure()).toBeNull();
});

for (const workspace of workspaces) {
  test(`the application restores and navigates the ${workspace.path} workspace`, async ({page}) => {
    const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
    await page.goto(`/projects/${fixture.project_id}/${workspace.path}`);
    await page.getByLabel("Local human credential").fill(fixture.token);
    await page.getByRole("button", {name: "Open workspace"}).click();
    await expect(page.locator(workspace.selector)).toBeVisible();
    await expect(page.locator("header .connection")).toHaveText("Live");
    await expect(page.getByRole("button", {name: workspace.label, exact: true}))
      .toHaveAttribute("aria-current", "page");
    await expect(page.getByLabel("Shared board message")).toHaveCount(0);
    await page.getByRole("button", {name: "Main board", exact: true}).click();
    await expect(page.getByLabel("Shared board message")).toBeVisible();
    await page.getByRole("button", {name: workspace.label, exact: true}).click();
    await expect(page.locator(workspace.selector)).toBeVisible();
    expect(new URL(page.url()).pathname).toBe(`/projects/${fixture.project_id}/${workspace.path}`);
  });
}
