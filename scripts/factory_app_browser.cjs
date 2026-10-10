// Independent real-Chromium assertions against the agents' actual application.
const path = require("node:path");
const { chromium } = require(path.join(__dirname, "../dashboard/node_modules/@playwright/test"));

(async () => {
  const [url, output] = process.argv.slice(2);
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto(url);
    const title = '<img src=x onerror="window.__xss=true"> real browser issue';
    await page.getByLabel("Issue title", { exact: true }).fill(title);
    await page.getByLabel("Description", { exact: true }).fill("Chromium acceptance evidence");
    await page.getByRole("button", { name: "Create issue", exact: true }).click();
    const card = page.getByTestId("issue").filter({ hasText: title });
    await card.waitFor();
    if (await page.evaluate(() => !!window.__xss)) throw Error("Issue title executed HTML");
    await Promise.all([
      page.waitForResponse(r => r.url().includes("/api/issues/") && r.request().method() === "PATCH"),
      card.getByTestId("issue-status").selectOption("in_progress"),
    ]);
    await page.reload();
    await card.waitFor();
    if (await card.getByTestId("issue-status").inputValue() !== "in_progress") throw Error("Status not persisted");
    await page.getByLabel("Status filter", { exact: true }).selectOption("closed");
    await card.waitFor({ state: "hidden" });
    await page.getByLabel("Status filter", { exact: true }).selectOption("in_progress");
    await card.waitFor();
    await page.screenshot({ path: path.join(output, "issue-tracker.png"), fullPage: true });
    await card.getByRole("button", { name: "Delete", exact: true }).click();
    await card.waitFor({ state: "hidden" });
    await page.reload();
    await card.waitFor({ state: "hidden" });
    console.log(JSON.stringify({ status: "PASS", checks: ["create", "safe-text", "status", "reload", "filter", "delete"] }));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
