import {test, expect} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

test("Inter loads locally and is inherited by text and controls", async ({page}) => {
  await page.goto("/");
  const fonts = await page.evaluate(async () => {
    const loaded = await document.fonts.load('400 14px "Inter Variable"', "Hivemind");
    return loaded.map(face => ({family: face.family, status: face.status}));
  });
  expect(fonts.length).toBeGreaterThan(0);
  expect(fonts.every(face => face.family.replace(/["']/g, "") === "Inter Variable" && face.status === "loaded")).toBeTruthy();
  for (const selector of ["body", "h1", "#credential", ".primary"]) {
    expect(await page.locator(selector).evaluate(element => getComputedStyle(element).fontFamily)).toContain("Inter Variable");
  }
  expect(await page.locator("body").evaluate(element => getComputedStyle(element).fontOpticalSizing)).toBe("auto");
  expect(await page.locator("body").evaluate(element => getComputedStyle(element).fontSynthesis)).toBe("none");
  const requests = await page.evaluate(() => performance.getEntriesByType("resource")
    .map(entry => entry.name).filter(name => name.includes(".woff2")));
  expect(requests.length).toBeGreaterThan(0);
  expect(requests.every(url => new URL(url).origin === new URL(page.url()).origin)).toBeTruthy();
});

test("shared text, semantic, and agent tokens meet AA text contrast", () => {
  const css = readFileSync(resolve("src/tokens.css"), "utf8");
  const color = (name: string) => {
    const hex = css.match(new RegExp(`--${name}:\\s*#([\\da-f]{6});`, "i"))![1];
    const channels = [0, 2, 4].map(offset => parseInt(hex.slice(offset, offset + 2), 16) / 255)
      .map(value => value <= .04045 ? value / 12.92 : ((value + .055) / 1.055) ** 2.4);
    return channels[0] * .2126 + channels[1] * .7152 + channels[2] * .0722;
  };
  for (const [foreground, background] of [["text", "bg"], ["muted", "surface"], ["muted", "raised"],
    ["accent-ink", "accent"], ["accent-ink", "accent-hover"], ["accent-text", "accent-surface"],
    ["success", "success-surface"], ["warning", "warning-surface"], ["danger", "danger-surface"],
    ...["blue", "teal", "violet", "coral", "green", "magenta", "nexus"].map(name => [`agent-${name}`, "surface"])]) {
    const values = [color(foreground), color(background)].sort((a, b) => b - a);
    expect((values[0] + .05) / (values[1] + .05)).toBeGreaterThanOrEqual(4.5);
  }
});

for (const [width, height] of [[320, 740], [390, 844], [768, 1024], [1280, 900], [1920, 1080]]) {
  test(`workspace layout at ${width}px`, async ({page}, testInfo) => {
    await page.setViewportSize({width, height});
    const errors: string[] = [];
    page.on("pageerror", error => errors.push(error.message));
    const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
    await page.goto("/");
    await page.getByLabel("Local human credential").fill(fixture.token);
    await page.getByRole("button", {name: "Open workspace"}).click();
    await expect(page.locator("header .connection")).toHaveText("Live");
    await expect(page.locator(".timeline")).toBeVisible();
    await expect(page.getByRole("button", {name: "Main board", exact: true})).toHaveAttribute("aria-current", "page");
    await expect(page.locator(".header-honeycomb, .header-beacon")).toHaveCount(2);
    expect(await page.evaluate(() => getComputedStyle(document.documentElement).colorScheme)).toBe("dark");
    await expect(page.locator(".timeline-search [role='status']")).toContainText("loaded");
    await page.screenshot({path: testInfo.outputPath("workspace.png"), fullPage: true});
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
    for (const name of ["Project", "Sign out"]) {
      const control = name === "Project" ? page.getByLabel(name, {exact: true})
        : page.getByRole("button", {name, exact: true});
      const box = await control.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.x).toBeGreaterThanOrEqual(0);
      expect(box!.x + box!.width).toBeLessThanOrEqual(width);
    }
    expect(errors).toEqual([]);
    if (width <= 800) {
      const post = page.getByRole("button", {name: "Post to board", exact: true});
      await post.scrollIntoViewIfNeeded();
      const box = await post.boundingBox();
      expect(box!.y).toBeGreaterThanOrEqual(0);
      expect(box!.y + box!.height).toBeLessThanOrEqual(height);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    }
  });
}

test("Radix tooltips describe details without interfering with keyboard access", async ({page}) => {
  const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(fixture.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.locator("header .connection")).toHaveText("Live");
  const card = page.locator(".roster-card").first();
  const details = card.locator(".roster-details-button");
  const closeName = (await details.getAttribute("aria-label"))!.replace("Details for ", "Close ") + " details";
  await card.locator(".agent-name").focus();
  await page.keyboard.press("Tab");
  await expect(details).toBeFocused();
  await expect(page.getByRole("tooltip")).toHaveText("Review tasks, run health, and evidence");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("tooltip")).toHaveCount(0);
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("agent-details-drawer")).toBeVisible();
  await page.getByRole("button", {name: closeName}).click();
  await expect(details).toBeFocused();
});

test("keyboard navigation, empty search recovery, and reduced motion", async ({page}) => {
  const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
  await page.emulateMedia({reducedMotion: "reduce"});
  await page.goto("/");
  await expect(page.getByRole("button", {name: "Open workspace"})).toBeDisabled();
  await page.getByLabel("Local human credential").fill(fixture.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.locator("header .connection")).toHaveText("Live");
  await page.locator(".skip-link").focus();
  await page.keyboard.press("Enter");
  await expect(page.locator("#workspace")).toBeFocused();
  await page.getByLabel("Search this view", {exact: true}).fill("no-such-history-" + crypto.randomUUID());
  await page.getByRole("button", {name: "Search", exact: true}).click();
  await expect(page.getByRole("heading", {name: "No matching history."})).toBeVisible();
  await page.getByRole("button", {name: "Clear search", exact: true}).click();
  await expect(page.getByLabel("Search this view", {exact: true})).toHaveValue("");
  await page.getByRole("button", {name: "Agent conversations", exact: true}).click();
  await expect(page.getByRole("button", {name: "Agent conversations", exact: true})).toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("button", {name: "Main board", exact: true})).not.toHaveAttribute("aria-current");
  expect(await page.locator(".primary").evaluate(element => getComputedStyle(element).transitionDuration)).toBe("0s");
});

for (const width of [390, 1280]) {
  test(`memory and administration layout at ${width}px`, async ({page}, testInfo) => {
    await page.setViewportSize({width, height: 900});
    const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
    await page.goto("/");
    await page.getByLabel("Local human credential").fill(fixture.token);
    await page.getByRole("button", {name: "Open workspace"}).click();
    await expect(page.locator("header .connection")).toHaveText("Live");
    for (const [name, selector] of [["Memory review", ".memory-workspace"], ["Administration", ".admin-workspace"]]) {
      await page.getByRole("button", {name, exact: true}).click();
      await expect(page.locator(selector)).toBeVisible();
      await expect(page.getByRole("button", {name, exact: true})).toHaveAttribute("aria-current", "page");
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
      await page.screenshot({path: testInfo.outputPath(`${name.replaceAll(" ", "-")}.png`), fullPage: true});
    }
  });
}

for (const width of [390, 1280]) {
  test(`additional workspaces fit the soft shell at ${width}px`, async ({page}, testInfo) => {
    await page.setViewportSize({width, height: 900});
    const fixture = JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8"));
    await page.goto("/");
    await page.getByLabel("Local human credential").fill(fixture.token);
    await page.getByRole("button", {name: "Open workspace"}).click();
    await expect(page.locator("header .connection")).toHaveText("Live");
    for (const [name, selector] of [["Context explorer", ".context-explorer"], ["Operations", ".operations-dashboard"],
      ["Tasks & runs", ".trw"]]) {
      await page.getByRole("button", {name, exact: true}).click();
      await expect(page.locator(selector)).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
      await page.screenshot({path: testInfo.outputPath(`${name.replaceAll(" ", "-")}.png`), fullPage: true});
    }
  });
}
