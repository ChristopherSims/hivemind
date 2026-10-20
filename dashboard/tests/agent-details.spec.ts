import {test, expect, Page} from "@playwright/test";
import {readFileSync} from "node:fs";
import {resolve} from "node:path";

function fixture() { return JSON.parse(readFileSync(resolve("../.hivemind-test/browser.json"), "utf8")); }

async function login(page: Page, token?: string) {
  const data = fixture();
  await page.goto("/");
  await page.getByLabel("Local human credential").fill(token ?? data.token);
  await page.getByRole("button", {name: "Open workspace"}).click();
  await expect(page.locator(".roster-card").first()).toBeVisible();
  return data;
}

const EVIDENCE = {
  agent: {id: "agent_demo", display_name: "Droid worker", description: "Building navigation",
    project_id: "project_demo"},
  generated_at: 1700000000, limit: 20,
  ledger: {items: [{id: "run_demo", revision: 2, status: "running", presence: "online",
    task_id: "task_demo", last_seen_at: 1700000000, started_at: "2023-11-14T22:13:20Z",
    capture_health: {state: "healthy"},
    summary: {text: "Implementing the header", origin: "agent_reported"}, control_revision: 1}],
    next_before: null},
  artifacts: {items: [{id: "artifact_demo", revision: 1, title: "Navigation plan",
    run_id: "run_demo", created_at: "2023-11-14T22:14:00Z",
    content: {media_type: "text/plain", text: "Breakpoint is 720px"}}], next_before: null},
  memories: {items: [{id: "memory_demo", revision: 1, title: "Header decision", category: "semantic",
    scope_kind: "agent", scope_owner_id: "agent_demo", status: "verified", effective_status: "verified",
    origin: "explicit", evidence_refs: [{kind: "artifact", id: "artifact_demo", revision: 1}],
    validity: {}, expires_at: null, created_at: "2023-11-14T22:15:00Z",
    content: {text: "Compact navigation starts at 720px"}}], next_before: null},
  events: {items: [{event_id: "evt_demo", type: "hivemind.context.decision.recorded",
    occurred_at: "2023-11-14T22:16:00Z", run_id: "run_demo", ingestion_seq: 42,
    source: {source_id: "hook", kind: "runtime_hook", attribution: "confirmed", sequence: 1},
    data: {text: "Ship the compact header"}}], next_before: null},
};

function emptyEvidence() {
  return {...EVIDENCE, ledger: {items: [], next_before: null}, artifacts: {items: [], next_before: null},
    memories: {items: [], next_before: null}, events: {items: [], next_before: null}};
}

async function openDrawer(page: Page, index = 0) {
  const data = fixture(), agent = data.agents[index];
  await page.getByRole("button", {name: `Details for ${agent.display_name}`, exact: true}).click();
  const drawer = page.getByTestId("agent-details-drawer");
  await expect(drawer).toBeVisible();
  return {drawer, agent, data};
}

test("drawer renders authorized ledger, artifacts, memory evidence, and coordination events", async ({page}) => {
  await page.route("**/v1/agents/*/evidence*", route => route.fulfill({json: {data: EVIDENCE}}));
  await login(page);
  const {drawer, agent} = await openDrawer(page);
  await expect(drawer.getByRole("heading", {name: agent.display_name})).toBeVisible();
  await expect(drawer.getByText("Only records this account is authorized to read are shown.")).toBeVisible();

  const ledger = drawer.getByTestId("agent-details-ledger");
  await expect(ledger).toContainText("running");
  await expect(ledger).toContainText("Implementing the header");
  await expect(ledger).toContainText("run_demo");

  const artifacts = drawer.getByTestId("agent-details-artifacts");
  await expect(artifacts).toContainText("Navigation plan");
  await expect(artifacts).toContainText("Breakpoint is 720px");

  const memories = drawer.getByTestId("agent-details-memories");
  await expect(memories).toContainText("Header decision");
  await expect(memories.getByLabel("Evidence references"))
    .toContainText("artifact · artifact_demo · revision 1");

  const events = drawer.getByTestId("agent-details-events");
  await expect(events).toContainText("decision.recorded");
  await expect(events).toContainText("run_demo");

  await drawer.getByRole("button", {name: `Close ${agent.display_name} details`}).click();
  await expect(drawer).toBeHidden();
});

test("drawer reads are bounded and coordination events paginate without duplicating", async ({page}) => {
  const seen: string[] = [];
  await page.route("**/v1/agents/*/evidence*", async route => {
    const params = new URL(route.request().url()).searchParams;
    seen.push(params.toString());
    expect(Number(params.get("limit"))).toBeLessThanOrEqual(50);
    if (params.get("events_before") === "42") {
      await route.fulfill({json: {data: {...EVIDENCE, ledger: {items: [], next_before: null},
        artifacts: {items: [], next_before: null}, memories: {items: [], next_before: null},
        events: {items: [{...EVIDENCE.events.items[0], event_id: "evt_older",
          type: "hivemind.context.tool.completed", ingestion_seq: 41}], next_before: null}}}});
      return;
    }
    await route.fulfill({json: {data: {...EVIDENCE,
      events: {items: [...EVIDENCE.events.items,
        {...EVIDENCE.events.items[0], event_id: "evt_newer", ingestion_seq: 43}], next_before: 42}}}});
  });
  await login(page);
  const {drawer} = await openDrawer(page);
  const events = drawer.getByTestId("agent-details-events");
  await expect(events.getByTestId("agent-details-event-item")).toHaveCount(2);
  expect(seen[0]).toContain("project_id=");
  await drawer.getByRole("button", {name: "Load more coordination events"}).click();
  await expect(events.getByTestId("agent-details-event-item")).toHaveCount(3);
  await expect(events).toContainText("tool.completed");
  await expect(events.getByRole("button", {name: "Load more coordination events"})).toHaveCount(0);
  expect(seen.some(search => search.includes("events_before=42") && search.includes("ledger_before=0")))
    .toBeTruthy();
});

test("drawer clears evidence and explains when access is lost", async ({page}) => {
  await page.route("**/v1/agents/*/evidence*", route => route.fulfill({
    status: 403, json: {error: {code: "forbidden", message: "forbidden"}}}));
  await login(page);
  await page.getByRole("button", {name: `Details for ${fixture().agents[0].display_name}`, exact: true}).click();
  await expect(page.getByRole("button", {name: "Recheck access"})).toBeVisible();
  await expect(page.getByTestId("agent-details-drawer")).toHaveCount(0);
  await expect(page.locator(".roster-card, .message")).toHaveCount(0);
});

test("drawer shows explicit empty states, closes with Escape, and fits mobile", async ({page}) => {
  await page.route("**/v1/agents/*/evidence*", route => route.fulfill({json: {data: emptyEvidence()}}));
  await login(page);
  const {drawer} = await openDrawer(page);
  await expect(drawer.getByText("No authorized run ledger entries recorded.")).toBeVisible();
  await expect(drawer.getByText("No artifacts produced by this agent's runs.")).toBeVisible();
  await expect(drawer.getByText("No agent or run scoped memory evidence recorded.")).toBeVisible();
  await expect(drawer.getByText("No authorized coordination events recorded.")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();

  const {drawer: mobile, agent} = await openDrawer(page, 1);
  await expect(mobile.getByRole("heading", {name: agent.display_name})).toBeVisible();
  await page.setViewportSize({width: 390, height: 844});
  await expect(mobile).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});
