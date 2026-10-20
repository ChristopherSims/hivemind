import {test, expect, type Page} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";
import {
  lagLabel, sampleTimeLabel, summarizeWarnings, warningTitle, windowNote,
  type OperationsSummary,
} from "../src/operations-dashboard";

/*
 * Operations dashboard.
 *
 * Part 1 covers the pure contracts in-process (no network, no harness). Part 2
 * mounts the component in an isolated vite dev server on port 5213, reserved
 * for this workstream and clear of the other harnesses (5197/5199/5201/5205)
 * and the python fixture (8766). Every request is stubbed by an in-spec fake
 * that mirrors the read-only contract of hivemind/operations_dashboard.py, so
 * no real Hivemind store is touched and this spec can run before the dashboard
 * owner wires the /projects/{id}/operations route into main.tsx.
 */

const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5213;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/operations-dashboard.html`;
let harness: ChildProcess | null = null;

async function waitForHarness() {
  for (let attempt = 0; attempt < 80; attempt++) {
    try {
      const response = await fetch(HARNESS);
      if (response.ok) return;
    } catch {
      // The dev server is still starting.
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Harness server did not start at ${HARNESS}`);
}

function summary(overrides: Partial<OperationsSummary> = {}): OperationsSummary {
  return {
    project_id: "proj-1", scope: "project", role: "admin",
    generated_at: 1750000000, generated_at_unix_seconds: 1750000000,
    ingestion: {
      available: true, canonical_through_ingestion_seq: 42,
      vector: {available: true, through_ingestion_seq: 42, pending_jobs: 0, lag_ingestion_seq: 0},
      graph: {available: true, through_ingestion_seq: 40, pending_jobs: 2, lag_ingestion_seq: 2},
      index_read_path: "dedicated", graph_projection: "available", semantic_encoder: "configured",
    },
    jobs: {
      available: true, scope: "project",
      project: {total: {pending: 2, done: 40, dead: 0, other: 0},
                by_kind: {embedding: {pending: 0, done: 20, dead: 0, other: 0},
                          graph: {pending: 2, done: 20, dead: 0, other: 0}}},
      global: {scope: "global_all_projects", by_state: {pending: 9, done: 400}},
    },
    delivery: {
      available: true, scope: "authorized_principal",
      states: {available: 1, leased: 0, retry_wait: 0, dead_letter: 0, expired: 0, acked: 5},
      scanned: 6, limit: 1000, truncated: false,
      window_note: "Counts cover the newest bounded window.",
    },
    runs: {
      available: true, scanned: 1, limit: 200, truncated: false,
      presence: {online: 1, stale: 0, offline: 0, unknown: 0},
      capture: {healthy: 0, gaps: 0, unknown: 1, other: 0},
      presence_thresholds_seconds: {online_within: 15, stale_within: 30},
      presence_basis: "last_seen_at_heartbeats",
      presence_note: "Presence reflects heartbeat recency only; an online run is not evidence of verified capture.",
      sample: [{id: "run-1", agent_id: "agent-1", status: "running", presence: "online",
                capture_state: "unknown", last_seen_at: 1750000000}],
      sample_limit: 50, sample_truncated: false,
    },
    telemetry: {available: true, scope: "project", enabled: false, export_enabled: false,
                queued: 0, dropped: 0, delivery: "at-least-once", content_capture: false},
    warnings: [],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Part 1: pure contracts
// ---------------------------------------------------------------------------

test.describe("operations dashboard contracts", () => {
  test("a measured zero lag reads as caught up; only an unknown watermark reads unknown", () => {
    expect(lagLabel({available: true, through_ingestion_seq: 5, pending_jobs: 0,
                     lag_ingestion_seq: 0})).toBe("Caught up");
    expect(lagLabel({available: true, through_ingestion_seq: 3, pending_jobs: 2,
                     lag_ingestion_seq: 2})).toBe("2 seq behind (2 pending)");
    expect(lagLabel(undefined)).toBe("Unknown");
    expect(lagLabel({available: false, through_ingestion_seq: 0, pending_jobs: 0,
                     lag_ingestion_seq: 0})).toBe("Unknown");
  });

  test("a missing sample timestamp is unknown, never rendered as now", () => {
    expect(sampleTimeLabel(null)).toBe("Unknown");
    expect(sampleTimeLabel(undefined)).toBe("Unknown");
    expect(sampleTimeLabel(Number.NaN)).toBe("Unknown");
    expect(sampleTimeLabel(1750000000)).not.toBe("Unknown");
  });

  test("warning grouping never invents a total when any count is unknown", () => {
    const grouped = summarizeWarnings([
      {code: "index_jobs_dead", scope: "project", count: 2, message: "a"},
      {code: "index_jobs_dead", scope: "project", count: 3, message: "b"},
      {code: "index_read_path_fallback", scope: "project", count: null, message: "c"},
    ]);
    expect(grouped).toHaveLength(2);
    const dead = grouped.find(group => group.code === "index_jobs_dead")!;
    expect(dead.count).toBe(5);
    expect(dead.occurrences).toBe(2);
    expect(dead.title).toBe("Indexing jobs exhausted retries");
    const fallback = grouped.find(group => group.code === "index_read_path_fallback")!;
    expect(fallback.count).toBeNull();
    expect(warningTitle({code: "unlisted", scope: "project", count: null, message: "x"}))
      .toBe("Operational warning");
  });

  test("a window note appears only for a measured truncation", () => {
    expect(windowNote(1000, 1000, true)).toContain("1000");
    expect(windowNote(6, 1000, false)).toBe("");
    expect(windowNote(undefined, 1000, true)).toBe("");
  });
});

// ---------------------------------------------------------------------------
// Part 2: isolated harness
// ---------------------------------------------------------------------------

async function installFake(page: Page, bodies: Record<string, OperationsSummary | "fail">,
                           requests: string[] = []) {
  await page.route("**/v1/projects/*/operations-summary", async route => {
    const url = new URL(route.request().url());
    const project = url.pathname.split("/")[3];
    requests.push(project);
    const body = bodies[project] ?? "fail";
    if (body === "fail") {
      await route.fulfill({status: 503, contentType: "application/json",
                           body: JSON.stringify({error: {code: "operations_unavailable",
                                                         message: "operations unavailable"}})});
      return;
    }
    await route.fulfill({status: 200, contentType: "application/json",
                         body: JSON.stringify({data: body})});
  });
  return requests;
}

test.describe("operations dashboard (isolated harness)", () => {
  test.beforeAll(async () => {
    harness = spawn(process.execPath,
      ["node_modules/vite/bin/vite.js", "--port", String(HARNESS_PORT), "--strictPort", "--host", "127.0.0.1"],
      {cwd: DASHBOARD, stdio: "ignore"});
    await waitForHarness();
  });

  test.afterAll(() => {
    harness?.kill();
    harness = null;
  });

  test("measured sections render; a measured zero reads as caught up", async ({page}) => {
    await installFake(page, {"proj-1": summary()});
    await page.goto(HARNESS);
    await expect(page.getByTestId("operations-vector")).toContainText("Caught up");
    await expect(page.getByTestId("operations-graph")).toContainText("2 seq behind (2 pending)");
    await expect(page.getByTestId("operations-read-path")).toContainText("dedicated index connection");
    await expect(page.getByTestId("operations-delivery-states")).toContainText("Acked");
    await expect(page.getByTestId("operations-no-warnings")).toBeVisible();
    await expect(page.getByTestId("operations-jobs-global")).toContainText("Global (all projects");
    await expect(page.getByTestId("operations-telemetry")).toBeVisible();
    await expect(page.getByTestId("operations-run")).toHaveAttribute("data-presence", "online");
    await expect(page.getByTestId("operations-run")).toHaveAttribute("data-capture", "unknown");
  });

  test("an unavailable section renders unknown, never a false healthy", async ({page}) => {
    const broken = summary({
      delivery: {available: false, reason: "section_measurement_failed"},
      warnings: [{code: "capture_gaps_reported", scope: "project", count: 2,
                  message: "2 run(s) report capture gaps (missing source sequences)."}],
    });
    await installFake(page, {"proj-1": broken});
    await page.goto(HARNESS);
    const delivery = page.getByTestId("operations-delivery");
    await expect(delivery).toHaveAttribute("data-available", "false");
    await expect(delivery).toContainText("Unknown");
    await expect(delivery).not.toContainText("Dead letter");
    const warning = page.getByTestId("operations-warning");
    await expect(warning).toHaveAttribute("data-code", "capture_gaps_reported");
    await expect(warning).toContainText("Measured capture gaps");
  });

  test("telemetry counters are administrator-only in the view", async ({page}) => {
    const nonAdmin = summary({role: "agent",
                              jobs: {...summary().jobs, global: null},
                              telemetry: {available: false, reason: "administrator_required"}});
    await installFake(page, {"proj-1": nonAdmin});
    await page.goto(`${HARNESS}?role=agent`);
    await expect(page.getByTestId("operations-telemetry")).toHaveCount(0);
    await expect(page.getByTestId("operations-jobs-global")).toHaveCount(0);
    await expect(page.getByTestId("operations-dashboard")).toHaveAttribute("data-role", "agent");
  });

  test("a withheld node-wide metric is unknown rather than a healthy zero", async ({page}) => {
    await installFake(page, {"proj-1": summary({
      jobs: {...summary().jobs, global: null},
      telemetry: {available: true, scope: "project", queued: 0, dropped: null,
                  dropped_scope: "global_all_projects", content_capture: false},
    })});
    await page.goto(HARNESS);
    const telemetry = page.getByTestId("operations-telemetry");
    await expect(telemetry.locator(".operations-count").filter({hasText: "Dropped (node-wide)"})
      .locator(".operations-count-value")).toHaveText("Unknown");
    await expect(telemetry.locator(".operations-count").filter({hasText: "Queued"})
      .locator(".operations-count-value")).toHaveText("0");
    await expect(page.getByTestId("operations-jobs-global")).toHaveCount(0);
  });
  test("a parent resync refetches; the dashboard owns no stream", async ({page}) => {
    const requests = await installFake(page, {"proj-1": summary()});
    await page.goto(HARNESS);
    await expect(page.getByTestId("operations-generated-at")).toBeVisible();
    expect(requests).toEqual(["proj-1"]);
    await page.getByTestId("simulate-resync").click();
    await expect.poll(() => requests.length).toBe(2);
    // No background polling: nothing else arrives on its own.
    await page.waitForTimeout(400);
    expect(requests).toHaveLength(2);
  });

  test("switching project or permission drops the previous scope's evidence", async ({page}) => {
    const requests: string[] = [];
    let releaseFirst: (() => void) | null = null;
    await page.route("**/v1/projects/*/operations-summary", async route => {
      const project = new URL(route.request().url()).pathname.split("/")[3];
      requests.push(project);
      if (project === "proj-1" && requests.length === 1) {
        // A slow first response must never land after the scope changed.
        await new Promise<void>(resolve => { releaseFirst = resolve; });
      }
      await route.fulfill({status: 200, contentType: "application/json",
                           body: JSON.stringify({data: summary({project_id: project})})});
    });
    await page.goto(HARNESS);
    await page.getByTestId("switch-project").click();
    await expect(page.getByTestId("harness-scope")).toHaveAttribute("data-project", "proj-2");
    await expect.poll(() => requests).toContain("proj-2");
    releaseFirst!();
    // Evidence shown belongs to proj-2; the late proj-1 sample is dropped.
    await expect(page.getByTestId("operations-generated-at")).toBeVisible();
    await expect(page.getByTestId("operations-dashboard")).toHaveAttribute("data-project", "proj-2");
    await page.getByTestId("rotate-permission").click();
    await expect.poll(() => requests.length).toBe(3);
  });

  test("a failed read drops evidence and reports a coarse error", async ({page}) => {
    await installFake(page, {"proj-1": "fail"});
    await page.goto(HARNESS);
    await expect(page.getByTestId("operations-error")).toBeVisible();
    await expect(page.getByTestId("harness-error")).toContainText("503");
    await expect(page.getByTestId("operations-ingestion")).toHaveCount(0);
    await expect(page.getByTestId("operations-warnings")).toHaveCount(0);
  });

  test("opening an agent from the run sample uses the callback", async ({page}) => {
    await installFake(page, {"proj-1": summary()});
    await page.goto(HARNESS);
    await page.getByTestId("operations-run").getByRole("button", {name: "Researcher"}).click();
    await expect(page.getByTestId("harness-opened")).toHaveText("agent-1");
  });

  test("dead-letter counts and truncation notes surface as measured", async ({page}) => {
    const degraded = summary({
      delivery: {available: true, scope: "authorized_principal",
                 states: {available: 0, leased: 0, retry_wait: 1, dead_letter: 3, expired: 0,
                          acked: 0},
                 scanned: 1000, limit: 1000, truncated: true, window_note: "bounded window"},
      warnings: [
        {code: "dead_letter_messages", scope: "project", count: 3,
         message: "3 message(s) in the authorized window reached dead letter."},
        {code: "delivery_window_truncated", scope: "project", count: 1000,
         message: "Delivery counts cover only the newest bounded window."},
      ],
    });
    await installFake(page, {"proj-1": degraded});
    await page.goto(HARNESS);
    await expect(page.getByTestId("operations-delivery-truncated")).toContainText("1000-row window");
    const warnings = page.getByTestId("operations-warning");
    await expect(warnings).toHaveCount(2);
    await expect(warnings.first()).toContainText("Undeliverable messages");
  });
});
