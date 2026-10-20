import {test, expect, type Page} from "@playwright/test";
import {spawn, type ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";
import {
  availableModes, buildRequest, clampHops, clampLimit, classifyFailure, degradationNotes,
  explorePath, graphSummary, indexStateLabel, statusPath, validateQuery, MAX_QUERY_CHARS,
  type ExploreResponse, type ExploreStatus,
} from "../src/context-explorer";

/*
 * Context explorer.
 *
 * Part 1 covers the pure contracts in-process (no network, no harness). Part 2
 * mounts the component in an isolated vite dev server on port 5212, clear of the
 * other harnesses (5197/5199/5201/5205) and the python fixture (8766). Every
 * request is stubbed by an in-spec fake server that mirrors the semantics of
 * hivemind/context_explorer.py, so no real store, encoder or graph database is
 * touched and this spec can run before main.tsx wires /projects/{id}/context.
 * Run with the temp no-webServer config (see README note in the spec header of
 * sibling specs): the default playwright webServer fixture is not used here.
 */

const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5212;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/context-explorer.html`;
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

function statusFixture(overrides: Partial<ExploreStatus> = {}): ExploreStatus {
  return {
    project_id: "proj-1", available: true, semantic_available: false,
    graph_available: false, modes: ["lexical"],
    freshness: {canonical_through_ingestion_seq: 7, vector_through_ingestion_seq: 7,
      vector_pending_jobs: 0, graph_through_ingestion_seq: 7, graph_pending_jobs: 0},
    projections: {vector_pending_jobs: 0, graph_pending_jobs: 0, stale: false, recheck: "x"},
    limits: {max_results: 50, max_graph_seeds: 8, max_hops: 3, max_nodes: 100, max_edges: 200},
    evidence_policy: "results_and_graph_include_only_currently_authorized_resources",
    ...overrides,
  };
}

function exploreFixture(overrides: Partial<ExploreResponse> = {}): ExploreResponse {
  return {
    project_id: "proj-1", mode_requested: "hybrid", mode_effective: "lexical_fallback",
    results: [{
      resource_id: "board_post_seed", kind: "board_post", revision: 1, score: 0.91,
      trust: "structured", title: "Seed finding", snippet: "quasar lattice",
      current_at_snapshot: true,
    }],
    result_count: 1,
    graph: {
      seed_ids: ["board_post_seed"],
      nodes: [
        {id: "board_post_seed", kind: "board_post", revision: 1, title: "Seed finding"},
        {id: "agent_explorer", kind: "agent", revision: 2, title: "Explorer"},
      ],
      edges: [{source: "board_post_seed", target: "agent_explorer", kind: "evidence"}],
      max_hops: 1, graph_fallback: true, truncated: false, warnings: [],
      scope: "authorized_resources_only",
    },
    freshness: {canonical_through_ingestion_seq: 9, vector_pending_jobs: 0,
      graph_pending_jobs: 0},
    projections: {vector_pending_jobs: 0, graph_pending_jobs: 0, stale: false,
      recheck: "rechecked"},
    embedding_profile: "eg2-text-768-v1", semantic_available: false,
    graph_available: false, truncated: false, scan: null,
    warnings: ["semantic index unavailable; truthful lexical fallback"],
    limits: {max_results: 50, max_graph_seeds: 8, max_hops: 3, max_nodes: 100,
      max_edges: 200, snippet_chars: 280},
    evidence_policy: "results_and_graph_include_only_currently_authorized_resources",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Part 1: pure contracts
// ---------------------------------------------------------------------------

test.describe("context explorer contracts", () => {
  test("query validation is explicit-submit and bounded", () => {
    expect(validateQuery("  ")).toEqual({ok: false, message: "Enter a query to explore."});
    expect(validateQuery("  quasar  ")).toEqual({ok: true, query: "quasar"});
    const tooLong = validateQuery("x".repeat(MAX_QUERY_CHARS + 1));
    expect(tooLong.ok).toBe(false);
  });

  test("requests are bounded to server ceilings", () => {
    const request = buildRequest({query: "q", mode: "hybrid", limit: 5000, maxHops: 99,
      graphSeeds: 99});
    expect(request).toMatchObject({limit: 50, max_hops: 3, graph_seeds: 8,
      expand_graph: true});
    expect(buildRequest({query: " ", mode: "lexical"})).toHaveProperty("error");
    expect(clampLimit(Number.NaN)).toBe(10);
    expect(clampHops(0)).toBe(1);
  });

  test("paths target the project-scoped routes", () => {
    expect(explorePath("proj 1")).toBe("projects/proj%201/context-explore");
    expect(statusPath("p")).toBe("projects/p/context-explore:status");
  });

  test("mode offering never promises semantic without an encoder", () => {
    expect(availableModes(statusFixture({semantic_available: false, modes: ["lexical"]})))
      .toEqual(["lexical"]);
    expect(availableModes(statusFixture({semantic_available: true,
      modes: ["lexical", "semantic", "hybrid"]}))).toContain("hybrid");
    expect(availableModes(null)).toContain("hybrid");
  });

  test("degradation labels are derived from server flags, never hidden", () => {
    const response = exploreFixture();
    expect(indexStateLabel(response)).toContain("lexical fallback");
    const notes = degradationNotes(response);
    expect(notes.some(note => note.includes("lexical"))).toBe(true);
    expect(notes.some(note => note.includes("canonical fallback"))).toBe(true);

    const stale = degradationNotes(exploreFixture({
      projections: {vector_pending_jobs: 3, graph_pending_jobs: 0, stale: true,
        recheck: "x"},
      truncated: true,
    }));
    expect(stale.some(note => note.includes("catching up"))).toBe(true);
    expect(stale.some(note => note.includes("bounded"))).toBe(true);
  });

  test("graph summary counts only returned authorized items", () => {
    expect(graphSummary(null)).toBe("");
    expect(graphSummary(exploreFixture().graph)).toContain("2 authorized resources");
    expect(graphSummary({...exploreFixture().graph!, truncated: true}))
      .toContain("truncated");
  });

  test("auth failures demand evidence clearing; other errors do not", () => {
    expect(classifyFailure({status: 401}).clearEvidence).toBe(true);
    expect(classifyFailure({status: 403}).clearEvidence).toBe(true);
    expect(classifyFailure({status: 500}).clearEvidence).toBe(false);
    expect(classifyFailure(new Error("boom")).message).toContain("boom");
  });
});

// ---------------------------------------------------------------------------
// Part 2: component against a fake canonical server
// ---------------------------------------------------------------------------

type Fake = {
  explores: {path: string; body: Record<string, unknown>}[];
  statuses: number;
  failStatus: number | null;
  response: ExploreResponse;
};

function fake(): Fake {
  return {explores: [], statuses: 0, failStatus: null, response: exploreFixture()};
}

async function installFake(page: Page, server: Fake) {
  await page.route(/\/v1\/streams/, route => route.abort());
  await page.route(/\/v1\/projects\/[^/]+\/context-explore:status/, route => {
    server.statuses += 1;
    if (server.failStatus) {
      return route.fulfill({status: server.failStatus,
        json: {error: {code: "forbidden", message: "denied", retryable: false}}});
    }
    return route.fulfill({json: {data: statusFixture()}});
  });
  await page.route(/\/v1\/projects\/[^/]+\/context-explore$/, async route => {
    server.explores.push({path: route.request().url(),
      body: route.request().postDataJSON() as Record<string, unknown>});
    if (server.failStatus) {
      return route.fulfill({status: server.failStatus,
        json: {error: {code: "forbidden", message: "denied", retryable: false}}});
    }
    return route.fulfill({json: {data: server.response}});
  });
}

test.describe("context explorer (isolated harness)", () => {
  test.beforeAll(async () => {
    harness = spawn(process.execPath,
      ["node_modules/vite/bin/vite.js", "--port", String(HARNESS_PORT), "--strictPort",
        "--host", "127.0.0.1"],
      {cwd: DASHBOARD, stdio: "ignore"});
    await waitForHarness();
  });

  test.afterAll(() => {
    harness?.kill();
    harness = null;
  });

  test("typing never searches; only explicit submit posts one bounded request", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await expect(page.getByTestId("context-explorer-status")).toContainText("Lexical/canonical");

    await page.getByTestId("context-explorer-query").fill("quasar lattice");
    expect(server.explores).toEqual([]);
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);
    expect(server.explores).toHaveLength(1);
    expect(server.explores[0].body).toMatchObject({query: "quasar lattice",
      mode: "lexical", limit: 10, expand_graph: true, max_hops: 1});
    // The body carries no project override; the project is the path parameter.
    expect(server.explores[0].body.project_id).toBeUndefined();
    expect(server.explores[0].path).toContain("/v1/projects/proj-1/context-explore");
  });

  test("empty submit is a local validation error with no request", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-notice")).toContainText("Enter a query");
    expect(server.explores).toEqual([]);
  });

  test("results, graph and degradation notes render as plain text with evidence ids", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-query").fill("quasar");
    await page.getByTestId("context-explorer-submit").click();

    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);
    await expect(page.getByTestId("context-explorer-result").first())
      .toContainText("board_post_seed");
    await expect(page.getByTestId("context-explorer-notes")).toContainText("lexical");
    await expect(page.getByTestId("context-explorer-graph-summary"))
      .toContainText("2 authorized resources");
    await expect(page.getByTestId("context-explorer-edge")).toHaveCount(1);
    // Evidence stays an authorized resource id, rendered as code text, not markup.
    const snippet = page.locator(".context-explorer-snippet").first();
    expect(await snippet.evaluate(node => node.innerHTML)).not.toContain("<");
  });

  test("agent nodes offer the open-agent callback", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-query").fill("quasar");
    await page.getByTestId("context-explorer-submit").click();
    const openButton = page.locator('[data-testid="context-explorer-open-agent"]' +
      '[data-agent-id="agent_explorer"]');
    await expect(openButton).toHaveCount(1);
    await openButton.click();
    await expect(page.getByTestId("harness-opened")).toHaveText("agent_explorer");
  });

  test("an authorization failure clears all evidence and reports via onError", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-query").fill("quasar");
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);

    server.failStatus = 403;
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(0);
    await expect(page.getByTestId("context-explorer-results")).toHaveCount(0);
    await expect(page.getByTestId("harness-error")).toContainText("Authorization changed");
  });

  test("permission rotation clears evidence; resync re-runs the in-memory query", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-query").fill("quasar");
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);

    await page.getByTestId("rotate-permission").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(0);

    await page.getByTestId("context-explorer-query").fill("quasar again");
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);
    const before = server.explores.length;
    await page.getByTestId("simulate-resync").click();
    await expect.poll(() => server.explores.length).toBe(before + 1);
    expect(server.explores[server.explores.length - 1].body.query).toBe("quasar again");
  });

  test("stale source deletion does not survive a resync", async ({page}) => {
    const server = fake();
    await installFake(page, server);
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-query").fill("quasar");
    await page.getByTestId("context-explorer-submit").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);

    // The canonical store drops the source; the resync re-run must not keep it.
    server.response = exploreFixture({results: [], result_count: 0, graph: null});
    await page.getByTestId("simulate-resync").click();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(0);
    await expect(page.getByTestId("context-explorer-meta")).toContainText("0 results");
  });

  test("a stale response from a superseded search is fenced out", async ({page}) => {
    const server = fake();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    await page.route(/\/v1\/streams/, route => route.abort());
    await page.route(/\/v1\/projects\/[^/]+\/context-explore:status/,
      route => route.fulfill({json: {data: statusFixture()}}));
    let calls = 0;
    await page.route(/\/v1\/projects\/[^/]+\/context-explore$/, async route => {
      calls += 1;
      if (calls === 1) await gate; // hold the first response until the second lands
      await route.fulfill({json: {data: exploreFixture({
        results: [{resource_id: `board_post_call_${calls}`, kind: "board_post",
          revision: 1, score: 0.5, trust: "structured", title: null, snippet: "x",
          current_at_snapshot: true}],
      })}});
    });
    await page.goto(HARNESS);
    await page.getByTestId("context-explorer-query").fill("first query");
    await page.getByTestId("context-explorer-submit").click();
    await page.getByTestId("context-explorer-query").fill("second query");
    await page.getByTestId("context-explorer-submit").click();
    release();
    await expect(page.getByTestId("context-explorer-result")).toHaveCount(1);
    await expect(page.getByTestId("context-explorer-result").first())
      .toHaveAttribute("data-resource-id", "board_post_call_2");
  });
});
