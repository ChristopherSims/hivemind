import {test, expect, Page} from "@playwright/test";
import {spawn, ChildProcess} from "node:child_process";
import {fileURLToPath} from "node:url";
import {
  confirmationMatches, deletionConfirmation, deletionPath, exportConfirmation,
  exportPayloadSafe, normalizeAudit, normalizeExports, normalizePermissions,
  normalizePreview, normalizeRetention, policySummary,
} from "../src/privacy-audit";

// ---------------------------------------------------------------------------
// Pure logic. No browser, no fixture, no shared store.
// ---------------------------------------------------------------------------

test("confirmations are exact, target-bound strings", () => {
  expect(exportConfirmation("project-1")).toBe("EXPORT project-1");
  expect(deletionConfirmation("mem_1")).toBe("DELETE mem_1");
  expect(confirmationMatches("EXPORT project-1", "EXPORT project-1")).toBe(true);
  expect(confirmationMatches(" EXPORT project-1 ", "EXPORT project-1")).toBe(true);
  expect(confirmationMatches("EXPORT project-2", "EXPORT project-1")).toBe(false);
  expect(confirmationMatches("export project-1", "EXPORT project-1")).toBe(false);
  expect(confirmationMatches("", "EXPORT project-1")).toBe(false);
});

test("permission normalization keeps only whitelisted fields", () => {
  const result = normalizePermissions({
    project_id: "project-1",
    generated_at: "2026-01-01T00:00:00Z",
    teams: [{id: "team_1", title: "Core", active: true,
             member_agent_ids: ["agent_alpha00001"], revision: 3, secret: "drop-me"}],
    resources: [{
      resource_id: "mem_1", kind: "memory", project_id: "project-1",
      title: "Fact", revision: 2, deletable_via: "DELETE /v1/memories/{resource_id}",
      body: "private content", token_hash: "abc",
      policy: {revision: 4, public_read: false, public_contribute: false,
               reader_agent_ids: ["agent_alpha00001"], contributor_agent_ids: [],
               reader_team_ids: [], contributor_team_ids: [], backdoor: true},
    }, {kind: "memory"}, null],
    truncated: true,
    policy_kinds: ["memory"],
    administration: "use PUT access",
  });
  expect(result.truncated).toBe(true);
  expect(result.resources).toHaveLength(1);
  const entry = result.resources[0];
  expect(Object.keys(entry).sort()).toEqual(
    ["deletable_via", "kind", "policy", "project_id", "resource_id", "revision", "title"]);
  expect(Object.keys(entry.policy).sort()).toEqual([
    "contributor_agent_ids", "contributor_team_ids", "public_contribute", "public_read",
    "reader_agent_ids", "reader_team_ids", "revision"]);
  expect(JSON.stringify(result)).not.toContain("private content");
  expect(JSON.stringify(result)).not.toContain("backdoor");
  expect(Object.keys(result.teams[0]).sort()).toEqual(
    ["active", "id", "member_agent_ids", "revision", "title"]);
  expect(policySummary(entry.policy))
    .toBe("restricted read · restricted contribute · 1 explicit grant");
  expect(policySummary(normalizePermissions({resources: [{
    resource_id: "b", kind: "board", policy: {revision: null, public_read: true,
      public_contribute: true}}]}).resources[0].policy))
    .toBe("public read · public contribute · default policy");
});

test("audit normalization drops malformed events and strips extra fields", () => {
  const result = normalizeAudit({
    project_id: "project-1",
    events: [
      {id: "aud_1", operation: "delete", resource_id: "mem_1", resource_kind: "memory",
       resource_deleted: true, principal_role: "admin", time: "2026-01-01T00:00:00Z",
       payload: "raw body", principal: "principal_1"},
      {id: "aud_2", operation: "delete"},  // incomplete -> dropped
      "nope",
    ],
    truncated: false, scope: "canonical_audit_metadata_only",
  });
  expect(result.events).toHaveLength(1);
  const event = result.events[0];
  expect(Object.keys(event).sort()).toEqual([
    "id", "operation", "principal_role", "resource_deleted", "resource_id",
    "resource_kind", "time"]);
  expect(JSON.stringify(result)).not.toContain("raw body");
  expect(JSON.stringify(result)).not.toContain("principal_1");
});

test("retention normalization preserves disabled boundaries with reasons", () => {
  const result = normalizeRetention({project_id: "project-1", capabilities: {
    logical_deletion: {supported: true, mechanism: "canonical delete"},
    physical_erasure: {supported: false, reason: "no compaction job"},
    unexpected_capability: {supported: true},  // not in the known order -> dropped
  }});
  expect(result.capabilities.logical_deletion.supported).toBe(true);
  expect(result.capabilities.physical_erasure.supported).toBe(false);
  expect(result.capabilities.physical_erasure.reason).toBe("no compaction job");
  expect(result.capabilities.unexpected_capability).toBeUndefined();
  // Missing capabilities normalize to a disabled, explained absence.
  expect(result.capabilities.cache_purge.supported).toBe(false);
});

test("preview normalization and deletion paths cover only canonical kinds", () => {
  const preview = normalizePreview({
    resource_id: "mem_1", kind: "memory", project_id: "project-1", revision: 2,
    deletable: true, endpoint: "DELETE /v1/memories/mem_1",
    dependent_memories: [{id: "mem_2", kind: "memory", title: "Dependent",
                          effect: "invalidated"}],
    dependent_count: 1, dependents_truncated: false,
    consequences: ["logical deletion of the resource"],
    not_performed: ["physical erasure of store bytes"],
  })!;
  expect(preview.deletable).toBe(true);
  expect(deletionPath(preview)).toBe("memories/mem_1");
  expect(deletionPath(normalizePreview({resource_id: "p", kind: "board_post",
    deletable: true})!)).toBe("posts/p");
  expect(deletionPath(normalizePreview({resource_id: "b", kind: "board",
    deletable: false, unsupported_reason: "no canonical endpoint"})!)).toBeNull();
  expect(deletionPath(normalizePreview({resource_id: "t", kind: "task",
    deletable: true})!)).toBeNull();  // no canonical endpoint -> never invent one
  expect(normalizePreview({kind: "memory"})).toBeNull();
});

test("export payloads are whitelisted and descriptor normalization is minimal", () => {
  expect(exportPayloadSafe(null)).toBe(false);
  expect(exportPayloadSafe({export_id: "e", permissions: {}, audit: {}})).toBe(true);
  expect(exportPayloadSafe({export_id: "e", owner_json: {}})).toBe(false);
  expect(exportPayloadSafe({export_id: "e", filesystem_paths: []})).toBe(false);
  const list = normalizeExports({project_id: "project-1", exports: [
    {id: "pe_1", project_id: "project-1", created_at: "2026-01-01T00:00:00Z",
     state: "available", revision: 1, scope: "operator_privacy_audit", payload: "nope"},
  ]});
  expect(Object.keys(list.exports[0]).sort()).toEqual(
    ["created_at", "id", "project_id", "revision", "scope", "state"]);
});

// ---------------------------------------------------------------------------
// Isolated harness on the reserved port 5214 (clear of 5197/5199/5202 and the
// python fixture on 8766). Own vite process, started and stopped by this file;
// every network call is stubbed. Synthetic data only.
// ---------------------------------------------------------------------------
const DASHBOARD = fileURLToPath(new URL("..", import.meta.url));
const HARNESS_PORT = 5214;
const HARNESS = `http://127.0.0.1:${HARNESS_PORT}/tests/harness/privacy-audit.html`;
let harness: ChildProcess | null = null;

async function waitForHarness() {
  for (let attempt = 0; attempt < 80; attempt++) {
    try {
      if ((await fetch(HARNESS)).ok) return;
    } catch {
      // The dev server is still starting.
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Harness server did not start at ${HARNESS}`);
}

const PERMISSIONS = {
  project_id: "project-1", generated_at: "2026-01-01T00:00:00Z", truncated: false,
  policy_kinds: ["board", "board_post", "artifact", "memory", "task"],
  administration: "Policies are changed only via PUT /v1/resources/{resource_id}/access.",
  teams: [{id: "team_1", title: "Core team", active: true,
           member_agent_ids: ["agent_alpha00001", "agent_beta000002"], revision: 2}],
  resources: [
    {resource_id: "mem_1", kind: "memory", project_id: "project-1", title: "Header fact",
     revision: 3, deletable_via: "DELETE /v1/memories/{resource_id}",
     policy: {revision: 1, public_read: false, public_contribute: false,
              reader_agent_ids: ["agent_alpha00001"], contributor_agent_ids: [],
              reader_team_ids: [], contributor_team_ids: []}},
    {resource_id: "board_1", kind: "board", project_id: "project-1", title: "Main board",
     revision: 1, deletable_via: null,
     policy: {revision: null, public_read: true, public_contribute: true,
              reader_agent_ids: [], contributor_agent_ids: [],
              reader_team_ids: [], contributor_team_ids: []}},
    {resource_id: "post_1", kind: "board_post", project_id: "project-1", title: "Temporary post",
     revision: 1, deletable_via: "DELETE /v1/posts/{resource_id}",
     policy: {revision: null, public_read: true, public_contribute: true,
              reader_agent_ids: [], contributor_agent_ids: [],
              reader_team_ids: [], contributor_team_ids: []}},
  ],
};

const AUDIT = {
  project_id: "project-1", generated_at: "2026-01-01T00:00:00Z", truncated: false,
  scope: "canonical_audit_metadata_only",
  excludes: ["command payloads", "tokens"],
  events: [
    {id: "aud_1", operation: "resource_access.update", resource_id: "board_1",
     resource_kind: "board", resource_deleted: false, principal_role: "admin",
     time: "2026-01-01T00:00:00Z"},
    {id: "aud_2", operation: "delete", resource_id: "mem_0", resource_kind: "memory",
     resource_deleted: true, principal_role: "human", time: "2026-01-01T00:01:00Z"},
  ],
};

const RETENTION = {
  project_id: "project-1", generated_at: "2026-01-01T00:00:00Z",
  capabilities: {
    logical_deletion: {supported: true, mechanism: "canonical Store.delete"},
    source_removal: {supported: true, mechanism: "same canonical delete"},
    index_invalidation: {supported: true, semantics: "pending jobs"},
    memory_expiry: {supported: true, mechanism: "expires_at"},
    physical_erasure: {supported: false, reason: "No compaction job exists in this build."},
    cache_purge: {supported: false, reason: "No cache-invalidation command exists."},
    external_provider_deletion: {supported: false, reason: "Provider deletion is unverifiable."},
    background_retention_sweep: {supported: false, reason: "No background sweep runs."},
  },
};

const PREVIEW = {
  resource_id: "mem_1", project_id: "project-1", kind: "memory", revision: 3,
  title: "Header fact", deletable: true, endpoint: "DELETE /v1/memories/mem_1",
  unsupported_reason: null,
  dependent_memories: [{id: "mem_2", kind: "memory", title: "Dependent fact",
                        effect: "invalidated (status=disputed)"}],
  dependent_count: 1, dependents_truncated: false,
  consequences: ["logical deletion of the resource",
                 "synchronous purge of its versioned content history"],
  not_performed: ["physical erasure of store bytes", "cache purge",
                  "external provider deletion"],
};

type MockOptions = {
  permissions?: () => {status: number; json: unknown};
  audit?: () => {status: number; json: unknown};
  retention?: () => {status: number; json: unknown};
  exports?: () => {status: number; json: unknown};
  preview?: () => {status: number; json: unknown};
  /** Held open until the test releases it, to observe in-flight cancellation. */
  previewGate?: Promise<void>;
  downloadGate?: Promise<void>;
  onDelete?: (body: unknown, key: string | null) => {status: number; json: unknown};
  onExportCreate?: (body: unknown, key: string | null) => {status: number; json: unknown};
  onDownload?: () => {status: number; json: unknown};
};

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return {promise, release};
}

async function installMocks(page: Page, options: MockOptions = {}) {
  const calls = {deletes: [] as {body: unknown; key: string | null}[],
                 exports: [] as {body: unknown; key: string | null}[], downloads: 0};
  // Playwright consults routes in reverse registration order: the catch-all
  // goes first so the specific stubs below always win.
  await page.route("**/v1/**", route =>
    route.fulfill({status: 404, json: {error: {message: "not_found"}}}));
  await page.route("**/v1/projects/*/privacy-audit/permissions**", route => {
    const reply = options.permissions?.() ?? {status: 200, json: {data: PERMISSIONS}};
    return route.fulfill({status: reply.status, json: reply.json});
  });
  await page.route("**/v1/projects/*/privacy-audit/audit**", route => {
    const reply = options.audit?.() ?? {status: 200, json: {data: AUDIT}};
    return route.fulfill({status: reply.status, json: reply.json});
  });
  await page.route("**/v1/projects/*/privacy-audit/retention**", route => {
    const reply = options.retention?.() ?? {status: 200, json: {data: RETENTION}};
    return route.fulfill({status: reply.status, json: reply.json});
  });
  await page.route("**/v1/projects/*/privacy-audit/deletion-preview**", async route => {
    await options.previewGate;
    if (options.preview) {
      const reply = options.preview();
      return route.fulfill({status: reply.status, json: reply.json});
    }
    // Echo the requested resource so selection-change tests can tell targets apart.
    const id = new URL(route.request().url()).searchParams.get("resource_id") ?? "mem_1";
    const kind = id.startsWith("post_") ? "board_post" : "memory";
    return route.fulfill({json: {data: {...PREVIEW, resource_id: id, kind,
      endpoint: `DELETE /v1/${kind === "memory" ? "memories" : "posts"}/${id}`}}});
  });
  await page.route("**/v1/projects/*/privacy-audit/exports", route => {
    if (route.request().method() === "POST") {
      const key = route.request().headers()["idempotency-key"] ?? null;
      const body = route.request().postDataJSON();
      calls.exports.push({body, key});
      const reply = options.onExportCreate?.(body, key) ?? {status: 200, json: {data: {
        id: "pe_1", project_id: "project-1", scope: "operator_privacy_audit",
        state: "available", payload_storage: "none_regenerated_at_download",
      }}};
      return route.fulfill({status: reply.status, json: reply.json});
    }
    const reply = options.exports?.() ?? {status: 200, json: {data: {
      project_id: "project-1",
      exports: [{id: "pe_1", project_id: "project-1", created_at: "2026-01-01T00:00:00Z",
                 state: "available", revision: 1, scope: "operator_privacy_audit"}],
    }}};
    return route.fulfill({status: reply.status, json: reply.json});
  });
  await page.route("**/v1/memories/*", route => {
    if (route.request().method() === "DELETE") {
      const key = route.request().headers()["idempotency-key"] ?? null;
      const body = route.request().postDataJSON();
      calls.deletes.push({body, key});
      const reply = options.onDelete?.(body, key) ?? {status: 200, json: {
        data: {deleted: true, derived_cleanup: "pending"}}};
      return route.fulfill({status: reply.status, json: reply.json});
    }
    return route.fulfill({status: 404, json: {error: {message: "not_found"}}});
  });
  await page.route("**/v1/privacy-exports/*/download", async route => {
    await options.downloadGate;
    calls.downloads += 1;
    const reply = options.onDownload?.() ?? {status: 200, json: {data: {
      export_id: "pe_1", project_id: "project-1", scope: "operator_privacy_audit",
      generated_at: "2026-01-01T00:02:00Z", principal_bound: true, regenerated: true,
      permissions: PERMISSIONS, audit: AUDIT, truncated: false,
      excludes: ["resource bodies and transcripts"], external_sharing: "not_performed",
    }}};
    return route.fulfill({status: reply.status, json: reply.json});
  });
  return calls;
}

test.describe("privacy & audit screens (isolated harness)", () => {
  test.beforeAll(async () => {
    harness = spawn(process.execPath,
      ["node_modules/vite/bin/vite.js", "--port", String(HARNESS_PORT),
       "--strictPort", "--host", "127.0.0.1"],
      {cwd: DASHBOARD, stdio: "ignore"});
    await waitForHarness();
  });

  test.afterAll(() => {
    harness?.kill();
    harness = null;
  });

  test("permission review renders canonical policies, teams and grant links", async ({page}) => {
    await installMocks(page);
    await page.goto(HARNESS);
    const panel = page.getByTestId("pa-permissions");
    await expect(panel.getByTestId("pa-team")).toHaveCount(1);
    await expect(panel.getByTestId("pa-team")).toContainText("Core team");
    await expect(panel.getByTestId("pa-team")).toContainText("2 members");
    const rows = panel.getByTestId("pa-resource");
    await expect(rows).toHaveCount(3);
    await expect(rows.first()).toContainText("restricted read · restricted contribute");
    await expect(rows.nth(1)).toContainText("public read · public contribute · default policy");
    // Grant links call back into the host instead of duplicating administration.
    await panel.getByRole("button", {name: "Alpha worker"}).click();
    await expect(page.getByTestId("log")).toContainText("open:agent_alpha00001");
    await expect(panel).toContainText("PUT /v1/resources/{resource_id}/access");
  });

  test("audit history shows whitelisted metadata only", async ({page}) => {
    await installMocks(page);
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Audit history"}).click();
    const panel = page.getByTestId("pa-audit");
    const events = panel.getByTestId("pa-audit-event");
    await expect(events).toHaveCount(2);
    await expect(events.first()).toContainText("resource_access.update");
    await expect(events.first()).toContainText("by admin");
    await expect(events.nth(1)).toContainText("deleted");
    await expect(panel).toContainText("Audit events are records, not permissions");
  });

  test("deletion requires scope review and an exact typed confirmation", async ({page}) => {
    const calls = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Retention & deletion"}).click();
    // Disabled boundaries are shown honestly, never faked.
    await expect(page.getByTestId("pa-cap-physical_erasure")).toContainText("not supported");
    await expect(page.getByTestId("pa-cap-physical_erasure")).toContainText("No compaction job");
    await expect(page.getByTestId("pa-cap-logical_deletion")).toContainText("supported");

    const danger = page.getByRole("button", {name: "Delete via canonical lifecycle"});
    await expect(danger).toHaveCount(0);  // no delete control before a review
    await page.getByLabel("Resource to delete").selectOption("mem_1");
    await page.getByRole("button", {name: "Review affected scope"}).click();
    const preview = page.getByTestId("pa-preview");
    await expect(preview).toContainText("1 dependent memory will be invalidated (not erased)");
    await expect(preview).toContainText("mem_2");
    await expect(preview).toContainText("Not performed: physical erasure");

    await expect(danger).toBeDisabled();
    await page.getByLabel("Deletion confirmation").fill("DELETE something-else");
    await expect(danger).toBeDisabled();
    await page.getByLabel("Deletion confirmation").fill("DELETE mem_1");
    await danger.click();
    await expect(page.getByTestId("pa-delete-status")).toContainText("logically deleted");
    await expect(page.getByTestId("pa-delete-status")).toContainText("index cleanup pending");
    expect(calls.deletes).toHaveLength(1);
    expect(calls.deletes[0].body).toEqual({expected_revision: 3});
    expect(calls.deletes[0].key).toBeTruthy();
  });

  test("a failed deletion sends nothing without review and reports the error", async ({page}) => {
    const calls = await installMocks(page, {
      onDelete: () => ({status: 412, json: {error: {message: "revision_conflict"}}}),
    });
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Retention & deletion"}).click();
    // The confirmation gate blocks submission: no request leaves before review.
    await page.getByLabel("Resource to delete").selectOption("mem_1");
    expect(calls.deletes).toHaveLength(0);
    await page.getByRole("button", {name: "Review affected scope"}).click();
    await page.getByLabel("Deletion confirmation").fill("DELETE mem_1");
    await page.getByRole("button", {name: "Delete via canonical lifecycle"}).click();
    await expect(page.getByRole("alert")).toContainText("revision_conflict");
    await expect(page.getByRole("alert")).toContainText("Review the affected scope again");
    expect(calls.deletes).toHaveLength(1);
  });

  test("exports require typed confirmation and download is an explicit re-authorized action", async ({page}) => {
    const calls = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Exports"}).click();
    const create = page.getByRole("button", {name: "Create export descriptor"});
    await expect(create).toBeDisabled();
    await page.getByLabel("Export confirmation").fill("EXPORT project-2");
    await expect(create).toBeDisabled();  // project-bound: a different project does not confirm
    await page.getByLabel("Export confirmation").fill("EXPORT project-1");
    await create.click();
    await expect(page.getByTestId("pa-export-status")).toContainText("re-checks your access");
    expect(calls.exports).toHaveLength(1);
    expect(calls.exports[0].body).toEqual({confirmation: "EXPORT project-1"});
    expect(calls.exports[0].key).toBeTruthy();

    expect(calls.downloads).toBe(0);  // never a background download
    const download = page.getByRole("button", {name: "Download (re-authorized)"});
    const [file] = await Promise.all([page.waitForEvent("download"), download.click()]);
    expect(file.suggestedFilename()).toMatch(/^privacy-audit-export-.*\.json$/);
    await expect(page.getByTestId("pa-export-status")).toContainText("Downloaded export pe_1");
    expect(calls.downloads).toBe(1);
  });

  test("a download authorization failure reports and never saves a file", async ({page}) => {
    const calls = await installMocks(page, {
      onDownload: () => ({status: 403, json: {error: {message: "operator_required"}}}),
    });
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Exports"}).click();
    await page.getByRole("button", {name: "Download (re-authorized)"}).click();
    await expect(page.getByRole("alert")).toContainText("operator_required");
    await expect(page.getByTestId("pa-export-status")).toHaveCount(0);
    expect(calls.downloads).toBe(1);  // attempted, re-authorized, denied: no artifact
  });

  test("an auth failure clears every scoped section", async ({page}) => {
    let failReads = false;
    await installMocks(page, {
      permissions: () => failReads
        ? {status: 401, json: {error: {message: "authentication_required"}}}
        : {status: 200, json: {data: PERMISSIONS}},
    });
    await page.goto(HARNESS);
    await expect(page.getByTestId("pa-resource")).toHaveCount(3);
    await page.getByRole("tab", {name: "Audit history"}).click();
    await expect(page.getByTestId("pa-audit-event")).toHaveCount(2);

    failReads = true;
    await page.getByTestId("bump-refresh").click();
    await expect(page.getByRole("alert")).toContainText("authentication_required");
    // Every scoped section is cleared, in the current tab and the others.
    await expect(page.getByTestId("pa-audit-event")).toHaveCount(0);
    await page.getByRole("tab", {name: "Permission review"}).click();
    await expect(page.getByTestId("pa-resource")).toHaveCount(0);
    await page.getByRole("tab", {name: "Exports"}).click();
    await expect(page.getByTestId("pa-export")).toHaveCount(0);
  });

  test("switching project or session fences stale loads", async ({page}) => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let permissionsCalls = 0;
    // Catch-all first: later, more specific stubs take precedence.
    await page.route("**/v1/**", route =>
      route.fulfill({status: 404, json: {error: {message: "not_found"}}}));
    await page.route("**/v1/projects/*/privacy-audit/permissions**", async route => {
      permissionsCalls += 1;
      if (permissionsCalls === 1) await gate;  // hold the first load open
      return route.fulfill({status: 200, json: {data: PERMISSIONS}});
    });
    await page.route("**/v1/projects/*/privacy-audit/audit**", route =>
      route.fulfill({json: {data: AUDIT}}));
    await page.route("**/v1/projects/*/privacy-audit/retention**", route =>
      route.fulfill({json: {data: RETENTION}}));
    await page.route("**/v1/projects/*/privacy-audit/exports", route =>
      route.fulfill({json: {data: {project_id: "project-1", exports: []}}}));

    await page.goto(HARNESS);
    // Switch project while the first load is in flight, then release it: the
    // stale completion must not paint project-1 data under project-2.
    await page.getByTestId("switch-project").click();
    release();
    await expect(page.getByTestId("pa-resource")).toHaveCount(3);
    await page.getByTestId("switch-session").click();
    await expect(page.getByTestId("pa-resource")).toHaveCount(3);
  });

  test("non-operator roles see a disabled boundary, not data", async ({page}) => {
    await page.goto(HARNESS);  // no mocks: nothing may be fetched at all
    await page.getByTestId("switch-role").click();
    await expect(page.getByLabel("Privacy and audit")).toContainText("Operator access required");
    await expect(page.getByLabel("Privacy and audit")).toContainText("agent");
    await expect(page.getByRole("tab", {name: "Exports"})).toHaveCount(0);
  });

  test("a refresh during an in-flight preview cancels it and frees the UI", async ({page}) => {
    const gate = deferred();
    await installMocks(page, {previewGate: gate.promise});
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Retention & deletion"}).click();
    await expect(page.getByTestId("pa-resource")).toHaveCount(0);  // permissions live in their tab
    await page.getByLabel("Resource to delete").selectOption("mem_1");
    await page.getByRole("button", {name: "Review affected scope"}).click();
    // The preview response is held; a refresh (as a shared SSE invalidation of
    // our own write would cause) must cancel it rather than strand busy.
    await page.getByTestId("bump-refresh").click();
    gate.release();
    await expect(page.getByTestId("pa-preview")).toHaveCount(0);
    // Selection survives, busy is freed, and the action works again.
    await expect(page.getByRole("button", {name: "Review affected scope"})).toBeEnabled();
    await page.getByRole("button", {name: "Review affected scope"}).click();
    await expect(page.getByTestId("pa-preview")).toContainText("mem_1");
  });

  test("a refresh during an in-flight download saves nothing and re-enables actions", async ({page}) => {
    const gate = deferred();
    const calls = await installMocks(page, {downloadGate: gate.promise});
    let downloads = 0;
    page.on("download", () => { downloads += 1; });
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Exports"}).click();
    await page.getByRole("button", {name: "Download (re-authorized)"}).click();
    await page.getByTestId("bump-refresh").click();
    gate.release();
    await page.waitForTimeout(500);  // allow any (incorrect) late completion to fire
    expect(downloads).toBe(0);
    expect(calls.downloads).toBe(1);  // the re-authorized read happened; its result was dropped
    await expect(page.getByTestId("pa-export-status")).toHaveCount(0);
    // A healthy refresh is not an actor change: the action is available again.
    const button = page.getByRole("button", {name: "Download (re-authorized)"});
    await expect(button).toBeEnabled();
    const [file] = await Promise.all([page.waitForEvent("download"), button.click()]);
    expect(file.suggestedFilename()).toMatch(/\.json$/);
    await expect(page.getByTestId("pa-export-status")).toContainText("Downloaded export pe_1");
  });

  test("a healthy refresh clears the unconfirmed preview but keeps actions and statuses", async ({page}) => {
    const calls = await installMocks(page);
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Retention & deletion"}).click();
    await page.getByLabel("Resource to delete").selectOption("mem_1");
    await page.getByRole("button", {name: "Review affected scope"}).click();
    await page.getByLabel("Deletion confirmation").fill("DELETE mem_1");
    // Refresh before confirming: the typed confirmation and preview are dropped
    // (the revision may have changed), but nothing is permanently disabled.
    await page.getByTestId("bump-refresh").click();
    await expect(page.getByTestId("pa-preview")).toHaveCount(0);
    expect(calls.deletes).toHaveLength(0);
    await page.getByRole("button", {name: "Review affected scope"}).click();
    await page.getByLabel("Deletion confirmation").fill("DELETE mem_1");
    await page.getByRole("button", {name: "Delete via canonical lifecycle"}).click();
    await expect(page.getByTestId("pa-delete-status")).toContainText("logically deleted");
    expect(calls.deletes).toHaveLength(1);
    expect(calls.deletes[0].body).toEqual({expected_revision: 3});
    // A further healthy refresh keeps the completed-action status metadata.
    await page.getByTestId("bump-refresh").click();
    await expect(page.getByTestId("pa-delete-status")).toContainText("logically deleted");
  });

  test("an explicit download cannot save after the component unmounts", async ({page}) => {
    const gate = deferred();
    const calls = await installMocks(page, {downloadGate: gate.promise});
    let downloads = 0;
    page.on("download", () => { downloads += 1; });
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Exports"}).click();
    await page.getByRole("button", {name: "Download (re-authorized)"}).click();
    await page.getByTestId("toggle-mount").click();  // unmount while the read is in flight
    gate.release();
    await page.waitForTimeout(500);
    expect(downloads).toBe(0);
    expect(calls.downloads).toBe(1);
    await expect(page.getByLabel("Privacy and audit")).toHaveCount(0);
    // Remounting starts clean: no resurrected status or stale descriptor state.
    await page.getByTestId("toggle-mount").click();
    await page.getByRole("tab", {name: "Exports"}).click();
    await expect(page.getByTestId("pa-export")).toHaveCount(1);
    await expect(page.getByTestId("pa-export-status")).toHaveCount(0);
  });

  test("a session change masks loaded evidence synchronously", async ({page}) => {
    const gate = deferred();
    let permissionsCalls = 0;
    await page.route("**/v1/**", route =>
      route.fulfill({status: 404, json: {error: {message: "not_found"}}}));
    await page.route("**/v1/projects/*/privacy-audit/permissions**", async route => {
      permissionsCalls += 1;
      if (permissionsCalls > 1) await gate.promise;  // hold the post-switch load open
      return route.fulfill({json: {data: PERMISSIONS}});
    });
    await page.route("**/v1/projects/*/privacy-audit/audit**", route =>
      route.fulfill({json: {data: AUDIT}}));
    await page.route("**/v1/projects/*/privacy-audit/retention**", route =>
      route.fulfill({json: {data: RETENTION}}));
    await page.route("**/v1/projects/*/privacy-audit/exports", route =>
      route.fulfill({json: {data: {project_id: "project-1", exports: []}}}));

    await page.goto(HARNESS);
    await expect(page.getByTestId("pa-resource")).toHaveCount(3);
    await page.getByTestId("switch-session").click();
    // Same tick, before the new scoped load completes: old evidence is masked.
    await expect(page.getByTestId("pa-resource")).toHaveCount(0);
    await expect(page.getByTestId("pa-permissions")).toContainText("Loading current access policies");
    gate.release();
    await expect(page.getByTestId("pa-resource")).toHaveCount(3);
  });

  test("a stale preview cannot overwrite a newer selection", async ({page}) => {
    const gate = deferred();
    await installMocks(page, {previewGate: gate.promise});
    await page.goto(HARNESS);
    await page.getByRole("tab", {name: "Retention & deletion"}).click();
    await page.getByLabel("Resource to delete").selectOption("mem_1");
    await page.getByRole("button", {name: "Review affected scope"}).click();
    // Move to another target while mem_1's preview is in flight.
    await page.getByLabel("Resource to delete").selectOption("post_1");
    gate.release();
    await page.waitForTimeout(300);
    await expect(page.getByTestId("pa-preview")).toHaveCount(0);
    await page.getByRole("button", {name: "Review affected scope"}).click();
    await expect(page.getByTestId("pa-preview")).toContainText("post_1");
    await expect(page.getByTestId("pa-preview")).toContainText("DELETE post_1");
    await expect(page.getByTestId("pa-preview")).not.toContainText("DELETE mem_1");
  });
});
