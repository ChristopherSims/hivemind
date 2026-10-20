# Real Factory integration test

This opt-in test launches **real `droid exec` sessions**, not mocked agents.
Three identities build a mini issue tracker: API, browser UI, and API tests.
The API and UI build concurrently; an API follow-up answers the UI's actual
Hivemind question; QA then reviews both implementations and writes tests.
There are at most four model-driven executions per invocation.

## Run

Requirements: authenticated Factory CLI, installed project MCP/dev extras,
Node.js, `dashboard/node_modules`, and Playwright Chromium:

```powershell
cd <repo>
uv sync --locked --all-extras
cd dashboard
npm ci
npx playwright install chromium
cd ..
.\.venv\Scripts\python.exe -m scripts.factory_integration --live --auto high
```

Optional: `--model MODEL_ID`, `--agent-timeout SECONDS` (30–600, default 300).
The default model is `gpt-6-sol`. Each session has a 35-tool-call limit.
Wall time/tool counts are enforced, **not a monetary/token spending cap**.
This test consumes Factory/provider credits and is excluded from ordinary pytest.
`--auto` defaults to `medium`; the installed Factory host requires explicitly
authorized `high` autonomy for the local MCP messaging/board writes. The high
setting does not widen the tool allowlist or Hivemind file scopes.

## Isolation and approvals

Each invocation creates a new ignored `.hivemind-test/factory-live_*/` directory
with its own app workspace, SQLite service, private credentials, project-local
Factory hooks/MCP settings, and ephemeral loopback ports. Existing global
Factory settings and the normal Hivemind data directory are not modified.

Agent file reads are restricted to the app workspace. Writes require an
exclusive Hivemind claim for `api/`, `web/`, or `tests/`. Agents receive only
file tools, read-only schema discovery, and six explicitly selected local Hivemind MCP tools. Shell,
deployment, delegated tools, arbitrary MCP mutations, and external tools are
not granted. Exact MCP IDs are resolved from the real Factory inventory;
Hivemind API identity and authorization checks remain in effect. The adapter
does not override host approvals or use `--skip-permissions-unsafe`.

**This is not an OS sandbox.** The independent harness executes the generated
Python app/tests locally, with a minimal environment containing no inherited
Factory/Hivemind/provider credentials. Use a disposable VM/account when stronger
isolation from your files/network is required. Agents themselves cannot execute
the app, install packages, or run commands.

## Evidence and logs

The runner prints the run directory and every conversation log path as it is
created. It updates the readable logs during execution, including on failure:

- `conversations/api--ui.log`, `api--qa.log`, `qa--ui.log`: complete pair transcripts.
- `conversations/*--thread-*.log`: one transcript per pair/thread.
- `conversations/main-board.log`: chronological main-board posts with authors.
- `*.stdout.log` / `*.stderr.log`: real Factory outputs, session IDs where exposed.
- `hivemind-evidence.json`: canonical events, ledger, claims, messages, board posts.
- `report.json`: objective overall result, execution limits, sessions, log paths,
  evidence counts, app checks, and generated-file hashes.
- `agent-tests.*.log`: output from the agent-written unittest suite.
- `browser.*.log`, `issue-tracker.png`: actual Chromium acceptance evidence.

Readable transcripts redact credential-like text and remove terminal control
characters. Treat all artifacts, including canonical exports and session logs,
as private and review them before sharing. Private credentials stay outside the
app workspace. Failed runs retain partial logs/ledger; nothing is auto-deleted.

Success requires real captured edits and claims for all three identities, native
Hivemind MCP use, each agent's own board post, a UI-to-API question and correlated
API response, and QA-to-API feedback. Independent checks verify API CRUD,
validation, filtering, static assets, SQLite persistence across restart, at least
four passing agent-written tests, and Chromium creation, safe text rendering,
status change, reload, filtering, and deletion.

The test deliberately does not claim coverage of Nexus steering, embedding
retrieval, delivery claim/ack execution, or production-host conformance.
For ownership conflicts, real Nexus steering, and verified memory reuse, run the
[second coordination scenario](factory-coordination.md) against a passing run.
