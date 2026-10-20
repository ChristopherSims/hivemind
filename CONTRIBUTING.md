# Contributing

Thanks for looking at Hivemind. This repository holds the local-first reference
implementation of the agent memory/coordination design; the design document
(`.memory/hivemind-plan.md`) is intentionally local-only for alpha.

## Setup

```powershell
uv venv --python 3.12
uv sync --locked
.\.venv\Scripts\hivemind.exe init --data-dir .hivemind-dev
```

On Linux/macOS use `sh scripts/install.sh` and `uv run` in place of
`.venv\Scripts`. Platform coverage today is Windows-first; other hosts run the
same code paths but are less exercised — see
[limitations](docs/limitations.md).

## Test gates (required for any PR)

```powershell
.\.venv\Scripts\python.exe -m pytest -q        # full offline suite (~14 min)
.\.venv\Scripts\ruff.exe check hivemind tests scripts examples
cd dashboard && npm ci && npm run build        # typecheck + production build
```

CI runs exactly these gates (plus the schema-conformance suite) on
`ubuntu-latest`. Keep tests offline: no model downloads, no paid vendor calls,
no host security changes. Opt-in real-model tests stay behind explicit
environment gates and are not part of the default run.

If your change touches the HTTP contract or models, regenerate the published
artifacts and include them in the PR:

```powershell
uv run python -c "from scripts.export_schemas import export; export()"
```

`spec/openapi.json` and `spec/schemas` must match the code; the conformance
tests verify this.

## Ground rules

- **Model/agent output is untrusted evidence.** It never grants permissions,
  acknowledges uncertain handling, verifies a claim, or bypasses a revision or
  epoch check. Tests should encode this fail-closed behavior, not just the
  happy path.
- Authorization/verification checks stay server-side. Adapters and MCP tools
  are façades over the authenticated API; a tool description is never a grant.
- Durable commands are idempotent and evidence-backed; an unknown external
  effect is never blindly retried, and a receipt is not proof of application.
- Every new route/tool/feature needs a regression test that fails without the
  feature and exercises its denial paths, not only its success path.
- Don't widen trust boundaries: no OS-level sandbox claims beyond what
  [os-isolation](docs/os-isolation.md) documents, no network exposure (loopback
  only), no ambient discovery of MCP servers or extensions.

## Commits and PRs

- Small, focused PRs; one feature or fix each, with its tests.
- Commit messages state what changed and why; no generated-noise summaries.
- Do not commit credentials, live data directories (`.hivemind-*`,
  `.memory/`, `benchmarks/`, agent-private state), or model caches — they are
  git-ignored on purpose.

## Documentation

User-facing behavior changes belong in `docs/*.md` (keep filenames lowercase;
feature guides live there). Anything you consider operator-local notes
(plans, milestone checklists, private findings) belongs in `.memory/`, which
is ignored by git.