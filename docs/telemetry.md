# OpenTelemetry traces

Hivemind exports the standard **OTLP/HTTP JSON trace** envelope (`resourceSpans`,
`scopeSpans`, hex trace/span IDs, decimal-string nanosecond timestamps).
Specification: https://opentelemetry.io/docs/specs/otlp/.
This implementation exports traces, not metrics or logs.

Server integration JSON may contain:

```json
{
  "telemetry": {
    "enabled": true,
    "directory": "<private-traces>",
    "endpoint": "http://127.0.0.1:4318/v1/traces",
    "allow_export": true,
    "max_spans": 10000,
    "batch_size": 100
  }
}
```

Worker private configuration accepts the same `telemetry` object. Canonical
commands and worker SDK queries/commands/adapter turns produce static-name spans.
Runtime control spans wrap only methods the underlying adapter actually exposes.
Only allowlisted canonical UUID identifiers are recorded. Prompts, message text,
source content, paths, provider output, tokens and error messages are not captured.
Diagnostic failures never replace the real operation result or exception.

The private SQLite queue is bounded and survives restart; overflow increments a
dropped counter rather than evicting unknown operation journals. Traces are
diagnostics, never authoritative progress, effect or recovery evidence.

Project administrators can inspect `GET /v1/projects/{id}/telemetry` and invoke
`POST /v1/projects/{id}/telemetry:export`. The configured collector must explicitly
accept the OTLP batch. Non-200, malformed, partial or lost receipts preserve the
whole local batch; retries are **at-least-once**, so duplicates are possible.
Redirects and inherited proxy configuration are disabled.

Export defaults off. A remote collector additionally requires
`allow_remote_export: true` and HTTPS. The URL must be credential-free, query-free,
and end in `/v1/traces`. API callers cannot choose an exporter URL or enable it.
No collector receives data merely because telemetry is enabled.
