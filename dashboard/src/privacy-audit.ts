// Privacy & audit screen logic: whitelisted normalization, confirmation strings
// and display models for the operator privacy surface. Everything here is pure:
// no fetch, no storage, no DOM. The backend endpoints are the canonical ones in
// hivemind/privacy_audit.py; anything they do not support is rendered as a
// disabled boundary, never faked.

export type AccessPolicy = {
  revision: number | null;
  public_read: boolean;
  public_contribute: boolean;
  reader_agent_ids: string[];
  contributor_agent_ids: string[];
  reader_team_ids: string[];
  contributor_team_ids: string[];
};

export type PermissionResource = {
  resource_id: string;
  kind: string;
  project_id: string;
  title: string | null;
  revision: number;
  deletable_via: string | null;
  policy: AccessPolicy;
};

export type PermissionTeam = {
  id: string;
  title: string | null;
  active: boolean;
  member_agent_ids: string[];
  revision: number;
};

export type PermissionsReview = {
  project_id: string;
  generated_at: string;
  teams: PermissionTeam[];
  resources: PermissionResource[];
  truncated: boolean;
  policy_kinds: string[];
  administration: string;
};

export type AuditEvent = {
  id: string;
  operation: string;
  resource_id: string;
  resource_kind: string;
  resource_deleted: boolean;
  principal_role: string;
  time: string;
};

export type AuditHistory = {
  project_id: string;
  generated_at: string;
  events: AuditEvent[];
  truncated: boolean;
  scope: string;
  excludes: string[];
};

export type RetentionCapability = {
  supported: boolean;
  mechanism?: string;
  semantics?: string;
  reason?: string;
};

export type RetentionCapabilities = {
  project_id: string;
  generated_at: string;
  capabilities: Record<string, RetentionCapability>;
};

export type DeletionPreview = {
  resource_id: string;
  project_id: string;
  kind: string;
  revision: number;
  title: string | null;
  deletable: boolean;
  endpoint: string | null;
  unsupported_reason: string | null;
  dependent_memories: {id: string; kind: string; title: string | null; effect: string}[];
  dependent_count: number;
  dependents_truncated: boolean;
  consequences: string[];
  not_performed: string[];
};

export type ExportDescriptor = {
  id: string;
  project_id: string;
  created_at: string;
  state: string;
  revision: number;
  scope: string;
};

export type ExportList = {project_id: string; exports: ExportDescriptor[]};

// ---------------------------------------------------------------------------
// Confirmations. Destructive or sensitive actions need the operator to type a
// string that names the exact target, so a click can never be an accident.
// ---------------------------------------------------------------------------

export function exportConfirmation(project: string): string {
  return `EXPORT ${project}`;
}

export function deletionConfirmation(resourceId: string): string {
  return `DELETE ${resourceId}`;
}

export function confirmationMatches(typed: string, expected: string): boolean {
  return typed.trim() === expected;
}

// ---------------------------------------------------------------------------
// Whitelisted normalization. Unknown or malformed entries are dropped, extra
// fields are stripped, so a compromised or buggy response cannot smuggle
// payload content (bodies, tokens, paths) into the UI model.
// ---------------------------------------------------------------------------

function text(value: unknown, max = 512): string | null {
  return typeof value === "string" && value.length <= max ? value : null;
}

function idList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.length <= 128)
    .slice(0, 128);
}

function normalizePolicy(raw: unknown): AccessPolicy {
  const source = (raw ?? {}) as Record<string, unknown>;
  return {
    revision: typeof source.revision === "number" ? source.revision : null,
    public_read: source.public_read === true,
    public_contribute: source.public_contribute === true,
    reader_agent_ids: idList(source.reader_agent_ids),
    contributor_agent_ids: idList(source.contributor_agent_ids),
    reader_team_ids: idList(source.reader_team_ids),
    contributor_team_ids: idList(source.contributor_team_ids),
  };
}

export function normalizePermissions(raw: unknown): PermissionsReview {
  const source = (raw ?? {}) as Record<string, unknown>;
  const resources = (Array.isArray(source.resources) ? source.resources : [])
    .flatMap((item): PermissionResource[] => {
      const entry = (item ?? {}) as Record<string, unknown>;
      const resourceId = text(entry.resource_id, 128);
      const kind = text(entry.kind, 64);
      if (!resourceId || !kind) return [];
      return [{
        resource_id: resourceId,
        kind,
        project_id: text(entry.project_id, 128) ?? "",
        title: text(entry.title, 256),
        revision: typeof entry.revision === "number" ? entry.revision : 0,
        deletable_via: text(entry.deletable_via, 128),
        policy: normalizePolicy(entry.policy),
      }];
    })
    .slice(0, 1000);
  const teams = (Array.isArray(source.teams) ? source.teams : [])
    .flatMap((item): PermissionTeam[] => {
      const entry = (item ?? {}) as Record<string, unknown>;
      const id = text(entry.id, 128);
      if (!id) return [];
      return [{
        id,
        title: text(entry.title, 256),
        active: entry.active === true,
        member_agent_ids: idList(entry.member_agent_ids),
        revision: typeof entry.revision === "number" ? entry.revision : 0,
      }];
    })
    .slice(0, 128);
  return {
    project_id: text(source.project_id, 128) ?? "",
    generated_at: text(source.generated_at, 64) ?? "",
    teams,
    resources,
    truncated: source.truncated === true,
    policy_kinds: idList(source.policy_kinds),
    administration: text(source.administration, 512) ?? "",
  };
}

export function normalizeAudit(raw: unknown): AuditHistory {
  const source = (raw ?? {}) as Record<string, unknown>;
  const events = (Array.isArray(source.events) ? source.events : [])
    .flatMap((item): AuditEvent[] => {
      const entry = (item ?? {}) as Record<string, unknown>;
      const id = text(entry.id, 128);
      const operation = text(entry.operation, 128);
      const resourceId = text(entry.resource_id, 128);
      const time = text(entry.time, 64);
      if (!id || !operation || !resourceId || !time) return [];
      return [{
        id, operation, resource_id: resourceId, time,
        resource_kind: text(entry.resource_kind, 64) ?? "unknown",
        resource_deleted: entry.resource_deleted === true,
        principal_role: text(entry.principal_role, 32) ?? "unknown",
      }];
    })
    .slice(0, 1000);
  return {
    project_id: text(source.project_id, 128) ?? "",
    generated_at: text(source.generated_at, 64) ?? "",
    events,
    truncated: source.truncated === true,
    scope: text(source.scope, 128) ?? "",
    excludes: idList(source.excludes),
  };
}

const RETENTION_ORDER = [
  "logical_deletion", "source_removal", "index_invalidation", "memory_expiry",
  "physical_erasure", "cache_purge", "external_provider_deletion",
  "background_retention_sweep",
];

export function normalizeRetention(raw: unknown): RetentionCapabilities {
  const source = (raw ?? {}) as Record<string, unknown>;
  const rawCaps = (source.capabilities ?? {}) as Record<string, unknown>;
  const capabilities: Record<string, RetentionCapability> = {};
  for (const name of RETENTION_ORDER) {
    const entry = (rawCaps[name] ?? {}) as Record<string, unknown>;
    capabilities[name] = {
      supported: entry.supported === true,
      mechanism: text(entry.mechanism) ?? undefined,
      semantics: text(entry.semantics) ?? undefined,
      reason: text(entry.reason) ?? undefined,
    };
  }
  return {
    project_id: text(source.project_id, 128) ?? "",
    generated_at: text(source.generated_at, 64) ?? "",
    capabilities,
  };
}

export function retentionOrder(): string[] {
  return [...RETENTION_ORDER];
}

export function normalizePreview(raw: unknown): DeletionPreview | null {
  const source = (raw ?? {}) as Record<string, unknown>;
  const resourceId = text(source.resource_id, 128);
  const kind = text(source.kind, 64);
  if (!resourceId || !kind) return null;
  return {
    resource_id: resourceId,
    project_id: text(source.project_id, 128) ?? "",
    kind,
    revision: typeof source.revision === "number" ? source.revision : 0,
    title: text(source.title, 256),
    deletable: source.deletable === true,
    endpoint: text(source.endpoint, 128),
    unsupported_reason: text(source.unsupported_reason, 512),
    dependent_memories: (Array.isArray(source.dependent_memories) ? source.dependent_memories : [])
      .flatMap((item) => {
        const entry = (item ?? {}) as Record<string, unknown>;
        const id = text(entry.id, 128);
        if (!id) return [];
        return [{id, kind: text(entry.kind, 64) ?? "memory",
                 title: text(entry.title, 256), effect: text(entry.effect, 256) ?? ""}];
      })
      .slice(0, 64),
    dependent_count: typeof source.dependent_count === "number" ? source.dependent_count : 0,
    dependents_truncated: source.dependents_truncated === true,
    consequences: idList(source.consequences),
    not_performed: idList(source.not_performed),
  };
}

export function normalizeExports(raw: unknown): ExportList {
  const source = (raw ?? {}) as Record<string, unknown>;
  const exports = (Array.isArray(source.exports) ? source.exports : [])
    .flatMap((item): ExportDescriptor[] => {
      const entry = (item ?? {}) as Record<string, unknown>;
      const id = text(entry.id, 128);
      if (!id) return [];
      return [{
        id,
        project_id: text(entry.project_id, 128) ?? "",
        created_at: text(entry.created_at, 64) ?? "",
        state: text(entry.state, 32) ?? "unknown",
        revision: typeof entry.revision === "number" ? entry.revision : 0,
        scope: text(entry.scope, 128) ?? "",
      }];
    })
    .slice(0, 100);
  return {project_id: text(source.project_id, 128) ?? "", exports};
}

// ---------------------------------------------------------------------------
// Export payload safety. A downloaded export must contain only the whitelisted
// sections; this guard is what the tests pin down.
// ---------------------------------------------------------------------------

export const EXPORT_TOP_LEVEL_KEYS = [
  "export_id", "project_id", "scope", "descriptor_created_at", "generated_at",
  "principal_bound", "regenerated", "permissions", "audit", "truncated",
  "excludes", "external_sharing",
] as const;

export function exportPayloadSafe(payload: unknown): boolean {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const keys = Object.keys(payload as Record<string, unknown>);
  return keys.every(key => (EXPORT_TOP_LEVEL_KEYS as readonly string[]).includes(key));
}

// The path segment for the canonical delete endpoint of a deletable kind.
export function deletionPath(preview: DeletionPreview): string | null {
  if (!preview.deletable) return null;
  if (preview.kind === "memory") return `memories/${encodeURIComponent(preview.resource_id)}`;
  if (preview.kind === "board_post") return `posts/${encodeURIComponent(preview.resource_id)}`;
  return null;
}

// Short, honest policy summary for one resource row.
export function policySummary(policy: AccessPolicy): string {
  const parts: string[] = [];
  parts.push(policy.public_read ? "public read" : "restricted read");
  parts.push(policy.public_contribute ? "public contribute" : "restricted contribute");
  const grants = policy.reader_agent_ids.length + policy.contributor_agent_ids.length
    + policy.reader_team_ids.length + policy.contributor_team_ids.length;
  if (grants) parts.push(`${grants} explicit grant${grants === 1 ? "" : "s"}`);
  if (policy.revision === null) parts.push("default policy");
  return parts.join(" · ");
}
