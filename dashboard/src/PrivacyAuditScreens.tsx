// Operator privacy & audit screens for /projects/{id}/privacy.
//
// All data comes from the canonical privacy-audit endpoints; destructive and
// sensitive actions require a typed, target-bound confirmation and reuse the
// canonical ACL/deletion lifecycle. Async safety model:
//
// * Scope identity (project + role + permissionKey) is recomputed every render
//   and mirrored into a ref synchronously, so a role/project/session change
//   masks previously loaded evidence *during the same render* -- nothing stale
//   is ever painted while the passive reload is still in flight.
// * Read tickets: every load increments a read epoch; only the newest ticket
//   for the current scope may write state. A healthy same-scope refresh
//   (e.g. a shared SSE invalidation after our own canonical write) is not an
//   actor change: data reloads, statuses survive, actions stay available.
// * Action tickets: every action captures the current action epoch. A scope
//   change, a refresh, a selection change or unmount invalidates it, its late
//   completion is dropped, and `busy` is reset by the scope effect so no
//   fenced `finally` can strand the UI.
// * Fail closed: any read/auth failure clears every scoped section.
import {useCallback, useEffect, useRef, useState} from "react";
import {api} from "./api";
import {
  AuditHistory, DeletionPreview, ExportList, PermissionsReview, RetentionCapabilities,
  confirmationMatches, deletionConfirmation, deletionPath, exportConfirmation,
  normalizeAudit, normalizeExports, normalizePermissions, normalizePreview,
  normalizeRetention, policySummary, retentionOrder,
} from "./privacy-audit";
import "./privacy-audit.css";

export type PrivacyAuditAgent = {
  id: string;
  display_name: string;
  base_color_token?: string;
  effective_role?: string;
};

export type PrivacyAuditScreensProps = {
  project: string;
  role: string;
  agents?: PrivacyAuditAgent[];
  refreshVersion?: number;
  permissionKey?: string;
  onError?: (message: string) => void;
  onOpenAgent?: (id: string) => void;
};

type Tab = "permissions" | "retention" | "exports" | "audit";

const OPERATOR_ROLES = ["admin", "human"];

/** State tagged with the scope that produced it; reads mask foreign scopes synchronously. */
function useScoped<T>(scopeKey: string, keyRef: {current: string}, initial: T) {
  const [state, setState] = useState<{key: string; value: T}>({key: scopeKey, value: initial});
  const set = useCallback((value: T) => {
    setState({key: keyRef.current, value});
  }, [keyRef]);
  return [state.key === scopeKey ? state.value : initial, set] as const;
}

export function PrivacyAuditScreens({project, role, agents = [], refreshVersion = 0,
                                     permissionKey = "", onError, onOpenAgent}: PrivacyAuditScreensProps) {
  const [tab, setTab] = useState<Tab>("permissions");
  const [busy, setBusy] = useState(false);
  // Scope identity, updated synchronously on every render: completions compare
  // against this ref, so an authority change invalidates them immediately.
  const scopeKey = `${project}:${role}:${permissionKey}`;
  const scopeRef = useRef(scopeKey);
  scopeRef.current = scopeKey;
  const mounted = useRef(true);
  const readEpoch = useRef(0);
  const actionEpoch = useRef(0);
  const selectionRef = useRef("");
  // Frozen per reviewed action: a confirmation cannot be replayed by a later click.
  const deleteKey = useRef<string | null>(null);
  const exportKey = useRef<string | null>(null);
  // Host callbacks may be inline; keep them out of effect dependency sets.
  const callbacks = useRef({onError, onOpenAgent});
  callbacks.current = {onError, onOpenAgent};
  const reportError = useCallback((message: string) => {
    callbacks.current.onError?.(message);
  }, []);

  const [permissions, setPermissions] = useScoped<PermissionsReview | null>(scopeKey, scopeRef, null);
  const [audit, setAudit] = useScoped<AuditHistory | null>(scopeKey, scopeRef, null);
  const [retention, setRetention] = useScoped<RetentionCapabilities | null>(scopeKey, scopeRef, null);
  const [exports_, setExports] = useScoped<ExportList | null>(scopeKey, scopeRef, null);
  const [preview, setPreview] = useScoped<DeletionPreview | null>(scopeKey, scopeRef, null);
  const [error, setError] = useScoped(scopeKey, scopeRef, "");
  const [selectedResource, setSelectedResource] = useScoped(scopeKey, scopeRef, "");
  const [deleteConfirm, setDeleteConfirm] = useScoped(scopeKey, scopeRef, "");
  const [deleteStatus, setDeleteStatus] = useScoped(scopeKey, scopeRef, "");
  const [exportConfirm, setExportConfirm] = useScoped(scopeKey, scopeRef, "");
  const [exportStatus, setExportStatus] = useScoped(scopeKey, scopeRef, "");

  const isOperator = OPERATOR_ROLES.includes(role);

  const clearData = useCallback(() => {
    setPermissions(null); setAudit(null); setRetention(null); setExports(null);
  }, [setPermissions, setAudit, setRetention, setExports]);
  const clearActions = useCallback(() => {
    setPreview(null); setDeleteConfirm(""); setDeleteStatus("");
    setExportConfirm("");
    deleteKey.current = null; exportKey.current = null;
  }, [setPreview, setDeleteConfirm, setDeleteStatus, setExportConfirm]);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      actionEpoch.current += 1;  // invalidate in-flight actions after unmount
    };
  }, []);

  useEffect(() => {
    // New scope or refresh: cancel in-flight reads/actions, free `busy` (a
    // fenced finally cannot), and drop unconfirmed previews/confirmations.
    const key = scopeKey;
    readEpoch.current += 1;
    actionEpoch.current += 1;
    const ticket = readEpoch.current;
    setBusy(false);
    clearActions();
    setError("");
    const live = () => mounted.current && scopeRef.current === key && readEpoch.current === ticket;
    if (!project || !isOperator) {
      clearData();
      setExportStatus("");
      setSelectedResource("");
      selectionRef.current = "";
      return;
    }
    async function load() {
      try {
        const encoded = encodeURIComponent(project);
        const [perms, auditData, retentionData, exportData] = await Promise.all([
          api<unknown>(`projects/${encoded}/privacy-audit/permissions`),
          api<unknown>(`projects/${encoded}/privacy-audit/audit`),
          api<unknown>(`projects/${encoded}/privacy-audit/retention`),
          api<unknown>(`projects/${encoded}/privacy-audit/exports`),
        ]);
        if (!live()) return;
        setPermissions(normalizePermissions(perms));
        setAudit(normalizeAudit(auditData));
        setRetention(normalizeRetention(retentionData));
        setExports(normalizeExports(exportData));
        setError("");
      } catch (failure) {
        if (!live()) return;
        // Fail closed: never keep this scope's (possibly unauthorized) data on screen.
        clearData(); clearActions();
        setExportStatus(""); setSelectedResource(""); selectionRef.current = "";
        const message = String(failure);
        setError(message);
        reportError(message);
      }
    }
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scopeKey, refreshVersion, isOperator, project]);

  /** Guard for action completions: current scope, current action epoch, still mounted. */
  const actionLive = useCallback((key: string, ticket: number) =>
    mounted.current && scopeRef.current === key && actionEpoch.current === ticket, []);

  const agentName = useCallback((id: string) =>
    agents.find(agent => agent.id === id)?.display_name ?? id, [agents]);

  async function runPreview(resourceId: string) {
    const key = scopeKey;
    const ticket = actionEpoch.current;
    selectionRef.current = resourceId;
    setBusy(true); setDeleteStatus(""); setPreview(null); setDeleteConfirm("");
    deleteKey.current = null;
    const live = () => actionLive(key, ticket) && selectionRef.current === resourceId;
    try {
      const raw = await api<unknown>(
        `projects/${encodeURIComponent(project)}/privacy-audit/deletion-preview` +
        `?resource_id=${encodeURIComponent(resourceId)}`);
      if (!live()) return;  // stale: scope changed, refreshed, or selection moved on
      const normalized = normalizePreview(raw);
      if (!normalized || normalized.project_id !== project) {
        throw new Error("deletion preview did not match this project");
      }
      setPreview(normalized);
      // Freeze the idempotency key at review time; it is used exactly once.
      deleteKey.current = crypto.randomUUID();
    } catch (failure) {
      if (!live()) return;
      setPreview(null); deleteKey.current = null;
      setError(String(failure)); reportError(String(failure));
    } finally {
      if (actionLive(key, ticket)) setBusy(false);
    }
  }

  async function runDelete() {
    if (!preview || !deleteKey.current) return;
    const path = deletionPath(preview);
    if (!path) return;
    if (!confirmationMatches(deleteConfirm, deletionConfirmation(preview.resource_id))) return;
    const key = scopeKey;
    const ticket = actionEpoch.current;
    const frozenKey = deleteKey.current;
    const target = preview.resource_id;
    const revision = preview.revision;
    setBusy(true);
    try {
      await api(path, "DELETE", {expected_revision: revision}, undefined, frozenKey);
      if (!actionLive(key, ticket)) return;
      deleteKey.current = null;
      setPreview(null); setSelectedResource(""); selectionRef.current = ""; setDeleteConfirm("");
      setDeleteStatus(`Deleted ${target}: logically deleted, content history purged, ` +
        "derived index cleanup pending, dependent memories invalidated (not erased).");
      // Re-read canonical state; never trust the local copy after a mutation.
      const encoded = encodeURIComponent(project);
      const [perms, auditData] = await Promise.all([
        api<unknown>(`projects/${encoded}/privacy-audit/permissions`),
        api<unknown>(`projects/${encoded}/privacy-audit/audit`),
      ]);
      if (!actionLive(key, ticket)) return;
      setPermissions(normalizePermissions(perms));
      setAudit(normalizeAudit(auditData));
    } catch (failure) {
      if (!actionLive(key, ticket)) return;
      setDeleteStatus("");
      setPreview(null); setDeleteConfirm(""); deleteKey.current = null;
      setError(`${String(failure)}. Review the affected scope again before retrying.`);
      reportError(String(failure));
    } finally {
      if (actionLive(key, ticket)) setBusy(false);
    }
  }

  async function runExportCreate() {
    if (!confirmationMatches(exportConfirm, exportConfirmation(project))) return;
    const key = scopeKey;
    const ticket = actionEpoch.current;
    if (!exportKey.current) exportKey.current = crypto.randomUUID();
    const frozenKey = exportKey.current;
    setBusy(true);
    try {
      await api(`projects/${encodeURIComponent(project)}/privacy-audit/exports`, "POST",
        {confirmation: exportConfirmation(project)}, undefined, frozenKey);
      if (!actionLive(key, ticket)) return;
      exportKey.current = null;
      setExportConfirm("");
      const raw = await api<unknown>(`projects/${encodeURIComponent(project)}/privacy-audit/exports`);
      if (!actionLive(key, ticket)) return;
      setExports(normalizeExports(raw));
      setExportStatus("Export descriptor created. Download regenerates the payload from " +
        "current, authorized records and re-checks your access.");
    } catch (failure) {
      if (!actionLive(key, ticket)) return;
      setError(String(failure)); reportError(String(failure));
    } finally {
      if (actionLive(key, ticket)) setBusy(false);
    }
  }

  async function runExportDownload(exportId: string) {
    // Explicit user action: the network read and the browser download both
    // happen only inside this click handler, and only while this mounted
    // component still owns the scope that started them.
    const key = scopeKey;
    const ticket = actionEpoch.current;
    setBusy(true);
    try {
      const payload = await api<unknown>(`privacy-exports/${encodeURIComponent(exportId)}/download`);
      if (!actionLive(key, ticket)) return;  // unmounted, rescoped or refreshed: save nothing
      const blob = new Blob([JSON.stringify(payload, null, 2)], {type: "application/json"});
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `privacy-audit-export-${exportId.slice(-8)}.json`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
      setExportStatus(`Downloaded export ${exportId} (${blob.size} bytes). ` +
        "The file was regenerated from current authorized records; sharing it is your responsibility.");
    } catch (failure) {
      if (!actionLive(key, ticket)) return;
      setError(String(failure)); reportError(String(failure));
    } finally {
      if (actionLive(key, ticket)) setBusy(false);
    }
  }

  if (!isOperator) {
    return <section className="pa-root" aria-label="Privacy and audit">
      <div className="timeline-heading"><div><p className="eyebrow">PRIVACY &amp; AUDIT</p>
        <h1>Operator access required.</h1></div></div>
      <p className="muted">Permission review, deletion controls, exports and audit history are
        available to project operators (admin or human) only. Your role ({role || "unknown"})
        can read this boundary but cannot act here.</p>
    </section>;
  }

  const deletable = (permissions?.resources ?? []).filter(item => item.deletable_via);

  return <section className="pa-root" aria-label="Privacy and audit">
    <div className="timeline-heading"><div><p className="eyebrow">PRIVACY &amp; AUDIT</p>
      <h1>Review access, retention and audit history.</h1></div></div>
    <p className="muted">Reads are scoped to this project and your operator role. Deletion uses the
      canonical lifecycle only; unsupported boundaries are shown as disabled, never faked. Exports
      are re-authorized and regenerated on every download.</p>
    {error && <div className="error" role="alert">{error}</div>}
    <div className="pa-tabs" role="tablist">
      {(["permissions", "retention", "exports", "audit"] as Tab[]).map(name =>
        <button key={name} role="tab" aria-selected={tab === name}
          className={tab === name ? "selected" : ""} onClick={() => setTab(name)}>
          {{permissions: "Permission review", retention: "Retention & deletion",
            exports: "Exports", audit: "Audit history"}[name]}
        </button>)}
    </div>

    {tab === "permissions" && <div className="pa-panel" data-testid="pa-permissions">
      {!permissions && !error && <p className="muted">Loading current access policies…</p>}
      {permissions && <>
        <p className="muted">{permissions.administration}</p>
        {permissions.truncated &&
          <p className="pa-warning">Showing the first {permissions.resources.length} resources; the list is truncated.</p>}
        <h2>Teams</h2>
        {permissions.teams.length === 0 && <p className="muted">No teams in this project.</p>}
        <ul className="pa-teams">{permissions.teams.map(team =>
          <li key={team.id} data-testid="pa-team">
            <strong>{team.title ?? team.id}</strong>
            <span className={`pa-badge ${team.active ? "pa-ok" : "pa-off"}`}>
              {team.active ? "active" : "inactive"}</span>
            <span className="muted"> {team.member_agent_ids.length} member
              {team.member_agent_ids.length === 1 ? "" : "s"} · revision {team.revision}</span>
          </li>)}</ul>
        <h2>Resource policies</h2>
        <ul className="pa-resources">{permissions.resources.map(item =>
          <li key={item.resource_id} data-testid="pa-resource" data-resource-id={item.resource_id}>
            <div><code>{item.resource_id}</code> <span className="pa-badge">{item.kind}</span>
              {item.title && <span> {item.title}</span>}
              <span className="muted"> · revision {item.revision}</span></div>
            <div className="muted">{policySummary(item.policy)}</div>
            {[...item.policy.reader_agent_ids, ...item.policy.contributor_agent_ids].length > 0 &&
              <div className="pa-grants">Agent grants: {[...new Set([
                ...item.policy.reader_agent_ids, ...item.policy.contributor_agent_ids])].map(id =>
                <button key={id} type="button" className="pa-link"
                  onClick={() => callbacks.current.onOpenAgent?.(id)}>{agentName(id)}</button>)}</div>}
          </li>)}</ul>
      </>}
    </div>}

    {tab === "retention" && <div className="pa-panel" data-testid="pa-retention">
      {!retention && !error && <p className="muted">Loading retention capabilities…</p>}
      {retention && <>
        <h2>Capabilities</h2>
        <ul className="pa-caps">{retentionOrder().map(name => {
          const capability = retention.capabilities[name];
          if (!capability) return null;
          return <li key={name} data-testid={`pa-cap-${name}`}
            className={capability.supported ? "" : "pa-disabled"}>
            <div><strong>{name.replaceAll("_", " ")}</strong>
              <span className={`pa-badge ${capability.supported ? "pa-ok" : "pa-off"}`}>
                {capability.supported ? "supported" : "not supported"}</span></div>
            {capability.mechanism && <p className="muted">{capability.mechanism}</p>}
            {capability.semantics && <p className="muted">{capability.semantics}</p>}
            {capability.reason && <p className="pa-warning">{capability.reason}</p>}
          </li>;})}
        </ul>
        <h2>Delete a resource</h2>
        <p className="muted">Deletion is logical deletion plus synchronous content-history purge with
          derived index invalidation queued. It is not physical erasure, a cache purge, or verified
          external-provider deletion. Review the affected scope first; dependent memories are
          invalidated, never silently erased.</p>
        <label>Resource <select aria-label="Resource to delete" value={selectedResource}
          onChange={event => {
            // A selection change cancels any reviewed preview for the old target.
            setSelectedResource(event.target.value);
            selectionRef.current = event.target.value;
            setPreview(null); setDeleteConfirm(""); setDeleteStatus("");
            deleteKey.current = null;
          }}>
          <option value="">Select a deletable resource</option>
          {deletable.map(item => <option key={item.resource_id} value={item.resource_id}>
            {item.kind} · {item.title ?? item.resource_id}</option>)}
        </select></label>
        <button type="button" disabled={busy || !selectedResource}
          onClick={() => void runPreview(selectedResource)}>Review affected scope</button>
        {preview && <div className="pa-preview" data-testid="pa-preview">
          {!preview.deletable && <p className="pa-warning">{preview.unsupported_reason}</p>}
          {preview.deletable && <>
            <p>Deleting <code>{preview.resource_id}</code> ({preview.kind}, revision {preview.revision}):</p>
            <ul>{preview.consequences.map(item => <li key={item}>{item}</li>)}</ul>
            <p className="muted">Not performed: {preview.not_performed.join("; ")}.</p>
            {preview.dependent_count > 0 && <>
              <p>{preview.dependent_count} dependent memor
                {preview.dependent_count === 1 ? "y" : "ies"} will be invalidated (not erased)
                {preview.dependents_truncated ? " — list truncated" : ""}:</p>
              <ul>{preview.dependent_memories.map(item =>
                <li key={item.id}><code>{item.id}</code> {item.title ?? ""} — {item.effect}</li>)}</ul>
            </>}
            <label>Type <code>{deletionConfirmation(preview.resource_id)}</code> to confirm
              <input aria-label="Deletion confirmation" value={deleteConfirm} disabled={busy}
                onChange={event => setDeleteConfirm(event.target.value)}/></label>
            <button type="button" className="pa-danger" disabled={busy ||
              !confirmationMatches(deleteConfirm, deletionConfirmation(preview.resource_id))}
              onClick={() => void runDelete()}>Delete via canonical lifecycle</button>
          </>}
        </div>}
        {deleteStatus && <p className="pa-status" data-testid="pa-delete-status">{deleteStatus}</p>}
      </>}
    </div>}

    {tab === "exports" && <div className="pa-panel" data-testid="pa-exports">
      {!exports_ && !error && <p className="muted">Loading export descriptors…</p>}
      {exports_ && <>
        <p className="muted">Exports contain only whitelisted audit metadata and the current
          permission posture of resources you can see. The payload is regenerated and your access
          re-checked on every download. Nothing is uploaded or shared.</p>
        <label>Type <code>{exportConfirmation(project)}</code> to create an export descriptor
          <input aria-label="Export confirmation" value={exportConfirm} disabled={busy}
            onChange={event => { setExportConfirm(event.target.value); exportKey.current = null; }}/></label>
        <button type="button" disabled={busy ||
          !confirmationMatches(exportConfirm, exportConfirmation(project))}
          onClick={() => void runExportCreate()}>Create export descriptor</button>
        {exports_.exports.length === 0 && <p className="muted">No export descriptors yet.</p>}
        <ul className="pa-exports">{exports_.exports.map(item =>
          <li key={item.id} data-testid="pa-export">
            <code>{item.id}</code> <span className="muted">created {item.created_at} · {item.state}</span>
            <button type="button" disabled={busy}
              onClick={() => void runExportDownload(item.id)}>Download (re-authorized)</button>
          </li>)}</ul>
        {exportStatus && <p className="pa-status" data-testid="pa-export-status">{exportStatus}</p>}
      </>}
    </div>}

    {tab === "audit" && <div className="pa-panel" data-testid="pa-audit">
      {!audit && !error && <p className="muted">Loading audit history…</p>}
      {audit && <>
        <p className="muted">Canonical audit metadata only ({audit.scope.replaceAll("_", " ")}).
          Excludes: {audit.excludes.join(", ")}. Audit events are records, not permissions.</p>
        {audit.truncated &&
          <p className="pa-warning">Showing the most recent {audit.events.length} events; history is truncated.</p>}
        {audit.events.length === 0 && <p className="muted">No audit events for this project yet.</p>}
        <ul className="pa-audit">{audit.events.map(event =>
          <li key={event.id} data-testid="pa-audit-event">
            <span className="muted">{event.time}</span> <strong>{event.operation}</strong>
            {" · "}<code>{event.resource_id}</code> <span className="pa-badge">{event.resource_kind}</span>
            {event.resource_deleted && <span className="pa-badge pa-off">deleted</span>}
            <span className="muted"> · by {event.principal_role}</span>
          </li>)}</ul>
      </>}
    </div>}
  </section>;
}
