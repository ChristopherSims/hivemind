import React, {useEffect, useLayoutEffect, useRef, useState} from "react";
import {api, ApiError} from "./api";
import "./credential-management.css";

/**
 * Credential rotation and revocation for canonical principals.
 *
 * Server contract (see hivemind/credential_management.py):
 *   GET  projects/{project}/credentials?role=&agent_id=&limit=
 *   GET  credentials/{id}
 *   GET  credentials/{id}/impact?operation=rotate|revoke
 *   POST credentials/{id}:rotate  {expected_revision, acknowledged_impact}
 *   POST credentials/{id}:revoke  {expected_revision, acknowledged_impact}
 *
 * Secrets discipline: the API never lists tokens or token hashes, and a rotation
 * token is returned exactly once. This component keeps that token in memory only
 * (never localStorage/sessionStorage), shows it masked until an explicit reveal,
 * clears it when the operator dismisses it, and drops it on project change, on an
 * authorization failure and on unmount. It is never rendered into logs, notices,
 * errors or screenshots of the surrounding page.
 */
export type CredentialAgent = {id: string; display_name: string};

export type CredentialRecord = {
  credential_id: string;
  project_id: string;
  role: string;
  agent_id?: string | null;
  agent_display_name?: string | null;
  coordinator: boolean;
  capture_authority: boolean;
  revision: number;
  state: string;
  usable: boolean;
  active_sessions: number;
  rotated_at?: string | null;
  revoked_at?: string | null;
  rotation_count: number;
  self: boolean;
};

export type CredentialRun = {run_id: string; presence: string; last_seen_at?: number | null};
export type CredentialWorker = {worker_id: string; state?: string | null};

export type CredentialImpact = {
  operation: "rotate" | "revoke";
  credential_id: string;
  revision: number;
  state: string;
  role: string;
  active_sessions: number;
  active_runs: CredentialRun[];
  active_run_ids: string[];
  owned_workers: CredentialWorker[];
  runs_truncated: boolean;
  workers_truncated: boolean;
  self_affected: boolean;
  last_usable_admin: boolean;
  usable_admins: number;
  project_admins: number;
  fingerprint: string;
  warnings: string[];
};

export type CredentialMutationResult = {
  status: string;
  token?: string;
  token_available: boolean;
  token_notice?: string;
  credential: CredentialRecord;
  sessions_invalidated: number;
  self_affected: boolean;
  old_token_invalidated?: boolean;
  revoked?: boolean;
  principal_preserved?: boolean;
};

export type CredentialList = {
  project_id: string;
  credentials: CredentialRecord[];
  count: number;
  limit: number;
  has_more: boolean;
  summary: {usable_admins: number; project_admins: number; revoked_credentials: number};
  policy: Record<string, string>;
  token_material_returned: boolean;
};

export type CredentialManagementProps = {
  /** Authorized project whose credentials are listed and mutated. */
  project: string;
  /** Current principal role; only an administrator may manage credentials. */
  role: string;
  /** Roster used to label agent-scoped credentials. */
  agents?: CredentialAgent[];
  /** Called once when the operator rotates their own credential, so the shell can
   * adopt the replacement token in memory. Without it the operator is signed out. */
  onSelfRotation?: (token: string) => void | Promise<void>;
  onBeforeSelfMutation?: () => void;
  onSelfMutationUnchanged?: () => void | Promise<void>;
  /** Called after the operator revokes their own credential; sign the shell out. */
  onSelfRevocation?: () => void;
  /** Mirror of credential errors into the owner's error surface. */
  onError?: (message: string) => void;
};

const id = encodeURIComponent;
const MAX_PENDING_COMMANDS = 32;

export function maskCredential(value: string) {
  return value.length <= 12 ? value : `…${value.slice(-12)}`;
}

function roleLabel(role: string) {
  return role === "agent" ? "Agent" : role === "admin" ? "Administrator"
    : role === "human" ? "Human" : role === "observer" ? "Observer" : role;
}

function stateLabel(state: string) {
  return state === "active" ? "Active" : state === "revoked" ? "Revoked" : "Principal missing";
}

function when(value?: string | null) {
  return value ? new Date(value).toLocaleString() : "Never";
}

export function CredentialManagement({
  project, role, agents = [], onSelfRotation, onSelfRevocation, onError, onBeforeSelfMutation,
  onSelfMutationUnchanged,
}: CredentialManagementProps) {
  const [listing, setListing] = useState<CredentialList | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [roleFilter, setRoleFilter] = useState("");
  const [agentFilter, setAgentFilter] = useState("");
  const [review, setReview] = useState<{credentialId: string; operation: "rotate" | "revoke";
    impact: CredentialImpact} | null>(null);
  const [confirm, setConfirm] = useState("");
  const [reveal, setReveal] = useState<{credentialId: string; token: string; masked: boolean;
    selfAffected: boolean; sessions: number} | null>(null);
  const alive = useRef(true);
  const scope = `${project}\u0000${role}`;
  const scopeRef = useRef(scope);
  scopeRef.current = scope;
  const impactTicket = useRef(0);
  const version = useRef(0);
  const commands = useRef(new Map<string, string>());
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;
  const onSelfRotationRef = useRef(onSelfRotation);
  onSelfRotationRef.current = onSelfRotation;
  const onSelfRevocationRef = useRef(onSelfRevocation);
  onSelfRevocationRef.current = onSelfRevocation;

  useEffect(() => {
    alive.current = true;
    return () => {
      // Unmount is a close: no token, no pending confirmation and no command key survive it.
      alive.current = false;
      version.current++;
      commands.current.clear();
      setReveal(null);
      setReview(null);
    };
  }, []);

  async function load(silent = false) {
    if (role !== "admin") return;
    const requestScope = scope;
    const current = ++version.current;
    if (!silent) setLoading(true);
    setError("");
    const params = new URLSearchParams();
    if (roleFilter) params.set("role", roleFilter);
    if (agentFilter) params.set("agent_id", agentFilter);
    try {
      const next = await api<CredentialList>(
        `projects/${id(project)}/credentials${params.size ? `?${params}` : ""}`);
      if (!alive.current || current !== version.current || scopeRef.current !== requestScope) return;
      setListing(next);
    } catch (e) {
      if (!alive.current || current !== version.current || scopeRef.current !== requestScope) return;
      setListing(null);
      // Never keep a revealed secret across an authorization failure.
      setReveal(null); setReview(null); setConfirm("");
      if (e instanceof ApiError && (e.status === 401 || e.status === 403)) commands.current.clear();
      setError(String(e));
      onErrorRef.current?.(String(e));
    } finally {
      if (alive.current && current === version.current && !silent) setLoading(false);
    }
  }

  useLayoutEffect(() => {
    // A project or role change is a new authorization scope: drop every secret and
    // confirmation from the previous scope before loading the new one.
    setListing(null); setReveal(null); setReview(null); setConfirm("");
    setBusy(false); impactTicket.current++;
    setNotice(""); setError(""); commands.current.clear();
    if (project && role === "admin") void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [project, role]);

  function keyFor(signature: string) {
    if (!commands.current.has(signature)) {
      if (commands.current.size >= MAX_PENDING_COMMANDS) return null;
      commands.current.set(signature, crypto.randomUUID());
    }
    return commands.current.get(signature) ?? null;
  }

  function agentName(record: CredentialRecord) {
    return record.agent_display_name
      ?? agents.find(agent => agent.id === record.agent_id)?.display_name
      ?? (record.agent_id ? maskCredential(record.agent_id) : null);
  }

  async function reviewImpact(record: CredentialRecord, operation: "rotate" | "revoke") {
    if (busy) return;
    const requestScope = scope;
    const ticket = ++impactTicket.current;
    setConfirm(""); setNotice(""); setError("");
    try {
      const impact = await api<CredentialImpact>(
        `credentials/${id(record.credential_id)}/impact?operation=${operation}`);
      if (!alive.current || scopeRef.current !== requestScope || impactTicket.current !== ticket) return;
      setReview({credentialId: record.credential_id, operation, impact});
    } catch (e) {
      if (!alive.current || scopeRef.current !== requestScope || impactTicket.current !== ticket) return;
      setReview(null);
      if (e instanceof ApiError && (e.status === 401 || e.status === 403)) {
        setReveal(null); commands.current.clear();
      }
      setError(String(e));
      onErrorRef.current?.(String(e));
    }
  }

  async function mutate(record: CredentialRecord, operation: "rotate" | "revoke") {
    if (busy || !review || review.credentialId !== record.credential_id
        || review.operation !== operation) return;
    const signature = `${record.credential_id}:${operation}:${review.impact.revision}`;
    // A stable key per pending command: a retried rotation must be answered with the
    // safe "already applied" receipt instead of a second token.
    const key = keyFor(signature);
    if (!key) {
      setError("Too many unresolved credential commands. Refresh canonical state before retrying.");
      return;
    }
    version.current++; impactTicket.current++;
    setBusy(true); setError(""); setNotice("");
    const requestScope = scope;
    if (review.impact.self_affected) onBeforeSelfMutation?.();
    try {
      const result = await api<CredentialMutationResult>(
        `credentials/${id(record.credential_id)}:${operation}`, "POST",
        {expected_revision: review.impact.revision, acknowledged_impact: review.impact.fingerprint},
        undefined, key);
      if (!alive.current || scopeRef.current !== requestScope) return;
      commands.current.delete(signature);
      setReview(null); setConfirm("");
      if (result.token_available && result.token) {
        // The single moment this token is readable. It is never written to storage.
        setReveal({credentialId: record.credential_id, token: result.token, masked: true,
          selfAffected: result.self_affected, sessions: result.sessions_invalidated});
        setNotice("Rotation applied. The replacement token is shown once below.");
        if (result.self_affected) await onSelfRotationRef.current?.(result.token);
      } else if (result.status === "already_applied") {
        // Honest idempotent retry: the mutation is committed but the secret is gone for good.
        setNotice(operation === "rotate"
          ? "Rotation already applied. The replacement token was returned once and cannot be retrieved "
            + "again; rotate again with the current revision if a new token is required."
          : "Revocation already applied. The credential is terminated and cannot be restored.");
      } else if (operation === "rotate") {
        setNotice("Rotation reported no token. Treat this credential as unusable and rotate again with the "
          + "current revision.");
      } else {
        setNotice(result.self_affected
          ? "Credential revoked. Your own session was signed out and cannot be restored."
          : "Credential revoked. Its sessions are invalidated and its principal, work and audit history "
            + "are preserved.");
      }
      if (operation === "revoke" && result.self_affected && result.status !== "already_applied") {
        onSelfRevocationRef.current?.();
      }
      if (result.self_affected && (result.status === "already_applied"
          || (operation === "rotate" && !result.token))) await onSelfMutationUnchanged?.();
      await load(true);
    } catch (e) {
      if (!alive.current || scopeRef.current !== requestScope) return;
      setConfirm("");
      if (e instanceof ApiError && (e.status === 412 || e.code === "impact_acknowledgement_stale"
          || e.code === "credential_already_revoked" || e.code === "credential_not_active")) {
        // The reviewed impact is stale: never reuse it, re-read canonical state.
        setReview(null);
        await load(true);
        if (alive.current) {
          setError(`${String(e)}. Review the current impact again before retrying.`);
        }
      } else {
        if (e instanceof ApiError && (e.status === 401 || e.status === 403)) {
          setReveal(null); setReview(null); commands.current.clear();
        }
        setError(String(e));
      }
      onErrorRef.current?.(String(e));
    } finally {
      if (alive.current && scopeRef.current === requestScope) setBusy(false);
    }
  }

  async function copyToken() {
    if (!reveal) return;
    try {
      if (!navigator.clipboard?.writeText) throw new Error("Clipboard unavailable");
      await navigator.clipboard.writeText(reveal.token);
      if (alive.current) setNotice("Token copied. Paste it into your credential store now; it cannot be "
        + "shown again.");
    } catch {
      if (alive.current) setNotice("Copy is unavailable in this browser. Reveal the token and copy it "
        + "manually; it cannot be shown again.");
    }
  }

  if (role !== "admin") {
    return <section className="credential-workspace" data-testid="credential-management"
      aria-label="Credential management">
      <div className="credential-heading"><div><p className="eyebrow">OPERATOR CONTROL CENTER</p>
        <h1>Credential rotation & revocation.</h1>
        <p className="muted">Credential management requires an administrator credential. No credential "
          + "metadata is loaded for this role.</p></div></div>
    </section>;
  }

  return <section className="credential-workspace" data-testid="credential-management"
    aria-label="Credential management">
    <div className="credential-heading"><div><p className="eyebrow">OPERATOR CONTROL CENTER</p>
      <h1>Credential rotation & revocation.</h1>
      <p className="muted">Rotate a leaked token or terminate a credential without deleting its principal,
        work or audit history. Tokens and token hashes are never listed; a rotation token is shown once.</p>
    </div><button className="outline-button" disabled={busy || loading} onClick={() => void load()}>
      Refresh credentials</button></div>
    <div className="credential-metrics">
      <div><span>Credentials listed</span><strong>{listing?.count ?? "…"}</strong>
        <small>{listing?.has_more ? `Page limit ${listing.limit}` : "Complete for this project"}</small></div>
      <div><span>Usable administrators</span><strong>{listing?.summary.usable_admins ?? "…"}</strong>
        <small>{listing?.summary.project_admins ?? 0} bound to this project</small></div>
      <div><span>Revoked</span><strong>{listing?.summary.revoked_credentials ?? "…"}</strong>
        <small>Revocation is terminal</small></div>
    </div>
    <div className="credential-filters">
      <label className="form-field">Role<select aria-label="Credential role" value={roleFilter}
        onChange={e => { setRoleFilter(e.target.value); setReview(null); setConfirm(""); }}>
        <option value="">All roles</option>
        <option value="admin">Administrator</option><option value="human">Human</option>
        <option value="observer">Observer</option><option value="agent">Agent</option>
      </select></label>
      <label className="form-field">Agent<select aria-label="Credential agent" value={agentFilter}
        onChange={e => { setAgentFilter(e.target.value); setReview(null); setConfirm(""); }}>
        <option value="">All agents</option>
        {agents.map(agent => <option key={agent.id} value={agent.id}>{agent.display_name}</option>)}
      </select></label>
      <button className="outline-button" disabled={busy || loading} onClick={() => void load()}>
        Apply filters</button>
    </div>
    {error && <div role="alert" className="error" data-testid="credential-error">{error}</div>}
    {notice && <p role="status" className="credential-notice" data-testid="credential-notice">{notice}</p>}
    {loading && <p role="status" className="muted">Loading canonical credential metadata…</p>}

    {reveal && <section className="credential-reveal" data-testid="credential-token"
      aria-label="One-time credential token">
      <div className="panel-heading"><strong>Replacement token for {maskCredential(reveal.credentialId)}</strong>
        <span className="state-pill healthy">Shown once</span></div>
      <p className="caution" role="alert">This value is displayed once. Hivemind never stores, logs or lists
        it, and it cannot be retrieved again.
        {reveal.selfAffected && " This is your own credential: its previous token and sessions were "
          + "invalidated. Save the replacement securely."}
        {reveal.sessions > 0 && ` ${reveal.sessions} session(s) were signed out.`}</p>
      <p className="credential-token-line">
        <code>{reveal.masked ? "•".repeat(24) : reveal.token}</code>
        <button type="button" aria-pressed={!reveal.masked}
          onClick={() => setReveal(value => value && {...value, masked: !value.masked})}>
          {reveal.masked ? "Reveal token" : "Hide token"}</button>
        <button type="button" onClick={() => void copyToken()}>Copy token</button>
        <button type="button" onClick={() => { setReveal(null); setNotice(""); }}>Dismiss token</button>
      </p>
    </section>}

    {!loading && listing && !listing.credentials.length &&
      <p className="empty">No credentials match these filters in this project.</p>}
    <div className="credential-list">
      {(listing?.credentials ?? []).map(record => <article className="credential-card" key={record.credential_id}
        data-testid="credential-row" data-credential-id={record.credential_id} data-state={record.state}>
        <div className="panel-heading">
          <strong>{agentName(record) ?? roleLabel(record.role)}</strong>
          <span className={`state-pill ${record.state === "active" ? "healthy" : ""}`}>
            {stateLabel(record.state)}</span>
        </div>
        <dl className="credential-facts">
          <div><dt>Role</dt><dd>{roleLabel(record.role)}{record.self ? " (this credential)" : ""}</dd></div>
          <div><dt>Credential</dt><dd><code>{maskCredential(record.credential_id)}</code></dd></div>
          <div><dt>Revision</dt><dd>{record.revision}</dd></div>
          <div><dt>Active sessions</dt><dd>{record.active_sessions}</dd></div>
          <div><dt>Grants</dt><dd>{[record.coordinator && "coordinator",
            record.capture_authority && "capture authority"].filter(Boolean).join(", ") || "None"}</dd></div>
          <div><dt>Rotations</dt><dd>{record.rotation_count} · last {when(record.rotated_at)}</dd></div>
          {record.state === "revoked" && <div><dt>Revoked</dt><dd>{when(record.revoked_at)}</dd></div>}
        </dl>
        <div className="credential-actions">
          {record.state === "active" && <button type="button" disabled={busy}
            onClick={() => void reviewImpact(record, "rotate")}>Review rotation</button>}
          {record.state !== "revoked" && <button type="button" className="danger-button" disabled={busy}
            onClick={() => void reviewImpact(record, "revoke")}>Review revocation</button>}
          {record.state === "revoked" && <p className="muted">Revocation is terminal. Issue a new credential
            out of band if this principal must act again.</p>}
        </div>
        {review?.credentialId === record.credential_id && <div className="credential-impact"
          data-testid="credential-impact" role="group"
          aria-label={`Impact of ${review.operation === "rotate" ? "rotation" : "revocation"}`}>
          <div className="panel-heading"><strong>{review.operation === "rotate" ? "Rotation impact"
            : "Revocation impact"}</strong><span className="state-pill">{stateLabel(review.impact.state)}</span></div>
          <dl className="credential-facts">
            <div><dt>Active sessions</dt><dd>{review.impact.active_sessions}</dd></div>
            <div><dt>Active runs</dt><dd>{review.impact.active_runs.length}
              {review.impact.runs_truncated ? " (truncated)" : ""}</dd></div>
            <div><dt>Owned workers</dt><dd>{review.impact.owned_workers.length}
              {review.impact.workers_truncated ? " (truncated)" : ""}</dd></div>
            <div><dt>Usable administrators</dt><dd>{review.impact.usable_admins}</dd></div>
            <div><dt>Affects you</dt><dd>{review.impact.self_affected ? "Yes" : "No"}</dd></div>
            <div><dt>Last administrator</dt><dd>{review.impact.last_usable_admin ? "Yes" : "No"}</dd></div>
          </dl>
          {review.impact.active_runs.length > 0 && <ul className="credential-runs">
            {review.impact.active_runs.map(run => <li key={run.run_id}><code>{maskCredential(run.run_id)}</code>
              <span>{run.presence}</span></li>)}</ul>}
          <ul className="credential-warnings" role="alert">
            {review.impact.warnings.map(warning => <li key={warning}>{warning}</li>)}
          </ul>
          {confirm === review.operation && <p className="caution" role="alert"
            data-testid="credential-confirm">
            {review.operation === "rotate"
              ? "Confirming replaces the stored token hash and signs out every session of this credential. "
                + "The new token is shown once and cannot be retrieved afterwards."
              : "Confirming terminates this credential, signs out its sessions and cannot be undone. "
                + "The principal, its work and its audit history are preserved."}</p>}
          <div className="credential-actions">
            <button type="button" className={review.operation === "revoke" ? "danger-button" : "primary"}
              disabled={busy} onClick={() => {
                if (confirm !== review.operation) { setConfirm(review.operation); return; }
                void mutate(record, review.operation);
              }}>{confirm === review.operation
                ? `Confirm ${review.operation === "rotate" ? "rotation" : "revocation"}`
                : `${review.operation === "rotate" ? "Rotate" : "Revoke"} credential`}</button>
            <button type="button" onClick={() => { setReview(null); setConfirm(""); }}>Cancel</button>
          </div>
        </div>}
      </article>)}
    </div>
    {listing && <details className="credential-policy"><summary>Server-declared credential policy</summary>
      <dl>{Object.entries(listing.policy).map(([key, value]) => <div key={key}>
        <dt>{key.replace(/_/g, " ")}</dt><dd>{value}</dd></div>)}</dl></details>}
  </section>;
}
