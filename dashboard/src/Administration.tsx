import React, {useEffect, useRef, useState} from "react";
import {api, ApiError} from "./api";
import {CredentialManagement, type CredentialManagementProps} from "./CredentialManagement";

type Person = {id: string; display_name: string; effective_role: string};
type Team = {id: string; revision: number; display_name: string; member_agent_ids: string[]; active: boolean};
type Resource = {id: string; project_id: string; kind: string; title: string};
type Rules = {public_read: boolean; public_contribute: boolean; reader_agent_ids: string[];
  contributor_agent_ids: string[]; reader_team_ids: string[]; contributor_team_ids: string[]};
type Policy = Partial<Rules> & {resource_id: string; revision: number | null};
type Grant = {id: string; revision: number; nexus_agent_id: string; target_agent_ids: string[];
  actions: string[]; state: string; expires_at: number};
type Worker = {id: string; revision: number; display_name?: string; agent_id: string; state: string;
  reason?: string; runtime?: string; monitoring?: {sandbox_verified: boolean; credential_revoked?: boolean; incident?: {
    reason?: string; recorded_at?: string; termination_confirmed?: boolean}}};
const emptyRules = (): Rules => ({public_read: false, public_contribute: false, reader_agent_ids: [],
  contributor_agent_ids: [], reader_team_ids: [], contributor_team_ids: []});
const actions = ["steer", "pause", "resume", "cancel", "assign_task", "monitor"];
const id = encodeURIComponent;

function Choices({label, items, value, change, max = 128}: {label: string;
  items: {id: string; name: string}[]; value: string[]; change: (value: string[]) => void; max?: number}) {
  const known = new Set(items.map(item => item.id));
  const options = [...items, ...value.filter(key => !known.has(key)).map(key => ({id: key, name: `Unavailable · ${key}`}))];
  return <fieldset className="choice-group"><legend>{label} <span>{value.length}/{max}</span></legend>
    <div className="choice-list">{options.length ? options.map(item => <label key={item.id}>
      <input type="checkbox" checked={value.includes(item.id)} disabled={!value.includes(item.id) && value.length >= max}
        onChange={e => change(e.target.checked ? [...value, item.id] : value.filter(key => key !== item.id))}/>
      <span>{item.name}</span></label>) : <p className="muted">No entries available.</p>}</div>
  </fieldset>;
}

export function Administration({project, role, agents, onBeforeSelfMutation, onSelfRotation,
  onSelfRevocation, onSelfMutationUnchanged, onCredentialError}: {project: string; role: string; agents: Person[];
    onCredentialError?: (message: string) => void} &
    Pick<CredentialManagementProps, "onBeforeSelfMutation" | "onSelfRotation" | "onSelfRevocation" | "onSelfMutationUnchanged">) {
  const [section, setSection] = useState("teams");
  const [teams, setTeams] = useState<Team[]>([]);
  const [grants, setGrants] = useState<Grant[]>([]);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [workerLimit, setWorkerLimit] = useState(false);
  const [workerAvailable, setWorkerAvailable] = useState<boolean | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [team, setTeam] = useState<Team | null>(null);
  const [teamName, setTeamName] = useState("");
  const [members, setMembers] = useState<string[]>([]);
  const [active, setActive] = useState(true);
  const [kind, setKind] = useState("board");
  const [resources, setResources] = useState<Resource[]>([]);
  const [resourceLimit, setResourceLimit] = useState(false);
  const [catalogReload, setCatalogReload] = useState(0);
  const [resource, setResource] = useState("");
  const [policy, setPolicy] = useState<Policy | null>(null);
  const [rules, setRules] = useState<Rules>(emptyRules);
  const [coordinator, setCoordinator] = useState("");
  const [targets, setTargets] = useState<string[]>([]);
  const [grantActions, setGrantActions] = useState<string[]>(["monitor"]);
  const [ttl, setTtl] = useState("3600");
  const [confirm, setConfirm] = useState("");
  const alive = useRef(true);
  const version = useRef(0);
  const policyVersion = useRef(0);
  const workerVersion = useRef(0);
  const commands = useRef(new Map<string, string>());
  const people = agents.map(agent => ({id: agent.id, name: agent.display_name}));
  const teamOptions = teams.map(item => ({id: item.id, name: `${item.display_name}${item.active ? "" : " (inactive)"}`}));
  const name = (key: string) => agents.find(agent => agent.id === key)?.display_name ?? key;

  useEffect(() => {
    alive.current = true;
    return () => { alive.current = false; version.current++; policyVersion.current++; workerVersion.current++; };
  }, []);

  async function refresh() {
    const current = ++version.current;
    const currentWorker = ++workerVersion.current;
    setLoading(true); setError(""); setConfirm("");
    try {
      const [nextTeams, nextGrants, nextWorkers] = await Promise.all([
        api<Team[]>(`projects/${id(project)}/teams`),
        api<Grant[]>(`projects/${id(project)}/nexus-grants`),
        role === "admin" ? api<{workers: Worker[]; has_more: boolean; unavailable?: boolean}>(`projects/${id(project)}/workers`)
          .catch(e => {
            if (e instanceof ApiError && e.status === 404) return {workers: [], has_more: false, unavailable: true};
            throw e;
          }) : null,
      ]);
      if (!alive.current || current !== version.current) return;
      setTeams(nextTeams); setGrants(nextGrants);
      if (currentWorker === workerVersion.current) {
        setWorkers(nextWorkers?.workers ?? []); setWorkerLimit(nextWorkers?.has_more ?? false);
        setWorkerAvailable(nextWorkers ? !nextWorkers.unavailable : false);
      }
    } catch (e) {
      if (alive.current && current === version.current) {
        setTeams([]); setGrants([]); setWorkers([]); setWorkerAvailable(null); setError(String(e));
      }
    } finally { if (alive.current && current === version.current) setLoading(false); }
  }
  useEffect(() => { if (project) void refresh(); }, [project, role]);

  useEffect(() => {
    let disposed = false;
    setResources([]); setResource(""); setPolicy(null); setConfirm("");
    policyVersion.current++;
    if (!project || section !== "access") return;
    setLoading(true); setError("");
    api<{resources: Resource[]; has_more: boolean}>(`projects/${id(project)}/access-resources?kind=${id(kind)}`).then(data => {
      if (!disposed) { setResources(data.resources); setResourceLimit(data.has_more); }
    }).catch(e => { if (!disposed) setError(String(e)); })
      .finally(() => { if (!disposed) setLoading(false); });
    return () => { disposed = true; };
  }, [project, kind, section, catalogReload]);

  useEffect(() => {
    if (section !== "workers" || role !== "admin" || workerAvailable !== true) return;
    let disposed = false, pending = false;
    async function poll() {
      if (pending || busy) return;
      pending = true;
      const current = ++workerVersion.current;
      try {
        const data = await api<{workers: Worker[]; has_more: boolean}>(`projects/${id(project)}/workers`);
        if (!disposed && current === workerVersion.current) { setWorkers(data.workers); setWorkerLimit(data.has_more); }
      } catch (e) {
        if (!disposed && current === workerVersion.current) { setWorkers([]); setWorkerAvailable(null); setError(String(e)); }
      } finally { pending = false; }
    }
    const timer = window.setInterval(() => void poll(), 5000);
    return () => { disposed = true; window.clearInterval(timer); };
  }, [project, section, role, workerAvailable, busy]);

  async function loadPolicy(key: string) {
    const current = ++policyVersion.current;
    setResource(key); setPolicy(null); setConfirm(""); setError(""); setNotice("");
    if (!key) return;
    try {
      const next = await api<Policy>(`resources/${id(key)}/access`);
      if (alive.current && current === policyVersion.current) {
        setPolicy(next); setRules({...emptyRules(), ...next});
      }
    } catch (e) { if (alive.current && current === policyVersion.current) setError(String(e)); }
  }

  async function mutate(path: string, method: string, body: unknown, done: () => void) {
    if (busy) return;
    const signature = JSON.stringify([path, method, body]);
    if (!commands.current.has(signature)) {
      if (commands.current.size >= 64) {
        setError("Too many unresolved requests. Inspect canonical state before making more changes."); return;
      }
      commands.current.set(signature, crypto.randomUUID());
    }
    setBusy(true); setError(""); setNotice("");
    try {
      await api(path, method, body, undefined, commands.current.get(signature));
      if (!alive.current) return;
      commands.current.delete(signature); setConfirm(""); setNotice("Saved. The canonical update is recorded.");
      done(); await refresh();
    } catch (e) {
      if (alive.current) { setConfirm(""); setError(`${String(e)}. Refresh to inspect current state before retrying.`); }
    } finally { if (alive.current) setBusy(false); }
  }
  function editTeam(item: Team | null) {
    setTeam(item); setTeamName(item?.display_name ?? ""); setMembers(item?.member_agent_ids ?? []);
    setActive(item?.active ?? true); setConfirm(""); setNotice("");
  }
  function changeRules(patch: Partial<Rules>) { setRules(value => ({...value, ...patch})); setConfirm(""); }

  return <section className="admin-workspace" aria-label="Team administration">
    <div className="admin-heading"><div><p className="eyebrow">OPERATOR CONTROL CENTER</p>
      <h1>People, permissions & peace of mind.</h1>
      <p className="muted">Manage who can collaborate, what Nexus can control, and how owned workers behave.</p>
    </div><button className="outline-button" disabled={busy || loading} onClick={() => {
      editTeam(null); setPolicy(null); void refresh();
      if (section === "access") setCatalogReload(value => value + 1);
    }}>Refresh administration</button></div>
    <div className="admin-metrics">
      <div><span>Teams</span><strong>{teams.length}</strong><small>{teams.filter(item => item.active).length} active</small></div>
      <div><span>Scoped grants</span><strong>{grants.filter(item => item.state === "active" && item.expires_at * 1000 > Date.now()).length}</strong><small>Target + action paired</small></div>
      <div><span>Worker incidents</span><strong>{role !== "admin" ? "Admin only" : workerAvailable === null ? "…" : !workerAvailable ? "Unavailable" : workers.filter(item => item.monitoring?.incident).length}</strong><small>Tool/API observations, not OS isolation</small></div>
    </div>
    <nav className="admin-navigation" aria-label="Administration sections">
      {[["teams", "Teams & membership"], ["access", "Resource access"], ["grants", "Nexus grants"],
        ...(role === "admin" ? [["workers", "Worker incidents"], ["credentials", "Credentials"]] : [])].map(([key, label]) =>
        <button key={key} disabled={busy} aria-pressed={section === key} className={section === key ? "selected" : ""}
          onClick={() => { setSection(key); setConfirm(""); setNotice(""); }}>{label}</button>)}
    </nav>
    {error && <div role="alert" className="error">{error}</div>}
    {notice && <p role="status" className="admin-notice">{notice}</p>}
    {loading && <p role="status" className="muted">Loading canonical administration state…</p>}
    <fieldset className="admin-controls" disabled={busy || loading || !project}>
    {section === "credentials" && role === "admin" && <CredentialManagement project={project} role={role}
      agents={agents} onBeforeSelfMutation={onBeforeSelfMutation} onSelfRotation={onSelfRotation}
      onSelfRevocation={onSelfRevocation} onSelfMutationUnchanged={onSelfMutationUnchanged} onError={onCredentialError}/>}
    {section === "teams" && <div className="admin-grid">
      <div className="admin-panel"><div className="panel-heading"><h2>Collaboration teams</h2>
        <button onClick={() => editTeam(null)}>New team</button></div>
        {!teams.length && <p className="muted">Create a team to grant access to a group of agents.</p>}
        {teams.map(item => <button className={`entity-row ${team?.id === item.id ? "chosen" : ""}`} key={item.id}
          data-team-id={item.id} onClick={() => editTeam(item)}><span><strong>{item.display_name}</strong>
            <small>{item.member_agent_ids.length} members · revision {item.revision}</small></span>
          <span className={`state-pill ${item.active ? "healthy" : ""}`}>{item.active ? "Active" : "Inactive"}</span></button>)}
      </div>
      <form className="admin-panel" onSubmit={e => {
        e.preventDefault();
        if (team && !active && confirm !== "team") { setConfirm("team"); return; }
        void mutate(team ? `teams/${id(team.id)}` : "teams", team ? "PATCH" : "POST", {
          project_id: project, display_name: teamName.trim(), member_agent_ids: members,
          ...(team ? {expected_revision: team.revision, active} : {}),
        }, () => editTeam(null));
      }}><h2>{team ? "Edit team membership" : "Create a team"}</h2>
        <label className="form-field">Team name<input required maxLength={128} value={teamName}
          onChange={e => { setTeamName(e.target.value); setConfirm(""); }}/></label>
        <Choices label="Member agents" items={people} value={members} change={value => { setMembers(value); setConfirm(""); }}/>
        {team && <label className="check-line"><input type="checkbox" checked={active}
          onChange={e => { setActive(e.target.checked); setConfirm(""); }}/>Team is active</label>}
        {confirm === "team" && <p className="caution" role="alert">Deactivation immediately removes this team's access. Membership is retained.</p>}
        <button className="primary" disabled={!teamName.trim()}>{busy ? "Saving…" : confirm === "team" ? "Confirm team deactivation" : team ? "Save team" : "Create team"}</button>
      </form>
    </div>}
    {section === "access" && <div className="admin-panel">
      <h2>Resource access policy</h2><p className="muted">Contributor access implies read. Parent, evidence, privacy and ownership checks still apply.</p>
      <div className="form-row"><label className="form-field">Resource type<select aria-label="Resource type" value={kind} onChange={e => setKind(e.target.value)}>
        {[["board", "Board"], ["board_post", "Board post"], ["task", "Task"], ["artifact", "Artifact"], ["memory", "Memory"]]
          .map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label>
        <label className="form-field">Resource<select aria-label="Resource" value={resource} onChange={e => void loadPolicy(e.target.value)}>
          <option value="">Select a resource</option>{resources.map(item => <option key={item.id} value={item.id}>{item.title} · {item.id.slice(-8)}</option>)}
        </select></label></div>
      {resourceLimit && <p className="caution">Showing the latest 200 visible resources of this type. Older resources require the API.</p>}
      {policy && <><p className="policy-caption">{policy.revision === null ? "Legacy policy: existing access is preserved until you save an explicit policy." : `Explicit policy · revision ${policy.revision}`}
        <code>{resource}</code></p>
        <div className="form-row"><label className="check-line"><input type="checkbox" checked={rules.public_read}
          onChange={e => changeRules({public_read: e.target.checked})}/>Public read</label>
          <label className="check-line"><input type="checkbox" checked={rules.public_contribute}
            onChange={e => changeRules({public_contribute: e.target.checked})}/>Public contribute</label></div>
        <div className="permission-grid">
          <Choices label="Reader teams" items={teamOptions} value={rules.reader_team_ids} change={value => changeRules({reader_team_ids: value})}/>
          <Choices label="Contributor teams" items={teamOptions} value={rules.contributor_team_ids} change={value => changeRules({contributor_team_ids: value})}/>
          <Choices label="Reader agents" items={people} value={rules.reader_agent_ids} change={value => changeRules({reader_agent_ids: value})}/>
          <Choices label="Contributor agents" items={people} value={rules.contributor_agent_ids} change={value => changeRules({contributor_agent_ids: value})}/>
        </div>
        {confirm === "access" && <div role="alert" className="caution">This replaces the entire policy, not just selected fields. Unlisted agents lose access.
          An empty policy is operator-only. Existing privacy restrictions remain.</div>}
        <div className="form-row"><button className="primary" onClick={() => {
          if (confirm !== "access") { setConfirm("access"); return; }
          const body = {...rules, expected_revision: policy.revision};
          // Never forward resource identity or receipt metadata as permission fields.
          void mutate(`resources/${id(resource)}/access`, "PUT", {
            ...emptyRules(), public_read: body.public_read, public_contribute: body.public_contribute,
            reader_agent_ids: body.reader_agent_ids, contributor_agent_ids: body.contributor_agent_ids,
            reader_team_ids: body.reader_team_ids, contributor_team_ids: body.contributor_team_ids,
            expected_revision: body.expected_revision,
          }, () => { void loadPolicy(resource); });
        }}>{confirm === "access" ? "Confirm access replacement" : "Review access changes"}</button>
          {confirm === "access" && <button onClick={() => setConfirm("")}>Cancel changes</button>}</div>
      </>}
    </div>}
    {section === "grants" && <div className="admin-grid">
      <form className="admin-panel" onSubmit={e => {
        e.preventDefault();
        if (confirm !== "grant") { setConfirm("grant"); return; }
        void mutate("nexus-grants", "POST", {project_id: project, nexus_agent_id: coordinator,
          target_agent_ids: targets, actions: grantActions, ttl_seconds: Number(ttl)}, () => {
            setCoordinator(""); setTargets([]); setGrantActions(["monitor"]); setTtl("3600");
          });
      }}><h2>Delegate a bounded mandate</h2><p className="muted">Grants do not appoint Nexus or replace its current lease.</p>
        <label className="form-field">Coordinator agent<select aria-label="Coordinator agent" required value={coordinator}
          onChange={e => { setCoordinator(e.target.value); setConfirm(""); }}><option value="">Select an agent</option>
          {people.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}</select></label>
        <Choices label="Target agents" items={people} value={targets} max={64} change={value => { setTargets(value); setConfirm(""); }}/>
        <Choices label="Allowed actions" items={actions.map(action => ({id: action, name: action.replace("_", " ")}))}
          value={grantActions} max={6} change={value => { setGrantActions(value); setConfirm(""); }}/>
        <label className="form-field">Lifetime in seconds<input type="number" required min={10} max={86400} step={1} value={ttl}
          onChange={e => { setTtl(e.target.value); setConfirm(""); }}/></label>
        {confirm === "grant" && <p className="caution" role="alert">Creating a grant permanently switches this agent to scoped authority.
          Expiry or revocation never restores broad authority. Monitor does not grant launch or stop rights.</p>}
        <button className="primary" disabled={!coordinator || !targets.length || !grantActions.length}>
          {confirm === "grant" ? "Confirm scoped grant" : "Review scoped grant"}</button>
      </form>
      <div className="admin-panel"><h2>Delegation ledger</h2>
        {!grants.length && <p className="muted">No scoped grants yet. Existing unrestricted Nexus behavior remains until its first grant.</p>}
        {grants.map(grant => {
          const current = grant.state === "active" && grant.expires_at * 1000 > Date.now();
          return <article className="grant-card" data-grant-id={grant.id} key={grant.id}>
            <div className="panel-heading"><strong>{name(grant.nexus_agent_id)}</strong>
              <span className={`state-pill ${current ? "healthy" : ""}`}>{current ? "Active" : grant.state === "active" ? "Expired" : grant.state}</span></div>
            <p>{grant.target_agent_ids.map(name).join(", ")}</p><div className="tag-list">{grant.actions.map(action => <span key={action}>{action}</span>)}</div>
            <small className="muted">Expires {new Date(grant.expires_at * 1000).toLocaleString()} · revision {grant.revision}</small>
            {grant.state === "active" && <div><button className="danger-button" onClick={() => {
              if (confirm !== grant.id) { setConfirm(grant.id); return; }
              void mutate(`nexus-grants/${id(grant.id)}:revoke`, "POST", {expected_revision: grant.revision}, () => {});
            }}>{confirm === grant.id ? "Confirm revoke" : "Revoke grant"}</button>
              {confirm === grant.id && <p className="caution" role="alert">Revocation retires dependent controls. Broad authority will not return.</p>}</div>}
          </article>;
        })}
      </div>
    </div>}
    {section === "workers" && role === "admin" && <div className="admin-panel">
      <h2>Owned workers & incidents</h2><p className="muted">Only API-created owned workers are listed. Denial monitoring is not universal filesystem or network isolation.</p>
      {workerLimit && <p className="caution">Showing the latest 200 workers. Older workers require lookup by ID through the API.</p>}
      {workerAvailable === false ? <p className="caution">Worker administration is unavailable. Enable the trusted worker provisioning integration to inspect owned workers.</p>
        : workerAvailable === true && !workers.length && <p className="empty">No owned workers in this project.</p>}
      {workers.map(worker => <article className="worker-card" key={worker.id} data-worker-id={worker.id}>
        <div className="panel-heading"><strong>{worker.display_name ?? name(worker.agent_id)}</strong>
          <span className={`state-pill ${worker.state === "running" ? "healthy" : ""}`}>{worker.state}</span></div>
        <p className="muted">{worker.runtime ?? "Managed runtime"} · {worker.reason ?? "No termination reported"}</p>
        <code>{worker.id}</code>
        {worker.monitoring?.incident && <div className="incident-box"><strong>Permission incident</strong>
          <p>{worker.monitoring.incident.reason ?? "Observed permission refusal"}</p>
          {worker.monitoring.incident.recorded_at && <small>Recorded {new Date(worker.monitoring.incident.recorded_at).toLocaleString()}</small>}
          <dl><dt>Process tree drained</dt><dd>{worker.monitoring.incident.termination_confirmed === true ? "Confirmed" : "Unconfirmed"}</dd>
            <dt>Credential revoked</dt><dd>{worker.monitoring.credential_revoked === true ? "Confirmed" : "Unconfirmed"}</dd></dl></div>}
        {worker.state === "running" && <button className="danger-button" onClick={() => {
          if (confirm !== worker.id) { setConfirm(worker.id); return; }
          void mutate(`workers/${id(worker.id)}:stop`, "POST", {expected_revision: worker.revision}, () => {});
        }}>{confirm === worker.id ? "Confirm worker stop" : "Stop worker"}</button>}
        {confirm === worker.id && <p role="alert" className="caution">Stop the owned process tree and revoke its credential? This does not resume work.</p>}
      </article>)}
    </div>}
    </fieldset>
  </section>;
}
