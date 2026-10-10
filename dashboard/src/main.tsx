import React, {useEffect, useRef, useState} from "react";
import {createRoot} from "react-dom/client";
import {api, setCsrf} from "./api";
import {MemoryReview} from "./MemoryReview";
import {loadTimeline} from "./timeline";
import "./style.css";

type Agent = {
  id: string; display_name: string; base_color_token: string; effective_role: string;
  description: string;
};
type Segment = {kind: "text"; text: string} | {kind: "agent_mention"; agent_id: string};
type Item = {
  id: string; revision: number; created_at: string; created_seq: number; content: {text?: string; segments?: Segment[]};
  author?: {kind: string; agent_id?: string}; from?: string; to?: string;
  thread_id: string; state?: string; visibility?: string;
  delivery_status?: string; attempt_count?: number; total_attempts?: number;
  next_attempt_at?: number | null; claim_until?: number; is_read?: boolean;
  delivery_history?: {action: string; at: string; run_id?: string; reason?: string}[];
};
type DeliverySummary = {unread: number; states: Record<string, number>;
  by_agent: Record<string, {pending: number; dead_letter: number; unread: number}>};
type Project = {id: string; title: string};
type Run = {
  id: string; agent_id: string; presence: string; status: string;
  summary?: {text: string}; capture_health: {state: string};
};

function App() {
  const [authenticated, setAuthenticated] = useState(false);
  const [token, setToken] = useState("");
  const [projects, setProjects] = useState<Project[]>([]);
  const [project, setProject] = useState("");
  const [agents, setAgents] = useState<Agent[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [board, setBoard] = useState("");
  const [posts, setPosts] = useState<Item[]>([]);
  const [messages, setMessages] = useState<Item[]>([]);
  const [delivery, setDelivery] = useState<DeliverySummary | null>(null);
  const [tab, setTab] = useState("board");
  const [tabs, setTabs] = useState<string[]>([]);
  const [draft, setDraft] = useState("");
  const [mentions, setMentions] = useState<Agent[]>([]);
  const [options, setOptions] = useState<Agent[]>([]);
  const [highlight, setHighlight] = useState(0);
  const [reply, setReply] = useState<Item | null>(null);
  const [connection, setConnection] = useState("Connecting");
  const [error, setError] = useState("");
  const [sending, setSending] = useState(false);
  const [newItems, setNewItems] = useState(false);
  const [before, setBefore] = useState<number | null>(null);
  const [messageBefore, setMessageBefore] = useState<number | null>(null);
  const [searchDraft, setSearchDraft] = useState("");
  const [searchText, setSearchText] = useState("");
  const [pageDepth, setPageDepth] = useState(1);
  const [pageCount, setPageCount] = useState(1);
  const [loadingTimeline, setLoadingTimeline] = useState(false);
  const [markingRead, setMarkingRead] = useState(false);
  const [timelineReload, setTimelineReload] = useState(0);
  const [role, setRole] = useState("");
  const timeline = useRef<HTMLDivElement>(null);
  const generation = useRef(0);
  const timelineGeneration = useRef(0);
  const boardProject = useRef("");
  const oldest = useRef<{key: string; posts?: number; messages?: number}>({key: ""});
  const postKey = useRef<string | null>(null);
  const userScrolled = useRef(false);
  const query = /(?:^|\s)@([^\s@]*)$/.exec(draft)?.[1];

  useEffect(() => {
    api<{csrf_token: string; role: string}>("session").then(session => {
      setCsrf(session.csrf_token); setRole(session.role); setAuthenticated(true);
    }).catch(() => {});
  }, []);

  async function login(event: React.FormEvent) {
    event.preventDefault();
    try {
      const result = await api<{csrf_token: string; role: string}>("session", "POST", {}, token);
      setCsrf(result.csrf_token); setRole(result.role);
      setToken(""); setError(""); setAuthenticated(true);
    } catch (e) { setError(String(e)); }
  }

  useEffect(() => {
    if (!authenticated) return;
    api<Project[]>("projects").then(data => {
      setProjects(data);
      const route = location.pathname.split("/")[2];
      setProject(data.find(p => p.id === route)?.id ?? data[0]?.id ?? "");
    }).catch(e => setError(String(e)));
  }, [authenticated]);

  useEffect(() => {
    if (!project) return;
    generation.current++;
    const version = generation.current;
    setPosts([]); setMessages([]); setDelivery(null); setAgents([]); setRuns([]); setTabs([]);
    setBoard(""); boardProject.current = ""; setSearchText(""); setSearchDraft(""); setPageDepth(1);
    setTab("board"); setMentions([]); setReply(null); setDraft(""); setBefore(null);
    let disposed = false;
    let firstRefresh = true;
    const initialRoute = location.pathname.split("/");
    let refreshing = false;
    let queued = false;
    async function refresh() {
      if (refreshing) { queued = true; return; }
      refreshing = true;
      do {
        queued = false;
        try {
          const [people, ledger, boards] = await Promise.all([
            api<Agent[]>(`projects/${project}/agent-appearances`),
            api<{runs: Run[]}>(`ledger?project_id=${project}`),
            api<{id: string}[]>(`boards?project_id=${project}`),
          ]);
          if (disposed || version !== generation.current) break;
          setAgents(people); setRuns(ledger.runs);
          if (firstRefresh) {
            if (initialRoute[3] === "agents" && people.some(a => a.id === initialRoute[4])) {
              setTabs([initialRoute[4]]); setTab(initialRoute[4]);
            } else if (initialRoute[3] === "conversations" && (role === "admin" || role === "observer")) {
              setTab("conversations");
            } else if (initialRoute[3] === "memories" && ["admin", "observer", "human"].includes(role)) {
              setTab("memories");
            }
            firstRefresh = false;
          }
          const boardId = boards[0]?.id;
          if (boardId) {
            boardProject.current = project; setBoard(boardId);
          }
          if (role === "admin" || role === "observer") {
            const summary = await api<DeliverySummary>(`projects/${project}/delivery-summary`);
            if (disposed || version !== generation.current) break;
            setDelivery(summary);
          }
          setTimelineReload(value => value + 1);
          if (!userScrolled.current) requestAnimationFrame(() => timeline.current?.scrollTo(0, timeline.current.scrollHeight));
          else setNewItems(true);
          setError("");
        } catch (e) {
          if (!disposed) {
            setError(String(e)); setConnection("Degraded");
            setPosts([]); setMessages([]); setDelivery(null); setAgents([]); setRuns([]); setOptions([]);
          }
        }
      } while (queued && !disposed);
      refreshing = false;
    }
    void refresh();
    // Stream deliberately carries only snapshot invalidation, never private message bodies.
    const events = new EventSource(`/v0.1/streams?project_id=${encodeURIComponent(project)}`);
    events.onopen = () => setConnection("Live");
    events.onerror = () => { setConnection("Reconnecting"); setPosts([]); setMessages([]); setDelivery(null); };
    events.addEventListener("resync", () => void refresh());
    const heartbeat = window.setInterval(() => void refresh(), 15000);
    return () => { disposed = true; events.close(); clearInterval(heartbeat); };
  }, [project, role]);

  useEffect(() => {
    if (!project || !board || boardProject.current !== project || tab === "memories") return;
    const version = ++timelineGeneration.current;
    let disposed = false;
    const key = `${project}:${tab}:${searchText}`;
    const changed = oldest.current.key !== key;
    if (changed) {
      oldest.current = {key}; setPosts([]); setMessages([]); setBefore(null); setMessageBefore(null);
    }
    setLoadingTimeline(true);
    const agent = !["board", "conversations"].includes(tab) ? tab : undefined;
    const params = `?limit=100&query=${encodeURIComponent(searchText)}`;
    const postPath = `boards/${board}/posts${params}${agent ? `&agent_id=${encodeURIComponent(agent)}` : ""}`;
    const messagePath = agent ? `agents/${agent}/conversation${params}` : `projects/${project}/conversations${params}`;
    async function load() {
      try {
        const [boardPage, messagePage] = await Promise.all([
          tab !== "conversations" ? loadTimeline<Item>(postPath, changed ? 1 : pageDepth, oldest.current.posts) : null,
          tab !== "board" && ["admin", "observer"].includes(role)
            ? loadTimeline<Item>(messagePath, changed ? 1 : pageDepth, oldest.current.messages) : null,
        ]);
        if (disposed || version !== timelineGeneration.current) return;
        setPosts(boardPage?.items ?? []); setMessages(messagePage?.items ?? []);
        setBefore(boardPage?.next_before ?? null); setMessageBefore(messagePage?.next_before ?? null);
        setPageCount(Math.max(boardPage?.page_count ?? 1, messagePage?.page_count ?? 1));
        oldest.current = {key, posts: boardPage?.items[0]?.created_seq, messages: messagePage?.items[0]?.created_seq};
        setError("");
        if (!userScrolled.current) requestAnimationFrame(() => timeline.current?.scrollTo(0, timeline.current.scrollHeight));
      } catch (e) {
        if (!disposed) { setPosts([]); setMessages([]); setBefore(null); setMessageBefore(null); setError(String(e)); }
      } finally { if (!disposed) setLoadingTimeline(false); }
    }
    void load();
    return () => { disposed = true; };
  }, [project, board, tab, role, searchText, pageDepth, timelineReload]);

  useEffect(() => { setPageDepth(1); setSearchDraft(""); setSearchText(""); }, [tab]);

  useEffect(() => {
    if (query === undefined || !project) { setOptions([]); return; }
    let cancelled = false;
    const timer = setTimeout(() => {
      api<Agent[]>(`projects/${project}/mention-options?query=${encodeURIComponent(query)}`)
        .then(data => { if (!cancelled) { setOptions(data); setHighlight(0); } })
        .catch(() => { if (!cancelled) setOptions([]); });
    }, 150);
    return () => { cancelled = true; clearTimeout(timer); };
  }, [query, project]);

  function openAgent(id: string) {
    setTabs(t => t.includes(id) ? t : [...t, id]);
    setTab(id);
    history.replaceState(null, "", `/projects/${project}/agents/${id}`);
  }

  async function refreshDelivery() {
    const version = generation.current;
    try {
      const summary = await api<DeliverySummary>(`projects/${project}/delivery-summary`);
      if (version !== generation.current) return;
      setDelivery(summary); setTimelineReload(value => value + 1); setError("");
    } catch (e) {
      if (version !== generation.current) return;
      setMessages([]); setDelivery(null); setError(String(e));
    }
  }

  function name(id: string) {
    const person = agents.find(a => a.id === id);
    return <button className={`agent-name ${person?.effective_role === "nexus" ? "nexus" : person?.base_color_token ?? ""}`}
      onClick={() => openAgent(id)} aria-label={`${person?.display_name ?? id}${person?.effective_role === "nexus" ? ", Nexus, main coordinator" : ""}`}>
      {person?.effective_role === "nexus" && <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m3 6 4 4 5-6 5 6 4-4-2 13H5Z"/></svg>}
      {person?.display_name ?? id.slice(0, 18)}
      {person?.effective_role === "nexus" && <small>Nexus</small>}
    </button>;
  }

  function choose(person: Agent) {
    postKey.current = null;
    setMentions(m => m.some(a => a.id === person.id) ? m : [...m, person]);
    setDraft(draft.replace(/@([^\s@]*)$/, ""));
    setOptions([]);
  }

  async function send(event: React.FormEvent) {
    event.preventDefault();
    if (!draft.trim() && !mentions.length) return;
    setSending(true);
    const segments: Segment[] = mentions.map(a => ({kind: "agent_mention", agent_id: a.id}));
    if (draft.trim()) segments.push({kind: "text", text: draft});
    postKey.current ??= crypto.randomUUID();
    try {
      await api<Item>(`boards/${board}/posts`, "POST", {
        client_post_id: postKey.current, kind: reply ? "answer" : "message",
        parent_post_id: reply?.id ?? null, thread_id: reply?.thread_id ?? null,
        content: {media_type: "application/vnd.hivemind.chat+json", segments},
      });
      setDraft(""); setMentions([]); setReply(null); postKey.current = null;
      setTimelineReload(value => value + 1); setError("");
    } catch (e) { setError(String(e)); }
    finally { setSending(false); }
  }

  const visible = tab === "board" ? posts : tab === "conversations" ? messages : [
    ...messages, ...posts,
  ].sort((a,b) => a.created_at.localeCompare(b.created_at));

  if (!authenticated) return <main className="login">
    <div className="brand-mark">H</div><p className="eyebrow">THE SHARED CONTEXT PLANE</p>
    <h1>Hivemind</h1><p>One place for agents, evidence, and ownership.</p>
    <form onSubmit={login}><label htmlFor="credential">Local human credential</label>
      <input id="credential" type="password" autoComplete="off" value={token} onChange={e => setToken(e.target.value)}/>
      <button className="primary">Open workspace</button></form>
    <p className="muted">Use the token from your private owner credential file. It stays out of URLs and browser storage.</p>
    {error && <p role="alert">{error}</p>}
  </main>;

  return <div className="shell">
    <header><div><strong className="brand">Hivemind</strong><span className="header-caption">MEMORY & COORDINATION</span></div>
      <label className="project-label">Workspace <select aria-label="Project" value={project} onChange={e => setProject(e.target.value)}>
        {projects.map(p => <option key={p.id} value={p.id}>{p.title}</option>)}</select></label>
      <span role="status" className={`connection ${connection === "Live" ? "live" : ""}`}>{connection}</span>
      <button onClick={async () => { await api("session", "DELETE"); setAuthenticated(false); setPosts([]); setMessages([]); setDelivery(null); }}>Sign out</button>
    </header>
    <aside><div className="section-title">AGENTS <span>{agents.length}</span></div>
      {agents.length === 0 && <p className="muted">No agents registered yet.</p>}
      {agents.map(a => {
        const active = runs.filter(r => r.agent_id === a.id);
        return <section className="roster-card" key={a.id}>{name(a.id)}
          <p>{active[0]?.summary?.text ?? a.description ?? "Ready to connect"}</p>
          <div className="run-tags">{active.map(r => <span key={r.id}>{r.presence} · {r.status}</span>)}</div>
          {delivery?.by_agent[a.id] && <small className="muted">{delivery.by_agent[a.id].pending} pending messages
            {" · "}{delivery.by_agent[a.id].dead_letter} dead letters</small>}
          {active.some(r => r.capture_health.state !== "healthy") && <small className="warning">Capture incomplete or unconfirmed</small>}
        </section>;
      })}
      <div className="aside-note">Structured events are evidence.<br/>Messages are not permissions.</div>
    </aside>
    <main className="workspace">
      <nav className="tabs" aria-label="Workspace tabs">
        <button className={tab === "board" ? "selected" : ""} onClick={() => { setTab("board"); history.replaceState(null,"",`/projects/${project}/board`); }}>Main board</button>
        {(role === "admin" || role === "observer") && <button aria-label="Agent conversations" className={tab === "conversations" ? "selected" : ""} onClick={() => { setTab("conversations"); history.replaceState(null,"",`/projects/${project}/conversations`); }}>Agent conversations
          {Boolean(delivery?.unread) && <span className="unread-badge">{delivery?.unread}</span>}</button>}
        {["admin", "observer", "human"].includes(role) && <button className={tab === "memories" ? "selected" : ""}
          onClick={() => { setTab("memories"); history.replaceState(null, "", `/projects/${project}/memories`); }}>Memory review</button>}
        {tabs.map(id => <span key={id} className="tab-group"><button className={tab === id ? "selected" : ""} onClick={() => setTab(id)}>{agents.find(a => a.id === id)?.display_name ?? id}</button>
          <button aria-label={`Close ${agents.find(a => a.id === id)?.display_name ?? id} tab`} onClick={() => {setTabs(t => t.filter(a => a !== id)); if (tab === id) setTab("board");}}>×</button></span>)}
      </nav>
      {tab === "memories" ? <MemoryReview key={project} project={project}/> : <>
      <div className="timeline-heading"><div><p className="eyebrow">{tab === "board" ? "PROJECT BLACKBOARD" : "AUTHORIZED CONVERSATIONS"}</p>
        <h1>{tab === "board" ? "A shared view of the work." : tab === "conversations" ? "Agents, in conversation." : agents.find(a => a.id === tab)?.display_name}</h1></div>
        <span className="count">{visible.length} items</span></div>
      {error && <div className="error" role="alert">{error}</div>}
      <form className="timeline-search" onSubmit={event => {
        event.preventDefault(); setPageDepth(1); oldest.current = {key: ""}; setSearchText(searchDraft.trim());
        setTimelineReload(value => value + 1);
      }}>
        <label htmlFor="timeline-search">Search this view</label>
        <input id="timeline-search" maxLength={256} value={searchDraft} onChange={e => setSearchDraft(e.target.value)}
          placeholder="Find text in authorized history"/>
        <button disabled={loadingTimeline}>Search</button>
        {searchText && <button type="button" onClick={() => {
          setSearchDraft(""); setSearchText(""); setPageDepth(1); oldest.current = {key: ""};
        }}>Clear search</button>}
        <span>{loadingTimeline ? "Loading…" : `${visible.length} loaded${searchText ? " matches" : " items"}`}</span>
      </form>
      {tab !== "board" && (role === "admin" || role === "observer") && <div className="delivery-toolbar">
        <span aria-label="Unread conversations">{delivery?.unread ?? 0} unread</span>
        <span>{delivery?.states.retry_wait ?? 0} retrying · {delivery?.states.dead_letter ?? 0} dead letters</span>
        <button onClick={() => void refreshDelivery()}>Refresh delivery</button>
        <button disabled={markingRead || loadingTimeline || !visible.some(item => item.from && !item.is_read)} onClick={async () => {
          const version = generation.current;
          const ids = visible.filter(item => item.from && !item.is_read).map(item => item.id);
          setMarkingRead(true);
          try {
            for (let index = 0; index < ids.length; index += 100) {
              if (version !== generation.current) break;
              await api(`projects/${project}/conversations:read`, "POST", {message_ids: ids.slice(index, index + 100)});
            }
            if (version === generation.current) await refreshDelivery();
          } catch (e) { if (version === generation.current) setError(String(e)); }
          finally { setMarkingRead(false); }
        }}>Mark displayed conversations read</button>
      </div>}
      <div className="timeline" ref={timeline} onScroll={() => {
        const el = timeline.current; if (el) userScrolled.current = el.scrollHeight - el.scrollTop - el.clientHeight > 80;
      }}>
        {(before || messageBefore) && <button disabled={loadingTimeline || pageCount >= 20}
          onClick={() => setPageDepth(Math.max(pageDepth, pageCount) + 1)}>
          {tab === "board" ? "Load earlier posts" : tab === "conversations" ? "Load earlier conversations" : "Load earlier activity"}
        </button>}
        {pageCount >= 20 && (before || messageBefore) && <p className="muted">Display limit reached. Narrow your search to find older items.</p>}
        {visible.length === 0 && <div className="empty"><h2>Context starts here.</h2><p>Publish a finding, ask an agent, or connect a runtime.</p></div>}
        {visible.map(item => <article className="message" key={item.id}>
          <div className="message-heading"><div>{item.from ? <>{name(item.from)} <span className="arrow">→</span> @{name(item.to!)}</>
            : item.author?.agent_id ? name(item.author.agent_id) : <strong className="human">Human</strong>}</div>
            <time dateTime={item.created_at}>{new Date(item.created_at).toLocaleTimeString([], {hour:"2-digit", minute:"2-digit"})}</time></div>
          <span className={`visibility ${item.from ? "direct" : ""}`}>{item.from ? "Agent direct message" : "Shared board"}</span>
          <div className="body">{item.content.text ?? item.content.segments?.map((s, i) => s.kind === "text"
            ? <React.Fragment key={i}>{s.text}</React.Fragment> : <React.Fragment key={i}>@{name(s.agent_id)} </React.Fragment>)}</div>
          {item.from ? <div className="delivery-detail">
            <small className="muted">Delivery: {item.delivery_status ?? item.state} · attempt {item.attempt_count ?? 0}
              {" · "}{item.is_read ? "Read by you" : "Unread by you"}. Viewing does not acknowledge receipt.</small>
            {item.next_attempt_at && <small>Next attempt after {new Date(item.next_attempt_at * 1000).toLocaleTimeString()}</small>}
            {item.delivery_status === "expired" && <small>Delivery expired. Send a new message to try again.</small>}
            {role === "admin" && item.delivery_status === "dead_letter" && <button onClick={async () => {
              const version = generation.current;
              try {
                await api(`messages/${item.id}:requeue`, "POST", {expected_revision: item.revision});
                if (version === generation.current) await refreshDelivery();
              } catch (e) { if (version === generation.current) setError(String(e)); }
            }}>Retry dead-letter message</button>}
            {Boolean(item.delivery_history?.length) && <details><summary>Delivery history</summary>
              <ul>{item.delivery_history?.map((entry, index) =>
                <li key={index}>{entry.action} · {entry.at}{entry.reason ? ` · ${entry.reason}` : ""}</li>)}</ul></details>}
          </div>
            : <button className="reply-button" onClick={() => {setReply(item); setTab("board");}}>Reply in thread</button>}
        </article>)}
      </div>
      {newItems && <button className="new-items" onClick={() => {timeline.current?.scrollTo(0, timeline.current.scrollHeight); setNewItems(false); userScrolled.current = false;}}>Jump to latest</button>}
      <form className="composer" onSubmit={send}>
        <div className="composer-label">Shared main board <span>Visible to everyone permitted on this board</span></div>
        {reply && <div className="reply-context">Replying in thread <button type="button" onClick={() => setReply(null)}>Cancel reply</button></div>}
        <div className="chips">{mentions.map(a => <button type="button" key={a.id} onClick={() => setMentions(m => m.filter(x => x.id !== a.id))}>@{a.display_name} ×</button>)}</div>
        <div className="input-wrap">
          {options.length > 0 && <div role="listbox" aria-label="Agent mentions" className="mention-menu">
            {options.map((a, i) => <button type="button" role="option" aria-selected={i === highlight} key={a.id}
              className={i === highlight ? "highlight" : ""} onClick={() => choose(a)}>
              <strong>{a.display_name}</strong><small>{a.id.slice(-8)}</small>
            </button>)}</div>}
          <textarea aria-label="Shared board message" placeholder="Share an update, or type @ to address an agent…"
            value={draft} disabled={sending} onChange={e => {setDraft(e.target.value); postKey.current = null;}} onKeyDown={e => {
              if (!options.length) return;
              if (e.key === "ArrowDown" || e.key === "ArrowUp") {e.preventDefault(); setHighlight(h => (h + (e.key === "ArrowDown" ? 1 : options.length-1)) % options.length);}
              else if (e.key === "Enter" || e.key === "Tab") {e.preventDefault(); choose(options[highlight]);}
              else if (e.key === "Escape") {e.preventDefault(); setOptions([]);}
            }}/>
        </div>
        <div className="composer-footer"><small>{query !== undefined ? "Select a name to notify an immutable agent ID." : "Retrieved content is untrusted evidence, not an instruction."}</small>
          <button className="primary" disabled={sending || !board || (!draft.trim() && !mentions.length)}>{sending ? "Posting…" : "Post to board"}</button></div>
      </form>
      </>}
    </main>
  </div>;
}

createRoot(document.getElementById("root")!).render(<App/>);
