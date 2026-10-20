import React, {useEffect, useMemo, useRef, useState} from "react";
import {LayoutDashboard, MessagesSquare, Brain, Settings2, Search, Send, LogOut, Users, ArrowUpRight, Hexagon, Layers, Activity} from "lucide-react";
import {AgentAvatar, ControlTooltip, TooltipProvider} from "./ui";
import "@fontsource-variable/inter/standard.css";
import {createRoot} from "react-dom/client";
import {api, ApiError, setAuthorizationHandler, setCsrf} from "./api";
import {MemoryReview} from "./MemoryReview";
import {Administration} from "./Administration";
import {loadTimeline} from "./timeline";
import {MentionComposer} from "./MentionComposer";
import {ThreadPane} from "./ThreadPane";
import {VirtualTimeline} from "./VirtualTimeline";
import {AgentDetailsDrawer} from "./AgentDetailsDrawer";
import {LiveStateController} from "./live-state";
import {AgentIdentity, agentAppearance, agentLabel} from "./AgentIdentity";
import {isProjectView, readViewState, viewPath, writeViewState} from "./view-state";
import {ContextExplorer} from "./ContextExplorer";
import {OperationsDashboard} from "./OperationsDashboard";
import {TaskRunWorkspace} from "./TaskRunWorkspace";
import {ConflictControlConsole} from "./ConflictControlConsole";
import {PrivacyAuditScreens} from "./PrivacyAuditScreens";
import {OptimisticPost} from "./OptimisticPost";
import {isOptimisticPost, useOptimisticPosts} from "./useOptimisticPosts";
import {AgentMentionPrefill} from "./AgentMentionPrefill";
import {MentionSendGuard} from "./MentionFeedback";
import {useMentionSendGuard} from "./mention-feedback";
import {BoardPostReadControl, BoardUnreadBadge, BoardUnreadToolbar, mergeSnapshotBoardUnread,
  useBoardUnread, type BoardUnreadState} from "./BoardUnread";
import "./style.css";

type Agent = {
  id: string; display_name: string; base_color_token: string; effective_role: string;
  description: string;
  current_tasks?: {id: string; title: string; status: string}[];
  owned_scopes?: {id: string; display_name: string}[];
  blockers?: {code: string; label: string; task_id?: string; run_id?: string}[];
};
type Segment = {kind: "text"; text: string} | {kind: "agent_mention"; agent_id: string};
type Item = {
  id: string; revision: number; created_at: string; created_seq: number; content: {text?: string; segments?: Segment[]};
  board_id?: string; client_post_id?: string;
  board_unread?: boolean;
  author?: {kind: string; agent_id?: string}; from?: string; to?: string;
  thread_id: string; state?: string; visibility?: string;
  delivery_status?: string; attempt_count?: number; total_attempts?: number;
  next_attempt_at?: number | null; claim_until?: number; is_read?: boolean;
  delivery_history?: {action: string; at: string; run_id?: string; reason?: string}[];
};
type DeliverySummary = {unread: number; states: Record<string, number>;
  by_agent: Record<string, {pending: number; dead_letter: number; unread: number}>};
type Project = {id: string; title: string};
type Board = {id: string; title: string; unread_count?: number; unread_truncated?: boolean};
type Run = {
  id: string; agent_id: string; presence: string; status: string;
  summary?: {text: string}; capture_health: {state: string};
};
type WorkspaceSnapshot = {
  agents: Agent[]; runs: Run[]; boards: Board[]; delivery: DeliverySummary | null; snapshot_cursor: string;
  board_unread?: BoardUnreadState;
};

function isTimelineView(tab: string) {
  return tab === "board" || tab === "conversations" || !isProjectView(tab);
}

function App() {
  const [authenticated, setAuthenticated] = useState(false);
  const [token, setToken] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const [projects, setProjects] = useState<Project[]>([]);
  const [project, setProject] = useState("");
  const [agents, setAgents] = useState<Agent[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [board, setBoard] = useState("");
  const [boards, setBoards] = useState<Board[]>([]);
  const [posts, setPosts] = useState<Item[]>([]);
  const [messages, setMessages] = useState<Item[]>([]);
  const [delivery, setDelivery] = useState<DeliverySummary | null>(null);
  const [tab, setTab] = useState("board");
  const [tabs, setTabs] = useState<string[]>([]);
  const [draft, setDraft] = useState("");
  const [mentions, setMentions] = useState<{id: string; display_name: string}[]>([]);
  const [mentionQueryActive, setMentionQueryActive] = useState(false);
  const [thread, setThread] = useState<Item | null>(null);
  const [detailsAgent, setDetailsAgent] = useState<string | null>(null);
  const [connection, setConnection] = useState("Connecting");
  const [error, setError] = useState("");
  const [timelineSnapshot, setTimelineSnapshot] = useState(0);
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
  const [accessRevoked, setAccessRevoked] = useState(false);
  const [accessEpoch, setAccessEpoch] = useState(0);
  const [sessionRestart, setSessionRestart] = useState(0);
  const [selfSessionBusy, setSelfSessionBusy] = useState(false);
  const selfSessionProject = useRef<string | null>(null);
  const [mobile, setMobile] = useState(() => window.matchMedia("(max-width: 800px)").matches);
  const [rosterOpen, setRosterOpen] = useState(false);
  const [viewReady, setViewReady] = useState(false);
  const timeline = useRef<HTMLDivElement>(null);
  const generation = useRef(0);
  const navigationIntent = useRef(0);
  const timelineGeneration = useRef(0);
  const boardProject = useRef("");
  const selectedBoard = useRef("");
  const oldest = useRef<{key: string; posts?: number; messages?: number}>({key: ""});
  const userScrolled = useRef(false);
  const threadTrigger = useRef<HTMLButtonElement | null>(null);
  const detailsTrigger = useRef<HTMLButtonElement | null>(null);
  const live = useRef<LiveStateController | null>(null);
  const workspaceCursor = useRef<string | null>(null);
  const refreshSnapshot = useRef<() => Promise<void>>(async () => {});
  const roster = useRef<HTMLElement>(null);
  const rosterToggle = useRef<HTMLButtonElement>(null);
  const rosterClose = useRef<HTMLButtonElement>(null);
  const activeAgentTab = useRef<HTMLButtonElement>(null);
  const restoreRoster = useRef(false);

  function closeRoster() {
    setRosterOpen(false);
    requestAnimationFrame(() => rosterToggle.current?.focus());
  }

  function selectTab(next: string) {
    navigationIntent.current++;
    if (next === tab) return;
    setTab(next); setPageDepth(1); setPageCount(1); setSearchDraft(""); setSearchText("");
    oldest.current = {key: ""};
    history.replaceState(null, "", viewPath(project, next) + writeViewState({board, query: "", pages: 1}));
  }

  function closeThread() {
    setThread(null);
    requestAnimationFrame(() => {
      if (threadTrigger.current?.isConnected) threadTrigger.current.focus();
      else timeline.current?.focus();
    });
  }

  function closeDetails() {
    setDetailsAgent(null);
    if (mobile && restoreRoster.current) setRosterOpen(true);
    restoreRoster.current = false;
    requestAnimationFrame(() => {
      if (detailsTrigger.current?.isConnected && detailsTrigger.current.getClientRects().length) detailsTrigger.current.focus();
      else if (mobile) rosterToggle.current?.focus();
      else timeline.current?.focus();
    });
  }

  function clearSensitiveState() {
    selfSessionProject.current = null; setSelfSessionBusy(false);
    generation.current++; timelineGeneration.current++;
    setPosts([]); setMessages([]); setDelivery(null); setAgents([]); setRuns([]);
    setBoards([]); setBoard(""); selectedBoard.current = ""; boardProject.current = "";
    setTabs([]); setTab("board"); setDetailsAgent(null); setThread(null); setDraft("");
    setMentions([]); setMentionQueryActive(false); setBefore(null); setMessageBefore(null);
    setMarkingRead(false); setLoadingTimeline(false);
    setNewItems(false); oldest.current = {key: ""};
    setPageDepth(1); setPageCount(1); userScrolled.current = false;
    workspaceCursor.current = null;
    setSearchText(""); setSearchDraft(""); setViewReady(false); setRosterOpen(false);
    restoreRoster.current = false;
    history.replaceState(null, "", location.pathname);
  }

  useEffect(() => {
    const media = window.matchMedia("(max-width: 800px)");
    const changed = () => {
      setMobile(media.matches);
      if (!media.matches) { setRosterOpen(false); restoreRoster.current = false; }
    };
    media.addEventListener("change", changed);
    return () => media.removeEventListener("change", changed);
  }, []);

  useEffect(() => {
    if (!mobile || !rosterOpen) return;
    rosterClose.current?.focus();
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") { event.preventDefault(); closeRoster(); return; }
      if (event.key !== "Tab" || !roster.current) return;
      const controls = Array.from(roster.current.querySelectorAll<HTMLButtonElement>("button:not(:disabled)"))
        .filter(element => element.getClientRects().length > 0);
      const first = controls[0], last = controls[controls.length - 1];
      if (event.shiftKey && (document.activeElement === first || !roster.current.contains(document.activeElement))) {
        event.preventDefault(); last?.focus();
      } else if (!event.shiftKey && (document.activeElement === last || !roster.current.contains(document.activeElement))) {
        event.preventDefault(); first?.focus();
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [mobile, rosterOpen]);

  useEffect(() => {
    if (!authenticated || !viewReady || accessRevoked || !project) return;
    const timelineView = isTimelineView(tab);
    if (timelineView && (!board || boardProject.current !== project)) return;
    history.replaceState(null, "", viewPath(project, tab) + (timelineView ? writeViewState({
      board, query: timelineView ? searchText : "", pages: timelineView ? Math.max(pageDepth, pageCount) : 1,
    }) : ""));
  }, [authenticated, viewReady, accessRevoked, project, board, tab, searchText, pageDepth, pageCount]);

  useEffect(() => {
    api<{csrf_token: string; role: string}>("session").then(session => {
      setCsrf(session.csrf_token); setRole(session.role); setAuthenticated(true);
    }).catch(() => {});
  }, []);

  async function login(event: React.FormEvent) {
    event.preventDefault();
    if (loggingIn || !token.trim()) return;
    setLoggingIn(true);
    try {
      const result = await api<{csrf_token: string; role: string}>("session", "POST", {}, token);
      setCsrf(result.csrf_token); setRole(result.role);
      setToken(""); setError(""); setAccessRevoked(false); setAuthenticated(true);
    } catch (e) { setError(String(e)); }
    finally { setLoggingIn(false); }
  }

  useEffect(() => {
    if (!authenticated) return;
    let disposed = false;
    api<Project[]>("projects").then(data => {
      if (disposed) return;
      setProjects(data);
      const route = location.pathname.split("/")[2];
      setProject(data.find(p => p.id === route)?.id ?? data[0]?.id ?? "");
    }).catch(e => { if (!disposed) setError(String(e)); });
    return () => { disposed = true; };
  }, [authenticated]);

  useEffect(() => {
    if (!authenticated || !project) return;
    const resumingSession = selfSessionProject.current === project;
    selfSessionProject.current = null;
    generation.current++;
    const version = generation.current;
    if (!resumingSession) {
    workspaceCursor.current = null;
    setViewReady(false); setRosterOpen(false); restoreRoster.current = false;
    setAccessRevoked(false);
    setPosts([]); setMessages([]); setDelivery(null); setAgents([]); setRuns([]); setTabs([]);
    userScrolled.current = false; setNewItems(false);
    setBoard(""); setBoards([]); selectedBoard.current = ""; boardProject.current = ""; setSearchText(""); setSearchDraft(""); setPageDepth(1); setPageCount(1);
    setTab("board"); setMentions([]); setThread(null); setDetailsAgent(null); setDraft(""); setBefore(null);
    }
    let disposed = false;
    let firstRefresh = !resumingSession;
    const initialNavigationIntent = navigationIntent.current;
    const initialRoute = location.pathname.split("/");
    const routeMatches = !initialRoute[2] || initialRoute[2] === project;
    const savedView = readViewState(routeMatches ? location.hash : "");
    let refreshing = false;
    let queued = false;
    async function refresh() {
      if (refreshing) { queued = true; return; }
      refreshing = true;
      do {
        queued = false;
        try {
          const [snapshot, accessibleProjects] = await Promise.all([
            api<WorkspaceSnapshot>(`projects/${project}/dashboard-snapshot`),
            api<Project[]>("projects"),
          ]);
          if (disposed || version !== generation.current) break;
          const {agents: people, boards: accessibleBoards} = snapshot;
          workspaceCursor.current = snapshot.snapshot_cursor;
          setAgents(people); setRuns(snapshot.runs); setProjects(accessibleProjects); setDelivery(snapshot.delivery);
          if (!people.some(person => person.id === detailsTrigger.current?.dataset.agentId)) setDetailsAgent(null);
          if (firstRefresh && navigationIntent.current === initialNavigationIntent) {
            let initialTab = "board";
            if (routeMatches) {
              if (initialRoute[3] === "agents" && people.some(a => a.id === initialRoute[4])) {
                setTabs([initialRoute[4]]); initialTab = initialRoute[4];
              } else if (initialRoute[3] === "conversations" && (role === "admin" || role === "observer")) {
                initialTab = "conversations";
              } else if (initialRoute[3] === "memories" && ["admin", "observer", "human"].includes(role)) {
                initialTab = "memories";
              } else if (initialRoute[3] === "administration" && ["admin", "human"].includes(role)) {
                initialTab = "administration";
              } else if (["context", "operations", "tasks", "coordination", "privacy"].includes(initialRoute[3])) {
                initialTab = initialRoute[3];
              }
            }
            setTab(initialTab);
            const validBoard = !savedView.board || accessibleBoards.some(item => item.id === savedView.board);
            const validView = !initialRoute[3] || initialRoute[3] === "board"
              || initialTab === initialRoute[3] || (initialRoute[3] === "agents" && initialTab === initialRoute[4]);
            if (validBoard && validView && isTimelineView(initialTab)) {
              setSearchText(savedView.query); setSearchDraft(savedView.query); setPageDepth(savedView.pages);
            }
            if (validBoard && savedView.board) selectedBoard.current = savedView.board;
          }
          if (firstRefresh) {
            setViewReady(true); firstRefresh = false;
          }
          setBoards(mergeSnapshotBoardUnread(accessibleBoards, snapshot.board_unread));
          const boardId = (accessibleBoards.find(item => item.id === selectedBoard.current)
            ?? accessibleBoards.find(item => item.title === "Main board") ?? accessibleBoards[0])?.id;
          if (boardId) {
            if (selectedBoard.current && selectedBoard.current !== boardId) {
              setThread(null); setDraft(""); setMentions([]);
            }
            selectedBoard.current = boardId;
            boardProject.current = project; setBoard(boardId);
          } else {
            selectedBoard.current = ""; boardProject.current = ""; setBoard(""); setPosts([]); setMessages([]);
            setThread(null); setDraft(""); setMentions([]);
          }
          setTimelineReload(value => value + 1);
          if (!userScrolled.current) requestAnimationFrame(() => timeline.current?.scrollTo(0, timeline.current.scrollHeight));
          else setNewItems(true);
          setError("");
        } catch (e) {
          if (!disposed && version === generation.current && !controller.reportError(e)) {
            setError(String(e)); setConnection("Degraded");
            setPosts([]); setMessages([]); setDelivery(null); setAgents([]); setRuns([]);
            setTimelineSnapshot(current => current + 1);
            setThread(null); setDetailsAgent(null);
          }
        }
      } while (queued && !disposed);
      refreshing = false;
    }
    const controller = new LiveStateController({
      url: `/v1/streams?project_id=${encodeURIComponent(project)}`,
      probe: () => api<{csrf_token: string; role: string}>("session"),
      onResync: info => {
        if (!disposed && info.matchesSnapshot !== true) void refresh();
      },
      onRevoke: failure => {
        if (disposed) return;
        clearSensitiveState(); setProjects([]); setAccessRevoked(true);
        setConnection("Access revoked");
        setError(failure.status === 401 ? "Your session ended. Sign in again."
          : "Your access to this workspace changed. Recheck access after it is restored.");
        if (failure.status === 401) {
          setAuthenticated(false); setProject(""); setRole(""); setCsrf("");
        }
      },
      onState: state => {
        if (disposed) return;
        if (state === "reconnecting") {
          setPosts([]); setMessages([]); setDelivery(null); setThread(null); setDetailsAgent(null);
          setTimelineSnapshot(current => current + 1);
        }
        setConnection(state === "live" ? "Live" : state === "connecting" ? "Connecting"
          : state === "revoked" ? "Access revoked" : state === "stopped" ? "Stopped" : "Reconnecting");
      },
      onHeartbeat: result => {
        if (disposed || version !== generation.current) return;
        const session = result as {csrf_token: string; role: string};
        setCsrf(session.csrf_token);
        if (session.role !== role) { clearSensitiveState(); setRole(session.role); return; }
        void refresh();
      },
    });
    live.current = controller;
    refreshSnapshot.current = refresh;
    setAuthorizationHandler(error => {
      if (!disposed && version === generation.current) controller.reportError(error);
    });
    controller.start(); void refresh();
    return () => {
      disposed = true; controller.stop();
      if (live.current === controller) {
        live.current = null; refreshSnapshot.current = async () => {}; setAuthorizationHandler(null);
      }
    };
  }, [project, role, authenticated, accessEpoch, sessionRestart]);

  function suspendSelfSession() {
    setSelfSessionBusy(true);
    selfSessionProject.current = project;
    generation.current++;
    live.current?.stop();
    setAuthorizationHandler(null);
  }

  function finishSelfRevocation() {
    selfSessionProject.current = null;
    clearSensitiveState(); setCsrf(""); setToken("");
    setAuthenticated(false); setProject(""); setProjects([]);
  }

  async function resumeSelfSession(replacementToken?: string) {
    try {
      const session = await api<{csrf_token: string; role: string}>(
        "session", replacementToken ? "POST" : "GET", replacementToken ? {} : undefined, replacementToken);
      setCsrf(session.csrf_token); setRole(session.role); setSessionRestart(current => current + 1);
    } catch {
      finishSelfRevocation();
      setError("The credential session could not be renewed. Sign in with the replacement credential.");
    } finally {
      setSelfSessionBusy(false);
    }
  }

  useEffect(() => {
    if (!project || !board || boardProject.current !== project || !isTimelineView(tab)) return;
    const version = ++timelineGeneration.current;
    let disposed = false;
    let retry: number | undefined;
    const key = `${project}:${board}:${tab}:${searchText}`;
    const changed = oldest.current.key !== key;
    if (changed) {
      oldest.current = {key}; setPosts([]); setMessages([]); setBefore(null); setMessageBefore(null);
    }
    setLoadingTimeline(true);
    const agent = !["board", "conversations"].includes(tab) ? tab : undefined;
    const cursor = workspaceCursor.current;
    const params = `?limit=100&query=${encodeURIComponent(searchText)}${cursor ? `&snapshot_cursor=${cursor}` : ""}`;
    const postPath = `boards/${board}/posts${params}${agent ? `&agent_id=${encodeURIComponent(agent)}` : ""}`;
    const messagePath = agent ? `agents/${agent}/conversation${params}` : `projects/${project}/conversations${params}`;
    async function load() {
      try {
        const [boardPage, messagePage] = await Promise.all([
          tab !== "conversations" ? loadTimeline<Item>(postPath, pageDepth, oldest.current.posts) : null,
          tab !== "board" && ["admin", "observer"].includes(role)
            ? loadTimeline<Item>(messagePath, pageDepth, oldest.current.messages) : null,
        ]);
        if (disposed || version !== timelineGeneration.current) return;
        if (boardPage?.cursor_drifted || messagePage?.cursor_drifted ||
            (cursor && ((boardPage && boardPage.snapshot_cursor !== cursor) ||
                        (messagePage && messagePage.snapshot_cursor !== cursor))) ||
            (boardPage?.snapshot_cursor && messagePage?.snapshot_cursor &&
             boardPage.snapshot_cursor !== messagePage.snapshot_cursor)) {
          throw new ApiError(409, "Snapshot changed while loading. Refresh this view.", "snapshot_changed");
        }
        live.current?.bindSnapshot(boardPage?.snapshot_cursor ?? messagePage?.snapshot_cursor ?? null);
        setPosts(boardPage?.items ?? []); setMessages(messagePage?.items ?? []);
        setTimelineSnapshot(current => current + 1);
        setBefore(boardPage?.next_before ?? null); setMessageBefore(messagePage?.next_before ?? null);
        setPageCount(Math.max(boardPage?.page_count ?? 1, messagePage?.page_count ?? 1));
        oldest.current = {key, posts: boardPage?.items[0]?.created_seq, messages: messagePage?.items[0]?.created_seq};
        setError("");
        if (!userScrolled.current) requestAnimationFrame(() => timeline.current?.scrollTo(0, timeline.current.scrollHeight));
      } catch (e) {
        if (!disposed && version === timelineGeneration.current && !live.current?.reportError(e)) {
          setPosts([]); setMessages([]); setBefore(null); setMessageBefore(null); setError(String(e));
          setTimelineSnapshot(current => current + 1);
          if (e instanceof ApiError && e.code === "snapshot_changed") {
            retry = window.setTimeout(() => {
              if (!disposed && version === timelineGeneration.current) void refreshSnapshot.current();
            }, 250);
          }
        }
      } finally { if (!disposed && version === timelineGeneration.current) setLoadingTimeline(false); }
    }
    void load();
    return () => { disposed = true; clearTimeout(retry); };
  }, [project, board, tab, role, searchText, pageDepth, timelineReload]);

  function openAgent(id: string) {
    setTabs(t => t.includes(id) ? t : [...t, id]);
    selectTab(id);
    if (mobile && rosterOpen) {
      setRosterOpen(false);
      requestAnimationFrame(() => activeAgentTab.current?.focus());
    }
  }

  async function refreshDelivery() {
    await refreshSnapshot.current();
  }

  function name(id: string) {
    const person = agents.find(a => a.id === id);
    return <button className={`agent-name ${agentAppearance(person)}`}
      onClick={() => openAgent(id)} aria-label={agentLabel(person, id)}>
      <AgentIdentity agent={person} fallback={id.slice(0, 18)}/>
    </button>;
  }

  function choose(person: {id: string; display_name: string}) {
    setMentions(m => m.some(a => a.id === person.id) ? m : [...m, person]);
  }

  const mentionGuard = useMentionSendGuard({
    value: draft, selections: mentions, contextKey: `${authenticated}:${project}:${board}:${role}:${accessEpoch}`,
  });
  useEffect(() => {
    if (mentionGuard.receiptPostId) mentionGuard.trackReceipts(mentionGuard.receiptPostId);
  }, [timelineSnapshot]);

  const optimistic = useOptimisticPosts<Item>({
    project, board, sessionKey: `${authenticated}:${role}:${accessEpoch}`,
    canonical: posts, snapshotVersion: timelineSnapshot,
    onConfirmed: post => { mentionGuard.trackReceipts(post.id); void refreshSnapshot.current(); },
    onError: failure => {
      const error = failure.status === undefined ? new Error(failure.message)
        : new ApiError(failure.status, failure.message, failure.code);
      if (!live.current?.reportError(error)) setError(failure.message);
    },
  });

  function send(event: React.FormEvent) {
    event.preventDefault();
    if (!board || accessRevoked || loadingTimeline || (!draft.trim() && !mentions.length)) return;
    if (!mentionGuard.trySend()) return;
    const segments: Segment[] = mentions.map(a => ({kind: "agent_mention", agent_id: a.id}));
    if (draft.trim()) segments.push({kind: "text", text: draft});
    if (optimistic.send({kind: "message", content: {segments}})) {
      setDraft(""); setMentions([]); setError(""); mentionGuard.reset();
    }
  }

  const visible = useMemo(() => tab === "board" ? posts : tab === "conversations" ? messages : [
    ...messages, ...posts,
  ].sort((a,b) => a.created_seq - b.created_seq), [tab, posts, messages]);
  const boardReads = useBoardUnread({
    projectId: authenticated && !accessRevoked && board ? project : "", boardId: board,
    refreshVersion: timelineReload, snapshotCursor: workspaceCursor.current,
    permissionKey: `${authenticated}:${role}:${accessEpoch}`,
    onError: message => { if (message.startsWith("409:")) void refreshSnapshot.current(); },
  });
  const timelineItems = tab === "board" ? boardReads.decorate(optimistic.items)
    : visible.map(item => item.from ? item : boardReads.decorate([item])[0]);
  const tabAgent = !isProjectView(tab)
    ? agents.find(agent => agent.id === tab) : undefined;

  if (!authenticated) return <main className="login">
    <div className="brand-mark" aria-hidden="true">H</div><p className="eyebrow">THE SHARED CONTEXT PLANE</p>
    <h1>Hivemind</h1><p>One place for agents, evidence, and ownership.</p>
    <form onSubmit={login} aria-busy={loggingIn}><label htmlFor="credential">Local human credential</label>
      <input id="credential" type="password" required autoComplete="off" value={token} onChange={e => setToken(e.target.value)}/>
      <button className="primary" disabled={loggingIn || !token.trim()}>{loggingIn ? "Opening workspace…" : "Open workspace"}</button></form>
    <p className="muted">Use the token from your private owner credential file. It stays out of URLs and browser storage.</p>
    {error && <p role="alert">{error}</p>}
  </main>;

  return <div className="shell" inert={selfSessionBusy} aria-busy={selfSessionBusy}>
    <a className="skip-link" href="#workspace" inert={mobile && rosterOpen}>Skip to workspace</a>
    <header inert={mobile && rosterOpen}><div><strong className="brand">Hivemind</strong><span className="header-caption">MEMORY & COORDINATION</span></div>
      <svg className="header-honeycomb" aria-hidden="true" focusable="false" width="100%" height="100%">
        <defs>
          <pattern id="header-honeycomb-pattern" width="72" height="42" patternUnits="userSpaceOnUse">
            <path d="M12 0H36L48 21L36 42H12L0 21ZM48 21H72M48 21L60 0M48 21L60 42"/>
          </pattern>
        </defs>
        <rect width="100%" height="100%" fill="url(#header-honeycomb-pattern)"/>
      </svg>
      <svg className="header-beacon" aria-hidden="true" focusable="false" viewBox="0 0 140 100">
        <path d="M72 8H108L126 39L108 70H72L54 39Z"/>
        <path d="M18 39H54L72 70L54 101H18L0 70Z"/>
      </svg>
      <label className="project-label">Workspace <select aria-label="Project" value={project} onChange={e => {
        history.replaceState(null, "", viewPath(e.target.value, "board"));
        setProject(e.target.value);
      }}>
        {projects.map(p => <option key={p.id} value={p.id}>{p.title}</option>)}</select></label>
      <span role="status" className={`connection ${connection === "Live" ? "live" : ""}`}>{connection}</span>
      <span className="role-badge">{role}</span>
      <button onClick={async () => {
        try {
          await api("session", "DELETE");
          clearSensitiveState(); setAuthenticated(false); setProject(""); setProjects([]); setRole("");
        } catch (e) { setError(String(e)); }
      }}><LogOut className="ui-icon" aria-hidden="true"/>Sign out</button>
    </header>
    {mobile && rosterOpen && <div className="mobile-roster-backdrop" onClick={closeRoster}/>}
    <aside ref={roster} id="agent-roster" className={`agent-roster ${mobile && rosterOpen ? "mobile-open" : ""}`}
      role={mobile && rosterOpen ? "dialog" : "complementary"}
      aria-modal={mobile && rosterOpen ? true : undefined} aria-label="Agent roster">
      <div className="section-title">AGENTS <span>{agents.length}</span>
        <button className="mobile-roster-close" ref={rosterClose} onClick={closeRoster}
          aria-label="Close agent roster">Close</button></div>
      {agents.length === 0 && <p className="muted">No agents registered yet.</p>}
      {agents.map(a => {
        const active = runs.filter(r => r.agent_id === a.id);
        return <section className="roster-card" key={a.id}>
          <div className="roster-identity"><AgentAvatar name={a.display_name} appearance={agentAppearance(a)}/>{name(a.id)}</div>
          <ControlTooltip text="Review tasks, run health, and evidence">
          <button className="roster-details-button" data-agent-id={a.id} aria-label={`Details for ${a.display_name}`}
            aria-expanded={detailsAgent === a.id} onClick={event => {
              detailsTrigger.current = event.currentTarget; setDetailsAgent(a.id);
              restoreRoster.current = mobile && rosterOpen;
              if (mobile) setRosterOpen(false);
            }}>Details <ArrowUpRight className="ui-icon" aria-hidden="true"/></button>
          </ControlTooltip>
          <p>{active[0]?.summary?.text ?? a.description ?? "Ready to connect"}</p>
          <dl className="roster-work">
            <div><dt>Current task</dt><dd>{a.current_tasks?.length
              ? <ul>{a.current_tasks.map(task => <li key={task.id}>{task.title} <span className="muted">({task.status})</span></li>)}</ul>
              : "No current task"}</dd></div>
            <div><dt>Owned scopes</dt><dd>{a.owned_scopes?.length
              ? <ul>{a.owned_scopes.map(scope => <li key={scope.id}>{scope.display_name}</li>)}</ul>
              : "No owned scopes"}</dd></div>
            <div><dt>Blockers</dt><dd>{a.blockers?.length
              ? <ul className="warning">{a.blockers.map((blocker, index) => <li key={index}>{blocker.label}</li>)}</ul>
              : "No reported blockers"}</dd></div>
          </dl>
          <div className="run-tags">{active.map(r => <span key={r.id}>{r.presence} · {r.status}</span>)}</div>
          {delivery && <small className="roster-unread" aria-label={`${a.display_name} unread count`}>
            {delivery.by_agent[a.id]?.unread ?? 0} unread conversations</small>}
          {delivery?.by_agent[a.id] && <small className="muted">{delivery.by_agent[a.id].pending} pending messages
            {" · "}{delivery.by_agent[a.id].dead_letter} dead letters</small>}
          {active.some(r => r.capture_health.state !== "healthy") && <small className="warning">Capture incomplete or unconfirmed</small>}
        </section>;
      })}
      <div className="aside-note">Structured events are evidence.<br/>Messages are not permissions.</div>
    </aside>
    <main className="workspace" id="workspace" tabIndex={-1} inert={mobile && rosterOpen}>
      <nav className="tabs" aria-label="Workspace tabs">
        <button ref={rosterToggle} className="mobile-agents-button" aria-label="Open agent roster"
          aria-expanded={rosterOpen} aria-controls="agent-roster" disabled={accessRevoked}
          onClick={() => setRosterOpen(true)}><Users className="ui-icon" aria-hidden="true"/>Agents <span>{agents.length}</span></button>
        <button aria-label="Main board" aria-current={tab === "board" ? "page" : undefined} className={tab === "board" ? "selected" : ""} onClick={() => selectTab("board")}><LayoutDashboard className="ui-icon" aria-hidden="true"/>Main board
          <BoardUnreadBadge count={boards.find(item => item.id === board)?.unread_count ?? 0}
            truncated={boards.find(item => item.id === board)?.unread_truncated}
            available={boards.find(item => item.id === board)?.unread_count !== undefined}/></button>
        {(role === "admin" || role === "observer") && <button aria-label="Agent conversations" aria-current={tab === "conversations" ? "page" : undefined} className={tab === "conversations" ? "selected" : ""} onClick={() => selectTab("conversations")}><MessagesSquare className="ui-icon" aria-hidden="true"/>Agent conversations
          {Boolean(delivery?.unread) && <span className="unread-badge">{delivery?.unread}</span>}</button>}
        {["admin", "observer", "human"].includes(role) && <button className={tab === "memories" ? "selected" : ""}
          aria-current={tab === "memories" ? "page" : undefined}
          onClick={() => selectTab("memories")}><Brain className="ui-icon" aria-hidden="true"/>Memory review</button>}
        {["admin", "human"].includes(role) && <button className={tab === "administration" ? "selected" : ""}
          aria-current={tab === "administration" ? "page" : undefined}
          onClick={() => selectTab("administration")}><Settings2 className="ui-icon" aria-hidden="true"/>Administration</button>}
        <button className={tab === "context" ? "selected" : ""} aria-current={tab === "context" ? "page" : undefined}
          onClick={() => selectTab("context")}><Layers className="ui-icon" aria-hidden="true"/>Context explorer</button>
        <button className={tab === "operations" ? "selected" : ""} aria-current={tab === "operations" ? "page" : undefined}
          onClick={() => selectTab("operations")}><Activity className="ui-icon" aria-hidden="true"/>Operations</button>
        <button className={tab === "tasks" ? "selected" : ""} aria-current={tab === "tasks" ? "page" : undefined}
          onClick={() => selectTab("tasks")}><Layers className="ui-icon" aria-hidden="true"/>Tasks & runs</button>
        <button className={tab === "coordination" ? "selected" : ""} aria-current={tab === "coordination" ? "page" : undefined}
          onClick={() => selectTab("coordination")}><Activity className="ui-icon" aria-hidden="true"/>Coordination</button>
        <button className={tab === "privacy" ? "selected" : ""} aria-current={tab === "privacy" ? "page" : undefined}
          onClick={() => selectTab("privacy")}><Settings2 className="ui-icon" aria-hidden="true"/>Privacy & audit</button>
        {tabs.map(id => {
          const person = agents.find(agent => agent.id === id);
          return <span key={id} className="tab-group">
            <button ref={tab === id ? activeAgentTab : undefined}
              className={`agent-tab ${agentAppearance(person)} ${tab === id ? "selected" : ""}`}
              aria-label={agentLabel(person, id)} aria-current={tab === id ? "page" : undefined}
              onClick={() => selectTab(id)}><AgentIdentity agent={person} fallback={id}/></button>
            <button aria-label={`Close ${person?.display_name ?? id} tab`} onClick={() => {
              setTabs(current => current.filter(agent => agent !== id)); if (tab === id) selectTab("board");
            }}>×</button></span>;
        })}
      </nav>
      {accessRevoked ? <div className="empty">
        <h1>Access changed.</h1><p role="alert">{error}</p>
        <button onClick={async () => {
          try {
            const session = await api<{csrf_token: string; role: string}>("session");
            setCsrf(session.csrf_token); setRole(session.role); setAccessEpoch(value => value + 1);
          } catch (e) {
            if (e instanceof ApiError && e.status === 401) {
              clearSensitiveState(); setAuthenticated(false); setProject(""); setRole(""); setCsrf("");
            }
            setError(String(e));
          }
        }}>Recheck access</button>
      </div> : tab === "administration" && ["admin", "human"].includes(role)
        ? <Administration key={project} project={project} role={role} agents={agents}
          onBeforeSelfMutation={suspendSelfSession} onSelfRotation={resumeSelfSession}
          onSelfRevocation={finishSelfRevocation}
          onSelfMutationUnchanged={resumeSelfSession}
          onCredentialError={() => { if (selfSessionProject.current) void resumeSelfSession(); }}/>
        : tab === "memories" ? <MemoryReview key={project} project={project}/>
        : tab === "context" ? <ContextExplorer key={project} project={project} role={role} agents={agents}
          refreshVersion={timelineReload} permissionKey={`${authenticated}:${role}:${accessEpoch}:${sessionRestart}`}
          onError={setError} onOpenAgent={openAgent}/>
        : tab === "operations" ? <OperationsDashboard key={project} project={project} role={role} agents={agents}
          refreshVersion={timelineReload} permissionKey={`${authenticated}:${role}:${accessEpoch}:${sessionRestart}`}
          onError={setError} onOpenAgent={openAgent}/>
        : tab === "tasks" ? <TaskRunWorkspace key={project} project={project} role={role} agents={agents}
          refreshVersion={timelineReload} permissionKey={`${authenticated}:${role}:${accessEpoch}:${sessionRestart}`}
          onError={setError} onOpenAgent={openAgent}/>
        : tab === "coordination" ? <ConflictControlConsole key={project} project={project} role={role} agents={agents}
          refreshVersion={timelineReload} permissionKey={`${authenticated}:${role}:${accessEpoch}:${sessionRestart}`}
          onError={setError} onOpenAgent={openAgent}/>
        : tab === "privacy" ? <PrivacyAuditScreens key={project} project={project} role={role} agents={agents}
          refreshVersion={timelineReload} permissionKey={`${authenticated}:${role}:${accessEpoch}:${sessionRestart}`}
          onError={setError} onOpenAgent={openAgent}/> : <>
      <div className="timeline-heading"><div><p className="eyebrow">{tab === "board" ? "PROJECT BLACKBOARD" : "AUTHORIZED CONVERSATIONS"}</p>
        <h1>{tab === "board" ? "A shared view of the work." : tab === "conversations" ? "Agents, in conversation." : agents.find(a => a.id === tab)?.display_name}</h1></div>
        <div className="board-view-controls">{boards.length > 1 && tab === "board" && <label className="board-picker">Board
          <select aria-label="Active board" value={board} onChange={e => {
            selectedBoard.current = e.target.value; setBoard(e.target.value); setPosts([]); setDraft("");
            setMentions([]); setThread(null); setSearchText(""); setSearchDraft(""); setPageDepth(1); setPageCount(1);
          }}>{boards.map(item => <option key={item.id} value={item.id}>{item.title}
            {item.unread_count ? ` (${item.unread_count}${item.unread_truncated ? "+" : ""} unread)` : ""}</option>)}</select></label>}
          <span className="count">{timelineItems.length} items</span></div></div>
      {error && <div className="error" role="alert">{error}</div>}
      <form className="timeline-search" onSubmit={event => {
        event.preventDefault(); setPageDepth(1); setPageCount(1); oldest.current = {key: ""}; setSearchText(searchDraft.trim());
        setTimelineReload(value => value + 1);
      }}>
        <label htmlFor="timeline-search">Search this view</label>
        <input id="timeline-search" type="search" maxLength={256} value={searchDraft} onChange={e => setSearchDraft(e.target.value)}
          placeholder="Search authorized history"/>
        <button disabled={loadingTimeline}><Search className="ui-icon" aria-hidden="true"/>Search</button>
        {searchText && <button type="button" onClick={() => {
          setSearchDraft(""); setSearchText(""); setPageDepth(1); setPageCount(1); oldest.current = {key: ""};
        }}>Clear search</button>}
        <span role="status">{loadingTimeline ? "Loading…" : `${visible.length} loaded${searchText ? " matches" : " items"}`}</span>
      </form>
      {tab !== "conversations" && <BoardUnreadToolbar projectId={project} boardId={board}
        items={posts} controller={boardReads} markLabel="Mark loaded posts read"
        onMarked={() => { void refreshSnapshot.current(); }}/>}
      {tab !== "board" && (role === "admin" || role === "observer") && <div className="delivery-toolbar">
        <span aria-label="Unread conversations">{delivery?.unread ?? 0} unread</span>
        <span>{delivery?.states.retry_wait ?? 0} retrying · {delivery?.states.dead_letter ?? 0} dead letters</span>
        <button onClick={() => void refreshDelivery()}>Refresh delivery</button>
        <button title="Marks all loaded conversations in this view, including off-screen rows"
          disabled={markingRead || loadingTimeline || !visible.some(item => item.from && !item.is_read)} onClick={async () => {
          const version = generation.current;
          const ids = visible.filter(item => item.from && !item.is_read).map(item => item.id);
          setMarkingRead(true);
          try {
            for (let index = 0; index < ids.length; index += 100) {
              if (version !== generation.current) break;
              await api(`projects/${project}/conversations:read`, "POST", {message_ids: ids.slice(index, index + 100)});
            }
            if (version === generation.current) await refreshDelivery();
          } catch (e) { if (version === generation.current && !live.current?.reportError(e)) setError(String(e)); }
          finally { if (version === generation.current) setMarkingRead(false); }
        }}>Mark displayed conversations read</button>
      </div>}
      <VirtualTimeline className="timeline" ref={timeline} items={timelineItems}
        getKey={optimistic.keyOf} loading={loadingTimeline} loadingSlot={null}
        ariaLabel={tab === "board" ? "Board posts" : "Conversations"} onScroll={() => {
        const el = timeline.current; if (el) userScrolled.current = el.scrollHeight - el.scrollTop - el.clientHeight > 80;
      }} header={<>
        {(before || messageBefore) && <button disabled={loadingTimeline || pageCount >= 20}
          onClick={() => setPageDepth(Math.max(pageDepth, pageCount) + 1)}>
          {tab === "board" ? "Load earlier posts" : tab === "conversations" ? "Load earlier conversations" : "Load earlier activity"}
        </button>}
        {pageCount >= 20 && (before || messageBefore) && <p className="muted">Display limit reached. Narrow your search to find older items.</p>}
      </>} empty={<div className="empty"><span className="empty-mark"><Hexagon aria-hidden="true"/></span><h2>{searchText ? "No matching history." : tab === "board" ? "Context starts here." : "No conversations yet."}</h2>
        <p>{searchText ? "Try a different phrase or clear the search to see this view."
          : tab === "board" ? "Publish a finding, ask an agent, or connect a runtime." : "Authorized agent messages will appear here. Posts below are shared on the board, not sent privately."}</p></div>}
        renderItem={item => isOptimisticPost(item)
          ? <OptimisticPost row={item} onRetry={optimistic.retry} onCancel={optimistic.cancel} renderMention={id => name(id)}/>
          : <article className="message" key={item.id}>
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
              } catch (e) { if (version === generation.current && !live.current?.reportError(e)) setError(String(e)); }
            }}>Retry dead-letter message</button>}
            {Boolean(item.delivery_history?.length) && <details><summary>Delivery history</summary>
              <ul>{item.delivery_history?.map((entry, index) =>
                <li key={index}>{entry.action} · {entry.at}{entry.reason ? ` · ${entry.reason}` : ""}</li>)}</ul></details>}
          </div>
            : <button className="reply-button" data-thread-pane-trigger onClick={event => {
              threadTrigger.current = event.currentTarget; setThread(item);
            }}>Reply in thread</button>}
          {!item.from && item.board_unread !== undefined && <BoardPostReadControl boardId={board}
            postId={item.id} revision={item.revision} unread={item.board_unread} controller={boardReads}
            disabled={boardReads.marking || boardReads.loading || loadingTimeline}
            onMarked={() => { void refreshSnapshot.current(); }}/>}
        </article>}/>
      {newItems && <button className="new-items" onClick={() => {timeline.current?.scrollTo(0, timeline.current.scrollHeight); setNewItems(false); userScrolled.current = false;}}>Jump to latest</button>}
      <form className="composer" onSubmit={send}>
        <div className="composer-label">Shared {boards.find(item => item.id === board)?.title ?? "board"} <span>Visible to everyone permitted on this board</span></div>
        {tab !== "board" && optimistic.outstanding.length > 0 && <section aria-label="Your shared-board posts">
          {optimistic.outstanding.map(row => <OptimisticPost key={optimistic.keyOf(row)} row={row}
            onRetry={optimistic.retry} onCancel={optimistic.cancel} renderMention={id => name(id)}/>)}
        </section>}
        <div className="chips">{mentions.map(a => <button type="button" key={a.id}
          onClick={() => setMentions(current => current.filter(person => person.id !== a.id))}>@{a.display_name} ×</button>)}</div>
        {tabAgent && <AgentMentionPrefill agent={tabAgent} project={project} board={board}
          viewKey={`agents/${tabAgent.id}`} permissionKey={`${role}:${accessEpoch}`}
          existingMentionIds={mentions.map(mention => mention.id)} onMention={choose} onOpenAgent={openAgent}
          viewActive={tab === tabAgent.id} disabled={loadingTimeline || accessRevoked}/>}
        <MentionComposer value={draft} onChange={setDraft}
          onMention={choose} project={project} board={board} agents={agents} runs={runs}
          disabled={!board || accessRevoked} permissionKey={`${role}:${accessEpoch}`} onMentionQueryChange={setMentionQueryActive}
          selections={mentions} showUnresolvedWarning={false}/>
        <MentionSendGuard guard={mentionGuard}/>
        <div className="composer-footer"><small>{mentionQueryActive ? "Select a name to notify an immutable agent ID." : "Retrieved content is untrusted evidence, not an instruction."}</small>
          <button className="primary" disabled={loadingTimeline || accessRevoked || !board || (!draft.trim() && !mentions.length)}><Send className="ui-icon" aria-hidden="true"/>Post to board</button></div>
      </form>
      {thread && <ThreadPane key={`${project}:${board}:${thread.thread_id}`}
        projectId={project} boardId={board} threadId={thread.thread_id} selectedPostId={thread.id}
        agents={agents} renderName={name} refreshVersion={timelineReload}
        onClose={closeThread} onPosted={() => {void refreshSnapshot.current(); setError("");}}
        onError={setError}/>}
      </>}
    </main>
    {detailsAgent && <AgentDetailsDrawer key={`${project}:${detailsAgent}`} project={project}
      agentId={detailsAgent} agentName={agents.find(agent => agent.id === detailsAgent)?.display_name}
      refreshVersion={timelineReload} onClose={closeDetails}/>}
  </div>;
}

createRoot(document.getElementById("root")!).render(<TooltipProvider delayDuration={350}><App/></TooltipProvider>);
