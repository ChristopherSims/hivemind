import React, {useEffect, useMemo, useRef, useState} from "react";
import {api, ApiError} from "./api";
import {normalizeSnapshotCursor} from "./live-state";
import {MentionComposer, type MentionSelection} from "./MentionComposer";
import {MentionSendGuard} from "./MentionFeedback";
import {useMentionSendGuard} from "./mention-feedback";
import {OptimisticPost} from "./OptimisticPost";
import {useOptimisticPosts} from "./useOptimisticPosts";
import "./thread-pane.css";

/** Structured post content, identical to the canonical board posting format. */
export type ThreadSegment = {kind: "text"; text: string} | {kind: "agent_mention"; agent_id: string};
export type ThreadAuthor = {kind: string; agent_id?: string; principal_id?: string};
export type ThreadPost = {
  id: string; revision: number; created_at: string; created_seq: number;
  board_id: string; thread_id: string; parent_post_id?: string | null;
  kind?: string; state?: string; visibility?: string;
  author?: ThreadAuthor; content?: {text?: string; segments?: ThreadSegment[]};
  client_post_id?: string;
};
type CanonicalThreadPost = ThreadPost & {content: NonNullable<ThreadPost["content"]>};
export type ThreadAgent = {
  id: string; display_name: string; base_color_token?: string; effective_role?: string;
};
export type ThreadPage = {
  thread_id: string; board_id: string; project_id: string; root_post_id: string | null;
  items: ThreadPost[]; count: number; has_more: boolean; next_before: number | null;
  snapshot_cursor: string;
};

export type ThreadPaneProps = {
  /** Authorized project and board that own the thread. */
  projectId: string;
  boardId: string;
  /** Thread identity from any visible post (`item.thread_id`). */
  threadId: string;
  /** Post to scroll to and highlight; usually the post that opened the pane. */
  selectedPostId?: string | null;
  /** Roster used to resolve author and mention names. */
  agents?: ThreadAgent[];
  /** Optional custom author renderer, e.g. main.tsx's clickable agent button. */
  renderName?: (agentId: string) => React.ReactNode;
  /** Required: the pane never replaces the timeline, the owner closes it. */
  onClose: () => void;
  /** Called with the canonical post returned by the board posting endpoint. */
  onPosted?: (post: ThreadPost) => void;
  /** Optional mirror of pane errors into the owner's error surface. */
  onError?: (message: string) => void;
  /** Page walk bound for the "full thread" load; defaults to 10 pages of 200. */
  maxPages?: number;
  /** Shared live snapshot invalidation; avoids a second independent stream. */
  refreshVersion?: number;
};

export const THREAD_PAGE_LIMIT = 200;

/** One page of the whole authorized thread; walk with `next_before` until it is null. */
export async function fetchThreadPage(
  boardId: string, threadId: string, before: number | null, limit = THREAD_PAGE_LIMIT, cursor?: string | null,
): Promise<ThreadPage> {
  const params = new URLSearchParams({limit: String(limit)});
  if (before !== null) params.set("before", String(before));
  if (cursor) params.set("snapshot_cursor", cursor);
  return api<ThreadPage>(`boards/${boardId}/threads/${threadId}/posts?${params.toString()}`);
}

/**
 * Load every authorized post in a thread, not only the posts already present in
 * the main timeline. Returns posts in creation order (oldest first).
 */
export async function loadThread(
  boardId: string, threadId: string, maxPages = 10,
): Promise<ThreadPost[]> {
  for (let attempt = 0; attempt < 3; attempt++) {
    try { return await loadThreadPages(boardId, threadId, maxPages); }
    catch (error) {
      if (!(error instanceof ApiError) || error.code !== "snapshot_changed" || attempt === 2) throw error;
    }
  }
  throw new ApiError(409, "Thread changed while loading.", "snapshot_changed");
}

async function loadThreadPages(boardId: string, threadId: string, maxPages: number): Promise<ThreadPost[]> {
  let before: number | null = null;
  let items: ThreadPost[] = [];
  let pages = 0;
  let cursor: string | null = null;
  do {
    const page = await fetchThreadPage(boardId, threadId, before, THREAD_PAGE_LIMIT, cursor);
    const current = normalizeSnapshotCursor(page.snapshot_cursor);
    if (pages === 0) cursor = current;
    else if (cursor && current !== cursor) throw new ApiError(409, "Thread changed while loading.", "snapshot_changed");
    items = [...page.items, ...items];
    before = page.next_before;
    pages++;
  } while (before !== null && pages < maxPages);
  return [...new Map(items.map(item => [item.id, item])).values()];
}

/** Depth-first thread order; replies whose parent is not loaded fall back to the root level. */
function threadTree(items: ThreadPost[]): {post: ThreadPost; depth: number}[] {
  const present = new Set(items.map(item => item.id));
  const children = new Map<string, ThreadPost[]>();
  const roots: ThreadPost[] = [];
  for (const item of items) {
    const parent = item.parent_post_id && present.has(item.parent_post_id) ? item.parent_post_id : null;
    if (parent) children.set(parent, [...(children.get(parent) ?? []), item]);
    else roots.push(item);
  }
  const ordered: {post: ThreadPost; depth: number}[] = [];
  const seen = new Set<string>();
  const visit = (post: ThreadPost, depth: number) => {
    if (seen.has(post.id) || depth > 32) return;
    seen.add(post.id);
    ordered.push({post, depth});
    for (const child of children.get(post.id) ?? []) visit(child, depth + 1);
  };
  for (const root of roots) visit(root, 0);
  for (const item of items) if (!seen.has(item.id)) visit(item, 0);
  return ordered;
}

function excerpt(post: ThreadPost | null) {
  const text = post?.content?.text
    ?? post?.content?.segments?.map(segment => segment.kind === "text" ? segment.text : "@agent").join(" ")
    ?? "";
  return text.length > 72 ? `${text.slice(0, 72)}…` : text;
}

export function ThreadPane({
  projectId, boardId, threadId, selectedPostId, agents = [], renderName, onClose, onPosted,
  onError, maxPages = 10, refreshVersion,
}: ThreadPaneProps) {
  const [items, setItems] = useState<ThreadPost[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [draft, setDraft] = useState("");
  const [mentions, setMentions] = useState<MentionSelection[]>([]);
  const [replyTo, setReplyTo] = useState<ThreadPost | null>(null);
  const [snapshotVersion, setSnapshotVersion] = useState(0);
  const pane = useRef<HTMLElement>(null);
  const refreshRef = useRef<() => Promise<void>>(async () => {});
  const generation = useRef(0);
  const loadSequence = useRef(0);
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  useEffect(() => {
    const version = ++generation.current;
    let disposed = false;
    // A thread is project/board scoped: never keep the previous thread's text, target or errors.
    setItems([]); setError(""); setDraft(""); setMentions([]); setReplyTo(null);
    async function load(silent: boolean) {
      const request = ++loadSequence.current;
      if (!silent) setLoading(true);
      try {
        const posts = await loadThread(boardId, threadId, maxPages);
        if (disposed || version !== generation.current || request !== loadSequence.current) return;
        setItems(posts); setError("");
        setSnapshotVersion(current => current + 1);
        setReplyTo(current => posts.find(post => post.id === current?.id) ?? null);
      } catch (e) {
        if (disposed || version !== generation.current || request !== loadSequence.current) return;
        const message = String(e);
        setItems([]); setDraft(""); setMentions([]); setReplyTo(null);
        setSnapshotVersion(current => current + 1);
        setError(message); onErrorRef.current?.(message);
      } finally {
        if (!disposed && request === loadSequence.current && !silent) setLoading(false);
      }
    }
    refreshRef.current = () => load(false);
    void load(false);
    // Standalone hosts poll; the dashboard supplies its cursor-bound invalidations.
    const heartbeat = refreshVersion === undefined ? window.setInterval(() => void load(true), 15000) : undefined;
    return () => { disposed = true; generation.current++; clearInterval(heartbeat); };
  }, [projectId, boardId, threadId, maxPages]);

  useEffect(() => {
    if (refreshVersion !== undefined) void refreshRef.current();
  }, [refreshVersion]);

  useEffect(() => {
    pane.current?.focus();
  }, [threadId]);

  useEffect(() => {
    if (!selectedPostId) return;
    const node = pane.current?.querySelector(`[data-post-id="${CSS.escape(selectedPostId)}"]`);
    node?.scrollIntoView({block: "center"});
  }, [selectedPostId, items]);

  const ordered = useMemo(() => threadTree(items), [items]);
  const rootPost = useMemo(() => items.find(item => !item.parent_post_id) ?? null, [items]);
  const targetPost = replyTo ?? rootPost ?? ordered[0]?.post ?? null;
  const canonical = useMemo<CanonicalThreadPost[]>(() =>
    items.map(post => ({...post, content: post.content ?? {}})), [items]);
  const mentionGuard = useMentionSendGuard({
    value: draft, selections: mentions, contextKey: `${projectId}:${boardId}:${threadId}`,
  });
  useEffect(() => {
    if (mentionGuard.receiptPostId) mentionGuard.trackReceipts(mentionGuard.receiptPostId);
  }, [snapshotVersion]);
  const optimistic = useOptimisticPosts<CanonicalThreadPost>({
    project: projectId, board: boardId, sessionKey: threadId, canonical, snapshotVersion,
    onConfirmed: post => {
      mentionGuard.trackReceipts(post.id);
      onPosted?.(post); void refreshRef.current();
    },
    onError: failure => { setError(failure.message); onErrorRef.current?.(failure.message); },
  });

  function agentName(id: string) {
    return agents.find(agent => agent.id === id)?.display_name ?? id.slice(0, 18);
  }

  function nameNode(id: string) {
    if (renderName) return renderName(id);
    const token = agents.find(agent => agent.id === id)?.base_color_token ?? "";
    return <span className={`agent-name ${token}`}>{agentName(id)}</span>;
  }

  function authorNode(post: ThreadPost) {
    return post.author?.agent_id
      ? nameNode(post.author.agent_id)
      : <strong className="human">{post.author?.kind === "agent" ? "Agent" : "Human"}</strong>;
  }

  function reply(event: React.FormEvent) {
    event.preventDefault();
    if (loading || !targetPost || (!draft.trim() && !mentions.length)) return;
    if (!mentionGuard.trySend()) return;
    const segments: ThreadSegment[] = mentions.map(person => ({kind: "agent_mention", agent_id: person.id}));
    if (draft.trim()) segments.push({kind: "text", text: draft});
    if (optimistic.send({kind: "answer", parent_post_id: targetPost.id, thread_id: threadId, content: {segments}})) {
      setDraft(""); setMentions([]); setReplyTo(null); setError(""); mentionGuard.reset();
    }
  }

  return <section className="thread-pane" data-testid="thread-pane" aria-label="Thread discussion"
    tabIndex={-1} ref={pane} aria-busy={loading}
    onKeyDown={event => {
      if (event.key === "Escape" && !event.defaultPrevented) { event.stopPropagation(); onClose(); }
    }}>
    <header className="thread-pane-heading">
      <div>
        <p className="eyebrow">FOCUSED THREAD</p>
        <h2>{rootPost ? excerpt(rootPost) : "Thread discussion"}</h2>
        <p className="thread-pane-meta">{items.length} authorized {items.length === 1 ? "post" : "posts"}
          {rootPost && <> · started by {authorNode(rootPost)}</>}</p>
      </div>
      <div className="thread-pane-actions">
        <button type="button" disabled={loading} onClick={() => void refreshRef.current()}>Refresh thread</button>
        <button type="button" aria-label="Close thread pane" onClick={onClose}>Close</button>
      </div>
    </header>
    {error && <div className="error" role="alert">{error}</div>}
    <div className="thread-pane-body">
      {items.length >= maxPages * THREAD_PAGE_LIMIT && <p className="muted">
        Thread display is limited to {maxPages * THREAD_PAGE_LIMIT} posts. Older replies may not be shown.</p>}
      {loading && items.length === 0 && <p className="muted" role="status">Loading thread…</p>}
      {!loading && !error && items.length === 0 && <p className="muted">No posts in this thread are visible to you.</p>}
      {ordered.map(({post, depth}, index) => <article className="thread-post" key={post.id}
        data-testid="thread-post" data-post-id={post.id} data-depth={depth}
        data-selected={post.id === selectedPostId ? "true" : undefined}
        style={{marginLeft: Math.min(depth, 6) * 16}}>
        <div className="message-heading">
          <div>{authorNode(post)}
            {post.id === rootPost?.id && <span className="visibility">Thread root</span>}
            {post.kind && post.kind !== "message" && <span className="visibility">{post.kind}</span>}</div>
          <time dateTime={post.created_at}>
            {new Date(post.created_at).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"})}</time>
        </div>
        <div className="body">{post.content?.text ?? post.content?.segments?.map((segment, position) =>
          segment.kind === "text"
            ? <React.Fragment key={position}>{segment.text}</React.Fragment>
            : <React.Fragment key={position}>@{nameNode(segment.agent_id)} </React.Fragment>)}</div>
        <button type="button" className="thread-reply-target" data-testid="thread-reply-target"
          aria-label={`Reply to post ${index + 1}`} aria-pressed={replyTo?.id === post.id}
          onClick={() => setReplyTo(post)}>
          {replyTo?.id === post.id ? "Replying here" : "Reply"}</button>
      </article>)}
      {optimistic.outstanding.length > 0 && <section aria-label="Your pending thread replies">
        {optimistic.outstanding.map(row => <OptimisticPost key={optimistic.keyOf(row)} row={row}
          onRetry={optimistic.retry} onCancel={optimistic.cancel} renderMention={nameNode}/>)}
      </section>}
    </div>
    <form className="thread-pane-composer" aria-label="Thread reply composer" onSubmit={reply}>
      <div className="thread-reply-context">
        {targetPost
          ? <>Replying to {authorNode(targetPost)}
            {targetPost.id !== rootPost?.id && <button type="button" onClick={() => setReplyTo(null)}>Clear reply target</button>}</>
          : <>No visible post can receive a reply in this thread.</>}
      </div>
      <div className="chips">{mentions.map(person => <button type="button" key={person.id}
        onClick={() => setMentions(current => current.filter(item => item.id !== person.id))}>
        @{person.display_name} ×</button>)}</div>
      <div className="thread-pane-input">
        <MentionComposer value={draft}
          onChange={setDraft}
          onMention={person => {
            setMentions(current => current.some(item => item.id === person.id) ? current : [...current, person]);
          }}
          project={projectId} board={boardId} agents={agents}
          permissionKey={`${projectId}:${boardId}`} ariaLabel="Reply in thread"
          placeholder="Reply in this thread, or type @ to address an agent…"
          selections={mentions} showUnresolvedWarning={false}/>
      </div>
      <MentionSendGuard guard={mentionGuard}/>
      <div className="thread-pane-footer">
        <small>Replies post to the shared board with the same thread and idempotency semantics as the timeline.</small>
        <button className="primary" disabled={loading || !targetPost || (!draft.trim() && !mentions.length)}>Post reply</button>
      </div>
    </form>
  </section>;
}
