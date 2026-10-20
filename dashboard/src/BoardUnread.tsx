import React, {useCallback, useEffect, useMemo, useRef, useState} from "react";
import {
  boardBadgeLabel, boardBadgeTitle, boardUnreadIds, decorateBoardItems, fetchBoardUnread,
  fetchBoardUnreadForBoard, markBoardPostsRead, markProjectBoardPostsRead, selfPolicyNote,
  type BoardUnreadItem, type BoardUnreadMarkResult, type BoardUnreadState,
} from "./board-unread";
import "./board-unread.css";

/**
 * React surface for shared-board unread state. All wire contracts and pure
 * helpers live in `./board-unread`; this module owns only the hook and the
 * three small components, and re-exports the pure API so a host has one import.
 *
 * The rule the components enforce: rendering never marks anything read. Only the
 * explicit `BoardUnreadToolbar` mark control and `BoardPostReadControl` write,
 * and both batch to the server's 100-post cap with the current snapshot cursor.
 */
export * from "./board-unread";

export type UseBoardUnreadOptions = {
  projectId: string;
  /** Pin the counts to one board; omit for project-wide badges. */
  boardId?: string | null;
  includeOwn?: boolean;
  markerLimit?: number;
  /** Shared live-state snapshot counter; a change refetches counts. */
  refreshVersion?: number;
  /** Standalone hosts poll; the dashboard passes `refreshVersion` instead. */
  autoRefreshMs?: number;
  onError?: (message: string) => void;
  permissionKey?: string;
  snapshotCursor?: string | null;
};

export type UseBoardUnreadResult = {
  state: BoardUnreadState | null;
  loading: boolean;
  marking: boolean;
  error: string;
  unreadIds: Set<string>;
  /** Decorate loaded timeline items with `is_read`/`board_unread`. */
  decorate: <T extends BoardUnreadItem>(items: T[]) => (T & {is_read?: boolean; board_unread?: boolean})[];
  refresh: (silent?: boolean) => Promise<void>;
  /** Mark explicit ids read (batched, snapshot-fenced, retried once on drift). */
  markIds: (postIds: string[], options?: {ignoreUnavailable?: boolean; postRevisions?: Record<string, number>}) => Promise<BoardUnreadMarkResult | null>;
  /** Mark every currently unread post among the given loaded items. */
  markVisible: (items: BoardUnreadItem[], options?: {ignoreUnavailable?: boolean}) => Promise<BoardUnreadMarkResult | null>;
  /** The post ids among the given items that this principal has not read. */
  unreadAmong: (items: BoardUnreadItem[]) => string[];
};

export function useBoardUnread(options: UseBoardUnreadOptions): UseBoardUnreadResult {
  const {projectId, boardId = null, includeOwn = false, markerLimit, refreshVersion,
    autoRefreshMs, onError, permissionKey = "", snapshotCursor} = options;
  const scope = JSON.stringify([projectId, boardId, includeOwn, markerLimit, permissionKey]);
  const activeScope = useRef(scope);
  activeScope.current = scope;
  const stateScope = useRef(scope);
  const markedRevisions = useRef(new Map<string, number>());
  const [state, setState] = useState<BoardUnreadState | null>(null);
  const [loading, setLoading] = useState(true);
  const [marking, setMarking] = useState(false);
  const [error, setError] = useState("");
  const generation = useRef(0);
  const ticket = useRef(0);
  const pending = useRef(0);
  const stateRef = useRef<BoardUnreadState | null>(null);
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;
  stateRef.current = state;

  const load = useCallback(async (silent: boolean, fenced = true) => {
    if (!projectId) { stateRef.current = null; setState(null); setLoading(false); return; }
    const version = generation.current;
    const request = ++ticket.current;
    if (!silent) { pending.current++; setLoading(true); }
    try {
      const next = boardId
        ? await fetchBoardUnreadForBoard(boardId, {includeOwn, markerLimit, cursor: fenced ? snapshotCursor : undefined})
        : await fetchBoardUnread(projectId, {includeOwn, markerLimit, cursor: fenced ? snapshotCursor : undefined});
      if (version !== generation.current || request !== ticket.current || scope !== activeScope.current) return;
      stateRef.current = next;
      for (const id of next.unread_post_ids) markedRevisions.current.delete(id);
      setState(next); setError("");
    } catch (e) {
      if (version !== generation.current || request !== ticket.current || scope !== activeScope.current) return;
      // 401/403 already clear privileged UI through the shared authorization handler.
      setState(null);
      stateRef.current = null;
      const message = String(e);
      setError(message); onErrorRef.current?.(message);
    } finally {
      if (!silent) {
        pending.current = Math.max(0, pending.current - 1);
        if (version === generation.current) setLoading(pending.current > 0);
      }
    }
  }, [projectId, boardId, includeOwn, markerLimit, scope, snapshotCursor]);

  useEffect(() => {
    // Counts are project/board scoped: never keep the previous scope's numbers.
    generation.current++;
    stateScope.current = scope;
    stateRef.current = null; markedRevisions.current.clear();
    setState(null); setError(""); setLoading(true);
    return () => { generation.current++; };
  }, [scope]);

  useEffect(() => {
    void load(false);
  }, [refreshVersion, load]);

  useEffect(() => {
    if (autoRefreshMs === undefined) return;
    const timer = window.setInterval(() => void load(true), autoRefreshMs);
    return () => window.clearInterval(timer);
  }, [autoRefreshMs, load]);

  const unreadIds = useMemo(() => boardUnreadIds(state), [state]);
  const decorate = useCallback(
    <T extends BoardUnreadItem>(items: T[]) => decorateBoardItems(items.map(item =>
      item.revision !== undefined && markedRevisions.current.get(item.id) === item.revision
        ? {...item, board_unread: false} : item), stateRef.current), []);

  const unreadAmong = useCallback((items: BoardUnreadItem[]) => {
    const current = stateRef.current;
    if (!current?.available) return [];
    return decorate(items).filter(item => item.board_unread).map(item => item.id);
  }, [decorate]);

  const markIds = useCallback(async (
    postIds: string[], markOptions: {ignoreUnavailable?: boolean; postRevisions?: Record<string, number>} = {},
  ): Promise<BoardUnreadMarkResult | null> => {
    const ids = [...new Set(postIds.filter(Boolean))];
    if (!ids.length) return null;
    const version = generation.current;
    setMarking(true);
    try {
      const options = {
        ...markOptions,
        beforeBatch: () => version === generation.current && scope === activeScope.current,
        cursor: stateRef.current?.snapshot_cursor,
        // A refused stale snapshot refetches the state and rebinds the cursor
        // once per batch, so a long marking run cannot fail on its own writes.
        refreshCursor: async () => {
          await load(true, false);
          return stateRef.current?.snapshot_cursor ?? null;
        },
      };
      const result = boardId
        ? await markBoardPostsRead(boardId, ids, options)
        : await markProjectBoardPostsRead(projectId, ids, options);
      if (version !== generation.current || scope !== activeScope.current) return null;
      // The response carries the fresh, exact state: no extra round trip needed.
      ticket.current++;
      for (const mark of result.marked) markedRevisions.current.set(mark.post_id, mark.revision);
      stateRef.current = result.state;
      setState(result.state); setError("");
      return result;
    } catch (e) {
      if (version === generation.current) {
        const message = String(e);
        setError(message); onErrorRef.current?.(message);
      }
      throw e;
    } finally {
      if (version === generation.current) setMarking(false);
    }
  }, [boardId, projectId, load, scope]);

  const markVisible = useCallback(async (
    items: BoardUnreadItem[], markOptions: {ignoreUnavailable?: boolean} = {},
  ) => markIds(unreadAmong(items), {...markOptions,
    ...(items.every(item => Number.isInteger(item.revision)) ? {
      postRevisions: Object.fromEntries(items.map(item => [item.id, item.revision!])),
    } : {}),
  }), [markIds, unreadAmong]);

  const currentScope = stateScope.current === scope;
  return {state: currentScope ? state : null, loading, marking, error: currentScope ? error : "",
    unreadIds: currentScope ? unreadIds : new Set(), decorate,
    refresh: (silent = true) => load(silent), markIds, markVisible, unreadAmong};
}

export type BoardUnreadBadgeProps = {
  count: number;
  truncated?: boolean;
  available?: boolean;
  boardTitle?: string | null;
};

/** A single board badge. Renders nothing for a zero count or an unknown state. */
export function BoardUnreadBadge({count, truncated = false, available = true, boardTitle}: BoardUnreadBadgeProps) {
  if (!available || (count <= 0 && !truncated)) return null;
  const title = boardBadgeTitle(count, truncated, available, boardTitle);
  return <span className={`board-unread-badge${truncated ? " truncated" : ""}`}
    data-testid="board-unread-badge" data-count={count} data-truncated={truncated ? "true" : "false"}
    title={title} aria-label={title}>{boardBadgeLabel(count, truncated)}</span>;
}

export type BoardUnreadToolbarProps = {
  projectId: string;
  boardId?: string | null;
  /** Currently loaded, visible timeline items; marking is explicit and batched. */
  items?: BoardUnreadItem[];
  includeOwn?: boolean;
  refreshVersion?: number;
  onError?: (message: string) => void;
  onMarked?: (result: BoardUnreadMarkResult) => void;
  /** Optional custom label; the control is always an explicit user action. */
  markLabel?: string;
  /** Reuse a host's hook instance instead of opening a second request scope. */
  controller?: UseBoardUnreadResult;
};

/**
 * Explicit unread toolbar: honest counts plus a batched "mark displayed posts
 * read" control. Rendering the toolbar never marks anything read.
 *
 * Pass `controller` to reuse a host's `useBoardUnread` instance (one request
 * scope per project/board); omit it and the toolbar owns its own instance.
 */
export function BoardUnreadToolbar(props: BoardUnreadToolbarProps) {
  if (props.controller) return <BoardUnreadToolbarView {...props} controller={props.controller}/>;
  return <BoundBoardUnreadToolbar {...props}/>;
}

function BoundBoardUnreadToolbar({
  projectId, boardId = null, includeOwn = false, refreshVersion, onError, ...rest
}: BoardUnreadToolbarProps) {
  const controller = useBoardUnread({projectId, boardId, includeOwn, refreshVersion, onError});
  return <BoardUnreadToolbarView {...rest} projectId={projectId} boardId={boardId}
    includeOwn={includeOwn} refreshVersion={refreshVersion} onError={onError}
    controller={controller}/>;
}

function BoardUnreadToolbarView({
  items = [], onMarked, markLabel = "Mark displayed posts read", controller,
}: BoardUnreadToolbarProps & {controller: UseBoardUnreadResult}) {
  const board = controller;
  const unread = board.unreadAmong(items);
  const state = board.state;
  const available = state?.available !== false;
  const truncated = Boolean(state?.truncated) || Boolean(state && !state.counts_complete);

  async function mark() {
    try {
      const result = await board.markVisible(items);
      if (result) onMarked?.(result);
    } catch {
      // The hook already surfaced the error through its own state.
    }
  }

  return <div className="board-unread-toolbar" data-testid="board-unread-toolbar">
    <span className="board-unread-count" data-testid="board-unread-count" role="status"
      data-complete={state ? String(Boolean(state.counts_complete && state.available)) : "unknown"}>
      {!state ? board.loading ? "Checking unread…" : "Unread state unavailable" : !available ? "Unread state unavailable"
        : `${boardBadgeLabel(state.unread_count, truncated) || "0"} unread`}
    </span>
    {Boolean(state?.available) && truncated && <span className="board-unread-note"
      data-testid="board-unread-truncated">
      Count is a lower bound; more posts exist beyond the scanned window.</span>}
    {Boolean(state?.available) && !truncated && state?.unread_markers_truncated &&
      <span className="board-unread-note" data-testid="board-unread-markers-truncated">
        Only the first {state.unread_post_ids.length} unread posts are listed.</span>}
    {Boolean(state?.available) && <span className="board-unread-note" data-testid="board-unread-self-policy">
      {selfPolicyNote(state)}</span>}
    <button type="button" disabled={board.marking || board.loading || !unread.length}
      title="Marks the loaded posts shown in this view; it never marks unseen posts"
      data-testid="board-unread-mark" onClick={() => void mark()}>
      {board.marking ? "Marking…" : `${markLabel}${unread.length ? ` (${unread.length})` : ""}`}</button>
    <button type="button" disabled={board.loading} data-testid="board-unread-refresh"
      onClick={() => void board.refresh(false)}>Refresh unread</button>
    {board.error && <span className="board-unread-error" role="alert">{board.error}</span>}
  </div>;
}

export type BoardPostReadControlProps = {
  boardId: string;
  postId: string;
  revision?: number;
  unread: boolean;
  disabled?: boolean;
  onMarked?: (postId: string, result: BoardUnreadMarkResult) => void;
  onError?: (message: string) => void;
  /**
   * Reuse the host's hook so the marker updates from the server response in the
   * same render pass. Without it the component marks the post directly and the
   * host must refresh its own unread state.
   */
  controller?: UseBoardUnreadResult;
};

/** Explicit per-post marker control; it is the only thing that marks one post. */
export function BoardPostReadControl({
  boardId, postId, revision, unread, disabled, onMarked, onError, controller,
}: BoardPostReadControlProps) {
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  if (!unread) return <span className="board-post-read-control read" data-testid="board-post-read"
    data-post-id={postId} data-unread="false">Read</span>;
  return <button type="button" className="board-post-read-control unread" data-testid="board-post-read"
    data-post-id={postId} data-unread="true" disabled={disabled || busy}
    aria-label="Mark this post read"
    onClick={async () => {
      setBusy(true); setFailed(false);
      try {
        const result = controller
          ? await controller.markIds([postId], {postRevisions: revision === undefined ? undefined : {[postId]: revision}})
          : await markBoardPostsRead(boardId, [postId], {postRevisions: revision === undefined ? undefined : {[postId]: revision}});
        if (result) onMarked?.(postId, result);
      } catch (e) {
        setFailed(true); onError?.(String(e));
      } finally { setBusy(false); }
    }}>{busy ? "Marking…" : failed ? "Retry mark read" : "Unread · mark read"}</button>;
}
