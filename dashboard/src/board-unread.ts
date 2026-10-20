import {api, ApiError} from "./api";
import {normalizeSnapshotCursor} from "./live-state";

/**
 * Shared-board unread state: pure contracts for the dashboard.
 *
 * Ground truth (`hivemind/board_reads.py`):
 *
 * - `GET /v1/boards/{board_id}/unread` and
 *   `GET /v1/projects/{project_id}/board-unread` return per-principal,
 *   revision-aware unread counts plus `unread_post_ids`. They are read-only:
 *   *viewing never marks anything read*.
 * - `POST /v1/boards/{board_id}/unread:read` and
 *   `POST /v1/projects/{project_id}/board-unread:read` are the only writers.
 *   One request marks at most 100 posts (`BATCH_LIMIT`); every id is
 *   re-authorized from the canonical store at write time.
 * - Markers are per principal and revision-bound: an edit makes a post unread
 *   again. A post authored by the current principal is never counted as unread
 *   (`self_policy: own_posts_never_unread`); pass `includeOwn` to count it.
 * - Counts can be truncated. When `counts_complete` is false the UI must say so
 *   and must never render a fabricated exact number.
 * - Board reads are independent of agent delivery acknowledgements and of
 *   `conversation_reads`; nothing here touches either.
 *
 * Integration contract (the dashboard owner wires this in `main.tsx`):
 *
 * 1. Timeline items gain markers by decoration, not by a shared route change:
 *
 *        const board = useBoardUnread({projectId: project, boardId: board});
 *        const decorated = useMemo(() => board.decorate(timelineItems), [timelineItems, board.decorate]);
 *
 *    `decorate` maps `[{id}] -> [{id, is_read, board_unread}]` and never invents
 *    an unread marker while the state is unknown (`available: false`).
 *
 * 2. Board badges come from the atomic snapshot: the server may add a
 *    `board_unread` key to `dashboard-snapshot` whose value is exactly the
 *    `GET /v1/projects/{id}/board-unread` body. Merge it with
 *    `mergeSnapshotBoardUnread(snapshot.boards, snapshot.board_unread)` to get
 *    `Board[] & {unread_count, unread_truncated}` for the picker and tabs.
 *
 * 3. Pass the shared live-state `refreshVersion` so a `resync` refetches counts;
 *    the hook never opens a second stream.
 *
 * 4. The React surface lives in `BoardUnread.tsx` and re-exports this module, so
 *    a host can import everything from `./BoardUnread`.
 */

/** Server cap for one explicit marking request. */
export const BATCH_LIMIT = 100;
/** Default client cap for the id list rendered from a response. */
export const MARKER_LIMIT = 200;
/** Stable, explicit author rule reported by the server. */
export const SELF_POLICY = "own_posts_never_unread";
/** Server error code for a stale principal/project snapshot. */
export const SNAPSHOT_CHANGED = "snapshot_changed";

export type BoardUnreadBadgeState = {
  board_id: string; title?: string | null;
  unread_count: number; visible_count: number; counts_complete: boolean;
};

export type BoardUnreadState = {
  project_id: string;
  board_id: string | null;
  board_title?: string | null;
  /** false when the backend module is not installed: never render a false zero. */
  available: boolean;
  revision_aware: boolean;
  self_policy: string;
  include_own: boolean;
  unread_count: number;
  unread_post_ids: string[];
  /** true when more unread ids exist than `unread_post_ids` returns. */
  unread_markers_truncated: boolean;
  visible_count: number;
  read_count: number;
  authored_count: number;
  own_excluded_count: number;
  counts_complete: boolean;
  truncated: boolean;
  examined_count: number;
  scan_limit: number;
  marker_limit: number;
  read_digest: string | null;
  generated_at: number;
  boards?: BoardUnreadBadgeState[];
  snapshot_cursor?: string | null;
};

export type BoardUnreadMark = {post_id: string; revision: number};
export type BoardUnreadSkip = {post_id: string; reason: string};
export type BoardUnreadMarkResult = {
  marked_count: number;
  already_read_count: number;
  marked: BoardUnreadMark[];
  skipped: BoardUnreadSkip[];
  project_id: string;
  board_id: string | null;
  state: BoardUnreadState;
  snapshot_cursor: string;
};

export type BoardUnreadItem = {
  id: string; revision?: number; created_seq?: number;
  is_read?: boolean; board_unread?: boolean;
  [key: string]: unknown;
};

export type BoardUnreadFetchOptions = {
  boardId?: string | null;
  includeOwn?: boolean;
  markerLimit?: number;
  cursor?: string | null;
};

export type BoardUnreadMarkOptions = {
  cursor?: string | null;
  ignoreUnavailable?: boolean;
  /**
   * Called at most once per batch when the server refuses a stale snapshot
   * (409 `snapshot_changed`). It must return the principal's current cursor, or
   * null when the state cannot be refreshed, in which case the error surfaces.
   */
  refreshCursor?: () => Promise<string | null>;
  postRevisions?: Record<string, number>;
  beforeBatch?: () => boolean;
};

/** Query string for a read-only unread request; only ever binds a valid cursor. */
export function boardUnreadQuery(options: BoardUnreadFetchOptions = {}): string {
  const params = new URLSearchParams();
  if (options.boardId) params.set("board_id", options.boardId);
  if (options.includeOwn) params.set("include_own", "true");
  if (options.markerLimit !== undefined) params.set("marker_limit", String(options.markerLimit));
  const cursor = normalizeSnapshotCursor(options.cursor);
  if (cursor) params.set("snapshot_cursor", cursor);
  const search = params.toString();
  return search ? `?${search}` : "";
}

/** Per-board unread counts and unread post ids for the current principal. */
export async function fetchBoardUnread(
  projectId: string, options: BoardUnreadFetchOptions = {},
): Promise<BoardUnreadState> {
  return api<BoardUnreadState>(`projects/${projectId}/board-unread${boardUnreadQuery(options)}`);
}

/** Unread state for exactly one board; the board must belong to the project. */
export async function fetchBoardUnreadForBoard(
  boardId: string, options: Omit<BoardUnreadFetchOptions, "boardId"> = {},
): Promise<BoardUnreadState> {
  return api<BoardUnreadState>(`boards/${boardId}/unread${boardUnreadQuery(options)}`);
}

/**
 * Explicitly mark posts read on one board. Chunks to the server's 100-post cap,
 * binds the newest cursor, and rebinds it after every batch so a long marking
 * run cannot fail on its own marker writes.
 */
export async function markBoardPostsRead(
  boardId: string, postIds: string[], options: BoardUnreadMarkOptions = {},
): Promise<BoardUnreadMarkResult> {
  return markInBatches(`boards/${boardId}/unread:read`, postIds, options);
}

/** Explicitly mark posts read across the project (each post is re-authorized). */
export async function markProjectBoardPostsRead(
  projectId: string, postIds: string[], options: BoardUnreadMarkOptions = {},
): Promise<BoardUnreadMarkResult> {
  return markInBatches(`projects/${projectId}/board-unread:read`, postIds, options);
}

async function markInBatches(
  path: string, postIds: string[], options: BoardUnreadMarkOptions,
): Promise<BoardUnreadMarkResult> {
  const batches = markingBatches(postIds);
  if (!batches.length) throw new ApiError(422, "No posts to mark read.", "board_read_batch_empty");
  let cursor = normalizeSnapshotCursor(options.cursor);
  let merged: BoardUnreadMarkResult | null = null;
  for (const batch of batches) {
    let result: BoardUnreadMarkResult | null = null;
    for (let attempt = 0; attempt < 2 && !result; attempt++) {
      if (options.beforeBatch && !options.beforeBatch()) {
        throw new ApiError(409, "Read scope changed; remaining posts were not marked.", "context_changed");
      }
      const body: Record<string, unknown> = {post_ids: batch};
      if (options.postRevisions) body.post_revisions = Object.fromEntries(
        batch.map(id => [id, options.postRevisions![id]]));
      if (cursor) body.snapshot_cursor = cursor;
      if (options.ignoreUnavailable) body.ignore_unavailable = true;
      try {
        result = await api<BoardUnreadMarkResult>(path, "POST", body);
      } catch (error) {
        const drifted = error instanceof ApiError && error.code === SNAPSHOT_CHANGED;
        if (!drifted || attempt === 1 || !options.refreshCursor) throw error;
        cursor = normalizeSnapshotCursor(await options.refreshCursor());
        if (!cursor) throw error;
      }
    }
    if (!result) throw new ApiError(409, "Snapshot changed while marking.", SNAPSHOT_CHANGED);
    // Rebind to the server's fresh cursor so the next batch is not refused by
    // the markers this batch just wrote.
    cursor = normalizeSnapshotCursor(result.snapshot_cursor) ?? cursor;
    merged = merged ? {
      ...result,
      marked_count: merged.marked_count + result.marked_count,
      already_read_count: merged.already_read_count + result.already_read_count,
      marked: [...merged.marked, ...result.marked],
      skipped: [...merged.skipped, ...result.skipped],
    } : result;
  }
  return merged!;
}

/** Split ids into server-sized marking batches (max 100, order preserved). */
export function markingBatches(postIds: string[], limit = BATCH_LIMIT): string[][] {
  const unique = [...new Set(postIds.filter(Boolean))];
  const batches: string[][] = [];
  for (let index = 0; index < unique.length; index += limit) {
    batches.push(unique.slice(index, index + limit));
  }
  return batches;
}

/**
 * The unread ids of a state as a lookup set. An unknown or unavailable state is
 * an empty set: a missing backend must never mark a post as unread.
 */
export function boardUnreadIds(state: BoardUnreadState | null | undefined): Set<string> {
  if (!state || !state.available) return new Set();
  return new Set(state.unread_post_ids ?? []);
}

/**
 * Join the unread id set onto timeline items: `is_read` is true only when the
 * principal's markers say so, and `board_unread` is false while the state is
 * unknown, so an unavailable backend can never paint every post as unread.
 */
export function decorateBoardItems<T extends BoardUnreadItem>(
  items: T[], unread: BoardUnreadState | Set<string> | null | undefined,
): (T & {is_read?: boolean; board_unread?: boolean})[] {
  const known = unread instanceof Set || Boolean(unread?.available && !unread.unread_markers_truncated);
  const ids = unread instanceof Set ? unread : boardUnreadIds(unread);
  return items.map(item => {
    if (item.optimistic === true) return {...item, is_read: undefined, board_unread: undefined};
    const isUnread = typeof item.board_unread === "boolean" ? item.board_unread
      : ids.has(item.id) ? true : known ? false : undefined;
    return {...item, is_read: isUnread === undefined ? undefined : !isUnread, board_unread: isUnread};
  });
}

/** The ids among `items` this principal has not read, in display order. */
export function unreadAmongItems(
  items: BoardUnreadItem[], state: BoardUnreadState | null | undefined,
): string[] {
  if (!state?.available) return [];
  const ids = boardUnreadIds(state);
  return items.map(item => item.id).filter(id => ids.has(id));
}

/** Honest badge text: never an exact number for a truncated count. */
export function boardBadgeLabel(count: number, truncated = false): string {
  if (count <= 0 && !truncated) return "";
  return truncated ? `${count}+` : String(count);
}

export function boardBadgeTitle(
  count: number, truncated = false, available = true, boardTitle?: string | null,
): string {
  if (!available) return "Unread state is unavailable for this deployment.";
  if (count <= 0 && !truncated) return boardTitle ? `No unread posts in ${boardTitle}.` : "No unread posts.";
  const scope = boardTitle ? ` in ${boardTitle}` : "";
  const base = `${count}${truncated ? "+" : ""} unread post${count === 1 ? "" : "s"}${scope}`;
  return truncated ? `${base}. More posts exist beyond the scanned window.` : `${base}.`;
}

/** A board row with its unread badge, as merged from an atomic snapshot. */
export type BoardWithUnread = {id: string; title?: string;
  unread_count?: number; unread_truncated?: boolean};

/**
 * Merge the optional `board_unread` key of `dashboard-snapshot` into `boards`.
 * Boards without a badge entry keep `unread_count: undefined`, which renders as
 * "unknown" rather than "zero", and an unavailable module never fabricates one.
 */
export function mergeSnapshotBoardUnread<T extends {id: string; title?: string}>(
  boards: T[], summary: BoardUnreadState | null | undefined,
): (T & {unread_count?: number; unread_truncated?: boolean})[] {
  const badges = new Map((summary?.boards ?? []).map(item => [item.board_id, item]));
  return boards.map(board => {
    const badge = badges.get(board.id);
    if (!badge || !summary?.available) {
      return {...board, unread_count: undefined, unread_truncated: false};
    }
    return {...board, unread_count: badge.unread_count, unread_truncated: !badge.counts_complete};
  });
}

/** One-line, honest explanation of the explicit author rule. */
export function selfPolicyNote(state: BoardUnreadState | null | undefined): string {
  if (!state || !state.available) return "";
  if (state.self_policy !== SELF_POLICY) return "";
  return state.own_excluded_count
    ? `${state.own_excluded_count} of your own ${state.own_excluded_count === 1 ? "post is" : "posts are"} never counted as unread.`
    : "Your own posts are never counted as unread.";
}
