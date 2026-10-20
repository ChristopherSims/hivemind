/**
 * Optimistic posting to the shared board.
 *
 * The canonical write is `POST boards/{id}/posts`. It is idempotent on
 * `client_post_id`: the body key is also sent as the `Idempotency-Key` header
 * (through the shared `api` helper), and the server records the command under
 * `post:{board_id}` with that key. A retry therefore can never create a second
 * post, which is what makes an ambiguous failure safe to retry.
 *
 * Contract:
 * - `send(draft)` freezes the payload, allocates one `client_post_id`, and adds
 *   a synthetic pending row to `items` immediately. The row is a structural
 *   `PostItem` (mirrors `Item` in main.tsx) with `optimistic: true`, so it can
 *   sit in the same list without changing the renderer's item shape.
 * - Confirmation may arrive from the HTTP response or from a later
 *   canonical/SSE snapshot. Both paths converge on one canonical id; the
 *   synthetic row is replaced by the canonical row and duplicates are
 *   suppressed. No automatic resend is ever performed.
 * - `keyOf` returns the *same* key for a pending row and for the canonical row
 *   that replaces it, so `VirtualTimeline` keeps the row's height cache and DOM
 *   node across the transition.
 * - A failed row keeps its frozen payload; `retry` re-sends that exact payload
 *   with the original `client_post_id`, and `cancel` drops the local row only.
 * - Nothing is persisted: no `localStorage`, `sessionStorage`, history or URL
 *   state. A reload loses pending rows (the canonical post, if committed, comes
 *   back from the next snapshot).
 * - Optimistic rows never carry `from`, `to`, `is_read`, `delivery_status` or
 *   any `delivery_*` field, so they can never be counted as unread and never
 *   offer canonical reply/read actions. `outstanding` exists only for local
 *   "sending/unconfirmed" affordances.
 *
 * Ordering / tie strategy (explicit):
 * - Canonical rows keep the order the caller supplied.
 * - `mergeOptimistic` appends optimistic rows after every canonical row. A
 *   synthetic row's `created_seq` is `max(canonical.created_seq) + record.order`,
 *   where `record.order` is a monotonic per-hook send counter (1, 2, 3, ...).
 *   Two rows sent in the same millisecond therefore always have distinct
 *   `created_seq` values, ordered by send. If a later canonical row happens to
 *   reuse a synthetic `created_seq` value, the canonical row still sorts first
 *   because canonical rows are emitted before synthetic rows.
 *
 * Context fencing:
 * - `project`, `board` and `sessionKey` form the context key. Any change drops
 *   all optimistic state, and a response or snapshot from an older context is
 *   ignored (it can neither commit nor wipe the draft that belongs to the new
 *   context). Merge/key lookups additionally filter by the active context key,
 *   so a row from another board can never be rendered.
 */

import {useCallback, useEffect, useMemo, useRef, useState} from "react";
import {api, ApiError} from "./api";

/** Structural mirror of `Segment` in dashboard/src/main.tsx. */
export type PostSegment = {kind: "text"; text: string} | {kind: "agent_mention"; agent_id: string};

/** Structural mirror of `Item` in dashboard/src/main.tsx. */
export type PostItem = {
  id: string; revision: number; created_at: string; created_seq: number;
  content: {text?: string; segments?: PostSegment[]};
  author?: {kind: string; agent_id?: string}; from?: string; to?: string;
  thread_id: string; state?: string; visibility?: string;
  delivery_status?: string; attempt_count?: number; total_attempts?: number;
  next_attempt_at?: number | null; claim_until?: number; is_read?: boolean;
  delivery_history?: {action: string; at: string; run_id?: string; reason?: string}[];
  /**
   * Present on canonical board posts (the server echoes the request body). Used
   * to tie a server row back to the send that produced it. Optional so a server
   * that omits it still type-checks; matching then falls back to `canonicalId`.
   */
  client_post_id?: string;
};

/** Media type main.tsx already uses for rich board posts. */
export const OPTIMISTIC_MEDIA_TYPE = "application/vnd.hivemind.chat+json";
/** Prefix of a synthetic row id: `optimistic:{client_post_id}`. */
export const OPTIMISTIC_ROW_PREFIX = "optimistic:";
/** Default watchdog after which a silent request is reported as unconfirmed. */
export const DEFAULT_OPTIMISTIC_TIMEOUT_MS = 15_000;

export type OptimisticPhase = "pending" | "failed";
export type ConfirmationSource = "http" | "stream";

/**
 * Failure of a send. `ambiguous` means the outcome is unknown: a network error,
 * an abort or a silent timeout may have committed the post. The payload is
 * never discarded for an ambiguous failure; retry is idempotent.
 */
export type OptimisticError = {
  message: string;
  /** HTTP status when the failure was an HTTP response. */
  status?: number;
  code?: string;
  /** True when the post may exist on the server and the retry must reuse the key. */
  ambiguous: boolean;
};

/** Frozen outgoing body. Never mutated after `send`. */
export type PostPayload = {
  client_post_id: string;
  kind: string;
  content: {media_type: string; text?: string; segments?: PostSegment[]};
  thread_id?: string;
  parent_post_id?: string;
};

export type OptimisticDraft = {
  kind?: string;
  content: {media_type?: string; text?: string; segments?: PostSegment[]};
  thread_id?: string;
  parent_post_id?: string;
};

/**
 * A locally-held row that satisfies `PostItem` so it renders through the same
 * timeline. Canonical-only fields (`from`, `is_read`, `delivery_*`) are never
 * set, which keeps it out of unread counts and canonical reply/read controls.
 */
export type OptimisticPost = PostItem & {
  optimistic: true;
  client_post_id: string;
  phase: OptimisticPhase;
  error?: OptimisticError;
};

/** Locally-held bookkeeping for one send. Exported for harness/fixtures. */
export type OptimisticRecord<T extends PostItem = PostItem> = {
  client_post_id: string;
  contextKey: string;
  project: string;
  board: string;
  /** Monotonic send counter; breaks synthetic `created_seq` ties. */
  order: number;
  phase: OptimisticPhase | "confirmed";
  payload: Readonly<PostPayload>;
  created_at: string;
  attempt: number;
  error?: OptimisticError;
  canonicalId?: string;
  canonicalPost?: T;
  confirmSource?: ConfirmationSource;
  snapshotVersion?: number;
};

export type ConfirmationInfo = {client_post_id: string; source: ConfirmationSource};
export type FailureInfo = {client_post_id: string};

export type OptimisticPostsOptions<T extends PostItem> = {
  project: string;
  board: string;
  /** Session/access fence (for example `${role}:${accessEpoch}`). */
  sessionKey?: string;
  /** Canonical items currently rendered; reconciled so duplicates never appear. */
  canonical?: readonly T[];
  /** Fired once per `client_post_id` when the post is known to be canonical. */
  onConfirmed?: (post: T, info: ConfirmationInfo) => void;
  /** Fired when a send fails or goes unconfirmed. Never fired for stale contexts. */
  onError?: (error: OptimisticError, info: FailureInfo) => void;
  /** Watchdog for a silent request; `0` disables it. Defaults to 15s. */
  timeoutMs?: number;
  /** Completed authoritative read generation; confirmed payloads expire at the next snapshot. */
  snapshotVersion?: number;
};

export type OptimisticPosts<T extends PostItem> = {
  /** Canonical items plus optimistic rows, deduplicated. Pass to `VirtualTimeline`. */
  items: Array<T | OptimisticPost>;
  /** Stable key for a row from `items`; identical across pending -> canonical. */
  keyOf: (item: T | OptimisticPost) => string;
  /** Type guard for rows that are still local (pending or failed). */
  isOptimistic: (item: T | OptimisticPost) => item is OptimisticPost;
  /** Send a new post. Returns the frozen `client_post_id`, or null with no board. */
  send: (draft: OptimisticDraft) => string | null;
  /** Re-send a failed row with its frozen payload and original `client_post_id`. */
  retry: (client_post_id: string) => void;
  /** Drop a pending/failed row locally. Never deletes a committed server post. */
  cancel: (client_post_id: string) => void;
  /** Pending + failed rows for the active context. Local affordance only. */
  outstanding: OptimisticPost[];
  /** In-flight sends (not failed, not confirmed). */
  pendingCount: number;
  /** Failed sends awaiting an explicit retry or cancel. */
  failedCount: number;
};

/** Reads the canonical `client_post_id` a server row echoes, if any. */
export function clientPostIdOf(item: PostItem): string | null {
  const value = (item as {client_post_id?: unknown}).client_post_id;
  return typeof value === "string" && value.length > 0 ? value : null;
}

/** True for a locally-held optimistic row. */
export function isOptimisticPost<T extends PostItem>(item: T | OptimisticPost): item is OptimisticPost {
  return (item as {optimistic?: unknown}).optimistic === true;
}

/**
 * Classifies a send failure. HTTP 4xx is definitive (the command did not
 * commit); a network error, abort, silent timeout, or 5xx is ambiguous because
 * the command may have committed before the response was lost. The idempotent
 * `client_post_id` makes an ambiguous retry safe either way.
 */
export function classifyOptimisticError(error: unknown): OptimisticError {
  if (error instanceof ApiError) {
    return {message: error.message, status: error.status, code: error.code,
      ambiguous: error.status >= 500};
  }
  const message = error instanceof Error ? error.message : String(error);
  return {message, ambiguous: true, code: "network_error"};
}

/** Builds and freezes the outgoing body. The frozen value is reused by retries. */
export function freezePayload(client_post_id: string, draft: OptimisticDraft): Readonly<PostPayload> {
  const content: PostPayload["content"] = {
    media_type: draft.content.media_type ?? OPTIMISTIC_MEDIA_TYPE,
    ...(draft.content.text !== undefined ? {text: draft.content.text} : {}),
    ...(draft.content.segments !== undefined ? {
      segments: Object.freeze(draft.content.segments.map(segment => Object.freeze({...segment}))) as PostSegment[],
    } : {}),
  };
  const payload: PostPayload = {
    client_post_id,
    kind: draft.kind ?? "message",
    content,
    ...(draft.thread_id ? {thread_id: draft.thread_id} : {}),
    ...(draft.parent_post_id ? {parent_post_id: draft.parent_post_id} : {}),
  };
  Object.freeze(content);
  return Object.freeze(payload);
}

/** Synthetic row for a pending/failed record. Satisfies `PostItem`. */
export function syntheticRow<T extends PostItem>(record: OptimisticRecord<T>, created_seq: number): OptimisticPost {
  const {content} = record.payload;
  return {
    id: `${OPTIMISTIC_ROW_PREFIX}${record.client_post_id}`,
    revision: 0,
    created_at: record.created_at,
    created_seq,
    content: {
      ...(content.text !== undefined ? {text: content.text} : {}),
      ...(content.segments !== undefined ? {segments: content.segments} : {}),
    },
    author: {kind: "human"},
    thread_id: record.payload.thread_id ?? `${OPTIMISTIC_ROW_PREFIX}${record.client_post_id}`,
    optimistic: true,
    client_post_id: record.client_post_id,
    phase: record.phase === "failed" ? "failed" : "pending",
    ...(record.error ? {error: record.error} : {}),
  };
}

/**
 * Merges canonical items with optimistic rows: canonical order is preserved,
 * optimistic rows are appended, and a canonical row that belongs to an
 * optimistic record is emitted exactly once (keyed by the stable synthetic key
 * via `keyOf`). Returns only the current context's rows.
 */
export function mergeOptimistic<T extends PostItem>(
  canonical: readonly T[], records: readonly OptimisticRecord<T>[], contextKey: string, snapshotVersion?: number,
): Array<T | OptimisticPost> {
  const live = records.filter(record => record.contextKey === contextKey);
  if (live.length === 0) return canonical.slice();

  let maxSeq = 0;
  const rows: Array<T | OptimisticPost> = [];
  const emitted = new Set<string>();
  for (const item of canonical) {
    if (emitted.has(item.id)) continue;
    emitted.add(item.id);
    if (Number.isFinite(item.created_seq)) maxSeq = Math.max(maxSeq, item.created_seq);
    rows.push(item);
  }

  for (const record of live) {
    const match = (record.canonicalId ? canonical.find(item => item.id === record.canonicalId) : undefined)
      ?? canonical.find(item => clientPostIdOf(item) === record.client_post_id)
      ?? (snapshotVersion === undefined || snapshotVersion === record.snapshotVersion ? record.canonicalPost : undefined);
    if (match) {
      // Already emitted at its canonical position (SSE/snapshot raced the HTTP
      // response) or emitted here when the list has not caught up yet.
      if (!emitted.has(match.id)) {
        emitted.add(match.id);
        rows.push(match);
      }
      continue;
    }
    if (record.phase === "confirmed") continue;
    rows.push(syntheticRow(record, maxSeq + record.order));
  }
  return rows;
}

/**
 * Stable `VirtualTimeline` key. A pending row and the canonical row that
 * replaces it resolve to the same `optimistic:{client_post_id}` key so the
 * height cache, DOM node, focus and scroll anchor survive the transition.
 */
export function optimisticKeyOf<T extends PostItem>(
  item: T | OptimisticPost, records: readonly OptimisticRecord<T>[], contextKey: string,
): string {
  if (isOptimisticPost(item)) return item.id;
  const clientPostId = clientPostIdOf(item);
  const record = records.find(candidate => candidate.contextKey === contextKey
    && (candidate.canonicalId === item.id
      || (clientPostId !== null && candidate.client_post_id === clientPostId)));
  return record ? `${OPTIMISTIC_ROW_PREFIX}${record.client_post_id}` : item.id;
}

/** Marks a record confirmed; identity-stable when nothing changes. */
export function applyOptimisticConfirmation<T extends PostItem>(
  records: OptimisticRecord<T>[], client_post_id: string, post: T, source: ConfirmationSource,
): OptimisticRecord<T>[] {
  const index = records.findIndex(record => record.client_post_id === client_post_id);
  if (index < 0) return records;
  const record = records[index];
  if (record.phase === "confirmed" && record.canonicalId === post.id) return records;
  const next = records.slice();
  next[index] = {...record, phase: "confirmed", canonicalId: post.id, canonicalPost: post,
    error: undefined, confirmSource: source};
  return next;
}

/** Marks a record failed; identity-stable when nothing changes. */
export function applyOptimisticFailure<T extends PostItem>(
  records: OptimisticRecord<T>[], client_post_id: string, error: OptimisticError,
): OptimisticRecord<T>[] {
  const index = records.findIndex(record => record.client_post_id === client_post_id);
  if (index < 0) return records;
  const record = records[index];
  // Confirmation wins: an ambiguous failure that resolves after the snapshot
  // already showed the canonical post must not un-confirm it.
  if (record.phase === "confirmed") return records;
  const next = records.slice();
  next[index] = {...record, phase: "failed", error};
  return next;
}

/**
 * Reconciles records against a canonical list (SSE/snapshot). Confirms any
 * record whose `client_post_id` or canonical id now appears. Identity-stable
 * when nothing changes so it can run in an effect without looping.
 */
export function reconcileRecords<T extends PostItem>(
  records: OptimisticRecord<T>[], canonical: readonly T[], contextKey: string,
): OptimisticRecord<T>[] {
  let changed = false;
  const next = records.map(record => {
    if (record.contextKey !== contextKey || record.phase === "confirmed") return record;
    const match = canonical.find(item => item.id === record.canonicalId
      || clientPostIdOf(item) === record.client_post_id);
    if (!match) return record;
    changed = true;
    return {...record, phase: "confirmed" as const, canonicalId: match.id, canonicalPost: match,
      error: undefined, confirmSource: "stream" as const};
  });
  return changed ? next : records;
}

function createClientPostId(): string {
  const source = globalThis.crypto;
  if (source && typeof source.randomUUID === "function") return source.randomUUID();
  return `cpid-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

type ActiveContext = {key: string; project: string; board: string};

export function useOptimisticPosts<T extends PostItem>(options: OptimisticPostsOptions<T>): OptimisticPosts<T> {
  const {project, board, sessionKey = "", canonical, timeoutMs = DEFAULT_OPTIMISTIC_TIMEOUT_MS, snapshotVersion} = options;
  const contextKey = `${project}\u0000${board}\u0000${sessionKey}`;

  const context = useMemo<ActiveContext>(
    () => ({key: contextKey, project, board}),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- contextKey encodes all three.
    [contextKey],
  );
  const contextRef = useRef(context);
  contextRef.current = context;
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);

  const [records, setRecords] = useState<OptimisticRecord<T>[]>([]);
  const recordsRef = useRef(records);
  recordsRef.current = records;

  const orderRef = useRef(0);
  const notifiedRef = useRef(new Set<string>());
  const onConfirmedRef = useRef(options.onConfirmed);
  onConfirmedRef.current = options.onConfirmed;
  const onErrorRef = useRef(options.onError);
  onErrorRef.current = options.onError;
  const timeoutRef = useRef(timeoutMs);
  timeoutRef.current = timeoutMs;
  const snapshotRef = useRef(snapshotVersion);
  snapshotRef.current = snapshotVersion;

  // Context changes clear every optimistic row: no draft, payload or row may
  // survive a project, board, session or access change.
  useEffect(() => {
    notifiedRef.current.clear();
    orderRef.current = 0;
    setRecords(current => current.length ? [] : current);
  }, [contextKey]);

  // Confirmations arriving through the canonical list (SSE/snapshot) are
  // committed even when the HTTP response has not arrived yet.
  useEffect(() => {
    if (!canonical) return;
    setRecords(current => {
      const retained = snapshotVersion === undefined ? current : current.filter(record =>
        record.phase !== "confirmed" || record.snapshotVersion === snapshotVersion);
      return reconcileRecords(retained, canonical, contextRef.current.key);
    });
  }, [canonical, snapshotVersion]);

  // Single notification path: state is the source of truth, so a confirmation
  // is reported exactly once no matter which source observed it.
  useEffect(() => {
    for (const record of records) {
      if (record.phase !== "confirmed" || record.contextKey !== contextRef.current.key) continue;
      if (notifiedRef.current.has(record.client_post_id)) continue;
      notifiedRef.current.add(record.client_post_id);
      if (record.canonicalPost) {
        onConfirmedRef.current?.(record.canonicalPost, {
          client_post_id: record.client_post_id,
          source: record.confirmSource ?? "http",
        });
      }
    }
  }, [records]);

  const deliver = useCallback((record: OptimisticRecord<T>, active: ActiveContext) => {
    let settled = false;
    const timeout = timeoutRef.current;
    const timer = timeout > 0 ? setTimeout(() => {
      if (settled) return;
      const failure: OptimisticError = {
        message: `No response after ${Math.round(timeout / 1000)}s.`,
        code: "timeout", ambiguous: true,
      };
      if (!mounted.current || contextRef.current.key !== record.contextKey) return;
      setRecords(current => applyOptimisticFailure(current, record.client_post_id, failure));
      onErrorRef.current?.(failure, {client_post_id: record.client_post_id});
    }, timeout) : undefined;

    void (async () => {
      try {
        const post = await api<T>(`boards/${encodeURIComponent(active.board)}/posts`, "POST",
          record.payload, undefined, record.client_post_id);
        settled = true;
        if (timer !== undefined) clearTimeout(timer);
        // Stale context: never commit, never notify, never clear the new draft.
        if (!mounted.current || contextRef.current.key !== record.contextKey) return;
        setRecords(current => applyOptimisticConfirmation(current, record.client_post_id, post, "http"));
      } catch (error) {
        settled = true;
        if (timer !== undefined) clearTimeout(timer);
        if (!mounted.current || contextRef.current.key !== record.contextKey) return;
        const failure = classifyOptimisticError(error);
        setRecords(current => applyOptimisticFailure(current, record.client_post_id, failure));
        onErrorRef.current?.(failure, {client_post_id: record.client_post_id});
      }
    })();
  }, []);

  const send = useCallback((draft: OptimisticDraft): string | null => {
    const active = contextRef.current;
    if (!mounted.current || !active.project || !active.board) return null;
    const hasContent = draft.content.text !== undefined && draft.content.text !== ""
      || Boolean(draft.content.segments?.length);
    if (!hasContent && !draft.parent_post_id) return null;
    const client_post_id = createClientPostId();
    const record: OptimisticRecord<T> = {
      client_post_id,
      contextKey: active.key,
      project: active.project,
      board: active.board,
      order: ++orderRef.current,
      phase: "pending",
      payload: freezePayload(client_post_id, draft),
      created_at: new Date().toISOString(),
      attempt: 1,
      snapshotVersion: snapshotRef.current,
    };
    setRecords(current => [...current, record]);
    deliver(record, active);
    return client_post_id;
  }, [deliver]);

  const retry = useCallback((client_post_id: string) => {
    const active = contextRef.current;
    const record = recordsRef.current.find(candidate => candidate.client_post_id === client_post_id);
    if (!record || record.phase !== "failed" || record.contextKey !== active.key) return;
    setRecords(current => current.map(candidate => candidate.client_post_id === client_post_id
      ? {...candidate, phase: "pending" as const, error: undefined, attempt: candidate.attempt + 1}
      : candidate));
    // Same frozen payload, same client_post_id: the server dedupes the retry.
    deliver(record, active);
  }, [deliver]);

  const cancel = useCallback((client_post_id: string) => {
    notifiedRef.current.delete(client_post_id);
    setRecords(current => current.some(record => record.client_post_id === client_post_id)
      ? current.filter(record => record.client_post_id !== client_post_id) : current);
  }, []);

  const items = useMemo(
    () => mergeOptimistic(canonical ?? [], records, context.key, snapshotVersion),
    [canonical, records, context.key, snapshotVersion],
  );

  const keyOf = useCallback(
    (item: T | OptimisticPost) => snapshotVersion !== undefined && clientPostIdOf(item)
      ? `${OPTIMISTIC_ROW_PREFIX}${clientPostIdOf(item)}` : optimisticKeyOf(item, records, context.key),
    [records, context.key, snapshotVersion],
  );

  const outstanding = useMemo(() => items.filter(isOptimisticPost), [items]);

  return {
    items,
    keyOf,
    isOptimistic: isOptimisticPost,
    send,
    retry,
    cancel,
    outstanding,
    pendingCount: outstanding.filter(row => row.phase === "pending").length,
    failedCount: outstanding.filter(row => row.phase === "failed").length,
  };
}
