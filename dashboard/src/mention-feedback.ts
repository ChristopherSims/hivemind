/**
 * Shared mention feedback: pre-submit detection of unresolved `@text` and
 * post-send reporting of canonical queued notifications.
 *
 * Two honest rules drive everything here:
 *
 * 1. A plain-text `@token` is only "resolved" when an explicitly selected
 *    mention chip with the same immutable name covers it. Typed text that the
 *    menu never confirmed notifies nobody, so the composer must say so before
 *    the post is sent instead of silently dropping the intent.
 * 2. "Queued" is only reported from a canonical notification receipt returned
 *    by the server. Presence is never used to guess delivery, and a missing or
 *    unreadable receipt is reported as unavailable rather than upgraded to a
 *    promise. The copy never implies an agent will reply.
 *
 * The module is host-agnostic: it reads only the draft value and the selected
 * chips, never mutates a host draft, and never persists anything
 * (no storage, cookies, or credentials).
 */
import {useCallback, useEffect, useMemo, useRef, useState} from "react";
import {api} from "./api";

/** The only fields the guard needs from a selected chip. */
export type MentionSelectionLike = {id: string; display_name: string};

/** A typed `@token` in the draft that no selected chip backs. */
export type UnresolvedMention = {
  /** Mention name, without the leading "@". */
  text: string;
  /** Index of the "@" that opens the token. */
  start: number;
  /** Exclusive end of the token (the name characters, no trailing space). */
  end: number;
};

const MENTION_TOKEN = /@([\p{L}\p{N}][\p{L}\p{N}_.'’-]*)/gu;
// A mention opener is not preceded by identifier characters ("user@example") or
// path separators ("https://example.com/@user").
const WORD_BEFORE = /[\p{L}\p{N}_.@/\-]/u;
const NAME_AFTER = /[\p{L}\p{N}_'’-]/u;
const BACKTICK_RUN = /`+/g;

function isEscaped(text: string, index: number): boolean {
  let backslashes = 0;
  for (let cursor = index - 1; cursor >= 0 && text[cursor] === "\\"; cursor -= 1) backslashes += 1;
  return backslashes % 2 === 1;
}

/** Half-open ranges covered by inline code spans and fenced code blocks. */
function codeRanges(text: string): Array<[number, number]> {
  const delimiters: {index: number; length: number}[] = [];
  for (const match of text.matchAll(BACKTICK_RUN)) {
    const index = match.index ?? 0;
    if (!isEscaped(text, index)) delimiters.push({index, length: match[0].length});
  }
  const ranges: Array<[number, number]> = [];
  for (let index = 0; index + 1 < delimiters.length; index += 2) {
    const close = delimiters[index + 1];
    ranges.push([delimiters[index].index, close.index + close.length]);
  }
  return ranges;
}

/**
 * Names typed as `@text` that no selected chip resolves.
 *
 * E-mail addresses, identifiers, escaped `\@name`, code spans/fences, and bare
 * "@" are deliberately not flagged. A selected chip resolves a token when the
 * chip's full display name appears at that token (so "@Droid worker" is covered
 * by the "Droid worker" chip) or when the token equals the chip name.
 */
export function findUnresolvedMentions(
  text: string,
  selections: MentionSelectionLike[] = [],
): UnresolvedMention[] {
  const names = selections
    .map(selection => (selection?.display_name ?? "").trim())
    .filter(name => name.length > 0);
  const code = codeRanges(text);
  const unresolved: UnresolvedMention[] = [];
  for (const match of text.matchAll(MENTION_TOKEN)) {
    const start = match.index ?? 0;
    const name = match[1];
    if (isEscaped(text, start)) continue;
    if (start > 0 && WORD_BEFORE.test(text[start - 1])) continue;
    if (code.some(([from, to]) => start >= from && start < to)) continue;
    if (isResolved(text, start, name, names)) continue;
    unresolved.push({text: name, start, end: start + 1 + name.length});
  }
  return unresolved;
}

function isResolved(text: string, start: number, name: string, names: string[]): boolean {
  const lower = name.toLowerCase();
  return names.some(candidate => {
    const expected = candidate.toLowerCase();
    if (expected === lower) return true;
    if (!expected.startsWith(`${lower} `)) return false;
    if (!text.toLowerCase().startsWith(expected, start + 1)) return false;
    const after = text[start + 1 + candidate.length] ?? "";
    return after === "" || !NAME_AFTER.test(after);
  });
}

/** Accessible one-line summary of what will not notify anyone. */
export function describeUnresolved(unresolved: UnresolvedMention[]): string {
  if (!unresolved.length) return "";
  const names = unresolved.map(item => `@${item.text}`);
  if (names.length === 1) return `${names[0]} is plain text and will not notify an agent.`;
  const last = names[names.length - 1];
  return `${names.slice(0, -1).join(", ")} and ${last} are plain text and will not notify any agent.`;
}

// ---------------------------------------------------------------------------
// Canonical receipts
// ---------------------------------------------------------------------------

export type MentionReceipt = {
  agent_id: string;
  display_name?: string;
  /** Canonical notification record id; a receipt without one is discarded. */
  notification_id: string;
  /** "queued" only while the canonical record is still available. */
  state: "queued" | "closed";
  created_at?: string;
  expires_at?: number;
  /** Coarse band from the server; never a delivery or read acknowledgement. */
  presence?: "online" | "stale" | "offline" | null;
};

export type MentionReceipts = {
  post_id: string;
  recipients: MentionReceipt[];
  queued_count: number;
  generated_at?: number;
};

export type MentionReceiptStatus = "idle" | "pending" | "queued" | "closed" | "none" | "unavailable";

export type MentionReceiptFeedback = {
  status: MentionReceiptStatus;
  message: string;
  recipients: MentionReceipt[];
};

export const IDLE_FEEDBACK: MentionReceiptFeedback = {status: "idle", message: "", recipients: []};

/** Drop anything that is not a canonical receipt, so nothing is fabricated. */
export function normalizeMentionReceipts(raw: unknown): MentionReceipts {
  const source = (raw && typeof raw === "object" ? raw : {}) as Record<string, unknown>;
  const rows = Array.isArray(source.recipients) ? source.recipients : [];
  const recipients: MentionReceipt[] = [];
  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const entry = row as Record<string, unknown>;
    const notificationId = typeof entry.notification_id === "string" ? entry.notification_id : "";
    const agentId = typeof entry.agent_id === "string" ? entry.agent_id : "";
    if (!notificationId || !agentId) continue;
    recipients.push({
      agent_id: agentId,
      display_name: typeof entry.agent_display_name === "string" ? entry.agent_display_name : undefined,
      notification_id: notificationId,
      state: entry.state === "queued" ? "queued" : "closed",
      created_at: typeof entry.created_at === "string" ? entry.created_at : undefined,
      expires_at: typeof entry.expires_at === "number" ? entry.expires_at : undefined,
      presence: entry.presence === "online" || entry.presence === "stale" || entry.presence === "offline"
        ? entry.presence
        : null,
    });
  }
  return {
    post_id: typeof source.post_id === "string" ? source.post_id : "",
    recipients,
    queued_count: recipients.filter(item => item.state === "queued").length,
    generated_at: typeof source.generated_at === "number" ? source.generated_at : undefined,
  };
}

function queuedSummary(queued: MentionReceipt[]): string {
  const noun = queued.length === 1 ? "mention notification is" : "mention notifications are";
  const offline = queued.filter(item => item.presence === "offline").length;
  const base = `${queued.length} ${noun} queued. Queued means the notification was recorded, not that an agent has replied.`;
  if (!offline) return base;
  const who = offline === 1 ? "1 recipient is" : `${offline} recipients are`;
  return `${base} ${who} offline. Queued notifications can be retrieved after reconnecting while available.`;
}

/**
 * Turn a receipt read (or its failure) into honest feedback. Pending is the
 * optimistic stage immediately after the canonical post is accepted; error and
 * missing payloads become "unavailable", never "queued".
 */
export function receiptFeedback(
  receipts: MentionReceipts | null,
  options: {pending?: boolean; error?: unknown} = {},
): MentionReceiptFeedback {
  if (options.pending) {
    return {status: "pending", recipients: [],
      message: "Post accepted. Confirming which mention notifications were recorded…"};
  }
  if (options.error || !receipts) {
    return {status: "unavailable", recipients: [],
      message: "Post accepted. Mention receipts could not be read, so no queued status is claimed."};
  }
  const queued = receipts.recipients.filter(item => item.state === "queued");
  if (queued.length) {
    return {status: "queued", recipients: receipts.recipients, message: queuedSummary(queued)};
  }
  if (receipts.recipients.length) {
    return {status: "closed", recipients: receipts.recipients,
      message: "The mention notifications for this post are no longer queued."};
  }
  return {status: "none", recipients: [],
    message: "No agent mention notifications were recorded for this post."};
}

export type MentionRequest = <T>(path: string, method?: string, body?: unknown) => Promise<T>;

const defaultRequest: MentionRequest = (path, method, body) => api(path, method, body);

/** Read the author-scoped receipts for one post. Injectable for hosts and tests. */
export async function fetchMentionReceipts(
  postId: string,
  request: MentionRequest = defaultRequest,
): Promise<MentionReceipts> {
  const raw = await request<unknown>(`posts/${encodeURIComponent(postId)}/mention-receipts`, "GET");
  return normalizeMentionReceipts(raw);
}

// ---------------------------------------------------------------------------
// Shared guard used by both composers
// ---------------------------------------------------------------------------

export type MentionSendGuardOptions = {
  /** The host's draft value. Never mutated. */
  value: string;
  /** Chips the host currently has attached to the draft. */
  selections?: MentionSelectionLike[];
  /** Defaults to true; false disables detection entirely (no warning, never blocks). */
  enabled?: boolean;
  /** Range of the `@token` the caret is still inside, if any, so typing is not flagged. */
  activeRange?: {start: number; end: number} | null;
  /** Changing this (board, project, permission revision) resets confirmation and receipts. */
  contextKey?: string;
  /** Notified whenever the unresolved set changes. */
  onChange?: (unresolved: UnresolvedMention[]) => void;
};

export type MentionSendGuardView = {
  unresolved: UnresolvedMention[];
  /** True while unresolved mentions exist and the author has not confirmed a plain-text send. */
  blocked: boolean;
  plainTextConfirmed: boolean;
  /** Record the author's explicit intent for the tokens currently in the draft. */
  confirmPlainText: () => void;
  /** Clear the confirmation (call after a send, or to require a fresh decision). */
  reset: () => void;
  /** Submit-handler gate: false means wait for the explicit plain-text confirmation. */
  trySend: () => boolean;
  receipts: MentionReceiptFeedback;
  /** Post the current receipts belong to, for an explicit re-check. */
  receiptPostId: string | null;
  /** Optimistic pending, then the canonical receipt (or an honest unavailable state). */
  trackReceipts: (postId: string) => void;
  resetReceipts: () => void;
};

export function useMentionSendGuard({
  value, selections, enabled = true, activeRange = null, contextKey = "", onChange,
}: MentionSendGuardOptions): MentionSendGuardView {
  const changeCallback = useRef(onChange);
  useEffect(() => {
    changeCallback.current = onChange;
  });

  const unresolved = useMemo(() => {
    if (!enabled) return [];
    const found = findUnresolvedMentions(value, selections ?? []);
    if (!activeRange) return found;
    // A token the caret is still inside is being typed, not yet abandoned.
    return found.filter(item => !(item.start === activeRange.start && item.end >= activeRange.end));
  }, [enabled, value, selections, activeRange]);

  useEffect(() => {
    changeCallback.current?.(unresolved);
  }, [unresolved]);

  // Consent is bound to the exact tokens it was given for: editing the draft
  // (or replacing the chips) invalidates it and asks again.
  const signature = useMemo(
    () => JSON.stringify([contextKey, value, selections?.map(item => [item.id, item.display_name]) ?? [],
      unresolved.map(item => [item.start, item.end, item.text])]),
    [contextKey, value, selections, unresolved]);
  const [confirmed, setConfirmed] = useState<string | null>(null);
  const plainTextConfirmed = unresolved.length > 0 && confirmed === signature;
  const blocked = unresolved.length > 0 && !plainTextConfirmed;
  const confirmPlainText = useCallback(() => setConfirmed(signature), [signature]);
  const resetGuard = useCallback(() => setConfirmed(null), []);
  const trySend = useCallback(
    () => !(unresolved.length > 0 && confirmed !== signature),
    [unresolved, confirmed, signature]);

  const [feedback, setFeedback] = useState<MentionReceiptFeedback>(IDLE_FEEDBACK);
  const [receiptPostId, setReceiptPostId] = useState<string | null>(null);
  const sequence = useRef(0);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  const context = useRef(contextKey);
  context.current = contextKey;

  const resetReceipts = useCallback(() => {
    sequence.current += 1;
    setReceiptPostId(null);
    setFeedback(IDLE_FEEDBACK);
  }, []);

  const previousContext = useRef(contextKey);
  useEffect(() => {
    if (previousContext.current === contextKey) return;
    previousContext.current = contextKey;
    setConfirmed(null);
    resetReceipts();
  }, [contextKey, resetReceipts]);

  const trackReceipts = useCallback((postId: string) => {
    const identity = String(postId ?? "").trim();
    const request = sequence.current + 1;
    const requestContext = context.current;
    sequence.current = request;
    setReceiptPostId(identity || null);
    if (!identity) {
      setFeedback(IDLE_FEEDBACK);
      return;
    }
    setFeedback(receiptFeedback(null, {pending: true}));
    void fetchMentionReceipts(identity)
      .then(receipts => {
        if (!mounted.current || request !== sequence.current || requestContext !== context.current) return;
        setFeedback(receiptFeedback(receipts));
      })
      .catch(() => {
        if (!mounted.current || request !== sequence.current || requestContext !== context.current) return;
        setFeedback(receiptFeedback(null, {error: true}));
      });
  }, []);

  return {
    unresolved, blocked, plainTextConfirmed, confirmPlainText, reset: resetGuard, trySend,
    receipts: previousContext.current === contextKey ? feedback : IDLE_FEEDBACK,
    receiptPostId: previousContext.current === contextKey ? receiptPostId : null, trackReceipts, resetReceipts,
  };
}
