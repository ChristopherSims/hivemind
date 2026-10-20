/**
 * Cursor-bound live-state recovery for the dashboard.
 *
 * Ground truth (mirrors `hivemind/api.py` and `hivemind/live_state.py`):
 *
 * - `GET /v1/streams?project_id=` is an SSE *invalidation* channel. Frames carry
 *   no content: either `event: resync` with `id: <snapshot_cursor>` and
 *   `data: {"resync_required":true}`, or a bare `: keepalive` comment.
 * - `snapshot_cursor` is the opaque 64-hex hash of the principal/project-bound
 *   visible snapshot returned by the timeline endpoints. It is NOT the activity
 *   stream replay cursor (`s<n>.a<n>` / `g<n>`); a foreign cursor is refused here
 *   so it can never be bound.
 * - The server compares the cursor it was bound to with a freshly computed one:
 *   a mismatch emits `resync` with the fresh `id`, a match emits only
 *   `keepalive`. A browser `EventSource` cannot set request headers, so the
 *   cursor a client actually holds is bound through an explicit `?cursor=`
 *   query parameter (`Last-Event-ID` only ever carries an id the server itself
 *   emitted). Without that binding a client can hold a snapshot the server does
 *   not know about and miss a `create`-then-`delete` invalidation entirely.
 *
 * The controller therefore:
 *
 * - binds the cursor of the snapshot the UI actually displays
 *   (`bindSnapshot`) and re-opens the stream whenever that binding changes;
 * - treats `resync` as "refetch the snapshot for the current epoch", reporting
 *   whether the frame's cursor already matches the displayed snapshot (so a
 *   refresh that races the stream does not thrash);
 * - fences every asynchronous commit with an epoch so a response from a
 *   superseded project/view never commits (`isCurrent`);
 * - re-checks session/current access on every heartbeat and on every stream
 *   error, and clears all privileged UI immediately - once - through
 *   `onRevoke` on 401/403 (or a terminal `revoked` frame), instead of
 *   reconnecting forever against a revoked session.
 */
export type ConnectionState = "connecting" | "live" | "reconnecting" | "revoked" | "stopped";
export type AuthorizationFailure = {status: number; reason: string};
export type ResyncInfo = {
  cursor: string | null; epoch: number;
  // true: the cursor the server just stated equals the displayed snapshot, so a
  // refetch is optional. false: the displayed snapshot is behind. null: one of
  // the two cursors is unknown (legacy server), refetch to be safe.
  matchesSnapshot: boolean | null;
  reason: "resync";
};
export type StreamEventLike = {data?: string; lastEventId?: string};
export type StreamListener = (event: StreamEventLike) => void;
export interface EventSourceLike {
  addEventListener(type: string, listener: StreamListener): void;
  close(): void;
  onopen: (() => void) | null;
  onerror: (() => void) | null;
}
export type LiveScheduler = {
  setTimeout: (fn: () => void, ms: number) => number;
  clearTimeout: (id: number) => void;
  setInterval: (fn: () => void, ms: number) => number;
  clearInterval: (id: number) => void;
};
export type LiveStateOptions = {
  /** Stream URL without a cursor, e.g. `/v1/streams?project_id=...`. */
  url: string;
  /** Session/current-access recheck. Must reject with an `ApiError` on 401/403. */
  probe: () => Promise<unknown>;
  /** Snapshot refetch for the current epoch; never a partial commit. */
  onResync: (info: ResyncInfo) => void;
  /** Clear every privileged view immediately. Called at most once per epoch. */
  onRevoke: (failure: AuthorizationFailure) => void;
  onState?: (state: ConnectionState) => void;
  onHeartbeat?: (result: unknown) => void;
  epoch?: number;
  heartbeatMs?: number;
  rebindCooldownMs?: number;
  eventSourceFactory?: (url: string) => EventSourceLike;
  /** Full timer replacement for tests; omitted in the browser. */
  scheduler?: LiveScheduler;
  now?: () => number;
};

export const RESYNC_EVENT = "resync";
/** Terminal frames the patched `/v1/streams` route emits before closing. */
export const TERMINAL_EVENTS = ["revoked"];
export const SNAPSHOT_CURSOR_PATTERN = /^[0-9a-f]{64}$/;
export const DEFAULT_HEARTBEAT_MS = 15000;
export const DEFAULT_REBIND_COOLDOWN_MS = 500;

/** Accept only the opaque snapshot hash; refuse the activity-stream cursor. */
export function normalizeSnapshotCursor(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const candidate = value.trim().toLowerCase();
  return SNAPSHOT_CURSOR_PATTERN.test(candidate) ? candidate : null;
}

/** Bind a cursor to a stream URL, replacing any previous binding. */
export function withCursor(url: string, cursor: string | null): string {
  const [head, fragment] = url.split("#", 2);
  const [path, search] = head.split("?", 2);
  const params = new URLSearchParams(search ?? "");
  params.delete("cursor");
  if (cursor) params.set("cursor", cursor);
  const query = params.toString();
  return `${path}${query ? `?${query}` : ""}${fragment === undefined ? "" : `#${fragment}`}`;
}

/** 401/403 from any request means the session or its permissions are gone. */
export function classifyAuthorizationFailure(error: unknown): AuthorizationFailure | null {
  if (!error || typeof error !== "object") return null;
  const status = (error as {status?: unknown}).status;
  if (status !== 401 && status !== 403) return null;
  const message = (error as {message?: unknown}).message;
  return {status, reason: typeof message === "string" && message ? message : "authorization_revoked"};
}

/** Decode a terminal `revoked` frame into the same failure shape. */
export function terminalFailure(event: StreamEventLike): AuthorizationFailure {
  let payload: unknown = null;
  try {
    payload = event?.data ? JSON.parse(event.data) : null;
  } catch {
    payload = null;
  }
  const record = (payload && typeof payload === "object" ? payload : {}) as
    {status?: unknown; code?: unknown; reason?: unknown};
  const status = typeof record.status === "number" ? record.status : 401;
  const reason = typeof record.code === "string" ? record.code
    : typeof record.reason === "string" ? record.reason : "authorization_revoked";
  return {status, reason};
}

const globalScheduler: LiveScheduler = {
  setTimeout: (fn, ms) => setTimeout(fn, ms) as unknown as number,
  clearTimeout: id => clearTimeout(id as unknown as ReturnType<typeof setTimeout>),
  setInterval: (fn, ms) => setInterval(fn, ms) as unknown as number,
  clearInterval: id => clearInterval(id as unknown as ReturnType<typeof setInterval>),
};

const defaultEventSourceFactory = (url: string): EventSourceLike =>
  new EventSource(url) as unknown as EventSourceLike;

export class LiveStateController {
  private readonly options: LiveStateOptions;
  private readonly scheduler: LiveScheduler;
  private readonly now: () => number;
  private epochValue: number;
  private stateValue: ConnectionState = "stopped";
  private snapshotCursorValue: string | null = null;
  private boundCursorValue: string | null = null;
  private connectedCursorValue: string | null = null;
  private streamCursorValue: string | null = null;
  private unboundValue = false;
  private revokedValue = false;
  private stream: EventSourceLike | null = null;
  private heartbeat: number | null = null;
  private pendingRebind: number | null = null;
  private lastRebindAt = Number.NEGATIVE_INFINITY;
  private probing = false;

  constructor(options: LiveStateOptions) {
    this.options = options;
    // Bound per method: an injected scheduler keeps its own receiver, and a
    // partial override still falls back to the global timers.
    const scheduler = options.scheduler;
    this.scheduler = scheduler
      ? {
          setTimeout: scheduler.setTimeout.bind(scheduler),
          clearTimeout: scheduler.clearTimeout.bind(scheduler),
          setInterval: scheduler.setInterval.bind(scheduler),
          clearInterval: scheduler.clearInterval.bind(scheduler),
        }
      : globalScheduler;
    this.now = options.now ?? (() => Date.now());
    this.epochValue = options.epoch ?? 0;
  }

  get state(): ConnectionState { return this.stateValue; }
  get epoch(): number { return this.epochValue; }
  /** Cursor of the snapshot currently displayed, or null when unbindable. */
  get snapshotCursor(): string | null { return this.snapshotCursorValue; }
  /** Cursor the server was last told about (connect binding or emitted id). */
  get boundCursor(): string | null { return this.boundCursorValue; }
  /** Last cursor the server stated in a `resync` frame. */
  get streamCursor(): string | null { return this.streamCursorValue; }
  /** True when the server supplied no valid cursor (legacy fallback mode). */
  get legacyUnbound(): boolean { return this.unboundValue; }

  /** Fence superseded projects/views: any older epoch must not commit. */
  fence(): number {
    this.epochValue++;
    return this.epochValue;
  }

  isCurrent(epoch: number): boolean {
    return epoch === this.epochValue && !this.revokedValue;
  }

  /**
   * Bind the cursor returned by the snapshot the UI now displays. Returns false
   * for a stale epoch (the response must not commit) or an absent cursor.
   */
  bindSnapshot(cursor: unknown, epoch: number = this.epochValue): boolean {
    // A revoked session must not commit privileged data or rebind a stream.
    if (this.revokedValue || epoch !== this.epochValue) return false;
    const normalized = normalizeSnapshotCursor(cursor);
    this.snapshotCursorValue = normalized;
    this.unboundValue = normalized === null;
    // The server only ever compares against the cursor it was bound to, so a new
    // snapshot binding must be pushed to the stream or a create-then-delete
    // invalidation can be missed.
    if (normalized !== this.connectedCursorValue) this.scheduleRebind();
    return normalized !== null;
  }

  /** Funnel every API error here: 401/403 clears privileged UI immediately. */
  reportError(error: unknown): boolean {
    const failure = classifyAuthorizationFailure(error);
    if (!failure) return false;
    this.revoke(failure);
    return true;
  }

  revoke(failure: AuthorizationFailure): void {
    if (this.revokedValue) return;
    this.revokedValue = true;
    this.clearHeartbeat();
    this.clearRebind();
    this.closeStream();
    this.setState("revoked");
    this.options.onRevoke(failure);
  }

  start(): void {
    if (this.revokedValue || this.stateValue !== "stopped") return;
    this.setState("connecting");
    this.openStream();
    this.heartbeat = this.scheduler.setInterval(
      () => void this.verify(), this.options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS);
  }

  stop(): void {
    this.fence();
    this.clearHeartbeat();
    this.clearRebind();
    this.closeStream();
    this.setState("stopped");
  }

  /**
   * Heartbeat and reconnect check: re-authenticate and recheck current access.
   * A revoked session stops the stream for good; a transient failure only marks
   * the connection as reconnecting so the browser keeps retrying.
   */
  async verify(): Promise<"ok" | "denied" | "unavailable" | "stale"> {
    if (this.revokedValue || this.stateValue === "stopped") return "stale";
    if (this.probing) return "ok";
    const epoch = this.epochValue;
    this.probing = true;
    let result: unknown;
    try {
      result = await this.options.probe();
    } catch (error) {
      if (epoch !== this.epochValue) return "stale";
      const failure = classifyAuthorizationFailure(error);
      if (failure) {
        this.revoke(failure);
        return "denied";
      }
      this.setState("reconnecting");
      return "unavailable";
    } finally {
      this.probing = false;
    }
    if (epoch !== this.epochValue || this.revokedValue) return "stale";
    // A successful recheck authorizes the reader; a stream that is still
    // reconnecting stays "reconnecting" until its own `open` fires.
    if (this.stateValue === "connecting") this.setState("live");
    this.options.onHeartbeat?.(result);
    return "ok";
  }

  private setState(state: ConnectionState): void {
    if (this.stateValue === state) return;
    this.stateValue = state;
    this.options.onState?.(state);
  }

  private openStream(): void {
    if (this.revokedValue || this.stateValue === "stopped") return;
    this.clearRebind();
    this.closeStream();
    const cursor = this.snapshotCursorValue;
    this.connectedCursorValue = cursor;
    this.boundCursorValue = cursor;
    this.lastRebindAt = this.now();
    const epoch = this.epochValue;
    const factory = this.options.eventSourceFactory ?? defaultEventSourceFactory;
    const stream = factory(withCursor(this.options.url, cursor));
    stream.onopen = () => {
      if (stream !== this.stream || epoch !== this.epochValue || this.revokedValue) return;
      this.setState("live");
    };
    stream.onerror = () => {
      if (stream !== this.stream || epoch !== this.epochValue || this.revokedValue || this.stateValue === "stopped") return;
      // A mid-stream revocation closes the connection, so the error itself never
      // decides: recheck access and only then keep reconnecting or revoke.
      this.setState("reconnecting");
      void this.verify();
    };
    stream.addEventListener(RESYNC_EVENT, event => {
      if (stream !== this.stream || epoch !== this.epochValue || this.revokedValue || this.stateValue === "stopped") return;
      const id = normalizeSnapshotCursor(event?.lastEventId);
      this.streamCursorValue = id;
      if (id) this.boundCursorValue = id;
      this.setState("live");
      this.options.onResync({
        cursor: id, epoch, reason: "resync",
        matchesSnapshot: id === null || this.snapshotCursorValue === null
          ? null : id === this.snapshotCursorValue,
      });
    });
    for (const name of TERMINAL_EVENTS) {
      stream.addEventListener(name, event => {
        if (stream !== this.stream || epoch !== this.epochValue || this.revokedValue) return;
        this.revoke(terminalFailure(event));
      });
    }
    this.stream = stream;
    if (this.stateValue === "live") this.setState("connecting");
  }

  private scheduleRebind(): void {
    if (this.revokedValue || this.stateValue === "stopped" || this.pendingRebind !== null) return;
    const cooldown = this.options.rebindCooldownMs ?? DEFAULT_REBIND_COOLDOWN_MS;
    const wait = Math.max(0, this.lastRebindAt + cooldown - this.now());
    if (wait === 0) {
      this.openStream();
      return;
    }
    this.pendingRebind = this.scheduler.setTimeout(() => {
      this.pendingRebind = null;
      this.openStream();
    }, wait);
  }

  private clearRebind(): void {
    if (this.pendingRebind === null) return;
    this.scheduler.clearTimeout(this.pendingRebind);
    this.pendingRebind = null;
  }

  private clearHeartbeat(): void {
    if (this.heartbeat === null) return;
    this.scheduler.clearInterval(this.heartbeat);
    this.heartbeat = null;
  }

  private closeStream(): void {
    if (!this.stream) return;
    this.stream.onopen = null;
    this.stream.onerror = null;
    this.stream.close();
    this.stream = null;
  }
}
