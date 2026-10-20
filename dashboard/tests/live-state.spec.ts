import {test, expect} from "@playwright/test";
import {
  DEFAULT_HEARTBEAT_MS, LiveStateController, TERMINAL_EVENTS,
  classifyAuthorizationFailure, normalizeSnapshotCursor, terminalFailure, withCursor,
  type EventSourceLike, type LiveScheduler, type ResyncInfo, type StreamListener,
} from "../src/live-state";

const CURSOR = "a".repeat(64);
const OTHER = "b".repeat(64);

class FakeEventSource implements EventSourceLike {
  static created: FakeEventSource[] = [];
  listeners: Record<string, StreamListener[]> = {};
  closed = false;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(public url: string) {
    FakeEventSource.created.push(this);
  }

  addEventListener(type: string, listener: StreamListener) {
    (this.listeners[type] ??= []).push(listener);
  }

  close() { this.closed = true; }

  emit(type: string, event: {data?: string; lastEventId?: string} = {}) {
    if (this.closed) return;
    for (const listener of this.listeners[type] ?? []) listener(event);
  }
}

class FakeScheduler implements LiveScheduler {
  time = 0;
  timeouts: {id: number; at: number; fn: () => void}[] = [];
  intervals: {id: number; at: number; every: number; fn: () => void}[] = [];
  private next = 1;

  setTimeout(fn: () => void, ms: number) {
    const id = this.next++;
    this.timeouts.push({id, at: this.time + ms, fn});
    return id;
  }

  clearTimeout(id: number) { this.timeouts = this.timeouts.filter(item => item.id !== id); }

  setInterval(fn: () => void, ms: number) {
    const id = this.next++;
    this.intervals.push({id, at: this.time + ms, every: ms, fn});
    return id;
  }

  clearInterval(id: number) { this.intervals = this.intervals.filter(item => item.id !== id); }

  advance(ms: number) {
    const target = this.time + ms;
    for (;;) {
      const pending = [...this.timeouts, ...this.intervals].filter(item => item.at <= target)
        .sort((left, right) => left.at - right.at)[0];
      if (!pending) break;
      this.time = pending.at;
      if ("every" in pending) pending.at += pending.every;
      else this.timeouts = this.timeouts.filter(item => item !== pending);
      pending.fn();
    }
    this.time = target;
  }

  get pendingTimers() { return this.timeouts.length + this.intervals.length; }
}

function apiError(status: number, message: string) {
  return Object.assign(new Error(`${status}: ${message}`), {status});
}

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

function setup(options: {
  probe?: () => Promise<unknown>;
  heartbeatMs?: number;
  rebindCooldownMs?: number;
  onResync?: (info: ResyncInfo) => void;
  onRevoke?: (failure: {status: number; reason: string}) => void;
  onHeartbeat?: (result: unknown) => void;
  onState?: (state: string) => void;
} = {}) {
  FakeEventSource.created = [];
  const scheduler = new FakeScheduler();
  const events = {resyncs: [] as ResyncInfo[], revocations: [] as {status: number; reason: string}[],
    heartbeats: [] as unknown[], states: [] as string[], probes: 0};
  const live = new LiveStateController({
    url: "/v1/streams?project_id=project_demo",
    probe: async () => { events.probes++; return options.probe ? options.probe() : {role: "admin"}; },
    onResync: info => { events.resyncs.push(info); options.onResync?.(info); },
    onRevoke: failure => { events.revocations.push(failure); options.onRevoke?.(failure); },
    onHeartbeat: result => { events.heartbeats.push(result); options.onHeartbeat?.(result); },
    onState: state => { events.states.push(state); options.onState?.(state); },
    heartbeatMs: options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS,
    rebindCooldownMs: options.rebindCooldownMs ?? 0,
    scheduler,
    // The clock is the scheduler's clock so cooldowns are deterministic.
    now: () => scheduler.time,
    eventSourceFactory: url => new FakeEventSource(url),
  });
  return {live, scheduler, events, stream: () => FakeEventSource.created[FakeEventSource.created.length - 1]};
}

// ------------------------------------------------------------------ cursor API
test("cursors are validated so a foreign or malformed cursor is never bound", () => {
  expect(normalizeSnapshotCursor(CURSOR)).toBe(CURSOR);
  expect(normalizeSnapshotCursor(CURSOR.toUpperCase())).toBe(CURSOR);
  expect(normalizeSnapshotCursor(` ${CURSOR} `)).toBe(CURSOR);
  for (const value of ["s3.a1", "g2", "", "a".repeat(63), "z".repeat(64), null, 42, [CURSOR]]) {
    expect(normalizeSnapshotCursor(value), String(value)).toBeNull();
  }
});

test("stream URLs carry exactly one cursor binding", () => {
  expect(withCursor("/v1/streams?project_id=p", null)).toBe("/v1/streams?project_id=p");
  expect(withCursor("/v1/streams?project_id=p", CURSOR))
    .toBe(`/v1/streams?project_id=p&cursor=${CURSOR}`);
  expect(withCursor(`/v1/streams?project_id=p&cursor=${OTHER}`, CURSOR))
    .toBe(`/v1/streams?project_id=p&cursor=${CURSOR}`);
  expect(withCursor(`/v1/streams?cursor=${CURSOR}`, null)).toBe("/v1/streams");
  expect(withCursor("/v1/streams", null)).toBe("/v1/streams");
});

test("authorization failures are separated from transient errors", () => {
  expect(classifyAuthorizationFailure(apiError(401, "authentication required")))
    .toEqual({status: 401, reason: "401: authentication required"});
  expect(classifyAuthorizationFailure(apiError(403, "forbidden"))?.status).toBe(403);
  expect(classifyAuthorizationFailure(apiError(404, "not found"))).toBeNull();
  expect(classifyAuthorizationFailure(new Error("Failed to fetch"))).toBeNull();
  expect(classifyAuthorizationFailure(undefined)).toBeNull();
  expect(terminalFailure({data: '{"status":403,"code":"permission_revoked"}'}))
    .toEqual({status: 403, reason: "permission_revoked"});
  expect(terminalFailure({data: "not json"})).toEqual({status: 401, reason: "authorization_revoked"});
});

// ------------------------------------------------- cursor-bound snapshot binding
test("a snapshot binding is pushed to the stream and resync reports the match", () => {
  const {live, events, stream} = setup();
  live.start();
  expect(FakeEventSource.created).toHaveLength(1);
  expect(stream().url).toBe("/v1/streams?project_id=project_demo");
  expect(live.state).toBe("connecting");
  stream().onopen?.();
  expect(live.state).toBe("live");

  // The snapshot the UI displays is fetched at CURSOR: the stream is rebound to
  // exactly that cursor so the server compares against what the client holds.
  expect(live.bindSnapshot(CURSOR)).toBe(true);
  expect(FakeEventSource.created).toHaveLength(2);
  expect(stream().url).toBe(`/v1/streams?project_id=project_demo&cursor=${CURSOR}`);
  expect(live.boundCursor).toBe(CURSOR);
  expect(live.snapshotCursor).toBe(CURSOR);

  // A resync that states the displayed cursor needs no refetch.
  stream().emit("resync", {lastEventId: CURSOR, data: '{"resync_required":true}'});
  expect(events.resyncs).toHaveLength(1);
  expect(events.resyncs[0]).toMatchObject({cursor: CURSOR, matchesSnapshot: true, reason: "resync"});
  expect(live.streamCursor).toBe(CURSOR);

  // A resync that states a different cursor means the displayed snapshot is behind.
  stream().emit("resync", {lastEventId: OTHER});
  expect(events.resyncs[1]).toMatchObject({cursor: OTHER, matchesSnapshot: false});
});

test("rebinding is idempotent for the same cursor and legacy servers stay unbound", () => {
  const {live, stream} = setup();
  live.start();
  live.bindSnapshot(CURSOR);
  const opened = FakeEventSource.created.length;
  expect(live.bindSnapshot(CURSOR)).toBe(true);
  expect(FakeEventSource.created.length).toBe(opened);
  expect(stream().closed).toBe(false);

  // A server that omits the cursor falls back to refresh-per-resync and never
  // invents a binding.
  expect(live.bindSnapshot(undefined)).toBe(false);
  expect(live.legacyUnbound).toBe(true);
  expect(live.snapshotCursor).toBeNull();
  expect(stream().url).toBe("/v1/streams?project_id=project_demo");
  stream().emit("resync", {lastEventId: OTHER});
  expect(live.streamCursor).toBe(OTHER);
});

test("the initial resync that races the first snapshot is not treated as stale", () => {
  const {live, events, stream} = setup();
  live.start();
  stream().emit("resync", {lastEventId: CURSOR});
  expect(events.resyncs[0].matchesSnapshot).toBeNull();
  live.bindSnapshot(CURSOR);
  expect(live.legacyUnbound).toBe(false);
});

// ------------------------------------------------------ revocation, race fences
test("a revoked session clears privileged state once and stops reconnecting", async () => {
  const probe = {fail: false};
  const {live, scheduler, events, stream} = setup({probe: async () => {
    if (probe.fail) throw apiError(401, "authentication required");
    return {role: "admin"};
  }});
  live.start();
  stream().onopen?.();
  probe.fail = true;

  // A stream error never decides on its own: the access recheck does.
  stream().onerror?.();
  expect(live.state).toBe("reconnecting");
  await flush();
  expect(events.revocations).toEqual([{status: 401, reason: "401: authentication required"}]);
  expect(live.state).toBe("revoked");
  expect(stream().closed).toBe(true);
  expect(scheduler.pendingTimers).toBe(0);
  expect(live.isCurrent(live.epoch)).toBe(false);

  // Revocation is terminal: no further resync, heartbeat, or double callback.
  stream().emit("resync", {lastEventId: OTHER});
  expect(events.resyncs).toHaveLength(0);
  expect(await live.verify()).toBe("stale");
  live.revoke({status: 401, reason: "again"});
  expect(events.revocations).toHaveLength(1);
  expect(live.bindSnapshot(CURSOR)).toBe(false);
  expect(FakeEventSource.created).toHaveLength(1);
});

test("a terminal revoked frame clears privileged state without waiting for the probe", () => {
  const {live, events, stream} = setup();
  live.start();
  stream().onopen?.();
  expect(TERMINAL_EVENTS).toContain("revoked");
  stream().emit("revoked", {data: '{"resync_required":true,"reason":"permission_revoked","status":403,"code":"forbidden"}'});
  expect(events.revocations).toEqual([{status: 403, reason: "forbidden"}]);
  expect(live.state).toBe("revoked");
  expect(stream().closed).toBe(true);
});

test("heartbeat rechecks session and current access on a fixed cadence", async () => {
  const {live, scheduler, events} = setup({heartbeatMs: 1000});
  live.start();
  for (let beat = 0; beat < 3; beat++) {
    scheduler.advance(1000);
    await flush();
  }
  expect(events.probes).toBe(3);
  expect(events.heartbeats).toEqual([{role: "admin"}, {role: "admin"}, {role: "admin"}]);
  expect(live.state).toBe("live");

  // A transient failure only marks the connection as reconnecting.
  const failing = setup({heartbeatMs: 1000, probe: async () => { throw new Error("Failed to fetch"); }});
  failing.live.start();
  failing.scheduler.advance(1000);
  await flush();
  expect(failing.events.revocations).toHaveLength(0);
  expect(failing.live.state).toBe("reconnecting");
});

test("epoch fences keep a superseded project or view from committing", async () => {
  const gate: (() => void)[] = [];
  const {live, events, stream} = setup({probe: () => new Promise<void>(resolve => { gate.push(resolve); })});
  live.start();
  const stale = live.epoch;
  const pending = live.verify();
  const fresh = live.fence();
  expect(live.isCurrent(stale)).toBe(false);
  expect(live.isCurrent(fresh)).toBe(true);
  expect(live.bindSnapshot(CURSOR, stale)).toBe(false);
  expect(live.snapshotCursor).toBeNull();
  // A late response for the superseded epoch must not flip the state.
  for (const resolve of gate) resolve();
  expect(await pending).toBe("stale");
  expect(live.state).not.toBe("live");
  // A late frame from the superseded stream is ignored as well.
  stream().emit("resync", {lastEventId: CURSOR});
  expect(events.resyncs).toHaveLength(0);
});

test("reportError funnels every authorization error and ignores the rest", () => {
  const {live, events} = setup();
  live.start();
  expect(live.reportError(new Error("Failed to fetch"))).toBe(false);
  expect(live.reportError(apiError(404, "not found"))).toBe(false);
  expect(events.revocations).toHaveLength(0);
  expect(live.reportError(apiError(403, "forbidden"))).toBe(true);
  expect(events.revocations).toEqual([{status: 403, reason: "403: forbidden"}]);
});

test("stop releases the stream and timers without reporting a revocation", () => {
  const {live, scheduler, events, stream} = setup();
  live.start();
  stream().onopen?.();
  live.stop();
  expect(live.state).toBe("stopped");
  expect(stream().closed).toBe(true);
  expect(scheduler.pendingTimers).toBe(0);
  expect(events.revocations).toHaveLength(0);
  stream().emit("resync", {lastEventId: CURSOR});
  expect(events.resyncs).toHaveLength(0);
  // A stopped controller can be restarted; a revoked one cannot.
  live.start();
  expect(FakeEventSource.created).toHaveLength(2);
});

test("a rebind is rate limited so a busy project cannot cause a reconnect storm", () => {
  const {live, scheduler} = setup({rebindCooldownMs: 500});
  live.start();
  const opened = FakeEventSource.created.length;
  live.bindSnapshot(CURSOR);
  expect(FakeEventSource.created).toHaveLength(opened);
  scheduler.advance(500);
  expect(FakeEventSource.created).toHaveLength(opened + 1);
  expect(FakeEventSource.created[opened].url).toContain(`cursor=${CURSOR}`);
});
