"use strict";
const crypto = require("node:crypto");
const {TextDecoder} = require("node:util");
const hash = value => crypto.createHash("sha256").update(value).digest("hex");

class RpcPeer {
  constructor(child, {notification = async () => {}, request = async () => {
    throw Error("Unsupported request");
  }, closed = () => {}, maxFrame = 1048576, timeout = 120000} = {}) {
    this.child = child; this.options = {notification, request, closed, maxFrame, timeout};
    this.pending = new Map(); this.buffer = Buffer.alloc(0); this.dead = false;
    this.queue = Promise.resolve(); this.queued = 0;
    child.stdout.on("data", chunk => {
      try { this.receive(chunk); } catch (_) { this.fail(); }
    });
    child.on("error", () => this.fail());
    child.on("exit", () => this.fail());
    child.stdin.on("error", () => this.fail());
  }
  send(frame) {
    if (this.dead) throw Error("Connection closed");
    const bytes = Buffer.from(JSON.stringify(frame) + "\n");
    if (bytes.length > this.options.maxFrame || this.child.stdin.writableLength > 1048576)
      throw Error("Transport bound exceeded");
    this.child.stdin.write(bytes);
  }
  call(method, params) {
    if (this.pending.size >= 32 || this.dead) return Promise.reject(Error("Connection unavailable"));
    const id = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.fail(); }, this.options.timeout);
      this.pending.set(id, {resolve, reject, timer});
      try { this.send({jsonrpc: "2.0", id, method, params}); } catch (_) { this.fail(); }
    });
  }
  receive(chunk) {
    if (this.dead) return;
    if (this.buffer.length + chunk.length > this.options.maxFrame * 2) throw Error("Oversized frame");
    this.buffer = Buffer.concat([this.buffer, chunk]);
    let newline;
    while ((newline = this.buffer.indexOf(10)) >= 0) {
      if (newline + 1 > this.options.maxFrame) throw Error("Oversized frame");
      const frame = JSON.parse(new TextDecoder("utf-8", {fatal: true})
        .decode(this.buffer.subarray(0, newline)));
      this.buffer = this.buffer.subarray(newline + 1);
      if (!frame || frame.jsonrpc !== "2.0") throw Error("Invalid JSON-RPC");
      if (typeof frame.method !== "string") {
        const pending = this.pending.get(frame.id);
        if (!pending || ("result" in frame) === ("error" in frame)) throw Error("Unexpected response");
        this.pending.delete(frame.id); clearTimeout(pending.timer);
        if ("error" in frame) pending.reject(Error("Agent request failed"));
        else pending.resolve(frame.result);
      } else {
        if (++this.queued > 128) throw Error("Notification queue exceeded");
        this.queue = this.queue.then(async () => {
          if (this.dead) return;
          if ("id" in frame) {
            if (!["string", "number"].includes(typeof frame.id)) throw Error("Invalid request ID");
            try {
              const result = await this.options.request(frame.method, frame.params);
              this.send({jsonrpc: "2.0", id: frame.id, result});
            } catch (_) {
              this.send({jsonrpc: "2.0", id: frame.id,
                error: {code: -32000, message: "Editor request denied"}});
            }
          } else await this.options.notification(frame.method, frame.params);
        }).catch(() => this.fail()).finally(() => { this.queued--; });
      }
    }
    if (this.buffer.length > this.options.maxFrame) throw Error("Oversized frame");
  }
  fail() {
    if (this.dead) return;
    this.dead = true;
    for (const item of this.pending.values()) {
      clearTimeout(item.timer); item.reject(Error("Connection interrupted"));
    }
    this.pending.clear();
    this.child.kill();
    this.options.closed();
  }
}

class AcpEditorClient {
  constructor({launch, capture, persist, readFile, showText = () => {}, state,
    binding, cwd, readFiles = false}) {
    if (state && (state.schema !== 1 || state.binding !== binding ||
        typeof state.sessionId !== "string" || !state.sessionId || state.sessionId.length > 256 ||
        !["idle", "running", "unknown"].includes(state.turnState)))
      throw Error("Session state binding mismatch");
    this.services = {launch, capture, persist, readFile, showText};
    this.binding = binding; this.cwd = cwd; this.readFiles = readFiles;
    this.state = state ? {...state} : null;
    if (this.state?.turnState === "running") this.state.turnState = "unknown";
    this.peer = null; this.connecting = false; this.prompting = false;
    this.disconnecting = Promise.resolve(); this.connection = null; this.delivery = "live";
    this.seenTools = new Map();
  }
  async save() { await this.services.persist({...this.state}); }
  async observe(observation, extra = {}) {
    await this.services.capture("observe", {type: "editor.acp_observed", data: {
      session_sha256: hash(this.state.sessionId), connection_id: this.connection,
      delivery: this.delivery, observation, ...extra}});
  }
  async connect() {
    if (this.connecting || (this.peer && !this.peer.dead)) throw Error("Already connected");
    this.connecting = true;
    try {
      await this.services.capture("authorize", {});
      await this.disconnecting;
      this.connection = "acp_" + crypto.randomUUID().replaceAll("-", "");
      this.seenTools.clear();
      const peer = new RpcPeer(this.services.launch(), {
        notification: (method, params) => {
          if (this.peer !== peer) throw Error("Superseded connection");
          return this.update(method, params);
        },
        request: (method, params) => {
          if (this.peer !== peer) throw Error("Superseded connection");
          return this.request(method, params);
        },
        closed: () => { this.disconnecting = this.disconnected(peer).catch(() => {}); }});
      this.peer = peer;
      const initialized = await peer.call("initialize", {protocolVersion: 1,
        clientInfo: {name: "hivemind-vscode", version: "0.2.0"},
        clientCapabilities: {fs: {readTextFile: this.readFiles, writeTextFile: false},
          terminal: false}});
      if (initialized?.protocolVersion !== 1) throw Error("Unsupported ACP version");
      if (this.state) {
        if (initialized.agentCapabilities?.loadSession !== true) throw Error("Session load unavailable");
        await this.save(); // Persist uncertain prior turn before reattaching.
        this.delivery = "replay";
        await peer.call("session/load", {sessionId: this.state.sessionId, cwd: this.cwd, mcpServers: []});
        await peer.queue;
        if (peer.dead) throw Error("Replay interrupted");
        await this.observe("session_loaded");
        this.delivery = "live";
      } else {
        const created = await peer.call("session/new", {cwd: this.cwd, mcpServers: []});
        if (typeof created?.sessionId !== "string" || !created.sessionId || created.sessionId.length > 256)
          throw Error("Invalid session identity");
        this.state = {schema: 1, binding: this.binding, sessionId: created.sessionId, turnState: "idle"};
        await this.save();
        await this.observe("session_created");
      }
    } catch (error) {
      this.peer?.fail();
      throw error;
    } finally { this.connecting = false; }
  }
  async prompt(text) {
    if (!this.peer || this.peer.dead || this.connecting || this.prompting || this.state?.turnState !== "idle")
      throw Error("Session is not ready; uncertain turns require reconciliation");
    if (typeof text !== "string" || !text.trim() || Buffer.byteLength(text) > 262144)
      throw Error("Invalid prompt");
    this.prompting = true;
    let started = false;
    try {
      await this.services.capture("authorize", {});
      started = true;
      this.state.turnState = "running";
      await this.save(); // Write uncertainty fence BEFORE any model execution.
      await this.observe("turn_started");
      const result = await this.peer.call("session/prompt", {sessionId: this.state.sessionId,
        prompt: [{type: "text", text}]});
      await this.peer.queue;
      if (this.peer.dead || !["end_turn", "max_tokens", "max_turn_requests", "refusal", "cancelled"]
        .includes(result?.stopReason)) throw Error("Uncertain turn");
      await this.observe("turn_completed"); // Turn stopped, NOT canonical run/task completion.
      this.state.turnState = "idle"; await this.save();
    } catch (error) {
      try {
        if (started) {
          this.state.turnState = "unknown"; await this.save();
        }
      } finally {
        this.peer.fail();
      }
      throw error;
    } finally { this.prompting = false; }
  }
  cancel() {
    if (this.state?.turnState === "running" && this.peer && !this.peer.dead)
      this.peer.send({jsonrpc: "2.0", method: "session/cancel", params: {sessionId: this.state.sessionId}});
  }
  async disconnected(peer) {
    if (!this.state || this.peer !== peer) return;
    if (this.state.turnState === "running") { this.state.turnState = "unknown"; await this.save(); }
    await this.observe("session_disconnected");
  }
  async update(method, params) {
    if (method !== "session/update") return; // Extension notifications confer no authority.
    if (!this.state || params?.sessionId !== this.state.sessionId) throw Error("Wrong-session update");
    const update = params.update;
    if (!update || typeof update.sessionUpdate !== "string") throw Error("Invalid update");
    if (["tool_call", "tool_call_update"].includes(update.sessionUpdate)) {
      if (typeof update.toolCallId !== "string" || !update.toolCallId || update.toolCallId.length > 512)
        throw Error("Invalid tool identity");
      const status = update.status || (update.sessionUpdate === "tool_call" ? "pending" : null);
      if (status === null) return;
      if (!["pending", "in_progress", "completed", "failed"].includes(status)) throw Error("Invalid tool status");
      const key = hash(update.toolCallId);
      if (this.seenTools.get(key) === status) return;
      if (this.seenTools.size >= 2048 && !this.seenTools.has(key)) throw Error("Tool bound exceeded");
      await this.observe("tool_" + status, {item_sha256: key});
      this.seenTools.set(key, status);
    } else if (update.sessionUpdate === "plan") {
      if (!Array.isArray(update.entries) || update.entries.length > 128) throw Error("Invalid plan");
      const entries = update.entries.map(entry => {
        if (typeof entry.content !== "string" || Buffer.byteLength(entry.content) > 8192 ||
            !["pending", "in_progress", "completed"].includes(entry.status) ||
            !["high", "medium", "low"].includes(entry.priority)) throw Error("Invalid plan entry");
        return {content_sha256: hash(entry.content), priority: entry.priority, status: entry.status};
      });
      await this.observe("plan", {entries});
    } else if (update.sessionUpdate === "agent_message_chunk" && this.delivery === "live" &&
        update.content?.type === "text" && typeof update.content.text === "string") {
      this.services.showText(update.content.text); // Ephemeral editor UI, never capture/spool.
    }
  }
  async request(method, params) {
    if (params?.sessionId !== this.state?.sessionId || this.delivery !== "live")
      throw Error("Unauthorized session");
    if (method === "session/request_permission")
      return {outcome: {outcome: "cancelled"}}; // No blanket grants or direct-agent tool approval.
    if (method !== "fs/read_text_file" || !this.readFiles) throw Error("Unsupported client capability");
    if (typeof params.path !== "string" || (params.line !== undefined &&
        (!Number.isSafeInteger(params.line) || params.line < 1)) ||
        (params.limit !== undefined && (!Number.isSafeInteger(params.limit) || params.limit < 1)))
      throw Error("Invalid read request");
    const file = await this.services.readFile(params.path, params.line, params.limit);
    await this.observe("file_read", {actor: "editor_observed", path: file.path, sha256: hash(file.content),
      byte_size: Buffer.byteLength(file.content)});
    return {content: file.content};
  }
  close() { this.peer?.fail(); }
}
module.exports = {RpcPeer, AcpEditorClient, hash};
