"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const {spawn} = require("node:child_process");
const path = require("node:path");
const fs = require("node:fs");
const os = require("node:os");
const {AcpEditorClient, hash, RpcPeer} = require("../acp-client");
const {privatePath, environment, CaptureChannel} = require("../acp-extension");
const {EventEmitter} = require("node:events");
const {PassThrough} = require("node:stream");

function setup(t, options = {}) {
  const observations = [], saves = [], shown = [];
  let launches = 0;
  const client = new AcpEditorClient({
    binding: "synthetic-binding", cwd: os.tmpdir(), state: options.state,
    readFiles: options.readFiles === true,
    launch: () => { launches++; return spawn(process.execPath,
      [path.join(__dirname, "fixture-agent.js")], {stdio: ["pipe", "pipe", "ignore"]}); },
    capture: async (operation, payload) => {
      if (options.denied) throw Error("Denied");
      if (operation === "observe") observations.push(payload);
    },
    persist: async state => saves.push({...state}),
    readFile: async () => ({path: "test.txt", content: "synthetic"}),
    showText: text => shown.push(text),
  });
  t.after(async () => { client.close(); await client.disconnecting; });
  return {client, observations, saves, shown, launches: () => launches};
}

test("real stdio initializes, creates session and captures hashed tool/plan metadata", async t => {
  const {client, observations, saves, shown} = setup(t);
  await client.connect(); await client.prompt("synthetic prompt");
  assert.equal(client.state.turnState, "idle");
  assert.equal(saves.some(state => state.turnState === "running"), true);
  const tool = observations.filter(event => event.data.observation.startsWith("tool_"));
  assert.equal(tool.length, 2); // Exact repeated status deduplicated within the connection.
  assert.equal(tool[0].data.item_sha256, hash("synthetic-call"));
  const plan = observations.find(event => event.data.observation === "plan");
  assert.equal(plan.data.entries[0].content_sha256, hash("synthetic private plan text"));
  assert.equal(JSON.stringify(observations).includes("synthetic private plan text"), false);
  assert.equal(JSON.stringify(observations).includes("synthetic prompt"), false);
  assert.deepEqual(shown, ["Synthetic response"]);
});

test("authorization denial prevents process launch", async t => {
  const harness = setup(t, {denied: true});
  await assert.rejects(harness.client.connect());
  assert.equal(harness.launches(), 0);
});

test("disconnect fences unknown turns and reconnect loads without replaying prompts", async t => {
  const {client, observations} = setup(t);
  await client.connect();
  await assert.rejects(client.prompt("disconnect"));
  assert.equal(client.state.turnState, "unknown");
  await client.connect();
  assert.equal(client.state.turnState, "unknown");
  await assert.rejects(client.prompt("must not resend"));
  assert.equal(observations.some(event => event.data.delivery === "replay" &&
    event.data.observation === "tool_completed"), true);
  assert.equal(observations.filter(event => event.data.observation === "turn_started").length, 1);
});

test("persisted running state becomes unknown; binding spoof is refused", async t => {
  const state = {schema: 1, binding: "synthetic-binding",
    sessionId: "synthetic-session", turnState: "running"};
  const {client} = setup(t, {state});
  assert.equal(client.state.turnState, "unknown");
  await client.connect(); await assert.rejects(client.prompt("blocked"));
  assert.throws(() => new AcpEditorClient({state: {...state, binding: "foreign"}, binding: "actual"}));
});

test("concurrent prompt commands cannot pass the same idle fence", async t => {
  const {client, observations} = setup(t);
  await client.connect();
  const turn = client.prompt("wait");
  await assert.rejects(client.prompt("second prompt"));
  await new Promise(resolve => setTimeout(resolve, 30));
  client.cancel(); await turn;
  assert.equal(observations.filter(event => event.data.observation === "turn_started").length, 1);
});

test("failed idle-state persistence leaves an unknown fence and closes transport", async t => {
  const {client} = setup(t);
  await client.connect();
  const persist = client.services.persist;
  client.services.persist = async state => {
    if (state.turnState === "idle") throw Error("Synthetic storage failure");
    await persist(state);
  };
  await assert.rejects(client.prompt("synthetic"));
  assert.equal(client.state.turnState, "unknown");
  assert.equal(client.peer.dead, true);
});

test("wrong-session updates interrupt instead of acquiring attribution", async t => {
  const {client, observations} = setup(t);
  await client.connect(); await assert.rejects(client.prompt("wrong-session"));
  assert.equal(client.state.turnState, "unknown");
  assert.equal(observations.some(event => event.data.session_sha256 === hash("foreign")), false);
});

test("cancel waits for actual turn response; unsupported fs and terminal calls are denied", async t => {
  const {client} = setup(t);
  await client.connect();
  const turn = client.prompt("wait");
  await new Promise(resolve => setTimeout(resolve, 30));
  client.cancel(); await turn;
  assert.equal(client.state.turnState, "idle");
  await assert.rejects(client.request("terminal/create", {sessionId: client.state.sessionId}));
  await assert.rejects(client.request("fs/write_text_file", {sessionId: client.state.sessionId}));
  assert.deepEqual(await client.request("session/request_permission",
    {sessionId: client.state.sessionId}), {outcome: {outcome: "cancelled"}});
});

test("explicit read capability captures editor-observed hashes and refuses replay reads", async t => {
  const {client, observations} = setup(t, {readFiles: true});
  await client.connect();
  assert.deepEqual(await client.request("fs/read_text_file", {
    sessionId: client.state.sessionId, path: path.join(os.tmpdir(), "test.txt")}), {content: "synthetic"});
  assert.equal(observations.at(-1).data.actor, "editor_observed");
  client.delivery = "replay";
  await assert.rejects(client.request("fs/read_text_file",
    {sessionId: client.state.sessionId, path: "/test"}));
});

function fakeChild() {
  const child = new EventEmitter();
  child.stdin = new PassThrough(); child.stdout = new PassThrough();
  child.kill = () => { child.killed = true; };
  return child;
}
test("oversized and malformed RPC frames fail closed", () => {
  for (const frame of ["x".repeat(1025), '{"jsonrpc":"1.0"}\n', "{}\n"]) {
    const child = fakeChild();
    const peer = new RpcPeer(child, {maxFrame: 1024});
    child.stdout.write(frame);
    assert.equal(peer.dead, true); assert.equal(child.killed, true);
  }
});
test("capture rejects unacknowledged delivery and closes pending callers on EOF", async () => {
  const child = fakeChild();
  let closed = 0;
  const channel = new CaptureChannel(child, () => closed++);
  let wire = "";
  child.stdin.on("data", chunk => { wire += chunk; });
  const call = channel.call("authorize", {});
  const request = JSON.parse(wire);
  child.stdout.write(JSON.stringify({request_id: request.request_id, ok: false}) + "\n");
  await assert.rejects(call);
  const interrupted = channel.call("authorize", {});
  child.emit("exit");
  await assert.rejects(interrupted);
  assert.equal(closed, 1);
});
test("private path and environment defenses reject workspace and loader overrides", t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "hivemind-acp-test-"));
  t.after(() => fs.rmSync(root, {recursive: true}));
  const workspace = path.join(root, "workspace"); fs.mkdirSync(workspace);
  assert.throws(() => privatePath(path.join(workspace, "secret.json"), workspace, true));
  assert.throws(() => environment(["NODE_OPTIONS"]));
  assert.throws(() => environment(["PYTHONPATH"]));
  assert.equal(privatePath(path.join(root, "state.json"), workspace, true), path.join(root, "state.json"));
});
