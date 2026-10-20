"use strict";
// Run with VS Code --extensionTestsPath. Real workspace/editor APIs, synthetic ACP/API.
const vscode = require("vscode");
const assert = require("node:assert/strict");
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const {registerAcp} = require("../acp-extension");

async function run() {
  const root = vscode.workspace.workspaceFolders[0].uri.fsPath;
  const privateRoot = fs.mkdtempSync(path.join(os.tmpdir(), "hivemind-acp-host-"));
  const captured = [], errors = [], subscriptions = [];
  const server = http.createServer((request, response) => {
    if (request.headers.authorization !== "Bearer synthetic-editor-token") {
      response.writeHead(403); response.end(); return;
    }
    let body = "";
    request.on("data", chunk => { body += chunk; });
    request.on("end", () => {
      let data;
      if (request.url === "/v1/runs/run_1")
        data = {project_id: "project_1", agent_id: "agent_1", status: "running"};
      else if (request.url === "/v1/runs/run_1/permits:check")
        data = {run_id: "run_1", scope_ids: ["scope_1"], expires_at: Date.now() / 1000 + 10};
      else if (request.url === "/v1/events:batch") {
        const events = JSON.parse(body).events; captured.push(...events);
        data = events.map(event => ({event_id: event.event_id, accepted: true, retryable: false}));
      } else if (request.url === "/v1/runs/run_1/heartbeat") data = {};
      else { response.writeHead(404); response.end(); return; }
      response.setHeader("Content-Type", "application/json");
      response.end(JSON.stringify({data}));
    });
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  let stop;
  try {
    const token = path.join(privateRoot, "token");
    fs.writeFileSync(token, "synthetic-editor-token", {mode: 0o600});
    const capture = path.join(privateRoot, "capture.json");
    fs.writeFileSync(capture, JSON.stringify({
      base_url: "http://127.0.0.1:" + server.address().port, token_file: token,
      project_id: "project_1", agent_id: "agent_1", run_id: "run_1",
      scope_ids: ["scope_1"], spool: path.join(privateRoot, "spool.sqlite"),
      workspace_root: root, python_executable: process.env.HIVEMIND_TEST_PYTHON}));
    const descriptor = path.join(privateRoot, "acp.json");
    fs.writeFileSync(descriptor, JSON.stringify({capture_descriptor: capture,
      executable: process.env.HIVEMIND_TEST_NODE, args: [path.join(__dirname, "fixture-agent.js")],
      session_state: path.join(privateRoot, "session.json"), allow_execution: true, read_files: true}));
    // Only picker/input/error UI is substituted. Commands, documents, edits and events are real VS Code.
    const api = {...vscode, window: {...vscode.window,
      showOpenDialog: async () => [vscode.Uri.file(descriptor)],
      showInputBox: async () => "synthetic editor prompt",
      showInformationMessage: () => {},
      showErrorMessage: message => errors.push(message)},
      commands: {registerCommand: (name, callback) =>
        vscode.commands.registerCommand("test." + name, callback)}};
    stop = registerAcp(api, {subscriptions});
    await vscode.commands.executeCommand("test.hivemind.startAcp");
    assert.deepEqual(errors, []);
    await vscode.commands.executeCommand("test.hivemind.promptAcp");
    assert.deepEqual(errors, []);
    const file = path.join(root, "sample.txt");
    fs.writeFileSync(file, "synthetic initial");
    const document = await vscode.workspace.openTextDocument(vscode.Uri.file(file));
    const edit = new vscode.WorkspaceEdit();
    edit.insert(document.uri, new vscode.Position(0, 0), "changed ");
    assert.equal(await vscode.workspace.applyEdit(edit), true);
    await document.save();
    const until = Date.now() + 10000;
    while (!captured.some(event => event.data.observation === "file_buffer_observed" &&
      event.data.editor_observation === "saved") && Date.now() < until)
      await new Promise(resolve => setTimeout(resolve, 50));
    assert.equal(captured.some(event => event.data.observation === "session_created"), true);
    assert.equal(captured.some(event => event.data.observation === "tool_completed"), true);
    assert.equal(captured.some(event => event.data.observation === "plan"), true);
    assert.equal(captured.some(event => event.data.observation === "file_buffer_observed" &&
      event.data.actor === "unattributed"), true);
    assert.equal(captured.every(event => event.source.attribution === "reported"), true);
    assert.equal(JSON.stringify(captured).includes("synthetic private plan text"), false);
    console.log("Real VS Code extension-host ACP/session/tool/plan/file integration passed");
  } finally {
    stop?.();
    for (const subscription of subscriptions) subscription.dispose?.();
    await new Promise(resolve => setTimeout(resolve, 700));
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    // The private fixture directory is intentionally retained until the owned capture process flushes.
  }
}
module.exports = {run: async () => {
  const marker = path.join(path.dirname(vscode.workspace.workspaceFolders[0].uri.fsPath), "result.json");
  try {
    await run();
    fs.writeFileSync(marker, JSON.stringify({passed: true}));
  } catch (error) {
    fs.writeFileSync(marker, JSON.stringify({passed: false, reason: String(error)}));
    throw error;
  }
}};
