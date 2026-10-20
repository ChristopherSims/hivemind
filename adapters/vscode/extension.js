"use strict";
const vscode = require("vscode");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const crypto = require("node:crypto");
const {spawn} = require("node:child_process");
let child;
let stopAcp;
let stalled = false;
let dropped = 0;
let workspaceRoots = [];
function inside(root, target) {
  const relative = path.relative(root, target);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}
function stop() {
  if (child) child.stdin.end(); // EOF lets the private capture spool flush; no process adoption.
}
function observe(document, observation) {
  if (!child || document.uri.scheme !== "file") return;
  if (stalled) { dropped += 1; return; }
  let file;
  try { file = fs.realpathSync(document.uri.fsPath); } catch (_) { return; }
  const root = workspaceRoots.find(value => inside(value, file));
  if (!root) return;
  const relative = path.relative(root, file).split(path.sep).join("/");
  if (relative.split("/").some(p => p.startsWith(".env") ||
      [".git", ".ssh", ".venv", "node_modules", ".hivemind-data", "dist"].includes(p))) return;
  const text = document.getText();
  const size = Buffer.byteLength(text, "utf8");
  const record = size > 1048576
    ? {type: "capture.gap_detected", data: {reason: "Editor buffer exceeds capture bound", dropped_observation_count: 1}}
    : {type: "editor.buffer_observed", data: {path: relative, observation,
        sha256: crypto.createHash("sha256").update(text).digest("hex"),
        byte_size: size, version: document.version}};
  stalled = !child.stdin.write(JSON.stringify(record) + "\n");
}
function activate(context) {
  stopAcp = require("./acp-extension").registerAcp(vscode, context);
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.startCapture", async () => {
    if (!vscode.workspace.isTrusted || child) return;
    const selected = await vscode.window.showOpenDialog({
      canSelectMany: false, openLabel: "Select private Hivemind capture descriptor"});
    if (!selected || selected[0].scheme !== "file") return;
    try {
      workspaceRoots = (vscode.workspace.workspaceFolders || []).map(f => fs.realpathSync(f.uri.fsPath));
      if (workspaceRoots.length !== 1) throw Error("Capture requires exactly one trusted workspace");
      const configPath = fs.realpathSync(selected[0].fsPath);
      if (workspaceRoots.some(root => inside(root, configPath))) throw Error("Descriptor must be outside the workspace");
      if (fs.statSync(configPath).size > 16384) throw Error("Descriptor is too large");
      const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
      if (!path.isAbsolute(config.workspace_root) ||
          fs.realpathSync(config.workspace_root) !== workspaceRoots[0])
        throw Error("Capture descriptor must bind the selected workspace");
      if (!path.isAbsolute(config.python_executable)) throw Error("An absolute operator Python is required");
      const python = fs.realpathSync(config.python_executable);
      if (workspaceRoots.some(root => inside(root, python))) throw Error("Python must be outside the workspace");
      const running = spawn(python, ["-m", "hivemind.editor_capture", configPath], {
        cwd: os.homedir(), windowsHide: true, shell: false, stdio: ["pipe", "ignore", "ignore"]});
      child = running;
      stalled = false;
      dropped = 0;
      running.stdin.on("error", () => { if (child === running) child = undefined; });
      running.stdin.on("drain", () => {
        stalled = false;
        if (dropped) {
          stalled = !running.stdin.write(JSON.stringify({type: "capture.gap_detected",
            data: {reason: "Editor capture backpressure", dropped_observation_count: dropped}}) + "\n");
          dropped = 0;
        }
      });
      const clear = () => { if (child === running) child = undefined; };
      running.on("error", clear);
      running.on("exit", clear);
    } catch (_) {
      vscode.window.showErrorMessage("Hivemind capture could not start. Check the private operator descriptor.");
    }
  }));
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.stopCapture", stop));
  context.subscriptions.push(vscode.workspace.onDidOpenTextDocument(d => observe(d, "opened")));
  context.subscriptions.push(vscode.workspace.onDidChangeTextDocument(e => observe(e.document, "changed")));
  context.subscriptions.push(vscode.workspace.onDidSaveTextDocument(d => observe(d, "saved")));
  context.subscriptions.push(vscode.workspace.onDidCloseTextDocument(d => observe(d, "closed")));
  context.subscriptions.push({dispose: stop});
}
module.exports = {activate, deactivate: () => { stop(); stopAcp?.(); }};
