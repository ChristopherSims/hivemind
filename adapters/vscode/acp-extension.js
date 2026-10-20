"use strict";
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const crypto = require("node:crypto");
const {spawn} = require("node:child_process");
const {AcpEditorClient, hash} = require("./acp-client");
const inside = (root, target) => {
  const relative = path.relative(root, target);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
};
const excluded = relative => relative.split("/").some(part => part.startsWith(".env") ||
  [".git", ".ssh", ".venv", "node_modules", ".hivemind-data", ".hivemind-test", "dist"].includes(part));
function privatePath(value, root, missing = false) {
  if (typeof value !== "string" || !path.isAbsolute(value) || inside(root, value))
    throw Error("Private paths must be outside the workspace");
  let current = value;
  while (true) {
    let entry;
    try { entry = fs.lstatSync(current); } catch (error) {
      if (error.code !== "ENOENT" || !missing || current !== value) throw Error("Missing private path");
    }
    if (entry?.isSymbolicLink()) throw Error("Symlink path");
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return value;
}
function boundedJson(file, max = 16384) {
  if (!fs.statSync(file).isFile() || fs.statSync(file).size > max) throw Error("Invalid private JSON");
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
function environment(names = []) {
  if (!Array.isArray(names) || names.length > 32 || names.some(name =>
    typeof name !== "string" || !/^[A-Z_][A-Z0-9_]*$/.test(name) ||
    /^(LD_|DYLD_|PYTHON|NODE_OPTIONS)/.test(name))) throw Error("Unsafe environment");
  const values = {};
  for (const name of ["PATH", "SystemRoot", "SystemDrive", "TEMP", "TMP", "HOME", "USERPROFILE",
    "LANG", "LC_ALL", ...names]) if (process.env[name] !== undefined) values[name] = process.env[name];
  return values;
}
class CaptureChannel {
  constructor(child, closed) {
    this.child = child; this.closed = closed; this.pending = new Map(); this.buffer = ""; this.dead = false;
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", chunk => {
      try {
        this.buffer += chunk;
        if (Buffer.byteLength(this.buffer) > 32768) throw Error("Capture response bound");
        let newline;
        while ((newline = this.buffer.indexOf("\n")) >= 0) {
          const frame = JSON.parse(this.buffer.slice(0, newline));
          this.buffer = this.buffer.slice(newline + 1);
          const item = this.pending.get(frame.request_id);
          if (!item) throw Error("Unexpected capture reply");
          this.pending.delete(frame.request_id); clearTimeout(item.timer);
          if (frame.ok === true) item.resolve(); else item.reject(Error("Capture authorization refused"));
        }
      } catch (_) { this.close(); }
    });
    const failed = () => this.close();
    child.on("error", failed); child.on("exit", failed); child.stdin.on("error", failed);
  }
  call(operation, payload) {
    if (this.dead || this.pending.size >= 32) return Promise.reject(Error("Capture unavailable"));
    const request_id = crypto.randomUUID();
    const line = JSON.stringify({request_id, operation, payload}) + "\n";
    if (Buffer.byteLength(line) > 16384 || this.child.stdin.writableLength > 65536)
      return Promise.reject(Error("Capture bound exceeded"));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.close(), 30000);
      this.pending.set(request_id, {resolve, reject, timer});
      this.child.stdin.write(line);
    });
  }
  close() {
    if (this.dead) return;
    this.dead = true;
    for (const item of this.pending.values()) { clearTimeout(item.timer); item.reject(Error("Capture closed")); }
    this.pending.clear(); this.child.stdin.end();
    this.closed();
    const timer = setTimeout(() => this.child.kill(), 5000); timer.unref();
  }
}
function registerAcp(vscode, context) {
  let active, channel, panel, output = "", starting = false, generation = 0;
  const showError = () => vscode.window.showErrorMessage(
    "Hivemind ACP operation refused or interrupted. Check authorization and private session state; prompts are not replayed.");
  const stop = () => {
    generation++; active?.close(); active = undefined; channel?.close(); channel = undefined;
    panel?.dispose(); panel = undefined; output = "";
  };
  const safeDocument = async (absolute, root, capture, line, limit, existingDocument) => {
    if (!path.isAbsolute(absolute)) throw Error("ACP paths must be absolute");
    let current = absolute;
    while (inside(root, current)) {
      if (fs.lstatSync(current).isSymbolicLink()) throw Error("File symlink");
      if (current === root) break;
      current = path.dirname(current);
    }
    const file = fs.realpathSync(absolute);
    if (!inside(root, file) || !fs.statSync(file).isFile()) throw Error("Outside workspace");
    const relative = path.relative(root, file).split(path.sep).join("/");
    if (excluded(relative) || fs.statSync(file).size > 1048576) throw Error("Excluded or oversized file");
    const snapshot = existingDocument?.getText();
    await capture.call("permit", {path: relative});
    const document = existingDocument || await vscode.workspace.openTextDocument(vscode.Uri.file(file));
    const text = snapshot === undefined ? document.getText() : snapshot;
    if (Buffer.byteLength(text) > 1048576) throw Error("Buffer bound exceeded");
    let content = text;
    if (line !== undefined || limit !== undefined) {
      const start = (line || 1) - 1;
      content = text.split("\n").slice(start, limit === undefined ? undefined : start + limit).join("\n");
    }
    await capture.call("permit", {path: relative});
    return {path: relative, content};
  };
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.startAcp", async () => {
    if (starting || active || !vscode.workspace.isTrusted) return;
    starting = true; const ticket = ++generation;
    try {
      const folders = vscode.workspace.workspaceFolders || [];
      if (folders.length !== 1 || folders[0].uri.scheme !== "file") throw Error("One local workspace required");
      const root = fs.realpathSync(folders[0].uri.fsPath);
      const selected = await vscode.window.showOpenDialog({canSelectMany: false,
        openLabel: "Select private ACP operator descriptor"});
      if (!selected || ticket !== generation) return;
      if (selected[0].scheme !== "file") throw Error("Local descriptor required");
      const descriptor = privatePath(selected[0].fsPath, root);
      const config = boundedJson(descriptor);
      const allowed = ["capture_descriptor", "executable", "args", "session_state",
        "allow_execution", "environment_passthrough", "read_files"];
      if (!config || Object.keys(config).some(key => !allowed.includes(key)) ||
          config.allow_execution !== true || !Array.isArray(config.args) ||
          config.args.length > 64 || config.args.some(arg => typeof arg !== "string" ||
            Buffer.byteLength(arg) > 4096) ||
          (config.read_files !== undefined && typeof config.read_files !== "boolean"))
        throw Error("Invalid opt-in descriptor");
      const captureFile = privatePath(config.capture_descriptor, root);
      const captureConfig = boundedJson(captureFile);
      if (fs.realpathSync(captureConfig.workspace_root) !== root) throw Error("Workspace mismatch");
      privatePath(captureConfig.token_file, root);
      privatePath(captureConfig.spool, root, true);
      const python = fs.realpathSync(captureConfig.python_executable);
      const executable = fs.realpathSync(config.executable);
      if (inside(root, python) || inside(root, executable) ||
          !path.isAbsolute(config.executable) || !path.isAbsolute(captureConfig.python_executable))
        throw Error("Operator executable required");
      const statePath = privatePath(config.session_state, root, true);
      if ([descriptor, captureFile, captureConfig.token_file, captureConfig.spool].includes(statePath))
        throw Error("Session state must have a distinct private path");
      const binding = hash(JSON.stringify({root, executable, args: config.args,
        capture: captureFile, project: captureConfig.project_id, agent: captureConfig.agent_id,
        run: captureConfig.run_id, scopes: captureConfig.scope_ids, readFiles: config.read_files === true}));
      const state = fs.existsSync(statePath) ? boundedJson(statePath) : null;
      const captureChild = spawn(python, ["-m", "hivemind.editor_capture", captureFile, "--editor-rpc"],
        {cwd: os.homedir(), shell: false, windowsHide: true, env: environment(),
          stdio: ["pipe", "pipe", "ignore"]});
      const localChannel = new CaptureChannel(captureChild, () => {
        if (channel === localChannel) active?.close();
      });
      channel = localChannel;
      const client = new AcpEditorClient({binding, state, cwd: root, readFiles: config.read_files === true,
        launch: () => spawn(executable, config.args, {cwd: root, shell: false, windowsHide: true,
          env: environment(config.environment_passthrough), stdio: ["pipe", "pipe", "ignore"]}),
        capture: (operation, payload) => localChannel.call(operation, payload),
        persist: async state => {
          privatePath(statePath, root, true);
          const temp = statePath + "." + crypto.randomUUID() + ".tmp";
          fs.writeFileSync(temp, JSON.stringify(state), {flag: "wx", mode: 0o600});
          fs.renameSync(temp, statePath);
        },
        readFile: (file, line, limit) => safeDocument(file, root, localChannel, line, limit),
        showText: text => {
          if (active !== client || ticket !== generation || Buffer.byteLength(output + text) > 262144) return;
          output += text;
          if (!panel) panel = vscode.window.createWebviewPanel("hivemindAcp", "Hivemind ACP",
            vscode.ViewColumn.Beside, {enableScripts: false, localResourceRoots: []});
          const escaped = output.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
          panel.webview.html = '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'"><pre>' +
            escaped + "</pre>";
        }});
      active = client;
      await client.connect();
      if (ticket !== generation || !vscode.workspace.isTrusted) { client.close(); return; }
      vscode.window.showInformationMessage("Hivemind ACP connected. Captured metadata is reported evidence, not applied effects.");
    } catch (_) { if (ticket === generation) stop(); showError(); }
    finally { starting = false; }
  }));
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.promptAcp", async () => {
    const client = active, ticket = generation;
    if (!client || !vscode.workspace.isTrusted) return;
    const prompt = await vscode.window.showInputBox({prompt: "Send a prompt to the connected ACP agent (may use paid models)",
      ignoreFocusOut: true});
    if (!prompt || client !== active || ticket !== generation) return;
    try { await client.prompt(prompt); } catch (_) { showError(); }
  }));
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.reconnectAcp", async () => {
    if (!active || !vscode.workspace.isTrusted) return;
    try { await active.connect(); } catch (_) { showError(); }
  }));
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.cancelAcp", () => active?.cancel()));
  context.subscriptions.push(vscode.commands.registerCommand("hivemind.stopAcp", stop));
  context.subscriptions.push(vscode.workspace.onDidChangeWorkspaceFolders(stop));
  const observe = async (document, observation) => {
    const client = active, capture = channel, ticket = generation;
    if (!client?.state || client.peer?.dead || document.uri.scheme !== "file" ||
        !vscode.workspace.isTrusted) return;
    const relative = path.relative(client.cwd, document.uri.fsPath).split(path.sep).join("/");
    if (!inside(client.cwd, document.uri.fsPath) || excluded(relative) ||
        Buffer.byteLength(document.getText()) > 1048576) return;
    try {
      const file = await safeDocument(document.uri.fsPath, client.cwd, capture, undefined, undefined, document);
      if (active !== client || ticket !== generation) return;
      await client.observe("file_buffer_observed", {actor: "unattributed", editor_observation: observation,
        path: file.path, sha256: hash(file.content), byte_size: Buffer.byteLength(file.content)});
    } catch (_) {
      // Authorization or delivery failure cannot leave an apparently healthy live client.
      if (active === client && ticket === generation) client.close();
    }
  };
  context.subscriptions.push(vscode.workspace.onDidOpenTextDocument(d => observe(d, "opened")));
  context.subscriptions.push(vscode.workspace.onDidChangeTextDocument(e => observe(e.document, "changed")));
  context.subscriptions.push(vscode.workspace.onDidSaveTextDocument(d => observe(d, "saved")));
  context.subscriptions.push(vscode.workspace.onDidCloseTextDocument(d => observe(d, "closed")));
  context.subscriptions.push({dispose: stop});
  return stop;
}
module.exports = {registerAcp, CaptureChannel, privatePath, environment, inside, excluded};
