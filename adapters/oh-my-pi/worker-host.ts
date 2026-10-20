/**
 * Fixed private SDK host for provisioned workers. No ambient MCP/extensions.
 * No prompt is submitted here; Python owns the fresh-authority prompt boundary.
 */
import {readFileSync, writeFileSync, openSync, closeSync, renameSync} from "node:fs";
import {pathToFileURL} from "node:url";

const config = process.env.HIVEMIND_CONFIG;
if (!config) throw new Error("Private worker configuration required");
const settings = JSON.parse(readFileSync(config, "utf8"));
const binding = settings.worker_mcp;
if (!binding?.host_owned || !binding.sdk_root) throw new Error("Private SDK binding required");
const moduleAt = (path: string) => pathToFileURL(`${binding.sdk_root}/src/${path}`).href;
const {createAgentSession} = await import(moduleAt("sdk.ts"));
const {MCPManager} = await import(moduleAt("mcp/index.ts"));
const {SessionManager} = await import(moduleAt("session/session-manager.ts"));
const {runRpcMode} = await import(moduleAt("modes/rpc/rpc-mode.ts"));
const manager = new MCPManager(process.cwd());
try {
  await manager.connectServers({hivemind: {
    command: process.env.HIVEMIND_PYTHON, args: ["-m", "hivemind.worker_mcp"],
    env: {HIVEMIND_CONFIG: config},
  }}, {}, undefined, 20000);
  const tools = manager.getTools();
  if (tools.length !== binding.tools.length ||
      tools.map((tool: {name: string}) => tool.name).sort().join("\0") !==
      [...binding.tools].sort().join("\0")) throw new Error("Worker MCP catalog mismatch");
  const sessionManager = binding.session_file
    ? await SessionManager.open(binding.session_file, binding.session_dir, undefined, {throwIfMissing: true})
    : SessionManager.create(process.cwd(), binding.session_dir);
  const {session} = await createAgentSession({
    cwd: process.cwd(), sessionManager, modelPattern: binding.model,
    enableMCP: false, enableLsp: false, enableIrc: false, cacheWarming: false,
    skipPythonPreflight: true, hasUI: false, interactivePrompts: false,
    deferUsageReserveConfirmation: true,
    restrictToolNames: true, allowRestrictedCustomTools: true,
    toolNames: binding.native_tools.concat(binding.tools), customTools: tools,
    disableExtensionDiscovery: true, additionalExtensionPaths: [binding.extension],
    extensions: [async (pi: {on: (event: string, handler: () => Promise<void>) => void}) => {
      pi.on("session_shutdown", () => manager.disconnectAll());
    }],
    skills: [], rules: [], contextFiles: [], promptTemplates: [], slashCommands: [],
  });
  const temporary = `${binding.ready_file}.${binding.nonce}.tmp`;
  const fd = openSync(temporary, "wx", 0o600);
  try {
    writeFileSync(fd, JSON.stringify({nonce: binding.nonce, connected: true,
      server: "hivemind", tools: binding.tools}));
  } finally { closeSync(fd); }
  renameSync(temporary, binding.ready_file);
  await runRpcMode(session, {headless: true});
} finally {
  await manager.disconnectAll();
}
