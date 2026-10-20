import type {ExtensionAPI} from "@oh-my-pi/pi-coding-agent";
import type {CustomToolContext} from "@oh-my-pi/pi-coding-agent/extensibility/custom-tools/types";
import {spawn} from "node:child_process";
import {closeSync, openSync, readFileSync, renameSync, writeFileSync} from "node:fs";

/**
 * Explicitly loaded with `omp --extension <absolute-path>`.
 * Set HIVEMIND_CONFIG to a private JSON configuration and HIVEMIND_PYTHON to the venv Python.
 * This bridge never bypasses the host's ordinary approvals.
 */
export default async function hivemind(pi: ExtensionAPI) {
  const config = process.env.HIVEMIND_CONFIG;
  const python = process.env.HIVEMIND_PYTHON;
  if (!config || !python) throw new Error("Set HIVEMIND_CONFIG and HIVEMIND_PYTHON");
  const settings = JSON.parse(readFileSync(config, "utf8"));
  const binding = settings.worker_mcp;
  if (binding?.enabled === true && binding.host_owned !== true) {
    const {MCPManager} = await import("@oh-my-pi/pi-coding-agent/mcp");
    // An isolated manager, never discover(): no ambient project/user MCP commands.
    const manager = new MCPManager(process.cwd());
    try {
      await manager.connectServers({hivemind: {
        command: python, args: ["-m", "hivemind.worker_mcp"],
        env: {HIVEMIND_CONFIG: config},
      }}, {}, undefined, 20000);
      const tools = manager.getTools();
      if (tools.length !== binding.tools.length ||
          [...tools.map(tool => tool.name)].sort().join("\0") !==
          [...binding.tools].sort().join("\0")) {
        throw new Error("Worker MCP connection or catalog not verified");
      }
      for (const tool of tools) {
        pi.registerTool({
          name: tool.name, label: tool.label, description: tool.description,
          parameters: tool.parameters,
          execute: (id, params, signal, update, ctx) =>
            tool.execute(id, params, update, ctx as unknown as CustomToolContext, signal),
        });
      }
      pi.on("session_shutdown", async () => { await manager.disconnectAll(); });
      const temporary = `${binding.ready_file}.${binding.nonce}.tmp`;
      const fd = openSync(temporary, "wx", 0o600);
      try {
        writeFileSync(fd, JSON.stringify({nonce: binding.nonce, connected: true,
          server: "hivemind", tools: binding.tools}));
      } finally { closeSync(fd); }
      renameSync(temporary, binding.ready_file);
    } catch {
      await manager.disconnectAll();
      throw new Error("Hivemind worker MCP registration failed");
    }
  }

  async function bridge(payload: unknown): Promise<{additionalContext?: string}> {
    return await new Promise<{additionalContext?: string}>((resolve, reject) => {
      const child = spawn(python!, ["-m", "hivemind.hooks", "--config", config!], {
        stdio: ["pipe", "pipe", "ignore"], windowsHide: true,
      });
      let output = "";
      child.stdout.on("data", chunk => { if (output.length < 8192) output += chunk.toString(); });
      const timeout = setTimeout(() => { child.kill(); reject(new Error("Hivemind validation timed out")); }, 20000);
      child.on("error", () => { clearTimeout(timeout); reject(new Error("Hivemind bridge failed")); });
      child.on("close", code => {
        clearTimeout(timeout);
        if (code !== 0) reject(new Error("Hivemind denied the operation; inspect assignment and claim."));
        else {
          try { resolve(output.trim() ? JSON.parse(output) : {}); }
          catch { reject(new Error("Invalid Hivemind bridge output")); }
        }
      });
      child.stdin.end(JSON.stringify(payload));
    });
  }
  const session = (ctx: {sessionManager: {getSessionId(): string | undefined}}) =>
    ctx.sessionManager.getSessionId() ?? "default";

  pi.on("session_start", async (_event, ctx) => {
    await bridge({type: "session_start", session_id: session(ctx), cwd: ctx.cwd});
  });
  pi.on("tool_call", async (event, ctx) => {
    try {
      return await bridge({...event, session_id: session(ctx), cwd: ctx.cwd});
    } catch {
      // A declared file scope is mandatory for mutations. Shell/custom tools fail closed.
      return {block: true, reason: "Hivemind blocked this operation. Read the current assignment and obtain a valid work claim."};
    }
  });
  pi.on("tool_result", async (event, ctx) => {
    await bridge({...event, session_id: session(ctx), cwd: ctx.cwd});
  });
  pi.registerCommand("hivemind", {
    description: "Show the Hivemind project and runtime connection",
    handler: async (_args, ctx) => {
      ctx.ui.notify(`Hivemind project: ${settings.project_id}. Scoped edits require an active claim.`, "info");
    },
  });
}
