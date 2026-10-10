import type {ExtensionAPI} from "@oh-my-pi/pi-coding-agent";
import {spawn} from "node:child_process";
import {readFileSync} from "node:fs";

/**
 * Explicitly loaded with `omp --extension <absolute-path>`.
 * Set HIVEMIND_CONFIG to a private JSON configuration and HIVEMIND_PYTHON to the venv Python.
 * This bridge never bypasses the host's ordinary approvals.
 */
export default function hivemind(pi: ExtensionAPI) {
  const config = process.env.HIVEMIND_CONFIG;
  const python = process.env.HIVEMIND_PYTHON;
  if (!config || !python) throw new Error("Set HIVEMIND_CONFIG and HIVEMIND_PYTHON");
  const settings = JSON.parse(readFileSync(config, "utf8"));

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
