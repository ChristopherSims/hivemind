export type AgentIdentityProps = {
  agent?: {id: string; display_name: string; base_color_token?: string; effective_role?: string};
  fallback: string;
};

const PALETTE = new Set(["blue", "teal", "violet", "coral", "green", "magenta"]);

export function agentAppearance(agent: AgentIdentityProps["agent"]): string {
  return agent?.effective_role === "nexus" ? "nexus"
    : agent?.base_color_token && PALETTE.has(agent.base_color_token) ? agent.base_color_token : "";
}

export function agentLabel(agent: AgentIdentityProps["agent"], fallback: string): string {
  return `${agent?.display_name ?? fallback}${agent?.effective_role === "nexus" ? ", Nexus, main coordinator" : ""}`;
}

export function AgentIdentity({agent, fallback}: AgentIdentityProps) {
  return <>
    {agent?.effective_role === "nexus" && <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="m3 6 4 4 5-6 5 6 4-4-2 13H5Z"/></svg>}
    {agent?.display_name ?? fallback}
    {agent?.effective_role === "nexus" && <small>Nexus</small>}
  </>;
}
