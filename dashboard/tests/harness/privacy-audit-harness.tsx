// Isolated mount for dashboard/tests/privacy-audit.spec.ts, served by a local
// vite dev server on the reserved port 5214. Every network call is stubbed with
// page.route in the spec; nothing here talks to a real Hivemind store.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {PrivacyAuditScreens} from "../../src/PrivacyAuditScreens";
import "../../src/tokens.css";

const AGENTS = [
  {id: "agent_alpha00001", display_name: "Alpha worker", base_color_token: "teal",
   effective_role: "worker"},
  {id: "agent_beta000002", display_name: "Beta worker", base_color_token: "violet",
   effective_role: "worker"},
];

function Harness() {
  const [project, setProject] = useState("project-1");
  const [role, setRole] = useState("human");
  const [permissionKey, setPermissionKey] = useState("session-a");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [mounted, setMounted] = useState(true);
  const [log, setLog] = useState<string[]>([]);

  return (
    <main style={{padding: "24px"}}>
      <h1>Privacy &amp; audit harness</h1>
      <p data-testid="log">{log.join(",")}</p>
      <div style={{display: "flex", gap: "8px", marginBottom: "12px"}}>
        <button type="button" data-testid="switch-project"
          onClick={() => setProject(current => current === "project-1" ? "project-2" : "project-1")}>
          Switch project
        </button>
        <button type="button" data-testid="switch-role"
          onClick={() => setRole(current => current === "human" ? "agent" : "human")}>
          Switch role
        </button>
        <button type="button" data-testid="switch-session"
          onClick={() => setPermissionKey(current =>
            current === "session-a" ? "session-b" : "session-a")}>
          Switch session
        </button>
        <button type="button" data-testid="bump-refresh"
          onClick={() => setRefreshVersion(current => current + 1)}>
          Refresh
        </button>
        <button type="button" data-testid="toggle-mount"
          onClick={() => setMounted(current => !current)}>
          {mounted ? "Unmount screens" : "Mount screens"}
        </button>
      </div>
      {mounted && <PrivacyAuditScreens
        project={project}
        role={role}
        agents={AGENTS}
        permissionKey={permissionKey}
        refreshVersion={refreshVersion}
        onError={message => setLog(current => [...current, `error:${message}`])}
        onOpenAgent={id => setLog(current => [...current, `open:${id}`])}
      />}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<Harness/>);
