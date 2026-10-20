// Isolated mount for dashboard/tests/operations-dashboard.spec.ts.
//
// Served by a local vite dev server so the operations summary contract can be
// verified before the dashboard owner wires it into dashboard/src/main.tsx.
// Every network call is stubbed with page.route in the spec; nothing here
// talks to a real Hivemind store, and no main.tsx/api.ts/style.css file is
// touched.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {OperationsDashboard} from "../../src/OperationsDashboard";
import "../../src/tokens.css";

const AGENTS = [
  {id: "agent-1", display_name: "Researcher", base_color_token: "amber", effective_role: "worker"},
  {id: "agent-2", display_name: "Reviewer", base_color_token: "teal", effective_role: "worker"},
];

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [role] = useState(params.get("role") ?? "admin");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [permissionKey, setPermissionKey] = useState("perm-1");
  const [error, setError] = useState("");
  const [opened, setOpened] = useState("");

  return <main style={{padding: 12, fontFamily: "system-ui", color: "#e8eef7", background: "#0c121c"}}>
    <p data-testid="harness-scope" data-project={project} data-role={role}
       data-permission={permissionKey}>{project} / {role} / {permissionKey}</p>
    {error && <p data-testid="harness-error" role="alert">{error}</p>}
    {opened && <p data-testid="harness-opened">{opened}</p>}
    <OperationsDashboard project={project} role={role} agents={AGENTS}
      refreshVersion={refreshVersion} permissionKey={permissionKey}
      onError={message => setError(message)} onOpenAgent={id => setOpened(id)}/>
    <div style={{display: "flex", gap: 8, marginTop: 14}}>
      <button type="button" data-testid="simulate-resync"
        onClick={() => setRefreshVersion(value => value + 1)}>Simulate resync</button>
      <button type="button" data-testid="switch-project"
        onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>Switch project</button>
      <button type="button" data-testid="rotate-permission"
        onClick={() => setPermissionKey(key => key === "perm-1" ? "perm-2" : "perm-1")}>
        Rotate permission</button>
    </div>
  </main>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
