// Isolated mount for dashboard/tests/conflict-control-console.spec.ts.
//
// Served by a local vite dev server (reserved port 5211) so the console can be
// verified before the dashboard owner wires /projects/{id}/coordination into
// main.tsx. Every network call is stubbed with page.route in the spec; nothing
// here talks to a real Hivemind store, and no shared dashboard file is touched.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {setAuthorizationHandler, type ApiError} from "../../src/api";
import {ConflictControlConsole} from "../../src/ConflictControlConsole";
import "../../src/tokens.css";

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [role, setRole] = useState(params.get("role") ?? "admin");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [permissionEpoch, setPermissionEpoch] = useState(0);
  const [error, setError] = useState("");
  const [opened, setOpened] = useState("");

  return <main style={{padding: "18px", fontFamily: "system-ui", color: "#e8eef7",
    background: "#0c121c"}}>
    <p data-testid="harness-scope" data-project={project} data-role={role}
      data-permission-epoch={permissionEpoch}>{project} / {role}</p>
    {error && <p data-testid="harness-error" role="alert">{error}</p>}
    {opened && <p data-testid="harness-opened">{opened}</p>}
    <ConflictControlConsole project={project} role={role}
      refreshVersion={refreshVersion} permissionKey={`${role}:${permissionEpoch}`}
      onError={message => setError(message)} onOpenAgent={id => setOpened(id)}/>
    <div style={{display: "flex", gap: 8, marginTop: 14}}>
      <button type="button" data-testid="simulate-refresh"
        onClick={() => setRefreshVersion(value => value + 1)}>Simulate live invalidation</button>
      <button type="button" data-testid="switch-project"
        onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>Switch project</button>
      <button type="button" data-testid="demote-role"
        onClick={() => setRole(role === "admin" ? "agent" : "admin")}>Toggle role</button>
      <button type="button" data-testid="revoke-permission"
        onClick={() => {
          setAuthorizationHandler((authError: ApiError) => {
            setError(`authorization: ${authError.status}`);
            setPermissionEpoch(value => value + 1);
          });
        }}>Arm revocation handler</button>
    </div>
  </main>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
