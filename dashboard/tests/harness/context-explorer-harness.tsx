// Isolated mount for dashboard/tests/context-explorer.spec.ts.
//
// Served by a local vite dev server so the context-explorer contract can be
// verified before the dashboard owner wires /projects/{id}/context into
// dashboard/src/main.tsx. Every network call is stubbed with page.route in the
// spec; nothing here talks to a real Hivemind store, and no
// main.tsx/api.ts/style.css file is touched.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {ContextExplorer} from "../../src/ContextExplorer";
import type {ExplorerAgent} from "../../src/context-explorer";
import "../../src/tokens.css";

const AGENTS: ExplorerAgent[] = [
  {id: "agent_explorer", display_name: "Explorer", base_color_token: "blue"},
  {id: "agent_reviewer", display_name: "Reviewer", base_color_token: "green"},
];

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [permissionKey, setPermissionKey] = useState("perm-1");
  const [error, setError] = useState("");
  const [opened, setOpened] = useState("");

  return <main style={{padding: "18px", fontFamily: "system-ui", background: "#0c121c",
    color: "#e8eef7", minHeight: "100vh"}}>
    <h1 style={{fontSize: 15}}>Context explorer</h1>
    <p data-testid="harness-scope" data-project={project} data-permission={permissionKey}>
      {project} / {permissionKey}</p>
    {error && <p role="alert" data-testid="harness-error">{error}</p>}
    {opened && <p data-testid="harness-opened">{opened}</p>}

    <ContextExplorer project={project} role="admin" agents={AGENTS}
      refreshVersion={refreshVersion} permissionKey={permissionKey}
      onError={message => setError(message)} onOpenAgent={id => setOpened(id)} />

    <div style={{display: "flex", gap: 8, marginTop: 14}}>
      <button type="button" data-testid="simulate-resync"
        onClick={() => setRefreshVersion(value => value + 1)}>Simulate resync</button>
      <button type="button" data-testid="rotate-permission"
        onClick={() => setPermissionKey(key => key === "perm-1" ? "perm-2" : "perm-1")}>
        Rotate permission</button>
      <button type="button" data-testid="switch-project"
        onClick={() => setProject(value => value === "proj-1" ? "proj-2" : "proj-1")}>
        Switch project</button>
    </div>
  </main>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
