// Isolated mount for dashboard/tests/task-run-workspace.spec.ts.
// Served by a local vite dev server so the workspace can be exercised before
// the owner wires it into dashboard/src/main.tsx. Network calls are stubbed
// with page.route in the spec; nothing here talks to a real Hivemind store.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {TaskRunWorkspace} from "../../src/TaskRunWorkspace";
import "../../src/tokens.css";

const agents = [
  {id: "agent-droid0001", display_name: "Droid worker", base_color_token: "teal"},
  {id: "agent-pi000002", display_name: "Oh my Pi worker", base_color_token: "blue"},
];

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [role] = useState(params.get("role") ?? "admin");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [permissionKey, setPermissionKey] = useState("perm-a");
  const [errors, setErrors] = useState<string[]>([]);
  const [opened, setOpened] = useState<string[]>([]);
  return <>
    <main style={{padding: "8px"}}>
      <p data-testid="shell-marker">Workspace host shell stays mounted.</p>
      <button type="button" onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>
        Switch project</button>
      <button type="button" onClick={() => setRefreshVersion(version => version + 1)}>
        Push refresh</button>
      <button type="button"
        onClick={() => setPermissionKey(key => key === "perm-a" ? "perm-b" : "perm-a")}>
        Rotate permissions</button>
      <p data-testid="errors">{errors.join(" | ")}</p>
      <p data-testid="opened">{opened.join(",")}</p>
    </main>
    <TaskRunWorkspace project={project} role={role} agents={agents}
      refreshVersion={refreshVersion} permissionKey={permissionKey}
      onError={message => setErrors(current => [...current, message])}
      onOpenAgent={id => setOpened(current => [...current, id])}/>
  </>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
