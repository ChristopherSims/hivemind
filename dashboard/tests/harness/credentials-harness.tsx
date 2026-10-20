/*
 * Isolated mount for dashboard/tests/credentials.spec.ts. Served by a local vite
 * dev server so the credential screen can be exercised before the owner wires it
 * into dashboard/src/Administration.tsx. Every network call is stubbed with
 * page.route; nothing here talks to a real Hivemind store.
 *
 * The harness reports callbacks by name only: it never renders a revealed token
 * into the shell, so screenshots and failure output cannot capture a secret.
 */
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {CredentialManagement, CredentialAgent} from "../../src/CredentialManagement";
import "../../src/tokens.css";
// The screen is designed to mount inside the Administration workspace, so the
// harness loads the real app stylesheet (shared .admin-*/.panel-heading rules).
import "../../src/style.css";

const agents: CredentialAgent[] = [
  {id: "agent-droid0001", display_name: "Droid worker"},
  {id: "agent-pi000002", display_name: "Oh my Pi worker"},
];

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [role, setRole] = useState(params.get("role") ?? "admin");
  const [rotation, setRotation] = useState("");
  const [revocation, setRevocation] = useState("");
  const [failures, setFailures] = useState<string[]>([]);
  return <>
    <main style={{padding: "20px"}}>
      <h1>Administration stays mounted</h1>
      <p data-testid="shell-marker">The surrounding administration workspace is still mounted.</p>
      <button type="button" onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>
        Switch project</button>
      <button type="button" onClick={() => setRole(role === "admin" ? "observer" : "admin")}>
        Switch role</button>
      <p data-testid="self-rotation">{rotation}</p>
      <p data-testid="self-revocation">{revocation}</p>
      <p data-testid="errors">{failures.join(" | ")}</p>
    </main>
    <CredentialManagement project={project} role={role} agents={agents}
      onSelfRotation={() => setRotation("received")}
      onSelfRevocation={() => setRevocation("received")}
      onError={message => setFailures(current => [...current, message.slice(0, 40)])}/>
  </>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
