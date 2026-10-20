// Isolated mount for dashboard/tests/thread-pane.spec.ts.
// Served by a local vite dev server so the pane can be exercised before the
// owner wires it into dashboard/src/main.tsx. Network calls are stubbed with
// page.route in the spec; nothing here talks to a real Hivemind store.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {ThreadAgent, ThreadPane, ThreadPost} from "../../src/ThreadPane";
import "../../src/tokens.css";

const agents: ThreadAgent[] = [
  {id: "agent-droid0001", display_name: "Droid worker", base_color_token: "teal"},
  {id: "agent-pi000002", display_name: "Oh my Pi worker", base_color_token: "blue"},
];

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [open, setOpen] = useState(true);
  const [posted, setPosted] = useState<string[]>([]);
  const thread = `thread-${project.replace("proj-", "")}`;
  return <>
    <main style={{padding: "24px"}}>
      <h1>Timeline stays mounted</h1>
      <p data-testid="timeline-marker">The main timeline is still mounted while the thread pane is open.</p>
      <button type="button" onClick={() => setOpen(true)}>Open thread</button>
      <button type="button" onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>
        Switch project</button>
      <p data-testid="posted">{posted.join(",")}</p>
    </main>
    {open && <ThreadPane projectId={project} boardId={`board-${project}`} threadId={thread}
      selectedPostId={params.get("selected")} agents={agents}
      onClose={() => setOpen(false)}
      onPosted={(post: ThreadPost) => setPosted(current => [...current, post.id])}/>}
  </>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
