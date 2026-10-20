// Isolated mount for dashboard/tests/agent-prefill.spec.ts.
// Served by a local vite dev server so the optional agent-tab prefill control
// can be exercised before the dashboard shell wires it into the shared-board
// composer. The host side (draft, chips, tab navigation) is emulated exactly
// like dashboard/src/main.tsx does it, and every network call is stubbed with
// page.route so no real Hivemind store is touched.
import React, {useState} from "react";
import {createRoot} from "react-dom/client";
import {AgentMentionPrefill} from "../../src/AgentMentionPrefill";
import type {MentionPrefillRequest} from "../../src/AgentMentionPrefill";
import "../../src/tokens.css";

const AGENT = {id: "agent-droid0001", display_name: "Droid worker", base_color_token: "teal"};

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [board, setBoard] = useState(params.get("board") ?? "board-1");
  const [viewKey, setViewKey] = useState(params.get("viewKey") ?? "agents/agent-droid0001");
  const [permission, setPermission] = useState(params.get("permission") ?? "revision-1");
  const [viewActive, setViewActive] = useState(params.get("viewActive") !== "false");
  const [draft, setDraft] = useState(params.get("draft") ?? "My own notes stay untouched");
  const [mentions, setMentions] = useState<MentionPrefillRequest[]>([]);
  const [mentionCalls, setMentionCalls] = useState(0);
  const [opened, setOpened] = useState<string[]>([]);
  const [posted, setPosted] = useState(false);

  // Mirrors main.tsx `choose`: dedupes by immutable id, never posts.
  function choose(person: MentionPrefillRequest) {
    setMentionCalls(count => count + 1);
    setMentions(current => current.some(agent => agent.id === person.id) ? current : [...current, person]);
  }

  // Mirrors main.tsx `openAgent`/`selectTab`: host-owned navigation only.
  function openAgent(id: string) {
    setOpened(current => [...current, id]);
    setViewActive(true);
    setViewKey(`agents/${id}`);
  }

  return <main style={{padding: "20px", fontFamily: "system-ui, sans-serif"}}>
    <h1>Agent mention prefill harness</h1>
    <p data-testid="context">{`${project} / ${board} / ${viewKey} / ${permission}`}</p>
    <div style={{display: "flex", flexWrap: "wrap", gap: "6px", marginBottom: "12px"}}>
      <button type="button" data-testid="switch-board"
        onClick={() => setBoard(board === "board-1" ? "board-2" : "board-1")}>Switch board</button>
      <button type="button" data-testid="switch-project"
        onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>Switch project</button>
      <button type="button" data-testid="switch-view"
        onClick={() => setViewKey(viewKey.endsWith("tab-b") ? `agents/${AGENT.id}` : `agents/${AGENT.id}:tab-b`)}>
        Switch view</button>
      <button type="button" data-testid="bump-permission"
        onClick={() => setPermission(current => `${current}-bump`)}>Raise permission revision</button>
      <button type="button" data-testid="toggle-view-active"
        onClick={() => setViewActive(current => !current)}>Toggle active view</button>
    </div>
    <form data-testid="composer" onSubmit={event => {event.preventDefault(); setPosted(true);}}>
      <label htmlFor="draft">Draft</label>
      <textarea id="draft" data-testid="draft" style={{display: "block", width: "420px", minHeight: "60px"}}
        value={draft} onChange={event => setDraft(event.target.value)} />
      <div data-testid="chips">
        {mentions.map(agent => <button type="button" key={agent.id} data-chip-id={agent.id}
          onClick={() => setMentions(current => current.filter(item => item.id !== agent.id))}>
          @{agent.display_name} ×</button>)}
      </div>
      <AgentMentionPrefill
        agent={AGENT}
        project={project}
        board={board}
        viewKey={viewKey}
        permissionKey={permission}
        existingMentionIds={mentions.map(agent => agent.id)}
        onMention={choose}
        onOpenAgent={openAgent}
        viewActive={viewActive}
        disabled={params.get("disabled") === "true"} />
      <button type="submit" data-testid="post">Post to board</button>
    </form>
    <p data-testid="mention-calls">{mentionCalls}</p>
    <p data-testid="opened">{opened.join(",")}</p>
    <p data-testid="posted">{posted ? "posted" : "not posted"}</p>
  </main>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
