/*
 * Isolated mount for dashboard/tests/optimistic-posts.spec.ts.
 *
 * Every network call is stubbed with page.route in the spec, so this harness
 * never talks to a real Hivemind store and never starts the fixture server. It
 * mirrors the main.tsx wiring: one `useOptimisticPosts` instance per
 * project/board, a canonical list that a snapshot refresh would rebuild, and
 * `VirtualTimeline` fed `items` + `keyOf`.
 */
import React, {useEffect, useMemo, useRef, useState} from "react";
import {createRoot} from "react-dom/client";
import {VirtualTimeline} from "../../src/VirtualTimeline";
import {OptimisticPost} from "../../src/OptimisticPost";
import {setAuthorizationHandler} from "../../src/api";
import {isOptimisticPost, useOptimisticPosts, type PostItem} from "../../src/useOptimisticPosts";
import "../../src/tokens.css";

export type HarnessState = {
  rows: Array<{kind: "canonical" | "optimistic"; id: string; key: string;
    phase?: string; client_post_id?: string; text?: string; ambiguous?: boolean}>;
  draft: string;
  pendingCount: number;
  failedCount: number;
  authCalls: number;
  project: string;
  board: string;
};

export type OptimisticHarnessControls = {
  send: (text: string, mentions?: string[]) => string | null;
  retry: (client_post_id: string) => void;
  cancel: (client_post_id: string) => void;
  setBoard: (board: string) => void;
  setSession: (key: string) => void;
  addCanonical: (post: PostItem) => void;
  clearCanonical: () => void;
  setDraft: (text: string) => void;
  state: () => HarnessState;
};

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [board, setBoard] = useState(params.get("board") ?? "board-1");
  const [session, setSession] = useState(params.get("session") ?? "session-1");
  const [canonical, setCanonical] = useState<PostItem[]>([]);
  const [draft, setDraft] = useState("");
  const authCalls = useRef(0);
  const lastSend = useRef<string | null>(null);
  // `?timeout=800` exercises the silent-request watchdog with a short window.
  const timeoutMs = Number(params.get("timeout") ?? "0") || undefined;

  useEffect(() => {
    setAuthorizationHandler(() => { authCalls.current += 1; });
    return () => setAuthorizationHandler(null);
  }, []);

  const posts = useOptimisticPosts<PostItem>({
    project, board, sessionKey: session, canonical,
    ...(timeoutMs !== undefined ? {timeoutMs} : {}),
    onConfirmed: (_post, info) => {
      // Mirrors main.tsx: only the send that is still current clears the draft.
      if (lastSend.current === info.client_post_id) setDraft("");
    },
  });

  const state = useRef<OptimisticHarnessControls["state"]>(() => ({
    rows: [], draft: "", pendingCount: 0, failedCount: 0, authCalls: 0, project: "", board: "",
  }));
  state.current = () => ({
    rows: posts.items.map(item => ({
      kind: isOptimisticPost(item) ? "optimistic" as const : "canonical" as const,
      id: item.id,
      key: posts.keyOf(item),
      ...(isOptimisticPost(item) ? {phase: item.phase, client_post_id: item.client_post_id,
        ...(item.error?.ambiguous !== undefined ? {ambiguous: item.error.ambiguous} : {})} : {}),
      text: item.content.text ?? item.content.segments
        ?.map(segment => segment.kind === "text" ? segment.text : `@${segment.agent_id}`).join(""),
    })),
    draft,
    pendingCount: posts.pendingCount,
    failedCount: posts.failedCount,
    authCalls: authCalls.current,
    project, board,
  });

  const controls: OptimisticHarnessControls = useMemo(() => ({
    send: (text, mentions = []) => {
      const segments = [
        ...mentions.map(agent_id => ({kind: "agent_mention" as const, agent_id})),
        ...(text ? [{kind: "text" as const, text}] : []),
      ];
      const client_post_id = posts.send({kind: "message", content: {segments}});
      lastSend.current = client_post_id;
      if (client_post_id) setDraft(text);
      return client_post_id;
    },
    retry: client_post_id => posts.retry(client_post_id),
    cancel: client_post_id => posts.cancel(client_post_id),
    setBoard: next => { setBoard(next); setCanonical([]); },
    setSession: next => setSession(next),
    addCanonical: post => setCanonical(current => [...current, post]),
    clearCanonical: () => setCanonical([]),
    setDraft,
    state: () => state.current(),
  }), [posts, project, board, draft]);

  useEffect(() => {
    (window as unknown as {optimisticHarness?: OptimisticHarnessControls}).optimisticHarness = controls;
    return () => { delete (window as unknown as {optimisticHarness?: OptimisticHarnessControls}).optimisticHarness; };
  }, [controls]);

  return <>
    <div className="harness-head">
      <span data-testid="board">board:{board}</span>
      <span data-testid="project">project:{project}</span>
      <span data-testid="session">session:{session}</span>
      <span data-testid="draft">draft:{draft}</span>
      <span data-testid="pending">pending:{posts.pendingCount}</span>
      <button type="button" onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>
        Switch project</button>
    </div>
    <VirtualTimeline
      className="harness-timeline"
      items={posts.items}
      getKey={posts.keyOf}
      ariaLabel="Optimistic harness timeline"
      estimatedItemHeight={120}
      empty={<div className="empty" role="status">No posts yet.</div>}
      renderItem={item => isOptimisticPost(item)
        ? <OptimisticPost row={item} onRetry={posts.retry} onCancel={posts.cancel}/>
        : <article className="canonical-post" data-canonical-id={item.id}>
          <p className="body">{item.content.text ?? item.content.segments?.map((segment, index) =>
            segment.kind === "text"
              ? <React.Fragment key={index}>{segment.text}</React.Fragment>
              : <React.Fragment key={index}>@{segment.agent_id} </React.Fragment>)}</p>
        </article>}
    />
  </>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
