// Isolated mount for dashboard/tests/mention-feedback.spec.ts, served by a local
// vite dev server so the guard, the composer and the receipt feedback can be
// exercised exactly the way main.tsx / ThreadPane.tsx are expected to wire them
// -- before those shared hosts are touched. Every network call is stubbed with
// page.route; nothing here talks to a real Hivemind store.
import React, {useRef, useState} from "react";
import {createRoot} from "react-dom/client";
import {api} from "../../src/api";
import {MentionComposer} from "../../src/MentionComposer";
import {MentionSendGuard} from "../../src/MentionFeedback";
import {useMentionSendGuard, type MentionSelectionLike} from "../../src/mention-feedback";
import "../../src/tokens.css";

type Post = {id: string};
type Segment = {kind: "text"; text: string} | {kind: "agent_mention"; agent_id: string};

function Harness() {
  const [draft, setDraft] = useState("");
  const [mentions, setMentions] = useState<MentionSelectionLike[]>([]);
  const [sending, setSending] = useState(false);
  const [context, setContext] = useState("project-1:board-1");
  const [log, setLog] = useState<string[]>([]);
  const postKey = useRef<string | null>(null);
  const guard = useMentionSendGuard({value: draft, selections: mentions, contextKey: context});
  // ?inline mounts only the composer's own fallback warning, without the footer guard.
  const inlineOnly = new URLSearchParams(location.search).has("inline");

  async function send(event: React.FormEvent) {
    event.preventDefault();
    // The gate: unresolved @text waits for the author's explicit confirmation.
    if (!guard.trySend()) return;
    setSending(true);
    const clientKey = (postKey.current ??= crypto.randomUUID());
    const segments: Segment[] = mentions.map(person => ({kind: "agent_mention", agent_id: person.id}));
    if (draft.trim()) segments.push({kind: "text", text: draft});
    try {
      const post = await api<Post>("boards/board-1/posts", "POST", {
        client_post_id: clientKey, kind: "message",
        content: {media_type: "application/vnd.hivemind.chat+json", segments},
      });
      setLog(current => [...current, `posted:${post.id}`]);
      setDraft(""); setMentions([]); postKey.current = null;
      // Optimistic pending, then the canonical receipt. Never a presence guess.
      guard.trackReceipts(post.id);
      guard.reset();
    } catch (error) {
      setLog(current => [...current, `error:${String(error)}`]);
    } finally {
      setSending(false);
    }
  }

  return (
    <main style={{padding: "24px", maxWidth: "720px"}}>
      <h1>Mention feedback harness</h1>
      <p data-testid="log">{log.join(",")}</p>
      <button type="button" data-testid="switch-context"
        onClick={() => setContext(current =>
          current === "project-1:board-1" ? "project-2:board-2" : "project-1:board-1")}>
        Switch project
      </button>
      <form aria-label="Board composer" onSubmit={send}>
        <div className="chips">
          {mentions.map(person => (
            <button type="button" key={person.id}
              onClick={() => setMentions(current => current.filter(item => item.id !== person.id))}>
              @{person.display_name} ×
            </button>
          ))}
        </div>
        <MentionComposer
          value={draft}
          onChange={next => { setDraft(next); postKey.current = null; }}
          onMention={person => {
            postKey.current = null;
            setMentions(current => current.some(item => item.id === person.id) ? current : [...current, person]);
          }}
          project="project-1"
          board="board-1"
          selections={mentions}
          showUnresolvedWarning={inlineOnly}
          disabled={sending}
          permissionKey="human"
          ariaLabel="Shared board message"
        />
        {!inlineOnly && <MentionSendGuard guard={guard} sending={sending} />}
        <div className="composer-footer">
          <button className="primary" disabled={sending || (!draft.trim() && !mentions.length)}>
            {sending ? "Posting…" : "Post to board"}
          </button>
        </div>
      </form>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<Harness/>);
