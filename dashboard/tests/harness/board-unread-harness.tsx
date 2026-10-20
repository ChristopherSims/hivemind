// Isolated mount for dashboard/tests/board-unread.spec.ts.
//
// Served by a local vite dev server so the shared-board unread contract can be
// verified before the dashboard owner wires it into dashboard/src/main.tsx.
// Every network call is stubbed with page.route in the spec; nothing here talks
// to a real Hivemind store, and no main.tsx/api.ts/style.css file is touched.
import React, {useEffect, useState} from "react";
import {createRoot} from "react-dom/client";
import {api} from "../../src/api";
import {
  BoardPostReadControl, BoardUnreadBadge, BoardUnreadToolbar, mergeSnapshotBoardUnread,
  useBoardUnread, type BoardUnreadItem, type BoardUnreadMarkResult,
} from "../../src/BoardUnread";
import "../../src/tokens.css";

type Board = {id: string; title: string};
type PostsPage = {items: BoardUnreadItem[]};

function Harness() {
  const params = new URLSearchParams(location.search);
  const [project, setProject] = useState(params.get("project") ?? "proj-1");
  const [board, setBoard] = useState(params.get("board") ?? "board-1");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [posts, setPosts] = useState<BoardUnreadItem[]>([]);
  const [boards, setBoards] = useState<Board[]>([]);
  const [marked, setMarked] = useState<string>("");
  const [postError, setPostError] = useState("");

  const controller = useBoardUnread({projectId: project, boardId: board, refreshVersion,
    onError: message => setPostError(message)});
  const badges = useBoardUnread({projectId: project, refreshVersion});
  const withBadges = mergeSnapshotBoardUnread(boards, badges.state);

  useEffect(() => {
    let disposed = false;
    setPosts([]); setBoards([]); setMarked(""); setPostError("");
    void (async () => {
      try {
        const page = await api<PostsPage>(`boards/${board}/posts?limit=100`);
        if (!disposed) setPosts(page.items ?? []);
      } catch (e) { if (!disposed) setPostError(String(e)); }
      try {
        const list = await api<Board[]>(`boards?project_id=${project}`);
        if (!disposed) setBoards(list ?? []);
      } catch (e) { if (!disposed) setPostError(String(e)); }
    })();
    return () => { disposed = true; };
  }, [project, board]);

  const decorated = controller.decorate(posts);
  const unreadIds = [...controller.unreadIds];

  return <main style={{padding: "18px", fontFamily: "system-ui", color: "#e8eef7", background: "#0c121c"}}>
    <h1 style={{fontSize: 15}}>Shared board unread</h1>
    <p data-testid="harness-scope" data-project={project} data-board={board}>
      {project} / {board}</p>
    <p data-testid="harness-unread-ids">{unreadIds.join(",")}</p>
    <p data-testid="harness-marked">{marked}</p>
    {postError && <p data-testid="harness-error" role="alert">{postError}</p>}

    <div data-testid="harness-boards">
      {withBadges.map(item => <div key={item.id} data-testid="harness-board" data-board-id={item.id}
        data-unread-count={item.unread_count === undefined ? "unknown" : String(item.unread_count)}
        data-truncated={item.unread_truncated ? "true" : "false"}>
        {item.title}<BoardUnreadBadge count={item.unread_count ?? 0}
          truncated={Boolean(item.unread_truncated)} available={item.unread_count !== undefined}
          boardTitle={item.title}/></div>)}
    </div>

    <BoardUnreadToolbar projectId={project} boardId={board} items={posts}
      controller={controller} onMarked={(result: BoardUnreadMarkResult) =>
        setMarked(`${result.marked_count}/${result.already_read_count}`)}/>

    <div data-testid="harness-posts">
      {decorated.map(post => <article key={post.id} data-testid="harness-post"
        data-post-id={post.id} data-unread={post.board_unread ? "true" : "false"}
        data-is-read={post.is_read ? "true" : "false"}>
        <span>{String((post.content as {text?: string} | undefined)?.text ?? post.id)}</span>
        <BoardPostReadControl boardId={board} postId={post.id} unread={Boolean(post.board_unread)}
          controller={controller}
          onMarked={(postId, result) => setMarked(`single:${postId}:${result.marked_count}`)}
          onError={message => setPostError(message)}/>
      </article>)}
    </div>

    <div style={{display: "flex", gap: 8, marginTop: 14}}>
      <button type="button" data-testid="simulate-resync"
        onClick={() => setRefreshVersion(value => value + 1)}>Simulate resync</button>
      <button type="button" data-testid="switch-project"
        onClick={() => setProject(project === "proj-1" ? "proj-2" : "proj-1")}>Switch project</button>
      <button type="button" data-testid="switch-board"
        onClick={() => setBoard(board === "board-1" ? "board-2" : "board-1")}>Switch board</button>
    </div>
  </main>;
}

createRoot(document.getElementById("root")!).render(<Harness/>);
