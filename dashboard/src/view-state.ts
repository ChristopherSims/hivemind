export type ViewState = {board: string; query: string; pages: number};

export const PROJECT_VIEWS = [
  "board", "conversations", "memories", "administration",
  "tasks", "coordination", "context", "operations", "privacy",
] as const;

export function isProjectView(tab: string): boolean {
  return (PROJECT_VIEWS as readonly string[]).includes(tab);
}

export function readViewState(fragment: string): ViewState {
  const params = new URLSearchParams(fragment.replace(/^#/, ""));
  const board = params.get("board") ?? "";
  const pageValue = params.get("pages") ?? "1";
  const pages = /^\d{1,3}$/.test(pageValue) ? Number(pageValue) : 1;
  return {
    board: board.length <= 128 ? board : "",
    query: (params.get("q") ?? "").slice(0, 256),
    pages: Math.max(1, Math.min(20, pages)),
  };
}

export function writeViewState(state: ViewState): string {
  const params = new URLSearchParams();
  if (state.board && state.board.length <= 128) params.set("board", state.board);
  if (state.query) params.set("q", state.query.slice(0, 256));
  const pages = Number.isFinite(state.pages) ? Math.max(1, Math.min(20, Math.trunc(state.pages))) : 1;
  if (pages > 1) params.set("pages", String(pages));
  return params.size ? `#${params}` : "";
}

export function viewPath(project: string, tab: string): string {
  const view = isProjectView(tab)
    ? tab : `agents/${encodeURIComponent(tab)}`;
  return `/projects/${encodeURIComponent(project)}/${view}`;
}
