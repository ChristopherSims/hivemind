import {api, ApiError} from "./api";
import {normalizeSnapshotCursor} from "./live-state";

export type TimelineItem = {id: string; created_seq: number};
export type TimelinePage<T> = {
  items: T[]; next_before: number | null;
  // Opaque, principal/project-bound snapshot hash (64-hex). Optional so a server
  // that predates the live-state binding keeps working; see the migration
  // contract below.
  snapshot_cursor?: string | null;
};
export type TimelineLoad<T> = TimelinePage<T> & {
  page_count: number;
  // Cursor of the newest page - the snapshot this timeline represents - or null
  // when the server supplied no valid cursor. Bind it with
  // `LiveStateController.bindSnapshot` so the invalidation stream is bound to
  // the snapshot actually displayed.
  snapshot_cursor: string | null;
  // True when an older page reported a cursor that differs from the newest
  // page: a write landed while paging, so the loaded window is not one single
  // snapshot. Every page is still individually authorized; callers may render
  // it but must refresh before treating the window as a consistent snapshot.
  cursor_drifted: boolean;
};

/**
 * Migration contract for `snapshot_cursor`:
 *
 * - Servers return it on every page of `boards/{id}/posts`,
 *   `projects/{id}/conversations` and `agents/{id}/conversation` (they all
 *   report the same project-wide hash for one principal). A missing or
 *   malformed value is normalized to `null`, never coerced: the activity-stream
 *   replay cursor (`s<n>.a<n>`) must never be bound to the invalidation stream.
 * - Only the newest page's cursor is returned, because that is the snapshot the
 *   caller displays. It is project- and principal-bound, so it must not be
 *   cached across sessions, projects, or principals.
 * - When the server omits the cursor, callers fall back to refreshing on every
 *   `resync` frame (the pre-binding behavior). The controller reports this via
 *   `legacyUnbound`.
 */
export async function loadTimeline<T extends TimelineItem>(
  path: string, pages: number, oldest?: number,
): Promise<TimelineLoad<T>> {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const result = await loadPages<T>(path, pages, oldest);
      if (!result.cursor_drifted) return result;
      throw new ApiError(409, "Snapshot changed while loading.", "snapshot_changed");
    } catch (error) {
      if (!(error instanceof ApiError) || error.code !== "snapshot_changed" || attempt === 2) throw error;
    }
  }
  throw new ApiError(409, "Snapshot changed while loading.", "snapshot_changed");
}

async function loadPages<T extends TimelineItem>(path: string, pages: number, oldest?: number): Promise<TimelineLoad<T>> {
  let before: number | null = null;
  let items: T[] = [];
  let count = 0;
  let snapshotCursor: string | null = null;
  let cursorDrifted = false;
  do {
    const result: TimelinePage<T> = await api<TimelinePage<T>>(pageUrl(path, before, snapshotCursor));
    const cursor = normalizeSnapshotCursor(result.snapshot_cursor);
    if (count === 0) snapshotCursor = cursor;
    else if (snapshotCursor !== null && cursor !== snapshotCursor) cursorDrifted = true;
    items = [...result.items, ...items];
    before = result.next_before;
    count++;
  } while (before !== null && count < 20 && (count < pages || (oldest !== undefined && before > oldest)));
  return {items: [...new Map(items.map(item => [item.id, item])).values()],
    next_before: before, page_count: count,
    snapshot_cursor: snapshotCursor, cursor_drifted: cursorDrifted};
}

function pageUrl(path: string, before: number | null, cursor: string | null): string {
  if (before === null) return path;
  const [route, search] = path.split("?", 2);
  const params = new URLSearchParams(search);
  params.set("before", String(before));
  if (cursor) params.set("snapshot_cursor", cursor);
  return `${route}?${params}`;
}
