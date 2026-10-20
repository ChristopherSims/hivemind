import {test, expect} from "@playwright/test";
import {loadTimeline} from "../src/timeline";

test("paging restarts from a fresh cursor after a snapshot conflict", async () => {
  const original = globalThis.fetch;
  const cursors = ["a".repeat(64), "b".repeat(64)];
  const urls: string[] = [];
  globalThis.fetch = async input => {
    urls.push(String(input));
    if (urls.length === 2) return new Response(JSON.stringify({
      error: {code: "snapshot_changed", message: "snapshot changed"},
    }), {status: 409});
    const cursor = urls.length === 1 ? cursors[0] : cursors[1];
    const item = urls.length === 1 ? {id: "stale", created_seq: 20}
      : urls.length === 3 ? {id: "newest", created_seq: 21} : {id: "oldest", created_seq: 9};
    return new Response(JSON.stringify({data: {items: [item], snapshot_cursor: cursor,
      next_before: urls.length === 4 ? null : 10}}));
  };
  try {
    const result = await loadTimeline<{id: string; created_seq: number}>("boards/demo/posts?limit=1", 2);
    expect(result.items.map(item => item.id)).toEqual(["oldest", "newest"]);
    expect(result.snapshot_cursor).toBe(cursors[1]);
    expect(result.cursor_drifted).toBe(false);
    expect(urls[1]).toContain(`snapshot_cursor=${cursors[0]}`);
    expect(urls[3]).toContain(`snapshot_cursor=${cursors[1]}`);
  } finally { globalThis.fetch = original; }
});

test("snapshot conflicts have a bounded retry count", async () => {
  const original = globalThis.fetch;
  let requests = 0;
  globalThis.fetch = async () => {
    requests++;
    return new Response(JSON.stringify({error: {code: "snapshot_changed", message: "snapshot changed"}}),
      {status: 409});
  };
  try {
    await expect(loadTimeline("boards/demo/posts", 2)).rejects.toMatchObject({code: "snapshot_changed"});
    expect(requests).toBe(3);
  } finally { globalThis.fetch = original; }
});
