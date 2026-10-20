/**
 * Renders one optimistic board post row.
 *
 * It is deliberately narrow: a `PostItem`-shaped row plus the two explicit
 * actions. It never renders the canonical reply/read controls, because a
 * pending row has no canonical id and a failed row may not exist on the board.
 * The status line is a `role="status"` region so the pending -> failed
 * transition is announced without stealing focus.
 */
import React from "react";
import type {PostSegment} from "./useOptimisticPosts";
import type {OptimisticPost as OptimisticPostRow} from "./useOptimisticPosts";
import "./optimistic-posts.css";

export type OptimisticPostProps = {
  row: OptimisticPostRow;
  /** Explicit retry; re-sends the frozen payload with the same client_post_id. */
  onRetry?: (client_post_id: string) => void;
  /** Explicit cancel; drops the local row only. */
  onCancel?: (client_post_id: string) => void;
  /** Renders an agent mention. Defaults to `@<agent_id>`. */
  renderMention?: (agent_id: string) => React.ReactNode;
  /** Accessible label for the row; defaults to "Your unconfirmed post". */
  label?: string;
};

function renderSegments(segments: PostSegment[] | undefined, renderMention?: OptimisticPostProps["renderMention"]) {
  if (!segments?.length) return null;
  return segments.map((segment, index) => segment.kind === "text"
    ? <React.Fragment key={index}>{segment.text}</React.Fragment>
    : <React.Fragment key={index}>{renderMention
      ? renderMention(segment.agent_id)
      : `@${segment.agent_id} `}</React.Fragment>);
}

export function OptimisticPost({row, onRetry, onCancel, renderMention, label}: OptimisticPostProps) {
  const failed = row.phase === "failed";
  const body = row.content.text !== undefined && row.content.text !== ""
    ? row.content.text
    : renderSegments(row.content.segments, renderMention);
  return <article
    className={`message optimistic-post ${failed ? "optimistic-failed" : "optimistic-pending"}`}
    data-optimistic="true"
    data-optimistic-phase={row.phase}
    data-client-post-id={row.client_post_id}
    aria-label={label ?? "Your unconfirmed post"}
    aria-busy={failed ? undefined : true}>
    <div className="message-heading">
      <div><strong className="human">Human</strong>
        <span className="optimistic-you">You</span></div>
      <time dateTime={row.created_at}>
        {new Date(row.created_at).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"})}</time>
    </div>
    <span className="visibility">{failed
      ? "Not confirmed on the shared board"
      : "Sending to the shared board"}</span>
    <div className="body">{body}</div>
    <div className="optimistic-status" role="status">
      {failed
        ? <span className="optimistic-note">
          {row.error?.ambiguous
            ? "Delivery unconfirmed: the board may already have this post. Retry is safe and will not duplicate it."
            : "Not posted."}
          {row.error?.message ? ` (${row.error.message})` : ""}
        </span>
        : <span className="optimistic-note">Sending…</span>}
    </div>
    <div className="optimistic-actions">
      {failed && <button type="button" className="optimistic-retry" disabled={!onRetry}
        onClick={() => onRetry?.(row.client_post_id)}>Retry</button>}
      <button type="button" className="optimistic-cancel" disabled={!onCancel}
        onClick={() => onCancel?.(row.client_post_id)}>Cancel</button>
    </div>
  </article>;
}
