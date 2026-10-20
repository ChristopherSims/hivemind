/**
 * Presentation for mention feedback, shared by both composers.
 *
 * Nothing here decides anything: the detection, the confirmation state, and the
 * canonical receipts all come from `mention-feedback.ts`, so the dashboard and
 * the thread pane render identical copy and identical honesty guarantees.
 */
import {
  describeUnresolved,
  type MentionReceiptFeedback,
  type MentionSendGuardView,
  type UnresolvedMention,
} from "./mention-feedback";
import "./mention-feedback.css";

export type MentionUnresolvedNoticeProps = {
  unresolved: UnresolvedMention[];
  /** True once the author explicitly chose a plain-text send for these tokens. */
  confirmed?: boolean;
  /** When provided, the notice offers the explicit "send as plain text anyway" action. */
  onConfirmPlainText?: () => void;
  /** Optional one-click send the host performs after the confirmation. */
  onSendPlainText?: () => void;
  sending?: boolean;
  compact?: boolean;
  className?: string;
  id?: string;
};

/**
 * Accessible warning that listed `@tokens` are plain text. Rendered only when
 * something is actually unresolved, so a clean draft never shows a notice.
 */
export function MentionUnresolvedNotice({
  unresolved, confirmed = false, onConfirmPlainText, onSendPlainText, sending = false,
  compact = false, className, id,
}: MentionUnresolvedNoticeProps) {
  if (!unresolved.length) return null;
  const guidance = confirmed
    ? "Confirmed: this post sends them as plain text."
    : onConfirmPlainText
      ? "Select a name from the mention list to notify an agent, or confirm to send as plain text."
      : "Select a name from the mention list to notify an agent.";
  return (
    <div
      id={id}
      role="alert"
      data-mention-unresolved="true"
      data-mention-unresolved-count={unresolved.length}
      className={`mention-feedback mention-feedback-warning${
        compact ? " mention-feedback--compact" : ""}${className ? ` ${className}` : ""}`}
    >
      <p className="mention-feedback-message">
        <strong>{describeUnresolved(unresolved)}</strong> {guidance}
      </p>
      <ul className="mention-feedback-tokens">
        {unresolved.map(item => (
          <li key={`${item.start}:${item.end}:${item.text}`}>
            <code>{`@${item.text}`}</code>
          </li>
        ))}
      </ul>
      {!confirmed && onConfirmPlainText && (
        <button
          type="button"
          className="mention-plain-text-button"
          data-mention-plain-text=""
          disabled={sending}
          onClick={() => {
            onConfirmPlainText();
            onSendPlainText?.();
          }}
        >
          Send as plain text anyway
        </button>
      )}
      {confirmed && (
        <p className="mention-feedback-confirmed" role="status">Plain-text send confirmed.</p>
      )}
    </div>
  );
}

export type MentionQueuedFeedbackProps = {
  feedback: MentionReceiptFeedback;
  /** Re-read the canonical receipts (only offered when the read failed). */
  onRetry?: () => void;
  compact?: boolean;
  className?: string;
};

/**
 * Post-acceptance feedback. "Queued" appears only when a canonical notification
 * receipt says so; a failed read says it failed instead of promising delivery,
 * and none of the copy promises a reply.
 */
export function MentionQueuedFeedback({
  feedback, onRetry, compact = false, className,
}: MentionQueuedFeedbackProps) {
  if (!feedback || feedback.status === "idle") return null;
  return (
    <div
      role="status"
      aria-live="polite"
      data-mention-receipts={feedback.status}
      className={`mention-feedback mention-feedback-receipts${
        compact ? " mention-feedback--compact" : ""}${className ? ` ${className}` : ""}`}
    >
      <p className="mention-feedback-message">{feedback.message}</p>
      {feedback.recipients.length > 0 && (
        <ul className="mention-receipt-list">
          {feedback.recipients.map(item => (
            <li className="mention-receipt" data-agent-id={item.agent_id} key={item.notification_id}>
              <span className="mention-receipt-name">
                {item.display_name ?? `Agent ${item.agent_id.slice(-8)}`}
              </span>
              <span className="mention-receipt-state">
                {item.state === "queued" ? "notification queued" : "notification no longer queued"}
              </span>
              {item.presence && (
                <span className="mention-receipt-presence" data-presence={item.presence}>
                  {item.presence}
                </span>
              )}
            </li>
          ))}
        </ul>
      )}
      {feedback.status === "unavailable" && onRetry && (
        <button type="button" className="mention-feedback-retry" onClick={onRetry}>
          Check receipts again
        </button>
      )}
    </div>
  );
}

export type MentionSendGuardProps = {
  guard: MentionSendGuardView;
  sending?: boolean;
  /** Optional one-click send after the explicit plain-text confirmation. */
  onSendPlainText?: () => void;
  compact?: boolean;
  className?: string;
};

/**
 * Drop-in guard for a composer footer: the unresolved-mention warning with its
 * explicit confirmation, plus the queued-receipt feedback after a send.
 */
export function MentionSendGuard({
  guard, sending = false, onSendPlainText, compact = false, className,
}: MentionSendGuardProps) {
  const retry = guard.receiptPostId
    ? () => guard.trackReceipts(guard.receiptPostId as string)
    : undefined;
  return (
    <>
      <MentionUnresolvedNotice
        unresolved={guard.unresolved}
        confirmed={guard.plainTextConfirmed}
        onConfirmPlainText={guard.confirmPlainText}
        onSendPlainText={onSendPlainText}
        sending={sending}
        compact={compact}
        className={className}
      />
      <MentionQueuedFeedback feedback={guard.receipts} onRetry={retry} compact={compact} />
    </>
  );
}

export default MentionSendGuard;
