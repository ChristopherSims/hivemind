import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import {api} from "./api";
import "./agent-mention-prefill.css";

/**
 * Optional, per-agent opt-in prefill control for the *shared board* composer on
 * an agent tab.
 *
 * Deliberate, reviewed constraints (each one is also asserted in
 * dashboard/tests/agent-prefill.spec.ts):
 *
 *  - Opt-in control, default OFF. The preference lives in React state only: it
 *    is never written to localStorage/sessionStorage, cookies, the URL, the
 *    server, or a transcript. Rationale: the opt-in is a UI preference, not a
 *    credential and not evidence, so it must not outlive the mounted view, be
 *    replayed into exported artifacts, or become part of shared history. A
 *    reload therefore starts OFF by design.
 *  - Merely rendering this control, viewing an agent tab, or switching
 *    project/board/view/permission never calls the network, never notifies an
 *    agent, and never touches the host draft or chip list. Nothing happens until
 *    the user opts in *and* uses the explicit add action.
 *  - The add action re-checks the selected agent's *exact* id against this
 *    board's `projects/{project}/mention-options` endpoint before anything is
 *    handed to the host. If the id is absent, the call fails, the context
 *    changed while the request was in flight, or the response is stale, nothing
 *    is added (fail closed). The display name is used only as a search hint for
 *    the endpoint, never as proof of identity.
 *  - This module never receives the draft or any message text, so it cannot
 *    quote, repost, or re-send private content. It only hands back the immutable
 *    `id` (with the endpoint's own display name and colour token) so the host's
 *    existing `choose` adds exactly the chip the composer menu would add.
 *  - Nothing is submitted: every control is `type="button"`, this component
 *    renders no form, and it never calls a write endpoint — even when the host
 *    mounts it inside the composer form (main.tsx does).
 *  - Audience is fixed and stated in the UI: the shared board, "Visible to
 *    everyone permitted on this board". Opting in, adding a mention, or opening
 *    the agent tab does not widen that audience, create a direct/private
 *    channel, or change post visibility; no visibility-changing API is used.
 *  - Navigation stays with the host: `onOpenAgent` is wired by main.tsx to
 *    `openAgent`/`selectTab`. This module never touches tabs or history.
 */

/** The exact audience sentence the control shows and is described by. */
export const PREFILL_AUDIENCE = "Visible to everyone permitted on this board";

/** Guard against sending an unbounded search hint to the endpoint. */
const IDENTITY_QUERY_LIMIT = 128;

/** A single `mention-options` row, shaped exactly as the backend returns it. */
export type MentionPrefillOption = {
  id: string;
  display_name: string;
  base_color_token?: string;
  description?: string;
  effective_role?: string;
};

/**
 * What the host receives on an explicit add: an immutable id plus the names the
 * endpoint itself vouched for. No message text is ever included.
 */
export type MentionPrefillRequest = {
  id: string;
  display_name: string;
  base_color_token?: string;
};

export type AgentMentionPrefillAgent = {
  id: string;
  display_name: string;
  base_color_token?: string;
};

export type AgentMentionPrefillStatus =
  | "off"
  | "armed"
  | "checking"
  | "eligible"
  | "added"
  | "duplicate"
  | "blocked"
  | "error";

/** Why an add was refused, so the UI can say what is missing. */
export type MentionPrefillBlockedDetail =
  | "no-project"
  | "no-board"
  | "no-identity"
  | "not-visible"
  | "not-verified"
  | null;

/** Test/reuse seam; defaults to the project mention-options endpoint. */
export type FetchMentionOptions = (
  query: string,
  context: {project: string; board?: string},
) => Promise<MentionPrefillOption[]>;

export type AgentMentionPrefillOptions = {
  /** The agent whose tab is open; the only mention this control can add. */
  agent: AgentMentionPrefillAgent;
  project: string;
  /** Shared board the composer posts to. Missing board fails closed. */
  board?: string;
  /**
   * Any string that identifies the current view (tab id, filter, etc.). A change
   * invalidates in-flight checks and re-arms the opt-in to OFF.
   */
  viewKey?: string;
  /** Anything that gates access (role, permission revision). Changing it disarms. */
  permissionKey?: string;
  /** Ids already chipped on the host composer, used to avoid duplicates. */
  existingMentionIds?: readonly string[];
  /**
   * Host callback — main.tsx `choose`. Adds the immutable chip to the draft
   * composer. This component never posts, so nothing is published here.
   */
  onMention: (request: MentionPrefillRequest) => void;
  fetchOptions?: FetchMentionOptions;
  /** Host-level busy state (e.g. while the surrounding composer posts). */
  disabled?: boolean;
};

export type AgentMentionPrefillProps = AgentMentionPrefillOptions & {
  /** Host callback — main.tsx `openAgent`/`selectTab`. Navigation stays host-owned. */
  onOpenAgent?: (agentId: string) => void;
  /** True when this agent's tab is already the active view. */
  viewActive?: boolean;
  /** Override the audience sentence. Must not weaken or widen the stated audience. */
  audience?: string;
  className?: string;
};

export type AgentMentionPrefillController = {
  armed: boolean;
  setArmed: (value: boolean) => void;
  status: AgentMentionPrefillStatus;
  detail: MentionPrefillBlockedDetail;
  /** Human-readable status; never contains message content. */
  note: string;
  pending: boolean;
  /** True when the host already has this agent's chip on the composer. */
  alreadyMentioned: boolean;
  /** True when an explicit, verified add is possible right now. */
  canAddress: boolean;
  disabled: boolean;
  /** Check the exact id against the board mention options without adding it. */
  verify: () => Promise<MentionPrefillRequest | null>;
  /** Verify, then hand the immutable id to `onMention`. Never submits anything. */
  requestMention: () => Promise<MentionPrefillRequest | null>;
  /** Disarm and forget in-flight work. */
  reset: () => void;
};

/**
 * Default eligibility lookup. Mirrors MentionComposer's seam so the host can
 * share one implementation; only the already-authorized endpoint is used.
 */
export async function defaultFetchMentionOptions(
  query: string,
  context: {project: string; board?: string},
): Promise<MentionPrefillOption[]> {
  const board = context.board ? `&board_id=${encodeURIComponent(context.board)}` : "";
  return api<MentionPrefillOption[]>(
    `projects/${context.project}/mention-options?query=${encodeURIComponent(query)}${board}`,
  );
}

/** Status copy. Pure so the wording can be reviewed and reused by hosts. */
export function mentionPrefillNote(
  status: AgentMentionPrefillStatus,
  detail: MentionPrefillBlockedDetail,
  name: string,
): string {
  switch (status) {
    case "armed":
      return `Opted in. Use the add button to place @${name} on your next reply on this board. Nothing is posted and no message text is copied.`;
    case "checking":
      return `Checking @${name} against this board's mention options…`;
    case "eligible":
      return `@${name} can be mentioned on this board.`;
    case "added":
      return `Added @${name} to this reply. Nothing has been posted.`;
    case "duplicate":
      return `@${name} is already addressed on this reply.`;
    case "error":
      return "Could not confirm this board's mention options. Nothing was added.";
    case "blocked":
      if (detail === "no-project") return "No workspace is selected, so no agent was verified. Nothing was added.";
      if (detail === "no-board") return "No board is selected, so no agent was verified. Nothing was added.";
      if (detail === "no-identity") return "This agent has no verifiable id in this view. Nothing was added.";
      return `This board's mention options do not include @${name}. Nothing was added.`;
    default:
      return "Off. Nothing is added or sent while this is off. Viewing or switching tabs never notifies anyone.";
  }
}

/**
 * Stateful helper behind the control. Kept separate from the component so a host
 * that already owns a composer layout can drive the same verified add flow.
 */
export function useAgentMentionPrefill(options: AgentMentionPrefillOptions): AgentMentionPrefillController {
  const {
    agent, project, board, viewKey, permissionKey, existingMentionIds,
    onMention, fetchOptions, disabled = false,
  } = options;

  const agentId = agent?.id ?? "";
  const displayName = (agent?.display_name ?? "").trim();

  // Every access-relevant input is folded into one key. A change to any part of
  // it invalidates in-flight work and returns the opt-in to OFF.
  const contextKey = [project ?? "", board ?? "", agentId, viewKey ?? "", permissionKey ?? ""].join("\u0000");

  const [armed, setArmedState] = useState(false);
  const [status, setStatus] = useState<AgentMentionPrefillStatus>("off");
  const [detail, setDetail] = useState<MentionPrefillBlockedDetail>(null);
  const [pending, setPending] = useState(false);

  const onMentionRef = useRef(onMention);
  const loadOptions = useRef(fetchOptions);
  const existingRef = useRef<readonly string[]>(existingMentionIds ?? []);
  const armedRef = useRef(false);
  const contextRef = useRef(contextKey);
  // Monotonic request token: bumping it discards any response that is still in
  // flight. `api()` is a shared helper without a signal parameter, so stale
  // responses cannot be aborted at the socket — they are dropped at the door and
  // never reach the host.
  const epoch = useRef(0);
  const busy = useRef(false);
  // Id handed to the host, cleared as soon as the host reports the chip.
  const handed = useRef<string | null>(null);

  contextRef.current = contextKey;

  useEffect(() => {
    onMentionRef.current = onMention;
    loadOptions.current = fetchOptions;
    existingRef.current = existingMentionIds ?? [];
    armedRef.current = armed;
  });

  const reset = useCallback(() => {
    epoch.current += 1;
    busy.current = false;
    handed.current = null;
    armedRef.current = false;
    setArmedState(false);
    setPending(false);
    setStatus("off");
    setDetail(null);
  }, []);

  // Fail closed on every project/board/view/permission change.
  useEffect(() => {
    reset();
  }, [contextKey, reset]);

  // The duplicate guard releases itself once the host shows the chip, so a chip
  // removed by the user can be added again in the same mount.
  useEffect(() => {
    if (handed.current && (existingMentionIds ?? []).includes(handed.current)) handed.current = null;
  }, [existingMentionIds]);

  const setArmed = useCallback((value: boolean) => {
    if (disabled) return;
    // Changing intent invalidates any check started under the previous intent.
    epoch.current += 1;
    busy.current = false;
    setPending(false);
    armedRef.current = value;
    setArmedState(value);
    if (!value) {
      setStatus("off");
      setDetail(null);
      return;
    }
    if (!project) { setStatus("blocked"); setDetail("no-project"); return; }
    if (!board) { setStatus("blocked"); setDetail("no-board"); return; }
    if (!agentId || !displayName) { setStatus("blocked"); setDetail("no-identity"); return; }
    setStatus("armed");
    setDetail(null);
  }, [disabled, project, board, agentId, displayName]);

  const verify = useCallback(async (): Promise<MentionPrefillRequest | null> => {
    if (!project) { setStatus("blocked"); setDetail("no-project"); return null; }
    if (!board) { setStatus("blocked"); setDetail("no-board"); return null; }
    if (!agentId || !displayName) { setStatus("blocked"); setDetail("no-identity"); return null; }

    const key = contextKey;
    const token = ++epoch.current;
    busy.current = true;
    setPending(true);
    setStatus("checking");
    setDetail(null);
    try {
      const load = loadOptions.current ?? defaultFetchMentionOptions;
      // The display name is only a search hint; the id below must match exactly.
      const rows = await load(displayName.slice(0, IDENTITY_QUERY_LIMIT), board ? {project, board} : {project});
      if (token !== epoch.current || key !== contextRef.current) return null;
      const match = Array.isArray(rows) ? rows.find(row => row?.id === agentId) : undefined;
      if (!match || !match.display_name) {
        // Fail closed: an id we could not confirm on this board is never added,
        // even when a lookalike display name came back.
        setStatus("blocked");
        setDetail("not-visible");
        return null;
      }
      setStatus("eligible");
      setDetail(null);
      return {
        id: match.id,
        display_name: match.display_name,
        base_color_token: match.base_color_token,
      };
    } catch {
      if (token !== epoch.current || key !== contextRef.current) return null;
      setStatus("error");
      setDetail("not-verified");
      return null;
    } finally {
      if (token === epoch.current) {
        busy.current = false;
        setPending(false);
      }
    }
  }, [project, board, agentId, displayName, contextKey]);

  const requestMention = useCallback(async (): Promise<MentionPrefillRequest | null> => {
    if (disabled) return null;
    // Default OFF: no explicit opt-in, no add.
    if (!armedRef.current) {
      setStatus("off");
      setDetail(null);
      return null;
    }
    if ((existingRef.current ?? []).includes(agentId) || handed.current === agentId) {
      setStatus("duplicate");
      setDetail(null);
      return null;
    }
    if (busy.current) return null;

    const request = await verify();
    if (!request) return null;
    // Re-checked after the await: the host may have chipped this agent through
    // the composer menu while the eligibility check was in flight.
    if ((existingRef.current ?? []).includes(request.id) || handed.current === request.id) {
      setStatus("duplicate");
      setDetail(null);
      return null;
    }
    handed.current = request.id;
    onMentionRef.current?.(request);
    setStatus("added");
    setDetail(null);
    return request;
  }, [disabled, agentId, verify]);

  const alreadyMentioned = useMemo(
    () => (existingMentionIds ?? []).includes(agentId),
    [existingMentionIds, agentId],
  );

  const canAddress = !disabled && armed && !alreadyMentioned && !pending
    && Boolean(project) && Boolean(board) && Boolean(agentId) && Boolean(displayName);
  const note = mentionPrefillNote(status, detail, displayName || "this agent");

  return {
    armed, setArmed, status, detail, note, pending, alreadyMentioned, canAddress, disabled,
    verify, requestMention, reset,
  };
}

/**
 * Optional explicit control for the shared-board composer on an agent tab.
 *
 * It owns no navigation and no composer state: `onMention` is the host's chip
 * adder and `onOpenAgent` is the host's tab opener.
 */
export function AgentMentionPrefill(props: AgentMentionPrefillProps) {
  const {agent, audience = PREFILL_AUDIENCE, className, onOpenAgent, viewActive = true} = props;
  const controller = useAgentMentionPrefill(props);

  const generatedId = useId();
  const audienceId = `${generatedId}-audience`;
  const noteId = `${generatedId}-note`;
  const toggleId = `${generatedId}-toggle`;

  const name = (agent?.display_name ?? "").trim() || "this agent";
  const note = controller.alreadyMentioned
    ? `@${name} is already addressed on this reply.`
    : controller.note;

  return <div
    className={`agent-prefill${className ? ` ${className}` : ""}`}
    role="group"
    aria-label={`Agent mention prefill for ${name}`}
    data-agent-prefill=""
    data-agent-prefill-target={agent?.id ?? ""}
    data-agent-prefill-status={controller.status}
    data-audience-scope="shared-board">
    <p className="agent-prefill-audience" id={audienceId} data-agent-prefill-audience="">{audience}</p>
    <div className="agent-prefill-control">
      <label className="agent-prefill-switch" htmlFor={toggleId}>
        <input
          id={toggleId}
          type="checkbox"
          data-agent-prefill-toggle=""
          checked={controller.armed}
          disabled={controller.disabled}
          aria-describedby={`${audienceId} ${noteId}`}
          onChange={event => controller.setArmed(event.target.checked)}
        />
        <span>Opt in to addressing {name} on the shared board</span>
      </label>
      {controller.armed && !controller.alreadyMentioned && <button
        type="button"
        className="agent-prefill-add"
        data-agent-prefill-add=""
        disabled={!controller.canAddress}
        aria-label={`Add @${name} to this reply on the shared board. Nothing is posted.`}
        aria-describedby={`${audienceId} ${noteId}`}
        onClick={() => { void controller.requestMention(); }}>
        {controller.pending ? `Checking @${name}…` : `Add @${name} to this reply`}
      </button>}
      {onOpenAgent && !viewActive && <button
        type="button"
        className="agent-prefill-open"
        data-agent-prefill-open=""
        aria-label={`Open the ${name} tab. This sends nothing and does not change who can see your post.`}
        onClick={() => onOpenAgent(agent.id)}>
        Open {name}'s tab
      </button>}
    </div>
    <p className="agent-prefill-note" id={noteId} role="status" data-agent-prefill-note="">{note}</p>
  </div>;
}

export default AgentMentionPrefill;
