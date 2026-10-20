import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import {api} from "./api";
import {applyMentionRemoval, caretRect, findMentionToken} from "./mentionCaret";
import {findUnresolvedMentions, type UnresolvedMention} from "./mention-feedback";
import {MentionUnresolvedNotice} from "./MentionFeedback";
import "./mention-composer.css";

/**
 * Structural types kept intentionally loose so the dashboard's existing
 * `Agent`, `Run` and ad-hoc option shapes remain assignable without importing
 * application state into the component.
 */
export type MentionAgent = {
  id: string;
  display_name: string;
  base_color_token?: string;
  effective_role?: string;
  description?: string;
  current_tasks?: {id: string; title: string; status: string}[];
  owned_scopes?: {id: string; display_name: string}[];
  blockers?: {code: string; label: string; task_id?: string; run_id?: string}[];
};

export type MentionRun = {
  id: string;
  agent_id: string;
  presence?: string;
  status?: string;
  summary?: {text: string};
  capture_health?: {state: string};
};

/** A single mention-options row exactly as the backend returns it. */
export type MentionOption = {
  id: string;
  display_name: string;
  base_color_token?: string;
  description?: string;
  effective_role?: string;
};

/** What the composer hands back to the host when a mention is chosen. */
export type MentionSelection = {
  id: string;
  display_name: string;
  base_color_token?: string;
};

export type MentionComposerProps = {
  value: string;
  onChange: (value: string) => void;
  onMention: (agent: MentionSelection) => void;
  project: string;
  board?: string;
  /** Authorized project roster (from `agent-appearances`). Used for enrichment only. */
  agents?: MentionAgent[];
  /** Authorized project run ledger (from `ledger`). Used for presence only. */
  runs?: MentionRun[];
  disabled?: boolean;
  placeholder?: string;
  ariaLabel?: string;
  maxLength?: number;
  className?: string;
  /**
   * Changing this string invalidates any in-flight or displayed options. Pass
   * anything that gates access (role, permission revision, etc.).
   */
  permissionKey?: string;
  /** Notified whenever an `@query` token becomes active or clears. */
  onMentionQueryChange?: (active: boolean) => void;
  /**
   * Chips currently attached to the draft, by immutable id. Passing this turns
   * on unresolved plain-text mention detection for the draft; the host's send
   * gate should use the shared `useMentionSendGuard` with the same inputs.
   */
  selections?: MentionSelection[];
  /**
   * Render the inline unresolved warning. Defaults to true once `selections` is
   * provided; set false when the host renders the shared `MentionSendGuard`
   * elsewhere (for example next to the send button) to avoid a duplicate notice.
   */
  showUnresolvedWarning?: boolean;
  /** Notified whenever the unresolved set changes, for host-side send gating. */
  onUnresolvedMentionsChange?: (unresolved: UnresolvedMention[]) => void;
  /** Test/reuse seam; defaults to the project mention-options endpoint. */
  fetchOptions?: (query: string, context: {project: string; board?: string}) => Promise<MentionOption[]>;
};

type EnrichedOption = {
  id: string;
  display_name: string;
  base_color_token?: string;
  effective_role?: string;
  presence?: string;
  status?: string;
  task?: string;
  scope?: string;
  nexus: boolean;
};

const PALETTE = new Set(["blue", "teal", "violet", "coral", "green", "magenta"]);
const PRESENCE_RANK: Record<string, number> = {online: 3, stale: 2, offline: 1};

async function defaultFetchOptions(query: string, context: {project: string; board?: string}): Promise<MentionOption[]> {
  const board = context.board ? `&board_id=${encodeURIComponent(context.board)}` : "";
  return api<MentionOption[]>(
    `projects/${context.project}/mention-options?query=${encodeURIComponent(query)}${board}`,
  );
}

function pickRun(runs: MentionRun[]): MentionRun | undefined {
  if (!runs.length) return undefined;
  return [...runs].sort((left, right) =>
    (PRESENCE_RANK[right.presence ?? ""] ?? 0) - (PRESENCE_RANK[left.presence ?? ""] ?? 0))[0];
}

function colorClass(token: string | undefined): string {
  return token && PALETTE.has(token) ? `mention-color-${token}` : "mention-color-default";
}

/**
 * Merge a returned option with the authorized roster/ledger. Only ids that the
 * backend returned become rows, and enrichment reads solely from the
 * permission-filtered `agents`/`runs` props, so an inaccessible appearance can
 * never be surfaced by cross-referencing a broader list.
 */
function enrich(option: MentionOption, agents: MentionAgent[] | undefined, runs: MentionRun[] | undefined): EnrichedOption {
  const appearance = agents?.find(agent => agent.id === option.id);
  const agentRuns = (runs ?? []).filter(run => run.agent_id === option.id);
  const run = pickRun(agentRuns);
  const effectiveRole = appearance?.effective_role ?? option.effective_role;
  const task = appearance?.current_tasks?.[0]?.title;
  const blocker = appearance?.blockers?.[0];
  const scope = appearance?.owned_scopes?.[0]?.display_name;
  const summary = task ?? run?.summary?.text ?? appearance?.description ?? option.description;
  return {
    id: option.id,
    display_name: option.display_name,
    base_color_token: appearance?.base_color_token ?? option.base_color_token,
    effective_role: effectiveRole,
    presence: run?.presence,
    status: run?.status,
    task: summary,
    scope: blocker ? blocker.label : scope,
    nexus: effectiveRole === "nexus",
  };
}

export function MentionComposer(props: MentionComposerProps) {
  const {
    value, onChange, onMention, project, board, agents, runs, disabled = false,
    placeholder = "Share an update, or type @ to address an agent…",
    ariaLabel = "Shared board message", maxLength, className,
    permissionKey, onMentionQueryChange, fetchOptions, selections, showUnresolvedWarning,
    onUnresolvedMentionsChange,
  } = props;

  const [caret, setCaret] = useState(() => value.length);
  const [items, setItems] = useState<MentionOption[]>([]);
  const [itemsKey, setItemsKey] = useState<string | null>(null);
  const [highlight, setHighlight] = useState(0);
  const [reposition, setReposition] = useState(0);
  const [anchor, setAnchor] = useState<{top: number; left: number}>({top: 0, left: 0});

  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const menuRef = useRef<HTMLUListElement>(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const pendingCaret = useRef<number | null>(null);
  const queryCallback = useRef(onMentionQueryChange);
  const loadOptions = useRef(fetchOptions);

  useEffect(() => {
    queryCallback.current = onMentionQueryChange;
    loadOptions.current = fetchOptions;
  });

  const generatedId = useId();
  const listId = `${generatedId}-list`;
  // Every access-relevant input is folded into one key. Options are only
  // rendered while they belong to the current key, so a project/board/permission
  // change can never flash stale or unauthorized rows.
  const contextKey = `${project}\u0000${board ?? ""}\u0000${permissionKey ?? ""}`;

  const token = useMemo(() => findMentionToken(value, caret), [value, caret]);
  const tokenQuery = token?.query ?? null;
  const requestKey = `${contextKey}\u0000${token?.start ?? ""}\u0000${tokenQuery ?? ""}`;
  const mentionActive = token !== null;

  useEffect(() => {
    queryCallback.current?.(mentionActive);
  }, [mentionActive]);

  // Plain-text mentions that no selected chip backs. Detection reads only the
  // draft and the host's chips, so the composer never writes to the draft on
  // its own: the warning is feedback, not an edit.
  const unresolved = useMemo(
    () => findUnresolvedMentions(value, selections ?? []),
    [value, selections],
  );
  // A token the caret is still inside is being typed, not yet abandoned.
  const visibleUnresolved = useMemo(() => {
    if (!unresolved.length || !token) return unresolved;
    return unresolved.filter(item => !(item.start === token.start && item.end >= token.end));
  }, [unresolved, token]);
  const unresolvedCallback = useRef(onUnresolvedMentionsChange);
  useEffect(() => {
    unresolvedCallback.current = onUnresolvedMentionsChange;
  });
  useEffect(() => {
    unresolvedCallback.current?.(unresolved);
  }, [unresolved]);
  const unresolvedNoticeId = `${generatedId}-unresolved`;
  const warnUnresolved = (showUnresolvedWarning ?? selections !== undefined) && visibleUnresolved.length > 0;

  // Cancel/clear options the moment the access context changes.
  useEffect(() => {
    setItems([]);
    setItemsKey(null);
    setHighlight(0);
  }, [contextKey]);

  useEffect(() => {
    if (tokenQuery === null || !project || disabled) {
      setItems([]);
      setItemsKey(requestKey);
      return;
    }
    const key = requestKey;
    let cancelled = false;
    const timer = window.setTimeout(() => {
      const load = loadOptions.current ?? defaultFetchOptions;
      Promise.resolve(load(tokenQuery, {project, board}))
        .then(data => {
          if (cancelled) return;
          setItems(Array.isArray(data) ? data : []);
          setItemsKey(key);
          setHighlight(0);
        })
        .catch(() => {
          if (cancelled) return;
          setItems([]);
          setItemsKey(key);
        });
    }, 150);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [tokenQuery, project, board, requestKey, disabled]);

  const visible = itemsKey === requestKey ? items : [];
  const enriched = useMemo(() => visible.map(option => enrich(option, agents, runs)), [visible, agents, runs]);
  const open = enriched.length > 0 && token !== null && !disabled;
  const activeIndex = enriched.length ? Math.min(Math.max(highlight, 0), enriched.length - 1) : 0;

  // Restore the caret after the host applies a programmatic value change.
  useLayoutEffect(() => {
    const field = fieldRef.current;
    if (pendingCaret.current === null || !field) return;
    const position = Math.max(0, Math.min(pendingCaret.current, field.value.length));
    field.setSelectionRange(position, position);
    setCaret(position);
    pendingCaret.current = null;
  }, [value]);

  useEffect(() => {
    setCaret(current => (current > value.length ? value.length : current));
  }, [value]);

  const syncCaret = useCallback(() => {
    const field = fieldRef.current;
    if (field) setCaret(field.selectionStart ?? field.value.length);
  }, []);

  const select = useCallback((index: number) => {
    const option = enriched[index];
    if (!option) return;
    const active = token ?? findMentionToken(value, caret);
    let next = value;
    let nextCaret = caret;
    if (active) {
      const removed = applyMentionRemoval(value, active);
      next = removed.value;
      nextCaret = removed.caret;
    }
    pendingCaret.current = nextCaret;
    onChange(next);
    onMention({id: option.id, display_name: option.display_name, base_color_token: option.base_color_token});
    setItems([]);
    setItemsKey(requestKey);
    setHighlight(0);
  }, [enriched, token, value, caret, onChange, onMention, requestKey]);

  function handleChange(event: ChangeEvent<HTMLTextAreaElement>) {
    pendingCaret.current = null;
    onChange(event.target.value);
    setCaret(event.target.selectionStart ?? event.target.value.length);
  }

  function handleKeyDown(event: ReactKeyboardEvent<HTMLTextAreaElement>) {
    if (event.nativeEvent.isComposing) return;
    if (!open) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlight(() => (activeIndex + 1) % enriched.length);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlight(() => (activeIndex - 1 + enriched.length) % enriched.length);
    } else if (event.key === "Enter" || event.key === "Tab") {
      // Selecting must never submit the surrounding form.
      event.preventDefault();
      event.stopPropagation();
      select(activeIndex);
    } else if (event.key === "Escape") {
      event.preventDefault();
      setItems([]);
      setItemsKey(requestKey);
    }
  }

  // Anchor the menu to the real textarea caret (mirror-layout measurement).
  useLayoutEffect(() => {
    const field = fieldRef.current;
    const wrap = wrapRef.current;
    const menu = menuRef.current;
    if (!open || !field || !wrap || !menu) return;
    const rect = caretRect(field, caret);
    const fieldBox = field.getBoundingClientRect();
    const wrapBox = wrap.getBoundingClientRect();
    const x = fieldBox.left - wrapBox.left + rect.left - field.scrollLeft;
    const y = fieldBox.top - wrapBox.top + rect.top - field.scrollTop;
    const menuWidth = menu.offsetWidth || 280;
    const menuHeight = menu.offsetHeight || 0;
    const left = Math.max(0, Math.min(x, Math.max(0, wrap.clientWidth - menuWidth - 4)));
    const caretBottomInViewport = fieldBox.top + rect.top - field.scrollTop + rect.height;
    const flipAbove = menuHeight > 0
      && caretBottomInViewport + menuHeight > window.innerHeight
      && y - menuHeight >= 0;
    const top = flipAbove ? y - menuHeight : y + rect.height;
    setAnchor(current => (current.top === top && current.left === left ? current : {top, left}));
  }, [open, caret, value, enriched.length, reposition]);

  useEffect(() => {
    if (!open) return;
    const field = fieldRef.current;
    const bump = () => setReposition(current => current + 1);
    field?.addEventListener("scroll", bump);
    window.addEventListener("resize", bump);
    return () => {
      field?.removeEventListener("scroll", bump);
      window.removeEventListener("resize", bump);
    };
  }, [open]);

  return (
    <div className={`mention-composer${className ? ` ${className}` : ""}`} data-mention-composer="">
      <div className="mention-input-wrap" ref={wrapRef}>
        {open && (
          <ul
            ref={menuRef}
            id={listId}
            role="listbox"
            aria-label="Agent mentions"
            className="mention-menu"
            data-anchored="caret"
            style={{top: anchor.top, left: anchor.left}}
          >
            {enriched.map((option, index) => (
              <li
                key={option.id}
                id={`${listId}-${index}`}
                role="option"
                aria-selected={index === activeIndex}
                className={`mention-option${index === activeIndex ? " highlight" : ""}`}
                onMouseDown={event => event.preventDefault()}
                onMouseEnter={() => setHighlight(index)}
                onClick={() => select(index)}
              >
                <span className={`mention-swatch ${colorClass(option.base_color_token)}`} aria-hidden="true" />
                <span className="mention-identity">
                  <span className="mention-name">
                    {option.display_name}
                    {option.nexus && (
                      <span className="mention-nexus">
                        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m3 6 4 4 5-6 5 6 4-4-2 13H5Z" /></svg>
                        Nexus
                      </span>
                    )}
                  </span>
                  <span className="mention-meta">
                    {option.presence && (
                      <span className={`mention-presence presence-${option.presence}`}>
                        {option.presence}{option.status ? ` · ${option.status}` : ""}
                      </span>
                    )}
                    {option.task && <span className="mention-task">{option.task}</span>}
                    {option.scope && <span className="mention-scope">{option.scope}</span>}
                  </span>
                </span>
                <span className="mention-id">{option.id.slice(-8)}</span>
              </li>
            ))}
          </ul>
        )}
        <textarea
          ref={fieldRef}
          className="mention-field"
          aria-label={ariaLabel}
          aria-autocomplete="list"
          aria-expanded={open}
          aria-controls={open ? listId : undefined}
          aria-activedescendant={open ? `${listId}-${activeIndex}` : undefined}
          aria-describedby={warnUnresolved ? unresolvedNoticeId : undefined}
          placeholder={placeholder}
          value={value}
          disabled={disabled}
          maxLength={maxLength}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onSelect={syncCaret}
          onClick={syncCaret}
          onKeyUp={syncCaret}
          onFocus={syncCaret}
        />
      </div>
      {warnUnresolved && (
        <MentionUnresolvedNotice id={unresolvedNoticeId} unresolved={visibleUnresolved} />
      )}
    </div>
  );
}

export default MentionComposer;
