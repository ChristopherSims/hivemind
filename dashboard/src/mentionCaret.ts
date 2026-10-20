/**
 * Pure helpers for caret-aware mention detection and on-screen caret placement.
 *
 * The dashboard composer treats `@` as the start of a mention only when it is
 * preceded by the beginning of the field or whitespace. Detection is scoped to
 * the text *before the caret*, so a mention typed in the middle of a draft is
 * recognised exactly like one typed at the end.
 */

export type MentionToken = {
  /** Index of the "@" that opens the token. */
  start: number;
  /** Caret index (exclusive end of the token being typed). */
  end: number;
  /** Text typed after the "@" and before the caret. */
  query: string;
};

/**
 * Locate the `@query` token that immediately precedes `caret`, if any.
 *
 * Returns `null` when the caret is not inside a mention token, which includes
 * e-mail-like text such as `user@example` (the `@` is not preceded by
 * whitespace or the start of the field).
 */
export function findMentionToken(value: string, caret: number): MentionToken | null {
  const end = Math.max(0, Math.min(caret, value.length));
  const before = value.slice(0, end);
  const match = /(^|\s)@([^\s@]*)$/.exec(before);
  if (!match) return null;
  const query = match[2];
  return {start: before.length - query.length - 1, end, query};
}

/**
 * Remove a mention token from a draft and report where the caret should land.
 *
 * Works for tokens anywhere in the value, not only at the end. When removing a
 * mid-draft token would leave a doubled space, one space is collapsed so
 * "Hello @Dro world" becomes "Hello world" rather than "Hello  world".
 */
export function applyMentionRemoval(value: string, token: MentionToken): {value: string; caret: number} {
  let next = value.slice(0, token.start) + value.slice(token.end);
  const caret = token.start;
  if (next[caret - 1] === " " && next[caret] === " ") {
    next = next.slice(0, caret) + next.slice(caret + 1);
  }
  return {value: next, caret};
}

export type CaretRect = {
  /** Distance from the field's border box to the caret, in pixels. */
  top: number;
  left: number;
  /** Line height at the caret, used to place the menu on the following line. */
  height: number;
};

const MIRROR_PROPERTIES = [
  "boxSizing",
  "overflowX",
  "overflowY",
  "borderTopWidth",
  "borderRightWidth",
  "borderBottomWidth",
  "borderLeftWidth",
  "borderStyle",
  "paddingTop",
  "paddingRight",
  "paddingBottom",
  "paddingLeft",
  "fontStyle",
  "fontVariant",
  "fontWeight",
  "fontStretch",
  "fontSize",
  "lineHeight",
  "fontFamily",
  "textAlign",
  "textTransform",
  "textIndent",
  "letterSpacing",
  "wordSpacing",
  "tabSize",
  "whiteSpace",
  "wordBreak",
  "overflowWrap",
  "direction",
] as const;

function kebab(name: string): string {
  return name.replace(/[A-Z]/g, character => `-${character.toLowerCase()}`);
}

/**
 * Measure where the caret sits inside a textarea by mirroring the field's
 * text layout in a hidden element and reading the offset of a trailing marker.
 *
 * Returns coordinates relative to the textarea's border box; callers translate
 * them into their own positioning context.
 */
export function caretRect(field: HTMLTextAreaElement, position: number): CaretRect {
  const computed = window.getComputedStyle(field);
  const borderLeft = parseFloat(computed.borderLeftWidth) || 0;
  const borderRight = parseFloat(computed.borderRightWidth) || 0;
  const borderTop = parseFloat(computed.borderTopWidth) || 0;

  const mirror = document.createElement("div");
  mirror.setAttribute("aria-hidden", "true");
  mirror.style.position = "absolute";
  mirror.style.visibility = "hidden";
  mirror.style.pointerEvents = "none";
  mirror.style.top = "0";
  mirror.style.left = "0";
  mirror.style.margin = "0";
  mirror.style.whiteSpace = "pre-wrap";
  mirror.style.overflowWrap = "break-word";
  mirror.style.wordBreak = "break-word";
  for (const property of MIRROR_PROPERTIES) {
    const value = computed.getPropertyValue(kebab(property));
    if (value) mirror.style.setProperty(kebab(property), value);
  }
  // Copying padding/border while forcing a border-box width equal to the
  // field's inner width reproduces the field's exact wrapping behaviour.
  mirror.style.boxSizing = "border-box";
  mirror.style.width = `${field.clientWidth + borderLeft + borderRight}px`;

  const safePosition = Math.max(0, Math.min(position, field.value.length));
  mirror.textContent = field.value.slice(0, safePosition);
  const marker = document.createElement("span");
  marker.textContent = field.value.slice(safePosition) || ".";
  mirror.append(marker);

  document.body.append(mirror);
  const top = marker.offsetTop + borderTop;
  const left = marker.offsetLeft + borderLeft;
  const measuredLineHeight = parseFloat(computed.lineHeight);
  const fontSize = parseFloat(computed.fontSize) || 14;
  mirror.remove();

  return {
    top,
    left,
    height: Number.isFinite(measuredLineHeight) ? measuredLineHeight : fontSize * 1.2,
  };
}
