import React, {
  useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState,
} from "react";
import {
  anchorOffset, buildLayout, indexAtOffset, restoredScrollTop, visibleRange,
  type VirtualLayout,
} from "./virtual-timeline-math";
import "./virtual-timeline.css";

/** Distance from the bottom (px) within which the reader is treated as following the latest item. */
const BOTTOM_THRESHOLD = 80;
const DEFAULT_ESTIMATED_ITEM_HEIGHT = 180;
const DEFAULT_OVERSCAN = 400;

export type VirtualTimelineProps<T> = {
  /** Every loaded item. Item count is unbounded; the DOM is not. */
  items: readonly T[];
  /** Renders one item. `index` is the index into `items`, not into the rendered window. */
  renderItem: (item: T, index: number) => React.ReactNode;
  /**
   * Stable, unique key for an item. Required for correct height caching and for
   * keeping DOM nodes (and focus) attached to the same item across updates.
   * Defaults to `String(item.id)` and falls back to the index when no id exists,
   * which is unstable under prepends — pass `getKey` when ids are not `item.id`.
   */
  getKey?: (item: T, index: number) => string;
  /** Slot above the items, inside the scroll area (paging controls, notices). */
  header?: React.ReactNode;
  /** Slot below the items, inside the scroll area. */
  footer?: React.ReactNode;
  /** Slot shown when there are no items and `loading` is false. */
  empty?: React.ReactNode;
  /** Shows the loading slot in the head. `loadingSlot={null}` suppresses the default status line. */
  loading?: boolean;
  loadingSlot?: React.ReactNode;
  /** Height assumed for items that have not been measured yet. */
  estimatedItemHeight?: number;
  /** Extra pixels rendered above and below the viewport. */
  overscan?: number;
  className?: string;
  /** Applied to the scroll container. */
  role?: React.AriaRole;
  /** Accessible name for the rendered item list. */
  ariaLabel?: string;
  /** Called for every scroll of the container, after internal state is synced. */
  onScroll?: React.UIEventHandler<HTMLDivElement>;
  /** Forwarded to the scroll container so callers can call `scrollTo`/`scrollHeight`. */
  ref?: React.Ref<HTMLDivElement>;
};

/**
 * Windowed renderer for timelines with variable-height items.
 *
 * Contract:
 * - `items` holds all loaded data; only items intersecting the viewport (plus
 *   `overscan`) exist in the DOM, so DOM size is bounded by viewport height.
 * - Item heights are measured with `ResizeObserver` and cached by stable key, so
 *   scrolling back to an item does not re-measure it and prepends keep offsets.
 * - The scroll container is the outer element and forwards `ref`/`onScroll`, so
 *   `ref.scrollTo(0, ref.scrollHeight)` and `ref.scrollHeight` keep working.
 * - While the reader is within `BOTTOM_THRESHOLD` of the bottom the view follows
 *   new items (scroll-to-latest). Otherwise the item under the viewport top is
 *   held in place when items are prepended or live-updated.
 * - Items must have unique, stable keys; duplicates would collide in the height
 *   cache and produce React key warnings.
 *
 * Accessibility:
 * - The rendered window is exposed as `role="list"` with `role="listitem"` and
 *   `aria-posinset`/`aria-setsize`, so assistive technology is told the true
 *   position of a rendered item inside the full, larger collection.
 * - Off-window items are absent from the DOM (never `aria-hidden` or visually
 *   hidden while focusable), so there is no hidden-but-reachable interaction.
 * - The container is programmatically focusable (`tabIndex={-1}`, not in the tab
 *   order) so focus can be recovered there instead of being lost to `<body>` when
 *   the focused control scrolls out of the window.
 */
export function VirtualTimeline<T>(props: VirtualTimelineProps<T>): React.ReactElement {
  const {
    items, renderItem, getKey, header, footer, empty, loading = false, loadingSlot,
    estimatedItemHeight = DEFAULT_ESTIMATED_ITEM_HEIGHT, overscan = DEFAULT_OVERSCAN,
    className, role, ariaLabel, onScroll, ref,
  } = props;

  const containerRef = useRef<HTMLDivElement | null>(null);
  const headRef = useRef<HTMLDivElement | null>(null);
  const heightsRef = useRef(new Map<string, number>());
  const keysRef = useRef<string[]>([]);
  const layoutRef = useRef<VirtualLayout>({keys: [], offsets: [], heights: [], total: 0});
  const prefixRef = useRef(0);
  const focusedKeyRef = useRef<string | null>(null);
  const itemObserverRef = useRef<ResizeObserver | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(0);
  const [headHeight, setHeadHeight] = useState(0);
  const [measureVersion, setMeasureVersion] = useState(0);

  const setContainer = useCallback((node: HTMLDivElement | null) => {
    containerRef.current = node;
    if (typeof ref === "function") ref(node);
    else if (ref) (ref as {current: HTMLDivElement | null}).current = node;
  }, [ref]);

  // Keys are rebuilt when the items array identity changes, but the previous
  // array is reused when the keys are unchanged. That keeps `layout` (and the
  // anchor-preserving layout effect) stable when a parent re-renders with a new
  // array that holds the same items.
  const keys = useMemo(() => {
    const next = items.map((item, index) => getKey
      ? String(getKey(item, index))
      : String((item as {id?: unknown} | null | undefined)?.id ?? index));
    const previous = keysRef.current;
    if (previous.length === next.length && previous.every((key, index) => key === next[index])) return previous;
    keysRef.current = next;
    return next;
  }, [items, getKey]);

  // `measureVersion` is bumped by the item observer; it is the signal that cached
  // heights changed even though `keys` did not.
  const layout = useMemo(
    () => buildLayout(keys, heightsRef.current, estimatedItemHeight),
    [keys, estimatedItemHeight, measureVersion],
  );

  const range = useMemo(
    () => visibleRange(layout.offsets, layout.heights, scrollTop, viewportHeight, overscan),
    [layout, scrollTop, viewportHeight, overscan],
  );

  const windowNodes: React.ReactNode[] = [];
  const renderedKeys: string[] = [];
  for (let index = range.start; index < range.end && index < keys.length; index++) {
    const key = keys[index];
    renderedKeys.push(key);
    windowNodes.push(
      <div
        key={key}
        className="virtual-timeline-item"
        data-vt-key={key}
        data-vt-index={index}
        role="listitem"
        aria-posinset={index + 1}
        aria-setsize={items.length}
        style={{transform: `translateY(${layout.offsets[index]}px)`}}
      >
        {renderItem(items[index], index)}
      </div>,
    );
  }
  const windowSignature = renderedKeys.join("|");

  // Track the container height so the window grows and shrinks with the viewport.
  useLayoutEffect(() => {
    const node = containerRef.current;
    if (!node) return;
    const sync = () => setViewportHeight(current => current === node.clientHeight ? current : node.clientHeight);
    const observer = new ResizeObserver(sync);
    observer.observe(node);
    sync();
    return () => observer.disconnect();
  }, []);

  // The head slot (paging controls, loading, empty state) is part of the scroll
  // content, so its measured height is the prefix for every item offset.
  useLayoutEffect(() => {
    const node = headRef.current;
    if (!node) return;
    const sync = () => {
      const height = node.getBoundingClientRect().height;
      setHeadHeight(current => Math.abs(current - height) < 0.5 ? current : height);
    };
    const observer = new ResizeObserver(sync);
    observer.observe(node);
    sync();
    return () => observer.disconnect();
  }, []);

  // Measure rendered items. Heights are cached by stable key, so an item that
  // scrolls out of the window keeps the height it had when it was measured.
  useLayoutEffect(() => {
    const observer = itemObserverRef.current ?? new ResizeObserver(entries => {
      let changed = false;
      for (const entry of entries) {
        const target = entry.target as HTMLElement;
        const key = target.dataset.vtKey;
        if (!key) continue;
        const height = target.getBoundingClientRect().height;
        if (height <= 0 || heightsRef.current.get(key) === height) continue;
        heightsRef.current.set(key, height);
        changed = true;
      }
      if (changed) setMeasureVersion(current => current + 1);
    });
    itemObserverRef.current = observer;
    const container = containerRef.current;
    if (!container) return;
    const observed: Element[] = [];
    container.querySelectorAll(".virtual-timeline-item").forEach(node => {
      observer.observe(node);
      observed.push(node);
    });
    return () => observed.forEach(node => observer.unobserve(node));
  }, [windowSignature]);

  useEffect(() => () => {
    itemObserverRef.current?.disconnect();
    itemObserverRef.current = null;
  }, []);

  // Drop cached heights for items that are no longer loaded.
  useEffect(() => {
    const live = new Set(keys);
    for (const key of Array.from(heightsRef.current.keys())) {
      if (!live.has(key)) heightsRef.current.delete(key);
    }
  }, [keys]);

  const handleScroll = useCallback((event: React.UIEvent<HTMLDivElement>) => {
    const top = event.currentTarget.scrollTop;
    setScrollTop(current => Math.abs(current - top) < 0.5 ? current : top);
    onScroll?.(event);
  }, [onScroll]);

  // Hold the reader's place: keep the anchor item's viewport offset across
  // prepends and live updates, and follow the bottom while the reader is at the
  // bottom (the scroll-to-latest behavior of the previous non-windowed list).
  useLayoutEffect(() => {
    const node = containerRef.current;
    const previous = layoutRef.current;
    const previousPrefix = prefixRef.current;
    layoutRef.current = layout;
    prefixRef.current = headHeight;
    if (!node) return;
    // Read the live scroll position, not the last scroll event: a programmatic
    // scrollTo or an update in the same frame must not be treated as stale.
    const before = node.scrollTop;
    const wasAtBottom = before + node.clientHeight >= previousPrefix + previous.total - BOTTOM_THRESHOLD;
    const anchor = anchorOffset(previous, previousPrefix, before);
    const fallback = anchor ? indexAtOffset(previous.offsets, before - previousPrefix) : 0;
    const bottom = Math.max(0, node.scrollHeight - node.clientHeight);
    const next = wasAtBottom || !anchor
      ? bottom
      : restoredScrollTop(layout, headHeight, anchor, fallback);
    if (Math.abs(node.scrollTop - next) > 0.5) node.scrollTop = next;
    setScrollTop(current => Math.abs(current - node.scrollTop) < 0.5 ? current : node.scrollTop);
  }, [layout, headHeight]);

  const handleFocus = useCallback((event: React.FocusEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement | null;
    const item = target?.closest(".virtual-timeline-item") ?? null;
    focusedKeyRef.current = item?.getAttribute("data-vt-key") ?? null;
  }, []);

  // If the focused control scrolls out of the window, keep keyboard users inside
  // the scroll region instead of dropping focus on <body>.
  useLayoutEffect(() => {
    const key = focusedKeyRef.current;
    const container = containerRef.current;
    if (!key || !container) return;
    const active = document.activeElement;
    if (active && active !== document.body && active !== document.documentElement) return;
    if (container.querySelector(`[data-vt-key="${CSS.escape(key)}"]`)) return;
    focusedKeyRef.current = null;
    container.focus({preventScroll: true});
  }, [windowSignature]);

  return (
    <div
      ref={setContainer}
      className={className ? `virtual-timeline ${className}` : "virtual-timeline"}
      onScroll={handleScroll}
      onFocusCapture={handleFocus}
      tabIndex={-1}
      role={role}
      aria-label={role ? ariaLabel : undefined}
      aria-busy={loading || undefined}
      data-vt-total={items.length}
      data-vt-start={range.start}
      data-vt-end={range.end}
      data-vt-rendered={windowNodes.length}
    >
      <div className="virtual-timeline-canvas" style={{height: headHeight + layout.total}}>
        <div className="virtual-timeline-head" ref={headRef}>
          {header}
          {loading && (loadingSlot === undefined
            ? <p className="virtual-timeline-status" role="status">Loading…</p>
            : loadingSlot)}
          {!loading && items.length === 0 && empty}
        </div>
        {items.length > 0 && (
          <div className="virtual-timeline-items" role="list" aria-label={ariaLabel}>
            {windowNodes}
          </div>
        )}
      </div>
      {footer}
    </div>
  );
}
