/**
 * Pure layout math for VirtualTimeline.
 *
 * This module deliberately has no React, DOM or stylesheet dependency so the
 * windowing/anchor rules can be unit tested directly in Node and reused by any
 * windowed renderer. `VirtualTimeline.tsx` is the only intended consumer.
 */

export type VirtualLayout = {
  /** Stable item keys in display order. */
  keys: readonly string[];
  /** Offset of each item from the top of the item canvas. */
  offsets: number[];
  /** Height used for each item: measured when known, estimated otherwise. */
  heights: number[];
  /** Total height of every item in the layout. */
  total: number;
};

/** Inclusive start, exclusive end. */
export type VirtualRange = {start: number; end: number};

/** Cumulative offsets for `keys`, using `measured` heights and `estimated` fallbacks. */
export function buildLayout(
  keys: readonly string[], measured: ReadonlyMap<string, number>, estimated: number,
): VirtualLayout {
  const offsets = new Array<number>(keys.length);
  const heights = new Array<number>(keys.length);
  let total = 0;
  for (let index = 0; index < keys.length; index++) {
    const value = measured.get(keys[index]);
    const height = value !== undefined && value > 0 ? value : estimated;
    offsets[index] = total;
    heights[index] = height;
    total += height;
  }
  return {keys, offsets, heights, total};
}

/**
 * Indices to render for a viewport, padded by `overscan` pixels above and below.
 * The result is bounded by the viewport height plus overscan, never by item count.
 */
export function visibleRange(
  offsets: readonly number[], heights: readonly number[],
  scrollTop: number, viewportHeight: number, overscan: number,
): VirtualRange {
  const count = offsets.length;
  if (count === 0) return {start: 0, end: 0};
  const top = Math.max(0, scrollTop - overscan);
  const bottom = scrollTop + Math.max(viewportHeight, 0) + overscan;
  let low = 0;
  let high = count - 1;
  let start = count - 1;
  while (low <= high) {
    const middle = (low + high) >> 1;
    if (offsets[middle] + heights[middle] > top) { start = middle; high = middle - 1; }
    else low = middle + 1;
  }
  let end = start;
  while (end < count && offsets[end] < bottom) end++;
  return {start, end: Math.max(end, start + 1)};
}

/** Last index whose offset is at or above `offset`; clamped to the first item. */
export function indexAtOffset(offsets: readonly number[], offset: number): number {
  if (offsets.length === 0) return -1;
  let low = 0;
  let high = offsets.length - 1;
  let found = 0;
  while (low <= high) {
    const middle = (low + high) >> 1;
    if (offsets[middle] <= offset) { found = middle; low = middle + 1; }
    else high = middle - 1;
  }
  return found;
}

/**
 * The item under the top of the viewport, with the distance from its top to the
 * viewport top. Restoring that pair keeps the reader's place across prepends and
 * live updates. `prefix` is the measured height of the head slot.
 */
export function anchorOffset(
  layout: VirtualLayout, prefix: number, scrollTop: number,
): {key: string; delta: number} | null {
  if (layout.keys.length === 0) return null;
  const index = indexAtOffset(layout.offsets, scrollTop - prefix);
  return {key: layout.keys[index], delta: scrollTop - prefix - layout.offsets[index]};
}

/** Scroll offset that keeps `anchor` visually where it was, in the new layout. */
export function restoredScrollTop(
  layout: VirtualLayout, prefix: number,
  anchor: {key: string; delta: number}, fallbackIndex: number,
): number {
  if (layout.keys.length === 0) return 0;
  let index = layout.keys.indexOf(anchor.key);
  if (index < 0) index = Math.min(Math.max(fallbackIndex, 0), layout.keys.length - 1);
  return Math.max(0, prefix + layout.offsets[index] + anchor.delta);
}
