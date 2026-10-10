import {api} from "./api";

export type TimelineItem = {id: string; created_seq: number};
export type TimelinePage<T> = {items: T[]; next_before: number | null};

export async function loadTimeline<T extends TimelineItem>(
  path: string, pages: number, oldest?: number,
): Promise<TimelinePage<T> & {page_count: number}> {
  let before: number | null = null;
  let items: T[] = [];
  let count = 0;
  do {
    const result: TimelinePage<T> = await api(`${path}${before === null ? "" : `&before=${before}`}`);
    items = [...result.items, ...items];
    before = result.next_before;
    count++;
  } while (before !== null && count < 20 && (count < pages || (oldest !== undefined && before > oldest)));
  return {items: [...new Map(items.map(item => [item.id, item])).values()],
    next_before: before, page_count: count};
}
