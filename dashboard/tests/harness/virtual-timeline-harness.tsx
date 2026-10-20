/*
 * Browser harness for VirtualTimeline. Served by the Vite dev server during
 * dashboard/tests/virtualization.spec.ts so the windowing rules can be exercised
 * with a large, variable-height dataset and without depending on the app shell.
 *
 * The harness intentionally passes an inline `getKey`/`renderItem` (new identity
 * on every render) and a freshly built `items` array, matching how main.tsx
 * renders the timeline, so key-stability and anchor preservation are covered.
 */
import React, {useCallback, useEffect, useRef, useState} from "react";
import {createRoot} from "react-dom/client";
import {VirtualTimeline} from "../../src/VirtualTimeline";

type Row = {id: string; label: string; tall: boolean};

const INITIAL_ROWS = 1200;
const TALL_EVERY = 37;

export type HarnessControls = {
  prepend: (count: number) => void;
  append: (count: number) => void;
  clear: () => void;
  setLoading: (value: boolean) => void;
  setHeaderExtra: (value: boolean) => void;
};

function initialRow(index: number): Row {
  return {id: `row-${index}`, label: `Row ${index}`, tall: index % TALL_EVERY === 0};
}

function Harness() {
  const [rows, setRows] = useState<Row[]>(() => Array.from({length: INITIAL_ROWS}, (_, index) => initialRow(index)));
  const [loading, setLoading] = useState(false);
  const [headerExtra, setHeaderExtra] = useState(false);
  const older = useRef(0);
  const newer = useRef(INITIAL_ROWS);

  const prepend = useCallback((count: number) => {
    setRows(current => {
      const added: Row[] = [];
      for (let index = 0; index < count; index++) {
        added.push({id: `older-${older.current++}`, label: "Earlier page row", tall: false});
      }
      return [...added, ...current];
    });
  }, []);

  const append = useCallback((count: number) => {
    setRows(current => {
      const added: Row[] = [];
      for (let index = 0; index < count; index++) {
        added.push({id: `newer-${newer.current++}`, label: "Latest row", tall: false});
      }
      return [...current, ...added];
    });
  }, []);

  const clear = useCallback(() => setRows([]), []);

  useEffect(() => {
    const controls: HarnessControls = {prepend, append, clear, setLoading, setHeaderExtra};
    (window as unknown as {harness?: HarnessControls}).harness = controls;
    return () => { delete (window as unknown as {harness?: HarnessControls}).harness; };
  }, [prepend, append, clear]);

  return (
    <VirtualTimeline
      className="harness-timeline"
      items={rows}
      getKey={row => row.id}
      estimatedItemHeight={140}
      overscan={300}
      loading={loading}
      ariaLabel="Harness timeline"
      header={<div className="header-extra">{headerExtra
        ? "Expanded head slot: this text makes the measured head taller so item offsets must be recomputed."
        : "Head slot"}</div>}
      empty={<div className="empty" role="status">No rows loaded.</div>}
      loadingSlot={<div className="loading" role="status">Loading earlier rows…</div>}
      renderItem={row => (
        <div className={row.tall ? "row tall" : "row short"} data-row-key={row.id}>
          <span>{row.label}</span>
          <button type="button" className="row-action">Action {row.id}</button>
        </div>
      )}
    />
  );
}

createRoot(document.getElementById("root")!).render(<Harness/>);
