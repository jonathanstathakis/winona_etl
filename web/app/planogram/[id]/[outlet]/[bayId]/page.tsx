"use client";
import React, { useState, useEffect, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import PrintDialog from "../../../components/PrintDialog";
import { DragDropProvider, DragOverlay } from "@dnd-kit/react";
import { useDraggable, useDroppable } from "@dnd-kit/react";

type Item = { id: string; sku: string; name: string; tags?: string; active?: number };
type BayLayout = (Item | null)[][];
type BayNavItem = { id: string; name: string };
type DropSide = "left" | "right";
type DragSource = { source: "sidebar" } | { source: "slot"; shelf: number; slot: number };
type DraggableData = Item & DragSource;

const NAVY = "#2E5FA3";
const DEFAULT_SLOTS = 7;

const btnStyle: React.CSSProperties = {
  padding: "6px 14px", fontSize: 13, cursor: "pointer",
  border: "1px solid #ccc", borderRadius: 4, background: "#f5f5f5",
};

function makeEmptyBayLayout(shelves: number[]): BayLayout {
  return shelves.map((n) => Array.from({ length: n }, () => null));
}

function reconcileLayout(old: BayLayout, newShelves: number[]): BayLayout {
  return newShelves.map((count, i) => {
    const row = old[i] ?? [];
    const next: (Item | null)[] = Array(count).fill(null);
    for (let j = 0; j < Math.min(count, row.length); j++) next[j] = row[j];
    return next;
  });
}

function wineColor(tags?: string): string {
  const t = (tags ?? "").toLowerCase();
  if (t.includes("red wine"))    return "#7B1C1C";
  if (t.includes("white wine"))  return "#BDB76B";
  if (t.includes("orange wine")) return "#C17B2A";
  if (t.includes("rose wine"))   return "#DB7093";
  return "#2D5A27";
}

function cascadeShift(slots: (Item | null)[], targetIdx: number, item: Item, dir: "left" | "right"): (Item | null)[] | null {
  if (dir === "right") {
    let emptyIdx = -1;
    for (let i = targetIdx + 1; i < slots.length; i++) { if (!slots[i]) { emptyIdx = i; break; } }
    if (emptyIdx === -1) return null;
    const next = [...slots];
    for (let i = emptyIdx; i > targetIdx; i--) next[i] = next[i - 1];
    next[targetIdx] = item;
    return next;
  } else {
    let emptyIdx = -1;
    for (let i = targetIdx - 1; i >= 0; i--) { if (!slots[i]) { emptyIdx = i; break; } }
    if (emptyIdx === -1) return null;
    const next = [...slots];
    for (let i = emptyIdx; i < targetIdx; i++) next[i] = next[i + 1];
    next[targetIdx] = item;
    return next;
  }
}

function insertOnShelf(slots: (Item | null)[], targetIdx: number, item: Item, dropSide: DropSide, sourceSlotIdx: number | null): (Item | null)[] {
  const work = [...slots];
  if (!work[targetIdx]) { work[targetIdx] = item; return work; }
  const preferred: "left" | "right" = dropSide === "left" ? "right" : "left";
  const opposite: "left" | "right" = preferred === "right" ? "left" : "right";
  let result = cascadeShift(work, targetIdx, item, preferred);
  if (result) return result;
  result = cascadeShift(work, targetIdx, item, opposite);
  if (result) return result;
  const displaced = work[targetIdx];
  work[targetIdx] = item;
  if (sourceSlotIdx !== null) work[sourceSlotIdx] = displaced;
  return work;
}

const BOTTLE_PATH_D =
  "m237.11-14.802c-2.0828 1.5833-3.2235 5.4484-3.6875 12.469-0.2329 3.5228-0.9539 7.0414-1.5938 7.8125-0.7091 0.85434-1.3578 7.0229-1.6562 15.813-0.414 12.191-0.2161 14.976 1.2187 18 0.7543 1.5895 1.1648 3.5781 1.2188 13.156l-6.83 188.74c0.48973 7.3004-1.2397 13.334-4.8704 19.598-2.9314 4.7479-7.6972 10.506-16.996 21.221-37.168 39.202-55.265 89.471-58.625 142.53l0.1562 529.78c1.7751 13.738 8.7702 35.164 19.781 39.781h230c11.011-4.6175 18.006-26.043 19.781-39.781l0.1563-529.78c-3.3599-53.061-21.458-103.33-58.625-142.53-10.854-12.506-15.532-18.26-18.326-23.536-3.1276-4.949-3.2868-10.244-3.49-15.865l-6.87-190.15c0.0539-9.5782 0.4645-11.567 1.2187-13.156 1.4348-3.0236 1.6328-5.8086 1.2188-18-0.2984-8.7896-0.9472-14.958-1.6563-15.813-0.6399-0.77108-1.3609-4.2897-1.5937-7.8125-0.4641-7.0203-1.6048-10.885-3.6875-12.469-28.65-2.2076-57.603-2.4136-86.25 0zm72.031 61.112h5v180h-5v-180zm56 388v521h-6v-521h6zm21 0v521h-13v-521h13z";

const WineBottle = React.forwardRef<SVGSVGElement, { color?: string; style?: React.CSSProperties }>(
  ({ color = "#2D5A27", style }, ref) => (
    <svg ref={ref} viewBox="0 0 66.758 250" style={style} xmlns="http://www.w3.org/2000/svg">
      <g transform="translate(116.44 -602.98)">
        <g transform="matrix(.24737 0 0 .24737 -176.37 598.08)">
          <g transform="translate(96.975 36.365)">
            <rect fill="#fff" height={560} width={80} y={403.79} x={322.86} />
            <rect fill="#fff" height={234.29} width={45.714} y={26.648} x={277.14} />
            <path fill={color} d={BOTTLE_PATH_D} />
          </g>
        </g>
      </g>
    </svg>
  ),
);
WineBottle.displayName = "WineBottle";

function DraggableItem({ item, placed }: { item: Item; placed: boolean }) {
  const { ref } = useDraggable({ id: item.id, data: { ...item, source: "sidebar" as const }, disabled: placed });
  return (
    <div ref={ref} style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 0", opacity: placed ? 0.4 : 1, cursor: "grab" }}>
      <WineBottle color={wineColor(item.tags)} style={{ width: 20, height: 40, flexShrink: 0 }} />
      <div style={{ fontSize: 12 }}>
        <div>{item.name}</div>
        <div style={{ color: "#888" }}>{item.sku}</div>
      </div>
    </div>
  );
}

function Bay({ layout, shelves, onRemove }: { layout: BayLayout; shelves: number[]; onRemove: (shelf: number, slot: number) => void }) {
  return (
    <div style={{ display: "inline-flex", flexDirection: "column", border: `8px solid ${NAVY}`, padding: 4 }}>
      {shelves.map((_, i) => (
        <Shelf key={i} slots={layout[i] ?? []} index={i} onRemove={onRemove} />
      ))}
    </div>
  );
}

function Shelf({ slots, index, onRemove }: { slots: (Item | null)[]; index: number; onRemove: (shelf: number, slot: number) => void }) {
  return (
    <div style={{ display: "flex", flexDirection: "row", gap: 4, alignItems: "center", borderBottom: `12px solid ${NAVY}`, paddingBottom: 4, marginBottom: 4 }}>
      <p style={{ textAlign: "center", width: 20, margin: 0 }}>{index + 1}</p>
      {slots.map((item, i) => <Slot key={i} item={item} index={i} shelfIndex={index} onRemove={onRemove} />)}
    </div>
  );
}

function Slot({ item, index, shelfIndex, onRemove }: { item: Item | null; index: number; shelfIndex: number; onRemove: (shelf: number, slot: number) => void }) {
  const [hovered, setHovered] = useState(false);
  const dndId = `${shelfIndex}-${index}`;
  const { ref: dropRef } = useDroppable({ id: dndId, data: { shelf: shelfIndex, slot: index } });
  const { ref: dragRef } = useDraggable({
    id: item?.id ?? dndId,
    data: item ? { ...item, source: "slot" as const, shelf: shelfIndex, slot: index } : undefined,
    disabled: !item,
  });

  return (
    <div id={`slot-${shelfIndex}-${index}`} ref={dropRef}
      onMouseEnter={() => setHovered(true)} onMouseLeave={() => setHovered(false)}
      style={{ position: "relative", width: 90, flexShrink: 0, height: 220, border: item?.active === 0 ? "2px solid #f0a500" : "1px dashed gray", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "flex-start", padding: "4px", boxSizing: "border-box" }}>
      {item ? (
        <>
          {hovered && (
            <button onClick={() => onRemove(shelfIndex, index)}
              style={{ position: "absolute", top: 2, right: 2, width: 16, height: 16, padding: 0, lineHeight: 1, fontSize: 11, cursor: "pointer", border: "none", borderRadius: 2, background: "#e55", color: "#fff" }}>×</button>
          )}
          {item.active === 0 && (
            <div style={{ position: "absolute", top: 2, left: 2, fontSize: 8, fontWeight: 700, color: "#f0a500", lineHeight: 1 }}>inactive</div>
          )}
          <div ref={dragRef} style={{ display: "flex", flexDirection: "column", alignItems: "center", cursor: "grab", width: "100%" }} title={item.name}>
            <WineBottle color={wineColor(item.tags)} style={{ width: 40, height: 100, display: "block", marginTop: 4 }} />
            <div style={{ marginTop: 6, width: "100%", textAlign: "center", padding: "0 3px", boxSizing: "border-box" }}>
              <div style={{ fontSize: 11, fontWeight: 600, lineHeight: 1.3, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 3, WebkitBoxOrient: "vertical" }}>{item.name}</div>
              <div style={{ fontSize: 10, color: "#666", marginTop: 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.sku}</div>
            </div>
          </div>
        </>
      ) : (
        <p style={{ margin: "auto", fontSize: 11, color: "#ccc" }}>{index + 1}</p>
      )}
    </div>
  );
}

export default function BayPlanogramEditor() {
  const { id: layoutId, outlet, bayId } = useParams<{ id: string; outlet: string; bayId: string }>();
  const router = useRouter();

  const [bayName, setBayName] = useState<string>("");
  const [allBays, setAllBays] = useState<BayNavItem[]>([]);
  const [shelves, setShelves] = useState<number[]>([]);
  const [layout, setLayout] = useState<BayLayout>([]);
  const [products, setProducts] = useState<Item[]>([]);
  const [listMode, setListMode] = useState<"all" | "unplaced">("unplaced");
  const [activeFilter, setActiveFilter] = useState<"all" | "active" | "inactive">("all");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [isDirty, setIsDirty] = useState(false);
  const [dragItem, setDragItem] = useState<DraggableData | null>(null);
  const [printOpen, setPrintOpen] = useState(false);
  const pointerXRef = useRef(0);
  const PAGE_SIZE = 50;

  useEffect(() => {
    const handler = (e: PointerEvent) => { pointerXRef.current = e.clientX; };
    window.addEventListener("pointermove", handler);
    return () => window.removeEventListener("pointermove", handler);
  }, []);

  useEffect(() => {
    Promise.all([
      fetch("/api/mart/wine").then((r) => r.json()),
      fetch(`/api/planogram/layouts/${layoutId}/bays/${bayId}`).then((r) => r.json()),
      fetch(`/api/planogram/bays?outlet=${outlet}`).then((r) => r.json()),
    ]).then(([prods, planData, baysData]) => {
      setProducts(prods);

      const bayMeta = baysData.find((b: any) => b.id === bayId);
      setBayName(bayMeta?.name ?? "");
      setAllBays(baysData.map((b: any) => ({ id: b.id, name: b.name })));

      const shelfCounts = [...(planData.shelves ?? [])].sort((a: any, b: any) => a.shelf_index - b.shelf_index).map((s: any) => s.slot_count);
      setShelves(shelfCounts);

      const newLayout = makeEmptyBayLayout(shelfCounts);
      const bysku = new Map<string, Item>(prods.map((p: Item) => [p.sku, p]));
      for (const p of planData.placements as { shelf_index: number; slot_index: number; sku: string }[]) {
        const shelf = newLayout[p.shelf_index];
        if (!shelf || p.slot_index >= shelf.length) continue;
        const item = bysku.get(p.sku);
        if (item) shelf[p.slot_index] = item;
      }
      setLayout(newLayout);
      setIsDirty(false);
    });
  }, [layoutId, bayId, outlet]);

  const placedIds = new Set(layout.flat().filter(Boolean).map((i) => i!.id));
  const searchLower = search.toLowerCase();
  const listedProducts = (listMode === "unplaced" ? products.filter((p) => !placedIds.has(p.id)) : products)
    .filter((p) => !searchLower || p.name.toLowerCase().includes(searchLower) || p.sku.toLowerCase().includes(searchLower))
    .filter((p) => {
      if (activeFilter === "active")   return p.active !== 0;
      if (activeFilter === "inactive") return p.active === 0;
      return true;
    });
  const totalPages = Math.ceil(listedProducts.length / PAGE_SIZE);
  const visibleProducts = listedProducts.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  function handleDragStart(event: any) { setDragItem(event.operation.source.data as DraggableData); }

  function handleDragEnd(event: any) {
    setDragItem(null);
    if (event.canceled || !event.operation.target) return;
    const sourceData = event.operation.source.data as DraggableData;
    const target = event.operation.target.data as { shelf: number; slot: number };
    const item: Item = { id: sourceData.id, sku: sourceData.sku, name: sourceData.name, tags: sourceData.tags, active: sourceData.active };

    const nextLayout = layout.map((s) => [...s]);
    if (sourceData.source === "slot") nextLayout[sourceData.shelf][sourceData.slot] = null;

    const isSameShelf = sourceData.source === "slot" && sourceData.shelf === target.shelf;
    const sourceSlotIdx = isSameShelf ? sourceData.slot : null;

    const slotEl = document.getElementById(`slot-${target.shelf}-${target.slot}`);
    const rect = slotEl?.getBoundingClientRect();
    const side: DropSide = rect && pointerXRef.current < rect.left + rect.width / 2 ? "left" : "right";

    nextLayout[target.shelf] = insertOnShelf(nextLayout[target.shelf], target.slot, item, side, sourceSlotIdx);
    setLayout(nextLayout);
    setIsDirty(true);
  }

  function handleRemoveItem(shelf: number, slot: number) {
    setLayout((prev) => prev.map((s, si) => s.map((cell, sli) => (si === shelf && sli === slot ? null : cell))));
    setIsDirty(true);
  }

  function handleClearInactive() {
    setLayout((prev) => prev.map((shelf) => shelf.map((cell) => (cell?.active === 0 ? null : cell))));
    setIsDirty(true);
  }

  async function handleSave() {
    const placements = layout.flatMap((shelfSlots, si) =>
      shelfSlots.flatMap((item, sli) => item ? [{ shelf_index: si, slot_index: sli, sku: item.sku }] : [])
    );
    const shelvesBody = shelves.map((slot_count, shelf_index) => ({ shelf_index, slot_count }));
    await fetch(`/api/planogram/layouts/${layoutId}/bays/${bayId}/save`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ shelves: shelvesBody, placements }),
    });
    setIsDirty(false);
  }

  const hasInactive = layout.some((s) => s.some((c) => c?.active === 0));

  return (
    <DragDropProvider onDragStart={handleDragStart} onDragEnd={handleDragEnd}>
      <div style={{ padding: 16 }}>
        {/* toolbar */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
          <button onClick={() => router.push(`/planogram/${layoutId}/${outlet}`)} style={{ ...btnStyle, marginRight: 4 }}>← Floor plan</button>
          <select value={bayId} onChange={(e) => router.push(`/planogram/${layoutId}/${outlet}/${e.target.value}`)}
            style={{ padding: "4px 8px", fontSize: 13, border: "1px solid #ccc", borderRadius: 4, cursor: "pointer" }}>
            {allBays.map((b) => <option key={b.id} value={b.id}>{b.name}</option>)}
          </select>
          <button onClick={() => setPrintOpen(true)} style={btnStyle}>Print…</button>
          <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>{bayName || "Bay"}</h1>
          <div style={{ flex: 1 }} />
          {hasInactive && (
            <button onClick={handleClearInactive} style={{ ...btnStyle, color: "#f0a500", borderColor: "#f0a500" }}>Clear inactive</button>
          )}
          <button onClick={handleSave} disabled={!isDirty} style={btnStyle}>Save</button>
        </div>

        {/* main content */}
        <div style={{ display: "flex", gap: 16 }}>
          {/* sidebar */}
          <div style={{ width: 200, flexShrink: 0, height: "80vh", overflowY: "auto", borderRight: "1px solid #ccc", paddingRight: 12 }}>
            <div style={{ display: "flex", gap: 4, marginBottom: 6 }}>
              {(["unplaced", "all"] as const).map((mode) => (
                <button key={mode} onClick={() => { setListMode(mode); setPage(0); }}
                  style={{ flex: 1, padding: "2px 0", fontSize: 11, fontWeight: listMode === mode ? "bold" : "normal", background: listMode === mode ? "#333" : "#eee", color: listMode === mode ? "#fff" : "#333", border: "none", borderRadius: 3, cursor: "pointer" }}>
                  {mode === "all" ? "All" : "Unplaced"}
                </button>
              ))}
            </div>
            <div style={{ display: "flex", gap: 4, marginBottom: 6 }}>
              {(["all", "active", "inactive"] as const).map((f) => (
                <button key={f} onClick={() => { setActiveFilter(f); setPage(0); }}
                  style={{ flex: 1, padding: "2px 0", fontSize: 11, fontWeight: activeFilter === f ? "bold" : "normal", background: activeFilter === f ? (f === "inactive" ? "#f0a500" : "#333") : "#eee", color: activeFilter === f ? "#fff" : "#333", border: "none", borderRadius: 3, cursor: "pointer" }}>
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
            <input type="text" placeholder="Search name or SKU…" value={search}
              onChange={(e) => { setSearch(e.target.value); setPage(0); }}
              style={{ width: "100%", marginBottom: 8, padding: "3px 6px", fontSize: 11, boxSizing: "border-box", border: "1px solid #ccc", borderRadius: 3 }} />
            {visibleProducts.map((product) => (
              <DraggableItem key={product.id} item={product} placed={placedIds.has(product.id)} />
            ))}
            {totalPages > 1 && (
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 8, fontSize: 12 }}>
                <button onClick={() => setPage((p) => Math.max(0, p - 1))} disabled={page === 0}>‹</button>
                <span>{page + 1} / {totalPages}</span>
                <button onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))} disabled={page === totalPages - 1}>›</button>
              </div>
            )}
          </div>

          {/* bay view */}
          <div style={{ flex: 1, minWidth: 0, overflow: "auto", height: "80vh" }}>
            {shelves.length > 0 ? (
              <Bay layout={layout} shelves={shelves} onRemove={handleRemoveItem} />
            ) : (
              <div style={{ color: "#aaa", fontSize: 13, padding: 20 }}>Loading bay…</div>
            )}
          </div>
        </div>
      </div>

      <DragOverlay>
        {() => dragItem ? (
          <div style={{ width: 90, height: 220, border: "1px solid #ccc", background: "#fff", display: "flex", flexDirection: "column", alignItems: "center", padding: 4, boxSizing: "border-box", opacity: 0.85, boxShadow: "0 4px 12px rgba(0,0,0,0.2)", transform: "translate(-50%, -80%)" }}>
            <WineBottle color={wineColor(dragItem.tags)} style={{ width: 40, height: 100, display: "block", marginTop: 4 }} />
            <div style={{ marginTop: 6, width: "100%", textAlign: "center", padding: "0 3px", boxSizing: "border-box" }}>
              <div style={{ fontSize: 11, fontWeight: 600, lineHeight: 1.3, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 3, WebkitBoxOrient: "vertical" }}>{dragItem.name}</div>
              <div style={{ fontSize: 10, color: "#666", marginTop: 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{dragItem.sku}</div>
            </div>
          </div>
        ) : null}
      </DragOverlay>
      <PrintDialog open={printOpen} onClose={() => setPrintOpen(false)} layoutId={layoutId} outlet={outlet} bayId={bayId} />
    </DragDropProvider>
  );
}
