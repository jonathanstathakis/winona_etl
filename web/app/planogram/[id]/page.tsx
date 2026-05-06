// TODO: export to csv
"use client";
import React, { useState, useEffect, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { DragDropProvider, DragOverlay } from "@dnd-kit/react";
import { useDraggable, useDroppable } from "@dnd-kit/react";

type Outlet = "rozelle" | "avalon" | "manly";
type Item = { id: string; sku: string; name: string; tags?: string };
type BayLayout = (Item | null)[][];
type DropSide = "left" | "right";
type DragSource =
  | { source: "sidebar" }
  | { source: "slot"; shelf: number; slot: number };
type DraggableData = Item & DragSource;

type BayConfig = {
  id: string;
  outlet: Outlet;
  name: string;
  description: string;
  shelves: number[];
};

type PlanogramMeta = {
  id: string;
  outlet: Outlet;
  name: string;
  status: "draft" | "active" | "archived";
};

type PlacementRaw = {
  bay_id: string;
  shelf_index: number;
  slot_index: number;
  sku: string;
};

const OUTLETS: Outlet[] = ["rozelle", "avalon", "manly"];
const DEFAULT_SLOTS = 7;
const NAVY = "#2E5FA3";

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

function apiBayToConfig(b: any): BayConfig {
  const shelves = [...(b.shelves ?? [])]
    .sort((a: any, b: any) => a.shelf_index - b.shelf_index)
    .map((s: any) => s.slot_count);
  return { id: b.id, outlet: b.outlet, name: b.name, description: b.description, shelves };
}

function configToApiBay(c: BayConfig) {
  return {
    outlet: c.outlet,
    name: c.name,
    description: c.description,
    shelves: c.shelves.map((count, idx) => ({ shelf_index: idx, slot_count: count })),
  };
}

function wineColor(tags?: string): string {
  const t = (tags ?? "").toLowerCase();
  if (t.includes("red wine")) return "#7B1C1C";
  if (t.includes("white wine")) return "#BDB76B";
  if (t.includes("orange wine")) return "#C17B2A";
  if (t.includes("rose wine")) return "#DB7093";
  return "#2D5A27";
}

function nextCopyName(name: string, existing: BayConfig[]): string {
  const base = name.replace(/ \(\d+\)$/, "");
  const used = new Set(
    existing.flatMap((b) => {
      const m = b.name.match(/^(.+) \((\d+)\)$/);
      return m && m[1] === base ? [parseInt(m[2])] : [];
    }),
  );
  let n = 1;
  while (used.has(n)) n++;
  return `${base} (${n})`;
}

function buildPlacements(bayLayouts: Record<string, BayLayout>): PlacementRaw[] {
  const out: PlacementRaw[] = [];
  for (const [bay_id, layout] of Object.entries(bayLayouts)) {
    for (let si = 0; si < layout.length; si++) {
      for (let sli = 0; sli < layout[si].length; sli++) {
        const item = layout[si][sli];
        if (item) out.push({ bay_id, shelf_index: si, slot_index: sli, sku: item.sku });
      }
    }
  }
  return out;
}

function cascadeShift(
  slots: (Item | null)[],
  targetIdx: number,
  item: Item,
  dir: "left" | "right",
): (Item | null)[] | null {
  if (dir === "right") {
    let emptyIdx = -1;
    for (let i = targetIdx + 1; i < slots.length; i++) {
      if (!slots[i]) { emptyIdx = i; break; }
    }
    if (emptyIdx === -1) return null;
    const next = [...slots];
    for (let i = emptyIdx; i > targetIdx; i--) next[i] = next[i - 1];
    next[targetIdx] = item;
    return next;
  } else {
    let emptyIdx = -1;
    for (let i = targetIdx - 1; i >= 0; i--) {
      if (!slots[i]) { emptyIdx = i; break; }
    }
    if (emptyIdx === -1) return null;
    const next = [...slots];
    for (let i = emptyIdx; i < targetIdx; i++) next[i] = next[i + 1];
    next[targetIdx] = item;
    return next;
  }
}

function insertOnShelf(
  slots: (Item | null)[],
  targetIdx: number,
  item: Item,
  dropSide: DropSide,
  sourceSlotIdx: number | null,
): (Item | null)[] {
  const work = [...slots];
  if (!work[targetIdx]) {
    work[targetIdx] = item;
    return work;
  }
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

// --- NameDialog ---

function NameDialog({
  title,
  confirmLabel,
  onConfirm,
  onCancel,
}: {
  title: string;
  confirmLabel: string;
  onConfirm: (name: string) => void;
  onCancel: () => void;
}) {
  const [name, setName] = useState("");
  return (
    <div
      style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1100 }}
      onClick={(e) => { if (e.target === e.currentTarget) onCancel(); }}
    >
      <div style={{ background: "#fff", borderRadius: 6, padding: 24, width: 320 }}>
        <h3 style={{ margin: "0 0 12px", fontSize: 16 }}>{title}</h3>
        <input
          autoFocus
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Name"
          style={{ width: "100%", padding: "6px 8px", fontSize: 14, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box", marginBottom: 16 }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && name.trim()) onConfirm(name.trim());
            if (e.key === "Escape") onCancel();
          }}
        />
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button onClick={onCancel} style={btnStyle}>Cancel</button>
          <button
            onClick={() => name.trim() && onConfirm(name.trim())}
            disabled={!name.trim()}
            style={{ ...btnStyle, background: "#333", color: "#fff", border: "none" }}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

// --- BayModal ---

const labelStyle: React.CSSProperties = {
  display: "block", fontSize: 12, fontWeight: "bold", marginBottom: 4, marginTop: 12,
};

const inputStyle: React.CSSProperties = {
  width: "100%", padding: "5px 8px", fontSize: 13,
  border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box",
};

const btnStyle: React.CSSProperties = {
  padding: "6px 14px", fontSize: 13, cursor: "pointer",
  border: "1px solid #ccc", borderRadius: 4, background: "#f5f5f5",
};

type BayModalProps = {
  open: boolean;
  bayConfigs: BayConfig[];
  defaultOutlet: Outlet;
  onClose: () => void;
  onSave: (config: BayConfig) => void;
  onDelete: (id: string) => void;
  onDuplicate: (config: BayConfig) => void;
  onReorder: (outlet: Outlet, orderedIds: string[]) => void;
};

function BayModal({ open, bayConfigs, defaultOutlet, onClose, onSave, onDelete, onDuplicate, onReorder }: BayModalProps) {
  const [view, setView] = useState<"list" | "form">("list");
  const [editing, setEditing] = useState<BayConfig | null>(null);
  const [formOutlet, setFormOutlet] = useState<Outlet>(defaultOutlet);
  const [formName, setFormName] = useState("");
  const [formDesc, setFormDesc] = useState("");
  const [formShelves, setFormShelves] = useState<number[]>([7, 7, 7, 7, 7]);
  const [dragIndex, setDragIndex] = useState<{ outlet: Outlet; i: number } | null>(null);
  const [overIndex, setOverIndex] = useState<{ outlet: Outlet; i: number } | null>(null);

  useEffect(() => { if (open) setView("list"); }, [open]);

  function startNew() {
    setEditing(null);
    setFormOutlet(defaultOutlet);
    setFormName("");
    setFormDesc("");
    setFormShelves([7, 7, 7, 7, 7]);
    setView("form");
  }

  function startEdit(config: BayConfig) {
    setEditing(config);
    setFormOutlet(config.outlet);
    setFormName(config.name);
    setFormDesc(config.description);
    setFormShelves([...config.shelves]);
    setView("form");
  }

  function handleSave() {
    onSave({
      id: editing?.id ?? "",
      outlet: formOutlet,
      name: formName,
      description: formDesc,
      shelves: formShelves,
    });
    setView("list");
  }

  if (!open) return null;

  return (
    <div
      style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div style={{ background: "#fff", borderRadius: 6, padding: 24, width: 480, maxHeight: "80vh", overflowY: "auto", boxSizing: "border-box" }}>
        {view === "list" ? (
          <>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <h2 style={{ margin: 0, fontSize: 18 }}>Bays</h2>
              <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 18, cursor: "pointer" }}>×</button>
            </div>
            {OUTLETS.map((o) => {
              const obs = bayConfigs.filter((b) => b.outlet === o);
              return (
                <div key={o} style={{ marginBottom: 16 }}>
                  <div style={{ fontSize: 12, fontWeight: "bold", textTransform: "capitalize", color: "#888", marginBottom: 6 }}>{o}</div>
                  {obs.length === 0 ? (
                    <div style={{ fontSize: 12, color: "#ccc", paddingLeft: 4 }}>No bays</div>
                  ) : obs.map((b, i) => {
                    const isDragging = dragIndex?.outlet === o && dragIndex.i === i;
                    const isOver = overIndex?.outlet === o && overIndex.i === i;
                    return (
                      <div
                        key={b.id}
                        draggable
                        onDragStart={() => setDragIndex({ outlet: o, i })}
                        onDragOver={(e) => { e.preventDefault(); setOverIndex({ outlet: o, i }); }}
                        onDrop={() => {
                          if (!dragIndex || dragIndex.outlet !== o) return;
                          const reordered = [...obs];
                          const [moved] = reordered.splice(dragIndex.i, 1);
                          reordered.splice(i, 0, moved);
                          setDragIndex(null);
                          setOverIndex(null);
                          onReorder(o, reordered.map((b) => b.id));
                        }}
                        onDragEnd={() => { setDragIndex(null); setOverIndex(null); }}
                        style={{
                          display: "flex", justifyContent: "space-between", alignItems: "center",
                          padding: "6px 0", borderBottom: "1px solid #eee",
                          opacity: isDragging ? 0.4 : 1,
                          outline: isOver ? "2px solid #333" : "none",
                          cursor: "grab",
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ color: "#bbb", fontSize: 16, cursor: "grab", userSelect: "none" }}>⠿</span>
                          <div>
                            <div style={{ fontSize: 14 }}>{b.name || "(unnamed)"}</div>
                            {b.description && <div style={{ fontSize: 12, color: "#888" }}>{b.description}</div>}
                            <div style={{ fontSize: 11, color: "#aaa" }}>{b.shelves.length} shelves · slots: {b.shelves.join(", ")}</div>
                          </div>
                        </div>
                        <div style={{ display: "flex", gap: 4 }}>
                          <button onClick={() => onDuplicate(b)} style={{ fontSize: 12, padding: "3px 10px", cursor: "pointer", border: "1px solid #ccc", borderRadius: 3, background: "#f5f5f5" }}>
                            Copy
                          </button>
                          <button onClick={() => startEdit(b)} style={{ fontSize: 12, padding: "3px 10px", cursor: "pointer", border: "1px solid #ccc", borderRadius: 3, background: "#f5f5f5" }}>
                            Edit
                          </button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            })}
            <button onClick={startNew} style={{ marginTop: 8, width: "100%", padding: "8px 0", fontSize: 13, cursor: "pointer", border: "1px dashed #aaa", borderRadius: 4, background: "none" }}>
              + Add new bay
            </button>
          </>
        ) : (
          <>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
              <button onClick={() => setView("list")} style={{ background: "none", border: "none", fontSize: 18, cursor: "pointer", padding: 0 }}>←</button>
              <h2 style={{ margin: 0, fontSize: 18 }}>{editing ? "Edit Bay" : "New Bay"}</h2>
            </div>
            <label style={labelStyle}>Outlet</label>
            <select value={formOutlet} onChange={(e) => setFormOutlet(e.target.value as Outlet)} style={inputStyle}>
              {OUTLETS.map((o) => <option key={o} value={o}>{o}</option>)}
            </select>
            <label style={labelStyle}>Name</label>
            <input type="text" value={formName} onChange={(e) => setFormName(e.target.value)} placeholder="e.g. Front wall" style={inputStyle} />
            <label style={labelStyle}>Description</label>
            <textarea value={formDesc} onChange={(e) => setFormDesc(e.target.value)} placeholder="Optional notes" rows={2} style={{ ...inputStyle, resize: "vertical" }} />
            <label style={{ ...labelStyle, marginBottom: 8 }}>Shelves</label>
            {formShelves.map((slots, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                <span style={{ fontSize: 12, width: 56, flexShrink: 0 }}>Shelf {i + 1}</span>
                <input
                  type="number" min={1} value={slots}
                  onChange={(e) => setFormShelves((s) => s.map((v, idx) => (idx === i ? Math.max(1, parseInt(e.target.value) || 1) : v)))}
                  style={{ width: 60, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 3 }}
                />
                <span style={{ fontSize: 12, color: "#888" }}>slots</span>
                <button
                  onClick={() => setFormShelves((s) => s.filter((_, idx) => idx !== i))}
                  disabled={formShelves.length === 1}
                  style={{ background: "none", border: "none", color: "#e55", cursor: "pointer", fontSize: 16, padding: 0, lineHeight: 1 }}
                >×</button>
              </div>
            ))}
            <button onClick={() => setFormShelves((s) => [...s, DEFAULT_SLOTS])} style={{ fontSize: 12, padding: "3px 10px", cursor: "pointer", border: "1px dashed #aaa", borderRadius: 3, background: "none", marginTop: 4 }}>
              + Add shelf
            </button>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
              {editing ? (
                <button onClick={() => { onDelete(editing.id); setView("list"); }} style={{ ...btnStyle, border: "1px solid #e55", color: "#e55", background: "none" }}>
                  Delete
                </button>
              ) : <div />}
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={() => setView("list")} style={btnStyle}>Cancel</button>
                <button onClick={handleSave} style={{ ...btnStyle, background: "#333", color: "#fff", border: "none" }}>Save</button>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// --- PDF export ---

async function exportPdf(
  planogram: PlanogramMeta,
  outlet: Outlet,
  bays: BayConfig[],
  layouts: Record<string, BayLayout>,
  exportedAt: number,
) {
  const { jsPDF } = await import("jspdf");
  const html2canvas = (await import("html2canvas")).default;

  const outletBays = bays.filter((b) => b.outlet === outlet);
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "a4" });
  const pageW = 210;
  const marginX = 15;
  const usableW = pageW - marginX * 2;

  const d = new Date(exportedAt);
  const pad = (n: number) => String(n).padStart(2, "0");
  const dateStr = `${pad(d.getDate())}-${pad(d.getMonth() + 1)}-${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;

  for (let i = 0; i < outletBays.length; i++) {
    const bay = outletBays[i];
    if (i > 0) doc.addPage();

    doc.setFont("helvetica", "bold");
    doc.setFontSize(16);
    doc.setTextColor(0);
    doc.text(planogram.name, marginX, 13);

    doc.setFont("helvetica", "normal");
    doc.setFontSize(9);
    doc.setTextColor(100);
    doc.text(`Exported ${dateStr}`, marginX, 19);

    doc.setDrawColor(0);
    doc.setLineWidth(0.4);
    doc.line(marginX, 23, pageW - marginX, 23);

    doc.setFont("helvetica", "bold");
    doc.setFontSize(22);
    doc.setTextColor(0);
    doc.text(`${outlet} · ${bay.name || "Bay"}`, marginX, 34);

    const container = document.createElement("div");
    container.style.cssText = "position:absolute;left:-9999px;top:0;background:#fff;padding:8px";
    document.body.appendChild(container);

    const { createRoot } = await import("react-dom/client");
    const ReactModule = await import("react");
    const root = createRoot(container);
    await new Promise<void>((resolve) => {
      root.render(ReactModule.createElement(PrintBay, { layout: layouts[bay.id] ?? [], shelves: bay.shelves }));
      setTimeout(resolve, 150);
    });

    const canvas = await html2canvas(container, { scale: 2, backgroundColor: "#fff", useCORS: true });
    root.unmount();
    document.body.removeChild(container);

    const imgData = canvas.toDataURL("image/png");
    const imgH = (canvas.height / canvas.width) * usableW;
    doc.addImage(imgData, "PNG", marginX, 40, usableW, imgH);
  }

  const total = doc.getNumberOfPages();
  for (let p = 1; p <= total; p++) {
    doc.setPage(p);
    doc.setFont("helvetica", "normal");
    doc.setFontSize(9);
    doc.setTextColor(150);
    doc.text(`${p} / ${total}`, pageW - marginX, 287, { align: "right" });
  }

  const safeName = planogram.name.replace(/[^a-z0-9]/gi, "_");
  doc.save(`${safeName}-${outlet}-${dateStr.replace(/[: ]/g, "-")}.pdf`);
}

// --- Page ---

export default function Page() {
  const { id: planogramId } = useParams<{ id: string }>();
  const router = useRouter();

  const [planogram, setPlanogram] = useState<PlanogramMeta | null>(null);
  const [bayConfigs, setBayConfigs] = useState<BayConfig[]>([]);
  const [bayLayouts, setBayLayouts] = useState<Record<string, BayLayout>>({});
  const [activeBayId, setActiveBayId] = useState<string | null>(null);
  const [bayModalOpen, setBayModalOpen] = useState(false);
  const [isDirty, setIsDirty] = useState(false);
  const [saveAsDialog, setSaveAsDialog] = useState(false);
  const [isExporting, setIsExporting] = useState(false);

  const [products, setProducts] = useState<Item[]>([]);
  const [listMode, setListMode] = useState<"all" | "unplaced">("unplaced");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;
  const pointerXRef = useRef(0);

  // pointer tracking for drop-side detection
  useEffect(() => {
    const handler = (e: PointerEvent) => { pointerXRef.current = e.clientX; };
    window.addEventListener("pointermove", handler);
    return () => window.removeEventListener("pointermove", handler);
  }, []);

  // load everything when planogramId changes
  useEffect(() => {
    if (!planogramId) return;
    setBayConfigs([]);
    setBayLayouts({});
    setActiveBayId(null);
    setPlanogram(null);

    Promise.all([
      fetch("/api/mart/wine").then((r) => r.json()),
      fetch(`/api/planogram/planograms/${planogramId}`).then((r) => r.json()),
    ]).then(([prods, planData]) => {
      setProducts(prods);
      const meta: PlanogramMeta = { id: planData.id, outlet: planData.outlet, name: planData.name, status: planData.status };
      setPlanogram(meta);

      fetch(`/api/planogram/bays?outlet=${planData.outlet}`)
        .then((r) => r.json())
        .then((baysData) => {
          const configs: BayConfig[] = baysData.map(apiBayToConfig);
          setBayConfigs(configs);
          setActiveBayId(configs[0]?.id ?? null);

          const newLayouts: Record<string, BayLayout> = {};
          for (const bay of configs) {
            newLayouts[bay.id] = makeEmptyBayLayout(bay.shelves);
          }
          const bysku = new Map<string, Item>(prods.map((p: Item) => [p.sku, p]));
          for (const p of planData.placements as PlacementRaw[]) {
            const layout = newLayouts[p.bay_id];
            if (!layout) continue;
            const shelf = layout[p.shelf_index];
            if (!shelf || p.slot_index >= shelf.length) continue;
            const item = bysku.get(p.sku);
            if (item) shelf[p.slot_index] = item;
          }
          setBayLayouts(newLayouts);
          setIsDirty(false);
        });
    });
  }, [planogramId]);

  const outlet = planogram?.outlet ?? "rozelle";
  const outletBays = bayConfigs.filter((b) => b.outlet === outlet);
  const activeBayConfig = bayConfigs.find((b) => b.id === activeBayId) ?? null;
  const activeBayLayout = activeBayId ? bayLayouts[activeBayId] ?? [] : [];

  const placedIds = new Set(
    outletBays.flatMap((b) => bayLayouts[b.id] ?? []).flat().filter(Boolean).map((i) => i!.id),
  );

  const searchLower = search.toLowerCase();
  const listedProducts = (
    listMode === "unplaced" ? products.filter((p) => !placedIds.has(p.id)) : products
  ).filter(
    (p) => !searchLower || p.name.toLowerCase().includes(searchLower) || p.sku.toLowerCase().includes(searchLower),
  );
  const totalPages = Math.ceil(listedProducts.length / PAGE_SIZE);
  const visibleProducts = listedProducts.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  // drag
  function handleDragEnd(event: any) {
    if (event.canceled || !event.operation.target || !activeBayId) return;
    const sourceData = event.operation.source.data as DraggableData;
    const target = event.operation.target.data as { shelf: number; slot: number };
    const item: Item = { id: sourceData.id, sku: sourceData.sku, name: sourceData.name, tags: sourceData.tags };

    const nextLayout = activeBayLayout.map((s) => [...s]);
    if (sourceData.source === "slot") nextLayout[sourceData.shelf][sourceData.slot] = null;

    const isSameShelf = sourceData.source === "slot" && sourceData.shelf === target.shelf;
    const sourceSlotIdx = isSameShelf ? sourceData.slot : null;

    const slotEl = document.getElementById(`slot-${target.shelf}-${target.slot}`);
    const rect = slotEl?.getBoundingClientRect();
    const side: DropSide = rect && pointerXRef.current < rect.left + rect.width / 2 ? "left" : "right";

    nextLayout[target.shelf] = insertOnShelf(nextLayout[target.shelf], target.slot, item, side, sourceSlotIdx);
    setBayLayouts((prev) => ({ ...prev, [activeBayId]: nextLayout }));
    setIsDirty(true);
  }

  // bay CRUD
  async function handleSaveBay(config: BayConfig) {
    const isNew = !config.id;
    const body = configToApiBay(config);
    if (isNew) {
      const res = await fetch("/api/planogram/bays", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const created = apiBayToConfig(await res.json());
      setBayConfigs((prev) => [...prev, created]);
      setBayLayouts((prev) => ({ ...prev, [created.id]: makeEmptyBayLayout(created.shelves) }));
      setActiveBayId(created.id);
    } else {
      await fetch(`/api/planogram/bays/${config.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      setBayConfigs((prev) => prev.map((b) => (b.id === config.id ? config : b)));
      setBayLayouts((prev) => ({
        ...prev,
        [config.id]: reconcileLayout(prev[config.id] ?? [], config.shelves),
      }));
    }
  }

  async function handleDuplicateBay(config: BayConfig) {
    const body = configToApiBay({ ...config, name: nextCopyName(config.name, bayConfigs) });
    const res = await fetch("/api/planogram/bays", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const created = apiBayToConfig(await res.json());
    setBayConfigs((prev) => [...prev, created]);
    setBayLayouts((prev) => ({ ...prev, [created.id]: makeEmptyBayLayout(created.shelves) }));
  }

  async function handleReorderBays(o: Outlet, orderedIds: string[]) {
    await fetch("/api/planogram/bays/reorder", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ outlet: o, ids: orderedIds }),
    });
    setBayConfigs((prev) => {
      const byId = Object.fromEntries(prev.map((b) => [b.id, b]));
      const reordered = orderedIds.map((id) => byId[id]).filter(Boolean) as BayConfig[];
      const others = prev.filter((b) => b.outlet !== o);
      return [...others, ...reordered];
    });
  }

  async function handleDeleteBay(id: string) {
    await fetch(`/api/planogram/bays/${id}`, { method: "DELETE" });
    const remaining = bayConfigs.filter((b) => b.id !== id && b.outlet === outlet);
    if (activeBayId === id) setActiveBayId(remaining[0]?.id ?? null);
    setBayConfigs((prev) => prev.filter((b) => b.id !== id));
    setBayLayouts((prev) => { const next = { ...prev }; delete next[id]; return next; });
  }

  function handleRemoveItem(shelf: number, slot: number) {
    if (!activeBayId) return;
    const next = activeBayLayout.map((s, si) => s.map((cell, sli) => (si === shelf && sli === slot ? null : cell)));
    setBayLayouts((prev) => ({ ...prev, [activeBayId]: next }));
    setIsDirty(true);
  }

  async function handleSave() {
    await fetch(`/api/planogram/planograms/${planogramId}/save`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildPlacements(bayLayouts)),
    });
    setIsDirty(false);
  }

  async function handleSaveAs(name: string) {
    const res = await fetch("/api/planogram/planograms", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ outlet, name }),
    });
    const created: PlanogramMeta = await res.json();
    await fetch(`/api/planogram/planograms/${created.id}/save`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildPlacements(bayLayouts)),
    });
    setSaveAsDialog(false);
    router.push(`/planogram/${created.id}`);
  }

  async function handleActivate() {
    const updated: PlanogramMeta = await fetch(
      `/api/planogram/planograms/${planogramId}/activate`,
      { method: "POST" },
    ).then((r) => r.json());
    setPlanogram(updated);
  }

  const statusColor = (s: string) =>
    s === "active" ? "#2a7" : s === "archived" ? "#999" : "#e90";

  return (
    <>
      <DragDropProvider onDragEnd={handleDragEnd}>
        <div style={{ padding: 16 }}>
          {/* toolbar */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
            <button onClick={() => router.push("/planogram")} style={{ ...btnStyle, marginRight: 4 }}>← Back</button>
            <span style={{ fontSize: 15, fontWeight: 600 }}>{planogram?.name ?? "…"}</span>
            {planogram && (
              <span style={{
                fontSize: 11, padding: "2px 7px", borderRadius: 3,
                background: statusColor(planogram.status), color: "#fff",
              }}>
                {planogram.status}
              </span>
            )}
            <div style={{ flex: 1 }} />
            <button onClick={handleSave} disabled={!isDirty} style={btnStyle}>Save</button>
            <button onClick={() => setSaveAsDialog(true)} style={btnStyle}>Save as</button>
            {planogram?.status === "draft" && (
              <button onClick={handleActivate} style={{ ...btnStyle, borderColor: "#2a7", color: "#2a7" }}>
                Activate
              </button>
            )}
            <button
              onClick={async () => {
                if (!planogram) return;
                setIsExporting(true);
                await exportPdf(planogram, outlet, bayConfigs, bayLayouts, Date.now());
                setIsExporting(false);
              }}
              disabled={isExporting}
              style={{ ...btnStyle, marginLeft: 4 }}
            >
              {isExporting ? "Exporting…" : "Export PDF"}
            </button>
          </div>

          {/* bay tabs */}
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 12 }}>
            {outletBays.map((b) => (
              <button
                key={b.id}
                onClick={() => setActiveBayId(b.id)}
                style={{
                  padding: "3px 12px", fontSize: 12,
                  fontWeight: activeBayId === b.id ? "bold" : "normal",
                  background: activeBayId === b.id ? "#333" : "#eee",
                  color: activeBayId === b.id ? "#fff" : "#333",
                  border: "none", borderRadius: 4, cursor: "pointer",
                }}
              >
                {b.name || "Bay"}
              </button>
            ))}
            <button
              onClick={() => setBayModalOpen(true)}
              style={{ padding: "3px 10px", fontSize: 12, background: "none", border: "1px solid #ccc", borderRadius: 4, cursor: "pointer", color: "#555" }}
            >
              Manage bays
            </button>
          </div>

          {/* main content */}
          <div style={{ display: "flex", gap: 16 }}>
            {/* sidebar */}
            <div style={{ width: 200, flexShrink: 0, height: "80vh", overflowY: "auto", borderRight: "1px solid #ccc", paddingRight: 12 }}>
              <div style={{ display: "flex", gap: 4, marginBottom: 6 }}>
                {(["unplaced", "all"] as const).map((mode) => (
                  <button
                    key={mode}
                    onClick={() => { setListMode(mode); setPage(0); }}
                    style={{
                      flex: 1, padding: "2px 0", fontSize: 11,
                      fontWeight: listMode === mode ? "bold" : "normal",
                      background: listMode === mode ? "#333" : "#eee",
                      color: listMode === mode ? "#fff" : "#333",
                      border: "none", borderRadius: 3, cursor: "pointer",
                    }}
                  >
                    {mode === "all" ? "All" : "Unplaced"}
                  </button>
                ))}
              </div>
              <input
                type="text"
                placeholder="Search name or SKU…"
                value={search}
                onChange={(e) => { setSearch(e.target.value); setPage(0); }}
                style={{ width: "100%", marginBottom: 8, padding: "3px 6px", fontSize: 11, boxSizing: "border-box", border: "1px solid #ccc", borderRadius: 3, background: "#fff", color: "#000" }}
              />
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
              {activeBayConfig ? (
                <>
                  <p style={{ margin: "0 0 8px", fontWeight: "bold" }}>
                    {activeBayConfig.name || "Bay"} · {activeBayConfig.outlet}
                  </p>
                  <Bay layout={activeBayLayout} shelves={activeBayConfig.shelves} onRemove={handleRemoveItem} />
                </>
              ) : (
                <div style={{ color: "#aaa", fontSize: 13, padding: 20 }}>
                  No bay selected.{" "}
                  <button onClick={() => setBayModalOpen(true)} style={{ background: "none", border: "none", color: "#555", textDecoration: "underline", cursor: "pointer", fontSize: 13 }}>
                    Add a bay
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
        <DragOverlay>
          {() => <WineBottle style={{ width: 40, height: 120, opacity: 0.8 }} />}
        </DragOverlay>
      </DragDropProvider>

      <BayModal
        open={bayModalOpen}
        bayConfigs={bayConfigs}
        defaultOutlet={outlet}
        onClose={() => setBayModalOpen(false)}
        onSave={handleSaveBay}
        onDelete={handleDeleteBay}
        onDuplicate={handleDuplicateBay}
        onReorder={handleReorderBays}
      />

      {saveAsDialog && (
        <NameDialog
          title="Save as"
          confirmLabel="Save"
          onConfirm={handleSaveAs}
          onCancel={() => setSaveAsDialog(false)}
        />
      )}
    </>
  );
}

// --- WineBottle ---

const WineBottle = React.forwardRef<SVGSVGElement, { color?: string; style?: React.CSSProperties }>(
  ({ color = "#2D5A27", style }, ref) => (
    <svg ref={ref} viewBox="0 0 66.758 250" style={style} xmlns="http://www.w3.org/2000/svg">
      <g transform="translate(116.44 -602.98)">
        <g transform="matrix(.24737 0 0 .24737 -176.37 598.08)">
          <g transform="translate(96.975 36.365)">
            <rect fill="#fff" height={560} width={80} y={403.79} x={322.86} />
            <rect fill="#fff" height={234.29} width={45.714} y={26.648} x={277.14} />
            <path fill={color} d="m237.11-14.802c-2.0828 1.5833-3.2235 5.4484-3.6875 12.469-0.2329 3.5228-0.9539 7.0414-1.5938 7.8125-0.7091 0.85434-1.3578 7.0229-1.6562 15.813-0.414 12.191-0.2161 14.976 1.2187 18 0.7543 1.5895 1.1648 3.5781 1.2188 13.156l-6.83 188.74c0.48973 7.3004-1.2397 13.334-4.8704 19.598-2.9314 4.7479-7.6972 10.506-16.996 21.221-37.168 39.202-55.265 89.471-58.625 142.53l0.1562 529.78c1.7751 13.738 8.7702 35.164 19.781 39.781h230c11.011-4.6175 18.006-26.043 19.781-39.781l0.1563-529.78c-3.3599-53.061-21.458-103.33-58.625-142.53-10.854-12.506-15.532-18.26-18.326-23.536-3.1276-4.949-3.2868-10.244-3.49-15.865l-6.87-190.15c0.0539-9.5782 0.4645-11.567 1.2187-13.156 1.4348-3.0236 1.6328-5.8086 1.2188-18-0.2984-8.7896-0.9472-14.958-1.6563-15.813-0.6399-0.77108-1.3609-4.2897-1.5937-7.8125-0.4641-7.0203-1.6048-10.885-3.6875-12.469-28.65-2.2076-57.603-2.4136-86.25 0zm72.031 61.112h5v180h-5v-180zm56 388v521h-6v-521h6zm21 0v521h-13v-521h13z" />
          </g>
        </g>
      </g>
    </svg>
  ),
);
WineBottle.displayName = "WineBottle";

// --- Print ---

function PrintBay({ layout, shelves }: { layout: BayLayout; shelves: number[] }) {
  return (
    <div style={{ display: "inline-flex", flexDirection: "column", border: `8px solid ${NAVY}`, padding: 4 }}>
      {shelves.map((_, i) => (
        <div key={i} style={{ display: "flex", flexDirection: "row", gap: 4, alignItems: "center", borderBottom: `12px solid ${NAVY}`, paddingBottom: 4, marginBottom: 4 }}>
          <p style={{ textAlign: "center", width: 20, margin: 0, fontSize: 11 }}>{i + 1}</p>
          {(layout[i] ?? []).map((item, j) => (
            <div
              key={j}
              style={{
                width: 80, height: 180,
                border: item ? "1px solid #ddd" : "1px solid #eee",
                display: "flex", flexDirection: "column",
                alignItems: "center", justifyContent: "flex-end",
                padding: "4px 2px", boxSizing: "border-box",
              }}
            >
              {item && (
                <>
                  <WineBottle color={wineColor(item.tags)} style={{ width: 30, height: 90, flexShrink: 0 }} />
                  <div style={{ textAlign: "center", marginTop: 4, width: "100%" }}>
                    <div style={{ fontSize: 9, lineHeight: 1.2, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical" }}>
                      {item.name}
                    </div>
                    <div style={{ fontSize: 9, color: "#666", marginTop: 2 }}>{item.sku}</div>
                  </div>
                </>
              )}
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

// --- DnD components ---

function DraggableItem({ item, placed }: { item: Item; placed: boolean }) {
  const { ref } = useDraggable({
    id: item.id,
    data: { ...item, source: "sidebar" as const },
    disabled: placed,
  });
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
      {slots.map((item, i) => (
        <Slot key={i} item={item} index={i} shelfIndex={index} onRemove={onRemove} />
      ))}
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
    <div
      id={`slot-${shelfIndex}-${index}`}
      ref={dropRef}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        position: "relative", width: 80, flexShrink: 0, height: 180,
        border: "1px dashed gray", display: "flex", flexDirection: "column",
        alignItems: "center", justifyContent: "flex-end",
        padding: "4px 2px", boxSizing: "border-box",
      }}
    >
      {item ? (
        <>
          {hovered && (
            <button
              onClick={() => onRemove(shelfIndex, index)}
              style={{ position: "absolute", top: 2, right: 2, width: 16, height: 16, padding: 0, lineHeight: 1, fontSize: 11, cursor: "pointer", border: "none", borderRadius: 2, background: "#e55", color: "#fff" }}
            >×</button>
          )}
          <WineBottle ref={dragRef} color={wineColor(item.tags)} style={{ width: 30, height: 90, cursor: "grab", flexShrink: 0 }} />
          <div style={{ textAlign: "center", marginTop: 4, width: "100%" }} title={item.name}>
            <div style={{ fontSize: 9, lineHeight: 1.2, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical" }}>
              {item.name}
            </div>
            <div style={{ fontSize: 9, color: "#888", marginTop: 2 }}>{item.sku}</div>
          </div>
        </>
      ) : (
        <p style={{ margin: 0, fontSize: 11, color: "#ccc" }}>{index + 1}</p>
      )}
    </div>
  );
}
