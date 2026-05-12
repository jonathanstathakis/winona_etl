"use client";
import React, { useState, useEffect, useRef, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";

const NAVY = "#1a2b5f";
const CANVAS_W = 2000;  // cm
const CANVAS_H = 1500;  // cm
const MIN_ZOOM = 0.05;  // px/cm
const MAX_ZOOM = 10;    // px/cm
const LOG_RANGE = Math.log(MAX_ZOOM / MIN_ZOOM);

type Vertex = { x: number; y: number };

type FloorBay = {
  id: string;
  name: string;
  floor_x: number | null;
  floor_y: number | null;
  floor_w: number;
  floor_h: number;
  floor_rotation: 0 | 90 | 180 | 270;
  color: string;
};

type Pan = { x: number; y: number };
type Mode = "place" | "room" | "pan";

type BayDrag = {
  bayId: string;
  startWorldX: number;
  startWorldY: number;
  origFloorX: number;
  origFloorY: number;
};

type PanDrag = {
  startClientX: number;
  startClientY: number;
  origPanX: number;
  origPanY: number;
};

type VertexDrag = { index: number };

const btnStyle: React.CSSProperties = {
  padding: "3px 10px", fontSize: 12, background: "#eee", color: "#333",
  border: "1px solid #ccc", borderRadius: 4, cursor: "pointer",
};

function clamp(v: number, lo: number, hi: number) { return Math.max(lo, Math.min(hi, v)); }
function snap(v: number) { return Math.round(v); }

function hexToRgb(hex: string) {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return m ? { r: parseInt(m[1], 16), g: parseInt(m[2], 16), b: parseInt(m[3], 16) } : null;
}

function labelColor(bg: string): string {
  const rgb = hexToRgb(bg);
  if (!rgb) return "#fff";
  return (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255 > 0.5 ? "#111" : "#fff";
}

function distToSegment(px: number, py: number, x1: number, y1: number, x2: number, y2: number) {
  const dx = x2 - x1, dy = y2 - y1;
  const lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return { dist: Math.hypot(px - x1, py - y1), x: x1, y: y1 };
  const t = Math.max(0, Math.min(1, ((px - x1) * dx + (py - y1) * dy) / lenSq));
  const nx = x1 + t * dx, ny = y1 + t * dy;
  return { dist: Math.hypot(px - nx, py - ny), x: nx, y: ny };
}

export default function FloorPlanEdit() {
  const { id: layoutId, outlet } = useParams<{ id: string; outlet: string }>();
  const router = useRouter();

  const svgRef = useRef<SVGSVGElement>(null);
  const bayDragRef = useRef<BayDrag | null>(null);
  const panDragRef = useRef<PanDrag | null>(null);
  const vertexDragRef = useRef<VertexDrag | null>(null);
  const historyRef = useRef<Array<{ vertices: Vertex[]; bays: FloorBay[]; polygonClosed: boolean }>>([]);
  const histIdxRef = useRef(-1);

  const [vertices, setVertices] = useState<Vertex[]>([]);
  const [polygonClosed, setPolygonClosed] = useState(false);
  const [bays, setBays] = useState<FloorBay[]>([]);
  const [mode, setMode] = useState<Mode>("place");
  const [selectedBayId, setSelectedBayId] = useState<string | null>(null);
  const [bayListFilter, setBayListFilter] = useState<"all" | "placed" | "unplaced">("unplaced");
  const [isDirty, setIsDirty] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [zoom, setZoom] = useState(0.4);
  const [pan, setPan] = useState<Pan>({ x: 20, y: 20 });
  const [dragCoord, setDragCoord] = useState<{ x: number; y: number } | null>(null);
  const [histVersion, setHistVersion] = useState(0);
  const [gridSnap, setGridSnap] = useState(false);

  // derived early so handlers can close over it
  const gridStep = ([1, 10, 100, 1000] as const).find((s) => s * zoom >= 8) ?? 1000;

  function snapBay(v: number): number {
    return gridSnap ? Math.round(v / gridStep) * gridStep : Math.round(v);
  }

  useEffect(() => {
    fetch(`/api/planogram/floor-plan/${outlet}`)
      .then((r) => r.json())
      .then((data) => {
        setVertices(data.vertices);
        setPolygonClosed(data.vertices.length >= 3);
        setBays(data.bays);
        setIsDirty(false);
        historyRef.current = [{ vertices: data.vertices, bays: data.bays, polygonClosed: data.vertices.length >= 3 }];
        histIdxRef.current = 0;
        setHistVersion(0);
        fitToRoom();
      });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [outlet]);

  const fitToRoom = useCallback(() => {
    const svg = svgRef.current;
    if (!svg) return;
    const { width: sw, height: sh } = svg.getBoundingClientRect();
    if (!sw || !sh) return;
    const fitZoom = clamp(Math.min(sw / CANVAS_W, sh / CANVAS_H) * 0.88, MIN_ZOOM, MAX_ZOOM);
    setPan({ x: (sw - CANVAS_W * fitZoom) / 2, y: (sh - CANVAS_H * fitZoom) / 2 });
    setZoom(fitZoom);
  }, []);

  // fit on first render after layout settles
  useEffect(() => {
    const t = setTimeout(fitToRoom, 50);
    return () => clearTimeout(t);
  }, [fitToRoom]);

  function pushHistory(verts: Vertex[], b: FloorBay[], closed: boolean) {
    historyRef.current = historyRef.current.slice(0, histIdxRef.current + 1);
    historyRef.current.push({ vertices: [...verts], bays: [...b], polygonClosed: closed });
    histIdxRef.current++;
    setHistVersion((n) => n + 1);
  }

  const undo = useCallback(() => {
    if (histIdxRef.current <= 0) return;
    histIdxRef.current--;
    const s = historyRef.current[histIdxRef.current];
    setVertices(s.vertices);
    setBays(s.bays);
    setPolygonClosed(s.polygonClosed);
    setIsDirty(true);
    setHistVersion((n) => n + 1);
  }, []);

  const redo = useCallback(() => {
    if (histIdxRef.current >= historyRef.current.length - 1) return;
    histIdxRef.current++;
    const s = historyRef.current[histIdxRef.current];
    setVertices(s.vertices);
    setBays(s.bays);
    setPolygonClosed(s.polygonClosed);
    setIsDirty(true);
    setHistVersion((n) => n + 1);
  }, []);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key === "z" && !e.shiftKey) { e.preventDefault(); undo(); }
      if (e.key === "z" && e.shiftKey)  { e.preventDefault(); redo(); }
      if (e.key === "y")                { e.preventDefault(); redo(); }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [undo, redo]);

  function worldCoords(e: { clientX: number; clientY: number }): { x: number; y: number } {
    const rect = svgRef.current!.getBoundingClientRect();
    return {
      x: (e.clientX - rect.left - pan.x) / zoom,
      y: (e.clientY - rect.top - pan.y) / zoom,
    };
  }

  // TODO: page scrolls when zooming via scroll wheel on canvas — e.preventDefault() has no effect
  // because React registers wheel listeners as passive; fix by attaching a non-passive listener via useEffect.
  function handleWheel(e: React.WheelEvent<SVGSVGElement>) {
    e.preventDefault();
    const rect = svgRef.current!.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const factor = e.deltaY < 0 ? 1.08 : 1 / 1.08;
    const newZoom = clamp(zoom * factor, MIN_ZOOM, MAX_ZOOM);
    const scale = newZoom / zoom;
    setPan((p) => ({ x: mx - (mx - p.x) * scale, y: my - (my - p.y) * scale }));
    setZoom(newZoom);
  }

  function handleSliderChange(e: React.ChangeEvent<HTMLInputElement>) {
    const t = parseFloat(e.target.value);
    const newZoom = clamp(MIN_ZOOM * Math.exp(t * LOG_RANGE), MIN_ZOOM, MAX_ZOOM);
    const svg = svgRef.current;
    if (svg) {
      const { width: sw, height: sh } = svg.getBoundingClientRect();
      const scale = newZoom / zoom;
      setPan((p) => ({ x: sw / 2 - (sw / 2 - p.x) * scale, y: sh / 2 - (sh / 2 - p.y) * scale }));
    }
    setZoom(newZoom);
  }

  function handleCanvasClick(e: React.MouseEvent<SVGSVGElement>) {
    if (mode !== "room") return;
    const target = e.target as SVGElement;
    if (target.getAttribute("data-vertex") || target.getAttribute("data-bay")) return;
    const { x, y } = worldCoords(e);
    const snapped = { x: clamp(snap(x), 0, CANVAS_W), y: clamp(snap(y), 0, CANVAS_H) };

    if (polygonClosed) {
      // insert vertex on nearest edge if within 15 screen-pixels
      const threshold = 15 / zoom;
      let bestDist = Infinity, bestIdx = -1, bestX = 0, bestY = 0;
      for (let i = 0; i < vertices.length; i++) {
        const j = (i + 1) % vertices.length;
        const r = distToSegment(x, y, vertices[i].x, vertices[i].y, vertices[j].x, vertices[j].y);
        if (r.dist < bestDist) { bestDist = r.dist; bestIdx = i; bestX = r.x; bestY = r.y; }
      }
      if (bestDist <= threshold) {
        pushHistory(vertices, bays, polygonClosed);
        const inserted = [...vertices];
        inserted.splice(bestIdx + 1, 0, { x: clamp(snap(bestX), 0, CANVAS_W), y: clamp(snap(bestY), 0, CANVAS_H) });
        setVertices(inserted);
        setIsDirty(true);
      }
      return;
    }

    if (vertices.length >= 3) {
      const first = vertices[0];
      if (Math.abs(snapped.x - first.x) <= 5 && Math.abs(snapped.y - first.y) <= 5) {
        pushHistory(vertices, bays, polygonClosed);
        setPolygonClosed(true);
        setIsDirty(true);
        return;
      }
    }
    pushHistory(vertices, bays, polygonClosed);
    setVertices((prev) => [...prev, snapped]);
    setIsDirty(true);
  }

  function handleBayPointerDown(e: React.PointerEvent<SVGRectElement>, bay: FloorBay) {
    if (mode !== "place" || bay.floor_x === null || bay.floor_y === null) return;
    e.stopPropagation();
    pushHistory(vertices, bays, polygonClosed);
    setSelectedBayId(bay.id);
    const { x, y } = worldCoords(e);
    bayDragRef.current = { bayId: bay.id, startWorldX: x, startWorldY: y, origFloorX: bay.floor_x, origFloorY: bay.floor_y };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function startPan(e: { clientX: number; clientY: number; pointerId?: number }, target?: Element) {
    panDragRef.current = { startClientX: e.clientX, startClientY: e.clientY, origPanX: pan.x, origPanY: pan.y };
    if (target && "pointerId" in e && e.pointerId !== undefined) {
      (target as Element & { setPointerCapture?: (id: number) => void }).setPointerCapture?.(e.pointerId);
    }
  }

  function handleBackgroundPointerDown(e: React.PointerEvent<SVGRectElement>) {
    if (mode !== "place") return;
    setSelectedBayId(null);
    startPan(e, e.target as Element);
  }

  function handleSvgPointerDown(e: React.PointerEvent<SVGSVGElement>) {
    if (mode !== "pan") return;
    startPan(e, svgRef.current ?? undefined);
  }

  function handleSvgPointerMove(e: React.PointerEvent<SVGSVGElement>) {
    // bay drag
    const bd = bayDragRef.current;
    if (bd) {
      const { x, y } = worldCoords(e);
      const newX = clamp(snapBay(bd.origFloorX + x - bd.startWorldX), 0, CANVAS_W);
      const newY = clamp(snapBay(bd.origFloorY + y - bd.startWorldY), 0, CANVAS_H);
      setBays((prev) => prev.map((b) => b.id === bd.bayId ? { ...b, floor_x: newX, floor_y: newY } : b));
      setDragCoord({ x: newX, y: newY });
      setIsDirty(true);
      return;
    }
    // pan drag
    const pd = panDragRef.current;
    if (pd) {
      setPan({ x: pd.origPanX + e.clientX - pd.startClientX, y: pd.origPanY + e.clientY - pd.startClientY });
    }
    // vertex drag
    const vd = vertexDragRef.current;
    if (vd) {
      const { x, y } = worldCoords(e);
      const snapped = { x: clamp(snap(x), 0, CANVAS_W), y: clamp(snap(y), 0, CANVAS_H) };
      setVertices((prev) => prev.map((v, i) => i === vd.index ? snapped : v));
      setIsDirty(true);
    }
    // always show cursor world position (bay drag sets its own coord and returns early above)
    const { x, y } = worldCoords(e);
    setDragCoord({ x: clamp(snap(x), 0, CANVAS_W), y: clamp(snap(y), 0, CANVAS_H) });
  }

  function handleSvgPointerUp() {
    bayDragRef.current = null;
    panDragRef.current = null;
    vertexDragRef.current = null;
  }

  function handleVertexPointerDown(e: React.PointerEvent<SVGCircleElement>, index: number) {
    e.stopPropagation();
    pushHistory(vertices, bays, polygonClosed);
    vertexDragRef.current = { index };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handlePlaceBay(bay: FloorBay) {
    pushHistory(vertices, bays, polygonClosed);
    const cx = clamp(snapBay(CANVAS_W / 2 - bay.floor_w / 2), 0, CANVAS_W);
    const cy = clamp(snapBay(CANVAS_H / 2 - bay.floor_h / 2), 0, CANVAS_H);
    setBays((prev) => prev.map((b) => b.id === bay.id ? { ...b, floor_x: cx, floor_y: cy } : b));
    setSelectedBayId(bay.id);
    setIsDirty(true);
  }

  function handleRotate() {
    if (!selectedBayId) return;
    pushHistory(vertices, bays, polygonClosed);
    setBays((prev) => prev.map((b) => {
      if (b.id !== selectedBayId) return b;
      const rot = ((b.floor_rotation + 90) % 360) as 0 | 90 | 180 | 270;
      return { ...b, floor_rotation: rot, floor_w: b.floor_h, floor_h: b.floor_w };
    }));
    setIsDirty(true);
  }

  function handleRemoveBay(bayId: string) {
    pushHistory(vertices, bays, polygonClosed);
    setBays((prev) => prev.map((b) => b.id === bayId ? { ...b, floor_x: null, floor_y: null } : b));
    setSelectedBayId(null);
    setIsDirty(true);
  }

  function handlePropChange(field: "floor_w" | "floor_h" | "color", value: string | number) {
    if (!selectedBayId) return;
    pushHistory(vertices, bays, polygonClosed);
    setBays((prev) => prev.map((b) => b.id !== selectedBayId ? b : { ...b, [field]: value }));
    setIsDirty(true);
  }

  async function handleSave() {
    setIsSaving(true);
    try {
      await fetch(`/api/planogram/floor-plan/${outlet}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vertices }),
      });
      await Promise.all(
        bays.map((b) =>
          fetch(`/api/planogram/bays/${b.id}/position`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ floor_x: b.floor_x, floor_y: b.floor_y, floor_w: b.floor_w, floor_h: b.floor_h, floor_rotation: b.floor_rotation, color: b.color }),
          })
        )
      );
      setIsDirty(false);
    } finally {
      setIsSaving(false);
    }
  }

  const canUndo = histVersion >= 0 && histIdxRef.current > 0;
  const canRedo = histVersion >= 0 && histIdxRef.current < historyRef.current.length - 1;

  const labelStep = gridStep * 10;

  // visible world rect (clipped to canvas)
  const svgEl = svgRef.current;
  const svgW = svgEl?.clientWidth ?? 900;
  const svgH = svgEl?.clientHeight ?? 600;
  const visLeft = clamp(Math.floor(-pan.x / zoom / gridStep) * gridStep, 0, CANVAS_W);
  const visRight = clamp(Math.ceil((svgW - pan.x) / zoom / gridStep) * gridStep, 0, CANVAS_W);
  const visTop = clamp(Math.floor(-pan.y / zoom / gridStep) * gridStep, 0, CANVAS_H);
  const visBot = clamp(Math.ceil((svgH - pan.y) / zoom / gridStep) * gridStep, 0, CANVAS_H);

  const gridXs: number[] = [];
  for (let x = visLeft; x <= visRight; x += gridStep) gridXs.push(x);
  const gridYs: number[] = [];
  for (let y = visTop; y <= visBot; y += gridStep) gridYs.push(y);

  const polygonPoints = vertices.map((v) => `${v.x},${v.y}`).join(" ");
  const placedBays = bays.filter((b) => b.floor_x !== null);
  const unplacedBays = bays.filter((b) => b.floor_x === null);
  const selectedBay = bays.find((b) => b.id === selectedBayId) ?? null;

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0, padding: 16, boxSizing: "border-box", overflow: "hidden" }}>
      {/* toolbar */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 12, flexWrap: "wrap" }}>
        <button onClick={() => router.push(`/planogram/${layoutId}/${outlet}/floor-plan`)} style={btnStyle}>← Floor plan</button>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>Edit Floor Plan</h1>
        <span style={{ width: 12 }} />
        <button onClick={undo} disabled={!canUndo} style={btnStyle} title="Undo (⌘Z)">↩ Undo</button>
        <button onClick={redo} disabled={!canRedo} style={btnStyle} title="Redo (⌘⇧Z)">↪ Redo</button>
        <span style={{ width: 4 }} />
        <button onClick={() => setMode("place")}
          style={{ ...btnStyle, background: mode === "place" ? NAVY : "#eee", color: mode === "place" ? "#fff" : "#333", border: "none" }}>
          Place Bays
        </button>
        <button onClick={() => { setMode("room"); setPolygonClosed(false); }}
          style={{ ...btnStyle, background: mode === "room" ? NAVY : "#eee", color: mode === "room" ? "#fff" : "#333", border: "none" }}>
          Edit Room
        </button>
        <button onClick={() => setMode("pan")}
          style={{ ...btnStyle, background: mode === "pan" ? NAVY : "#eee", color: mode === "pan" ? "#fff" : "#333", border: "none" }}
          title="Hand tool — drag to pan">
          ✋ Hand
        </button>
        {mode === "room" && vertices.length >= 3 && !polygonClosed && (
          <button onClick={() => { setPolygonClosed(true); setIsDirty(true); }} style={{ ...btnStyle, borderColor: "#2a7", color: "#2a7" }}>Close polygon</button>
        )}
        {mode === "room" && vertices.length > 0 && (
          <button onClick={() => { pushHistory(vertices, bays, polygonClosed); setVertices([]); setPolygonClosed(false); setIsDirty(true); }} style={{ ...btnStyle, borderColor: "#e55", color: "#e55" }}>Clear room</button>
        )}
        <span style={{ flex: 1 }} />
        <button onClick={fitToRoom} style={btnStyle}>Fit</button>
        <button onClick={() => setGridSnap((v) => !v)}
          style={{ ...btnStyle, background: gridSnap ? NAVY : "#eee", color: gridSnap ? "#fff" : "#333", border: "none" }}
          title="Snap bays to visible grid">
          ⊞ {gridSnap ? `${gridStep}cm` : "1cm"}
        </button>
        <input type="range" min={0} max={1} step={0.001}
          value={Math.log(zoom / MIN_ZOOM) / LOG_RANGE}
          onChange={handleSliderChange}
          style={{ width: 120, cursor: "pointer", verticalAlign: "middle" }} />
        <span style={{ fontSize: 11, color: "#666", minWidth: 56, textAlign: "right" }}>{Math.round(zoom * 100) / 100} px/cm</span>
        <button onClick={handleSave} disabled={!isDirty || isSaving} style={btnStyle}>{isSaving ? "Saving…" : "Save"}</button>
      </div>

      <div style={{ display: "flex", flex: 1, gap: 16, minHeight: 0 }}>
        {/* bay list sidebar — hidden in pan/room modes */}
        {mode === "place" && (
          <div style={{ width: 160, flexShrink: 0, borderRight: "1px solid #ccc", paddingRight: 12, overflowY: "auto" }}>
            <div style={{ display: "flex", gap: 3, marginBottom: 8 }}>
              {(["all", "placed", "unplaced"] as const).map((f) => (
                <button key={f} onClick={() => setBayListFilter(f)}
                  style={{ flex: 1, padding: "2px 0", fontSize: 10, fontWeight: bayListFilter === f ? "bold" : "normal", background: bayListFilter === f ? "#333" : "#eee", color: bayListFilter === f ? "#fff" : "#333", border: "none", borderRadius: 3, cursor: "pointer" }}>
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
            {bays.filter((b) => bayListFilter === "all" || (bayListFilter === "placed" ? b.floor_x !== null : b.floor_x === null)).map((b) => (
              <div key={b.id}
                onClick={() => b.floor_x !== null && setSelectedBayId(b.id)}
                style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #eee", cursor: b.floor_x !== null ? "pointer" : "default", background: b.id === selectedBayId ? "#f0f4ff" : "transparent" }}>
                <span style={{ fontSize: 12, color: b.floor_x !== null ? "#333" : "#888" }}>{b.name}</span>
                {b.floor_x === null && (
                  <button onClick={(e) => { e.stopPropagation(); handlePlaceBay(b); }}
                    style={{ fontSize: 11, padding: "1px 6px", background: NAVY, color: "#fff", border: "none", borderRadius: 3, cursor: "pointer" }}>+</button>
                )}
              </div>
            ))}
            {bays.filter((b) => bayListFilter === "all" || (bayListFilter === "placed" ? b.floor_x !== null : b.floor_x === null)).length === 0 && (
              <p style={{ fontSize: 11, color: "#ccc" }}>{bayListFilter === "unplaced" ? "All bays placed" : "No bays"}</p>
            )}
          </div>
        )}

        {/* canvas */}
        <div style={{ flex: 1, minHeight: 0, position: "relative", overflow: "hidden", background: "#f9f9f9", border: "1px solid #ddd" }}>
          <svg ref={svgRef}
            style={{ display: "block", width: "100%", height: "100%",
              cursor: mode === "pan" ? "grab" : mode === "room" && !polygonClosed ? "crosshair" : "default" }}
            onWheel={handleWheel}
            onClick={handleCanvasClick}
            onPointerDown={handleSvgPointerDown}
            onPointerMove={handleSvgPointerMove}
            onPointerUp={handleSvgPointerUp}
            onPointerLeave={() => setDragCoord(null)}>

            <g transform={`translate(${pan.x},${pan.y}) scale(${zoom})`}>
              {/* canvas boundary */}
              <rect x={0} y={0} width={CANVAS_W} height={CANVAS_H} fill="white" stroke="#ccc" strokeWidth={1 / zoom} />

              {/* background drag target (pan) */}
              <rect x={-CANVAS_W * 2} y={-CANVAS_H * 2} width={CANVAS_W * 5} height={CANVAS_H * 5} fill="transparent"
                style={{ cursor: mode === "place" ? "grab" : "default" }}
                onPointerDown={handleBackgroundPointerDown} />

              {/* adaptive grid dots */}
              {gridXs.flatMap((x) =>
                gridYs.map((y) => (
                  <circle key={`${x}-${y}`} cx={x} cy={y} r={1.5 / zoom} fill="#ccc" style={{ pointerEvents: "none" }} />
                ))
              )}

              {/* grid labels */}
              {gridXs.filter((x) => x % labelStep === 0).map((x) => (
                <text key={`lx-${x}`} x={x} y={-6 / zoom} textAnchor="middle" fontSize={10 / zoom} fill="#999" style={{ pointerEvents: "none" }}>{x}</text>
              ))}
              {gridYs.filter((y) => y % labelStep === 0).map((y) => (
                <text key={`ly-${y}`} x={-6 / zoom} y={y} textAnchor="end" dominantBaseline="middle" fontSize={10 / zoom} fill="#999" style={{ pointerEvents: "none" }}>{y}</text>
              ))}

              {/* room polygon */}
              {vertices.length >= 2 && polygonClosed && (
                <polygon points={polygonPoints} fill={`${NAVY}15`} stroke={NAVY} strokeWidth={2 / zoom} strokeLinejoin="round" style={{ pointerEvents: "none" }} />
              )}
              {vertices.length >= 2 && !polygonClosed && (
                <polyline points={polygonPoints} fill="none" stroke={NAVY} strokeWidth={2 / zoom} strokeDasharray={`${6 / zoom},${4 / zoom}`} strokeLinejoin="round" style={{ pointerEvents: "none" }} />
              )}

              {/* placed bays */}
              {placedBays.map((b) => {
                const x = b.floor_x ?? 0;
                const y = b.floor_y ?? 0;
                const isSelected = b.id === selectedBayId;
                const fontSize = clamp(b.floor_w / Math.max(b.name.length, 1) * 0.8, 6 / zoom, 24 / zoom);
                return (
                  <g key={b.id} data-bay="true">
                    <rect x={x} y={y} width={b.floor_w} height={b.floor_h} fill={b.color}
                      stroke={isSelected ? "#f0a500" : "rgba(0,0,0,0.25)"} strokeWidth={isSelected ? 3 / zoom : 1.5 / zoom} rx={3 / zoom}
                      style={{ cursor: mode === "place" ? "grab" : "default" }}
                      onPointerDown={(e) => handleBayPointerDown(e, b)} />
                    <text x={x + b.floor_w / 2} y={y + b.floor_h / 2} textAnchor="middle" dominantBaseline="middle"
                      fontSize={fontSize} fontWeight="bold" fill={labelColor(b.color)}
                      style={{ pointerEvents: "none", userSelect: "none" }}>{b.name}</text>
                  </g>
                );
              })}

              {/* vertex handles */}
              {mode === "room" && vertices.map((v, i) => (
                <circle key={i} cx={v.x} cy={v.y} r={7 / zoom}
                  fill={i === 0 ? "#2a7" : "#fff"} stroke={NAVY} strokeWidth={2 / zoom}
                  data-vertex="true" style={{ cursor: "move" }}
                  onPointerDown={(e) => handleVertexPointerDown(e, i)} />
              ))}
            </g>
          </svg>

          {/* snap level badge */}
          <div style={{ position: "absolute", bottom: 8, right: 8, fontSize: 10, color: "#aaa", background: "rgba(255,255,255,0.8)", padding: "2px 6px", borderRadius: 3, pointerEvents: "none" }}>
            snap {gridStep} cm
          </div>
        </div>

        {/* properties panel */}
        {mode === "place" && selectedBay && selectedBay.floor_x !== null && (
          <div style={{ width: 160, flexShrink: 0, borderLeft: "1px solid #ccc", paddingLeft: 12 }}>
            <p style={{ fontSize: 11, color: "#888", margin: "0 0 10px", fontWeight: "bold" }}>{selectedBay.name}</p>

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
              Width (cm)
              <input type="number" min={10} max={2000} step={10}
                key={`${selectedBayId}-w`}
                defaultValue={selectedBay.floor_w}
                onBlur={(e) => handlePropChange("floor_w", Math.max(10, Number(e.target.value) || 10))}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
              Depth (cm)
              <input type="number" min={10} max={2000} step={10}
                key={`${selectedBayId}-h`}
                defaultValue={selectedBay.floor_h}
                onBlur={(e) => handlePropChange("floor_h", Math.max(10, Number(e.target.value) || 10))}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 12 }}>
              Colour
              <input type="color" value={selectedBay.color}
                onChange={(e) => handlePropChange("color", e.target.value)}
                style={{ display: "block", width: "100%", height: 32, marginTop: 3, padding: 2, border: "1px solid #ccc", borderRadius: 4, cursor: "pointer", boxSizing: "border-box" }} />
            </label>

            <div style={{ fontSize: 10, color: "#aaa", marginBottom: 10 }}>
              {selectedBay.floor_x !== null ? `${selectedBay.floor_x}, ${selectedBay.floor_y} cm` : ""}
            </div>

            <button onClick={handleRotate} style={{ ...btnStyle, width: "100%", marginBottom: 6, borderColor: "#f0a500", color: "#f0a500" }}>Rotate 90°</button>
            <button onClick={() => handleRemoveBay(selectedBay.id)} style={{ ...btnStyle, width: "100%", borderColor: "#e55", color: "#e55" }}>Remove</button>
          </div>
        )}
      </div>

      <div style={{ marginTop: 8, fontSize: 11, color: "#aaa", display: "flex", justifyContent: "space-between" }}>
        <span>
          {mode === "room" && !polygonClosed && "Click to add vertices. Click near the first vertex (green) to close. Click on an edge of a closed polygon to insert a node."}
          {mode === "room" && polygonClosed && "Drag vertices to adjust. Click on an edge to insert a node. Click \"Edit Room\" to add more."}
          {mode === "pan" && "Drag anywhere to pan · scroll to zoom · switch to Place Bays to move bays."}
          {mode === "place" && !selectedBay && "Scroll to zoom · drag background to pan · click + to place a bay · click a bay to select it."}
          {mode === "place" && selectedBay && "Drag to reposition. Adjust size and colour in the panel."}
        </span>
        {dragCoord && (
          <span style={{ color: "#555", fontVariantNumeric: "tabular-nums", flexShrink: 0, marginLeft: 16 }}>
            x: {dragCoord.x} cm &nbsp; y: {dragCoord.y} cm
          </span>
        )}
      </div>
    </div>
  );
}
