"use client";
import React, { useState, useEffect, useRef, useCallback } from "react";
import { Monitor, Flower2 } from "lucide-react";
import { useParams, useRouter } from "next/navigation";

const NAVY = "#1a2b5f";
const CANVAS_W = 1000;  // cm
const CANVAS_H = 1000;  // cm
const MIN_ZOOM = 0.1;   // px/cm
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
type Mode = "edit" | "pan";

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

type VertexDrag = { index: number; origX: number; origY: number };

type LandmarkType = "door" | "computer" | "display";
type Landmark = { id: string; type: LandmarkType; x: number; y: number; w: number; h: number; rotation: number; label: string };
type LandmarkDrag = { id: string; startWorldX: number; startWorldY: number; origX: number; origY: number };
type LandmarkResizeDrag = {
  id: string; corner: Corner;
  startWorldX: number; startWorldY: number;
  origX: number; origY: number; origW: number; origH: number;
};

type Corner = "tl" | "tr" | "bl" | "br";
type BayResizeDrag = {
  bayId: string;
  corner: Corner;
  startWorldX: number;
  startWorldY: number;
  origFloorX: number;
  origFloorY: number;
  origFloorW: number;
  origFloorH: number;
};

const btnStyle: React.CSSProperties = {
  padding: "3px 10px", fontSize: 12, background: "#eee", color: "#333",
  border: "1px solid #ccc", borderRadius: 4, cursor: "pointer",
};

function clamp(v: number, lo: number, hi: number) { return Math.max(lo, Math.min(hi, v)); }


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

const LANDMARK_SIZE = 40; // cm, default icon footprint

const LUCIDE_ICON: Record<Exclude<LandmarkType, "door">, React.ElementType> = {
  computer: Monitor,
  display: Flower2,
};

// draw.io floorplan.doorLeft paths — wall rect + quarter-circle bezier arc
function DoorPaths({ c }: { c: string }) {
  return (
    <>
      <rect x="0" y="0" width="80" height="5" fill="none" stroke={c} strokeWidth="3" />
      <path d="M 80 5 C 80 49.18 44.18 85 0 85 L 0 5" fill="none" stroke={c} strokeWidth="3" />
    </>
  );
}

// HTML context (palette buttons)
function LandmarkIcon({ type, size, color }: { type: LandmarkType; size: number; color?: string }) {
  const c = color ?? NAVY;
  if (type === "door") {
    return (
      <svg width={size} height={size} viewBox="0 0 80 85" style={{ display: "block" }}>
        <DoorPaths c={c} />
      </svg>
    );
  }
  const Icon = LUCIDE_ICON[type];
  return <Icon size={size} color={c} strokeWidth={1.5} />;
}

// SVG canvas context — nested <svg> maps canvas units to icon coordinate space
function LandmarkIconSvg({ type, w, h, color }: { type: LandmarkType; w: number; h: number; color?: string }) {
  const c = color ?? NAVY;
  if (type === "door") {
    return (
      <svg x={0} y={0} width={w} height={h} viewBox="0 0 80 85" style={{ pointerEvents: "none", overflow: "visible" }}>
        <DoorPaths c={c} />
      </svg>
    );
  }
  const Icon = LUCIDE_ICON[type];
  return (
    <svg x={0} y={0} width={w} height={h} viewBox="0 0 24 24" style={{ pointerEvents: "none", overflow: "visible" }}>
      <Icon size={24} color={c} strokeWidth={1.5} />
    </svg>
  );
}

const LANDMARK_LABELS: Record<LandmarkType, string> = { door: "Door", computer: "PC", display: "Plant" };

const DEFAULT_ROOM: Vertex[] = [
  { x: 400, y: 400 }, { x: 600, y: 400 },
  { x: 600, y: 600 }, { x: 400, y: 600 },
];

export default function FloorPlanEdit() {
  const { id: layoutId, outlet } = useParams<{ id: string; outlet: string }>();
  const router = useRouter();

  const svgRef = useRef<SVGSVGElement>(null);
  const bayDragRef = useRef<BayDrag | null>(null);
  const bayResizeDragRef = useRef<BayResizeDrag | null>(null);
  const panDragRef = useRef<PanDrag | null>(null);
  const vertexDragRef = useRef<VertexDrag | null>(null);
  const landmarkDragRef = useRef<LandmarkDrag | null>(null);
  const landmarkResizeDragRef = useRef<LandmarkResizeDrag | null>(null);
  const historyRef = useRef<Array<{ vertices: Vertex[]; bays: FloorBay[]; landmarks: Landmark[] }>>([]);
  const histIdxRef = useRef(-1);

  const [vertices, setVertices] = useState<Vertex[]>([]);
  const [bays, setBays] = useState<FloorBay[]>([]);
  const [mode, setMode] = useState<Mode>("edit");
  const [selectedBayId, setSelectedBayId] = useState<string | null>(null);
  const [selectedVertexIdx, setSelectedVertexIdx] = useState<number | null>(null);
  const [bayListFilter, setBayListFilter] = useState<"all" | "placed" | "unplaced">("unplaced");
  const [isDirty, setIsDirty] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [zoom, setZoom] = useState(0.7);
  const [pan, setPan] = useState<Pan>({ x: 20, y: 20 });
  const [dragCoord, setDragCoord] = useState<{ x: number; y: number } | null>(null);
  const [histVersion, setHistVersion] = useState(0);
  const [gridSnap, setGridSnap] = useState(false);
  const [landmarks, setLandmarks] = useState<Landmark[]>([]);
  const [selectedLandmarkId, setSelectedLandmarkId] = useState<string | null>(null);
  const [placingType, setPlacingType] = useState<LandmarkType | null>(null);
  const [baysSectionOpen, setBaysSectionOpen] = useState(true);
  const [iconsSectionOpen, setIconsSectionOpen] = useState(true);
  const [showNewBay, setShowNewBay] = useState(false);
  const [newBayName, setNewBayName] = useState("");
  const [newBayDesc, setNewBayDesc] = useState("");
  const [newBayShelves, setNewBayShelves] = useState<number[]>([7]);
  const [isCreating, setIsCreating] = useState(false);

  // derived early so handlers can close over it
  const gridStep = ([1, 10, 100, 1000] as const).find((s) => s * zoom >= 8) ?? 1000;

  function snapGrid(v: number): number {
    return gridSnap ? Math.round(v / gridStep) * gridStep : Math.round(v);
  }

  useEffect(() => {
    fetch(`/api/planogram/floor-plan/${outlet}`)
      .then((r) => r.json())
      .then((data) => {
        const verts = data.vertices.length >= 3 ? data.vertices : DEFAULT_ROOM;
        const lms: Landmark[] = data.landmarks ?? [];
        setVertices(verts);
        setBays(data.bays);
        setLandmarks(lms);
        setIsDirty(data.vertices.length < 3);
        historyRef.current = [{ vertices: verts, bays: data.bays, landmarks: lms }];
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

  function pushHistory(verts: Vertex[], b: FloorBay[], lms?: Landmark[]) {
    historyRef.current = historyRef.current.slice(0, histIdxRef.current + 1);
    historyRef.current.push({ vertices: [...verts], bays: [...b], landmarks: [...(lms ?? landmarks)] });
    histIdxRef.current++;
    setHistVersion((n) => n + 1);
  }

  const undo = useCallback(() => {
    if (histIdxRef.current <= 0) return;
    histIdxRef.current--;
    const s = historyRef.current[histIdxRef.current];
    setVertices(s.vertices);
    setBays(s.bays);
    setLandmarks(s.landmarks);
    setIsDirty(true);
    setHistVersion((n) => n + 1);
  }, []);

  const redo = useCallback(() => {
    if (histIdxRef.current >= historyRef.current.length - 1) return;
    histIdxRef.current++;
    const s = historyRef.current[histIdxRef.current];
    setVertices(s.vertices);
    setBays(s.bays);
    setLandmarks(s.landmarks);
    setIsDirty(true);
    setHistVersion((n) => n + 1);
  }, []);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") { setSelectedVertexIdx(null); return; }
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key === "z" && !e.shiftKey) { e.preventDefault(); undo(); }
      if (e.key === "z" && e.shiftKey)  { e.preventDefault(); redo(); }
      if (e.key === "y")                { e.preventDefault(); redo(); }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [undo, redo]);

  useEffect(() => {
    function onDelete(e: KeyboardEvent) {
      const tag = (document.activeElement as HTMLElement)?.tagName;
      if ((e.key !== "Delete" && e.key !== "Backspace") || mode !== "edit" || tag === "INPUT" || tag === "TEXTAREA") return;
      if (selectedLandmarkId !== null) {
        e.preventDefault();
        pushHistory(vertices, bays, landmarks.filter((lm) => lm.id !== selectedLandmarkId));
        setLandmarks((prev) => prev.filter((lm) => lm.id !== selectedLandmarkId));
        setSelectedLandmarkId(null);
        setIsDirty(true);
        return;
      }
      if (selectedVertexIdx !== null) {
        e.preventDefault();
        if (vertices.length <= 3) return;
        pushHistory(vertices, bays);
        setVertices(vertices.filter((_, i) => i !== selectedVertexIdx));
        setSelectedVertexIdx(null);
        setIsDirty(true);
      }
    }
    window.addEventListener("keydown", onDelete);
    return () => window.removeEventListener("keydown", onDelete);
  }, [mode, vertices, selectedVertexIdx, bays, landmarks, selectedLandmarkId]);

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
    if (mode !== "edit") return;
    const target = e.target as SVGElement;
    if (target.getAttribute("data-vertex") || target.getAttribute("data-bay") || target.getAttribute("data-landmark")) return;
    const { x, y } = worldCoords(e);
    if (placingType !== null) {
      const newLm: Landmark = { id: crypto.randomUUID(), type: placingType, x: snapGrid(x), y: snapGrid(y), w: LANDMARK_SIZE, h: LANDMARK_SIZE, rotation: 0, label: "" };
      pushHistory(vertices, bays, [...landmarks, newLm]);
      setLandmarks((prev) => [...prev, newLm]);
      setSelectedLandmarkId(newLm.id);
      setPlacingType(null);
      setIsDirty(true);
      return;
    }
    if (vertices.length < 3) return;
    // insert vertex on nearest edge if within 15 screen-pixels
    const threshold = 15 / zoom;
    let bestDist = Infinity, bestIdx = -1, bestX = 0, bestY = 0;
    for (let i = 0; i < vertices.length; i++) {
      const j = (i + 1) % vertices.length;
      const r = distToSegment(x, y, vertices[i].x, vertices[i].y, vertices[j].x, vertices[j].y);
      if (r.dist < bestDist) { bestDist = r.dist; bestIdx = i; bestX = r.x; bestY = r.y; }
    }
    if (bestDist <= threshold) {
      pushHistory(vertices, bays);
      const inserted = [...vertices];
      inserted.splice(bestIdx + 1, 0, { x: clamp(snapGrid(bestX), 0, CANVAS_W), y: clamp(snapGrid(bestY), 0, CANVAS_H) });
      setVertices(inserted);
      setSelectedVertexIdx(bestIdx + 1);
      setIsDirty(true);
    }
  }

  function handleBayPointerDown(e: React.PointerEvent<SVGRectElement>, bay: FloorBay) {
    if (mode !== "edit" || bay.floor_x === null || bay.floor_y === null) return;
    e.stopPropagation();
    pushHistory(vertices, bays);
    setSelectedBayId(bay.id);
    setSelectedVertexIdx(null);
    const { x, y } = worldCoords(e);
    setSelectedLandmarkId(null);
    bayDragRef.current = { bayId: bay.id, startWorldX: x, startWorldY: y, origFloorX: bay.floor_x, origFloorY: bay.floor_y };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handleResizePointerDown(e: React.PointerEvent<SVGRectElement>, bay: FloorBay, corner: Corner) {
    if (bay.floor_x === null || bay.floor_y === null) return;
    e.stopPropagation();
    pushHistory(vertices, bays);
    const { x, y } = worldCoords(e);
    bayResizeDragRef.current = {
      bayId: bay.id, corner,
      startWorldX: x, startWorldY: y,
      origFloorX: bay.floor_x, origFloorY: bay.floor_y,
      origFloorW: bay.floor_w, origFloorH: bay.floor_h,
    };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function startPan(e: { clientX: number; clientY: number; pointerId?: number }, target?: Element) {
    panDragRef.current = { startClientX: e.clientX, startClientY: e.clientY, origPanX: pan.x, origPanY: pan.y };
    if (target && "pointerId" in e && e.pointerId !== undefined) {
      (target as Element & { setPointerCapture?: (id: number) => void }).setPointerCapture?.(e.pointerId);
    }
  }

  function handleBackgroundPointerDown(e: React.PointerEvent<SVGRectElement>) {
    if (mode !== "edit") return;
    setSelectedBayId(null);
    setSelectedLandmarkId(null);
    startPan(e, e.target as Element);
  }

  function handleSvgPointerDown(e: React.PointerEvent<SVGSVGElement>) {
    if (mode !== "pan") return;
    startPan(e, svgRef.current ?? undefined);
  }

  function handleSvgPointerMove(e: React.PointerEvent<SVGSVGElement>) {
    // landmark resize
    const lr = landmarkResizeDragRef.current;
    if (lr) {
      const { x, y } = worldCoords(e);
      const dx = x - lr.startWorldX;
      const dy = y - lr.startWorldY;
      const MIN = 10;
      let nx = lr.origX, ny = lr.origY, nw = lr.origW, nh = lr.origH;
      if (lr.corner === "tl") { nx = lr.origX + dx; ny = lr.origY + dy; nw = lr.origW - dx; nh = lr.origH - dy; }
      if (lr.corner === "tr") {                      ny = lr.origY + dy; nw = lr.origW + dx; nh = lr.origH - dy; }
      if (lr.corner === "bl") { nx = lr.origX + dx;                      nw = lr.origW - dx; nh = lr.origH + dy; }
      if (lr.corner === "br") {                                           nw = lr.origW + dx; nh = lr.origH + dy; }
      nw = snapGrid(Math.max(MIN, nw)); nh = snapGrid(Math.max(MIN, nh));
      nx = clamp(snapGrid(nx), 0, CANVAS_W - MIN); ny = clamp(snapGrid(ny), 0, CANVAS_H - MIN);
      setLandmarks((prev) => prev.map((lm) => lm.id === lr.id ? { ...lm, x: nx, y: ny, w: nw, h: nh } : lm));
      setDragCoord({ x: nx, y: ny });
      setIsDirty(true);
      return;
    }
    // landmark drag
    const ld = landmarkDragRef.current;
    if (ld) {
      const { x, y } = worldCoords(e);
      const nx = clamp(snapGrid(ld.origX + x - ld.startWorldX), 0, CANVAS_W);
      const ny = clamp(snapGrid(ld.origY + y - ld.startWorldY), 0, CANVAS_H);
      setLandmarks((prev) => prev.map((lm) => lm.id === ld.id ? { ...lm, x: nx, y: ny } : lm));
      setDragCoord({ x: nx, y: ny });
      setIsDirty(true);
      return;
    }
    // bay resize
    const rd = bayResizeDragRef.current;
    if (rd) {
      const { x, y } = worldCoords(e);
      const dx = x - rd.startWorldX;
      const dy = y - rd.startWorldY;
      const MIN = 10;
      let nx = rd.origFloorX, ny = rd.origFloorY;
      let nw = rd.origFloorW, nh = rd.origFloorH;
      if (rd.corner === "tl") { nx = rd.origFloorX + dx; ny = rd.origFloorY + dy; nw = rd.origFloorW - dx; nh = rd.origFloorH - dy; }
      if (rd.corner === "tr") {                           ny = rd.origFloorY + dy; nw = rd.origFloorW + dx; nh = rd.origFloorH - dy; }
      if (rd.corner === "bl") { nx = rd.origFloorX + dx;                           nw = rd.origFloorW - dx; nh = rd.origFloorH + dy; }
      if (rd.corner === "br") {                                                     nw = rd.origFloorW + dx; nh = rd.origFloorH + dy; }
      nw = snapGrid(Math.max(MIN, nw)); nh = snapGrid(Math.max(MIN, nh));
      nx = clamp(snapGrid(nx), 0, CANVAS_W - MIN); ny = clamp(snapGrid(ny), 0, CANVAS_H - MIN);
      setBays((prev) => prev.map((b) => b.id === rd.bayId ? { ...b, floor_x: nx, floor_y: ny, floor_w: nw, floor_h: nh } : b));
      setDragCoord({ x: nx, y: ny });
      setIsDirty(true);
      return;
    }
    // bay drag
    const bd = bayDragRef.current;
    if (bd) {
      const { x, y } = worldCoords(e);
      const newX = clamp(snapGrid(bd.origFloorX + x - bd.startWorldX), 0, CANVAS_W);
      const newY = clamp(snapGrid(bd.origFloorY + y - bd.startWorldY), 0, CANVAS_H);
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
      let nx = clamp(snapGrid(x), 0, CANVAS_W);
      let ny = clamp(snapGrid(y), 0, CANVAS_H);
      if (e.shiftKey) {
        if (Math.abs(x - vd.origX) >= Math.abs(y - vd.origY)) ny = vd.origY; // horizontal
        else nx = vd.origX;                                                     // vertical
      }
      setVertices((prev) => prev.map((v, i) => i === vd.index ? { x: nx, y: ny } : v));
      setIsDirty(true);
    }
    // always show cursor world position (bay drag sets its own coord and returns early above)
    const { x, y } = worldCoords(e);
    setDragCoord({ x: clamp(snapGrid(x), 0, CANVAS_W), y: clamp(snapGrid(y), 0, CANVAS_H) });
  }

  function handleSvgPointerUp() {
    bayDragRef.current = null;
    bayResizeDragRef.current = null;
    panDragRef.current = null;
    vertexDragRef.current = null;
    landmarkDragRef.current = null;
    landmarkResizeDragRef.current = null;
  }

  function handleVertexPointerDown(e: React.PointerEvent<SVGCircleElement>, index: number) {
    e.stopPropagation();
    pushHistory(vertices, bays);
    vertexDragRef.current = { index, origX: vertices[index].x, origY: vertices[index].y };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handleLandmarkPropChange(field: "x" | "y" | "w" | "h" | "label", value: string | number) {
    if (!selectedLandmarkId) return;
    pushHistory(vertices, bays);
    setLandmarks((prev) => prev.map((lm) => lm.id !== selectedLandmarkId ? lm : { ...lm, [field]: value }));
    setIsDirty(true);
  }

  function handleLandmarkResizePointerDown(e: React.PointerEvent<SVGRectElement>, lm: Landmark, corner: Corner) {
    e.stopPropagation();
    pushHistory(vertices, bays);
    const { x, y } = worldCoords(e);
    landmarkResizeDragRef.current = { id: lm.id, corner, startWorldX: x, startWorldY: y, origX: lm.x, origY: lm.y, origW: lm.w, origH: lm.h };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handleLandmarkPointerDown(e: React.PointerEvent<SVGGElement>, lm: Landmark) {
    if (mode !== "edit") return;
    e.stopPropagation();
    pushHistory(vertices, bays);
    setSelectedLandmarkId(lm.id);
    setSelectedBayId(null);
    setSelectedVertexIdx(null);
    const { x, y } = worldCoords(e);
    landmarkDragRef.current = { id: lm.id, startWorldX: x, startWorldY: y, origX: lm.x, origY: lm.y };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handlePlaceBay(bay: FloorBay) {
    pushHistory(vertices, bays);
    const cx = clamp(snapGrid(CANVAS_W / 2 - bay.floor_w / 2), 0, CANVAS_W);
    const cy = clamp(snapGrid(CANVAS_H / 2 - bay.floor_h / 2), 0, CANVAS_H);
    setBays((prev) => prev.map((b) => b.id === bay.id ? { ...b, floor_x: cx, floor_y: cy } : b));
    setSelectedBayId(bay.id);
    setIsDirty(true);
  }

  function handleRotate() {
    if (!selectedBayId) return;
    pushHistory(vertices, bays);
    setBays((prev) => prev.map((b) => {
      if (b.id !== selectedBayId) return b;
      const rot = ((b.floor_rotation + 90) % 360) as 0 | 90 | 180 | 270;
      return { ...b, floor_rotation: rot, floor_w: b.floor_h, floor_h: b.floor_w };
    }));
    setIsDirty(true);
  }

  function handleRemoveBay(bayId: string) {
    pushHistory(vertices, bays);
    setBays((prev) => prev.map((b) => b.id === bayId ? { ...b, floor_x: null, floor_y: null } : b));
    setSelectedBayId(null);
    setIsDirty(true);
  }

  function handlePropChange(field: "floor_w" | "floor_h" | "floor_x" | "floor_y" | "color", value: string | number) {
    if (!selectedBayId) return;
    pushHistory(vertices, bays);
    setBays((prev) => prev.map((b) => b.id !== selectedBayId ? b : { ...b, [field]: value }));
    setIsDirty(true);
  }

  async function handleRenameBay(bayId: string, newName: string) {
    const name = newName.trim();
    if (!name) return;
    // fetch current bay to preserve shelves, then PUT with new name
    const res = await fetch(`/api/planogram/bays?outlet=${encodeURIComponent(outlet)}`);
    const allBays = await res.json();
    const current = allBays.find((b: { id: string }) => b.id === bayId);
    if (!current) return;
    await fetch(`/api/planogram/bays/${bayId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ outlet: current.outlet, name, description: current.description, shelves: current.shelves }),
    });
    setBays((prev) => prev.map((b) => b.id === bayId ? { ...b, name } : b));
  }

  async function handleCreateBay() {
    const name = newBayName.trim();
    if (!name) return;
    setIsCreating(true);
    try {
      const res = await fetch("/api/planogram/bays", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          outlet,
          name,
          description: newBayDesc.trim(),
          shelves: newBayShelves.map((slot_count, shelf_index) => ({ shelf_index, slot_count })),
        }),
      });
      const created = await res.json();
      const newBay: FloorBay = {
        id: created.id, name: created.name,
        floor_x: null, floor_y: null, floor_w: 50, floor_h: 100,
        floor_rotation: 0, color: "#2E5FA3",
      };
      setBays((prev) => [...prev, newBay]);
      setShowNewBay(false);
      setNewBayName(""); setNewBayDesc(""); setNewBayShelves([7]);
    } finally {
      setIsCreating(false);
    }
  }

  async function handleSave() {
    setIsSaving(true);
    try {
      await fetch(`/api/planogram/floor-plan/${outlet}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vertices, landmarks }),
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
  const selectedBay = bays.find((b) => b.id === selectedBayId) ?? null;

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0, padding: 16, boxSizing: "border-box", overflow: "hidden" }}>
      {/* toolbar */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 12, flexWrap: "wrap" }}>
        <button onClick={() => router.push(`/planogram/${layoutId}/${outlet}`)} style={btnStyle}>← Outlet</button>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>Edit Floor Plan</h1>
        <span style={{ width: 12 }} />
        <button onClick={undo} disabled={!canUndo} style={btnStyle} title="Undo (⌘Z)">↩ Undo</button>
        <button onClick={redo} disabled={!canRedo} style={btnStyle} title="Redo (⌘⇧Z)">↪ Redo</button>
        <span style={{ width: 4 }} />
        <button
          onClick={() => { setMode(mode === "pan" ? "edit" : "pan"); setSelectedBayId(null); setSelectedVertexIdx(null); }}
          style={{ ...btnStyle, background: mode === "pan" ? NAVY : "#eee", color: mode === "pan" ? "#fff" : "#333", border: "none" }}
          title="Hand tool — drag to pan">
          ✋ Hand
        </button>
        <button onClick={() => { pushHistory(vertices, bays); setVertices(DEFAULT_ROOM); setSelectedVertexIdx(null); setIsDirty(true); }} style={{ ...btnStyle, borderColor: "#e55", color: "#e55" }}>Reset room</button>
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
        <button onClick={async () => { await handleSave(); router.push(`/planogram/${layoutId}/${outlet}`); }} disabled={isSaving} style={{ ...btnStyle, background: NAVY, color: "#fff", border: "none" }}>Save &amp; Exit</button>
      </div>

      <div style={{ display: "flex", flex: 1, minHeight: 0 }}>

        {/* left panel — shape library */}
        {mode === "edit" && (
          <div style={{ width: 180, flexShrink: 0, borderRight: "1px solid #ddd", display: "flex", flexDirection: "column", background: "#fafafa", overflowY: "auto" }}>

            {/* Bays section */}
            <button onClick={() => setBaysSectionOpen((v) => !v)}
              style={{ display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", padding: "7px 10px", background: "#f0f0f0", border: "none", borderBottom: "1px solid #ddd", cursor: "pointer", fontWeight: 600, fontSize: 12, color: "#333", flexShrink: 0 }}>
              Bays <span style={{ fontSize: 10 }}>{baysSectionOpen ? "▾" : "▸"}</span>
            </button>
            {baysSectionOpen && (
              <div style={{ padding: "8px 10px", borderBottom: "1px solid #e8e8e8" }}>
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
                    onClick={() => { if (b.floor_x !== null) { setSelectedBayId(b.id); setSelectedVertexIdx(null); setSelectedLandmarkId(null); } }}
                    style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #eee", cursor: b.floor_x !== null ? "pointer" : "default", background: b.id === selectedBayId ? "#f0f4ff" : "transparent" }}>
                    <span style={{ fontSize: 12, color: b.floor_x !== null ? "#333" : "#888" }}>{b.name}</span>
                    {b.floor_x === null && (
                      <button onClick={(e) => { e.stopPropagation(); handlePlaceBay(b); }}
                        style={{ fontSize: 11, padding: "1px 6px", background: NAVY, color: "#fff", border: "none", borderRadius: 3, cursor: "pointer" }}>+</button>
                    )}
                  </div>
                ))}
                {bays.filter((b) => bayListFilter === "all" || (bayListFilter === "placed" ? b.floor_x !== null : b.floor_x === null)).length === 0 && (
                  <p style={{ fontSize: 11, color: "#bbb", margin: "4px 0" }}>{bayListFilter === "unplaced" ? "All bays placed" : "No bays"}</p>
                )}
                <button onClick={() => setShowNewBay(true)}
                  style={{ marginTop: 8, width: "100%", padding: "4px 0", fontSize: 11, background: NAVY, color: "#fff", border: "none", borderRadius: 3, cursor: "pointer" }}>
                  + New Bay
                </button>
              </div>
            )}

            {/* Icons section */}
            <button onClick={() => setIconsSectionOpen((v) => !v)}
              style={{ display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", padding: "7px 10px", background: "#f0f0f0", border: "none", borderBottom: "1px solid #ddd", cursor: "pointer", fontWeight: 600, fontSize: 12, color: "#333", flexShrink: 0 }}>
              Landmarks <span style={{ fontSize: 10 }}>{iconsSectionOpen ? "▾" : "▸"}</span>
            </button>
            {iconsSectionOpen && (
              <div style={{ padding: "8px 10px", display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
                {(["door", "computer", "display"] as LandmarkType[]).map((t) => (
                  <button key={t} title={`Place ${LANDMARK_LABELS[t]}`}
                    onClick={() => setPlacingType(placingType === t ? null : t)}
                    style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4, padding: "8px 4px", border: "1px solid", borderColor: placingType === t ? "#e0a500" : "#ddd", borderRadius: 4, background: placingType === t ? "#fff8e0" : "#fff", cursor: "pointer" }}>
                    <LandmarkIcon type={t} size={32} color={placingType === t ? "#c88a00" : NAVY} />
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {/* canvas */}
        <div style={{ flex: 1, minHeight: 0, position: "relative", overflow: "hidden", background: "#f9f9f9", border: "1px solid #ddd" }}>
          <svg ref={svgRef}
            style={{ display: "block", width: "100%", height: "100%",
              cursor: mode === "pan" ? "grab" : "default" }}
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
                style={{ cursor: mode === "edit" ? "grab" : "default" }}
                onPointerDown={handleBackgroundPointerDown} />

              {/* adaptive grid lines */}
              {gridXs.map((x) => (
                <line key={`gx-${x}`} x1={x} y1={visTop} x2={x} y2={visBot}
                  stroke={x % labelStep === 0 ? "#ccc" : "#e8e8e8"} strokeWidth={1 / zoom} style={{ pointerEvents: "none" }} />
              ))}
              {gridYs.map((y) => (
                <line key={`gy-${y}`} x1={visLeft} y1={y} x2={visRight} y2={y}
                  stroke={y % labelStep === 0 ? "#ccc" : "#e8e8e8"} strokeWidth={1 / zoom} style={{ pointerEvents: "none" }} />
              ))}


              {/* room polygon */}
              {vertices.length >= 3 && (
                <polygon points={polygonPoints} fill={`${NAVY}15`} stroke={NAVY} strokeWidth={2 / zoom} strokeLinejoin="round" style={{ pointerEvents: "none" }} />
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
                      style={{ cursor: mode === "edit" ? "grab" : "default" }}
                      onPointerDown={(e) => handleBayPointerDown(e, b)} />
                    <text x={x + b.floor_w / 2} y={y + b.floor_h / 2} textAnchor="middle" dominantBaseline="middle"
                      fontSize={fontSize} fontWeight="bold" fill={labelColor(b.color)}
                      style={{ pointerEvents: "none", userSelect: "none" }}>{b.name}</text>
                    {isSelected && mode === "edit" && (
                      [["tl", x, y, "nwse-resize"], ["tr", x + b.floor_w, y, "nesw-resize"],
                       ["bl", x, y + b.floor_h, "nesw-resize"], ["br", x + b.floor_w, y + b.floor_h, "nwse-resize"]]
                        .map(([corner, cx, cy, cursor]) => (
                          <rect key={corner as string}
                            x={(cx as number) - 5 / zoom} y={(cy as number) - 5 / zoom}
                            width={10 / zoom} height={10 / zoom}
                            fill="white" stroke={NAVY} strokeWidth={1.5 / zoom}
                            style={{ cursor: cursor as string }}
                            onPointerDown={(e) => handleResizePointerDown(e, b, corner as Corner)} />
                        ))
                    )}
                  </g>
                );
              })}

              {/* landmarks */}
              {landmarks.map((lm) => {
                const isSelLm = lm.id === selectedLandmarkId;
                return (
                  <g key={lm.id} data-landmark="true"
                    transform={`translate(${lm.x},${lm.y}) rotate(${lm.rotation},${lm.w / 2},${lm.h / 2})`}
                    style={{ cursor: mode === "edit" ? "grab" : "default" }}
                    onPointerDown={(e) => handleLandmarkPointerDown(e, lm)}>
                    <rect x={0} y={0} width={lm.w} height={lm.h}
                      fill={isSelLm ? "#fff8e0" : lm.type === "door" ? "transparent" : "rgba(255,255,255,0.7)"}
                      stroke={isSelLm ? "#f0a500" : lm.type === "door" ? "none" : NAVY} strokeWidth={isSelLm ? 2 / zoom : 1 / zoom}
                      rx={3 / zoom} style={{ pointerEvents: "all" }} />
                    <LandmarkIconSvg type={lm.type} w={lm.w} h={lm.h} color={NAVY} />
                    {lm.label && (
                      <text x={lm.w / 2} y={lm.h + 12 / zoom} textAnchor="middle"
                        fontSize={10 / zoom} fill="#555" style={{ pointerEvents: "none", userSelect: "none" }}>
                        {lm.label}
                      </text>
                    )}
                    {isSelLm && mode === "edit" && (
                      [["tl", 0, 0, "nwse-resize"], ["tr", lm.w, 0, "nesw-resize"],
                       ["bl", 0, lm.h, "nesw-resize"], ["br", lm.w, lm.h, "nwse-resize"]]
                        .map(([corner, cx, cy, cursor]) => (
                          <rect key={corner as string}
                            x={(cx as number) - 5 / zoom} y={(cy as number) - 5 / zoom}
                            width={10 / zoom} height={10 / zoom}
                            fill="white" stroke={NAVY} strokeWidth={1.5 / zoom}
                            style={{ cursor: cursor as string }}
                            onPointerDown={(e) => handleLandmarkResizePointerDown(e, lm, corner as Corner)} />
                        ))
                    )}
                  </g>
                );
              })}

              {/* vertex handles */}
              {mode === "edit" && vertices.map((v, i) => (
                <circle key={i} cx={v.x} cy={v.y} r={7 / zoom}
                  fill={i === selectedVertexIdx ? "#e55" : i === 0 ? "#2a7" : "#fff"}
                  stroke={NAVY} strokeWidth={2 / zoom}
                  data-vertex="true" style={{ cursor: "move" }}
                  onPointerDown={(e) => handleVertexPointerDown(e, i)}
                  onClick={(e) => { e.stopPropagation(); setSelectedVertexIdx(i); setSelectedBayId(null); setSelectedLandmarkId(null); }} />
              ))}
            </g>
          </svg>

          {/* snap level badge */}
          <div style={{ position: "absolute", bottom: 8, right: 8, fontSize: 10, color: "#aaa", background: "rgba(255,255,255,0.8)", padding: "2px 6px", borderRadius: 3, pointerEvents: "none" }}>
            snap {gridStep} cm
          </div>

          {/* y-axis ruler strip — left edge */}
          <div style={{ position: "absolute", top: 0, left: 0, width: 44, bottom: 0, background: "#f5f5f5", borderRight: "1px solid #ddd", pointerEvents: "none", zIndex: 1, overflow: "hidden" }}>
            {gridYs.map((y) => (
              <span key={y} style={{ position: "absolute", right: 5, top: y * zoom + pan.y - 6, fontSize: 10, color: y % labelStep === 0 ? "#888" : "#bbb", fontWeight: y % labelStep === 0 ? 600 : 400, whiteSpace: "nowrap" }}>{y}</span>
            ))}
          </div>

          {/* x-axis ruler strip — top edge */}
          <div style={{ position: "absolute", top: 0, left: 44, right: 0, height: 20, background: "#f5f5f5", borderBottom: "1px solid #ddd", pointerEvents: "none", zIndex: 1, overflow: "hidden" }}>
            {gridXs.map((x) => (
              <span key={x} style={{ position: "absolute", left: x * zoom + pan.x - 44, top: 4, fontSize: 10, color: x % labelStep === 0 ? "#888" : "#bbb", fontWeight: x % labelStep === 0 ? 600 : 400, transform: "translateX(-50%)", whiteSpace: "nowrap" }}>{x}</span>
            ))}
          </div>
        </div>

        {/* properties panel */}
        {mode === "edit" && selectedBay && selectedBay.floor_x !== null && (
          <div style={{ width: 160, flexShrink: 0, borderLeft: "1px solid #ddd", padding: "8px 10px", overflowY: "auto", background: "#fafafa" }}>
            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 10 }}>
              Name
              <input type="text"
                key={`${selectedBayId}-name`}
                defaultValue={selectedBay.name}
                onBlur={(e) => handleRenameBay(selectedBay.id, e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

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

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
              X (cm)
              <input type="number" min={0} max={CANVAS_W} step={1}
                key={`${selectedBayId}-x`}
                defaultValue={selectedBay.floor_x ?? 0}
                onBlur={(e) => handlePropChange("floor_x", clamp(Math.round(Number(e.target.value) || 0), 0, CANVAS_W))}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 10 }}>
              Y (cm)
              <input type="number" min={0} max={CANVAS_H} step={1}
                key={`${selectedBayId}-y`}
                defaultValue={selectedBay.floor_y ?? 0}
                onBlur={(e) => handlePropChange("floor_y", clamp(Math.round(Number(e.target.value) || 0), 0, CANVAS_H))}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <button onClick={handleRotate} style={{ ...btnStyle, width: "100%", marginBottom: 6, borderColor: "#f0a500", color: "#f0a500" }}>Rotate 90°</button>
            <button onClick={() => handleRemoveBay(selectedBay.id)} style={{ ...btnStyle, width: "100%", borderColor: "#e55", color: "#e55" }}>Remove</button>
          </div>
        )}

        {/* landmark properties panel */}
        {mode === "edit" && selectedLandmarkId !== null && !selectedBay && selectedVertexIdx === null && (() => {
          const lm = landmarks.find((l) => l.id === selectedLandmarkId);
          if (!lm) return null;
          return (
            <div style={{ width: 160, flexShrink: 0, borderLeft: "1px solid #ddd", padding: "8px 10px", background: "#fafafa", overflowY: "auto" }}>
              <p style={{ fontSize: 11, color: "#888", margin: "0 0 10px", fontWeight: "bold" }}>{LANDMARK_LABELS[lm.type]}</p>

              <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
                Label
                <input type="text"
                  key={`lm-${lm.id}-label`}
                  defaultValue={lm.label}
                  onBlur={(e) => handleLandmarkPropChange("label", e.target.value.trim())}
                  onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                  style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
              </label>

              <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
                Width (cm)
                <input type="number" min={10} max={2000} step={10}
                  key={`lm-${lm.id}-w`}
                  defaultValue={lm.w}
                  onBlur={(e) => handleLandmarkPropChange("w", Math.max(10, Number(e.target.value) || 10))}
                  onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                  style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
              </label>

              <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
                Depth (cm)
                <input type="number" min={10} max={2000} step={10}
                  key={`lm-${lm.id}-h`}
                  defaultValue={lm.h}
                  onBlur={(e) => handleLandmarkPropChange("h", Math.max(10, Number(e.target.value) || 10))}
                  onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                  style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
              </label>

              <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
                X (cm)
                <input type="number" min={0} max={CANVAS_W} step={1}
                  key={`lm-${lm.id}-x`}
                  defaultValue={lm.x}
                  onBlur={(e) => handleLandmarkPropChange("x", clamp(Math.round(Number(e.target.value) || 0), 0, CANVAS_W))}
                  onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                  style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
              </label>

              <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 10 }}>
                Y (cm)
                <input type="number" min={0} max={CANVAS_H} step={1}
                  key={`lm-${lm.id}-y`}
                  defaultValue={lm.y}
                  onBlur={(e) => handleLandmarkPropChange("y", clamp(Math.round(Number(e.target.value) || 0), 0, CANVAS_H))}
                  onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                  style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
              </label>

              <button onClick={() => {
                pushHistory(vertices, bays, landmarks.filter((l) => l.id !== lm.id));
                setLandmarks((prev) => prev.filter((l) => l.id !== lm.id));
                setSelectedLandmarkId(null);
                setIsDirty(true);
              }} style={{ ...btnStyle, width: "100%", borderColor: "#e55", color: "#e55" }}>Remove</button>
            </div>
          );
        })()}

        {/* vertex properties panel */}
        {mode === "edit" && selectedVertexIdx !== null && vertices[selectedVertexIdx] && (
          <div style={{ width: 160, flexShrink: 0, borderLeft: "1px solid #ddd", padding: "8px 10px", background: "#fafafa" }}>
            <p style={{ fontSize: 11, color: "#888", margin: "0 0 10px", fontWeight: "bold" }}>Vertex {selectedVertexIdx + 1}</p>

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
              X (cm)
              <input type="number" min={0} max={CANVAS_W} step={1}
                key={`v${selectedVertexIdx}-x`}
                defaultValue={vertices[selectedVertexIdx].x}
                onBlur={(e) => {
                  const nx = clamp(Math.round(Number(e.target.value) || 0), 0, CANVAS_W);
                  pushHistory(vertices, bays);
                  setVertices((prev) => prev.map((v, i) => i === selectedVertexIdx ? { ...v, x: nx } : v));
                  setIsDirty(true);
                }}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <label style={{ fontSize: 11, color: "#555", display: "block", marginBottom: 6 }}>
              Y (cm)
              <input type="number" min={0} max={CANVAS_H} step={1}
                key={`v${selectedVertexIdx}-y`}
                defaultValue={vertices[selectedVertexIdx].y}
                onBlur={(e) => {
                  const ny = clamp(Math.round(Number(e.target.value) || 0), 0, CANVAS_H);
                  pushHistory(vertices, bays);
                  setVertices((prev) => prev.map((v, i) => i === selectedVertexIdx ? { ...v, y: ny } : v));
                  setIsDirty(true);
                }}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>
          </div>
        )}
      </div>

      {showNewBay && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
          onClick={(e) => { if (e.target === e.currentTarget) setShowNewBay(false); }}>
          <div style={{ background: "#fff", borderRadius: 8, padding: 24, width: 340, boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}>
            <h2 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 700 }}>New Bay</h2>

            <label style={{ display: "block", fontSize: 12, color: "#555", marginBottom: 10 }}>
              Name
              <input autoFocus value={newBayName} onChange={(e) => setNewBayName(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleCreateBay()}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "4px 8px", fontSize: 13, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <label style={{ display: "block", fontSize: 12, color: "#555", marginBottom: 14 }}>
              Description
              <input value={newBayDesc} onChange={(e) => setNewBayDesc(e.target.value)}
                style={{ display: "block", width: "100%", marginTop: 3, padding: "4px 8px", fontSize: 13, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box" }} />
            </label>

            <div style={{ fontSize: 12, color: "#555", marginBottom: 6 }}>Shelves</div>
            {newBayShelves.map((slots, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                <span style={{ fontSize: 11, color: "#888", width: 52 }}>Shelf {i + 1}</span>
                <input type="number" min={1} max={20} value={slots}
                  onChange={(e) => setNewBayShelves((prev) => prev.map((v, j) => j === i ? Math.max(1, Number(e.target.value) || 1) : v))}
                  style={{ width: 56, padding: "3px 6px", fontSize: 12, border: "1px solid #ccc", borderRadius: 4 }} />
                <span style={{ fontSize: 11, color: "#aaa" }}>slots</span>
                {newBayShelves.length > 1 && (
                  <button onClick={() => setNewBayShelves((prev) => prev.filter((_, j) => j !== i))}
                    style={{ marginLeft: "auto", fontSize: 11, color: "#e55", background: "none", border: "none", cursor: "pointer" }}>✕</button>
                )}
              </div>
            ))}
            <button onClick={() => setNewBayShelves((prev) => [...prev, 7])}
              style={{ fontSize: 11, color: NAVY, background: "none", border: "none", cursor: "pointer", padding: 0, marginBottom: 18 }}>+ Add shelf</button>

            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button onClick={() => setShowNewBay(false)} style={btnStyle}>Cancel</button>
              <button onClick={handleCreateBay} disabled={!newBayName.trim() || isCreating}
                style={{ ...btnStyle, background: NAVY, color: "#fff", border: "none", opacity: !newBayName.trim() ? 0.5 : 1 }}>
                {isCreating ? "Creating…" : "Create"}
              </button>
            </div>
          </div>
        </div>
      )}

      <div style={{ marginTop: 8, fontSize: 11, color: "#aaa", display: "flex", justifyContent: "space-between" }}>
        <span>
          {mode === "edit" && placingType !== null && `Click canvas to place ${LANDMARK_LABELS[placingType]} · click palette button again to cancel.`}
          {mode === "edit" && placingType === null && selectedVertexIdx !== null && "Vertex selected — drag (Shift = axis lock) · Delete to remove (min 3) · Esc to deselect."}
          {mode === "edit" && placingType === null && selectedBay && selectedVertexIdx === null && "Bay selected — drag to reposition · adjust size and colour in the panel."}
          {mode === "edit" && placingType === null && selectedLandmarkId !== null && !selectedBay && selectedVertexIdx === null && "Landmark selected — drag to reposition · Delete to remove."}
          {mode === "edit" && placingType === null && !selectedBay && selectedVertexIdx === null && selectedLandmarkId === null && "Click a vertex to select it · click an edge to insert · drag bays to reposition · drag background to pan."}
          {mode === "pan" && "Drag anywhere to pan · scroll to zoom."}
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
