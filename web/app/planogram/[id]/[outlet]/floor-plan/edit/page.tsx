"use client";
import React, { useState, useEffect, useRef } from "react";
import { useParams, useRouter } from "next/navigation";

const NAVY = "#1a2b5f";
const GRID_STEP = 40;
const CANVAS_W = 30;
const CANVAS_H = 20;
const RULER_W = 36;
const CM_PER_GRID = 50;

type Vertex = { x: number; y: number };

type FloorBay = {
  id: string;
  name: string;
  floor_x: number | null;
  floor_y: number | null;
  floor_w: number;
  floor_h: number;
  floor_rotation: 0 | 90 | 180 | 270;
};

type Mode = "place" | "room";

type BayDrag = {
  bayId: string;
  startSvgX: number;
  startSvgY: number;
  origFloorX: number;
  origFloorY: number;
};

type VertexDrag = { index: number };

const btnStyle: React.CSSProperties = {
  padding: "3px 10px", fontSize: 12, background: "#eee", color: "#333",
  border: "1px solid #ccc", borderRadius: 4, cursor: "pointer",
};

function snapGrid(px: number): number {
  return Math.round(px / GRID_STEP);
}

export default function FloorPlanEdit() {
  const { id: layoutId, outlet } = useParams<{ id: string; outlet: string }>();
  const router = useRouter();

  const svgRef = useRef<SVGSVGElement>(null);
  const bayDragRef = useRef<BayDrag | null>(null);
  const vertexDragRef = useRef<VertexDrag | null>(null);

  const [vertices, setVertices] = useState<Vertex[]>([]);
  const [polygonClosed, setPolygonClosed] = useState(false);
  const [bays, setBays] = useState<FloorBay[]>([]);
  const [mode, setMode] = useState<Mode>("place");
  const [selectedBayId, setSelectedBayId] = useState<string | null>(null);
  const [isDirty, setIsDirty] = useState(false);
  const [isSaving, setIsSaving] = useState(false);

  useEffect(() => {
    fetch(`/api/planogram/floor-plan/${outlet}`)
      .then((r) => r.json())
      .then((data) => {
        setVertices(data.vertices);
        setPolygonClosed(data.vertices.length >= 3);
        setBays(data.bays);
        setIsDirty(false);
      });
  }, [outlet]);

  function svgCoords(e: React.PointerEvent | MouseEvent): { x: number; y: number } {
    const svg = svgRef.current!;
    const pt = svg.createSVGPoint();
    pt.x = e.clientX;
    pt.y = e.clientY;
    const local = pt.matrixTransform(svg.getScreenCTM()!.inverse());
    return { x: local.x, y: local.y };
  }

  function handleCanvasClick(e: React.MouseEvent<SVGSVGElement>) {
    if (mode !== "room" || polygonClosed) return;
    const target = e.target as SVGElement;
    if (target.getAttribute("data-vertex")) return;
    const { x, y } = svgCoords(e as unknown as MouseEvent);
    const snapped = { x: Math.max(0, snapGrid(x)), y: Math.max(0, snapGrid(y)) };
    if (vertices.length >= 3) {
      const first = vertices[0];
      if (Math.abs(snapped.x - first.x) <= 1 && Math.abs(snapped.y - first.y) <= 1) {
        setPolygonClosed(true);
        setIsDirty(true);
        return;
      }
    }
    setVertices((prev) => [...prev, snapped]);
    setIsDirty(true);
  }

  function handleBayPointerDown(e: React.PointerEvent<SVGRectElement>, bay: FloorBay) {
    if (mode !== "place" || bay.floor_x === null || bay.floor_y === null) return;
    e.stopPropagation();
    setSelectedBayId(bay.id);
    const { x, y } = svgCoords(e);
    bayDragRef.current = { bayId: bay.id, startSvgX: x, startSvgY: y, origFloorX: bay.floor_x, origFloorY: bay.floor_y };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handleBayPointerMove(e: React.PointerEvent<SVGRectElement>) {
    const drag = bayDragRef.current;
    if (!drag) return;
    const { x, y } = svgCoords(e);
    const newX = Math.max(0, snapGrid(drag.origFloorX * GRID_STEP + (x - drag.startSvgX)));
    const newY = Math.max(0, snapGrid(drag.origFloorY * GRID_STEP + (y - drag.startSvgY)));
    setBays((prev) => prev.map((b) => b.id === drag.bayId ? { ...b, floor_x: newX, floor_y: newY } : b));
    setIsDirty(true);
  }

  function handleBayPointerUp() { bayDragRef.current = null; }

  function handleVertexPointerDown(e: React.PointerEvent<SVGCircleElement>, index: number) {
    e.stopPropagation();
    vertexDragRef.current = { index };
    (e.target as Element).setPointerCapture(e.pointerId);
  }

  function handleVertexPointerMove(e: React.PointerEvent<SVGCircleElement>) {
    const drag = vertexDragRef.current;
    if (!drag) return;
    const { x, y } = svgCoords(e);
    const snapped = { x: Math.max(0, snapGrid(x)), y: Math.max(0, snapGrid(y)) };
    setVertices((prev) => prev.map((v, i) => i === drag.index ? snapped : v));
    setIsDirty(true);
  }

  function handleVertexPointerUp() { vertexDragRef.current = null; }

  function handlePlaceBay(bay: FloorBay) {
    const cx = Math.round(CANVAS_W / 2 - bay.floor_w / 2);
    const cy = Math.round(CANVAS_H / 2 - bay.floor_h / 2);
    setBays((prev) => prev.map((b) => b.id === bay.id ? { ...b, floor_x: cx, floor_y: cy } : b));
    setSelectedBayId(bay.id);
    setIsDirty(true);
  }

  function handleRotate() {
    if (!selectedBayId) return;
    setBays((prev) => prev.map((b) => {
      if (b.id !== selectedBayId) return b;
      const rot = ((b.floor_rotation + 90) % 360) as 0 | 90 | 180 | 270;
      return { ...b, floor_rotation: rot, floor_w: b.floor_h, floor_h: b.floor_w };
    }));
    setIsDirty(true);
  }

  function handleRemoveBay(bayId: string) {
    setBays((prev) => prev.map((b) => b.id === bayId ? { ...b, floor_x: null, floor_y: null } : b));
    setSelectedBayId(null);
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
            body: JSON.stringify({ floor_x: b.floor_x, floor_y: b.floor_y, floor_w: b.floor_w, floor_h: b.floor_h, floor_rotation: b.floor_rotation }),
          })
        )
      );
      setIsDirty(false);
    } finally {
      setIsSaving(false);
    }
  }

  const placedBays = bays.filter((b) => b.floor_x !== null);
  const unplacedBays = bays.filter((b) => b.floor_x === null);
  const selectedBay = bays.find((b) => b.id === selectedBayId) ?? null;
  const polygonPoints = vertices.map((v) => `${v.x * GRID_STEP},${v.y * GRID_STEP}`).join(" ");

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh", padding: 16, boxSizing: "border-box", overflow: "hidden" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 12, flexWrap: "wrap" }}>
        <button onClick={() => router.push(`/planogram/${layoutId}/${outlet}/floor-plan`)} style={btnStyle}>← Floor plan</button>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>Edit Floor Plan</h1>

        <span style={{ width: 12 }} />

        <button onClick={() => setMode("place")}
          style={{ ...btnStyle, background: mode === "place" ? NAVY : "#eee", color: mode === "place" ? "#fff" : "#333", border: "none" }}>
          Place Bays
        </button>
        <button onClick={() => { setMode("room"); setPolygonClosed(false); }}
          style={{ ...btnStyle, background: mode === "room" ? NAVY : "#eee", color: mode === "room" ? "#fff" : "#333", border: "none" }}>
          Edit Room
        </button>

        {mode === "room" && vertices.length >= 3 && !polygonClosed && (
          <button onClick={() => { setPolygonClosed(true); setIsDirty(true); }}
            style={{ ...btnStyle, borderColor: "#2a7", color: "#2a7" }}>Close polygon</button>
        )}
        {mode === "room" && vertices.length > 0 && (
          <button onClick={() => { setVertices([]); setPolygonClosed(false); setIsDirty(true); }}
            style={{ ...btnStyle, borderColor: "#e55", color: "#e55" }}>Clear room</button>
        )}

        {selectedBay && selectedBay.floor_x !== null && mode === "place" && (
          <>
            <button onClick={handleRotate} style={{ ...btnStyle, borderColor: "#f0a500", color: "#f0a500" }}>Rotate 90°</button>
            <button onClick={() => handleRemoveBay(selectedBay.id)} style={{ ...btnStyle, borderColor: "#e55", color: "#e55" }}>Remove</button>
          </>
        )}

        <span style={{ flex: 1 }} />
        <button onClick={handleSave} disabled={!isDirty || isSaving} style={btnStyle}>
          {isSaving ? "Saving…" : "Save"}
        </button>
      </div>

      <div style={{ display: "flex", flex: 1, gap: 16, minHeight: 0 }}>
        {mode === "place" && (
          <div style={{ width: 160, flexShrink: 0, borderRight: "1px solid #ccc", paddingRight: 12, overflowY: "auto" }}>
            <p style={{ fontSize: 11, color: "#888", margin: "0 0 8px", fontWeight: "bold" }}>Unplaced bays</p>
            {unplacedBays.length === 0 && <p style={{ fontSize: 11, color: "#ccc" }}>All bays placed</p>}
            {unplacedBays.map((b) => (
              <div key={b.id} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #eee" }}>
                <span style={{ fontSize: 12 }}>{b.name}</span>
                <button onClick={() => handlePlaceBay(b)}
                  style={{ fontSize: 11, padding: "1px 6px", background: NAVY, color: "#fff", border: "none", borderRadius: 3, cursor: "pointer" }}>+</button>
              </div>
            ))}
          </div>
        )}

        <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
          <svg ref={svgRef}
            viewBox={`${-RULER_W} ${-RULER_W} ${CANVAS_W * GRID_STEP + RULER_W} ${CANVAS_H * GRID_STEP + RULER_W}`}
            style={{ display: "block", width: "100%", height: "auto", background: "#f9f9f9", border: "1px solid #ddd",
              cursor: mode === "room" && !polygonClosed ? "crosshair" : "default" }}
            onClick={handleCanvasClick}>

            <rect x={-RULER_W} y={-RULER_W} width={RULER_W} height={RULER_W} fill="#e0e0e0" />
            <text x={-RULER_W / 2} y={-RULER_W / 2} textAnchor="middle" dominantBaseline="middle" fontSize={10} fill="#888" style={{ pointerEvents: "none" }}>cm</text>
            <rect x={0} y={-RULER_W} width={CANVAS_W * GRID_STEP} height={RULER_W} fill="#e8e8e8" />
            <rect x={-RULER_W} y={0} width={RULER_W} height={CANVAS_H * GRID_STEP} fill="#e8e8e8" />

            {Array.from({ length: CANVAS_W + 1 }, (_, i) => {
              const x = i * GRID_STEP;
              const cm = i * CM_PER_GRID;
              const major = i % 2 === 0;
              return (
                <g key={`tr-${i}`} style={{ pointerEvents: "none" }}>
                  <line x1={x} y1={major ? -RULER_W : -RULER_W / 2} x2={x} y2={0} stroke="#aaa" strokeWidth={0.5} />
                  {major && <text x={x} y={-RULER_W / 2 - 1} textAnchor="middle" dominantBaseline="auto" fontSize={10} fill="#555">{cm}</text>}
                </g>
              );
            })}

            {Array.from({ length: CANVAS_H + 1 }, (_, i) => {
              const y = i * GRID_STEP;
              const cm = i * CM_PER_GRID;
              const major = i % 2 === 0;
              return (
                <g key={`lr-${i}`} style={{ pointerEvents: "none" }}>
                  <line x1={major ? -RULER_W : -RULER_W / 2} y1={y} x2={0} y2={y} stroke="#aaa" strokeWidth={0.5} />
                  {major && <text x={-3} y={y} textAnchor="end" dominantBaseline="middle" fontSize={10} fill="#555">{cm}</text>}
                </g>
              );
            })}

            {Array.from({ length: CANVAS_W + 1 }, (_, xi) =>
              Array.from({ length: CANVAS_H + 1 }, (_, yi) => (
                <circle key={`${xi}-${yi}`} cx={xi * GRID_STEP} cy={yi * GRID_STEP} r={1.5} fill="#ccc" style={{ pointerEvents: "none" }} />
              ))
            )}

            {vertices.length >= 2 && polygonClosed && (
              <polygon points={polygonPoints} fill={`${NAVY}15`} stroke={NAVY} strokeWidth={2} strokeLinejoin="round" style={{ pointerEvents: "none" }} />
            )}
            {vertices.length >= 2 && !polygonClosed && (
              <polyline points={polygonPoints} fill="none" stroke={NAVY} strokeWidth={2} strokeDasharray="6,4" strokeLinejoin="round" style={{ pointerEvents: "none" }} />
            )}

            {placedBays.map((b) => {
              const x = (b.floor_x ?? 0) * GRID_STEP;
              const y = (b.floor_y ?? 0) * GRID_STEP;
              const w = b.floor_w * GRID_STEP;
              const h = b.floor_h * GRID_STEP;
              const isSelected = b.id === selectedBayId;
              return (
                <g key={b.id}>
                  <rect x={x} y={y} width={w} height={h} fill="#fff"
                    stroke={isSelected ? "#f0a500" : NAVY} strokeWidth={isSelected ? 2.5 : 2} rx={3}
                    style={{ cursor: mode === "place" ? "grab" : "default" }}
                    onPointerDown={(e) => handleBayPointerDown(e, b)}
                    onPointerMove={handleBayPointerMove}
                    onPointerUp={handleBayPointerUp} />
                  <text x={x + w / 2} y={y + h / 2} textAnchor="middle" dominantBaseline="middle"
                    fontSize={Math.min(14, w / b.name.length * 1.6)} fontWeight="bold" fill={NAVY}
                    style={{ pointerEvents: "none", userSelect: "none" }}>{b.name}</text>
                </g>
              );
            })}

            {mode === "room" && vertices.map((v, i) => (
              <circle key={i} cx={v.x * GRID_STEP} cy={v.y * GRID_STEP} r={7}
                fill={i === 0 ? "#2a7" : "#fff"} stroke={NAVY} strokeWidth={2}
                data-vertex="true" style={{ cursor: "move" }}
                onPointerDown={(e) => handleVertexPointerDown(e, i)}
                onPointerMove={handleVertexPointerMove}
                onPointerUp={handleVertexPointerUp} />
            ))}
          </svg>
        </div>
      </div>

      <div style={{ marginTop: 8, fontSize: 11, color: "#aaa" }}>
        {mode === "room" && !polygonClosed && "Click the canvas to add vertices. Click near the first vertex (green) to close the polygon."}
        {mode === "room" && polygonClosed && "Drag vertices to adjust. Click \"Edit Room\" again to reopen the polygon."}
        {mode === "place" && "Click + to place a bay, then drag it into position. Click a placed bay to select it."}
      </div>
    </div>
  );
}
