"use client";
import { useState, useEffect } from "react";
import { useParams, useRouter } from "next/navigation";
import PrintDialog from "../../components/PrintDialog";

const NAVY = "#1a2b5f";
const CANVAS_W = 2000;
const CANVAS_H = 1500;

type Vertex = { x: number; y: number };
type FloorBay = { id: string; name: string; floor_x: number | null; floor_y: number | null; floor_w: number; floor_h: number; floor_rotation: number; color: string };
type FloorPlanData = { vertices: Vertex[]; bays: FloorBay[] };
type BayPlanogramSummary = { bay_id: string; bay_name: string; has_planogram: boolean; shelf_count: number };

function labelColor(bg: string): string {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(bg);
  if (!m) return "#fff";
  const lum = (0.299 * parseInt(m[1], 16) + 0.587 * parseInt(m[2], 16) + 0.114 * parseInt(m[3], 16)) / 255;
  return lum > 0.5 ? "#111" : "#fff";
}

function computeBounds(vertices: Vertex[], bays: FloorBay[], padding = 80): { viewBox: string; vbW: number } {
  const xs: number[] = [];
  const ys: number[] = [];
  for (const v of vertices) { xs.push(v.x); ys.push(v.y); }
  for (const b of bays) {
    if (b.floor_x !== null) {
      xs.push(b.floor_x, b.floor_x + b.floor_w);
      ys.push(b.floor_y ?? 0, (b.floor_y ?? 0) + b.floor_h);
    }
  }
  if (xs.length === 0) return { viewBox: `0 0 ${CANVAS_W} ${CANVAS_H}`, vbW: CANVAS_W };
  const minX = Math.min(...xs) - padding;
  const minY = Math.min(...ys) - padding;
  const maxX = Math.max(...xs) + padding;
  const maxY = Math.max(...ys) + padding;
  const vbW = maxX - minX;
  return { viewBox: `${minX} ${minY} ${vbW} ${maxY - minY}`, vbW };
}

const smallBtn: React.CSSProperties = {
  padding: "3px 10px", fontSize: 12, background: "#eee", color: "#333",
  border: "1px solid #ccc", borderRadius: 4, cursor: "pointer",
};

export default function OutletHub() {
  const { id: layoutId, outlet } = useParams<{ id: string; outlet: string }>();
  const router = useRouter();
  const [floorPlan, setFloorPlan] = useState<FloorPlanData | null>(null);
  const [bays, setBays] = useState<BayPlanogramSummary[]>([]);
  const [printOpen, setPrintOpen] = useState(false);

  useEffect(() => {
    fetch(`/api/planogram/floor-plan/${outlet}`).then((r) => r.json()).then(setFloorPlan);
    fetch(`/api/planogram/layouts/${layoutId}/outlets/${outlet}/bays`).then((r) => r.json()).then(setBays);
  }, [layoutId, outlet]);

  const placedBays = floorPlan?.bays.filter((b) => b.floor_x !== null) ?? [];
  const polygonPoints = floorPlan?.vertices.map((v) => `${v.x},${v.y}`).join(" ") ?? "";
  const closed = (floorPlan?.vertices.length ?? 0) >= 3;
  const { viewBox, vbW } = floorPlan
    ? computeBounds(floorPlan.vertices, floorPlan.bays)
    : { viewBox: `0 0 ${CANVAS_W} ${CANVAS_H}`, vbW: CANVAS_W };
  const s = vbW / CANVAS_W;

  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", padding: 24, boxSizing: "border-box", overflow: "hidden" }}>
      {/* header */}
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16, flexShrink: 0 }}>
        <button onClick={() => router.push(`/planogram/${layoutId}`)} style={smallBtn}>← Layout</button>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, textTransform: "capitalize", flex: 1 }}>{outlet}</h1>
        <button onClick={() => setPrintOpen(true)} style={smallBtn}>Print…</button>
        <button onClick={() => router.push(`/planogram/${layoutId}/${outlet}/floor-plan/edit`)} style={smallBtn}>Edit floor plan</button>
      </div>
      <PrintDialog open={printOpen} onClose={() => setPrintOpen(false)} layoutId={layoutId} outlet={outlet} />

      {/* body: bay list + floor plan side by side */}
      <div style={{ display: "flex", gap: 20, alignItems: "stretch", flex: 1, minHeight: 0 }}>

        {/* bay list */}
        <div style={{ width: 200, flexShrink: 0, overflowY: "auto" }}>
          <div style={{ fontWeight: 600, fontSize: 13, color: "#888", marginBottom: 6 }}>Bays</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
            {bays.length === 0 ? (
              <div style={{ color: "#aaa", fontSize: 13 }}>No bays for {outlet}.</div>
            ) : bays.map((b) => (
              <div key={b.bay_id}
                onClick={() => router.push(`/planogram/${layoutId}/${outlet}/${b.bay_id}`)}
                style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "10px 12px", borderRadius: 5, cursor: "pointer", border: "1px solid #e8e8e8", background: "#fff" }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "#f5f5f5")}
                onMouseLeave={(e) => (e.currentTarget.style.background = "#fff")}>
                <span style={{ fontSize: 13, fontWeight: 500 }}>{b.bay_name}</span>
                <span style={{ fontSize: 11, padding: "2px 7px", borderRadius: 3, background: b.has_planogram ? "#2a7" : "#e90", color: "#fff" }}>
                  {b.has_planogram ? "planned" : "empty"}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* floor plan */}
        <div style={{ flex: 1, minWidth: 0, minHeight: 0, display: "flex", flexDirection: "column" }}>
          <div style={{ fontWeight: 600, fontSize: 13, color: "#888", marginBottom: 6, flexShrink: 0 }}>Floor Plan</div>
          <div style={{ flex: 1, minHeight: 0, position: "relative", border: "1px solid #ddd", borderRadius: 6, overflow: "hidden", background: "#f9f9f9" }}>
            {floorPlan ? (
              <svg viewBox={viewBox}
                style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}>
                {closed && <polygon points={polygonPoints} fill={`${NAVY}15`} stroke={NAVY} strokeWidth={8 * s} strokeLinejoin="round" style={{ pointerEvents: "none" }} />}
                {!closed && (floorPlan.vertices.length >= 2) && <polyline points={polygonPoints} fill="none" stroke={NAVY} strokeWidth={8 * s} strokeDasharray={`${24 * s},${16 * s}`} style={{ pointerEvents: "none" }} />}
                {placedBays.map((b) => {
                  const x = b.floor_x ?? 0;
                  const y = b.floor_y ?? 0;
                  const w = b.floor_w;
                  const h = b.floor_h;
                  return (
                    <g key={b.id} style={{ cursor: "pointer" }}
                      onClick={() => router.push(`/planogram/${layoutId}/${outlet}/${b.id}`)}>
                      <rect x={x} y={y} width={w} height={h} fill={b.color} stroke="rgba(0,0,0,0.2)" strokeWidth={6 * s} rx={12 * s} />
                      <text x={x + w / 2} y={y + h / 2} textAnchor="middle" dominantBaseline="middle"
                        fontSize={Math.min(60 * s, (w / Math.max(b.name.length, 1)) * 1.6)} fontWeight="bold" fill={labelColor(b.color)}
                        style={{ userSelect: "none", pointerEvents: "none" }}>{b.name}</text>
                    </g>
                  );
                })}
              </svg>
            ) : (
              <div style={{ width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
                <span style={{ fontSize: 12, color: "#bbb" }}>Loading…</span>
              </div>
            )}
          </div>
        </div>

      </div>
    </div>
  );
}
