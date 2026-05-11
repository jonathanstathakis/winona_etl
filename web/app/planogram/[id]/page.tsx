"use client";
import { useState, useEffect } from "react";
import { useParams, useRouter } from "next/navigation";
import PrintDialog from "../components/PrintDialog";

const NAVY = "#1a2b5f";
const GRID_STEP = 40;
const CANVAS_W = 30;
const CANVAS_H = 20;
const OUTLETS = ["rozelle", "avalon", "manly"] as const;
type Outlet = (typeof OUTLETS)[number];

type Vertex = { x: number; y: number };
type FloorBay = { id: string; name: string; floor_x: number | null; floor_y: number | null; floor_w: number; floor_h: number };
type FloorPlan = { vertices: Vertex[]; bays: FloorBay[] };

type OutletSummary = { outlet: Outlet; bay_count: number; planogram_count: number };
type LayoutDetail = { id: string; name: string; is_current: boolean; created_at: string; outlets: OutletSummary[] };

const smallBtn: React.CSSProperties = {
  padding: "3px 10px", fontSize: 12, background: "#eee", color: "#333",
  border: "1px solid #ccc", borderRadius: 4, cursor: "pointer",
};

function FloorPlanThumb({ floorPlan }: { floorPlan: FloorPlan | null }) {
  if (!floorPlan) {
    return (
      <div style={{ aspectRatio: `${CANVAS_W}/${CANVAS_H}`, background: "#f5f5f5", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <span style={{ fontSize: 11, color: "#bbb" }}>Loading…</span>
      </div>
    );
  }
  const placedBays = floorPlan.bays.filter((b) => b.floor_x !== null);
  const closed = floorPlan.vertices.length >= 3;
  const polygonPoints = floorPlan.vertices.map((v) => `${v.x * GRID_STEP},${v.y * GRID_STEP}`).join(" ");
  return (
    <svg viewBox={`0 0 ${CANVAS_W * GRID_STEP} ${CANVAS_H * GRID_STEP}`}
      style={{ display: "block", width: "100%", height: "auto", background: "#f9f9f9" }}>
      {Array.from({ length: CANVAS_W + 1 }, (_, xi) =>
        Array.from({ length: CANVAS_H + 1 }, (_, yi) => (
          <circle key={`${xi}-${yi}`} cx={xi * GRID_STEP} cy={yi * GRID_STEP} r={1.2} fill="#e0e0e0" />
        ))
      )}
      {closed && <polygon points={polygonPoints} fill={`${NAVY}18`} stroke={NAVY} strokeWidth={2} strokeLinejoin="round" />}
      {!closed && floorPlan.vertices.length >= 2 && <polyline points={polygonPoints} fill="none" stroke={NAVY} strokeWidth={2} strokeDasharray="6,4" />}
      {placedBays.map((b) => {
        const x = (b.floor_x ?? 0) * GRID_STEP;
        const y = (b.floor_y ?? 0) * GRID_STEP;
        const w = b.floor_w * GRID_STEP;
        const h = b.floor_h * GRID_STEP;
        return (
          <g key={b.id}>
            <rect x={x} y={y} width={w} height={h} fill="#fff" stroke={NAVY} strokeWidth={2} rx={3} />
            <text x={x + w / 2} y={y + h / 2} textAnchor="middle" dominantBaseline="middle"
              fontSize={Math.min(14, (w / Math.max(b.name.length, 1)) * 1.6)} fontWeight="bold" fill={NAVY}
              style={{ userSelect: "none", pointerEvents: "none" }}>{b.name}</text>
          </g>
        );
      })}
    </svg>
  );
}

export default function LayoutHub() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const [layout, setLayout] = useState<LayoutDetail | null>(null);
  const [floorPlans, setFloorPlans] = useState<Record<Outlet, FloorPlan | null>>({ rozelle: null, avalon: null, manly: null });
  const [printOpen, setPrintOpen] = useState(false);

  useEffect(() => {
    fetch(`/api/planogram/layouts/${id}`).then((r) => r.json()).then(setLayout);
    OUTLETS.forEach((outlet) => {
      fetch(`/api/planogram/floor-plan/${outlet}`)
        .then((r) => r.json())
        .then((data) => setFloorPlans((prev) => ({ ...prev, [outlet]: data })));
    });
  }, [id]);

  if (!layout) return <div style={{ padding: 24, color: "#aaa" }}>Loading…</div>;

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 24 }}>
        <button onClick={() => router.push("/planogram")} style={smallBtn}>← Layouts</button>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, flex: 1 }}>{layout.name}</h1>
        {layout.is_current && (
          <span style={{ fontSize: 10, padding: "2px 7px", borderRadius: 3, background: "#2a7", color: "#fff", fontWeight: 600 }}>CURRENT</span>
        )}
        <button onClick={() => setPrintOpen(true)} style={smallBtn}>Print…</button>
      </div>
      <PrintDialog open={printOpen} onClose={() => setPrintOpen(false)} layoutId={id} />

      <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 10 }}>Outlets</div>

      <div style={{ display: "flex", gap: 12 }}>
        {OUTLETS.map((outlet) => {
          const summary = layout.outlets.find((o) => o.outlet === outlet);
          const bayCount = summary?.bay_count ?? 0;
          const planCount = summary?.planogram_count ?? 0;
          return (
            <div key={outlet}
              onClick={() => router.push(`/planogram/${id}/${outlet}`)}
              style={{ width: "calc(100vw / 6)", flexShrink: 0, aspectRatio: "1/1", borderRadius: 8, cursor: "pointer", border: "1px solid #e0e0e0", background: "#fff", overflow: "hidden", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", display: "flex", flexDirection: "column" }}
              onMouseEnter={(e) => (e.currentTarget.style.boxShadow = "0 4px 12px rgba(0,0,0,0.1)")}
              onMouseLeave={(e) => (e.currentTarget.style.boxShadow = "0 1px 3px rgba(0,0,0,0.05)")}>
              <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
                <FloorPlanThumb floorPlan={floorPlans[outlet]} />
              </div>
              <div style={{ padding: "6px 10px", borderTop: "1px solid #eee", flexShrink: 0 }}>
                <div style={{ fontSize: 13, fontWeight: 600, textTransform: "capitalize", color: NAVY }}>{outlet}</div>
                <div style={{ fontSize: 11, color: "#888", marginTop: 1 }}>
                  {bayCount} bay{bayCount !== 1 ? "s" : ""} · {planCount}/{bayCount} planograms
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
