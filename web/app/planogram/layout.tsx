export default function PlanogramLayout({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ height: "calc(100vh - 112px)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ background: "#1a2b5f", color: "#fff", padding: "10px 24px", fontSize: 15, fontWeight: 700, letterSpacing: "0.04em", flexShrink: 0 }}>
        Planogram
      </div>
      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
        {children}
      </div>
    </div>
  );
}
