"use client";
import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";

type Outlet = "rozelle" | "avalon" | "manly";
type PlanogramMeta = {
  id: string;
  outlet: Outlet;
  name: string;
  status: "draft" | "active" | "archived";
  created_at: string;
};

const OUTLETS: Outlet[] = ["rozelle", "avalon", "manly"];
// TODO: finish planogram menu
const statusColor = (s: string) =>
  s === "active" ? "#2a7" : s === "archived" ? "#999" : "#e90";

const btnStyle: React.CSSProperties = {
  padding: "6px 14px", fontSize: 13, cursor: "pointer",
  border: "1px solid #ccc", borderRadius: 4, background: "#f5f5f5",
};

function NameDialog({
  title,
  initial,
  confirmLabel,
  onConfirm,
  onCancel,
}: {
  title: string;
  initial?: string;
  confirmLabel: string;
  onConfirm: (name: string) => void;
  onCancel: () => void;
}) {
  const [name, setName] = useState(initial ?? "");
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

function DotMenu({ onRename, onDelete }: { onRename: () => void; onDelete: () => void }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        onClick={(e) => { e.stopPropagation(); setOpen((v) => !v); }}
        style={{ background: "none", border: "none", cursor: "pointer", fontSize: 18, padding: "0 4px", color: "#888", lineHeight: 1 }}
      >
        ⋮
      </button>
      {open && (
        <div style={{
          position: "absolute", right: 0, top: "100%", zIndex: 200,
          background: "#fff", border: "1px solid #ddd", borderRadius: 4,
          boxShadow: "0 2px 8px rgba(0,0,0,0.12)", minWidth: 120, overflow: "hidden",
        }}>
          <button
            onClick={(e) => { e.stopPropagation(); setOpen(false); onRename(); }}
            style={{ display: "block", width: "100%", textAlign: "left", padding: "8px 14px", fontSize: 13, background: "none", border: "none", cursor: "pointer" }}
            onMouseEnter={(e) => (e.currentTarget.style.background = "#f5f5f5")}
            onMouseLeave={(e) => (e.currentTarget.style.background = "none")}
          >
            Rename
          </button>
          <button
            onClick={(e) => { e.stopPropagation(); setOpen(false); onDelete(); }}
            style={{ display: "block", width: "100%", textAlign: "left", padding: "8px 14px", fontSize: 13, background: "none", border: "none", cursor: "pointer", color: "#e55" }}
            onMouseEnter={(e) => (e.currentTarget.style.background = "#fff5f5")}
            onMouseLeave={(e) => (e.currentTarget.style.background = "none")}
          >
            Delete
          </button>
        </div>
      )}
    </div>
  );
}

export default function PlanogramLanding() {
  const router = useRouter();
  const [outlet, setOutlet] = useState<Outlet>("rozelle");
  const [planograms, setPlanograms] = useState<PlanogramMeta[]>([]);
  const [showNew, setShowNew] = useState(false);
  const [renaming, setRenaming] = useState<PlanogramMeta | null>(null);

  useEffect(() => {
    setPlanograms([]);
    fetch(`/api/planogram/planograms?outlet=${outlet}`)
      .then((r) => r.json())
      .then(setPlanograms);
  }, [outlet]);

  async function handleCreate(name: string) {
    const res = await fetch("/api/planogram/planograms", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ outlet, name }),
    });
    const created: PlanogramMeta = await res.json();
    router.push(`/planogram/${created.id}`);
  }

  async function handleRename(id: string, name: string) {
    const res = await fetch(`/api/planogram/planograms/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    const updated: PlanogramMeta = await res.json();
    setPlanograms((prev) => prev.map((p) => (p.id === id ? { ...p, name: updated.name } : p)));
    setRenaming(null);
  }

  async function handleDelete(id: string) {
    await fetch(`/api/planogram/planograms/${id}`, { method: "DELETE" });
    setPlanograms((prev) => prev.filter((p) => p.id !== id));
  }

  return (
    <div style={{ padding: 24, maxWidth: 640 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Planograms</h1>
        <button
          onClick={() => setShowNew(true)}
          style={{ ...btnStyle, background: "#333", color: "#fff", border: "none" }}
        >
          + New
        </button>
      </div>

      {/* outlet tabs */}
      <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
        {OUTLETS.map((o) => (
          <button
            key={o}
            onClick={() => setOutlet(o)}
            style={{
              padding: "5px 16px", fontSize: 13, textTransform: "capitalize",
              fontWeight: outlet === o ? "bold" : "normal",
              background: outlet === o ? "#333" : "#eee",
              color: outlet === o ? "#fff" : "#333",
              border: "none", borderRadius: 4, cursor: "pointer",
            }}
          >
            {o}
          </button>
        ))}
      </div>

      {/* planogram list */}
      {planograms.length === 0 ? (
        <div style={{ color: "#aaa", fontSize: 13 }}>No planograms for {outlet}.</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {planograms.map((p) => (
            <div
              key={p.id}
              onClick={() => router.push(`/planogram/${p.id}`)}
              style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "10px 14px", borderRadius: 5, cursor: "pointer",
                border: "1px solid #e8e8e8", background: "#fff",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#f5f5f5")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "#fff")}
            >
              <span style={{ fontSize: 14, fontWeight: 500 }}>{p.name}</span>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 11, color: "#aaa" }}>
                  {new Date(p.created_at).toLocaleDateString("en-AU")}
                </span>
                <span style={{
                  fontSize: 11, padding: "2px 7px", borderRadius: 3,
                  background: statusColor(p.status), color: "#fff",
                }}>
                  {p.status}
                </span>
                <DotMenu
                  onRename={() => setRenaming(p)}
                  onDelete={() => handleDelete(p.id)}
                />
              </div>
            </div>
          ))}
        </div>
      )}

      {showNew && (
        <NameDialog
          title="New planogram"
          confirmLabel="Create"
          onConfirm={handleCreate}
          onCancel={() => setShowNew(false)}
        />
      )}

      {renaming && (
        <NameDialog
          title="Rename planogram"
          initial={renaming.name}
          confirmLabel="Rename"
          onConfirm={(name) => handleRename(renaming.id, name)}
          onCancel={() => setRenaming(null)}
        />
      )}
    </div>
  );
}
