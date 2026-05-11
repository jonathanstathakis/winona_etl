"use client";
import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";

type LayoutOut = {
  id: string;
  name: string;
  is_current: boolean;
  created_at: string;
};

const btnStyle: React.CSSProperties = {
  padding: "6px 14px", fontSize: 13, cursor: "pointer",
  border: "1px solid #ccc", borderRadius: 4, background: "#f5f5f5",
};

function NameDialog({
  title, initial, confirmLabel, onConfirm, onCancel,
}: {
  title: string; initial?: string; confirmLabel: string;
  onConfirm: (name: string) => void; onCancel: () => void;
}) {
  const [name, setName] = useState(initial ?? "");
  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1100 }}
      onClick={(e) => { if (e.target === e.currentTarget) onCancel(); }}>
      <div style={{ background: "#fff", borderRadius: 6, padding: 24, width: 320 }}>
        <h3 style={{ margin: "0 0 12px", fontSize: 16 }}>{title}</h3>
        <input autoFocus type="text" value={name} onChange={(e) => setName(e.target.value)} placeholder="Name"
          style={{ width: "100%", padding: "6px 8px", fontSize: 14, border: "1px solid #ccc", borderRadius: 4, boxSizing: "border-box", marginBottom: 16 }}
          onKeyDown={(e) => { if (e.key === "Enter" && name.trim()) onConfirm(name.trim()); if (e.key === "Escape") onCancel(); }} />
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button onClick={onCancel} style={btnStyle}>Cancel</button>
          <button onClick={() => name.trim() && onConfirm(name.trim())} disabled={!name.trim()}
            style={{ ...btnStyle, background: "#333", color: "#fff", border: "none" }}>{confirmLabel}</button>
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
    function handler(e: MouseEvent) { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button onClick={(e) => { e.stopPropagation(); setOpen((v) => !v); }}
        style={{ background: "none", border: "none", cursor: "pointer", fontSize: 18, padding: "0 4px", color: "#888", lineHeight: 1 }}>⋮</button>
      {open && (
        <div style={{ position: "absolute", right: 0, top: "100%", zIndex: 200, background: "#fff", border: "1px solid #ddd", borderRadius: 4, boxShadow: "0 2px 8px rgba(0,0,0,0.12)", minWidth: 120, overflow: "hidden" }}>
          <button onClick={(e) => { e.stopPropagation(); setOpen(false); onRename(); }}
            style={{ display: "block", width: "100%", textAlign: "left", padding: "8px 14px", fontSize: 13, background: "none", border: "none", cursor: "pointer" }}
            onMouseEnter={(e) => (e.currentTarget.style.background = "#f5f5f5")}
            onMouseLeave={(e) => (e.currentTarget.style.background = "none")}>Rename</button>
          <button onClick={(e) => { e.stopPropagation(); setOpen(false); onDelete(); }}
            style={{ display: "block", width: "100%", textAlign: "left", padding: "8px 14px", fontSize: 13, background: "none", border: "none", cursor: "pointer", color: "#e55" }}
            onMouseEnter={(e) => (e.currentTarget.style.background = "#fff5f5")}
            onMouseLeave={(e) => (e.currentTarget.style.background = "none")}>Delete</button>
        </div>
      )}
    </div>
  );
}

export default function PlanogramLanding() {
  const router = useRouter();
  const [layouts, setLayouts] = useState<LayoutOut[]>([]);
  const [showNew, setShowNew] = useState(false);
  const [renaming, setRenaming] = useState<LayoutOut | null>(null);

  useEffect(() => {
    fetch("/api/planogram/layouts")
      .then((r) => r.json())
      .then((data) => setLayouts(Array.isArray(data) ? data : []));
  }, []);

  async function handleCreate(name: string) {
    const res = await fetch("/api/planogram/layouts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    const created: LayoutOut = await res.json();
    setShowNew(false);
    router.push(`/planogram/${created.id}`);
  }

  async function handleRename(id: string, name: string) {
    const res = await fetch(`/api/planogram/layouts/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    const updated: LayoutOut = await res.json();
    setLayouts((prev) => prev.map((l) => (l.id === id ? updated : l)));
    setRenaming(null);
  }

  async function handleDelete(id: string) {
    await fetch(`/api/planogram/layouts/${id}`, { method: "DELETE" });
    setLayouts((prev) => prev.filter((l) => l.id !== id));
  }

  async function handleSetCurrent(id: string) {
    const res = await fetch(`/api/planogram/layouts/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ is_current: true }),
    });
    const updated: LayoutOut = await res.json();
    setLayouts((prev) => prev.map((l) => ({ ...l, is_current: l.id === updated.id })));
  }

  return (
    <div style={{ padding: 24, maxWidth: 640 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Planogram Layouts</h1>
        <button onClick={() => setShowNew(true)} style={{ ...btnStyle, background: "#333", color: "#fff", border: "none" }}>+ New layout</button>
      </div>

      {layouts.length === 0 ? (
        <div style={{ color: "#aaa", fontSize: 13 }}>No layouts yet. Create one to get started.</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {layouts.map((l) => (
            <div key={l.id}
              onClick={() => router.push(`/planogram/${l.id}`)}
              style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 14px", borderRadius: 5, cursor: "pointer", border: `1px solid ${l.is_current ? "#2a7" : "#e8e8e8"}`, background: "#fff" }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#f5f5f5")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "#fff")}>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 14, fontWeight: 500 }}>{l.name}</span>
                {l.is_current && (
                  <span style={{ fontSize: 10, padding: "2px 7px", borderRadius: 3, background: "#2a7", color: "#fff", fontWeight: 600 }}>CURRENT</span>
                )}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 11, color: "#aaa" }}>{new Date(l.created_at).toLocaleDateString("en-AU")}</span>
                {!l.is_current && (
                  <button onClick={(e) => { e.stopPropagation(); handleSetCurrent(l.id); }}
                    style={{ fontSize: 11, padding: "2px 8px", borderRadius: 3, border: "1px solid #ccc", background: "#f5f5f5", cursor: "pointer" }}>
                    Set current
                  </button>
                )}
                <DotMenu onRename={() => setRenaming(l)} onDelete={() => handleDelete(l.id)} />
              </div>
            </div>
          ))}
        </div>
      )}

      {showNew && <NameDialog title="New layout" confirmLabel="Create" onConfirm={handleCreate} onCancel={() => setShowNew(false)} />}
      {renaming && (
        <NameDialog title="Rename layout" initial={renaming.name} confirmLabel="Rename"
          onConfirm={(name) => handleRename(renaming.id, name)} onCancel={() => setRenaming(null)} />
      )}
    </div>
  );
}
