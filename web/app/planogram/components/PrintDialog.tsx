"use client";
import React, { useState } from "react";

const NAVY = "#2E5FA3";
const OUTLETS = ["rozelle", "avalon", "manly"] as const;

const BOTTLE_PATH_D =
  "m237.11-14.802c-2.0828 1.5833-3.2235 5.4484-3.6875 12.469-0.2329 3.5228-0.9539 7.0414-1.5938 7.8125-0.7091 0.85434-1.3578 7.0229-1.6562 15.813-0.414 12.191-0.2161 14.976 1.2187 18 0.7543 1.5895 1.1648 3.5781 1.2188 13.156l-6.83 188.74c0.48973 7.3004-1.2397 13.334-4.8704 19.598-2.9314 4.7479-7.6972 10.506-16.996 21.221-37.168 39.202-55.265 89.471-58.625 142.53l0.1562 529.78c1.7751 13.738 8.7702 35.164 19.781 39.781h230c11.011-4.6175 18.006-26.043 19.781-39.781l0.1563-529.78c-3.3599-53.061-21.458-103.33-58.625-142.53-10.854-12.506-15.532-18.26-18.326-23.536-3.1276-4.949-3.2868-10.244-3.49-15.865l-6.87-190.15c0.0539-9.5782 0.4645-11.567 1.2187-13.156 1.4348-3.0236 1.6328-5.8086 1.2188-18-0.2984-8.7896-0.9472-14.958-1.6563-15.813-0.6399-0.77108-1.3609-4.2897-1.5937-7.8125-0.4641-7.0203-1.6048-10.885-3.6875-12.469-28.65-2.2076-57.603-2.4136-86.25 0zm72.031 61.112h5v180h-5v-180zm56 388v521h-6v-521h6zm21 0v521h-13v-521h13z";

type Item = { sku: string; name: string; tags?: string };
type BayLayout = (Item | null)[][];
type PdfBay = { outlet: string; bayName: string; shelves: number[]; grid: BayLayout };
type PrintScope = "bay" | "outlet" | "layout";

type Props = {
  open: boolean;
  onClose: () => void;
  layoutId: string;
  outlet?: string;
  bayId?: string;
};

function wineColor(tags?: string): string {
  const t = (tags ?? "").toLowerCase();
  if (t.includes("red wine")) return "#7B1C1C";
  if (t.includes("white wine")) return "#BDB76B";
  if (t.includes("orange wine")) return "#C17B2A";
  if (t.includes("rose wine")) return "#DB7093";
  return "#2D5A27";
}

export default function PrintDialog({ open, onClose, layoutId, outlet, bayId }: Props) {
  const availableScopes: PrintScope[] = bayId
    ? ["bay", "outlet", "layout"]
    : outlet
    ? ["outlet", "layout"]
    : ["layout"];

  const [scope, setScope] = useState<PrintScope>(availableScopes[0]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!open) return null;

  const scopeLabel: Record<PrintScope, string> = {
    bay: "This bay only",
    outlet: `All bays in ${outlet ?? "outlet"}`,
    layout: "All outlets (full layout)",
  };

  async function fetchJson(url: string) {
    const r = await fetch(url);
    if (!r.ok) {
      const text = await r.text().catch(() => r.statusText);
      throw new Error(`${url} → ${r.status}: ${text.slice(0, 200)}`);
    }
    return r.json();
  }

  async function handleDownload() {
    setLoading(true);
    setError(null);
    try {
      const [products, layoutDetail] = await Promise.all([
        fetchJson("/api/mart/wine"),
        fetchJson(`/api/planogram/layouts/${layoutId}`),
      ]);
      const productMap = new Map<string, Item>(
        (Array.isArray(products) ? products : []).map((p: Item) => [p.sku, p])
      );
      const layoutName: string = layoutDetail.name ?? "Planogram";

      const outletNames: string[] =
        scope === "layout" ? [...OUTLETS] : [outlet!];

      const pdfBays: PdfBay[] = [];

      for (const outletName of outletNames) {
        const bays: { bay_id: string; bay_name: string }[] = await fetchJson(
          `/api/planogram/layouts/${layoutId}/outlets/${outletName}/bays`
        );

        const baysToFetch =
          scope === "bay" && bayId
            ? bays.filter((b) => b.bay_id === bayId)
            : bays;

        const planograms = await Promise.all(
          baysToFetch.map((b) =>
            fetchJson(`/api/planogram/layouts/${layoutId}/bays/${b.bay_id}`)
          )
        );

        baysToFetch.forEach((bay, i) => {
          const pgram = planograms[i];
          const shelves: number[] = (pgram.shelves ?? []).map((s: { slot_count: number }) => s.slot_count);
          const grid: BayLayout = shelves.map((count, si) =>
            Array.from({ length: count }, (_, sli) => {
              const p = (pgram.placements ?? []).find(
                (pl: { shelf_index: number; slot_index: number; sku: string }) =>
                  pl.shelf_index === si && pl.slot_index === sli
              );
              return p ? (productMap.get(p.sku) ?? null) : null;
            })
          );
          pdfBays.push({ outlet: outletName, bayName: bay.bay_name, shelves, grid });
        });
      }

      const safeName = layoutName.replace(/[^a-z0-9]/gi, "_");
      const filename = `${safeName}-${scope}.pdf`;

      const { Document, Page, View, Text, StyleSheet, pdf, Svg, G, Path, Rect: PdfRect } =
        await import("@react-pdf/renderer");

      const PAD = 24;
      const HEADER_H = 60;
      const FOOTER_H = 28;
      const SLOT_GAP = 1.5;
      const SHELF_BAR = 5;
      const SHELF_MARGIN = 3;

      function bayFit(maxSlots: number, shelfCount: number) {
        const naturalW = 14 + maxSlots * (90 + SLOT_GAP);
        const naturalH = shelfCount * (220 + SHELF_BAR + SHELF_MARGIN);
        const scaleFor = (pw: number, ph: number) =>
          Math.min(1, (pw - PAD * 2) / naturalW, (ph - PAD * 2 - HEADER_H - FOOTER_H) / naturalH);
        const portrait = scaleFor(595, 842);
        const landscape = scaleFor(842, 595);
        return landscape > portrait
          ? { orientation: "landscape" as const, scale: landscape }
          : { orientation: "portrait" as const, scale: portrait };
      }

      const styles = StyleSheet.create({
        page: { padding: PAD, fontFamily: "Helvetica", flexDirection: "column" },
        header: { borderBottomWidth: 2, borderBottomColor: NAVY, borderBottomStyle: "solid", paddingBottom: 8, marginBottom: 14 },
        headerTitle: { fontSize: 18, fontWeight: "bold", color: "#111" },
        headerMeta: { fontSize: 9, color: "#555", marginTop: 3 },
        slot: { flexDirection: "column", alignItems: "center", borderWidth: 0.5, borderColor: "#ddd", borderStyle: "solid", padding: 2 },
        emptySlot: { borderColor: "#eee", justifyContent: "center", alignItems: "center" },
        footer: { position: "absolute", bottom: 20, left: PAD, right: PAD, flexDirection: "row", justifyContent: "space-between", borderTopWidth: 0.5, borderTopColor: "#ddd", borderTopStyle: "solid", paddingTop: 4 },
        footerText: { fontSize: 7.5, color: "#888" },
      });

      const d = new Date();
      const pad = (n: number) => String(n).padStart(2, "0");
      const dateStr = `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;

      function PdfWineBottle({ color, width, height }: { color: string; width: number; height: number }) {
        return (
          <Svg viewBox="0 0 66.758 250" style={{ width, height }}>
            <G transform="translate(116.44,-602.98)">
              <G transform="matrix(0.24737,0,0,0.24737,-176.37,598.08)">
                <G transform="translate(96.975,36.365)">
                  <PdfRect fill="white" x={322.86} y={403.79} width={80} height={560} />
                  <PdfRect fill="white" x={277.14} y={26.648} width={45.714} height={234.29} />
                  <Path fill={color} d={BOTTLE_PATH_D} />
                </G>
              </G>
            </G>
          </Svg>
        );
      }

      function PlanogramDoc() {
        return (
          <Document>
            {pdfBays.map((bay, bi) => {
              const maxSlots = Math.max(...bay.shelves, 1);
              const { orientation, scale } = bayFit(maxSlots, bay.shelves.length);
              const slotW = 90 * scale, slotH = 220 * scale, btlW = 40 * scale, btlH = 100 * scale;
              const gap = SLOT_GAP * scale, barH = SHELF_BAR * scale, labelW = 14 * scale;
              const fs = { name: 11 * scale, sku: 10 * scale, num: 7 * scale, empty: 11 * scale };
              const outletLabel = bay.outlet.charAt(0).toUpperCase() + bay.outlet.slice(1);

              return (
                <Page key={bi} size="A4" orientation={orientation} style={styles.page}>
                  <View style={[styles.header, { flexDirection: "row", justifyContent: "space-between", alignItems: "flex-start" }]}>
                    <View>
                      <Text style={styles.headerTitle}>{layoutName}</Text>
                      <Text style={styles.headerMeta}>{outletLabel} · {bay.bayName} · {bay.shelves.length} shelves · Exported {dateStr}</Text>
                    </View>
                    <Text style={styles.headerTitle}>Winona Wine</Text>
                  </View>

                  <View style={{ flex: 1 }}>
                    {bay.shelves.map((slotCount, si) => {
                      const slots = bay.grid[si] ?? [];
                      return (
                        <View key={si} style={{ marginBottom: SHELF_MARGIN * scale }}>
                          <View style={{ flexDirection: "row", alignItems: "flex-start" }}>
                            <Text style={{ width: labelW, fontSize: fs.num, color: "#888", textAlign: "center", paddingTop: 4 }}>{String(si + 1)}</Text>
                            {Array.from({ length: slotCount }, (_, sli) => {
                              const item = slots[sli] ?? null;
                              const color = item ? wineColor(item.tags) : "#eee";
                              return (
                                <View key={sli} style={[styles.slot, ...(!item ? [styles.emptySlot] : []), { width: slotW, height: slotH, marginRight: gap }]}>
                                  {item ? (
                                    <>
                                      <View style={{ marginTop: 4 * scale }}>
                                        <PdfWineBottle color={color} width={btlW} height={btlH} />
                                      </View>
                                      <View style={{ marginTop: 6 * scale, width: "100%", paddingLeft: 3 * scale, paddingRight: 3 * scale }}>
                                        <Text style={{ fontSize: fs.name, fontWeight: "bold", textAlign: "center", lineHeight: 1.3, color: "#111" }}>{item.name}</Text>
                                        <Text style={{ fontSize: fs.sku, color: "#666", textAlign: "center", marginTop: 3 * scale }}>{item.sku}</Text>
                                      </View>
                                    </>
                                  ) : (
                                    <Text style={{ fontSize: fs.empty, color: "#ccc", textAlign: "center", margin: "auto" }}>{String(sli + 1)}</Text>
                                  )}
                                </View>
                              );
                            })}
                          </View>
                          <View style={{ height: barH, backgroundColor: NAVY, width: labelW + slotCount * (slotW + gap) }} />
                        </View>
                      );
                    })}
                  </View>

                  <View style={styles.footer} fixed>
                    <Text style={styles.footerText}>{layoutName} · {outletLabel} · {bay.bayName}</Text>
                    <Text style={styles.footerText} render={({ pageNumber, totalPages }: { pageNumber: number; totalPages: number }) => `${pageNumber} / ${totalPages}`} />
                  </View>
                </Page>
              );
            })}
          </Document>
        );
      }

      const blob = await pdf(<PlanogramDoc />).toBlob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", zIndex: 1000, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={onClose}>
      <div style={{ background: "#fff", borderRadius: 8, padding: 24, minWidth: 320, boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}
        onClick={(e) => e.stopPropagation()}>
        <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 16 }}>Download PDF</div>

        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
          {availableScopes.map((s) => (
            <label key={s} style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontSize: 13 }}>
              <input type="radio" name="scope" value={s} checked={scope === s} onChange={() => setScope(s)} />
              {scopeLabel[s]}
            </label>
          ))}
        </div>

        {error && <div style={{ fontSize: 12, color: "#e55", marginBottom: 12 }}>{error}</div>}

        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button onClick={onClose} style={{ padding: "6px 14px", fontSize: 13, border: "1px solid #ccc", borderRadius: 4, background: "#f5f5f5", cursor: "pointer" }}>
            Cancel
          </button>
          <button onClick={handleDownload} disabled={loading}
            style={{ padding: "6px 14px", fontSize: 13, border: "none", borderRadius: 4, background: NAVY, color: "#fff", cursor: loading ? "default" : "pointer", opacity: loading ? 0.7 : 1 }}>
            {loading ? "Generating…" : "Download PDF"}
          </button>
        </div>
      </div>
    </div>
  );
}
