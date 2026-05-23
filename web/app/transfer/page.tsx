"use client";
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AgGridReact } from "ag-grid-react";
import { AllCommunityModule, ModuleRegistry, type ColDef } from "ag-grid-community";
import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-quartz.css";
import {
  Alert,
  Autocomplete,
  Snackbar,
  Box,
  Button,
  Checkbox,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  ListItemText,
  MenuItem,
  Select,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import SaveIcon from "@mui/icons-material/Save";
import DeleteIcon from "@mui/icons-material/Delete";
import DownloadIcon from "@mui/icons-material/Download";
import FolderOpenIcon from "@mui/icons-material/FolderOpen";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";

ModuleRegistry.registerModules([AllCommunityModule]);

const TRANSFERS_KEY = "winona_transfers";
const VENUES = ["Avalon", "Manly", "Rozelle", "Warehouse"];
const OPS = [
  { value: "any", label: "Any" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "eq", label: "=" },
];

/**
 * Returns true if `val` satisfies the comparison `op` against `threshold`.
 * @param val - The numeric value to test.
 * @param op - Comparison operator key: "any", "gt", "gte", "lt", "lte", or "eq".
 * @param threshold - The right-hand side of the comparison.
 * @returns True when the condition holds, or unconditionally when op is "any".
 */
function applyOp(val: number, op: string, threshold: number): boolean {
  if (op === "any") return true;
  if (op === "gt") return val > threshold;
  if (op === "gte") return val >= threshold;
  if (op === "lt") return val < threshold;
  if (op === "lte") return val <= threshold;
  if (op === "eq") return val === threshold;
  return true;
}

/**
 * Finds the first key in `row` whose name starts with `prefix` and contains `venue` (case-insensitive).
 * @param row - A single product catalog row used as a key-schema reference.
 * @param prefix - Column name prefix to match (e.g. "inventory", "restock_level").
 * @param venue - Venue name fragment to match within the column name.
 * @returns The matching column key, or null if none is found.
 */
function detectCol(row: Record<string, unknown>, prefix: string, venue: string): string | null {
  return (
    Object.keys(row).find(
      (k) => k.toLowerCase().startsWith(prefix) && k.toLowerCase().includes(venue.toLowerCase()),
    ) ?? null
  );
}

/**
 * Returns the inventory stock column name for the given venue by delegating to `detectCol` with the "inventory" prefix.
 * @param row - A single product catalog row used as a key-schema reference.
 * @param venue - Venue name to search for in the column key.
 * @returns The matching inventory column key, or null if none is found.
 */
function detectStockCol(row: Record<string, unknown>, venue: string): string | null {
  return detectCol(row, "inventory", venue);
}

/** Comparison filter applied to a venue's stock level when bulk-matching products. */
interface StockFilter {
  /** Comparison operator key passed to `applyOp` (e.g. "gt", "lte", "any"). */
  op: string;
  /** Numeric threshold for the comparison. */
  val: number;
}

/** A single row in the transfer order AG Grid, derived from the product catalog with an added transfer quantity. */
interface TransferRow extends Record<string, unknown> {
  id: string;
  /** Lightspeed product handle used in CSV exports. */
  handle: string;
  sku: string;
  name: string;
  product_category: string;
  /** Units to transfer; the only editable field in the grid. */
  quantity: number;
}

/** A persisted transfer order stored in localStorage. */
interface SavedTransfer {
  /** UUID identifying the transfer. */
  id: string;
  name: string;
  /** ISO 8601 timestamp of the last save. */
  savedAt: string;
  sourceVenue: string;
  destVenue: string;
  rows: TransferRow[];
}

/** Reads all saved transfers from localStorage, returning an empty array on parse failure. */
function loadTransfers(): SavedTransfer[] {
  try {
    return JSON.parse(localStorage.getItem(TRANSFERS_KEY) ?? "[]");
  } catch {
    return [];
  }
}

/** Serialises and writes the full transfers list to localStorage. */
function persistTransfers(transfers: SavedTransfer[]) {
  localStorage.setItem(TRANSFERS_KEY, JSON.stringify(transfers));
}

const GRID_DEFAULT_COL_DEF = { resizable: true, sortable: true, filter: true };
const getRowId = (p: { data: TransferRow }) => String(p.data.id);

// TODO: AG Grid resets column order when editing the quantity cell. Multiple approaches
// tried (onGridColumnsChanged + applyColumnState, React.memo isolation, stable prop refs)
// but the reset persists. Investigate AG Grid cell editing lifecycle / columnDefs reprocessing.
/**
 * Memoised AG Grid wrapper for the transfer order table.
 *
 * Callbacks that mutate parent state are passed in via stable refs so that the
 * grid does not re-mount on every parent render.  The `navigateRef` callback
 * scrolls to a row and opens its quantity cell for editing; `deleteRowRef`
 * removes a row from the parent state; `getRowsRef` reads the current rows
 * directly from the AG Grid API, bypassing React state, to guarantee an
 * up-to-date snapshot when saving or exporting.
 */
const TransferGrid = memo(function TransferGrid({
  rowData,
  colDefs,
  getRowsRef,
  navigateRef,
  deleteRowRef,
}: {
  rowData: TransferRow[];
  colDefs: ColDef[];
  getRowsRef: React.MutableRefObject<() => TransferRow[]>;
  navigateRef: React.MutableRefObject<(id: string) => void>;
  deleteRowRef: React.MutableRefObject<(id: string) => void>;
}) {
  const gridRef = useRef<AgGridReact>(null);
  const highlightedIndexRef = useRef<number | null>(null);

  const actionCol: ColDef = useMemo(() => ({
    headerName: "",
    width: 44,
    minWidth: 44,
    sortable: false,
    filter: false,
    resizable: false,
    suppressMovable: true,
    cellRenderer: (params: { data: TransferRow }) => (
      <IconButton size="small" onClick={() => deleteRowRef.current(params.data.id)}
        sx={{ color: "text.disabled", "&:hover": { color: "error.main" } }}>
        <DeleteIcon fontSize="small" />
      </IconButton>
    ),
  }), []); // eslint-disable-line react-hooks/exhaustive-deps

  const getRowStyle = useCallback((params: { rowIndex: number | null }) => {
    if (params.rowIndex !== null && params.rowIndex === highlightedIndexRef.current) {
      return { backgroundColor: "#fff9c4" };
    }
    return undefined;
  }, []);

  getRowsRef.current = () => {
    const rows: TransferRow[] = [];
    gridRef.current?.api?.forEachNode((node) => rows.push(node.data as TransferRow));
    return rows;
  };
  navigateRef.current = (id: string) => {
    const api = gridRef.current?.api;
    if (!api) return;
    const node = api.getRowNode(id);
    if (!node || node.rowIndex === null) return;
    const targetIndex = node.rowIndex;
    highlightedIndexRef.current = targetIndex;
    api.redrawRows();
    api.ensureIndexVisible(targetIndex, "middle");
    api.startEditingCell({ rowIndex: targetIndex, colKey: "quantity" });
  };
  return (
    <Box className="ag-theme-quartz" sx={{ height: "calc(100vh - 340px)", minHeight: 400 }}>
      <AgGridReact
        ref={gridRef}
        rowData={rowData}
        columnDefs={[actionCol, ...colDefs]}
        getRowId={getRowId}
        defaultColDef={GRID_DEFAULT_COL_DEF}
        getRowStyle={getRowStyle}
      />
    </Box>
  );
});

/** Transfer Generator page: builds, filters, saves, and exports inventory transfer orders between Winona outlets. */
export default function TransferPage() {
  const [products, setProducts] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [venueStockCols, setVenueStockCols] = useState<Record<string, string>>({});
  const [venueRestockCols, setVenueRestockCols] = useState<Record<string, string>>({});
  const [venueReorderCols, setVenueReorderCols] = useState<Record<string, string>>({});
  const [allCategories, setAllCategories] = useState<string[]>([]);

  const [sourceVenue, setSourceVenue] = useState("");
  const [destVenue, setDestVenue] = useState("");
  const [selCategories, setSelCategories] = useState<string[]>([]);
  const [sourceFilter, setSourceFilter] = useState<StockFilter>({ op: "gt", val: 0 });
  const [destFilter, setDestFilter] = useState<StockFilter>({ op: "any", val: 0 });

  const [defaultQty, setDefaultQty] = useState(0);

  const [rowData, setRowData] = useState<TransferRow[]>([]);
  const getRowsRef = useRef<() => TransferRow[]>(() => []);
  const navigateRef = useRef<(id: string) => void>(() => {});
  const deleteRowRef = useRef<(id: string) => void>(() => {});
  deleteRowRef.current = (id: string) => setRowData((prev) => prev.filter((r) => r.id !== id));

  const [transferName, setTransferName] = useState("");
  const [savedTransfers, setSavedTransfers] = useState<SavedTransfer[]>([]);
  const [browserOpen, setBrowserOpen] = useState(false);
  const [currentTransferId, setCurrentTransferId] = useState<string | null>(null);
  const [justSaved, setJustSaved] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [exportZeroQty, setExportZeroQty] = useState(true);
  const [exportError, setExportError] = useState(false);
  const [exportSuccess, setExportSuccess] = useState<string | null>(null);
  const [bulkAddOpen, setBulkAddOpen] = useState(false);
  const [singleAddWarning, setSingleAddWarning] = useState<string | null>(null);

  useEffect(() => {
    setSavedTransfers(loadTransfers());
    fetch("/api/mart/product-catalog")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: Record<string, unknown>[]) => {
        setProducts(data);
        if (data.length > 0) {
          const cols: Record<string, string> = {};
          const restockCols: Record<string, string> = {};
          const reorderCols: Record<string, string> = {};
          for (const venue of VENUES) {
            const col = detectStockCol(data[0], venue);
            if (col) cols[venue] = col;
            const restock = detectCol(data[0], "restock_level", venue);
            if (restock) restockCols[venue] = restock;
            const reorder = detectCol(data[0], "reorder_point", venue);
            if (reorder) reorderCols[venue] = reorder;
          }
          setVenueStockCols(cols);
          setVenueRestockCols(restockCols);
          setVenueReorderCols(reorderCols);
          const cats = [
            ...new Set(data.map((r) => r.product_category as string).filter(Boolean)),
          ].sort();
          setAllCategories(cats);
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  /** Reads all current rows directly from the AG Grid API via the stable ref. */
  const getGridRows = () => getRowsRef.current();

  /**
   * Products that pass the current bulk-add filters: category selection, source
   * stock filter, and destination stock filter.
   */
  const bulkMatchProducts = useMemo(() => {
    const srcCol = venueStockCols[sourceVenue];
    const dstCol = venueStockCols[destVenue];
    return products.filter((p) => {
      if (selCategories.length > 0 && !selCategories.includes(p.product_category as string))
        return false;
      if (srcCol && !applyOp((p[srcCol] as number) ?? 0, sourceFilter.op, sourceFilter.val))
        return false;
      if (dstCol && !applyOp((p[dstCol] as number) ?? 0, destFilter.op, destFilter.val))
        return false;
      return true;
    });
  }, [products, venueStockCols, sourceVenue, destVenue, selCategories, sourceFilter, destFilter]);

  const bulkDuplicateCount = useMemo(
    () => bulkMatchProducts.filter((p) => rowData.some((r) => r.id === String(p.id))).length,
    [bulkMatchProducts, rowData],
  );
  const bulkNewCount = bulkMatchProducts.length - bulkDuplicateCount;

  /** Appends all bulk-matched products not already in the transfer list, each with the current default quantity. */
  const handleBulkAdd = () => {
    const existingIds = new Set(rowData.map((r) => r.id));
    const toAdd = bulkMatchProducts
      .filter((p) => !existingIds.has(String(p.id)))
      .map((p) => ({ ...p, quantity: defaultQty } as TransferRow));
    setRowData((prev) => [...prev, ...toAdd]);
    setBulkAddOpen(false);
  };

  /**
   * Adds a single product to the transfer list, or navigates to and highlights its
   * existing row (and shows a warning snackbar) if it is already present.
   */
  const handleAddSingle = (product: Record<string, unknown> | null) => {
    if (!product) return;
    const id = String(product.id);
    if (rowData.some((r) => r.id === id)) {
      navigateRef.current(id);
      setSingleAddWarning(`"${product.name}" is already in the transfer`);
      return;
    }
    setRowData((prev) => [...prev, { ...product, quantity: defaultQty } as TransferRow]);
  };

  /**
   * Generates and triggers a browser download of a CSV file containing the transfer
   * rows (handle, sku, quantity).  Rows with zero quantity are included or excluded
   * based on the `exportZeroQty` option.  Shows an error snackbar if no rows remain
   * after filtering.
   */
  const handleExportCSV = () => {
    setExportOpen(false);
    const rows = exportZeroQty ? getGridRows() : getGridRows().filter((r) => (r.quantity ?? 0) > 0);
    if (rows.length === 0) { setExportError(true); return; }
    const lines = [
      "handle,sku,quantity",
      ...rows.map((r) => `"${r.handle}","${r.sku}",${r.quantity}`),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const filename = `${transferName || "transfer"}.csv`;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    setExportSuccess(filename);
  };

  /**
   * Upserts the current transfer into the saved-transfers list and persists it to localStorage.
   * @param name - Display name for the transfer.
   * @param id - Existing transfer UUID to overwrite; omit to create a new transfer.
   */
  const handleSave = (name: string, id?: string) => {
    const trimmed = name.trim();
    if (!trimmed) return;
    const transferId = id ?? crypto.randomUUID();
    const transfer: SavedTransfer = {
      id: transferId,
      name: trimmed,
      savedAt: new Date().toISOString(),
      sourceVenue,
      destVenue,
      rows: getGridRows(),
    };
    const updated = [...savedTransfers.filter((t) => t.id !== transferId), transfer];
    setSavedTransfers(updated);
    persistTransfers(updated);
    setCurrentTransferId(transferId);
    setTransferName(trimmed);
    setJustSaved(true);
  };

  /** Restores a previously saved transfer into the active editor and closes the browser dialog. */
  const handleLoadTransfer = (transfer: SavedTransfer) => {
    setRowData(transfer.rows);
    setSourceVenue(transfer.sourceVenue);
    setDestVenue(transfer.destVenue);
    setTransferName(transfer.name);
    setCurrentTransferId(transfer.id);
    setBrowserOpen(false);
  };

  /** Removes a saved transfer by ID from state and localStorage. */
  const handleDeleteTransfer = (id: string) => {
    const updated = savedTransfers.filter((t) => t.id !== id);
    setSavedTransfers(updated);
    persistTransfers(updated);
  };

  /**
   * AG Grid column definitions derived from the currently selected source and
   * destination venues.  Venue-specific stock, restock level, and reorder point
   * columns are included only when the corresponding catalog column is detected.
   */
  const colDefs = useMemo((): ColDef[] => {
    const srcCol = venueStockCols[sourceVenue];
    const dstCol = venueStockCols[destVenue];
    const dstRestock = venueRestockCols[destVenue];
    const dstReorder = venueReorderCols[destVenue];
    return [
      { field: "name", headerName: "Name", flex: 2, minWidth: 200 },
      { field: "sku", headerName: "SKU", width: 140 },
      { field: "handle", headerName: "Handle", width: 160 },
      { field: "product_category", headerName: "Category", width: 140 },
      ...(srcCol
        ? [{ field: srcCol, headerName: `${sourceVenue} Stock`, width: 130, type: "numericColumn" as const }]
        : []),
      ...(dstCol
        ? [{ field: dstCol, headerName: `${destVenue} Stock`, width: 130, type: "numericColumn" as const }]
        : []),
      { field: "inv_total", headerName: "Total Stock", width: 120, type: "numericColumn" as const },
      ...(dstRestock
        ? [{ field: dstRestock, headerName: `${destVenue} Restock`, width: 130, type: "numericColumn" as const }]
        : []),
      ...(dstReorder
        ? [{ field: dstReorder, headerName: `${destVenue} Reorder Pt`, width: 130, type: "numericColumn" as const }]
        : []),
      {
        field: "quantity",
        headerName: "Qty",
        width: 90,
        editable: true,
        type: "numericColumn" as const,
        cellEditor: "agNumberCellEditor",
        cellEditorParams: { min: 0 },
        cellStyle: { backgroundColor: "#f0f7ff" },
      },
    ];
  }, [venueStockCols, venueRestockCols, venueReorderCols, sourceVenue, destVenue]);

  if (loading) return <CircularProgress />;
  if (error) return <Alert severity="error">{error}</Alert>;

  const hasVenueCols = !!sourceVenue && !!destVenue && !!venueStockCols[sourceVenue] && !!venueStockCols[destVenue];

  return (
    <>
      {/* TODO: add a 'create new' button that clears the current transfer (rowData, transferName, currentTransferId) to start fresh */}
      <Typography variant="h5" gutterBottom>Transfer Generator</Typography>

      {/* Row 1: venue selectors + transfer management */}
      <Stack direction="row" spacing={1} flexWrap="wrap" sx={{ mb: 1.5, gap: 1 }} alignItems="center">
        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Source</InputLabel>
          <Select value={sourceVenue} label="Source" onChange={(e) => setSourceVenue(e.target.value)}>
            {VENUES.map((v) => <MenuItem key={v} value={v} disabled={v === destVenue}>{v}</MenuItem>)}
          </Select>
        </FormControl>
        <Typography variant="body2" color="text.secondary">→</Typography>
        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Destination</InputLabel>
          <Select value={destVenue} label="Destination" onChange={(e) => setDestVenue(e.target.value)}>
            {VENUES.map((v) => <MenuItem key={v} value={v} disabled={v === sourceVenue}>{v}</MenuItem>)}
          </Select>
        </FormControl>

        <Divider orientation="vertical" flexItem />

        <TextField
          size="small" label="Transfer name" value={transferName}
          onChange={(e) => { setTransferName(e.target.value); setJustSaved(false); }}
          onKeyDown={(e) => { if (e.key === "Enter" && transferName.trim()) handleSave(transferName, currentTransferId ?? undefined); }}
          sx={{ width: 200 }}
        />
        {/* TODO: "Saved!" status does not reset when grid values (e.g. quantity) are modified.
             Need to hook into AG Grid's onCellValueChanged to call setJustSaved(false). */}
        {currentTransferId && (
          <Button
            size="small" startIcon={<SaveIcon />}
            onClick={() => handleSave(transferName, currentTransferId)}
            disabled={!transferName.trim()}
            color={justSaved ? "success" : "primary"}
          >
            {justSaved ? "Saved!" : "Save"}
          </Button>
        )}
        <Button
          size="small"
          variant={currentTransferId ? "outlined" : "contained"}
          startIcon={<SaveIcon />}
          onClick={() => handleSave(transferName)}
          disabled={!transferName.trim()}
          color={justSaved ? "success" : "primary"}
        >
          {justSaved ? "Saved!" : currentTransferId ? "Save as new" : "Save"}
        </Button>
        {savedTransfers.length > 0 && (
          <Button size="small" startIcon={<FolderOpenIcon />} onClick={() => setBrowserOpen(true)}>
            Open saved
          </Button>
        )}
      </Stack>

      {/* Row 2: product actions */}
      <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }} flexWrap="wrap">
        <Autocomplete
          options={products}
          getOptionLabel={(o) => `${o.name as string} (${o.sku as string})`}
          filterOptions={(options, { inputValue }) => {
            const q = inputValue.toLowerCase();
            if (!q) return [];
            return options.filter(
              (o) =>
                (o.name as string)?.toLowerCase().includes(q) ||
                (o.sku as string)?.toLowerCase().includes(q) ||
                (o.handle as string)?.toLowerCase().includes(q),
            ).slice(0, 25);
          }}
          onChange={(_, value) => handleAddSingle(value)}
          renderInput={(params) => <TextField {...params} size="small" placeholder="Add product…" />}
          sx={{ width: 260 }}
          clearOnEscape blurOnSelect value={null}
        />
        <Button
          size="small" variant="outlined" startIcon={<PlayArrowIcon />}
          onClick={() => setBulkAddOpen(true)} disabled={!hasVenueCols}
        >
          Bulk add
        </Button>

        <Divider orientation="vertical" flexItem />

        <Autocomplete
          options={rowData}
          getOptionLabel={(o) => `${o.name} (${o.sku})`}
          filterOptions={(options, { inputValue }) => {
            const q = inputValue.toLowerCase();
            if (!q) return [];
            return options.filter(
              (o) =>
                o.name?.toLowerCase().includes(q) ||
                o.sku?.toLowerCase().includes(q) ||
                o.handle?.toLowerCase().includes(q),
            ).slice(0, 25);
          }}
          onChange={(_, value) => { if (value) navigateRef.current(String(value.id)); }}
          renderInput={(params) => <TextField {...params} size="small" placeholder="Find in list…" />}
          sx={{ width: 240 }}
          clearOnEscape blurOnSelect value={null}
        />

        <Divider orientation="vertical" flexItem />

        <Button size="small" startIcon={<DownloadIcon />} onClick={() => setExportOpen(true)} disabled={rowData.length === 0}>
          Export CSV
        </Button>
        <Typography variant="body2" color="text.secondary">{rowData.length} items</Typography>
      </Stack>

      {/* Grid */}
      {rowData.length === 0 ? (
        <Box sx={{
          display: "flex", alignItems: "center", justifyContent: "center",
          height: 200, border: "1px dashed", borderColor: "divider", borderRadius: 1,
        }}>
          <Typography color="text.secondary">
            No products added. Use "Add product" or "Bulk add" to get started.
          </Typography>
        </Box>
      ) : (
        <TransferGrid rowData={rowData} colDefs={colDefs} getRowsRef={getRowsRef} navigateRef={navigateRef} deleteRowRef={deleteRowRef} />
      )}

      {/* Export options dialog */}
      <Dialog open={exportOpen} onClose={() => setExportOpen(false)} maxWidth="xs" fullWidth>
        <DialogTitle>Export CSV options</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            <Stack direction="row" alignItems="center" justifyContent="space-between">
              <Box>
                <Typography variant="body2" fontWeight={500}>Include zero quantity rows</Typography>
                <Typography variant="caption" color="text.secondary">Export all rows, including those with qty = 0</Typography>
              </Box>
              <Checkbox
                checked={exportZeroQty}
                onChange={(e) => setExportZeroQty(e.target.checked)}
              />
            </Stack>
            <Stack direction="row" spacing={1} justifyContent="flex-end">
              <Button size="small" onClick={() => setExportOpen(false)}>Cancel</Button>
              <Button size="small" variant="contained" startIcon={<DownloadIcon />} onClick={handleExportCSV}>
                Export
              </Button>
            </Stack>
          </Stack>
        </DialogContent>
      </Dialog>

      <Snackbar open={exportError} autoHideDuration={4000} onClose={() => setExportError(false)}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}>
        <Alert severity="warning" onClose={() => setExportError(false)}>
          No rows to export — all quantities are zero.
        </Alert>
      </Snackbar>
      <Snackbar open={!!exportSuccess} autoHideDuration={4000} onClose={() => setExportSuccess(null)}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}>
        <Alert severity="success" onClose={() => setExportSuccess(null)}>
          CSV {exportSuccess} exported!
        </Alert>
      </Snackbar>

      {/* Bulk add dialog */}
      <Dialog open={bulkAddOpen} onClose={() => setBulkAddOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Bulk add products</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            <Alert severity="warning" icon={false}>
              Duplicate products are ignored in favor of the existing product in the transfer list.
            </Alert>

            <FormControl size="small" fullWidth>
              <InputLabel>Categories</InputLabel>
              <Select
                multiple value={selCategories} label="Categories"
                onChange={(e) => setSelCategories((e.target.value as string[]).filter((v) => v !== "__toggle_all__"))}
                MenuProps={{ PaperProps: { sx: { maxHeight: 400 } } }}
                renderValue={(vals) => vals.length === 0 ? "All" : `${vals.length} selected`}
              >
                <MenuItem value="__toggle_all__" dense
                  onMouseDown={(e) => {
                    e.preventDefault();
                    setSelCategories((prev) => prev.length === allCategories.length ? [] : [...allCategories]);
                  }}
                  sx={{ position: "sticky", top: 0, zIndex: 1, backgroundColor: "#fff !important" }}
                >
                  <Checkbox checked={selCategories.length === allCategories.length && allCategories.length > 0} size="small" />
                  <ListItemText primary="Select / deselect all" />
                </MenuItem>
                <Divider />
                {allCategories.map((c) => (
                  <MenuItem key={c} value={c} dense sx={{ pl: 3 }}>
                    <Checkbox checked={selCategories.includes(c)} size="small" />
                    <ListItemText primary={c} />
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="body2" color="text.secondary" sx={{ minWidth: 110 }}>{sourceVenue || "Source"} stock</Typography>
              <FormControl size="small" sx={{ minWidth: 80 }}>
                <Select value={sourceFilter.op} onChange={(e) => setSourceFilter((f) => ({ ...f, op: e.target.value }))}>
                  {OPS.map((o) => <MenuItem key={o.value} value={o.value}>{o.label}</MenuItem>)}
                </Select>
              </FormControl>
              {sourceFilter.op !== "any" && (
                <TextField size="small" type="number" value={sourceFilter.val} sx={{ width: 80 }}
                  onChange={(e) => setSourceFilter((f) => ({ ...f, val: Number(e.target.value) }))} />
              )}
            </Stack>

            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="body2" color="text.secondary" sx={{ minWidth: 110 }}>{destVenue || "Destination"} stock</Typography>
              <FormControl size="small" sx={{ minWidth: 80 }}>
                <Select value={destFilter.op} onChange={(e) => setDestFilter((f) => ({ ...f, op: e.target.value }))}>
                  {OPS.map((o) => <MenuItem key={o.value} value={o.value}>{o.label}</MenuItem>)}
                </Select>
              </FormControl>
              {destFilter.op !== "any" && (
                <TextField size="small" type="number" value={destFilter.val} sx={{ width: 80 }}
                  onChange={(e) => setDestFilter((f) => ({ ...f, val: Number(e.target.value) }))} />
              )}
            </Stack>

            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="body2" color="text.secondary" sx={{ minWidth: 110 }}>Default qty</Typography>
              <TextField size="small" type="number" value={defaultQty} sx={{ width: 80 }}
                inputProps={{ min: 0 }}
                onChange={(e) => setDefaultQty(Number(e.target.value))} />
            </Stack>

            <Typography variant="body2" color={bulkNewCount === 0 ? "text.disabled" : "text.primary"}>
              {bulkMatchProducts.length === 0
                ? "No products match the current filters."
                : <>
                    <strong>{bulkMatchProducts.length}</strong> product{bulkMatchProducts.length !== 1 ? "s" : ""} match
                    {bulkDuplicateCount > 0 && ` — ${bulkDuplicateCount} already in transfer (will be ignored), ${bulkNewCount} will be added`}
                    {bulkDuplicateCount === 0 && ` — ${bulkNewCount} will be added`}
                  </>
              }
            </Typography>

            <Stack direction="row" spacing={1} justifyContent="flex-end">
              <Button size="small" onClick={() => setBulkAddOpen(false)}>Cancel</Button>
              <Button size="small" variant="contained" startIcon={<AddIcon />}
                onClick={handleBulkAdd} disabled={bulkNewCount === 0}>
                Add {bulkNewCount > 0 ? bulkNewCount : ""} product{bulkNewCount !== 1 ? "s" : ""}
              </Button>
            </Stack>
          </Stack>
        </DialogContent>
      </Dialog>

      <Snackbar open={!!singleAddWarning} autoHideDuration={4000} onClose={() => setSingleAddWarning(null)}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}>
        <Alert severity="info" onClose={() => setSingleAddWarning(null)}>
          {singleAddWarning}
        </Alert>
      </Snackbar>

      {/* Transfer browser */}
      <Dialog open={browserOpen} onClose={() => setBrowserOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Saved transfers</DialogTitle>
        <DialogContent>
          <Stack spacing={1} sx={{ pt: 1 }}>
            {savedTransfers.length === 0 && (
              <Typography color="text.secondary">No saved transfers.</Typography>
            )}
            {[...savedTransfers]
              .sort((a, b) => b.savedAt.localeCompare(a.savedAt))
              .map((t) => (
                <Stack key={t.id} direction="row" alignItems="center" justifyContent="space-between">
                  <Box>
                    <Typography variant="body2" fontWeight={500}>{t.name}</Typography>
                    <Typography variant="caption" color="text.secondary">
                      {t.sourceVenue} → {t.destVenue} · {t.rows.length} items · {new Date(t.savedAt).toLocaleString()}
                    </Typography>
                  </Box>
                  <Stack direction="row" spacing={0.5}>
                    <Button size="small" onClick={() => handleLoadTransfer(t)}>Open</Button>
                    <Tooltip title="Delete">
                      <IconButton size="small" onClick={() => handleDeleteTransfer(t.id)}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                </Stack>
              ))}
          </Stack>
        </DialogContent>
      </Dialog>
    </>
  );
}
