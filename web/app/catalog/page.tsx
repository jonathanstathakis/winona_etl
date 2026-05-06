"use client";
import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import DownloadIcon from "@mui/icons-material/Download";
import SearchIcon from "@mui/icons-material/Search";
import SaveIcon from "@mui/icons-material/Save";
import DeleteIcon from "@mui/icons-material/Delete";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { FilterPanel, FilterItem, applyColumnFilters, nextFilterId } from "./FilterPanel";
import { SortPanel, SortItem, applySort, nextSortId } from "./SortPanel";
import { ColumnSelect, applyColumnSelection } from "./ColumnPanel";

const PRESETS_KEY = "winona_catalog_filter_presets";

interface Preset {
  name: string;
  items: FilterItem[];
  logic?: string;
  sortItems: SortItem[];
  columnFields: string[];
}

function loadPresets(): Preset[] {
  try {
    return JSON.parse(localStorage.getItem(PRESETS_KEY) ?? "[]");
  } catch {
    return [];
  }
}

function savePresets(presets: Preset[]) {
  localStorage.setItem(PRESETS_KEY, JSON.stringify(presets));
}

function buildColumns(row: Record<string, unknown>): GridColDef[] {
  return Object.keys(row).map((key) => {
    const base: GridColDef = {
      field: key,
      headerName: key.replace(/_/g, " "),
      width: 160,
      type: typeof row[key] === "number" ? ("number" as const) : ("string" as const),
    };
    if (key === "export_timestamp") {
      return {
        ...base,
        type: "string" as const,
        valueFormatter: (value: number) => value ? new Date(value * 1000).toLocaleString() : "",
      };
    }
    return base;
  });
}

export default function CatalogPage() {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [columns, setColumns] = useState<GridColDef[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // pending state (being built)
  const [pendingItems, setPendingItems] = useState<FilterItem[]>([]);
  const [pendingSortItems, setPendingSortItems] = useState<SortItem[]>([]);
  const [pendingColumnFields, setPendingColumnFields] = useState<string[]>([]);

  // applied state (active on grid)
  const [applied, setApplied] = useState<{
    items: FilterItem[];
    sortItems: SortItem[];
    columnFields: string[];
  }>({ items: [], sortItems: [], columnFields: [] });

  // presets
  const [presets, setPresets] = useState<Preset[]>([]);
  const [presetName, setPresetName] = useState("");

  useEffect(() => {
    setPresets(loadPresets());
    fetch("/api/mart/product-catalog")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => {
        setRows(data);
        if (data.length > 0) {
          const cols = buildColumns(data[0]);
          setColumns(cols);
          setPendingColumnFields(cols.map((c) => c.field));
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const visibleColumns = useMemo(
    () => applyColumnSelection(columns, pendingColumnFields),
    [columns, pendingColumnFields],
  );

  const filteredRows = useMemo(() => {
    const filtered = applyColumnFilters(rows, applied.items);
    return applySort(filtered, applied.sortItems);
  }, [rows, applied]);

  const handleSearch = () =>
    setApplied({ items: pendingItems, sortItems: pendingSortItems, columnFields: pendingColumnFields });

  const handleExportCSV = () => {
    if (filteredRows.length === 0) return;
    const fields = visibleColumns.map((c) => c.field);
    const headers = visibleColumns.map((c) => `"${c.headerName ?? c.field}"`).join(",");
    const lines = [
      headers,
      ...filteredRows.map((r) => fields.map((f) => {
        const v = r[f];
        return v === null || v === undefined ? "" : `"${String(v).replace(/"/g, '""')}"`;
      }).join(",")),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "product-catalog.csv";
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleSavePreset = () => {
    const name = presetName.trim();
    if (!name) return;
    const updated = [
      ...presets.filter((p) => p.name !== name),
      { name, items: pendingItems, sortItems: pendingSortItems, columnFields: pendingColumnFields },
    ];
    setPresets(updated);
    savePresets(updated);
    setPresetName("");
  };

  const handleLoadPreset = (name: string) => {
    const preset = presets.find((p) => p.name === name);
    if (!preset) return;
    const items = preset.items.map((item) => ({ ...item, id: nextFilterId() }));
    const sortItems = (preset.sortItems ?? []).map((item) => ({ ...item, id: nextSortId() }));
    setPendingItems(items);
    setPendingSortItems(sortItems);
    setPendingColumnFields(preset.columnFields ?? []);
    setPresetName(preset.name);
  };

  const handleDeletePreset = (name: string) => {
    const updated = presets.filter((p) => p.name !== name);
    setPresets(updated);
    savePresets(updated);
  };

  if (loading) return <CircularProgress />;
  if (error) return <Alert severity="error">{error}</Alert>;

  return (
    <>
      <Typography variant="h5" gutterBottom>Product Catalog</Typography>

      <FilterPanel
        columns={columns}
        items={pendingItems}
        onChange={(items) => setPendingItems(items)}
      />

      <SortPanel
        columns={columns}
        items={pendingSortItems}
        onChange={setPendingSortItems}
      />

      <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" sx={{ mb: 2, gap: 1 }}>
        <ColumnSelect
          columns={columns}
          selected={pendingColumnFields}
          onChange={setPendingColumnFields}
        />

        <Button variant="contained" size="small" startIcon={<SearchIcon />} onClick={handleSearch}>
          Search
        </Button>
        <Button size="small" startIcon={<DownloadIcon />} onClick={handleExportCSV} disabled={filteredRows.length === 0}>
          Export CSV
        </Button>

        <Divider orientation="vertical" flexItem />

        <TextField
          size="small"
          label="Save as..."
          value={presetName}
          onChange={(e) => setPresetName(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") handleSavePreset(); }}
          sx={{ width: 180 }}
        />
        <Button size="small" startIcon={<SaveIcon />} onClick={handleSavePreset} disabled={!presetName.trim()}>
          Save
        </Button>

        {presets.length > 0 && (
          <>
            <Divider orientation="vertical" flexItem />
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Load preset</InputLabel>
              <Select
                value=""
                label="Load preset"
                onChange={(e) => handleLoadPreset(e.target.value)}
                renderValue={() => "Load preset"}
              >
                {presets.map((p) => (
                  <MenuItem key={p.name} value={p.name} sx={{ justifyContent: "space-between" }}>
                    <span>{p.name}</span>
                    <Tooltip title="Delete preset">
                      <IconButton
                        size="small"
                        onClick={(e) => { e.stopPropagation(); handleDeletePreset(p.name); }}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </>
        )}
      </Stack>

      <Box sx={{ mb: 1 }}>
        <Typography variant="body2" color="text.secondary">
          {filteredRows.length} of {rows.length} items
        </Typography>
      </Box>

      <DataGrid
        rows={filteredRows}
        columns={visibleColumns}
        getRowId={(r) => r.id as string}
        pageSizeOptions={[25, 50, 100]}
        initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
        density="compact"
        disableRowSelectionOnClick
        disableColumnSorting
        disableColumnFilter
        ignoreDiacritics
      />
    </>
  );
}
