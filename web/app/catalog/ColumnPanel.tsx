"use client";
import {
  Checkbox,
  Divider,
  FormControl,
  InputLabel,
  ListItemText,
  MenuItem,
  Select,
} from "@mui/material";
import { GridColDef } from "@mui/x-data-grid";

interface Props {
  columns: GridColDef[];
  selected: string[];
  onChange: (fields: string[]) => void;
}

export function ColumnSelect({ columns, selected, onChange }: Props) {
  const allSelected = selected.length === columns.length && columns.length > 0;

  const handleToggleAll = () => {
    onChange(allSelected ? [] : columns.map((c) => c.field));
  };

  return (
    <FormControl size="small" sx={{ minWidth: 220 }}>
      <InputLabel>Columns</InputLabel>
      <Select
        multiple
        value={selected}
        label="Columns"
        onChange={(e) => {
          const vals = e.target.value as string[];
          if (vals.includes("__toggle_all__")) {
            onChange(allSelected ? [] : columns.map((c) => c.field));
          } else {
            onChange(vals);
          }
        }}
        MenuProps={{ PaperProps: { sx: { maxHeight: 400 } } }}
        renderValue={(vals) =>
          vals.length === 0 ? "All columns" : `${vals.length} column${vals.length === 1 ? "" : "s"}`
        }
      >
        <MenuItem
          value="__toggle_all__"
          dense
          sx={{ position: "sticky", top: 0, zIndex: 1, backgroundColor: "#fff !important" }}
        >
          <Checkbox checked={allSelected} size="small" />
          <ListItemText primary="Show / hide all" />
        </MenuItem>
        <Divider />
        {columns.map((col) => (
          <MenuItem key={col.field} value={col.field} dense sx={{ pl: 3 }}>
            <Checkbox checked={selected.includes(col.field)} size="small" />
            <ListItemText primary={col.headerName ?? col.field} />
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
}

export function applyColumnSelection(columns: GridColDef[], selected: string[]): GridColDef[] {
  return selected
    .map((field) => columns.find((c) => c.field === field))
    .filter((c): c is GridColDef => c != null);
}
