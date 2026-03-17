"use client";
import { useState } from "react";
import {
  Box,
  Button,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import DragIndicatorIcon from "@mui/icons-material/DragIndicator";
import { GridColDef } from "@mui/x-data-grid";

export type SortDirection = "asc" | "desc";

export interface SortItem {
  id: string;
  column: string;
  direction: SortDirection;
}

let _id = 0;
export function nextSortId() {
  return `s${++_id}`;
}

interface Props {
  columns: GridColDef[];
  items: SortItem[];
  onChange: (items: SortItem[]) => void;
}

export function SortPanel({ columns, items, onChange }: Props) {
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);

  const handleDragStart = (idx: number) => setDragIndex(idx);
  const handleDragOver = (e: React.DragEvent, idx: number) => { e.preventDefault(); setOverIndex(idx); };
  const handleDrop = (idx: number) => {
    if (dragIndex === null || dragIndex === idx) { setDragIndex(null); setOverIndex(null); return; }
    const next = [...items];
    const [moved] = next.splice(dragIndex, 1);
    next.splice(idx, 0, moved);
    onChange(next);
    setDragIndex(null);
    setOverIndex(null);
  };
  const handleDragEnd = () => { setDragIndex(null); setOverIndex(null); };

  const addRow = () => {
    if (columns.length === 0) return;
    // default to first column not already in use
    const used = new Set(items.map((i) => i.column));
    const col = columns.find((c) => !used.has(c.field)) ?? columns[0];
    onChange([...items, { id: nextSortId(), column: col.field, direction: "asc" }]);
  };

  const removeRow = (id: string) => onChange(items.filter((i) => i.id !== id));

  const setField = <K extends keyof SortItem>(id: string, key: K, val: SortItem[K]) =>
    onChange(items.map((item) => (item.id === id ? { ...item, [key]: val } : item)));

  const toggleDirection = (id: string) =>
    onChange(items.map((item) => (item.id === id ? { ...item, direction: item.direction === "asc" ? "desc" : "asc" } : item)));

  return (
    <Box sx={{ mb: 2 }}>
      {items.length > 0 && (
        <Stack spacing={1} sx={{ mb: 1 }}>
          {items.map((item, idx) => (
            <Stack
              key={item.id}
              direction="row"
              spacing={1}
              alignItems="center"
              draggable
              onDragStart={() => handleDragStart(idx)}
              onDragOver={(e) => handleDragOver(e, idx)}
              onDrop={() => handleDrop(idx)}
              onDragEnd={handleDragEnd}
              sx={{ opacity: dragIndex === idx ? 0.4 : 1, outline: overIndex === idx && dragIndex !== idx ? "2px solid" : "none", outlineColor: "primary.main", borderRadius: 1 }}
            >
              <DragIndicatorIcon sx={{ cursor: "grab", color: "text.disabled", flexShrink: 0 }} />
              <Box sx={{ width: 96, color: "text.secondary", fontSize: 12, textAlign: "right", pr: 1 }}>
                {idx === 0 ? "Sort by" : "then by"}
              </Box>
              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Column</InputLabel>
                <Select
                  value={item.column}
                  label="Column"
                  onChange={(e) => setField(item.id, "column", e.target.value)}
                >
                  {columns.map((c) => (
                    <MenuItem key={c.field} value={c.field}>
                      {c.headerName ?? c.field}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <IconButton size="small" onClick={() => toggleDirection(item.id)} title={item.direction}>
                {item.direction === "asc" ? <ArrowUpwardIcon fontSize="small" /> : <ArrowDownwardIcon fontSize="small" />}
              </IconButton>
              <Box sx={{ fontSize: 12, color: "text.secondary", width: 32 }}>
                {item.direction === "asc" ? "asc" : "desc"}
              </Box>
              <IconButton size="small" onClick={() => removeRow(item.id)}>
                <DeleteIcon fontSize="small" />
              </IconButton>
            </Stack>
          ))}
        </Stack>
      )}
      <Button size="small" startIcon={<AddIcon />} onClick={addRow} disabled={columns.length === 0}>
        Add sort
      </Button>
    </Box>
  );
}

export function applySort(
  rows: Record<string, unknown>[],
  items: SortItem[],
): Record<string, unknown>[] {
  if (items.length === 0) return rows;
  return [...rows].sort((a, b) => {
    for (const item of items) {
      const aVal = a[item.column];
      const bVal = b[item.column];
      let cmp = 0;
      if (aVal == null && bVal == null) cmp = 0;
      else if (aVal == null) cmp = -1;
      else if (bVal == null) cmp = 1;
      else if (typeof aVal === "number" && typeof bVal === "number") cmp = aVal - bVal;
      else cmp = String(aVal).localeCompare(String(bVal));
      if (cmp !== 0) return item.direction === "asc" ? cmp : -cmp;
    }
    return 0;
  });
}
