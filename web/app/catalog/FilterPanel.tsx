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
  TextField,
  ToggleButton,
  ToggleButtonGroup,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import DragIndicatorIcon from "@mui/icons-material/DragIndicator";
import { GridColDef } from "@mui/x-data-grid";

type ColType = "string" | "number";

const STRING_OPS = [
  { value: "contains", label: "contains" },
  { value: "not_contains", label: "does not contain" },
  { value: "equals", label: "equals" },
  { value: "not_equals", label: "does not equal" },
  { value: "starts_with", label: "starts with" },
  { value: "ends_with", label: "ends with" },
  { value: "is_empty", label: "is empty" },
  { value: "is_not_empty", label: "is not empty" },
] as const;

const NUMBER_OPS = [
  { value: "eq", label: "=" },
  { value: "neq", label: "≠" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "is_empty", label: "is empty" },
  { value: "is_not_empty", label: "is not empty" },
] as const;

type StringOp = (typeof STRING_OPS)[number]["value"];
type NumberOp = (typeof NUMBER_OPS)[number]["value"];
export type FilterOp = StringOp | NumberOp;
export type LogicOperator = "and" | "or";

export interface FilterItem {
  id: string;
  column: string;
  operator: FilterOp;
  value: string;
}

const NO_VALUE_OPS = new Set<FilterOp>(["is_empty", "is_not_empty"]);

function defaultOp(colType: ColType): FilterOp {
  return colType === "number" ? "eq" : "contains";
}

function opsForType(colType: ColType) {
  return colType === "number" ? NUMBER_OPS : STRING_OPS;
}

let _id = 0;
export function nextFilterId() {
  return String(++_id);
}

interface Props {
  columns: GridColDef[];
  items: FilterItem[];
  logic: LogicOperator;
  onChange: (items: FilterItem[], logic: LogicOperator) => void;
}

export function FilterPanel({ columns, items, logic, onChange }: Props) {
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);

  const update = (nextItems: FilterItem[], nextLogic = logic) => onChange(nextItems, nextLogic);

  const handleDragStart = (idx: number) => setDragIndex(idx);
  const handleDragOver = (e: React.DragEvent, idx: number) => { e.preventDefault(); setOverIndex(idx); };
  const handleDrop = (idx: number) => {
    if (dragIndex === null || dragIndex === idx) { setDragIndex(null); setOverIndex(null); return; }
    const next = [...items];
    const [moved] = next.splice(dragIndex, 1);
    next.splice(idx, 0, moved);
    update(next);
    setDragIndex(null);
    setOverIndex(null);
  };
  const handleDragEnd = () => { setDragIndex(null); setOverIndex(null); };

  const addRow = () => {
    if (columns.length === 0) return;
    const col = columns[0];
    const colType = (col.type ?? "string") as ColType;
    update([...items, { id: nextFilterId(), column: col.field, operator: defaultOp(colType), value: "" }]);
  };

  const removeRow = (id: string) => update(items.filter((i) => i.id !== id));

  const setField = <K extends keyof FilterItem>(id: string, key: K, val: FilterItem[K]) => {
    update(
      items.map((item) => {
        if (item.id !== id) return item;
        if (key === "column") {
          const col = columns.find((c) => c.field === val);
          const colType = (col?.type ?? "string") as ColType;
          return { ...item, column: val as string, operator: defaultOp(colType), value: "" };
        }
        return { ...item, [key]: val };
      }),
    );
  };

  const handleLogicChange = (_: unknown, val: LogicOperator | null) => {
    if (val) update(items, val);
  };

  return (
    <Box sx={{ mb: 2 }}>
      {items.length > 0 && (
        <Stack spacing={1} sx={{ mb: 1 }}>
          {items.map((item, idx) => {
            const col = columns.find((c) => c.field === item.column);
            const colType = (col?.type ?? "string") as ColType;
            const ops = opsForType(colType);
            const noValue = NO_VALUE_OPS.has(item.operator);
            return (
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
                {idx === 0 ? (
                  <Box sx={{ width: 96, fontSize: 12, color: "text.secondary", textAlign: "right", pr: 1 }}>
                    Where
                  </Box>
                ) : (
                  <ToggleButtonGroup
                    value={logic}
                    exclusive
                    onChange={handleLogicChange}
                    size="small"
                    sx={{ width: 96 }}
                  >
                    <ToggleButton value="and" sx={{ flex: 1 }}>AND</ToggleButton>
                    <ToggleButton value="or" sx={{ flex: 1 }}>OR</ToggleButton>
                  </ToggleButtonGroup>
                )}
                <FormControl size="small" sx={{ minWidth: 180 }}>
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
                <FormControl size="small" sx={{ minWidth: 160 }}>
                  <InputLabel>Operator</InputLabel>
                  <Select
                    value={item.operator}
                    label="Operator"
                    onChange={(e) => setField(item.id, "operator", e.target.value as FilterOp)}
                  >
                    {ops.map((op) => (
                      <MenuItem key={op.value} value={op.value}>
                        {op.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                {!noValue && (
                  <TextField
                    size="small"
                    label="Value"
                    value={item.value}
                    onChange={(e) => setField(item.id, "value", e.target.value)}
                    sx={{ minWidth: 160 }}
                    type={colType === "number" ? "number" : "text"}
                  />
                )}
                <IconButton size="small" onClick={() => removeRow(item.id)}>
                  <DeleteIcon fontSize="small" />
                </IconButton>
              </Stack>
            );
          })}
        </Stack>
      )}
      <Button size="small" startIcon={<AddIcon />} onClick={addRow} disabled={columns.length === 0}>
        Add filter
      </Button>
    </Box>
  );
}

export function applyColumnFilters(
  rows: Record<string, unknown>[],
  items: FilterItem[],
  logic: LogicOperator,
): Record<string, unknown>[] {
  if (items.length === 0) return rows;
  return rows.filter((row) => {
    const results = items.map((item) => matchesFilter(row, item));
    return logic === "and" ? results.every(Boolean) : results.some(Boolean);
  });
}

function matchesFilter(row: Record<string, unknown>, item: FilterItem): boolean {
  const raw = row[item.column];

  if (item.operator === "is_empty") return raw == null || raw === "";
  if (item.operator === "is_not_empty") return raw != null && raw !== "";
  if (raw == null) return false;

  const strVal = String(raw).toLowerCase();
  const filterVal = item.value.toLowerCase();
  const numVal = Number(raw);
  const filterNum = Number(item.value);

  switch (item.operator) {
    case "contains": return strVal.includes(filterVal);
    case "not_contains": return !strVal.includes(filterVal);
    case "equals": return strVal === filterVal;
    case "not_equals": return strVal !== filterVal;
    case "starts_with": return strVal.startsWith(filterVal);
    case "ends_with": return strVal.endsWith(filterVal);
    case "eq": return numVal === filterNum;
    case "neq": return numVal !== filterNum;
    case "gt": return numVal > filterNum;
    case "gte": return numVal >= filterNum;
    case "lt": return numVal < filterNum;
    case "lte": return numVal <= filterNum;
    default: return true;
  }
}
