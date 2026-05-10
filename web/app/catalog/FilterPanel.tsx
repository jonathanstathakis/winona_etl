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

/** Column value type used to select the appropriate operator set. */
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

/** Union of all valid string-column filter operator values. */
type StringOp = (typeof STRING_OPS)[number]["value"];
/** Union of all valid number-column filter operator values. */
type NumberOp = (typeof NUMBER_OPS)[number]["value"];
/** Union of all filter operators supported across string and number columns. */
export type FilterOp = StringOp | NumberOp;
/** Boolean combinator applied between consecutive filter rows. */
export type LogicOperator = "and" | "or";

/** Represents a single filter rule in the filter panel. */
export interface FilterItem {
  /** Stable unique identifier used as a React key and for targeted updates. */
  id: string;
  /** Field name of the column being filtered. */
  column: string;
  operator: FilterOp;
  /** Raw filter value entered by the user; ignored for no-value operators such as `is_empty`. */
  value: string;
  /** How this row is combined with the preceding filter row. */
  logic: LogicOperator;
}

/** Operators that do not require a user-supplied value field. */
const NO_VALUE_OPS = new Set<FilterOp>(["is_empty", "is_not_empty"]);

/**
 * Returns the default filter operator for a given column type.
 * @param colType - The column's value type.
 * @returns The default operator string for that type.
 */
function defaultOp(colType: ColType): FilterOp {
  return colType === "number" ? "eq" : "contains";
}

/**
 * Returns the operator list appropriate for a given column type.
 * @param colType - The column's value type.
 * @returns The operator descriptor array for that type.
 */
function opsForType(colType: ColType) {
  return colType === "number" ? NUMBER_OPS : STRING_OPS;
}

let _id = 0;
/** Returns a new unique filter row ID using a module-level counter. */
export function nextFilterId() {
  return String(++_id);
}

/** Props for the FilterPanel component. */
interface Props {
  columns: GridColDef[];
  items: FilterItem[];
  onChange: (items: FilterItem[]) => void;
}

/** Interactive panel for building a list of column filter rules with drag-to-reorder support. */
export function FilterPanel({ columns, items, onChange }: Props) {
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);

  const update = (nextItems: FilterItem[]) => onChange(nextItems);

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
    update([...items, { id: nextFilterId(), column: col.field, operator: defaultOp(colType), value: "", logic: "and" }]);
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
                    value={item.logic}
                    exclusive
                    onChange={(_: unknown, val: LogicOperator | null) => { if (val) setField(item.id, "logic", val); }}
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

/**
 * Filters a row array by applying each FilterItem rule, respecting AND/OR logic between rows.
 * @param rows - The full dataset to filter.
 * @param items - Ordered list of filter rules to apply.
 * @returns The subset of rows that satisfy the combined filter expression.
 */
export function applyColumnFilters(
  rows: Record<string, unknown>[],
  items: FilterItem[],
): Record<string, unknown>[] {
  if (items.length === 0) return rows;
  return rows.filter((row) => {
    let result = matchesFilter(row, items[0]);
    for (let i = 1; i < items.length; i++) {
      const match = matchesFilter(row, items[i]);
      result = items[i].logic === "and" ? result && match : result || match;
    }
    return result;
  });
}

/**
 * Tests whether a single row satisfies a single filter rule.
 * @param row - The data row to test.
 * @param item - The filter rule to evaluate against the row.
 * @returns `true` if the row's column value matches the rule.
 */
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
