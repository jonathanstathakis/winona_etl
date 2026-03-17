"use client";
import { useEffect, useState } from "react";
import { Alert, CircularProgress, Typography } from "@mui/material";
import { DataGrid, GridColDef } from "@mui/x-data-grid";

const COLUMNS: GridColDef[] = [
  { field: "name", headerName: "Name", width: 240 },
  { field: "sku", headerName: "SKU", width: 160 },
  { field: "brand_name", headerName: "Brand", width: 180 },
  { field: "retail_price", headerName: "Retail", width: 100, type: "number" },
  { field: "supply_price", headerName: "Supply", width: 100, type: "number" },
  { field: "tags", headerName: "Tags", width: 300 },
];

interface WineItem {
  id: string;
  name: string;
  sku: string;
  brand_name: string;
  retail_price: number;
  supply_price: number;
  tags: string;
}

export default function WinePage() {
  const [rows, setRows] = useState<WineItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/mart/wine")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setRows)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <CircularProgress />;
  if (error) return <Alert severity="error">{error}</Alert>;

  return (
    <>
      <Typography variant="h5" gutterBottom>Wine</Typography>
      <DataGrid
        rows={rows}
        columns={COLUMNS}
        getRowId={(r) => r.id}
        pageSizeOptions={[25, 50, 100]}
        initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
        density="compact"
        disableRowSelectionOnClick
      />
    </>
  );
}
