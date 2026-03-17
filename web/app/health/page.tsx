"use client";
import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";

interface ProductExport {
  export_timestamp: number;
}

interface SaleHistoryOutlet {
  outlet: string;
  earliest_sale: string;
  latest_sale: string;
}

interface HealthData {
  product_exports: ProductExport[];
  sale_history_by_outlet: SaleHistoryOutlet[];
}

function fmtTs(unix: number): string {
  return new Date(unix * 1000).toLocaleString();
}

function fmtDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

export default function HealthPage() {
  const [data, setData] = useState<HealthData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/mart/data-health")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <CircularProgress />;
  if (error) return <Alert severity="error">{error}</Alert>;
  if (!data) return null;

  const [latest, ...older] = data.product_exports;

  return (
    <Stack spacing={3}>
      <Typography variant="h5">Data Health</Typography>

      {/* Product exports */}
      <Paper variant="outlined" sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>Product Catalog Exports</Typography>

        <Box sx={{ mb: 2 }}>
          <Typography variant="body2" color="text.secondary" gutterBottom>Most recent</Typography>
          {latest ? (
            <Chip label={fmtTs(latest.export_timestamp)} color="success" />
          ) : (
            <Typography variant="body2">No exports found</Typography>
          )}
        </Box>

        <Typography variant="body2" color="text.secondary" gutterBottom>
          All exports ({data.product_exports.length})
        </Typography>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>#</TableCell>
              <TableCell>Export timestamp</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.product_exports.map((row, i) => (
              <TableRow key={row.export_timestamp} selected={i === 0}>
                <TableCell>{data.product_exports.length - i}</TableCell>
                <TableCell>{fmtTs(row.export_timestamp)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Paper>

      {/* Sale history by outlet */}
      <Paper variant="outlined" sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>Sale History Coverage</Typography>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Outlet</TableCell>
              <TableCell>Earliest sale</TableCell>
              <TableCell>Most recent sale</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.sale_history_by_outlet.length === 0 ? (
              <TableRow>
                <TableCell colSpan={3}>
                  <Typography variant="body2" color="text.secondary">No sale history found</Typography>
                </TableCell>
              </TableRow>
            ) : (
              data.sale_history_by_outlet.map((row) => (
                <TableRow key={row.outlet}>
                  <TableCell sx={{ textTransform: "capitalize" }}>{row.outlet}</TableCell>
                  <TableCell>{fmtDate(row.earliest_sale)}</TableCell>
                  <TableCell>{fmtDate(row.latest_sale)}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Paper>
    </Stack>
  );
}
