"use client";
import { useState } from "react";
import { Alert, Box, Button, Card, CardActionArea, CardContent, CircularProgress, Divider, Stack, Typography } from "@mui/material";
import InventoryIcon from "@mui/icons-material/Inventory";
import ReceiptLongIcon from "@mui/icons-material/ReceiptLong";
import SyncIcon from "@mui/icons-material/Sync";
import Link from "next/link";

const UPLOADS = [
  {
    href: "/upload/product_catalog",
    icon: <InventoryIcon sx={{ fontSize: 40 }} />,
    label: "Product Catalog",
    description: "Upload a Lightspeed product export CSV to refresh the catalog.",
  },
  {
    href: "/upload/sales_history",
    icon: <ReceiptLongIcon sx={{ fontSize: 40 }} />,
    label: "Sales History",
    description: "Upload a Lightspeed sales history CSV for a specific outlet.",
  },
];

export default function UploadIndexPage() {
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null);

  const handleRunDbt = async () => {
    setRunning(true);
    setResult(null);
    try {
      const r = await fetch("/api/loader/run-dbt", { method: "POST" });
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        setResult({ ok: false, message: body.detail ?? `HTTP ${r.status}` });
      } else {
        setResult({ ok: true, message: "Pipeline ran successfully." });
      }
    } catch (e) {
      setResult({ ok: false, message: String(e) });
    } finally {
      setRunning(false);
    }
  };

  return (
    <>
      <Typography variant="h5" gutterBottom>Upload</Typography>
      <Stack direction="row" spacing={2} sx={{ mt: 2 }}>
        {UPLOADS.map((u) => (
          <Card key={u.href} variant="outlined" sx={{ width: 220 }}>
            <CardActionArea component={Link} href={u.href} sx={{ height: "100%" }}>
              <CardContent>
                <Box sx={{ mb: 1, color: "primary.main" }}>{u.icon}</Box>
                <Typography variant="subtitle1" fontWeight={600} gutterBottom>{u.label}</Typography>
                <Typography variant="body2" color="text.secondary">{u.description}</Typography>
              </CardContent>
            </CardActionArea>
          </Card>
        ))}
      </Stack>

      <Divider sx={{ my: 3 }} />

      <Typography variant="subtitle1" fontWeight={600} gutterBottom>Run pipeline</Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Re-run the dbt transformation pipeline against existing data — use this to reflect model changes without uploading new files.
      </Typography>
      <Stack direction="row" spacing={2} alignItems="center">
        <Button
          variant="outlined"
          startIcon={running ? <CircularProgress size={16} /> : <SyncIcon />}
          onClick={handleRunDbt}
          disabled={running}
        >
          {running ? "Running…" : "Run pipeline"}
        </Button>
        {result && (
          <Alert severity={result.ok ? "success" : "error"} sx={{ py: 0 }}>
            {result.message}
          </Alert>
        )}
      </Stack>
    </>
  );
}
