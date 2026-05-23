"use client";
import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  FormControl,
  InputLabel,
  LinearProgress,
  MenuItem,
  Select,
  Stack,
  Typography,
} from "@mui/material";
import CloudUploadIcon from "@mui/icons-material/CloudUpload";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";

const OUTLETS = ["rozelle", "avalon", "manly"];

/** Represents each stage of the upload workflow from file selection through server commit. */
type Step = "idle" | "previewing" | "preview_ready" | "committing" | "success";

/** Preview summary returned by the server before a sales history upload is committed. */
interface PreviewData {
  /** ISO datetime of the earliest sale in the uploaded file. */
  file_min: string | null;
  /** ISO datetime of the latest sale in the uploaded file. */
  file_max: string | null;
  /** Total number of sale rows in the file. */
  row_count: number;
  /** ISO datetime of the earliest existing sale for the outlet in the warehouse. */
  existing_min: string | null;
  /** ISO datetime of the latest existing sale for the outlet in the warehouse. */
  existing_max: string | null;
  /** Number of distinct timestamps in the file that already exist in the warehouse. */
  duplicate_count: number;
  /** Number of individual rows affected by duplicate timestamps. */
  duplicate_rows: number;
}

/** Date range of sales already stored in the warehouse for a single outlet. */
interface OutletCoverage {
  /** ISO datetime of the earliest sale on record for the outlet. */
  existing_min: string | null;
  /** ISO datetime of the most recent sale on record for the outlet. */
  existing_max: string | null;
}

/**
 * Formats an ISO datetime string for display, returning an em-dash when the value is absent.
 * @param iso - ISO 8601 datetime string or null.
 * @returns Locale-formatted datetime string, or "—" if null.
 */
function fmtDt(iso: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

/**
 * POSTs a FormData payload via XHR, invoking a callback with upload progress percentage.
 * @param url - Endpoint to POST to.
 * @param form - Multipart form data to send.
 * @param onProgress - Called with a 0–100 integer as bytes are transmitted.
 * @returns Resolved with the response status and raw response text.
 */
function uploadWithProgress(
  url: string,
  form: FormData,
  onProgress: (pct: number) => void,
): Promise<{ ok: boolean; status: number; text: string }> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) onProgress(Math.round((e.loaded / e.total) * 100));
    };
    xhr.onload = () => resolve({ ok: xhr.status < 400, status: xhr.status, text: xhr.responseText });
    xhr.onerror = () => reject(new Error("Network error"));
    xhr.send(form);
  });
}

/** Page for uploading a Lightspeed sales history CSV with duplicate detection and outlet selection. */
export default function SalesHistoryUploadPage() {
  const [outlet, setOutlet] = useState("rozelle");
  const [coverage, setCoverage] = useState<OutletCoverage | null>(null);
  const [coverageLoading, setCoverageLoading] = useState(true);
  const [file, setFile] = useState<File | null>(null);
  const [step, setStep] = useState<Step>("idle");
  const [preview, setPreview] = useState<PreviewData | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [uploadPct, setUploadPct] = useState(0);
  const [commitError, setCommitError] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);

  useEffect(() => {
    setCoverageLoading(true);
    fetch("/api/mart/data-health")
      .then((r) => r.json())
      .then((data) => {
        const row = data.sale_history_by_outlet?.find(
          (r: { outlet: string }) => r.outlet === outlet,
        );
        setCoverage(row
          ? { existing_min: row.earliest_sale, existing_max: row.latest_sale }
          : { existing_min: null, existing_max: null },
        );
      })
      .catch(() => setCoverage(null))
      .finally(() => setCoverageLoading(false));
  }, [outlet]);

  /**
   * Sends the selected file to the preview endpoint and advances the workflow to `preview_ready`.
   * @param f - The CSV file chosen by the user.
   */
  async function handleFileChange(f: File) {
    setFile(f);
    setPreview(null);
    setPreviewError(null);
    setCommitError(null);
    setStep("previewing");

    const form = new FormData();
    form.append("file", f);
    form.append("outlet", outlet);

    try {
      const res = await fetch("/api/loader/sale-history/preview", { method: "POST", body: form });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? `HTTP ${res.status}`);
      setPreview(data);
      setStep("preview_ready");
    } catch (err) {
      setPreviewError((err as Error).message);
      setStep("idle");
    }
  }

  /** Commits the previewed file to the server, optionally stripping duplicate rows before insertion. */
  async function handleConfirm() {
    if (!file) return;
    setCommitError(null);
    setStep("committing");
    setUploadPct(0);

    const form = new FormData();
    form.append("file", file);
    form.append("outlet", outlet);
    form.append("deduplicate", "true");

    try {
      const res = await uploadWithProgress("/api/loader/sale-history", form, (pct) => {
        setUploadPct(pct);
      });
      let data: { detail?: string } = {};
      try { data = JSON.parse(res.text); } catch { /* plain-text */ }
      if (!res.ok) throw new Error(data.detail ?? res.text.slice(0, 300) ?? `HTTP ${res.status}`);
      setStep("success");
    } catch (err) {
      setCommitError((err as Error).message);
      setStep("preview_ready");
    } finally {
      setUploadPct(0);
    }
  }

  /** Resets all form state back to `idle` so the user can start a fresh upload. */
  function handleReset() {
    setFile(null);
    setPreview(null);
    setPreviewError(null);
    setCommitError(null);
    setStep("idle");
  }

  /**
   * Updates the selected outlet and re-runs the file preview when a file is already chosen.
   * @param newOutlet - The outlet name selected by the user.
   */
  function handleOutletChange(newOutlet: string) {
    setOutlet(newOutlet);
    if (file) handleFileChange(file);
  }

  const canConfirm = !!preview;
  const netRows = preview ? preview.row_count - preview.duplicate_rows : 0;

  return (
    <>
      <Typography variant="h5" gutterBottom>Upload Sales History</Typography>
      <Stack spacing={3} sx={{ maxWidth: 520 }}>

        <FormControl fullWidth>
          <InputLabel>Outlet</InputLabel>
          <Select
            value={outlet}
            label="Outlet"
            onChange={(e) => handleOutletChange(e.target.value)}
            disabled={step === "committing"}
          >
            {OUTLETS.map((o) => (
              <MenuItem key={o} value={o} sx={{ textTransform: "capitalize" }}>{o}</MenuItem>
            ))}
          </Select>
        </FormControl>

        <Box sx={{ border: 1, borderColor: "divider", borderRadius: 1, p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Current {outlet} coverage
          </Typography>
          {coverageLoading ? (
            <CircularProgress size={16} />
          ) : coverage?.existing_min ? (
            <Stack direction="row" spacing={1} flexWrap="wrap">
              <Chip size="small" label={`From: ${fmtDt(coverage.existing_min)}`} variant="outlined" />
              <Chip size="small" label={`To: ${fmtDt(coverage.existing_max)}`} variant="outlined" />
            </Stack>
          ) : (
            <Typography variant="body2" color="text.secondary">No data yet</Typography>
          )}
        </Box>

        <Box
          component="label"
          onDragOver={(e) => { e.preventDefault(); if (step !== "committing") setIsDragging(true); }}
          onDragLeave={() => setIsDragging(false)}
          onDrop={(e) => {
            e.preventDefault();
            setIsDragging(false);
            if (step === "committing") return;
            const f = e.dataTransfer.files?.[0];
            if (f) handleFileChange(f);
          }}
          sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            gap: 1,
            p: 3,
            border: 2,
            borderStyle: "dashed",
            borderColor: isDragging ? "primary.main" : "divider",
            borderRadius: 1,
            bgcolor: isDragging ? "action.hover" : "transparent",
            cursor: step === "committing" ? "not-allowed" : "pointer",
            transition: "border-color 0.15s, background-color 0.15s",
            opacity: step === "committing" ? 0.5 : 1,
          }}
        >
          <CloudUploadIcon color={isDragging ? "primary" : "action"} fontSize="large" />
          <Typography variant="body2" color="text.secondary" textAlign="center">
            {file ? file.name : "Drop a CSV here, or click to browse"}
          </Typography>
          <input
            type="file"
            accept=".csv"
            hidden
            disabled={step === "committing"}
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) handleFileChange(f);
            }}
          />
        </Box>

        {previewError && <Alert severity="error">{previewError}</Alert>}

        {step === "previewing" && (
          <Stack direction="row" spacing={1} alignItems="center">
            <CircularProgress size={18} />
            <Typography variant="body2" color="text.secondary">Checking for overlaps…</Typography>
          </Stack>
        )}

        {step === "preview_ready" && preview && (
          <Box sx={{ border: 1, borderColor: "divider", borderRadius: 1, p: 2 }}>
            <Stack spacing={1.5}>
              <Typography variant="subtitle2">File preview</Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap">
                <Chip size="small" label={`${preview.row_count} sale rows`} />
                <Chip size="small" label={`From: ${fmtDt(preview.file_min)}`} variant="outlined" />
                <Chip size="small" label={`To: ${fmtDt(preview.file_max)}`} variant="outlined" />
              </Stack>

              <Divider />

              <Typography variant="subtitle2">Current {outlet} coverage</Typography>
              {preview.existing_min ? (
                <Stack direction="row" spacing={1} flexWrap="wrap">
                  <Chip size="small" label={`From: ${fmtDt(preview.existing_min)}`} variant="outlined" />
                  <Chip size="small" label={`To: ${fmtDt(preview.existing_max)}`} variant="outlined" />
                </Stack>
              ) : (
                <Typography variant="body2" color="text.secondary">No data yet</Typography>
              )}

              {preview.duplicate_count > 0 ? (
                <Alert severity="info">
                  {preview.duplicate_rows} duplicate row{preview.duplicate_rows !== 1 ? "s" : ""} will be skipped
                  — {netRows} new row{netRows !== 1 ? "s" : ""} will be inserted.
                </Alert>
              ) : (
                <Alert severity="success" icon={false}>No overlaps detected.</Alert>
              )}

              {commitError && <Alert severity="error">{commitError}</Alert>}

              <Stack direction="row" spacing={1}>
                <Button variant="contained" onClick={handleConfirm} disabled={!canConfirm}>
                  Confirm Upload
                </Button>
                <Button variant="outlined" onClick={handleReset}>Cancel</Button>
              </Stack>
            </Stack>
          </Box>
        )}

        {step === "committing" && (
          <Box>
            <Typography variant="caption" color="text.secondary">
              {uploadPct < 100 ? `Uploading… ${uploadPct}%` : "Running dbt…"}
            </Typography>
            <LinearProgress variant={uploadPct < 100 ? "determinate" : "indeterminate"} value={uploadPct} />
          </Box>
        )}

        {step === "success" && preview && (
          <Box sx={{ border: 1, borderColor: "success.main", borderRadius: 1, p: 2 }}>
            <Stack spacing={1.5}>
              <Stack direction="row" spacing={1} alignItems="center">
                <CheckCircleOutlineIcon color="success" />
                <Typography variant="subtitle2">Upload successful</Typography>
              </Stack>
              <Stack direction="row" spacing={1} flexWrap="wrap">
                <Chip size="small" color="success" label={`${netRows} rows added`} />
                <Chip size="small" label={`From: ${fmtDt(preview.file_min)}`} variant="outlined" />
                <Chip size="small" label={`To: ${fmtDt(preview.file_max)}`} variant="outlined" />
              </Stack>
              <Typography variant="body2" color="text.secondary">Ready to upload the next batch?</Typography>
              <Box>
                <Button variant="outlined" onClick={handleReset}>Upload another file</Button>
              </Box>
            </Stack>
          </Box>
        )}

      </Stack>
    </>
  );
}
