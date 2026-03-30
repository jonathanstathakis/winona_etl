"use client";
import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import {
  Alert,
  Box,
  Button,
  LinearProgress,
  Stack,
  Typography,
} from "@mui/material";
import CloudUploadIcon from "@mui/icons-material/CloudUpload";

type Phase = "uploading" | "processing" | null;

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

export default function ProductCatalogUploadPage() {
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<{ ok: boolean; message: string } | null>(null);
  const [phase, setPhase] = useState<Phase>(null);
  const [uploadPct, setUploadPct] = useState(0);

  const busy = phase !== null;

  const onDrop = useCallback((accepted: File[]) => {
    if (accepted[0]) setFile(accepted[0]);
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { "text/csv": [".csv"] },
    multiple: false,
    disabled: busy,
  });

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!file) return;
    setStatus(null);
    setPhase("uploading");
    setUploadPct(0);

    const form = new FormData();
    form.append("file", file);

    try {
      const res = await uploadWithProgress("/api/loader/product-export", form, (pct) => {
        setUploadPct(pct);
        if (pct === 100) setPhase("processing");
      });
      let data: { detail?: string } = {};
      try { data = JSON.parse(res.text); } catch { /* plain-text */ }
      if (!res.ok) throw new Error(data.detail ?? res.text.slice(0, 300) ?? `HTTP ${res.status}`);
      setStatus({ ok: true, message: "Upload successful — dbt models rebuilt." });
    } catch (err) {
      setStatus({ ok: false, message: (err as Error).message });
    } finally {
      setPhase(null);
      setUploadPct(0);
    }
  }

  return (
    <>
      <Typography variant="h5" gutterBottom>Upload Product Catalog</Typography>
      <Box component="form" onSubmit={handleSubmit} sx={{ maxWidth: 480 }}>
        <Stack spacing={3}>
          <Box
            {...getRootProps()}
            sx={{
              border: "2px dashed",
              borderColor: isDragActive ? "primary.main" : "divider",
              borderRadius: 2,
              p: 4,
              textAlign: "center",
              cursor: busy ? "not-allowed" : "pointer",
              bgcolor: isDragActive ? "action.hover" : "background.paper",
              transition: "border-color 0.15s, background-color 0.15s",
            }}
          >
            <input {...getInputProps()} />
            <CloudUploadIcon sx={{ fontSize: 40, color: "text.secondary", mb: 1 }} />
            <Typography variant="body1">
              {file ? file.name : isDragActive ? "Drop CSV here…" : "Drag & drop a CSV, or click to select"}
            </Typography>
            {file && (
              <Typography variant="caption" color="text.secondary">
                Click to change file
              </Typography>
            )}
          </Box>

          <Button type="submit" variant="contained" disabled={!file || busy}>
            {busy ? (phase === "uploading" ? `Uploading… ${uploadPct}%` : "Running dbt…") : "Upload & Run dbt"}
          </Button>

          {phase === "uploading" && (
            <Box>
              <Typography variant="caption" color="text.secondary">Uploading file…</Typography>
              <LinearProgress variant="determinate" value={uploadPct} />
            </Box>
          )}
          {phase === "processing" && (
            <Box>
              <Typography variant="caption" color="text.secondary">Processing — running dbt models…</Typography>
              <LinearProgress variant="indeterminate" />
            </Box>
          )}
          {status && <Alert severity={status.ok ? "success" : "error"}>{status.message}</Alert>}
        </Stack>
      </Box>
    </>
  );
}
