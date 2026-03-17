"use client";
import { Box, Card, CardActionArea, CardContent, Stack, Typography } from "@mui/material";
import InventoryIcon from "@mui/icons-material/Inventory";
import ReceiptLongIcon from "@mui/icons-material/ReceiptLong";
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
    </>
  );
}
