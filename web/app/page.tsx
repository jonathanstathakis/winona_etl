import {
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Grid,
  Typography,
} from "@mui/material";
import Link from "next/link";

const FEATURES = [
  {
    title: "Product Catalog",
    href: "/catalog",
    description:
      "Browse, filter, sort, and export the full product catalog. Save filter and column presets for quick access.",
  },
  {
    title: "Transfer",
    href: "/transfer",
    description:
      "Build inventory transfer orders between venues. Add products individually or in bulk, edit quantities, and export as CSV.",
  },
  {
    title: "Wine",
    href: "/wine",
    description:
      "Wine-specific product view showing name, SKU, brand, pricing, and tags.",
  },
  {
    title: "Upload",
    href: "/upload",
    description:
      "Upload Lightspeed product export or sales history CSVs to refresh the data warehouse.",
  },
  {
    title: "Data Health",
    href: "/health",
    description:
      "View data coverage — product export history and sale date ranges per outlet.",
  },
];

export default function Home() {
  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Winona
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 4 }}>
        Internal data warehouse for Winona Wine. Ingest Lightspeed exports,
        explore the product catalog, and manage inventory transfers.
      </Typography>
      <Grid container spacing={3}>
        {FEATURES.map((f) => (
          <Grid key={f.href} size={{ xs: 12, sm: 6, md: 4 }}>
            <Card variant="outlined" sx={{ height: "100%", display: "flex", flexDirection: "column" }}>
              <CardContent sx={{ flexGrow: 1 }}>
                <Typography variant="h6" gutterBottom>
                  {f.title}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {f.description}
                </Typography>
              </CardContent>
              <CardActions>
                <Link href={f.href}>
                  <Button size="small">Open</Button>
                </Link>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>
    </Box>
  );
}
