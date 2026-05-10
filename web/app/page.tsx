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
import { auth } from "@/auth";

/** Navigation cards shown on the home page, filtered by the current user's role. */
const FEATURES = [
  {
    title: "Product Catalog",
    href: "/catalog",
    description:
      "Browse, filter, sort, and export the full product catalog. Save filter and column presets for quick access.",
    roles: ["superuser", "admin", "viewer"],
  },
  {
    title: "Transfer",
    href: "/transfer",
    description:
      "Build inventory transfer orders between venues. Add products individually or in bulk, edit quantities, and export as CSV.",
    roles: ["superuser", "admin", "viewer"],
  },
  {
    title: "Wine",
    href: "/wine",
    description:
      "Wine-specific product view showing name, SKU, brand, pricing, and tags.",
    roles: ["superuser", "admin", "viewer"],
  },
  {
    title: "Upload",
    href: "/upload",
    description:
      "Upload Lightspeed product export or sales history CSVs to refresh the data warehouse.",
    roles: ["superuser", "admin"],
  },
  {
    title: "Data Health",
    href: "/health",
    description:
      "View data coverage — product export history and sale date ranges per outlet.",
    roles: ["superuser", "admin", "viewer"],
  },
  {
    title: "Users",
    href: "/admin/users",
    description:
      "Manage user accounts — create, assign roles, and remove users.",
    roles: ["superuser"],
  },
  {
    title: "Planogram",
    href: "/planogram",
    description:
    "Manage store planograms - shelf display of stock items.",
    roles: ["superuser","admin","viewer"]
  }
];

/** Home page displaying role-filtered feature cards for the Winona data warehouse. */
export default async function Home() {
  const session = await auth();
  const role = session?.user?.role;
  const features = FEATURES.filter((f) => !role || f.roles.includes(role));

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
        {features.map((f) => (
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
