"use client";
import { useState } from "react";
import {
  AppBar,
  Box,
  CssBaseline,
  Drawer,
  IconButton,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  ThemeProvider,
  Toolbar,
  Typography,
  createTheme,
} from "@mui/material";
import MenuIcon from "@mui/icons-material/Menu";
import Link from "next/link";
import "./globals.css";

const theme = createTheme({ palette: { mode: "light" } });

const NAV = [
  { label: "Product Catalog", href: "/catalog" },
  { label: "Transfer", href: "/transfer" },
  { label: "Wine", href: "/wine" },
  { label: "Upload", href: "/upload" },
  { label: "Data Health", href: "/health" },
];

const DRAWER_WIDTH = 220;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const [open, setOpen] = useState(true);

  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <ThemeProvider theme={theme}>
          <CssBaseline />
          <Box sx={{ display: "flex" }}>
            <AppBar position="fixed" sx={{ zIndex: (t) => t.zIndex.drawer + 1 }}>
              <Toolbar>
                <IconButton
                  color="inherit"
                  edge="start"
                  onClick={() => setOpen((o) => !o)}
                  sx={{ mr: 2 }}
                >
                  <MenuIcon />
                </IconButton>
                <Typography variant="h6" noWrap>Winona</Typography>
              </Toolbar>
            </AppBar>
            <Drawer
              variant="permanent"
              sx={{
                width: open ? DRAWER_WIDTH : 0,
                flexShrink: 0,
                transition: "width 0.2s",
                "& .MuiDrawer-paper": {
                  width: open ? DRAWER_WIDTH : 0,
                  boxSizing: "border-box",
                  overflowX: "hidden",
                  transition: "width 0.2s",
                },
              }}
            >
              <Toolbar />
              <List>
                {NAV.map((n) => (
                  <ListItem key={n.href} disablePadding>
                    <ListItemButton component={Link} href={n.href}>
                      <ListItemText primary={n.label} />
                    </ListItemButton>
                  </ListItem>
                ))}
              </List>
            </Drawer>
            <Box component="main" sx={{ flexGrow: 1, minWidth: 0, p: 3 }}>
              <Toolbar />
              {children}
            </Box>
          </Box>
        </ThemeProvider>
      </body>
    </html>
  );
}
