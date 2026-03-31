"use client";
import { useState } from "react";
import { signOut } from "next-auth/react";
import {
  AppBar,
  Box,
  Button,
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
import type { Role } from "@/lib/users";

const theme = createTheme({ palette: { mode: "light" } });

const NAV = [
  { label: "Home", href: "/", roles: ["superuser", "admin", "viewer"] },
  { label: "Product Catalog", href: "/catalog", roles: ["superuser", "admin", "viewer"] },
  { label: "Transfer", href: "/transfer", roles: ["superuser", "admin", "viewer"] },
  { label: "Wine", href: "/wine", roles: ["superuser", "admin", "viewer"] },
  { label: "Upload", href: "/upload", roles: ["superuser", "admin"] },
  { label: "Data Health", href: "/health", roles: ["superuser", "admin", "viewer"] },
  { label: "Users", href: "/admin/users", roles: ["superuser"] },
];

const DRAWER_WIDTH = 220;

interface Props {
  children: React.ReactNode;
  user: { name?: string | null; role?: Role } | null;
}

export default function AppShell({ children, user }: Props) {
  const [open, setOpen] = useState(false);

  const visibleNav = NAV.filter((n) =>
    !user?.role || n.roles.includes(user.role)
  );

  return (
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
              <MenuIcon sx={{ transition: "transform 0.2s", transform: open ? "rotate(0deg)" : "rotate(-90deg)" }} />
            </IconButton>
            <Typography variant="h6" noWrap>Winona</Typography>
            {user && (
              <Box sx={{ ml: "auto", display: "flex", alignItems: "center", gap: 1 }}>
                <Typography variant="body2">{user.name} ({user.role})</Typography>
                <Button color="inherit" size="small" onClick={() => signOut({ callbackUrl: "/login" })}>
                  Sign out
                </Button>
              </Box>
            )}
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
            {visibleNav.map((n) => (
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
  );
}
