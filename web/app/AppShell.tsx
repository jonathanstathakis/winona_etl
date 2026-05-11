"use client";
import { useState } from "react";
import {
  AppBar,
  Box,
  Button,
  Drawer,
  IconButton,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Toolbar,
  Typography,
} from "@mui/material";
import MenuIcon from "@mui/icons-material/Menu";
import Link from "next/link";
import ThemeRegistry from "./ThemeRegistry";
import SignOutButton from "./SignOutButton";
import type { Role } from "@/lib/users";

/** Navigation links shown in the side drawer, each restricted to specific roles. */
const NAV = [
  { label: "Home", href: "/", roles: ["superuser", "admin", "viewer"] },
  { label: "Product Catalog", href: "/catalog", roles: ["superuser", "admin", "viewer"] },
  { label: "Transfer", href: "/transfer", roles: ["superuser", "admin", "viewer"] },
  { label: "Wine", href: "/wine", roles: ["superuser", "admin", "viewer"] },
  { label: "Upload", href: "/upload", roles: ["superuser", "admin"] },
  { label: "Data Health", href: "/health", roles: ["superuser", "admin", "viewer"] },
  { label: "Planogram", href: "/planogram", roles: ["superuser", "admin", "viewer"] },
  { label: "Users", href: "/admin/users", roles: ["superuser"] },
  { label: "Settings", href: "/settings", roles: ["superuser", "admin", "viewer"] },
];

const DRAWER_WIDTH = 220;

/** Props for the AppShell component. */
interface Props {
  children: React.ReactNode;
  /** Authenticated user, or null when the visitor is unauthenticated. */
  user: { name?: string | null; role?: Role } | null;
  /** Server action invoked when the user clicks Sign out. */
  signOutAction: () => Promise<void>;
}

/** Top-level application shell with a fixed AppBar and a collapsible side drawer filtered by user role. */
export default function AppShell({ children, user, signOutAction }: Props) {
  const [open, setOpen] = useState(false);

  const visibleNav = NAV.filter((n) =>
    !user?.role || n.roles.includes(user.role)
  );

  return (
    <ThemeRegistry>
      <Box sx={{ display: "flex" }}>
        <AppBar position="fixed" sx={{ zIndex: (t) => t.zIndex.drawer + 1 }}>
          <Toolbar>
            {user && (
              <IconButton
                color="inherit"
                edge="start"
                onClick={() => setOpen((o) => !o)}
                sx={{ mr: 2 }}
              >
                <MenuIcon sx={{ transition: "transform 0.2s", transform: open ? "rotate(0deg)" : "rotate(-90deg)" }} />
              </IconButton>
            )}
            <Typography variant="h6" noWrap>Winona</Typography>
            {user && (
              <Box sx={{ ml: "auto", display: "flex", alignItems: "center", gap: 1 }}>
                <Typography variant="body2">{user.name} ({user.role})</Typography>
                <SignOutButton action={signOutAction} />
              </Box>
            )}
          </Toolbar>
        </AppBar>
        {user && (
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
        )}
        <Box component="main" sx={{ flexGrow: 1, minWidth: 0, p: 3 }}>
          <Toolbar />
          {children}
        </Box>
      </Box>
    </ThemeRegistry>
  );
}
