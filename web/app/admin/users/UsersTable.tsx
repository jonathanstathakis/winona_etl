"use client";
import { useState } from "react";
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Paper,
  Select,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import type { Role } from "@/lib/users";

/** A user record returned by the admin users API. */
interface User {
  id: string;
  username: string;
  role: Role;
  /** ISO 8601 timestamp string of when the account was created. */
  created_at: string;
}

const ROLES: Role[] = ["viewer", "admin", "superuser"];

/** Interactive table for managing users — supports adding, deleting, and changing roles. */
export default function UsersTable({ initialUsers }: { initialUsers: User[] }) {
  const [users, setUsers] = useState<User[]>(initialUsers);
  const [error, setError] = useState<string | null>(null);
  const [addOpen, setAddOpen] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState<Role>("viewer");
  const [adding, setAdding] = useState(false);

  /** Fetches the current user list from the API and refreshes local state. */
  async function reload() {
    const res = await fetch("/api/admin/users");
    if (res.ok) setUsers(await res.json());
  }

  /**
   * Prompts for confirmation then deletes a user via the API.
   * @param id - The user's unique identifier.
   * @param username - The username shown in the confirmation prompt.
   */
  async function handleDelete(id: string, username: string) {
    if (!confirm(`Delete user '${username}'?`)) return;
    const res = await fetch(`/api/admin/users/${id}`, { method: "DELETE" });
    if (!res.ok) { setError("Failed to delete user."); return; }
    setUsers((u) => u.filter((x) => x.id !== id));
  }

  /**
   * Patches a user's role via the API and updates local state on success.
   * @param id - The user's unique identifier.
   * @param role - The new role to assign.
   */
  async function handleRoleChange(id: string, role: Role) {
    const res = await fetch(`/api/admin/users/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ role }),
    });
    if (!res.ok) { setError("Failed to update role."); return; }
    setUsers((u) => u.map((x) => x.id === id ? { ...x, role } : x));
  }

  /** Submits the new-user form to the API and reloads the user list on success. */
  async function handleAdd() {
    if (!newUsername || !newPassword) return;
    setAdding(true);
    setError(null);
    const res = await fetch("/api/admin/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: newUsername, password: newPassword, role: newRole }),
    });
    setAdding(false);
    if (!res.ok) { setError("Failed to create user."); return; }
    setAddOpen(false);
    setNewUsername("");
    setNewPassword("");
    setNewRole("viewer");
    await reload();
  }

  return (
    <Box>
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      <Button variant="contained" sx={{ mb: 2 }} onClick={() => setAddOpen(true)}>
        Add user
      </Button>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Username</TableCell>
              <TableCell>Role</TableCell>
              <TableCell>Created</TableCell>
              <TableCell />
            </TableRow>
          </TableHead>
          <TableBody>
            {users.map((u) => (
              <TableRow key={u.id}>
                <TableCell>{u.username}</TableCell>
                <TableCell>
                  <Select
                    size="small"
                    value={u.role}
                    onChange={(e) => handleRoleChange(u.id, e.target.value as Role)}
                  >
                    {ROLES.map((r) => (
                      <MenuItem key={r} value={r}>{r}</MenuItem>
                    ))}
                  </Select>
                </TableCell>
                <TableCell>
                  <Typography variant="body2">
                    {new Date(u.created_at).toLocaleDateString()}
                  </Typography>
                </TableCell>
                <TableCell align="right">
                  <Button
                    size="small"
                    color="error"
                    onClick={() => handleDelete(u.id, u.username)}
                  >
                    Delete
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog open={addOpen} onClose={() => setAddOpen(false)} maxWidth="xs" fullWidth>
        <DialogTitle>Add user</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Username"
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              autoFocus
            />
            <TextField
              label="Password"
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
            />
            <Select value={newRole} onChange={(e) => setNewRole(e.target.value as Role)}>
              {ROLES.map((r) => (
                <MenuItem key={r} value={r}>{r}</MenuItem>
              ))}
            </Select>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={handleAdd} disabled={adding}>
            {adding ? "Creating…" : "Create"}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
