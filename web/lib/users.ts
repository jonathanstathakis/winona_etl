import { pool } from "./db";

export type Role = "superuser" | "admin" | "viewer";

export interface DbUser {
  id: string;
  username: string;
  password_hash: string;
  role: Role;
}

export async function getUserByUsername(username: string): Promise<DbUser | null> {
  const { rows } = await pool.query<DbUser>(
    "SELECT id, username, password_hash, role FROM auth.users WHERE username = $1",
    [username],
  );
  return rows[0] ?? null;
}

export async function listUsers(): Promise<Omit<DbUser, "password_hash">[]> {
  const { rows } = await pool.query(
    "SELECT id, username, role, created_at FROM auth.users ORDER BY created_at",
  );
  return rows;
}

export async function createUser(username: string, passwordHash: string, role: Role): Promise<void> {
  await pool.query(
    "INSERT INTO auth.users (username, password_hash, role) VALUES ($1, $2, $3)",
    [username, passwordHash, role],
  );
}

export async function deleteUser(id: string): Promise<void> {
  await pool.query("DELETE FROM auth.users WHERE id = $1", [id]);
}

export async function updateUserRole(id: string, role: Role): Promise<void> {
  await pool.query(
    "UPDATE auth.users SET role = $1, updated_at = now() WHERE id = $2",
    [role, id],
  );
}

export async function updatePassword(id: string, passwordHash: string): Promise<void> {
  await pool.query(
    "UPDATE auth.users SET password_hash = $1, updated_at = now() WHERE id = $2",
    [passwordHash, id],
  );
}
