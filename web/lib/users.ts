import { pool } from "./db";

/** Application-level access roles, ordered from most to least privileged. */
export type Role = "superuser" | "admin" | "viewer";

/** Row shape returned by `auth.users` in the data warehouse. */
export interface DbUser {
  id: string;
  username: string;
  /** bcrypt hash of the user's password. */
  password_hash: string;
  role: Role;
}

/**
 * Looks up a user record by username.
 * @param username - The username to search for.
 * @returns The matching `DbUser`, or `null` if not found.
 */
export async function getUserByUsername(username: string): Promise<DbUser | null> {
  const { rows } = await pool.query<DbUser>(
    "SELECT id, username, password_hash, role FROM auth.users WHERE username = $1",
    [username],
  );
  return rows[0] ?? null;
}

/** Returns all users ordered by creation date, omitting `password_hash`. */
export async function listUsers(): Promise<Omit<DbUser, "password_hash">[]> {
  const { rows } = await pool.query(
    "SELECT id, username, role, created_at FROM auth.users ORDER BY created_at",
  );
  return rows;
}

/**
 * Inserts a new user into `auth.users`.
 * @param username - Unique login name for the user.
 * @param passwordHash - Pre-computed bcrypt hash of the user's password.
 * @param role - Access role to assign to the new user.
 */
export async function createUser(username: string, passwordHash: string, role: Role): Promise<void> {
  await pool.query(
    "INSERT INTO auth.users (username, password_hash, role) VALUES ($1, $2, $3)",
    [username, passwordHash, role],
  );
}

/**
 * Permanently removes a user from `auth.users`.
 * @param id - UUID of the user to delete.
 */
export async function deleteUser(id: string): Promise<void> {
  await pool.query("DELETE FROM auth.users WHERE id = $1", [id]);
}

/**
 * Updates the role of an existing user.
 * @param id - UUID of the user to update.
 * @param role - New role to assign.
 */
export async function updateUserRole(id: string, role: Role): Promise<void> {
  await pool.query(
    "UPDATE auth.users SET role = $1, updated_at = now() WHERE id = $2",
    [role, id],
  );
}

/**
 * Replaces the stored password hash for a user.
 * @param id - UUID of the user whose password is being changed.
 * @param passwordHash - New bcrypt hash to store.
 */
export async function updatePassword(id: string, passwordHash: string): Promise<void> {
  await pool.query(
    "UPDATE auth.users SET password_hash = $1, updated_at = now() WHERE id = $2",
    [passwordHash, id],
  );
}
