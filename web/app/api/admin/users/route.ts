import { auth } from "@/auth";
import { NextResponse } from "next/server";
import { listUsers, createUser } from "@/lib/users";
import bcrypt from "bcryptjs";
import type { Role } from "@/lib/users";

/**
 * Checks that the current session belongs to a superuser.
 * @returns `null` if the check passes, or a 403 `NextResponse` if it fails.
 */
async function requireSuperuser() {
  const session = await auth();
  if (session?.user?.role !== "superuser")
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  return null;
}

/**
 * `GET /api/admin/users` — Returns all users (without password hashes), ordered by creation date.
 *
 * Requires superuser role; responds with 403 otherwise.
 * @returns JSON array of `{ id, username, role, created_at }` objects.
 */
export async function GET() {
  const denied = await requireSuperuser();
  if (denied) return denied;
  const users = await listUsers();
  return NextResponse.json(users);
}

/**
 * `POST /api/admin/users` — Creates a new user with a bcrypt-hashed password.
 *
 * Requires superuser role; responds with 403 otherwise.
 * @param req - JSON body: `{ username: string, password: string, role: Role }`.
 * @returns `{ ok: true }` with status 201 on success.
 */
export async function POST(req: Request) {
  const denied = await requireSuperuser();
  if (denied) return denied;
  const { username, password, role } = await req.json() as { username: string; password: string; role: Role };
  const hash = await bcrypt.hash(password, 12);
  await createUser(username, hash, role);
  return NextResponse.json({ ok: true }, { status: 201 });
}
