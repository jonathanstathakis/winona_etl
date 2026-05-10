import { auth } from "@/auth";
import { NextResponse } from "next/server";
import { deleteUser, updateUserRole } from "@/lib/users";
import type { Role } from "@/lib/users";

/**
 * `DELETE /api/admin/users/[id]` — Permanently deletes the user with the given ID.
 *
 * Requires superuser role; responds with 403 otherwise.
 * @param params - Route params containing the user `id` (UUID).
 * @returns `{ ok: true }` on success.
 */
export async function DELETE(_: Request, { params }: { params: Promise<{ id: string }> }) {
  const session = await auth();
  if (session?.user?.role !== "superuser")
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  const { id } = await params;
  await deleteUser(id);
  return NextResponse.json({ ok: true });
}

/**
 * `PATCH /api/admin/users/[id]` — Updates the role of the user with the given ID.
 *
 * Requires superuser role; responds with 403 otherwise.
 * @param req - JSON body: `{ role: Role }`.
 * @param params - Route params containing the user `id` (UUID).
 * @returns `{ ok: true }` on success.
 */
export async function PATCH(req: Request, { params }: { params: Promise<{ id: string }> }) {
  const session = await auth();
  if (session?.user?.role !== "superuser")
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  const { id } = await params;
  const { role } = await req.json() as { role: Role };
  await updateUserRole(id, role);
  return NextResponse.json({ ok: true });
}
