import { auth } from "@/auth";
import { NextResponse } from "next/server";
import { deleteUser, updateUserRole } from "@/lib/users";
import type { Role } from "@/lib/users";

export async function DELETE(_: Request, { params }: { params: Promise<{ id: string }> }) {
  const session = await auth();
  if (session?.user?.role !== "superuser")
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  const { id } = await params;
  await deleteUser(id);
  return NextResponse.json({ ok: true });
}

export async function PATCH(req: Request, { params }: { params: Promise<{ id: string }> }) {
  const session = await auth();
  if (session?.user?.role !== "superuser")
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  const { id } = await params;
  const { role } = await req.json() as { role: Role };
  await updateUserRole(id, role);
  return NextResponse.json({ ok: true });
}
