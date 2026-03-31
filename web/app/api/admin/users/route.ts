import { auth } from "@/auth";
import { NextResponse } from "next/server";
import { listUsers, createUser } from "@/lib/users";
import bcrypt from "bcryptjs";
import type { Role } from "@/lib/users";

async function requireSuperuser() {
  const session = await auth();
  if (session?.user?.role !== "superuser")
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  return null;
}

export async function GET() {
  const denied = await requireSuperuser();
  if (denied) return denied;
  const users = await listUsers();
  return NextResponse.json(users);
}

export async function POST(req: Request) {
  const denied = await requireSuperuser();
  if (denied) return denied;
  const { username, password, role } = await req.json() as { username: string; password: string; role: Role };
  const hash = await bcrypt.hash(password, 12);
  await createUser(username, hash, role);
  return NextResponse.json({ ok: true }, { status: 201 });
}
