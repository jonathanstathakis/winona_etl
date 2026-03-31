import { auth } from "@/auth";
import { getUserByUsername } from "@/lib/users";
import { updatePassword } from "@/lib/users";
import { AuthError } from "next-auth";
import bcrypt from "bcryptjs";
import ChangePasswordForm from "./ChangePasswordForm";

export default async function SettingsPage() {
  const session = await auth();
  const userId = session?.user?.id;

  async function changePassword(_: string | null, formData: FormData): Promise<string | null> {
    "use server";
    const current = formData.get("current") as string;
    const next = formData.get("new") as string;
    const confirm = formData.get("confirm") as string;

    if (next !== confirm) return "New passwords do not match.";
    if (next.length < 8) return "New password must be at least 8 characters.";

    const user = await getUserByUsername(session?.user?.name ?? "");
    if (!user) return "User not found.";

    const valid = await bcrypt.compare(current, user.password_hash);
    if (!valid) return "Current password is incorrect.";

    const hash = await bcrypt.hash(next, 12);
    await updatePassword(userId!, hash);
    return "ok";
  }

  return <ChangePasswordForm action={changePassword} />;
}
