import { auth, signIn } from "@/auth";
import { redirect } from "next/navigation";
import { AuthError } from "next-auth";
import LoginForm from "./LoginForm";

export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ callbackUrl?: string }>;
}) {
  const session = await auth();
  if (session) redirect("/");

  const { callbackUrl } = await searchParams;

  async function login(_: string | null, formData: FormData): Promise<string | null> {
    "use server";
    try {
      await signIn("credentials", {
        username: formData.get("username"),
        password: formData.get("password"),
        redirectTo: callbackUrl ?? "/",
      });
    } catch (e) {
      if (e instanceof AuthError) return "Invalid username or password.";
      throw e;
    }
    return null;
  }

  return <LoginForm action={login} />;
}
