import { auth, signIn } from "@/auth";
import { redirect } from "next/navigation";
import { AuthError } from "next-auth";
import LoginForm from "./LoginForm";

/** Login page that redirects authenticated users to home and renders the login form for others. */
export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ callbackUrl?: string }>;
}) {
  const session = await auth();
  if (session) redirect("/");

  const { callbackUrl } = await searchParams;

  /**
   * Server action that attempts credential sign-in and returns an error message on failure.
   * @param _ - Previous action state (unused).
   * @param formData - Form data containing `username` and `password` fields.
   * @returns An error string on failure, or null on success.
   */
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
