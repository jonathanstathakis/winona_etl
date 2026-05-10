import { auth, signOut } from "@/auth";
import AppShell from "./AppShell";
import "./globals.css";

/** Root layout that wraps every page with authentication state and the AppShell. */
export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const session = await auth();
  const user = session?.user ?? null;

  /** Server action that signs the current user out and redirects to /login. */
  async function handleSignOut() {
    "use server";
    await signOut({ redirectTo: "/login" });
  }

  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <AppShell user={user} signOutAction={handleSignOut}>{children}</AppShell>
      </body>
    </html>
  );
}
