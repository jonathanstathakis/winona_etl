import { auth, signOut } from "@/auth";
import AppShell from "./AppShell";
import "./globals.css";

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const session = await auth();
  const user = session?.user ?? null;

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
