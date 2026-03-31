import { auth } from "@/auth";
import AppShell from "./AppShell";
import "./globals.css";

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const session = await auth();
  const user = session?.user ?? null;
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <AppShell user={user}>{children}</AppShell>
      </body>
    </html>
  );
}
