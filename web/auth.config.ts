import type { NextAuthConfig } from "next-auth";
import type { Role } from "@/lib/users";

export const authConfig = {
  session: { strategy: "jwt" },
  callbacks: {
    jwt({ token, user }) {
      if (user) token.role = (user as { role: Role }).role;
      return token;
    },
    session({ session, token }) {
      session.user.role = token.role as Role;
      session.user.id = token.sub ?? "";
      return session;
    },
  },
  pages: {
    signIn: "/login",
  },
  providers: [],
} satisfies NextAuthConfig;
