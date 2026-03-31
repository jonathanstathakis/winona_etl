import NextAuth from "next-auth";
import { authConfig } from "./auth.config";
import { NextResponse } from "next/server";

const { auth } = NextAuth(authConfig);

const PUBLIC_ROUTES = ["/login"];
const ADMIN_ROUTES = ["/upload"];
const SUPERUSER_ROUTES = ["/admin"];

export default auth((req) => {
  const { pathname } = req.nextUrl;

  if (PUBLIC_ROUTES.includes(pathname) || pathname.startsWith("/api/auth")) {
    return NextResponse.next();
  }

  const session = req.auth;
  const role = session?.user?.role;

  if (!session) {
    const loginUrl = new URL("/login", req.url);
    loginUrl.searchParams.set("callbackUrl", pathname);
    return NextResponse.redirect(loginUrl);
  }

  if (SUPERUSER_ROUTES.some((r) => pathname.startsWith(r))) {
    if (role !== "superuser") return NextResponse.redirect(new URL("/", req.url));
  }

  if (ADMIN_ROUTES.some((r) => pathname.startsWith(r))) {
    if (role !== "admin" && role !== "superuser") {
      return NextResponse.redirect(new URL("/", req.url));
    }
  }

  return NextResponse.next();
});

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
