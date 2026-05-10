import { handlers } from "@/auth";

/**
 * Next.js catch-all route that delegates all `GET` and `POST` requests under
 * `/api/auth/*` to the Auth.js request handlers (sign-in, sign-out, callbacks, etc.).
 */
export const { GET, POST } = handlers;
