"use client";
import { Button } from "@mui/material";

/** Button that submits a form to invoke the provided server-side sign-out action. */
export default function SignOutButton({ action }: { action: () => Promise<void> }) {
  return (
    <form action={action}>
      <Button type="submit" color="inherit" size="small">
        Sign out
      </Button>
    </form>
  );
}
