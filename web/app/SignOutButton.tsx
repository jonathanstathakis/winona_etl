"use client";
import { Button } from "@mui/material";

export default function SignOutButton({ action }: { action: () => Promise<void> }) {
  return (
    <form action={action}>
      <Button type="submit" color="inherit" size="small">
        Sign out
      </Button>
    </form>
  );
}
