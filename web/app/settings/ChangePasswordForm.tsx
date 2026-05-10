"use client";
import { useActionState } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Stack,
  TextField,
  Typography,
} from "@mui/material";

/** Props for the ChangePasswordForm component. */
interface Props {
  /**
   * Server action used as the form's `useActionState` handler; returns `"ok"` on
   * success or an error message string on failure.
   */
  action: (_: string | null, formData: FormData) => Promise<string | null>;
}

/** Form that allows the authenticated user to change their password, showing success or error feedback. */
export default function ChangePasswordForm({ action }: Props) {
  const [result, formAction, pending] = useActionState(action, null);

  return (
    <Box>
      <Typography variant="h5" gutterBottom>Settings</Typography>
      <Card variant="outlined" sx={{ maxWidth: 400 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>Change Password</Typography>
          {result === "ok" && <Alert severity="success" sx={{ mb: 2 }}>Password updated.</Alert>}
          {result && result !== "ok" && <Alert severity="error" sx={{ mb: 2 }}>{result}</Alert>}
          <form action={formAction}>
            <Stack spacing={2}>
              <TextField name="current" label="Current password" type="password" required />
              <TextField name="new" label="New password" type="password" required />
              <TextField name="confirm" label="Confirm new password" type="password" required />
              <Button type="submit" variant="contained" disabled={pending}>
                {pending ? "Saving…" : "Update password"}
              </Button>
            </Stack>
          </form>
        </CardContent>
      </Card>
    </Box>
  );
}
