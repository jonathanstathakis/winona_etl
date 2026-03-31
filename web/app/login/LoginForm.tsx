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

interface Props {
  action: (_: string | null, formData: FormData) => Promise<string | null>;
}

export default function LoginForm({ action }: Props) {
  const [error, formAction, pending] = useActionState(action, null);

  return (
    <Box sx={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: "100vh" }}>
      <Card variant="outlined" sx={{ width: 360 }}>
        <CardContent>
          <Typography variant="h5" gutterBottom>Sign in to Winona</Typography>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          <form action={formAction}>
            <Stack spacing={2}>
              <TextField name="username" label="Username" required autoFocus />
              <TextField name="password" label="Password" type="password" required />
              <Button type="submit" variant="contained" disabled={pending}>
                {pending ? "Signing in…" : "Sign in"}
              </Button>
            </Stack>
          </form>
        </CardContent>
      </Card>
    </Box>
  );
}
