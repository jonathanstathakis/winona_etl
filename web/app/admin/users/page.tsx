import { listUsers } from "@/lib/users";
import { Typography } from "@mui/material";
import UsersTable from "./UsersTable";

export default async function AdminUsersPage() {
  const users = await listUsers();
  return (
    <>
      <Typography variant="h5" gutterBottom>User Management</Typography>
      <UsersTable initialUsers={users} />
    </>
  );
}
