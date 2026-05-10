import { listUsers } from "@/lib/users";
import { Typography } from "@mui/material";
import UsersTable from "./UsersTable";

/** Admin page that fetches all users and renders the UsersTable management interface. */
export default async function AdminUsersPage() {
  const users = await listUsers();
  return (
    <>
      <Typography variant="h5" gutterBottom>User Management</Typography>
      <UsersTable initialUsers={users} />
    </>
  );
}
