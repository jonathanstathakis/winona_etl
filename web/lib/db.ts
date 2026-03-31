import { Pool } from "pg";

const globalForPg = globalThis as unknown as { pgPool?: Pool };

export const pool =
  globalForPg.pgPool ??
  new Pool({
    host:     process.env.PGHOST     ?? "localhost",
    port:     Number(process.env.PGPORT ?? 5432),
    user:     process.env.PGUSER     ?? "winona",
    password: process.env.PGPASSWORD ?? "winona",
    database: process.env.PGDATABASE ?? "winona_dw",
  });

if (process.env.NODE_ENV !== "production") globalForPg.pgPool = pool;
