import { Pool } from "pg";

const globalForPg = globalThis as unknown as { pgPool?: Pool };

/**
 * Shared pg connection pool for the Winona data warehouse, reused across hot-reloads in development.
 *
 * Connection parameters are read from `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`
 * environment variables, falling back to the local development defaults.
 */
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
