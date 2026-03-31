import { Pool } from "pg";
import bcrypt from "bcryptjs";

const pool = new Pool({
  host:     process.env.PGHOST     ?? "localhost",
  port:     Number(process.env.PGPORT ?? 5432),
  user:     process.env.PGUSER     ?? "winona",
  password: process.env.PGPASSWORD ?? "winona",
  database: process.env.PGDATABASE ?? "winona_dw",
});

const username = process.env.SUPERUSER_USERNAME ?? "superuser";
const password = process.env.SUPERUSER_PASSWORD ?? "changeme";

async function seed() {
  await pool.query(`
    CREATE SCHEMA IF NOT EXISTS auth;
    CREATE TABLE IF NOT EXISTS auth.users (
      id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
      username      TEXT        NOT NULL UNIQUE,
      password_hash TEXT        NOT NULL,
      role          TEXT        NOT NULL CHECK (role IN ('superuser','admin','viewer')),
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  const existing = await pool.query(
    "SELECT id FROM auth.users WHERE username = $1",
    [username],
  );
  if (existing.rows.length > 0) {
    console.log(`Superuser '${username}' already exists — skipping seed.`);
    await pool.end();
    return;
  }

  const hash = await bcrypt.hash(password, 12);
  await pool.query(
    "INSERT INTO auth.users (username, password_hash, role) VALUES ($1, $2, 'superuser')",
    [username, hash],
  );
  console.log(`Superuser '${username}' created.`);
  await pool.end();
}

seed().catch((e) => { console.error(e); process.exit(1); });
