import logging
from .db import get_conn

log = logging.getLogger(__name__)


def _ddl(conn, sql: str) -> None:
    escaped = sql.replace("'", "''")
    conn.execute(f"CALL postgres_execute('wh', '{escaped}')")


def run_migrations() -> None:
    conn = get_conn()
    log.info("Running migrations...")
    _ddl(conn, "CREATE SCHEMA IF NOT EXISTS planogram")
    _ddl(conn, "DROP TABLE IF EXISTS planogram.layout")
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.bay (
            id          TEXT        PRIMARY KEY,
            outlet      TEXT        NOT NULL,
            name        TEXT        NOT NULL,
            description TEXT        NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    _ddl(conn, "ALTER TABLE planogram.bay ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0")
    _ddl(conn, """
        UPDATE planogram.bay b
        SET sort_order = sub.rn
        FROM (
            SELECT id, ROW_NUMBER() OVER (PARTITION BY outlet ORDER BY created_at, id) - 1 AS rn
            FROM planogram.bay
            WHERE sort_order = 0
        ) sub
        WHERE b.id = sub.id
    """)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.bay_shelf (
            bay_id      TEXT        NOT NULL REFERENCES planogram.bay(id) ON DELETE CASCADE,
            shelf_index INTEGER     NOT NULL,
            slot_count  INTEGER     NOT NULL,
            PRIMARY KEY (bay_id, shelf_index)
        )
    """)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.planogram (
            id          TEXT        PRIMARY KEY,
            outlet      TEXT        NOT NULL,
            name        TEXT        NOT NULL,
            description TEXT        NOT NULL,
            status      TEXT        NOT NULL DEFAULT 'draft',
            created_at  TIMESTAMPTZ DEFAULT now(),
            updated_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.placement (
            planogram_id TEXT       NOT NULL REFERENCES planogram.planogram(id) ON DELETE CASCADE,
            bay_id       TEXT       NOT NULL REFERENCES planogram.bay(id) ON DELETE CASCADE,
            shelf_index  INTEGER    NOT NULL,
            slot_index   INTEGER    NOT NULL,
            sku          TEXT       NOT NULL,
            PRIMARY KEY (planogram_id, bay_id, shelf_index, slot_index)
        )
    """)
    log.info("Migrations complete.")
