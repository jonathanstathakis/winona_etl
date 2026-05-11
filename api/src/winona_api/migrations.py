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
    # drop old outlet-scoped planogram tables before recreating with new schema
    _ddl(conn, "DROP TABLE IF EXISTS planogram.placement CASCADE")
    _ddl(conn, "DROP TABLE IF EXISTS planogram.planogram CASCADE")
    # layout: top-level named snapshot across all outlets (one is_current at a time)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.layout (
            id         TEXT        PRIMARY KEY,
            name       TEXT        NOT NULL,
            is_current BOOLEAN     NOT NULL DEFAULT false,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    # planogram: bay-level layout within a layout version
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.planogram (
            id         TEXT        PRIMARY KEY,
            layout_id  TEXT        NOT NULL REFERENCES planogram.layout(id) ON DELETE CASCADE,
            bay_id     TEXT        NOT NULL REFERENCES planogram.bay(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE (layout_id, bay_id)
        )
    """)
    # planogram_shelf: shelf config per planogram (shelves live under planogram, not bay)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.planogram_shelf (
            planogram_id TEXT    NOT NULL REFERENCES planogram.planogram(id) ON DELETE CASCADE,
            shelf_index  INTEGER NOT NULL,
            slot_count   INTEGER NOT NULL DEFAULT 7,
            PRIMARY KEY (planogram_id, shelf_index)
        )
    """)
    # placement: product at a slot within a planogram (bay implied by planogram)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.placement (
            planogram_id TEXT    NOT NULL REFERENCES planogram.planogram(id) ON DELETE CASCADE,
            shelf_index  INTEGER NOT NULL,
            slot_index   INTEGER NOT NULL,
            sku          TEXT    NOT NULL,
            PRIMARY KEY (planogram_id, shelf_index, slot_index)
        )
    """)
    _ddl(conn, """
        CREATE TABLE IF NOT EXISTS planogram.floor_plan (
            outlet   TEXT PRIMARY KEY,
            vertices TEXT NOT NULL DEFAULT '[]'
        )
    """)
    _ddl(conn, "ALTER TABLE planogram.bay ADD COLUMN IF NOT EXISTS floor_x FLOAT")
    _ddl(conn, "ALTER TABLE planogram.bay ADD COLUMN IF NOT EXISTS floor_y FLOAT")
    _ddl(conn, "ALTER TABLE planogram.bay ADD COLUMN IF NOT EXISTS floor_w FLOAT NOT NULL DEFAULT 3")
    _ddl(conn, "ALTER TABLE planogram.bay ADD COLUMN IF NOT EXISTS floor_h FLOAT NOT NULL DEFAULT 2")
    _ddl(conn, "ALTER TABLE planogram.bay ADD COLUMN IF NOT EXISTS floor_rotation INT NOT NULL DEFAULT 0")
    log.info("Migrations complete.")
