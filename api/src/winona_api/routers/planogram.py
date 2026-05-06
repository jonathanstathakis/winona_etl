import uuid
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..db import get_conn

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/planogram", tags=["planogram"])

OUTLETS = {"rozelle", "avalon", "manly"}


# --- helpers ---

def _esc(s: str) -> str:
    return s.replace("'", "''")


def _pg_exec(conn, sql: str) -> None:
    escaped = sql.replace("'", "''")
    conn.execute(f"CALL postgres_execute('wh', '{escaped}')")


def _rows(conn, query: str) -> list[dict]:
    rel = conn.execute(query)
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _validate_outlet(outlet: str) -> None:
    if outlet not in OUTLETS:
        raise HTTPException(status_code=422, detail=f"outlet must be one of {sorted(OUTLETS)}")


# --- models ---

class ShelfConfig(BaseModel):
    shelf_index: int
    slot_count: int


class BayIn(BaseModel):
    outlet: str
    name: str = ""
    description: str = ""
    shelves: list[ShelfConfig]


class BayOut(BaseModel):
    id: str
    outlet: str
    name: str
    description: str
    shelves: list[ShelfConfig]


class PlanogramIn(BaseModel):
    outlet: str
    name: str
    description: str = ""


class PlanogramOut(BaseModel):
    id: str
    outlet: str
    name: str
    description: str
    status: str
    created_at: str
    updated_at: str


class PlacementIn(BaseModel):
    bay_id: str
    shelf_index: int
    slot_index: int
    sku: str


class PlanogramDetail(PlanogramOut):
    placements: list[PlacementIn]


# --- bay endpoints ---

def _next_sort_order(conn, outlet: str) -> int:
    rows = _rows(conn, f"SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM wh.planogram.bay WHERE outlet = '{_esc(outlet)}'")
    return int(rows[0]["n"])


def _load_bays(conn, outlet: str) -> list[BayOut]:
    rows = _rows(conn, f"""
        SELECT b.id, b.outlet, b.name, b.description,
               bs.shelf_index, bs.slot_count
        FROM wh.planogram.bay b
        LEFT JOIN wh.planogram.bay_shelf bs ON bs.bay_id = b.id
        WHERE b.outlet = '{_esc(outlet)}'
        ORDER BY b.sort_order, bs.shelf_index
    """)
    bays: dict[str, BayOut] = {}
    for row in rows:
        bid = row["id"]
        if bid not in bays:
            bays[bid] = BayOut(
                id=bid, outlet=row["outlet"],
                name=row["name"], description=row["description"],
                shelves=[],
            )
        if row["shelf_index"] is not None:
            bays[bid].shelves.append(
                ShelfConfig(shelf_index=row["shelf_index"], slot_count=row["slot_count"])
            )
    return list(bays.values())


@router.get("/bays")
def list_bays(outlet: str) -> list[BayOut]:
    _validate_outlet(outlet)
    return _load_bays(get_conn(), outlet)


@router.post("/bays", status_code=201)
def create_bay(body: BayIn) -> BayOut:
    _validate_outlet(body.outlet)
    conn = get_conn()
    bay_id = str(uuid.uuid4())
    sort_order = _next_sort_order(conn, body.outlet)
    _pg_exec(conn, f"""
        INSERT INTO planogram.bay (id, outlet, name, description, sort_order)
        VALUES ('{bay_id}', '{_esc(body.outlet)}', '{_esc(body.name)}', '{_esc(body.description)}', {sort_order})
    """)
    for s in body.shelves:
        _pg_exec(conn, f"""
            INSERT INTO planogram.bay_shelf (bay_id, shelf_index, slot_count)
            VALUES ('{bay_id}', {s.shelf_index}, {s.slot_count})
        """)
    return BayOut(id=bay_id, outlet=body.outlet, name=body.name,
                  description=body.description, shelves=body.shelves)


@router.put("/bays/{bay_id}")
def update_bay(bay_id: str, body: BayIn) -> BayOut:
    _validate_outlet(body.outlet)
    conn = get_conn()
    _pg_exec(conn, f"""
        UPDATE planogram.bay
        SET outlet = '{_esc(body.outlet)}', name = '{_esc(body.name)}', description = '{_esc(body.description)}'
        WHERE id = '{_esc(bay_id)}'
    """)
    _pg_exec(conn, f"DELETE FROM planogram.bay_shelf WHERE bay_id = '{_esc(bay_id)}'")
    for s in body.shelves:
        _pg_exec(conn, f"""
            INSERT INTO planogram.bay_shelf (bay_id, shelf_index, slot_count)
            VALUES ('{_esc(bay_id)}', {s.shelf_index}, {s.slot_count})
        """)
    return BayOut(id=bay_id, outlet=body.outlet, name=body.name,
                  description=body.description, shelves=body.shelves)


@router.delete("/bays/{bay_id}", status_code=204)
def delete_bay(bay_id: str) -> None:
    _pg_exec(get_conn(), f"DELETE FROM planogram.bay WHERE id = '{_esc(bay_id)}'")


class ReorderBaysIn(BaseModel):
    outlet: str
    ids: list[str]


@router.post("/bays/reorder", status_code=204)
def reorder_bays(body: ReorderBaysIn) -> None:
    _validate_outlet(body.outlet)
    conn = get_conn()
    for i, bay_id in enumerate(body.ids):
        _pg_exec(conn, f"""
            UPDATE planogram.bay SET sort_order = {i}
            WHERE id = '{_esc(bay_id)}' AND outlet = '{_esc(body.outlet)}'
        """)


# --- planogram endpoints ---

@router.get("/planograms")
def list_planograms(outlet: str) -> list[PlanogramOut]:
    _validate_outlet(outlet)
    rows = _rows(get_conn(), f"""
        SELECT id, outlet, name, description, status,
               created_at::text AS created_at, updated_at::text AS updated_at
        FROM wh.planogram.planogram
        WHERE outlet = '{_esc(outlet)}'
        ORDER BY created_at DESC
    """)
    return [PlanogramOut(**r) for r in rows]


@router.post("/planograms", status_code=201)
def create_planogram(body: PlanogramIn) -> PlanogramOut:
    _validate_outlet(body.outlet)
    conn = get_conn()
    plan_id = str(uuid.uuid4())
    _pg_exec(conn, f"""
        INSERT INTO planogram.planogram (id, outlet, name, description, status)
        VALUES ('{plan_id}', '{_esc(body.outlet)}', '{_esc(body.name)}', '{_esc(body.description)}', 'draft')
    """)
    rows = _rows(conn, f"""
        SELECT id, outlet, name, description, status,
               created_at::text AS created_at, updated_at::text AS updated_at
        FROM wh.planogram.planogram WHERE id = '{plan_id}'
    """)
    return PlanogramOut(**rows[0])


@router.get("/planograms/{plan_id}")
def get_planogram(plan_id: str) -> PlanogramDetail:
    conn = get_conn()
    rows = _rows(conn, f"""
        SELECT id, outlet, name, description, status,
               created_at::text AS created_at, updated_at::text AS updated_at
        FROM wh.planogram.planogram WHERE id = '{_esc(plan_id)}'
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Planogram not found")
    placements = _rows(conn, f"""
        SELECT bay_id, shelf_index, slot_index, sku
        FROM wh.planogram.placement
        WHERE planogram_id = '{_esc(plan_id)}'
    """)
    return PlanogramDetail(**rows[0], placements=[PlacementIn(**p) for p in placements])


@router.post("/planograms/{plan_id}/save")
def save_planogram(plan_id: str, placements: list[PlacementIn]):
    conn = get_conn()
    _pg_exec(conn, f"DELETE FROM planogram.placement WHERE planogram_id = '{_esc(plan_id)}'")
    if placements:
        values = ", ".join(
            f"('{_esc(plan_id)}', '{_esc(p.bay_id)}', {p.shelf_index}, {p.slot_index}, '{_esc(p.sku)}')"
            for p in placements
        )
        _pg_exec(conn, f"""
            INSERT INTO planogram.placement (planogram_id, bay_id, shelf_index, slot_index, sku)
            VALUES {values}
        """)
    _pg_exec(conn, f"UPDATE planogram.planogram SET updated_at = now() WHERE id = '{_esc(plan_id)}'")
    return {"status": "ok"}


@router.post("/planograms/{plan_id}/activate")
def activate_planogram(plan_id: str) -> PlanogramOut:
    conn = get_conn()
    rows = _rows(conn, f"SELECT outlet FROM wh.planogram.planogram WHERE id = '{_esc(plan_id)}'")
    if not rows:
        raise HTTPException(status_code=404, detail="Planogram not found")
    outlet = rows[0]["outlet"]
    # archive current active
    _pg_exec(conn, f"""
        UPDATE planogram.planogram
        SET status = 'archived', updated_at = now()
        WHERE outlet = '{_esc(outlet)}' AND status = 'active'
    """)
    # activate this one
    _pg_exec(conn, f"""
        UPDATE planogram.planogram
        SET status = 'active', updated_at = now()
        WHERE id = '{_esc(plan_id)}'
    """)
    updated = _rows(conn, f"""
        SELECT id, outlet, name, description, status,
               created_at::text AS created_at, updated_at::text AS updated_at
        FROM wh.planogram.planogram WHERE id = '{_esc(plan_id)}'
    """)
    return PlanogramOut(**updated[0])


class RenamePlanogramIn(BaseModel):
    name: str


@router.patch("/planograms/{plan_id}")
def rename_planogram(plan_id: str, body: RenamePlanogramIn) -> PlanogramOut:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="name is required")
    conn = get_conn()
    _pg_exec(conn, f"""
        UPDATE planogram.planogram SET name = '{_esc(name)}', updated_at = now()
        WHERE id = '{_esc(plan_id)}'
    """)
    rows = _rows(conn, f"""
        SELECT id, outlet, name, description, status,
               created_at::text AS created_at, updated_at::text AS updated_at
        FROM wh.planogram.planogram WHERE id = '{_esc(plan_id)}'
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Planogram not found")
    return PlanogramOut(**rows[0])


@router.delete("/planograms/{plan_id}", status_code=204)
def delete_planogram(plan_id: str) -> None:
    _pg_exec(get_conn(), f"DELETE FROM planogram.planogram WHERE id = '{_esc(plan_id)}'")
