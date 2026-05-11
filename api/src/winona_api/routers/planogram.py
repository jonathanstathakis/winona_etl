import uuid
import json
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


class PlacementIn(BaseModel):
    shelf_index: int
    slot_index: int
    sku: str


class LayoutOut(BaseModel):
    id: str
    name: str
    is_current: bool
    created_at: str


class OutletSummary(BaseModel):
    outlet: str
    bay_count: int
    planogram_count: int


class LayoutDetail(LayoutOut):
    outlets: list[OutletSummary]


class LayoutIn(BaseModel):
    name: str


class LayoutPatchIn(BaseModel):
    name: str | None = None
    is_current: bool | None = None


class BayPlanogramSummary(BaseModel):
    bay_id: str
    bay_name: str
    has_planogram: bool
    shelf_count: int


class BayPlanogramDetail(BaseModel):
    planogram_id: str | None
    bay_id: str
    layout_id: str
    shelves: list[ShelfConfig]
    placements: list[PlacementIn]


# --- bay endpoints (physical fixtures, unchanged) ---

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


# --- layout endpoints ---

def _load_layout(conn, layout_id: str) -> LayoutOut:
    rows = _rows(conn, f"""
        SELECT id, name, is_current, created_at::text AS created_at
        FROM wh.planogram.layout WHERE id = '{_esc(layout_id)}'
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Layout not found")
    return LayoutOut(**rows[0])


@router.get("/layouts")
def list_layouts() -> list[LayoutOut]:
    rows = _rows(get_conn(), """
        SELECT id, name, is_current, created_at::text AS created_at
        FROM wh.planogram.layout
        ORDER BY created_at DESC
    """)
    return [LayoutOut(**r) for r in rows]


@router.post("/layouts", status_code=201)
def create_layout(body: LayoutIn) -> LayoutOut:
    conn = get_conn()
    layout_id = str(uuid.uuid4())
    _pg_exec(conn, f"""
        INSERT INTO planogram.layout (id, name, is_current)
        VALUES ('{layout_id}', '{_esc(body.name)}', false)
    """)
    return _load_layout(conn, layout_id)


@router.get("/layouts/{layout_id}")
def get_layout(layout_id: str) -> LayoutDetail:
    conn = get_conn()
    base = _load_layout(conn, layout_id)
    outlet_rows = _rows(conn, f"""
        SELECT b.outlet,
               COUNT(DISTINCT b.id) AS bay_count,
               COUNT(DISTINCT p.id) AS planogram_count
        FROM wh.planogram.bay b
        LEFT JOIN wh.planogram.planogram p
               ON p.bay_id = b.id AND p.layout_id = '{_esc(layout_id)}'
        GROUP BY b.outlet
        ORDER BY b.outlet
    """)
    outlets = [
        OutletSummary(
            outlet=r["outlet"],
            bay_count=int(r["bay_count"]),
            planogram_count=int(r["planogram_count"]),
        )
        for r in outlet_rows
    ]
    return LayoutDetail(**base.model_dump(), outlets=outlets)


@router.patch("/layouts/{layout_id}")
def update_layout(layout_id: str, body: LayoutPatchIn) -> LayoutOut:
    conn = get_conn()
    _load_layout(conn, layout_id)  # 404 if missing
    if body.name is not None:
        name = body.name.strip()
        if not name:
            raise HTTPException(status_code=422, detail="name is required")
        _pg_exec(conn, f"UPDATE planogram.layout SET name = '{_esc(name)}' WHERE id = '{_esc(layout_id)}'")
    if body.is_current is True:
        _pg_exec(conn, "UPDATE planogram.layout SET is_current = false")
        _pg_exec(conn, f"UPDATE planogram.layout SET is_current = true WHERE id = '{_esc(layout_id)}'")
    return _load_layout(conn, layout_id)


@router.delete("/layouts/{layout_id}", status_code=204)
def delete_layout(layout_id: str) -> None:
    _pg_exec(get_conn(), f"DELETE FROM planogram.layout WHERE id = '{_esc(layout_id)}'")


# --- bay planogram endpoints (bay-level layout within a layout version) ---

@router.get("/layouts/{layout_id}/outlets/{outlet}/bays")
def list_outlet_bays(layout_id: str, outlet: str) -> list[BayPlanogramSummary]:
    _validate_outlet(outlet)
    conn = get_conn()
    rows = _rows(conn, f"""
        SELECT b.id AS bay_id, b.name AS bay_name,
               p.id IS NOT NULL AS has_planogram,
               COUNT(ps.shelf_index) AS shelf_count
        FROM wh.planogram.bay b
        LEFT JOIN wh.planogram.planogram p
               ON p.bay_id = b.id AND p.layout_id = '{_esc(layout_id)}'
        LEFT JOIN wh.planogram.planogram_shelf ps ON ps.planogram_id = p.id
        WHERE b.outlet = '{_esc(outlet)}'
        GROUP BY b.id, b.name, b.sort_order, p.id
        ORDER BY b.sort_order
    """)
    return [
        BayPlanogramSummary(
            bay_id=r["bay_id"],
            bay_name=r["bay_name"],
            has_planogram=bool(r["has_planogram"]),
            shelf_count=int(r["shelf_count"]),
        )
        for r in rows
    ]


@router.get("/layouts/{layout_id}/bays/{bay_id}")
def get_bay_planogram(layout_id: str, bay_id: str) -> BayPlanogramDetail:
    conn = get_conn()
    p_rows = _rows(conn, f"""
        SELECT id FROM wh.planogram.planogram
        WHERE layout_id = '{_esc(layout_id)}' AND bay_id = '{_esc(bay_id)}'
    """)

    if p_rows:
        planogram_id = p_rows[0]["id"]
        shelf_rows = _rows(conn, f"""
            SELECT shelf_index, slot_count FROM wh.planogram.planogram_shelf
            WHERE planogram_id = '{_esc(planogram_id)}'
            ORDER BY shelf_index
        """)
        placement_rows = _rows(conn, f"""
            SELECT shelf_index, slot_index, sku FROM wh.planogram.placement
            WHERE planogram_id = '{_esc(planogram_id)}'
        """)
        shelves = [ShelfConfig(**r) for r in shelf_rows]
        placements = [PlacementIn(**r) for r in placement_rows]
    else:
        planogram_id = None
        # seed shelf config from physical bay template
        shelf_rows = _rows(conn, f"""
            SELECT shelf_index, slot_count FROM wh.planogram.bay_shelf
            WHERE bay_id = '{_esc(bay_id)}'
            ORDER BY shelf_index
        """)
        shelves = [ShelfConfig(**r) for r in shelf_rows]
        placements = []

    return BayPlanogramDetail(
        planogram_id=planogram_id,
        bay_id=bay_id,
        layout_id=layout_id,
        shelves=shelves,
        placements=placements,
    )


class SaveBayPlanogramIn(BaseModel):
    shelves: list[ShelfConfig]
    placements: list[PlacementIn]


@router.post("/layouts/{layout_id}/bays/{bay_id}/save")
def save_bay_planogram(layout_id: str, bay_id: str, body: SaveBayPlanogramIn) -> BayPlanogramDetail:
    conn = get_conn()
    p_rows = _rows(conn, f"""
        SELECT id FROM wh.planogram.planogram
        WHERE layout_id = '{_esc(layout_id)}' AND bay_id = '{_esc(bay_id)}'
    """)
    if p_rows:
        planogram_id = p_rows[0]["id"]
    else:
        planogram_id = str(uuid.uuid4())
        _pg_exec(conn, f"""
            INSERT INTO planogram.planogram (id, layout_id, bay_id)
            VALUES ('{planogram_id}', '{_esc(layout_id)}', '{_esc(bay_id)}')
        """)

    _pg_exec(conn, f"DELETE FROM planogram.planogram_shelf WHERE planogram_id = '{_esc(planogram_id)}'")
    for s in body.shelves:
        _pg_exec(conn, f"""
            INSERT INTO planogram.planogram_shelf (planogram_id, shelf_index, slot_count)
            VALUES ('{_esc(planogram_id)}', {s.shelf_index}, {s.slot_count})
        """)

    _pg_exec(conn, f"DELETE FROM planogram.placement WHERE planogram_id = '{_esc(planogram_id)}'")
    for p in body.placements:
        _pg_exec(conn, f"""
            INSERT INTO planogram.placement (planogram_id, shelf_index, slot_index, sku)
            VALUES ('{_esc(planogram_id)}', {p.shelf_index}, {p.slot_index}, '{_esc(p.sku)}')
        """)

    return get_bay_planogram(layout_id, bay_id)


# --- floor plan endpoints (outlet-level, shared across layout versions) ---

class Vertex(BaseModel):
    x: float
    y: float


class FloorBayOut(BaseModel):
    id: str
    name: str
    floor_x: float | None
    floor_y: float | None
    floor_w: float
    floor_h: float
    floor_rotation: int
    color: str


class FloorPlanOut(BaseModel):
    outlet: str
    vertices: list[Vertex]
    bays: list[FloorBayOut]


class FloorPlanIn(BaseModel):
    vertices: list[Vertex]


class FloorPositionIn(BaseModel):
    floor_x: float | None = None
    floor_y: float | None = None
    floor_w: float = 3
    floor_h: float = 2
    floor_rotation: int = 0
    color: str = "#2E5FA3"


@router.get("/floor-plan/{outlet}")
def get_floor_plan(outlet: str) -> FloorPlanOut:
    _validate_outlet(outlet)
    conn = get_conn()
    rows = _rows(conn, f"SELECT vertices FROM wh.planogram.floor_plan WHERE outlet = '{_esc(outlet)}'")
    vertices = [Vertex(**v) for v in json.loads(rows[0]["vertices"] if rows else "[]")]
    bay_rows = _rows(conn, f"""
        SELECT id, name, floor_x, floor_y, floor_w, floor_h, floor_rotation, color
        FROM wh.planogram.bay
        WHERE outlet = '{_esc(outlet)}'
        ORDER BY sort_order
    """)
    bays = [FloorBayOut(**r) for r in bay_rows]
    return FloorPlanOut(outlet=outlet, vertices=vertices, bays=bays)


@router.put("/floor-plan/{outlet}")
def save_floor_plan(outlet: str, body: FloorPlanIn) -> FloorPlanOut:
    _validate_outlet(outlet)
    conn = get_conn()
    vertices_json = _esc(json.dumps([v.model_dump() for v in body.vertices]))
    _pg_exec(conn, f"""
        INSERT INTO planogram.floor_plan (outlet, vertices)
        VALUES ('{_esc(outlet)}', '{vertices_json}')
        ON CONFLICT (outlet) DO UPDATE SET vertices = EXCLUDED.vertices
    """)
    return get_floor_plan(outlet)


@router.put("/bays/{bay_id}/position", status_code=204)
def update_bay_position(bay_id: str, body: FloorPositionIn) -> None:
    floor_x = "NULL" if body.floor_x is None else str(body.floor_x)
    floor_y = "NULL" if body.floor_y is None else str(body.floor_y)
    _pg_exec(get_conn(), f"""
        UPDATE planogram.bay
        SET floor_x = {floor_x}, floor_y = {floor_y},
            floor_w = {body.floor_w}, floor_h = {body.floor_h},
            floor_rotation = {body.floor_rotation},
            color = '{_esc(body.color)}'
        WHERE id = '{_esc(bay_id)}'
    """)
