import duckdb
from fastapi import APIRouter
from ..db import get_conn

router = APIRouter(prefix="/api/mart", tags=["mart"])


def _rows_to_dicts(conn, query: str) -> list[dict]:
    rel = conn.execute(query)
    cols = [desc[0] for desc in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


@router.get("/product-catalog")
def product_catalog():
    conn = get_conn()
    return _rows_to_dicts(conn, """
        select * from wh.mart.mart_item_curr
        order by name
    """)


@router.get("/item-by-tag")
def item_by_tag():
    conn = get_conn()
    return _rows_to_dicts(conn, """
        select * from wh.mart.mart_item_by_tag
        order by name, tag
    """)


@router.get("/wine")
def wine():
    conn = get_conn()
    return _rows_to_dicts(conn, """
        select * from wh.mart.mart_wine_curr
        order by name
    """)


@router.get("/data-health")
def data_health():
    conn = get_conn()

    product_exports = _rows_to_dicts(conn, """
        select distinct export_timestamp
        from wh.raw.product_export_dump
        order by export_timestamp desc
    """)

    try:
        sale_history = _rows_to_dicts(conn, """
            select
                outlet,
                min(date) as earliest_sale,
                max(date) as latest_sale
            from wh.raw.sale_history_dump
            group by outlet
            order by outlet
        """)
    except duckdb.CatalogException:
        sale_history = []

    return {
        "product_exports": product_exports,
        "sale_history_by_outlet": sale_history,
    }

@router.get("/item-detail")
def item_detail(item_id: str):
    conn: duckdb.DuckDBPYConnection = get_conn()

    query = "select * from wh.mart.mart_item_curr where id = ?"
    rel = conn.execute(query, [item_id])
    cols = [desc[0] for desc in rel.description]
    return dict(zip(cols, rel.fetchone()))