import logging
import tempfile
import duckdb
from pathlib import Path
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from winona.loader import ingest_product_export, ingest_sale_history
from winona.loader import _run_dbt
from ..config import get_conn_str
from ..db import get_conn

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/loader", tags=["loader"])

OUTLET_NAMES = ["rozelle", "avalon", "manly"]


@router.post("/run-dbt")
async def run_dbt():
    try:
        _run_dbt()
    except Exception as e:
        log.exception("dbt run error")
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok"}


@router.post("/product-export")
async def product_export(file: UploadFile = File(...)):
    tmp_path = None
    try:
        conn_str = get_conn_str()
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
        ingest_product_export(filepath=tmp_path, conn_str=conn_str)
        _run_dbt(select=["path:models/raw/product", "path:models/stg/product", "path:models/mart/product"])
    except Exception as e:
        log.exception("loader error")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)
    return {"status": "ok"}


@router.post("/sale-history/preview")
async def sale_history_preview(file: UploadFile = File(...), outlet: str = Form(...)):
    if outlet not in OUTLET_NAMES:
        raise HTTPException(status_code=422, detail=f"outlet must be one of {OUTLET_NAMES}")
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        conn = get_conn()

        file_row = conn.execute(f"""
            SELECT MIN(date), MAX(date), COUNT(*)
            FROM read_csv('{tmp_path}', normalize_names=true)
            WHERE line_type = 'Sale'
        """).fetchone()
        file_min, file_max, row_count = file_row

        try:
            existing_row = conn.execute(f"""
                SELECT MIN(date), MAX(date)
                FROM wh.raw.sale_history_dump
                WHERE outlet = '{outlet}'
            """).fetchone()
            existing_min, existing_max = existing_row

            dup_row = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT receipt_number::VARCHAR
                    FROM read_csv('{tmp_path}', normalize_names=true)
                    WHERE line_type = 'Sale'
                ) f
                WHERE f.receipt_number IN (
                    SELECT DISTINCT receipt_number
                    FROM wh.raw.sale_history_dump
                    WHERE outlet = '{outlet}' AND line_type = 'Sale'
                )
            """).fetchone()
            duplicate_count = int(dup_row[0]) if dup_row else 0

            # All rows (all line types) whose receipt is already in the warehouse
            dup_rows_row = conn.execute(f"""
                SELECT COUNT(*)
                FROM read_csv('{tmp_path}', normalize_names=true)
                WHERE receipt_number::VARCHAR IN (
                    SELECT DISTINCT receipt_number
                    FROM wh.raw.sale_history_dump
                    WHERE outlet = '{outlet}' AND line_type = 'Sale'
                )
            """).fetchone()
            duplicate_rows = int(dup_rows_row[0]) if dup_rows_row else 0

        except duckdb.CatalogException:
            existing_min, existing_max, duplicate_count, duplicate_rows = None, None, 0, 0

        def fmt(dt):
            return dt.isoformat() if dt is not None else None

        return {
            "file_min": fmt(file_min),
            "file_max": fmt(file_max),
            "row_count": int(row_count),
            "existing_min": fmt(existing_min),
            "existing_max": fmt(existing_max),
            "duplicate_count": duplicate_count,
            "duplicate_rows": duplicate_rows,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("preview error")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)


@router.post("/sale-history")
async def sale_history(
    file: UploadFile = File(...),
    outlet: str = Form(...),
    deduplicate: bool = Form(False),
):
    if outlet not in OUTLET_NAMES:
        raise HTTPException(status_code=422, detail=f"outlet must be one of {OUTLET_NAMES}")
    tmp_path = None
    filtered_path = None
    try:
        conn_str = get_conn_str()
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        ingest_path = tmp_path

        if deduplicate:
            filtered_path = tmp_path + "_dedup.csv"
            conn = get_conn()
            try:
                conn.execute(f"""
                    COPY (
                        SELECT *
                        FROM read_csv('{tmp_path}', normalize_names=true)
                        WHERE receipt_number::VARCHAR NOT IN (
                            SELECT DISTINCT receipt_number
                            FROM wh.raw.sale_history_dump
                            WHERE outlet = '{outlet}' AND line_type = 'Sale'
                        )
                    ) TO '{filtered_path}' (HEADER, DELIMITER ',')
                """)
                ingest_path = filtered_path
            except duckdb.CatalogException:
                # No existing data — nothing to filter, use original file
                pass

        ingest_sale_history(filepath=ingest_path, conn_str=conn_str, outlet=outlet)
        _run_dbt()
    except Exception as e:
        log.exception("loader error")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)
        if filtered_path:
            Path(filtered_path).unlink(missing_ok=True)
    return {"status": "ok"}
