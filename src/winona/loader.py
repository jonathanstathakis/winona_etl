import os
from pathlib import Path
import httpx
import pandas as pd
import typer
import duckdb as db
from typing import Annotated, Optional
from .db_utils import attach_target_db, generate_connection
from .config import get_conn_str

_ETL_URL = os.environ.get("ETL_URL", "http://etl:8001")


def _run_dbt(select: list[str] | None = None):
    print("triggering dbt run via ETL service..")
    with httpx.Client(timeout=300) as client:
        resp = client.post(f"{_ETL_URL}/run", json={"select": select})
        resp.raise_for_status()
    print("dbt run complete.")

app = typer.Typer(help="Ingest Lightspeed exports into the data warehouse.")


def get_creation_time(source_path):
    ctime = Path(source_path).stat()[9]
    return ctime


def load_product_export_df(filepath) -> pd.DataFrame:
    print(f"reading file at {filepath}..")
    df = pd.read_csv(filepath)
    df["export_timestamp"] = get_creation_time(filepath)
    return df


def create_product_export_dump(conn):
    query = """
CREATE TABLE if NOT EXISTS raw.product_export_dump (
    pk varchar primary key, -- hash of id and export timestamp
    id VARCHAR,
    handle VARCHAR,
    sku VARCHAR,
    composite_name VARCHAR,
    composite_sku VARCHAR,
    composite_quantity DOUBLE,
    name VARCHAR,
    description VARCHAR,
    product_category VARCHAR,
    variant_option_one_name VARCHAR,
    variant_option_one_value VARCHAR,
    variant_option_two_name VARCHAR,
    variant_option_two_value VARCHAR,
    variant_option_three_name VARCHAR,
    variant_option_three_value VARCHAR,
    tags VARCHAR,
    supply_price DOUBLE,
    retail_price DOUBLE,
    loyalty_value DOUBLE,
    loyalty_value_default VARCHAR,
    tax_name VARCHAR,
    tax_value DOUBLE,
    account_code VARCHAR,
    account_code_purchase VARCHAR,
    brand_name VARCHAR,
    supplier_name VARCHAR,
    supplier_code VARCHAR,
    active DOUBLE,
    track_inventory DOUBLE,
    inventory_winona_avalon DOUBLE,
    reorder_point_winona_avalon DOUBLE,
    restock_level_winona_avalon DOUBLE,
    inventory_winona_manly DOUBLE,
    reorder_point_winona_manly DOUBLE,
    restock_level_winona_manly DOUBLE,
    inventory_winona_rozelle DOUBLE,
    reorder_point_winona_rozelle DOUBLE,
    restock_level_winona_rozelle DOUBLE,
    inventory_winona_warehouse DOUBLE,
    reorder_point_winona_warehouse DOUBLE,
    restock_level_winona_warehouse DOUBLE,
    export_timestamp bigint
);
    """
    conn.execute(query)


def insert_into_product_export_dump(conn, df):
    query = """
    insert into raw.product_export_dump
    with pk_hash as (
        select
        hash(id::varchar, export_timestamp::varchar, composite_sku::varchar) as pk,
        *
        from df)
    select * from pk_hash
    """
    print(f"before insert, count={conn.execute('select count(*) from raw.product_export_dump').fetchall()}")
    print("inserting new file into raw.product_export_dump..")
    conn.execute(query)
    print(f"new count={conn.execute('select count(*) from raw.product_export_dump').fetchall()}")


def create_raw_sale_history(filepath, conn, outlet: str):
    print(f"reading file at {filepath}..")
    ctime = get_creation_time(filepath)
    filename = Path(filepath).stem
    print(f"reading {filepath} into staging table..")
    query = f"""
    create or replace table raw.raw_sale_history as
    select
        row_number() over () as insert_idx,
        {ctime} as export_timestamp,
        '{filename}' as export_filename,
        '{outlet}' as outlet,
        hash(row_number() over (), receipt_number, details, date, outlet, line_type) as sale_history_line_id,
        *
    from read_csv('{filepath}', normalize_names=true);
    """
    conn.execute(query)


def create_sale_history_dump(conn):
    query = """
CREATE TABLE if NOT EXISTS raw.sale_history_dump (
    sale_history_line_id varchar primary key,
    insert_index int,
    export_timestamp bigint,
    export_filename varchar,
    outlet varchar,
    date timestamp,
    receipt_number varchar,
    line_type varchar,
    customer_code varchar,
    customer_name varchar,
    note varchar,
    quantity double,
    subtotal double,
    sales_tax double,
    discount double,
    loyalty double,
    total double,
    paid double,
    details varchar,
    register varchar,
    _user varchar,
    status varchar,
    sku varchar,
    accountcodesale varchar,
    accountcodepurchase varchar,
    state varchar,
    attributes varchar
);
    """
    conn.execute(query)


def insert_into_sale_history_dump(conn):
    query = """
    insert into raw.sale_history_dump
    select
        sale_history_line_id, insert_idx, export_timestamp, export_filename, outlet,
        date, receipt_number, line_type, customer_code, customer_name, note, quantity,
        subtotal, sales_tax, discount, loyalty, total, paid, details, register, _user,
        status, sku, accountcodesale, accountcodepurchase, state, attributes
    from raw.raw_sale_history
    """
    print(f"before insert, count={conn.execute('select count(*) from raw.sale_history_dump').fetchall()}")
    print("inserting new file into sale_history_dump..")
    conn.execute(query)
    print(f"new count={conn.execute('select count(*) from raw.sale_history_dump').fetchall()}")


def ingest_sale_history(filepath, conn_str, outlet):
    conn = generate_connection()
    attach_target_db(conn, conn_str)
    conn.execute("create schema if not exists raw")
    create_raw_sale_history(filepath, conn, outlet)
    create_sale_history_dump(conn)
    insert_into_sale_history_dump(conn)


def ingest_product_export(filepath, conn_str):
    df = load_product_export_df(filepath)
    conn = generate_connection()
    attach_target_db(conn, conn_str)
    conn.execute("create schema if not exists raw")
    create_product_export_dump(conn)
    insert_into_product_export_dump(conn, df)


@app.command()
def sale_history(
    outlet: str,
    source_path: Annotated[str, typer.Option()],
    conn_str: Annotated[Optional[str], typer.Option(help="Full connection string. Overrides WINONA_DATABASE_URL.")] = None,
    delete_source: Annotated[bool, typer.Option(help="If true, delete source file.")] = False,
    skip_dbt: Annotated[bool, typer.Option(help="Skip dbt run after ingestion.")] = False,
):
    """
    Ingest a sales history CSV into `raw.sale_history_dump`, then run dbt to rebuild marts.
    Outlet must be one of: rozelle, avalon, manly. Rows are deduplicated via a hash of
    (row_number, receipt_number, details, date, outlet, line_type).
    DB connection is read from WINONA_DATABASE_URL.
    """
    OUTLET_NAMES = ["rozelle", "avalon", "manly"]
    if outlet not in OUTLET_NAMES:
        raise ValueError(f"expect outlet to be one of {OUTLET_NAMES} but got {outlet}")
    ingest_sale_history(source_path, conn_str or get_conn_str(), outlet)
    if delete_source:
        print("deleting source file..")
        Path(source_path).unlink()
    if not skip_dbt:
        _run_dbt()  # full run — rebuilds all models including cross-domain marts


@app.command()
def product_export(
    source_path: Annotated[str, typer.Option()],
    conn_str: Annotated[Optional[str], typer.Option(help="Full connection string. Overrides WINONA_DATABASE_URL.")] = None,
    delete_source: Annotated[bool, typer.Option(help="If true, delete source file.")] = False,
    skip_dbt: Annotated[bool, typer.Option(help="Skip dbt run after ingestion.")] = False,
):
    """
    Ingest a product catalog CSV into `raw.product_export_dump`, then run dbt to rebuild marts.
    Each export is timestamped and appended, building a time-series snapshot of the catalog.
    Primary key is a hash of (id, export_timestamp, composite_sku).
    DB connection is read from WINONA_DATABASE_URL.
    """
    ingest_product_export(filepath=source_path, conn_str=conn_str or get_conn_str())
    if delete_source:
        print("deleting source file..")
        Path(source_path).unlink()
    if not skip_dbt:
        _run_dbt(select=["path:models/raw/product", "path:models/stg/product", "path:models/mart/product"])
