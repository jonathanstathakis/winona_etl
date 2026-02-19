import typer
import duckdb as db
from typing import Annotated
from .sales_history_dump import (
    display_latest_data_date,
    move_sales_data,
    insert_sales_data,
)
from .db_utils import attach_target_db, generate_connection

app = typer.Typer()


@app.command()
def sale_history(
    dbname: Annotated[str, typer.Option()],
    user: Annotated[str, typer.Option()],
    source_path: Annotated[str, typer.Option()],
    destination_path: Annotated[str, typer.Option()],
    host: Annotated[str, typer.Option()] = "localhost",
    password: Annotated[str, typer.Option()] = "",
    conn_str: Annotated[str, typer.Option()] = "",
):
    if not conn_str:
        conn_str = f"dbname={dbname} user={user} password={password} host={host}"

    move_sales_data(source_path=source_path, destination_path=destination_path)
    with db.connect() as conn:
        attach_target_db(conn, conn_str)
        insert_sales_data(conn, dir_path=destination_path)
        display_latest_data_date(conn)


def move_product_export(source_path, destination_path, delete_source: bool = False):
    from pathlib import Path

    # ctime = Path(source_path).stat()["st_ctime"]
    ctime = Path(source_path).stat()[9]
    import pandas as pd

    print(f"reading file at {source_path}..")
    df = pd.read_csv(source_path)
    print("adding the expore timestamp to rows..")
    df["export_timestamp"] = ctime
    outpath = Path(destination_path) / f"product-export-{ctime}.csv"
    print(f"writing table to {outpath}..")

    if not outpath.parent.exists():
        outpath.parent.mkdir()
    df.to_csv(outpath, index=False)

    if delete_source:
        print("deleting source file..")

        Path(source_path).unlink()
    print("finished.")

    return outpath


def create_product_export_stg(filepath, conn):
    """
    ingest the current csv file into a staging table
    """
    print(f"reading {filepath} into stg table..")
    query = f"""
    create or replace temp table product_export_stg as select * from read_csv('{filepath}',sample_size=10000);
    """

    conn.execute(query)


def create_product_export_dump(conn):
    """
    creates the product export dump table if it doesnt exist
    """

    query = """
CREATE TABLE if NOT EXISTS product_export_dump (
    pk varchar primary key, -- hash of id and export timestamp
    id VARCHAR,
    handle VARCHAR,
    sku VARCHAR,
    composite_name VARCHAR,
    composite_sku VARCHAR,
    composite_quantity DOUBLE,
    NAME VARCHAR,
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
    inventory_Winona_Avalon DOUBLE,
    reorder_point_Winona_Avalon DOUBLE,
    restock_level_Winona_Avalon DOUBLE,
    inventory_Winona_Manly DOUBLE,
    reorder_point_Winona_Manly DOUBLE,
    restock_level_Winona_Manly DOUBLE,
    inventory_Winona_Rozelle DOUBLE,
    reorder_point_Winona_Rozelle DOUBLE,
    restock_level_Winona_Rozelle DOUBLE,
    inventory_Winona_Warehouse DOUBLE,
    reorder_point_Winona_Warehouse DOUBLE,
    restock_level_Winona_Warehouse DOUBLE,
    export_timestamp bigint
);
   """
    conn.execute(query)


def insert_into_product_export_dump(conn):
    """
    insert new csv into product_export_dump. Will fail if hash of export timestamp and product id already exists in table
    """

    query = """
    
    insert into product_export_dump
    with pk_hash as (
        select
        hash(id::varchar, export_timestamp::varchar) as pk,
        *
        from
        product_export_stg)
    select
        *
    from
        pk_hash
    """
    print(
        f"before insert, count={conn.execute('select count(*) from product_export_dump').fetchall()}"
    )
    print("inserting new file into product_export_dump..")
    conn.execute(query)
    print(
        f"new count={conn.execute('select count(*) from product_export_dump').fetchall()}"
    )


def ingest_product_export(filepath, conn_str):
    """
    ingest the product export data file
    """
    conn = generate_connection()
    attach_target_db(conn, conn_str)
    create_product_export_stg(filepath, conn)
    create_product_export_dump(conn)
    insert_into_product_export_dump(conn)


@app.command()
def product_export(
    dbname: Annotated[str, typer.Option()],
    user: Annotated[str, typer.Option()],
    source_path: Annotated[str, typer.Option()],
    destination_dir_path: Annotated[str, typer.Option()],
    host: Annotated[str, typer.Option()] = "localhost",
    password: Annotated[str, typer.Option()] = "",
    conn_str: Annotated[str, typer.Option()] = "",
    delete_source: Annotated[
        bool, typer.Option(help="If true, delete source file.")
    ] = False,
):
    """
    2 actions:

    1. Takes a single product-export.csv file `--source-path`, makes a copy within `--destination-dir-path`, recording the file creation date within the new csv file, and optionally deleting the original file `--delete_source`.

    2. ingest the csv into `product_export_dump`. As opposed to the current functioning of `sale_history_dump` this dump has a primary key based on the hash of the product id
    and export timestamp.

    It should be noted that this command differs from sale_history in that sale_history globs all files matching the pattern wheras this one needs to be pointed to single files,
    and sale_history replaces the content of the dump table when run, wheras this appends.
    """
    if not conn_str:
        conn_str = f"dbname={dbname} user={user} password={password} host={host}"

    new_file = move_product_export(
        source_path, destination_dir_path, delete_source=delete_source
    )

    ingest_product_export(filepath=new_file, conn_str=conn_str)


if __name__ == "__main__":
    app()
