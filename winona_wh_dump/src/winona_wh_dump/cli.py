from pathlib import Path
import pandas as pd
import typer
import duckdb as db
from typing import Annotated
from .db_utils import attach_target_db, generate_connection

app = typer.Typer()


def get_creation_time(source_path):
    ctime = Path(source_path).stat()[9]
    return ctime


def create_product_export_stg(filepath, conn):
    """
    ingest the current csv file into a staging table
    """
    print(f"reading file at {filepath}..")
    df = pd.read_csv(filepath)
    print("adding the export timestamp to rows..")
    ctime = get_creation_time(filepath)
    df["export_timestamp"] = ctime

    print(f"reading {filepath} into stg table..")
    query = """
    create or replace table product_export_stg as select * from df;
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
        hash(id::varchar, export_timestamp::varchar, composite_sku::varchar) as pk, -- include composite sku to compensate for repeated ids in composite item rows.
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


def create_raw_sale_history(filepath, conn, outlet: str):
    print(f"reading file at {filepath}..")
    print("adding the export timestamp to rows..")
    ctime = get_creation_time(filepath)

    print(f"reading {filepath} into staging table..")
    query = f"""
    create or replace table cli.raw_sale_history as select
    {ctime} as export_timestamp,
    '{outlet}' as outlet,
    hash(row_number() over (), receipt_number, details, date, outlet, line_type) as sale_history_line_id,
    *
    from read_csv('{filepath}', normalize_names=true);
    """
    conn.execute(query)


def create_sale_history_dump(conn):
    """
    creates the product export dump table if it doesnt exist
    """

    query = """
CREATE TABLE if NOT EXISTS cli.sale_history_dump (
    sale_history_line_id varchar primary key,
    export_timestamp bigint,
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
    user varchar,
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
    """ """

    query = """
    insert into cli.sale_history_dump 
    select
        sale_history_line_id,
        export_timestamp,
        outlet,
        date,
        receipt_number,
        line_type,
        customer_code,
        customer_name,
        note,
        quantity,
        subtotal,
        sales_tax,
        discount,
        loyalty,
        total,
        paid,
        details,
        register,
        user,
        status,
        sku,
        accountcodesale,
        accountcodepurchase,
        state,
        attributes 
    from
        cli.raw_sale_history
    """
    print(
        f"before insert, count={conn.execute('select count(*) from cli.sale_history_dump').fetchall()}"
    )
    print("inserting new file into sale_history_dump..")

    conn.execute(query)
    print(
        f"new count={conn.execute('select count(*) from cli.sale_history_dump').fetchall()}"
    )


def ingest_sale_history(filepath, conn_str, outlet):
    conn = generate_connection()
    attach_target_db(conn, conn_str)
    conn.execute("create schema if not exists cli")
    create_raw_sale_history(filepath, conn, outlet)
    create_sale_history_dump(conn)
    insert_into_sale_history_dump(conn)


def ingest_product_export(filepath, conn_str):
    """
    ingest the product export data file
    """
    conn = generate_connection()
    attach_target_db(conn, conn_str)
    create_product_export_stg(filepath, conn)
    create_product_export_dump(conn)
    insert_into_product_export_dump(conn)

    # cleanup


@app.command()
def sale_history(
    outlet: str,
    dbname: Annotated[str, typer.Option()],
    user: Annotated[str, typer.Option()],
    source_path: Annotated[str, typer.Option()],
    host: Annotated[str, typer.Option()] = "localhost",
    password: Annotated[str, typer.Option()] = "",
    conn_str: Annotated[str, typer.Option()] = "",
    delete_source: Annotated[
        bool, typer.Option(help="If true, delete source file.")
    ] = False,
):
    if not conn_str:
        conn_str = f"dbname={dbname} user={user} password={password} host={host}"

    OUTLET_NAMES = ["rozelle", "avalon", "manly"]
    if outlet not in OUTLET_NAMES:
        raise ValueError(f"expect outlet to be one of {OUTLET_NAMES} but got {outlet}")
    ingest_sale_history(source_path, conn_str, outlet)

    if delete_source:
        print("deleting source file..")

        Path(source_path).unlink()


@app.command()
def product_export(
    dbname: Annotated[str, typer.Option()],
    user: Annotated[str, typer.Option()],
    source_path: Annotated[str, typer.Option()],
    host: Annotated[str, typer.Option()] = "localhost",
    password: Annotated[str, typer.Option()] = "",
    conn_str: Annotated[str, typer.Option()] = "",
    delete_source: Annotated[
        bool, typer.Option(help="If true, delete source file.")
    ] = False,
):
    """
    ingest the csv into `product_export_dump`. As opposed to the current functioning of `sale_history_dump` this dump has a primary key based on the hash of the product id
    and export timestamp.

    It should be noted that this command differs from sale_history in that sale_history globs all files matching the pattern wheras this one needs to be pointed to single files,
    and sale_history replaces the content of the dump table when run, wheras this appends.
    """
    if not conn_str:
        conn_str = f"dbname={dbname} user={user} password={password} host={host}"

    ingest_product_export(filepath=source_path, conn_str=conn_str)
    if delete_source:
        print("deleting source file..")

        Path(source_path).unlink()


if __name__ == "__main__":
    app()
