from pathlib import Path

import duckdb as db
from rich import print
from rich.text import Text

ROOT = Path(__file__).parent
CONFIG = ROOT / "config.yml"


def attach_target_db(conn: db.DuckDBPyConnection, conn_str):
    """
    attach the target database, getting the connection string from config.yml
    """
    query = f"""--sql
    -- load postgres extension
    install postgres
    ;
    load postgres
    ;

    -- connect to postgres db
    attach '{conn_str}' as sales_dump_pg(
        type postgres
    )
    ;
    """

    conn.execute(query)


def display_latest_data_date(conn):
    result = conn.execute("""
    select date from latest_date;
                    """)

    latest_date = result.fetchone()[0].strftime("%b %d, %Y")
    text = Text()
    text.append("earliest transaction date: ")
    text.append(latest_date, style="bold magenta")
    print(text)


def move_sales_data(source_path, destination_path):
    sales_files = list(Path(source_path).glob("sales_*.csv"))

    if len(list(sales_files)) > 0:
        print("sales files found, moving to destionation path..")
        for sale_file in sales_files:
            print(f"moving {sale_file}..")
            new_path = Path(destination_path) / sale_file.name
            print(f"new path: {new_path}..")
            sale_file.rename(new_path)
            print("successfully moved.")

    print("finished.")


def insert_sales_data(conn, dir_path):
    query = f"""--sql
    /* use duckdb to connect to the postgres db and perform a read csv over all files in the dir */
    -- use postgres db as default
    use sales_dump_pg
    ;

    -- create a table and insert all the data from the csv files in raw/
    create or replace table raw_sale_history as 
    select
        row_number() over () as insert_idx,
        now() as append_dt, 
        *
    from 
        read_csv('{dir_path}', normalize_names = true, filename = true)
    ;

    create or replace table latest_date as select min(date) as date from raw_sale_history;
    -- display resulting table.
    select *
    from raw_sale_history
    ;
    """

    print("executing insertion query..")
    conn.execute(query)
    print("query executed successfully.")

