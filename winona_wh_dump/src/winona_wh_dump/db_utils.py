import duckdb as db


def generate_connection():
    return db.connect()


def attach_target_db(conn: db.DuckDBPyConnection, conn_str):
    """
    attach the target database, getting the connection string from config.yml
    """

    print(f"connecting to db with connection string: {conn_str}..")
    query = f"""--sql
    -- load postgres extension
    install postgres
    ;
    load postgres
    ;

    -- connect to postgres db
    attach '{conn_str}' as sales_dump_pg(
        type postgres
    );
    use sales_dump_pg;
    """
    conn.execute(query)

    return conn
