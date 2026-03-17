import duckdb
from .config import get_conn_str


def get_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn_str = get_conn_str()
    conn.execute(f"""
        install postgres;
        load postgres;
        attach '{conn_str}' as wh (type postgres);
    """)
    return conn
