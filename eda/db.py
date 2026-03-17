import os
from pathlib import Path
import duckdb

_ENV_PATH = Path(__file__).parents[1] / ".env"
_ENV_VAR = "WINONA_DATABASE_URL"


def _load_env():
    if not _ENV_PATH.exists():
        return
    for line in _ENV_PATH.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


_load_env()


def connect() -> duckdb.DuckDBPyConnection:
    conn_str = os.environ.get(_ENV_VAR)
    if not conn_str:
        raise EnvironmentError(
            f"Environment variable {_ENV_VAR!r} is not set. "
            f"Add it to {_ENV_PATH}, e.g.:\n"
            f"  {_ENV_VAR}=postgresql://user:password@localhost:5432/dbname"
        )
    conn = duckdb.connect()
    conn.execute(f"""
        install postgres;
        load postgres;
        attach '{conn_str}' as wh (type postgres);
    """)
    return conn
