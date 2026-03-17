import os
from pathlib import Path
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).parents[3] / ".env"
load_dotenv(_ENV_PATH)

ENV_VAR = "WINONA_DATABASE_URL"


class ConfigError(Exception):
    pass


def get_conn_str() -> str:
    conn_str = os.environ.get(ENV_VAR)
    if not conn_str:
        raise ConfigError(
            f"Environment variable {ENV_VAR!r} is not set. "
            f"Add it to {_ENV_PATH}, e.g.:\n"
            f"  {ENV_VAR}=postgresql://user:password@localhost:5432/dbname"
        )
    return conn_str
