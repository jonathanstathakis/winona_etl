import typer
from .loader import app as loader_app, _run_dbt
from .explorer import app as explorer_app
from .config_app import app as config_app
from .summary_app import app as summary_app
from .serve_app import app as serve_app
from .db_utils import attach_target_db, generate_connection
from .config import get_conn_str
from .loader import create_product_export_dump, create_sale_history_dump

app = typer.Typer()
app.add_typer(loader_app, name="loader")
app.add_typer(explorer_app, name="explorer")
app.add_typer(config_app, name="config")
app.add_typer(summary_app, name="summary")
app.add_typer(serve_app, name="serve")


@app.command()
def init_db():
    """Initialise the database schema without loading any data."""
    conn = generate_connection()
    attach_target_db(conn, get_conn_str())
    conn.execute("create schema if not exists raw")
    create_product_export_dump(conn)
    create_sale_history_dump(conn)
    print("Raw schema and tables created.")
    _run_dbt()
    print("Database initialised and ready.")

if __name__ == "__main__":
    app()
