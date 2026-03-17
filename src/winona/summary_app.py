import duckdb
import typer
from rich.console import Console
from rich.table import Table
from .config import get_conn_str, ConfigError

app = typer.Typer(help="Report data ranges for ingested exports.")
console = Console()


def _connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("install postgres; load postgres;")
    conn.execute(f"attach '{get_conn_str()}' as wh (type postgres); use wh;")
    return conn


@app.callback(invoke_without_command=True)
def show(ctx: typer.Context):
    """
    Show date ranges and row counts for sale history (by outlet) and product catalog exports.
    """
    if ctx.invoked_subcommand is not None:
        return

    try:
        conn = _connect()
    except ConfigError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Could not connect to database: {e}")
        raise typer.Exit(1)

    table = Table(show_header=True, header_style="bold")
    table.add_column("Stream")
    table.add_column("Outlet")
    table.add_column("Earliest", style="cyan")
    table.add_column("Latest", style="cyan")
    table.add_column("Exports", justify="right")
    table.add_column("Rows", justify="right")

    # Sale history — one row per outlet
    outlets = conn.execute("""
        select
            outlet,
            min(date),
            max(date),
            count(distinct export_filename),
            count(*)
        from raw.sale_history_dump
        group by outlet
        order by outlet
    """).fetchall()

    for i, row in enumerate(outlets):
        table.add_row(
            "sale-history" if i == 0 else "",
            row[0],
            str(row[1].date()) if row[1] else "—",
            str(row[2].date()) if row[2] else "—",
            str(row[3]),
            str(row[4]),
        )

    # Product export — export_timestamp is unix seconds
    row = conn.execute("""
        select
            epoch_ms(min(export_timestamp) * 1000)::date,
            epoch_ms(max(export_timestamp) * 1000)::date,
            count(distinct export_timestamp),
            count(*)
        from raw.product_export_dump
    """).fetchone()
    table.add_row(
        "product-export",
        "—",
        str(row[0]) if row[0] else "—",
        str(row[1]) if row[1] else "—",
        str(row[2]),
        str(row[3]),
    )

    console.print(table)
