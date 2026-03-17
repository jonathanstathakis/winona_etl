import typer
from rich.console import Console
from .config import ENV_VAR, get_conn_str, ConfigError

console = Console()

app = typer.Typer(help="Manage winona configuration.")


@app.command()
def verify():
    """
    Check WINONA_DATABASE_URL is set and test the database connection.
    """
    import os
    import duckdb

    conn_str_raw = os.environ.get(ENV_VAR)
    if not conn_str_raw:
        console.print(f"[red]✗[/red] {ENV_VAR} is not set")
        raise typer.Exit(1)
    console.print(f"[green]✓[/green] {ENV_VAR} is set")

    try:
        conn_str = get_conn_str()
        conn = duckdb.connect()
        conn.execute(f"""
            install postgres;
            load postgres;
            attach '{conn_str}' as _verify_test (type postgres);
        """)
        conn.execute("detach _verify_test")
        console.print("[green]✓[/green] database connection ok")
    except ConfigError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] database connection failed: {e}")
        raise typer.Exit(1)
