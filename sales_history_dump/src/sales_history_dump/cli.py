import typer

from .sales_history_dump import sales_history_dump

app = typer.Typer()
app.command()(sales_history_dump)


if __name__ == "__main__":
    app()
