import typer
from .loader import app as loader_app
from .explorer import app as explorer_app
from .config_app import app as config_app
from .summary_app import app as summary_app
from .serve_app import app as serve_app

app = typer.Typer()
app.add_typer(loader_app, name="loader")
app.add_typer(explorer_app, name="explorer")
app.add_typer(config_app, name="config")
app.add_typer(summary_app, name="summary")
app.add_typer(serve_app, name="serve")

if __name__ == "__main__":
    app()
