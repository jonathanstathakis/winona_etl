import os
from pathlib import Path
import subprocess
import typer
from typing import Annotated

app = typer.Typer(help="Create and open Marimo notebooks for data exploration.")

_EDA_DIR = Path(__file__).parents[2] / "eda"
_TEMPLATE = _EDA_DIR / "product_catalog_eda.py"
_EXCLUDED = {"db.py", "product_catalog_eda.py"}


@app.command()
def new(
    name: Annotated[str, typer.Argument(help="Name for the new notebook (without .py)")],
):
    """
    Create a new notebook from the template and open it in Marimo.
    """
    dest = _EDA_DIR / f"{name}.py"
    if dest.exists():
        raise typer.BadParameter(f"{dest} already exists.")
    dest.write_text(_TEMPLATE.read_text())
    print(f"created {dest}")
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    subprocess.run(["uv", "run", "marimo", "edit", f"{name}.py"], cwd=_EDA_DIR, env=env)


@app.command()
def open():
    """
    Interactively select and open a notebook from eda/.
    """
    import questionary

    notebooks = sorted(
        f.stem for f in _EDA_DIR.glob("*.py") if f.name not in _EXCLUDED
    )
    if not notebooks:
        print("No notebooks found in eda/")
        raise typer.Exit()

    name = questionary.select("Select a notebook:", choices=notebooks).ask()
    if name:
        env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    subprocess.run(["uv", "run", "marimo", "edit", f"{name}.py"], cwd=_EDA_DIR, env=env)
