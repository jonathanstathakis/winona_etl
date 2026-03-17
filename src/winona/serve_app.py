import os
import signal
import subprocess
import sys
from pathlib import Path
import typer

app = typer.Typer(help="Start the API and web frontend.")

_ROOT = Path(__file__).parents[2]
_API_DIR = _ROOT / "api"
_WEB_DIR = _ROOT / "web"


@app.callback(invoke_without_command=True)
def serve(
    ctx: typer.Context,
    api_port: int = typer.Option(8000, help="Port for the FastAPI server."),
    web_port: int = typer.Option(3000, help="Port for the Next.js dev server."),
):
    """
    Start the FastAPI backend and Next.js frontend together.
    Ctrl-C stops both.
    """
    if ctx.invoked_subcommand is not None:
        return

    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}

    api = subprocess.Popen(
        ["uv", "run", "uvicorn", "winona_api.main:app", "--reload", f"--port={api_port}"],
        cwd=_API_DIR,
        env=env,
    )
    web = subprocess.Popen(
        ["npm", "run", "dev", "--", f"--port={web_port}"],
        cwd=_WEB_DIR,
        env=env,
    )

    print(f"API  → http://localhost:{api_port}/docs")
    print(f"Web  → http://localhost:{web_port}")
    print("Ctrl-C to stop both.")

    def _stop(sig, frame):
        api.terminate()
        web.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    api.wait()
    web.wait()
