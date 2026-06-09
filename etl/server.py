from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
from pathlib import Path

DBT_DIR = Path("/workspaces/etl")


def _dbt(*args: str) -> None:
    subprocess.run(["dbt", *args, "--profiles-dir", "."], cwd=DBT_DIR, check=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _dbt("deps")
    yield


app = FastAPI(lifespan=lifespan)


class RunRequest(BaseModel):
    select: list[str] | None = None


@app.post("/run")
def run(req: RunRequest = RunRequest()):
    try:
        _dbt("seed")
        args = ["run"]
        if req.select:
            args += ["--select"] + req.select
        _dbt(*args)
        return {"status": "ok"}
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "ok"}
