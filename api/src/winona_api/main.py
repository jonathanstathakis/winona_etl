import logging
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
from fastapi.middleware.cors import CORSMiddleware
from .routers import mart, loader

app = FastAPI(title="Winona API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(mart.router)
app.include_router(loader.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}
