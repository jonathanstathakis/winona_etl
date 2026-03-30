#!/bin/bash
set -e
echo "Initialising database..."
uv run winona init-db
echo "Starting API..."
cd api && exec uv run uvicorn winona_api.main:app --host 0.0.0.0 --port 8000
