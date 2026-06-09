FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc git && rm -rf /var/lib/apt/lists/*

RUN pip install uv

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
    "dbt-postgres>=1.10.0" \
    "dbt-core>=1.11.5" \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.34.0"

COPY server.py .

EXPOSE 8001
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001"]
