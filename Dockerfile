FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY api/ api/
COPY winona_etl/ winona_etl/

ARG TARGETARCH
RUN apt-get update && apt-get install -y wget unzip && \
    ARCH=$([ "$TARGETARCH" = "arm64" ] && echo "aarch64" || echo "amd64") && \
    wget -q https://github.com/duckdb/duckdb/releases/download/v1.4.4/duckdb_cli-linux-${ARCH}.zip && \
    unzip duckdb_cli-linux-${ARCH}.zip -d /usr/local/bin && \
    rm duckdb_cli-linux-${ARCH}.zip && \
    apt-get remove -y wget unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
RUN uv sync
RUN cd api && uv sync
RUN cd winona_etl && uv sync && uv run dbt deps --profiles-dir .

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/docker-entrypoint.sh"]
