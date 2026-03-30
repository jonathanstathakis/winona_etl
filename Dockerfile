FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY api/ api/
COPY winona_etl/ winona_etl/

RUN uv sync
RUN cd api && uv sync
RUN cd winona_etl && uv sync && uv run dbt deps --profiles-dir .

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/docker-entrypoint.sh"]
