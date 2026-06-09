FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY api/ api/

ARG TARGETARCH
RUN uv sync
RUN cd api && uv sync

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/docker-entrypoint.sh"]
