FROM postgres:16
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl https://install.duckdb.org | sh
ENV PATH="/root/.duckdb/cli/latest:$PATH"