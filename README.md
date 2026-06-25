# Winona Data Warehouse

This project is an in-house tool to manage and automate as many tasks as possible in my day to day.

## Long Term Goals


Monorepo for ingesting and transforming Lightspeed exports for Winona Wine.

- `src/winona_wh/` — Python CLI for ingesting product catalog exports and sales history into PostgreSQL (`uv run winona_wh --help`)
- `winona_etl/` — dbt project for transforming raw dumps into a normalised warehouse (staging → marts)
- `eda/` — Marimo notebooks for exploratory analysis