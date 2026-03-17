# Winona Data Warehouse — Claude Context

## Project overview

Internal tooling for a wine shop (Winona). Ingests Lightspeed POS exports into a Postgres data warehouse, transforms with dbt, and surfaces data via a FastAPI + Next.js web app.

Three outlets: **rozelle**, **avalon**, **manly**

---

## Architecture

```
winona_data_wh/
├── src/winona/          ← CLI package (typer). Entry: winona.cli:app
├── winona_etl/          ← dbt project (postgres adapter)
├── api/                 ← FastAPI backend (uv project)
│   └── src/winona_api/
│       ├── main.py      ← FastAPI app + CORS
│       ├── db.py        ← DuckDB postgres extension to connect to wh
│       ├── config.py    ← reads WINONA_DATABASE_URL from .env
│       └── routers/
│           ├── mart.py    ← GET /api/mart/*
│           └── loader.py  ← POST /api/loader/*
├── web/                 ← Next.js frontend (App Router, MUI, AG Grid)
│   └── app/
│       ├── catalog/     ← Product catalog (mart_item_curr)
│       ├── transfer/    ← Transfer order generator
│       ├── wine/        ← Wine-specific view
│       └── upload/      ← CSV upload form
└── eda/                 ← marimo EDA notebooks
```

---

## Running the stack

```bash
# API (port 8000)
cd api && uv run uvicorn winona_api.main:app --reload --port 8000

# Frontend (port 3000, proxies /api → localhost:8000)
cd web && npm run dev

# CLI
uv run winona --help
```

Next.js proxies `/api/*` → `http://localhost:8000/api/*` via `next.config.ts` rewrites.

---

## Database

- **Postgres** — main warehouse. Schemas are first-level: `raw`, `stg`, `mart`
- **DuckDB** — used in API and CLI to query Postgres via the `postgres` extension
- Connection string in `.env` as `WINONA_DATABASE_URL` (postgres DSN)
- API/CLI attaches Postgres as: `attach '...' as wh (type postgres)`
- When querying via DuckDB, prefix with the attach alias: `wh.raw.*`, `wh.stg.*`, `wh.mart.*`
- When querying Postgres directly (psql, dbt), schemas are just `raw.*`, `stg.*`, `mart.*`

---

## dbt project (`winona_etl/`)

```bash
cd winona_etl
uv run dbt run                          # full run
uv run dbt run -s mart_item_curr        # single model
uv run dbt run -s path:models/stg/product  # by path
```

Key models:
- `raw.product_export_dump` ← source table
- `stg_item` → `stg_item_curr` (latest export only) → `mart_item_curr` (+ export_date column)
- `mart_item_curr_active` — active items only
- `mart_item_by_tag` — denormalised items × tags
- `mart_wine_curr` — wine-specific subset
- `mart_sale` — sale history mart

---

## API endpoints

```
GET  /api/mart/product-catalog   → mart.mart_item_curr (ordered by name)
GET  /api/mart/item-by-tag       → mart.mart_item_by_tag
GET  /api/mart/wine              → mart.mart_wine_curr
GET  /api/mart/data-health       → distinct export timestamps + sale date range per outlet
POST /api/loader/product-export  → multipart: file (CSV)
POST /api/loader/sale-history    → multipart: file (CSV), outlet (str)
GET  /api/health
```

---

## Frontend notes

- **MUI** for layout/forms, **AG Grid Community** for the transfer grid, **MUI DataGrid** for catalog/wine
- `export_timestamp` is stored as Unix seconds (bigint) — convert with `new Date(val * 1000)`
- The dbt `mart_item_curr` view also has an `export_date` column (human-readable string from Postgres `to_char`)
- Transfer page: AG Grid with refs pattern for stable callbacks (`getRowsRef`, `navigateRef`, `deleteRowRef`)
- Catalog page: filter/sort/column presets saved to `localStorage` under key `winona_catalog_filter_presets`
- Next.js `next.config.ts` rewrites handle API proxying in dev

---

## CLI package (`src/winona/`)

Key modules:
- `cli.py` — typer app, commands: `load-product-export`, `load-sale-history`, `serve`, etc.
- `loader.py` — `ingest_product_export()`, `ingest_sale_history()`, `_run_dbt()`
- `config.py` — reads `.env`
- `db_utils.py` — shared DB helpers

---

## Known TODOs (in code)

- `transfer/page.tsx`: Add "create new" button (clears grid, transferName, currentTransferId)
- `transfer/page.tsx`: Fix AG Grid column order reset on quantity cell edit
- `transfer/page.tsx`: Fix "Saved!" status not resetting when grid cell values change (needs `onCellValueChanged`)
