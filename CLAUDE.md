# Winona Data Warehouse — Claude Context

## Project overview

Internal tooling for a wine shop (Winona). Ingests Lightspeed POS exports into a Postgres data warehouse, transforms with dbt, and surfaces data via a FastAPI + Next.js web app.

Three outlets: **rozelle**, **avalon**, **manly**

---

## Architecture

```
winona_data_wh/
├── src/winona/          ← CLI package (typer). Entry: winona.cli:app
├── etl/                 ← dbt project + ETL service
│   ├── models/          ← dbt SQL models (raw → stg → mart)
│   ├── seeds/           ← dbt seed CSV files
│   ├── macros/          ← dbt macros
│   ├── profiles.yml     ← dbt connection profiles (reads WINONA_DB_HOST)
│   ├── dbt_project.yml
│   ├── server.py        ← FastAPI ETL server (POST /run, GET /health)
│   └── Dockerfile       ← builds the etl service image
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
├── .devcontainer/
│   └── devcontainer.json  ← VS Code Dev Container config (targets etl service)
└── eda/                 ← marimo EDA notebooks
```

---

## Running the stack

```bash
# First time: copy .env.example to .env (edit WINONA_DB_PASSWORD if needed)
cp .env.example .env

# Start everything (Postgres + ETL + API + web)
docker compose up --build

# Web  → http://localhost:3000
# API  → http://localhost:8000/docs
# ETL  → http://localhost:8001/docs
```

Four services start in dependency order: `db` → `etl` → `api` → `web`.

Next.js proxies `/api/*` → `http://localhost:8000/api/*` via `next.config.ts` rewrites.

For local CLI development (requires uv):
```bash
uv run winona --help
```

---

## Database

- **Postgres** — main warehouse. Schemas are first-level: `raw`, `stg`, `mart`
- **DuckDB** — used in API and CLI to query Postgres via the `postgres` extension
- Connection string in `.env` as `WINONA_DATABASE_URL` (postgres DSN)
- API/CLI attaches Postgres as: `attach '...' as wh (type postgres)`
- When querying via DuckDB, prefix with the attach alias: `wh.raw.*`, `wh.stg.*`, `wh.mart.*`
- When querying Postgres directly (psql, dbt), schemas are just `raw.*`, `stg.*`, `mart.*`

---

## dbt project (`etl/`)

The dbt project lives in `etl/` and runs inside the `etl` Docker container, which has direct access to the `db` service on the Docker network. See the **ETL container** section below for development workflow.

To trigger a full dbt run manually (e.g. from the API entrypoint or for testing):
```bash
docker exec winona_etl-etl-1 sh -c "cd /workspaces/etl && dbt run --profiles-dir ."
docker exec winona_etl-etl-1 sh -c "cd /workspaces/etl && dbt run --profiles-dir . --select mart_item_curr"
```

Key models:
- `raw.product_export_dump` ← source table
- `stg_item` → `stg_item_curr` (latest export only) → `mart_item_curr` (+ export_date column)
- `mart_item_curr_active` — active items only
- `mart_item_by_tag` — denormalised items × tags
- `mart_wine_curr` — wine-specific subset
- `mart_sale` — sale history mart

---

## ETL container

### Why dbt runs in a container

dbt connects to Postgres directly using the postgres adapter — it's not proxied through DuckDB like the API is. This means it needs a hostname that resolves to the database.

Inside Docker, the database is reachable at `db` (the compose service name). From the host machine, it's at `localhost:5432` via port-forwarding. These two values can't both be the default in `profiles.yml` at the same time.

Rather than managing this split with environment variable tricks, the dbt project runs entirely inside the `etl` container where `db` always resolves correctly. The API triggers dbt runs by calling `POST http://etl:8001/run` (see `src/winona/loader.py:_run_dbt()`) instead of running dbt as a subprocess.

The ETL service (`etl/server.py`) is a small FastAPI app:
- `GET  /health` — liveness check (used by compose healthcheck)
- `POST /run`    — runs `dbt seed` + `dbt run`; accepts optional `select` list to target specific models

On startup the server runs `dbt deps` to ensure packages are installed.

### Developing dbt models with VS Code Dev Containers

The `etl` container is also a VS Code Dev Container. Opening it gives you dbt Power User with a live connection to the database — intellisense, lineage, query preview, all working inside the container where `db` resolves natively.

**First time setup:**
1. Start the stack: `docker compose up --build`
2. In VS Code: Command Palette → **Dev Containers: Reopen in Container**
3. VS Code connects to the running `etl` container and installs extensions
4. Run `dbt compile --profiles-dir .` in the container terminal to generate `target/manifest.json`

**Subsequent opens:**
- Command Palette → **Dev Containers: Reopen in Container**
- If dbt Power User shows no intellisense or buttons do nothing, run `dbt compile --profiles-dir .` — the manifest may be stale

**Running dbt manually in the container terminal:**
```bash
dbt run --profiles-dir .
dbt run --profiles-dir . --select mart_item_curr
dbt test --profiles-dir .
```

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

## Planogram Domain

### Terminology
- **Layout** — named, versioned snapshot of the planogram configuration across all 3 outlets. One is flagged `is_current` at any time. Duplicated then modified (no branching for now).
- **Outlet** — one of 3 physical stores: manly, avalon, rozelle.
- **Bay** — physical fixture within an outlet. Outlet-specific, not shared across outlets.
- **Planogram** — the actual shelf/slot/product layout for a single bay within a layout.
- **Shelf** — a horizontal row within a planogram.
- **Slot** — an individual product position on a shelf.

### Data hierarchy
Layout → Outlets (3) → Bays → Planogram → Shelves → Slots → Products

### Key behaviours
- Layouts are duplicated then modified (no in-place branching for now).
- Bays are outlet-specific, not shared across outlets.
- One Layout is flagged `is_current` at a time.
- Range updates are event-driven (range reviews, supplier changes), not seasonal.
- Floor plan (room polygon + bay positions) is outlet-level, shared across layout versions.

### Route structure
```
/planogram                                  ← Layout list (picker)
/planogram/[id]                             ← Layout hub (3 outlet cards)
/planogram/[id]/[outlet]                    ← Outlet hub (bay list + floor plan thumbnail)
/planogram/[id]/[outlet]/floor-plan         ← Floor plan view
/planogram/[id]/[outlet]/floor-plan/edit    ← Floor plan editor
/planogram/[id]/[outlet]/[bay-id]           ← Bay planogram editor
```

---

## Known TODOs (in code)

- `transfer/page.tsx`: Add "create new" button (clears grid, transferName, currentTransferId)
- `transfer/page.tsx`: Fix AG Grid column order reset on quantity cell edit
- `[id]/[outlet]/[bayId]/page.tsx`: Bay planogram editor should show all bays in the outlet as a list/nav so you can switch between them without going back to the outlet hub
- `floor-plan/edit/page.tsx`: Add icon catalog — placeable objects for computer workstations, VM/display screens, doors
- `floor-plan/edit/page.tsx`: Add drag-to-resize for placed bay objects (resize handles on selected bay)
- `floor-plan/edit/page.tsx`, `[outlet]/page.tsx`: Add floor plan export (SVG/PDF download)
- `[id]/[outlet]/[bayId]/page.tsx`, `components/PrintDialog.tsx`: Test and finalise printing — bay planogram print, floor plan print
- `floor-plan/edit/page.tsx`: Fix page scrolling when zooming via scroll wheel on canvas — React registers wheel listeners as passive so e.preventDefault() has no effect; fix with a non-passive useEffect listener
