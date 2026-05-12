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
# First time: copy .env.example to .env (edit WINONA_DB_PASSWORD if needed)
cp .env.example .env

# Start everything (Postgres + API + web)
docker compose up --build

# Web → http://localhost:3000
# API → http://localhost:8000/docs
```

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
- `transfer/page.tsx`: Fix "Saved!" status not resetting when grid cell values change (needs `onCellValueChanged`)
- `floor-plan/edit/page.tsx`: Show full bay list (all outlets) somewhere in the floor plan designer for reference
- `[id]/[outlet]/[bayId]/page.tsx`: Bay planogram editor should show all bays in the outlet as a list/nav so you can switch between them without going back to the outlet hub
- `floor-plan/edit/page.tsx`: Add icon catalog — placeable objects for computer workstations, VM/display screens, doors
- `floor-plan/edit/page.tsx`: Add drag-to-resize for placed bay objects (resize handles on selected bay)
- `floor-plan/edit/page.tsx`, `[outlet]/page.tsx`: Add floor plan export (SVG/PDF download)
- `[id]/[outlet]/[bayId]/page.tsx`, `components/PrintDialog.tsx`: Test and finalise printing — bay planogram print, floor plan print
- `floor-plan/edit/page.tsx`: Fix page scrolling when zooming via scroll wheel on canvas — React registers wheel listeners as passive so e.preventDefault() has no effect; fix with a non-passive useEffect listener
- `floor-plan/edit/page.tsx`: Add ability to delete individual polygon vertices in Edit Room mode (e.g. select vertex + Delete key, or right-click context menu)
