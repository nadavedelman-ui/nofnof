# נופנוף · Nofnof — המאגר החכם לצמחי נוף בישראל

A searchable, filterable database of plants relevant to Israeli landscaping. An immutable,
authority-first dataset with a static, in-browser front-end.

**Live site:** served from `web/` via GitHub Pages.

## What it is
- **2,679 canonical taxa** with stable identity (GBIF taxon keys), Hebrew + scientific names,
  family, life-form, flowering season, habitat and distribution.
- **Invasive status** for every taxon (`listed` / `potential` / `not_listed`).
- A **commercial layer** (price / pot size / availability) joined on top from a retail nursery.
- **CC-licensed plant photos** (Wikimedia Commons → GBIF fallback), with attribution.

## Pipeline (build-time, Python + DuckDB)
```
Stage 0  scrape      one module per source → data/raw/<source>/<date>/
Stage 1  stage (EAV) raw → DuckDB staging table
Stage 2  resolve     GBIF name-match → canonical_taxon_id (+ review queue)
Stage 3  canonical   reconcile → web/public/canonical.parquet (+ provenance)
images / invasive / commercial layers join on top of the spine
```
Run: `python run_phase2.py` (identity + flags) → `python run_phase3.py` (seller) →
`python -m pipeline.images` (photos).

## Front-end (static)
`web/` is a static site: **DuckDB-WASM** loads `canonical.parquet` and runs all filtering
in the browser. RTL Hebrew UI. No backend.
Local: `python -m http.server --directory web`.

## Data sources
- **GBIF** — taxonomic backbone / identity.
- **צמח השדה (KKL / wildflowers.co.il)** — native-flora spine (names, life-form, habitat, bloom).
- **GRIIS-Israel** (GBIF checklist) + curated literature (Dufour-Dror / INPA) — invasive status.
- **משתלת אזור (azurflowers.co.il)** — commercial availability.
- **Wikimedia Commons / GBIF media** — CC-licensed photos (attributed in the UI).

Every value carries its source in a provenance table; conflicting values are preserved, not
overwritten. Horticultural columns shown in the design mockups (sun / water / frost) are a
future curated layer and are not yet populated.
