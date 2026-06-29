# Plant DB — Build Spec (v1)

Searchable, interactive plant database for Israeli landscape developers.
Immutable dataset. Authority-spine first, seller availability joined on top.

---

## 1. Goal

A read-only, filterable database of plants relevant to Israeli landscaping, where
each plant carries the attributes a landscape architect actually filters on
(water demand, salt/frost tolerance, mature size, native/invasive status, function),
not just commercial catalog fields. The data layer is the hard part; this spec
covers acquisition → reconciliation → a single published artifact → a static front-end.

Division of labour: pipeline (Nadav) produces breadth + clean identity; horticultural
judgment columns (the aunt) are the curated moat the pipeline feeds.

---

## 2. Stack rationale (constraint → choice)

| Constraint | Choice |
|---|---|
| Immutable dataset, "no Supabase" | DuckDB file-based processing; no DB server |
| Prefer Railway/Vercel/Netlify | Static deploy on Vercel/Netlify; no backend |
| Fast in-browser filtering | DuckDB-WASM queries a Parquet file client-side |
| One engine to learn | DuckDB at build-time AND query-time (same SQL) |
| Messy Hebrew/Latin names across sources | GBIF name-match API → stable taxon key as join key |
| "Extract maximally, sort later" | EAV staging (schema-on-read), reconcile downstream |

Railway fallback: only if write features or dataset size later break the static model.

---

## 3. Decisions to confirm (react before Phase 0 locks)

1. **Serving model** — *Recommend: static.* Build emits `canonical.parquet`; front-end
   is a static site (Vercel/Netlify) using DuckDB-WASM to query it in-browser. Confirm,
   or you want a Railway-hosted query API instead. *(Confidence: high for static.)*
2. **Granularity** — *Recommend: species-level canonical row, cultivar as an attribute*
   (`cultivar` field), with the option to promote specific cultivars to their own rows
   later (e.g. dwarf forms that matter for landscaping). Confirm.
3. **First spine source (Phase 1)** — *Recommend: native-flora DB (wildflowers.co.il)*
   as the Hebrew↔Latin crosswalk + `native_status` spine, since it's the cleanest
   structured source. Ornamental-species ingestion (Hishtil/Prat/חוות הנוי) comes later.
   Confirm, or start ornamental-first.
4. **UI language** — *Recommend: Hebrew-primary, bilingual fields, RTL UI.* Confirm.
5. **Snapshot policy** — *Recommend: one-time scrape per source, committed as a dated
   immutable snapshot* (`data/raw/<source>/<YYYY-MM-DD>/`). Re-scrape only the volatile
   commercial layer if/when needed. Confirm.

---

## 4. Data model

### 4.1 Canonical wide table (the published artifact → `canonical.parquet`)

One row per taxon. Provenance tracked per value (see 4.3).

**Identity**
`canonical_taxon_id` (GBIF usageKey, PK) · `scientific_name` · `family` · `genus`
`cultivar` · `synonyms_latin[]` · `name_he` · `name_he_synonyms[]` · `common_names_en[]`

**Form**
`life_form` (tree|shrub|perennial|annual|groundcover|climber|succulent|bulb|grass|palm|aquatic|fern)
`evergreen_deciduous` · `growth_rate` (slow|moderate|fast)

**Dimensions**
`height_min_m` · `height_max_m` · `spread_min_m` · `spread_max_m`

**Site & tolerance (core filters)**
`sun[]` (full|part|shade) · `water_demand` (very_low|low|medium|high) ·
`salt_tolerance` (low|med|high) · `frost_sensitivity` (hardy|semi|tender) · `min_temp_c` ·
`soil_type[]` (sandy|loam|clay|chalk|rocky) · `soil_ph` (acidic|neutral|alkaline) ·
`climate_zone[]` (coastal_plain|northern_valleys|mountains|negev_arava) ·
`wind_tolerance` · `pollution_tolerance`

**Ornamental & function**
`flower_color[]` · `bloom_season[]` · `foliage_color` · `fragrant` (bool) ·
`function[]` (shade_tree|street_tree|hedge|screen|groundcover|erosion_control|pollinator|edible|specimen|container)

**Flags (high-value)**
`native_status` (native|naturalized|exotic) · `invasive_status` (listed|potential|not_listed) ·
`toxicity` (humans|pets|none) · `allergenic` (bool) · `thorns_spines` (bool)

**Commercial layer (volatile — kept separate, joined on top)**
`sold_by[]` · `pot_sizes[]` · `price_band` · `availability` · `source_urls[]`

### 4.2 EAV staging table (Stage 1)

```
source           text     -- e.g. 'wildflowers', 'hishtil'
source_id        text     -- source's own id / slug
raw_name_he      text
raw_name_lat     text
attribute        text     -- source's own field name, untranslated
value            text     -- raw value
raw_text         text     -- full raw blob for that record (audit)
scraped_at       date
```
Append-only. No upfront schema commitment — each source dumps whatever it has.

### 4.3 Provenance

For every value in the canonical table, retain `(canonical_taxon_id, field, value, source, confidence)`
in a side table so conflicts are inspectable and the front-end can show "source: X".
Reconciliation precedence (default): authority sources > ornamental catalogs > seller copy.

---

## 5. Pipeline

```
Stage 0  scrape          python, one scraper/source → data/raw/<source>/<date>/*.json
Stage 1  stage (EAV)     duckdb: load raw → staging.duckdb : eav
Stage 2  resolve         python: GBIF match → canonical_taxon_id on every eav row
Stage 3  canonical       duckdb: reconcile → wide table → export canonical.parquet
```

**Identity resolution flow (Stage 2):**
- Record has Latin name → `GET api.gbif.org/v1/species/match?name=<latin>` → usageKey.
- Record has only Hebrew → look up `crosswalk/he_lat.csv` (built from native-flora DB) →
  Latin → GBIF match.
- `matchType=NONE` or `confidence < 90` → write to `review/unresolved.csv` (manual queue).
- Cache all GBIF responses to `cache/gbif/` (keyed by name) — never call twice for same string.

Politeness: sequential or low-concurrency calls, cache aggressively, respect robots.txt /
ToS on seller sites; treat scrapes as one-time snapshots of factual catalog data.

---

## 6. Serving architecture (static)

- Build output: `web/public/canonical.parquet` (expect well under a few MB for a few-thousand-row,
  ~40-column table — trivial to ship to the browser).
- Front-end: static site, DuckDB-WASM loads the Parquet once, runs filter queries client-side.
- Deploy: Vercel or Netlify, connected to the repo; build step copies the latest Parquet in.
- No backend, no DB server, no auth. Updating data = rebuild Parquet + redeploy.

Front-end build itself is a later phase (see Phase 4) and gets its own design pass.

---

## 7. Repo structure

```
plant-db/
  spec/build-spec.md
  pipeline/
    stage0_scrape/        # one module per source
    stage1_stage.py       # raw → eav (duckdb)
    stage2_resolve.py     # gbif resolver + cache + review queue
    stage3_canonical.py   # reconcile → canonical.parquet
  crosswalk/he_lat.csv
  cache/gbif/             # cached match responses
  review/unresolved.csv   # manual review queue
  data/
    raw/<source>/<date>/  # verbatim snapshots
    staging.duckdb
  web/                    # static front-end (Phase 4)
    public/canonical.parquet
```

---

## 8. Phases & verification gates

### Phase 0 — Scaffold + GBIF resolver
**Objective:** prove identity resolution before any real ingestion.
**Tasks:** repo scaffold; `stage2_resolve.py` as a standalone function
`resolve(name_lat) -> {usageKey, scientific_name, family, confidence, matchType}`;
response caching; a 50-name test set (mix of clean Latin, typos, synonyms, Hebrew-only).
**Gate:** ≥90% of the clean/typo Latin names resolve at confidence ≥90; Hebrew-only names
route to the review queue (expected — crosswalk not built yet); zero duplicate API calls
on a second run (cache works).

### Phase 1 — Native-flora spine, end to end
**Objective:** one source flows Stage 0→3 and produces a real `canonical.parquet` slice.
**Tasks:** scraper for the native-flora source → `data/raw`; load to EAV; build
`crosswalk/he_lat.csv` from its Hebrew+Latin pairs; resolve; reconcile to wide table;
export Parquet.
**Gate:** N taxa in `canonical.parquet` with populated identity + `native_status`;
unresolved rate quantified and queued; provenance table populated; spot-check 10 taxa
by hand against the source.

### Phase 2 — Enrichment flags
**Objective:** join the high-value flags.
**Tasks:** ingest gov.il invasive-ornamental list + parks.org.il lists → set
`invasive_status`; cross native-status; (optional) red-book endangered flag.
**Gate:** every taxon has a resolved `invasive_status` (listed|potential|not_listed);
spot-check known invasives (e.g. an Acacia, a Pennisetum) flag correctly.

### Phase 3 — Commercial layer
**Objective:** seller availability joined on top of the spine.
**Tasks:** scraper(s) for 1–2 sellers (start wholesale-ornamental: Hishtil/Prat) → EAV →
resolve to existing taxa → populate `sold_by[]`, `pot_sizes[]`, `price_band`,
`availability`; flag conflicts via provenance.
**Gate:** seller rows attach to existing canonical taxa (not creating orphans);
match rate reported; conflicting attributes visible in provenance, not silently overwritten.

### Phase 4 — Static front-end
**Objective:** filterable UI on the Parquet.
**Tasks:** static app loading `canonical.parquet` via DuckDB-WASM; RTL Hebrew UI; filters
across the core attributes; deploy to Vercel/Netlify.
**Gate:** representative filter combinations return correct sets (verified against direct
DuckDB queries on the same Parquet); RTL renders correctly; cold load acceptable.

---

## 9. Risks / open items

- **Horticultural attribute coverage** is the thin spot: native DBs cover natives, sellers
  cover commerce, neither covers ornamental specs (sun/water/size/tolerance) well. Expect a
  Mediterranean-climate reference + manual curation (aunt) to fill these. Flag fields as
  `source=curated` so they're distinguishable from scraped.
- **Cultivar vs species** granularity (Decision #2) — revisit if landscape use demands
  cultivar-level rows.
- **Seller name quality** — Hebrew shop spellings will stress GBIF fuzzy matching; the review
  queue absorbs this, but watch the Phase 3 match rate.
- **ToS/robots** on seller sites — keep scrapes light, snapshot-only, factual fields.

---

## 10. Next action

Confirm/adjust Section 3, then begin Phase 0. Phase 0 is a weekend: resolver + cache +
50-name test, no real ingestion until the gate passes.
