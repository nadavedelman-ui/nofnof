"""Stage 1 — load raw snapshots into the DuckDB EAV staging table.

Schema-on-read: every (attribute, value) pair from each raw record becomes one EAV
row. No upfront schema commitment — reconciliation happens downstream in Stage 3.
Also emits crosswalk/he_lat.csv (the Hebrew↔Latin pairs) — a Phase 1 deliverable.
"""
import csv, json
from pathlib import Path

import duckdb

STAGING_DB = "data/staging.duckdb"
RAW_GLOB = "data/raw/wildflowers"
CROSSWALK = Path("crosswalk/he_lat.csv")

EAV_DDL = """
CREATE TABLE eav (
    source        TEXT,
    source_id     TEXT,
    raw_name_he   TEXT,
    raw_name_lat  TEXT,
    attribute     TEXT,   -- source's own field name, untranslated
    value         TEXT,   -- raw value
    raw_text      TEXT,   -- full raw blob for that record (audit)
    scraped_at    DATE
);
"""


def _iter_raw_records():
    for p in sorted(Path(RAW_GLOB).glob("*/plant_*.json")):
        yield json.loads(p.read_text(encoding="utf-8"))


def load_eav(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DROP TABLE IF EXISTS eav")
    con.execute(EAV_DDL)
    rows, n_records = [], 0
    for rec in _iter_raw_records():
        n_records += 1
        raw_text = json.dumps(rec["fields"], ensure_ascii=False)
        for attr, val in rec["fields"].items():
            rows.append((rec["source"], rec["source_id"], rec.get("name_he"),
                         rec.get("name_lat"), attr, val, raw_text, rec["scraped_at"]))
    con.executemany("INSERT INTO eav VALUES (?,?,?,?,?,?,?,?)", rows)
    print(f"Stage 1: loaded {len(rows)} EAV rows from {n_records} raw records "
          f"into {STAGING_DB} : eav")
    return n_records


def write_crosswalk(con: duckdb.DuckDBPyConnection) -> int:
    """Distinct (name_he, name_lat, source_id) identity pairs → crosswalk/he_lat.csv."""
    pairs = con.execute("""
        SELECT DISTINCT raw_name_he, raw_name_lat, source_id
        FROM eav
        WHERE raw_name_he IS NOT NULL AND raw_name_lat IS NOT NULL
        ORDER BY source_id
    """).fetchall()
    CROSSWALK.parent.mkdir(parents=True, exist_ok=True)
    with open(CROSSWALK, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["name_he", "name_lat", "source", "source_id"])
        for he, lat, sid in pairs:
            w.writerow([he, lat, "wildflowers", sid])
    print(f"Stage 1: wrote {len(pairs)} Hebrew↔Latin pairs to {CROSSWALK}")
    return len(pairs)


def run() -> int:
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect(STAGING_DB)
    try:
        n = load_eav(con)
        write_crosswalk(con)
        return n
    finally:
        con.close()


if __name__ == "__main__":
    run()
