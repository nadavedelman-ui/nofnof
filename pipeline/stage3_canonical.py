"""Stage 3 — resolve identity, reconcile EAV into the canonical wide table, export Parquet.

Flow:
  1. Pivot EAV back to one attribute-dict per source taxon.
  2. Resolve each Latin name via GBIF (Stage 2) → canonical_taxon_id. Unresolved → review queue.
  3. Reconcile resolved taxa into one row per canonical_taxon_id (identity + native_status
     + life_form + name_he + common_en + raw descriptive carry-overs).
  4. Record provenance per value. Export web/public/canonical.parquet.
"""
import csv, json
from pathlib import Path

import duckdb

from pipeline.stage2_resolve import resolve, STATS

STAGING_DB = "data/staging.duckdb"
PARQUET_OUT = Path("web/public/canonical.parquet")
REVIEW_OUT = Path("review/unresolved.csv")
SOURCE = "wildflowers"

# raw Hebrew field labels (from the source pages)
F_NAME_HE, F_NAME_LAT = "שם הצמח", "שם מדעי"
F_COMMON_EN, F_LIFE_FORM = "שם עממי", "צורת חיים"
F_BLOOM, F_HABITAT, F_DISTRIB = "עונת הפריחה", "בית גידול", "תפוצה בארץ"

# צורת חיים (Hebrew) → canonical life_form enum. Checked in order; first substring hit wins,
# so more-specific terms precede the general ones (עשבוני before עשב).
LIFE_FORM_MAP = [
    ("עץ", "tree"), ("דקל", "palm"), ("מטפס", "climber"),
    ("בן-שיח", "shrub"), ("שיח", "shrub"),
    ("חד-שנתי", "annual"), ("דו-שנתי", "annual"),
    ("גיאופיט", "bulb"), ("בצל", "bulb"),
    ("סוקולנט", "succulent"), ("בשרני", "succulent"),
    ("שרכים", "fern"), ("שרך", "fern"), ("דגן", "grass"),
    ("עשבוני", "perennial"), ("רב-שנתי", "perennial"),
    ("מים", "aquatic"), ("עשב", "grass"),
]


def bare_binomial(name: str | None) -> str:
    """Genus + epithet, lowercased — drops the author string for synonym comparison."""
    return " ".join((name or "").lower().split()[:2])


def map_life_form(he: str | None) -> str | None:
    if not he:
        return None
    for token, enum in LIFE_FORM_MAP:
        if token in he:
            return enum
    return None


def pivot_taxa(con) -> list[dict]:
    """One dict per source taxon: {source_id, name_he, name_lat, attrs:{label:value}}."""
    rows = con.execute("""
        SELECT source_id, raw_name_he, raw_name_lat, attribute, value
        FROM eav ORDER BY source_id
    """).fetchall()
    taxa: dict[str, dict] = {}
    for sid, he, lat, attr, val in rows:
        t = taxa.setdefault(sid, {"source_id": sid, "name_he": he,
                                  "name_lat": lat, "attrs": {}})
        t["attrs"].setdefault(attr, val)
    return list(taxa.values())


def build(con, invasive=None) -> dict:
    taxa = pivot_taxa(con)
    inv = invasive or {"listed": set(), "potential": set()}

    # ---- Stage 2: resolve identity -------------------------------------------------
    canonical: dict[int, dict] = {}      # canonical_taxon_id -> reconciled row
    provenance: list[tuple] = []         # (taxon_id, field, value, source, confidence)
    review: list[dict] = []
    resolved_n = 0

    for t in taxa:
        lat = t["name_lat"]
        r = resolve(lat or "", "lat")
        if r["status"] != "resolved":
            review.append({"source_id": t["source_id"], "name_he": t["name_he"],
                           "name_lat": lat, "reason": r["reason"],
                           "confidence": r["confidence"], "matchType": r["matchType"]})
            continue
        resolved_n += 1
        tid = r["canonical_taxon_id"]
        a = t["attrs"]
        life_form = map_life_form(a.get(F_LIFE_FORM))
        common_en = [a[F_COMMON_EN]] if a.get(F_COMMON_EN) else []
        syns = [lat] if lat and bare_binomial(lat) != bare_binomial(r["scientific_name"]) else []

        if tid in inv["listed"]:
            inv_status, inv_source = "listed", "curated-il-invasive"
        elif tid in inv["potential"]:
            inv_status, inv_source = "potential", "griis-israel"
        else:
            inv_status, inv_source = "not_listed", "default"

        row = canonical.get(tid)
        if row is None:                  # first source record for this taxon
            row = {
                "canonical_taxon_id": tid,
                "scientific_name": r["scientific_name"],
                "family": r["family"], "genus": r["genus"],
                "name_he": t["name_he"],
                "common_names_en": common_en,
                "synonyms_latin": syns,
                "life_form": life_form,
                "native_status": "native",            # spine: present in IL wild-flora DB
                "invasive_status": inv_status,
                "bloom_months_he": a.get(F_BLOOM),
                "habitat_he": a.get(F_HABITAT),
                "distribution_il": a.get(F_DISTRIB),
                "source_id": t["source_id"],
                "source_url": f"https://www.kkl.org.il/wild-flower/hebrew/plant.asp?ID={t['source_id']}",
            }
            canonical[tid] = row
            # provenance: identity from GBIF (carry its confidence); facts from source
            gc = r["confidence"]
            provenance += [
                (tid, "scientific_name", r["scientific_name"], "gbif", gc),
                (tid, "family", r["family"], "gbif", gc),
                (tid, "genus", r["genus"], "gbif", gc),
                (tid, "name_he", t["name_he"], SOURCE, 100),
                (tid, "native_status", "native", SOURCE, 100),
                (tid, "invasive_status", inv_status, inv_source, 100),
            ]
            if life_form:
                provenance.append((tid, "life_form", a.get(F_LIFE_FORM), SOURCE, 100))
        else:                            # merge: collect synonyms + fill empty scalar gaps
            for s in syns:
                if s not in row["synonyms_latin"]:
                    row["synonyms_latin"].append(s)
            for en in common_en:
                if en not in row["common_names_en"]:
                    row["common_names_en"].append(en)
            gaps = {"life_form": life_form, "name_he": t["name_he"],
                    "bloom_months_he": a.get(F_BLOOM), "habitat_he": a.get(F_HABITAT),
                    "distribution_il": a.get(F_DISTRIB)}
            for k, v in gaps.items():
                if not row[k] and v:
                    row[k] = v

    # ---- write review queue --------------------------------------------------------
    REVIEW_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(REVIEW_OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["source_id", "name_he", "name_lat",
                                          "reason", "confidence", "matchType"])
        w.writeheader(); w.writerows(review)

    # ---- load canonical + provenance into DuckDB, export Parquet --------------------
    rows = list(canonical.values())
    con.execute("DROP TABLE IF EXISTS canonical")
    con.execute("""
        CREATE TABLE canonical (
            canonical_taxon_id BIGINT,
            scientific_name TEXT, family TEXT, genus TEXT,
            name_he TEXT, common_names_en TEXT[], synonyms_latin TEXT[],
            life_form TEXT, native_status TEXT, invasive_status TEXT,
            bloom_months_he TEXT, habitat_he TEXT, distribution_il TEXT,
            source_id TEXT, source_url TEXT
        )
    """)
    con.executemany(
        "INSERT INTO canonical VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r["canonical_taxon_id"], r["scientific_name"], r["family"], r["genus"],
          r["name_he"], r["common_names_en"], r["synonyms_latin"], r["life_form"],
          r["native_status"], r["invasive_status"], r["bloom_months_he"], r["habitat_he"],
          r["distribution_il"], r["source_id"], r["source_url"]) for r in rows])

    con.execute("DROP TABLE IF EXISTS provenance")
    con.execute("""CREATE TABLE provenance (
            canonical_taxon_id BIGINT, field TEXT, value TEXT, source TEXT, confidence INTEGER)""")
    con.executemany("INSERT INTO provenance VALUES (?,?,?,?,?)", provenance)

    PARQUET_OUT.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY canonical TO '{PARQUET_OUT.as_posix()}' (FORMAT PARQUET)")

    total = len(taxa)
    inv_breakdown = {"listed": 0, "potential": 0, "not_listed": 0}
    for r in rows:
        inv_breakdown[r["invasive_status"]] += 1
    return {"taxa_in": total, "resolved": resolved_n, "canonical_rows": len(rows),
            "review": len(review), "provenance_rows": len(provenance),
            "invasive": inv_breakdown, "cache": dict(STATS)}


def run(invasive=None) -> dict:
    con = duckdb.connect(STAGING_DB)
    try:
        res = build(con, invasive)
    finally:
        con.close()
    print(f"Stage 3: {res['taxa_in']} source taxa → {res['resolved']} resolved "
          f"→ {res['canonical_rows']} canonical rows (deduped); "
          f"{res['review']} to review; {res['provenance_rows']} provenance rows.")
    print(f"  parquet: {PARQUET_OUT}")
    print(f"  review : {REVIEW_OUT}")
    return res


if __name__ == "__main__":
    run()
