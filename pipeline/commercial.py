"""Phase 3 — commercial layer joined ON TOP of the canonical spine.

Reads the published canonical.parquet (does NOT rebuild identity), matches seller
products (Hebrew-only) to existing taxa via the he→spine crosswalk, and attaches the
volatile commercial fields: sold_by[], pot_sizes[], price, price_band, availability,
source_urls[]. Seller items that don't match an existing taxon go to a review file —
we never create orphan canonical rows. Every seller value is recorded in provenance,
so multiple listings for one taxon are preserved, not silently overwritten.
"""
import csv, json, re, unicodedata
from pathlib import Path

import duckdb

from pipeline.stage3_canonical import PARQUET_OUT

STAGING_DB = "data/staging.duckdb"
AZUR_RAW = Path("data/raw/azur/2026-06-25")
UNMATCHED_OUT = Path("review/seller_unmatched.csv")
SELLER = "azur"

NIQQUD = re.compile(r"[֑-ׇ]")
POT_TOKENS = re.compile(r'קוטר\s*\d+\s*ס(?:"|״)?מ|\d+\s*ס(?:"|״)?מ|\d+\s*ליטר|עציץ\s*\d+')
PUNCT = re.compile(r'[\'"׳״\-–,.()\[\]/]+')


def norm_he(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = NIQQUD.sub("", s)
    s = POT_TOKENS.sub(" ", s)
    s = PUNCT.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def price_band(p: float | None) -> str | None:
    if p is None:
        return None
    return ("budget" if p < 40 else "mid" if p < 100
            else "premium" if p < 200 else "luxury")


def _load_canonical_names(con):
    """Build the he→spine index: exact normalized map + genus(first-token) index.

    Precision-first: generic fuzzy matching produces same-genus/wrong-species false
    friends (Abelia≈willow, raspberry≈unrelated), so we only accept an exact name or a
    UNIQUE genus-containment (a seller genus trade-name like 'הדס' resolving to the lone
    'הדס מצוי' in the spine)."""
    rows = con.execute("SELECT canonical_taxon_id, name_he FROM canon WHERE name_he IS NOT NULL").fetchall()
    exact, by_genus = {}, {}
    for tid, he in rows:
        n = norm_he(he)
        if not n:
            continue
        exact.setdefault(n, tid)
        by_genus.setdefault(n.split()[0], []).append((n, tid))
    return exact, by_genus


def _match(name_he, exact, by_genus):
    """Return (canonical_taxon_id, how) or (None, None)."""
    n = norm_he(name_he)
    if not n:
        return None, None
    if n in exact:
        return exact[n], "exact"
    toks, first = set(n.split()), n.split()[0]
    hits = {tid for sn, tid in by_genus.get(first, []) if toks <= set(sn.split())}
    if len(hits) == 1:                              # unique genus-level trade name
        return next(iter(hits)), "genus_containment"
    return None, None


def run() -> dict:
    con = duckdb.connect(STAGING_DB)
    try:
        con.execute(f"CREATE OR REPLACE TABLE canon AS SELECT * FROM '{PARQUET_OUT.as_posix()}'")
        rows_before = con.execute("SELECT count(*) FROM canon").fetchone()[0]
        # idempotent: a prior Phase 3 run may have left these columns / provenance behind
        for col, typ in [("sold_by", "TEXT[]"), ("pot_sizes", "TEXT[]"),
                         ("price_ils", "DOUBLE"), ("price_band", "TEXT"),
                         ("availability", "TEXT"), ("source_urls", "TEXT[]")]:
            con.execute(f"ALTER TABLE canon DROP COLUMN IF EXISTS {col}")
            con.execute(f"ALTER TABLE canon ADD COLUMN {col} {typ}")
        con.execute("DELETE FROM provenance WHERE source = ?", [SELLER])

        exact, by_genus = _load_canonical_names(con)

        products = [json.loads(p.read_text(encoding="utf-8"))
                    for p in sorted(AZUR_RAW.glob("*.json"))]
        attach: dict[int, dict] = {}
        provenance, unmatched = [], []
        matched_products = 0

        for pr in products:
            tid, how = _match(pr["name_he"], exact, by_genus)
            if tid is None:
                unmatched.append(pr)
                continue
            matched_products += 1
            a = attach.setdefault(tid, {"sold_by": set(), "pot_sizes": set(),
                                        "prices": [], "avail": set(), "urls": set(),
                                        "listings": 0})
            a["sold_by"].add(SELLER)
            a["pot_sizes"].update(pr.get("pot_sizes") or [])
            if pr.get("price") is not None:
                a["prices"].append(pr["price"])
            a["avail"].add(pr.get("availability"))
            a["urls"].add(pr["url"])
            a["listings"] += 1
            # provenance: one row per seller value (conflicts preserved, not overwritten)
            provenance.append((tid, "sold_by", SELLER, SELLER, 100))
            if pr.get("price") is not None:
                provenance.append((tid, "price_ils", str(pr["price"]), SELLER, 100))
            for ps in (pr.get("pot_sizes") or []):
                provenance.append((tid, "pot_sizes", ps, SELLER, 100))
            provenance.append((tid, "availability", pr.get("availability"), SELLER, 100))
            provenance.append((tid, "match_method", how, SELLER, 100))

        # write attached fields back onto canon
        for tid, a in attach.items():
            min_price = min(a["prices"]) if a["prices"] else None
            avail = "in_stock" if "in_stock" in a["avail"] else "out_of_stock"
            con.execute("""UPDATE canon SET sold_by=?, pot_sizes=?, price_ils=?,
                           price_band=?, availability=?, source_urls=? WHERE canonical_taxon_id=?""",
                        [sorted(a["sold_by"]), sorted(a["pot_sizes"]), min_price,
                         price_band(min_price), avail, sorted(a["urls"]), tid])

        # provenance side table (append)
        con.executemany("INSERT INTO provenance VALUES (?,?,?,?,?)", provenance)

        # unmatched seller items -> review (NO orphan canonical rows)
        UNMATCHED_OUT.parent.mkdir(parents=True, exist_ok=True)
        with open(UNMATCHED_OUT, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["source_id", "name_he", "category", "price", "url"])
            for pr in unmatched:
                w.writerow([pr["source_id"], pr["name_he"], pr.get("category"),
                            pr.get("price"), pr["url"]])

        # re-export the published artifact
        con.execute(f"COPY canon TO '{PARQUET_OUT.as_posix()}' (FORMAT PARQUET)")

        rows_after = con.execute("SELECT count(*) FROM canon").fetchone()[0]
        conflicts = sum(1 for a in attach.values() if len(a["prices"]) > 1
                        or a["listings"] > 1)
        return {"seller_products": len(products), "matched": matched_products,
                "unmatched": len(unmatched), "taxa_with_seller": len(attach),
                "multi_listing_taxa": conflicts, "provenance_added": len(provenance),
                "canon_rows_before": rows_before, "canon_rows_after": rows_after}
    finally:
        con.close()


if __name__ == "__main__":
    res = run()
    print(res)
