"""Phase 4+ — resolve a Creative-Commons image per taxon and join it onto canonical.parquet.

Two licensed sources, both keyed off identity we already have:
  1. Wikidata P225 (taxon name) → P18 (image) → Wikimedia Commons imageinfo (thumb+license+author).
     Batched by scientific name. Clean "lead" photos with proper attribution.
  2. GBIF occurrence media (StillImage) by canonical_taxon_id — fallback; broad coverage
     (much of it iNaturalist CC). Carries license + rightsHolder.
Taxa with neither keep the botanical SVG placeholder in the UI. Everything is cached, so
re-runs are free; only misses are retried.
"""
import json, re, time, html, urllib.parse
from pathlib import Path

import requests, duckdb

from pipeline.stage3_canonical import PARQUET_OUT

UA = "NofnofPlantDB/1.0 (Israeli landscape plant database; mailto:nadav@fandf.co.il)"
WD_SPARQL = "https://query.wikidata.org/sparql"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
GBIF_OCC = "https://api.gbif.org/v1/occurrence/search"
IMG_DIR = Path("data/raw/images/2026-06-25"); IMG_DIR.mkdir(parents=True, exist_ok=True)
GBIF_DIR = IMG_DIR / "gbif"; GBIF_DIR.mkdir(exist_ok=True)
WD_CACHE = IMG_DIR / "wikidata_p18.json"
RESOLVED = IMG_DIR / "resolved.json"

S = requests.Session(); S.headers.update({"User-Agent": UA})

# Commons filenames that signal a drawing/engraving/herbarium rather than a live photo —
# when matched we prefer a GBIF occurrence PHOTO instead (illustration kept only as last resort).
ILLU = re.compile(
    r"illustration|drawing|sketch|lithograph|engraving|\bplate\b|\btaf\b|\bfig\.|\bpl\.|"
    r"k[oö]hler|sturm|lindman|thom[eé]|masclef|\bflora\b.*\b(batava|von|der|de)\b|"
    r"english botany|curtis|botanical magazine|nordens flora|deutschlands flora|"
    r"atlas|planche|herbarium|herbier|specimen|\.svg$|kunstformen|1[678]\d\d|190\d",
    re.I)


def bare(name: str) -> str:
    toks = (name or "").split()
    return " ".join(toks[:2]) if len(toks) >= 2 else (name or "")


def https(u):
    return u.replace("http://", "https://") if u else u


def cleanhtml(s):
    if not s: return None
    s = re.sub(r"<[^>]+>", "", html.unescape(s))
    return (re.sub(r"\s+", " ", s).strip()[:90]) or None


def _filename(filepath_url):
    m = re.search(r"Special:FilePath/(.+)$", filepath_url or "")
    return urllib.parse.unquote(m.group(1)) if m else None


def wikidata_p18(names):
    """{scientific_bare_name: Commons FilePath URL or None}, batched + cached."""
    cache = json.loads(WD_CACHE.read_text(encoding="utf-8")) if WD_CACHE.exists() else {}
    todo = [n for n in names if n and n not in cache]
    for i in range(0, len(todo), 120):
        batch = todo[i:i+120]
        values = " ".join('"%s"' % n.replace("\\", "\\\\").replace('"', '\\"') for n in batch)
        q = f'SELECT ?n ?img WHERE {{ VALUES ?n {{ {values} }} ?t wdt:P225 ?n; wdt:P18 ?img. }}'
        try:
            r = S.post(WD_SPARQL, data={"query": q, "format": "json"},
                       headers={"Accept": "application/sparql-results+json"}, timeout=90)
            r.raise_for_status()
            found = {}
            for b in r.json()["results"]["bindings"]:
                found.setdefault(b["n"]["value"], b["img"]["value"])
            for n in batch: cache[n] = found.get(n)
        except Exception as e:
            print("  ! wikidata batch:", e)
            for n in batch: cache.setdefault(n, None)
        WD_CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        print(f"  wikidata {min(i+120,len(todo))}/{len(todo)} resolved")
        time.sleep(1.0)
    return cache


def commons_info(filenames):
    """{filename: {thumb,license,artist,page}} via batched imageinfo (50/req)."""
    out = {}
    fns = sorted({f for f in filenames if f})
    for i in range(0, len(fns), 50):
        batch = fns[i:i+50]
        params = {"action": "query", "format": "json", "prop": "imageinfo",
                  "iiprop": "url|extmetadata", "iiurlwidth": "640",
                  "titles": "|".join("File:" + f for f in batch)}
        try:
            r = S.get(COMMONS_API, params=params, timeout=50); r.raise_for_status()
            for p in r.json().get("query", {}).get("pages", {}).values():
                ii = (p.get("imageinfo") or [{}])[0]
                ext = ii.get("extmetadata", {}) or {}
                out[p.get("title", "").replace("File:", "")] = {
                    "thumb": https(ii.get("thumburl") or ii.get("url")),
                    "license": (ext.get("LicenseShortName", {}) or {}).get("value"),
                    "artist": cleanhtml((ext.get("Artist", {}) or {}).get("value")),
                    "page": ii.get("descriptionurl"),
                }
        except Exception as e:
            print("  ! commons batch:", e)
        time.sleep(0.4)
    return out


def gbif_image(taxon_key):
    """First StillImage occurrence-media for a taxon key (cached per taxon)."""
    p = GBIF_DIR / f"{taxon_key}.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    rec = None
    try:
        r = S.get(GBIF_OCC, params={"taxonKey": taxon_key, "mediaType": "StillImage", "limit": 5}, timeout=40)
        for occ in r.json().get("results", []):
            if not isinstance(occ, dict): continue
            for m in (occ.get("media") or []):
                if isinstance(m, dict) and m.get("identifier"):
                    rec = {"thumb": https(m["identifier"]), "license": m.get("license"),
                           "artist": m.get("rightsHolder") or m.get("creator"),
                           "page": occ.get("references")}
                    break
            if rec: break
    except Exception as e:
        print("  ! gbif occ", taxon_key, e)
    p.write_text(json.dumps(rec, ensure_ascii=False), encoding="utf-8")
    time.sleep(0.05)
    return rec


def join_into_parquet(resolved):
    con = duckdb.connect("data/staging.duckdb")
    con.execute(f"CREATE OR REPLACE TABLE canon AS SELECT * FROM '{PARQUET_OUT.as_posix()}'")
    cols = [("image_thumb", "TEXT"), ("image_source", "TEXT"), ("image_license", "TEXT"),
            ("image_credit", "TEXT"), ("image_page", "TEXT")]
    for c, t in cols:
        con.execute(f"ALTER TABLE canon DROP COLUMN IF EXISTS {c}")
        con.execute(f"ALTER TABLE canon ADD COLUMN {c} {t}")
    con.executemany("""UPDATE canon SET image_thumb=?, image_source=?, image_license=?,
                       image_credit=?, image_page=? WHERE canonical_taxon_id=?""",
                    [(r["thumb"], r["source"], r.get("license"), r.get("credit"),
                      r.get("page"), int(tid)) for tid, r in resolved.items()])
    con.execute("DELETE FROM provenance WHERE field='image'")
    con.executemany("INSERT INTO provenance VALUES (?,?,?,?,?)",
                    [(int(tid), "image", r["thumb"], r["source"], 100) for tid, r in resolved.items()])
    con.execute(f"COPY canon TO '{PARQUET_OUT.as_posix()}' (FORMAT PARQUET)")
    con.close()


def run():
    con = duckdb.connect()
    taxa = con.execute(f"SELECT CAST(canonical_taxon_id AS BIGINT), scientific_name FROM '{PARQUET_OUT.as_posix()}'").fetchall()
    con.close()

    names = sorted({bare(s) for _, s in taxa if s})
    print(f"resolving images for {len(taxa)} taxa ({len(names)} distinct names)…")
    wd = wikidata_p18(names)
    info = commons_info([_filename(u) for u in wd.values() if u])

    resolved = {}; nw = ng = nillu = 0
    for tid, sci in taxa:
        wiki, wiki_illu = None, False
        fp = wd.get(bare(sci))
        if fp:
            fn = _filename(fp); meta = info.get(fn)
            if meta and meta.get("thumb"):
                wiki = {"thumb": meta["thumb"], "source": "wikimedia",
                        "license": meta.get("license"), "credit": meta.get("artist"), "page": meta.get("page")}
                wiki_illu = bool(ILLU.search(fn or ""))
        gb = None
        if wiki is None or wiki_illu:               # only spend a GBIF call when we lack a photo
            g = gbif_image(tid)
            if g and g.get("thumb"):
                gb = {"thumb": g["thumb"], "source": "gbif",
                      "license": g.get("license"), "credit": g.get("artist"), "page": g.get("page")}
        if wiki and not wiki_illu:
            rec = wiki; nw += 1                      # real Commons photo (preferred)
        elif gb:
            rec = gb; ng += 1                        # GBIF photo (beats a Wikidata illustration)
        elif wiki:
            rec = wiki; nw += 1; nillu += 1          # illustration kept only when no photo exists
        else:
            rec = None
        if rec:
            resolved[str(tid)] = rec
    RESOLVED.write_text(json.dumps(resolved, ensure_ascii=False, indent=1), encoding="utf-8")
    join_into_parquet(resolved)
    have = len(resolved); total = len(taxa)
    print(f"\nimages: {have}/{total} = {have/total:.0%}  (wikimedia {nw} [incl {nillu} illustration-fallback], gbif {ng}, placeholder {total-have})")
    return {"total": total, "have": have, "wikimedia": nw, "gbif": ng, "illustration_fallback": nillu}


if __name__ == "__main__":
    run()
