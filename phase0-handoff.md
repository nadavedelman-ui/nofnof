# Phase 0 — Handoff: GBIF resolver + cache + gate

**Parent spec:** `plant-db-build-spec.md` (Section 8, Phase 0).
**Goal of this phase only:** prove identity resolution works before any real ingestion.

## Scope guardrail (read first)

DO build: the resolver, the file cache, the test harness, and run the gate.
DO NOT, in this phase: scrape any source, build the Hebrew↔Latin crosswalk, write to DuckDB,
or touch Stage 1/3. Hebrew-only names are *expected* to route to the review queue here —
that is a pass, not a bug. Stop at the gate and report results.

## Prereqs

- Python 3.11+, `pip install requests`.
- Network access to `https://api.gbif.org` (works in your local env; the API is open, no key).
- `gbif_test_set.csv` (provided) placed at repo root.

## Deliverables

```
pipeline/stage2_resolve.py   # resolver + cache (reference impl below — keep the GBIF contract)
test_resolver.py             # gate harness (below)
cache/gbif/                  # auto-created; cached match responses
gbif_test_set.csv            # provided
```

## GBIF contract (so the response shape isn't guessed)

`GET https://api.gbif.org/v1/species/match?name=<name>&kingdom=Plantae&strict=false`

Relevant response fields:
- `usageKey` (int) — taxon key for the matched name.
- `acceptedUsageKey` (int, present when the match is a synonym) — key of the accepted taxon.
- `scientificName`, `accepted` (accepted name string, present when synonym).
- `rank` (SPECIES | GENUS | …), `status` (ACCEPTED | SYNONYM | …).
- `matchType` (EXACT | FUZZY | HIGHERRANK | NONE), `confidence` (0–100).
- `kingdom`, `family`, `genus`.

Resolver rules:
1. `kingdom=Plantae` hint disambiguates plant/animal homonyms (Prunella, Phoenix).
2. Collapse synonyms: canonical key = `acceptedUsageKey or usageKey`; canonical name = `accepted or scientificName`.
3. `matchType=HIGHERRANK` or `rank != SPECIES/SUBSPECIES/VARIETY/FORM` → review (we want species).
4. `confidence < 90` or `matchType=NONE` → review.
5. `kingdom != Plantae` in the response → review (non-plant).
6. Cultivar fallback: if first match is NONE/low, strip `'…'`/`"…"` and retry.
7. Cache every call keyed by lowercased name; second run must make zero network calls.

## Reference implementation — `pipeline/stage2_resolve.py`

```python
import hashlib, json, re, time, unicodedata
from pathlib import Path
import requests

GBIF_MATCH = "https://api.gbif.org/v1/species/match"
CACHE_DIR = Path("cache/gbif"); CACHE_DIR.mkdir(parents=True, exist_ok=True)
CONFIDENCE_THRESHOLD = 90
REQUEST_SLEEP = 0.05
SPECIES_RANKS = {"SPECIES", "SUBSPECIES", "VARIETY", "FORM"}
CULTIVAR_RE = re.compile(r"\s*'[^']*'|\s*\"[^\"]*\"")
STATS = {"hits": 0, "misses": 0}

def normalize_name(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", s).strip())

def _is_latin(s: str) -> bool:
    return bool(re.search(r"[A-Za-z]", s)) and not re.search(r"[\u0590-\u05FF]", s)

def _cache_path(name: str) -> Path:
    return CACHE_DIR / f"{hashlib.sha1(name.lower().encode()).hexdigest()[:16]}.json"

def _gbif_call(name: str) -> dict:
    p = _cache_path(name)
    if p.exists():
        STATS["hits"] += 1
        return json.loads(p.read_text(encoding="utf-8"))
    STATS["misses"] += 1
    r = requests.get(GBIF_MATCH,
                     params={"name": name, "kingdom": "Plantae", "strict": "false"},
                     timeout=20)
    r.raise_for_status()
    data = r.json()
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    time.sleep(REQUEST_SLEEP)
    return data

def match_gbif(name: str) -> dict:
    name = normalize_name(name)
    data = _gbif_call(name)
    if data.get("matchType", "NONE") == "NONE" or data.get("confidence", 0) < CONFIDENCE_THRESHOLD:
        stripped = normalize_name(CULTIVAR_RE.sub("", name))
        if stripped and stripped != name:
            data = _gbif_call(stripped)
    return data

def resolve(input_name: str, input_lang: str = "lat") -> dict:
    name = normalize_name(input_name)
    out = {"input": input_name, "status": "review", "reason": None,
           "canonical_taxon_id": None, "scientific_name": None, "family": None,
           "genus": None, "confidence": None, "matchType": None, "gbif_status": None}
    if not name:
        out["reason"] = "empty"; return out
    if input_lang == "he" or not _is_latin(name):
        out["reason"] = "hebrew_no_crosswalk"; return out   # Phase 0: crosswalk not built yet

    d = match_gbif(name)
    out.update(matchType=d.get("matchType", "NONE"), confidence=d.get("confidence", 0),
               gbif_status=d.get("status"), family=d.get("family"), genus=d.get("genus"))
    if out["matchType"] == "NONE":
        out["reason"] = "no_match"; return out
    if d.get("kingdom") and d["kingdom"] != "Plantae":
        out["reason"] = f"non_plant:{d.get('kingdom')}"; return out
    if out["matchType"] == "HIGHERRANK" or d.get("rank") not in SPECIES_RANKS:
        out["reason"] = f"higher_rank:{d.get('rank')}"; return out
    if out["confidence"] < CONFIDENCE_THRESHOLD:
        out["reason"] = f"low_confidence:{out['confidence']}"; return out

    out.update(status="resolved",
               canonical_taxon_id=d.get("acceptedUsageKey") or d.get("usageKey"),
               scientific_name=d.get("accepted") or d.get("scientificName"))
    return out
```

## Gate harness — `test_resolver.py`

```python
import csv
from pipeline.stage2_resolve import resolve, STATS

def run(path="gbif_test_set.csv"):
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    results = [(r, resolve(r["input_name"], r["input_lang"])) for r in rows]

    res_rows = [(r, x) for r, x in results if r["expected_bucket"] == "resolve"]
    rev_rows = [(r, x) for r, x in results if r["expected_bucket"] == "review"]
    res_ok = [(r, x) for r, x in res_rows if x["status"] == "resolved"]
    rev_ok = [(r, x) for r, x in rev_rows if x["status"] == "review"]

    prunella = next((x for r, x in results if r["input_name"] == "Prunella vulgaris"), {})
    felis    = next((x for r, x in results if r["input_name"] == "Felis catus"), {})

    print(f"GATE resolve : {len(res_ok)}/{len(res_rows)} = {len(res_ok)/len(res_rows):.0%}  (need >=90%)")
    print(f"GATE review  : {len(rev_ok)}/{len(rev_rows)} = {len(rev_ok)/len(rev_rows):.0%}  (need 100%)")
    print(f"Homonym Prunella stays Plantae : {'PASS' if prunella.get('status')=='resolved' else 'FAIL'}")
    print(f"Animal Felis catus rejected    : {'PASS' if felis.get('status')=='review' else 'FAIL'}")
    print(f"Cache: hits={STATS['hits']} misses={STATS['misses']}  (run twice; 2nd run misses must be 0)")

    print("\n-- resolve misses --")
    for r, x in res_rows:
        if x["status"] != "resolved":
            print(f"  [{r['category']}] {r['input_name']!r}: {x['reason']} conf={x['confidence']} mt={x['matchType']}")
    print("\n-- review leaks (resolved but expected review) --")
    for r, x in rev_rows:
        if x["status"] != "review":
            print(f"  [{r['category']}] {r['input_name']!r} -> {x['scientific_name']} ({x['canonical_taxon_id']})")

if __name__ == "__main__":
    run()
```

## Verification gate (Phase 0 passes iff all hold)

1. **resolve-bucket ≥ 90%** of 34 names resolve to a species-level key at confidence ≥ 90
   (clean + typo + synonym + cultivar + the Prunella homonym).
2. **review-bucket = 100%** of 16 names route to review (10 Hebrew-only, 3 higher_rank, 3 junk).
3. **Prunella vulgaris** resolves as a plant (kingdom hint beat the bird-genus homonym).
4. **Felis catus** does NOT yield a plant key (kingdom guard works).
5. **Cache**: run the harness twice; on the second run `misses == 0`.

## How to read the interesting rows

- Synonyms/reclassifications (Plumbago capensis, Ficus retusa/nitida, Pennisetum setaceum,
  Tecomaria, Aptenia, Senecio cineraria) should collapse to an accepted key. Whether GBIF
  reports `status=SYNONYM` or already treats the name as accepted is backbone-dependent —
  either way it counts as resolved. Rosmarinus officinalis is the most likely to still come
  back as accepted rather than collapsing to *Salvia rosmarinus*; that's fine.
- A resolve miss on a *typo* row is the meaningful signal — it means fuzzy matching or the
  threshold needs tuning. Note any in the report.

## Run

```bash
pip install requests
python test_resolver.py     # 1st run populates cache
python test_resolver.py     # 2nd run: confirm misses == 0
```

## Stop condition

Stop at the gate. Report: the four PASS/FAIL lines, the resolve %, and any resolve-misses /
review-leaks. Do not proceed to Phase 1 (native-flora ingestion + crosswalk) until the gate
is green and I've seen the resolve-miss list.

## Forward note (not this phase)

For the full pipeline (thousands of names) swap the sequential loop for a
`ThreadPoolExecutor(max_workers=8)` over `resolve`; the cache makes it idempotent so reruns
are free. Phase 0 stays sequential.
