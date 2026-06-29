"""Phase 2 — invasive-status data layer.

Two authoritative inputs, both reduced to GBIF backbone keys so they join directly on
canonical_taxon_id (which IS the GBIF usageKey):

  * GRIIS-Israel (GBIF checklist 6b45e498-…) — the alien / introduced plant set → 'potential'
  * curated IL invasive list (data/reference/il_invasive_curated.csv, from Dufour-Dror /
    INPA literature) — recognized invasives → 'listed'

A taxon is 'listed' if it is on the curated invasive list, else 'potential' if it is an
introduced alien per GRIIS, else 'not_listed' (the native majority).
"""
import json
from pathlib import Path

import requests

from pipeline.stage2_resolve import resolve

GRIIS_KEY = "6b45e498-23e8-4e39-8620-77011495e42c"
GRIIS_RAW = Path("data/raw/griis/2026-06-25")
CURATED_CSV = Path("data/reference/il_invasive_curated.csv")
PLANTAE = "Plantae"


def fetch_griis_plants() -> dict:
    """Paginate the GRIIS-Israel checklist, keep Plantae, return {nubKey: scientificName}.
    Cached: the raw page dump is reused on re-run."""
    GRIIS_RAW.mkdir(parents=True, exist_ok=True)
    cache = GRIIS_RAW / "griis_israel_plants.json"
    if cache.exists():
        return {int(k): v for k, v in json.loads(cache.read_text(encoding="utf-8")).items()}

    plants, offset = {}, 0
    while True:
        r = requests.get("https://api.gbif.org/v1/species/search",
                         params={"datasetKey": GRIIS_KEY, "limit": 300, "offset": offset},
                         timeout=30).json()
        for x in r.get("results", []):
            if x.get("kingdom") == PLANTAE and x.get("nubKey"):
                plants[x["nubKey"]] = x.get("scientificName") or x.get("canonicalName")
        offset += 300
        if offset >= r.get("count", 0):
            break
    cache.write_text(json.dumps({str(k): v for k, v in plants.items()}, ensure_ascii=False,
                                indent=2), encoding="utf-8")
    return plants


def resolve_curated_listed() -> tuple[dict, list]:
    """Resolve curated invasive names → {backbone_key: name}. Returns (keys, unresolved)."""
    listed, unresolved = {}, []
    seen = set()
    for line in CURATED_CSV.read_text(encoding="utf-8").splitlines()[1:]:
        if not line.strip():
            continue
        name = line.split(",", 1)[0].strip()
        if name in seen:
            continue
        seen.add(name)
        r = resolve(name, "lat")
        if r["status"] == "resolved":
            listed[r["canonical_taxon_id"]] = r["scientific_name"]
        else:
            unresolved.append((name, r["reason"]))
    return listed, unresolved


def load_invasive_sets() -> dict:
    """Return listed/potential key sets + metadata for the canonical build."""
    griis = fetch_griis_plants()                       # {key: name}
    listed, unresolved = resolve_curated_listed()      # {key: name}
    potential = set(griis) - set(listed)               # GRIIS alien but not on invasive list
    print(f"Phase 2 sources: GRIIS-Israel plants={len(griis)} | "
          f"curated invasive resolved={len(listed)} (unresolved={len(unresolved)})")
    if unresolved:
        print("  curated unresolved:", unresolved)
    return {"listed": set(listed), "potential": potential,
            "griis_names": griis, "listed_names": listed}


if __name__ == "__main__":
    s = load_invasive_sets()
    print("listed keys:", len(s["listed"]), "| potential keys:", len(s["potential"]))
