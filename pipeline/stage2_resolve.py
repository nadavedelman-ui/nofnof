import hashlib, json, re, time, unicodedata
from pathlib import Path
import requests

GBIF_MATCH = "https://api.gbif.org/v1/species/match"
CACHE_DIR = Path("cache/gbif"); CACHE_DIR.mkdir(parents=True, exist_ok=True)
CONFIDENCE_THRESHOLD = 88
REQUEST_SLEEP = 0.05
SPECIES_RANKS = {"SPECIES", "SUBSPECIES", "VARIETY", "FORM"}
CULTIVAR_RE = re.compile(r"\s*'[^']*'|\s*\"[^\"]*\"")
STATS = {"hits": 0, "misses": 0}

def normalize_name(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", s).strip())

def _is_latin(s: str) -> bool:
    return bool(re.search(r"[A-Za-z]", s)) and not re.search(r"[֐-׿]", s)

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
