"""Stage 0 — scraper for the Israeli native-flora DB (wildflowers.co.il / KKL 'צמח השדה').

Polite, one-time snapshot of FACTUAL botanical data. Idempotent: a plant already
snapshotted is skipped on re-run, so an interrupted run just resumes. Writes one JSON
per valid taxon to data/raw/wildflowers/<date>/plant_<ID>.json.

The species pages have a clean, regular markup:
    <li><span class="prop_name">LABEL:</span> <span class="prop_data">VALUE</span></li>
so we harvest every prop_name/prop_data pair verbatim (schema-on-read / EAV-friendly).
Invalid IDs return HTTP 404; valid IDs span ~1..3400 (sparse).

Low-concurrency by default (spec permits it). A WAF guards the site — if it starts
returning "Unauthorized Request Blocked" pages we abort rather than hammer it.
"""
import json, re, time, html, threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

SOURCE = "wildflowers"
SNAPSHOT_DATE = "2026-06-25"                         # dated immutable snapshot
BASE = "https://www.kkl.org.il/wild-flower/hebrew/plant.asp?ID={id}"
RAW_DIR = Path(f"data/raw/{SOURCE}/{SNAPSHOT_DATE}")
RAW_DIR.mkdir(parents=True, exist_ok=True)
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
REQUEST_SLEEP = 0.12                                 # per-request throttle (gentle)
BLOCK_MARKER = "unauthorized request blocked"
BLOCK_ABORT_THRESHOLD = 15                            # bail if the WAF starts blocking

PAIR_RE = re.compile(
    r'<span class="prop_name">(.*?)</span>\s*<span class="prop_data">(.*?)</span>', re.S)
# per-plant special-status icons (protected / red-list / nectar / edible / medicinal …)
STATUS_RE = re.compile(r'<div class="prop-img_item">.*?<img[^>]*?alt="([^"]*)".*?</div>', re.S)
TAG_RE = re.compile(r"<[^>]+>")
LABEL_NAME_HE = "שם הצמח"
LABEL_NAME_LAT = "שם מדעי"

_lock = threading.Lock()
_abort = threading.Event()


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub("", s))).strip()


def parse_plant(html_text: str) -> dict:
    """Return {hebrew_label: value} for every prop pair on the page (may be empty)."""
    fields = {}
    for k, v in PAIR_RE.findall(html_text):
        k, v = _clean(k).rstrip(":").strip(), _clean(v)
        if k and k not in fields:
            fields[k] = v
    return fields


def _raw_path(plant_id: int) -> Path:
    return RAW_DIR / f"plant_{plant_id}.json"


def scrape_id(plant_id: int) -> str:
    """Fetch + snapshot one plant. Returns cached|ok|absent|no_fields|blocked."""
    p = _raw_path(plant_id)
    if p.exists():
        return "cached"
    r = requests.get(BASE.format(id=plant_id), headers={"User-Agent": UA}, timeout=25)
    if r.status_code == 404:
        return "absent"
    r.raise_for_status()
    r.encoding = "utf-8"
    if BLOCK_MARKER in r.text.lower():
        return "blocked"
    fields = parse_plant(r.text)
    if LABEL_NAME_LAT not in fields and LABEL_NAME_HE not in fields:
        return "no_fields"
    statuses = sorted({_clean(a) for a in STATUS_RE.findall(r.text) if _clean(a)})
    if statuses:                                     # synthetic field → flows through EAV
        fields["_statuses"] = "; ".join(statuses)
    record = {
        "source": SOURCE, "source_id": str(plant_id),
        "url": BASE.format(id=plant_id), "scraped_at": SNAPSHOT_DATE,
        "name_he": fields.get(LABEL_NAME_HE), "name_lat": fields.get(LABEL_NAME_LAT),
        "fields": fields,
    }
    p.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    time.sleep(REQUEST_SLEEP)
    return "ok"


def run(start: int = 1, end: int = 3450, workers: int = 5) -> dict:
    stats = {"ok": 0, "cached": 0, "absent": 0, "no_fields": 0, "blocked": 0, "error": 0}
    total = end - start + 1
    done = 0

    def handle(plant_id: int):
        nonlocal done
        if _abort.is_set():
            return
        try:
            st = scrape_id(plant_id)
        except Exception as e:
            st = "error"
            print(f"  ! ID {plant_id}: {type(e).__name__} {e}")
        with _lock:
            stats[st] += 1
            done += 1
            if st == "blocked" and stats["blocked"] >= BLOCK_ABORT_THRESHOLD:
                _abort.set()
                print(f"  !! WAF blocking ({stats['blocked']} blocks) — aborting scrape")
            if done % 200 == 0:
                print(f"  ...{done}/{total} scanned  {stats}")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(handle, range(start, end + 1)))

    valid = stats["ok"] + stats["cached"]
    print(f"\nStage 0 done. IDs {start}-{end}: {valid} taxa snapshotted "
          f"(ok={stats['ok']} cached={stats['cached']} absent={stats['absent']} "
          f"no_fields={stats['no_fields']} blocked={stats['blocked']} error={stats['error']})")
    print(f"  raw dir: {RAW_DIR}")
    return stats


if __name__ == "__main__":
    import sys
    a = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    b = int(sys.argv[2]) if len(sys.argv) > 2 else 3450
    w = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    run(a, b, w)
