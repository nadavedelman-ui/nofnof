"""Stage 0 — scraper for an Israeli retail nursery (משתלת אזור / azurflowers.co.il, WooCommerce).

Phase 3 commercial layer. Robots.txt allows product pages; we enumerate via the
product sitemap (no URL guessing) and keep only plant categories. Hebrew-only names —
they get matched to the spine via the he_lat crosswalk in the commercial join step.

Per product we capture the FACTUAL commercial fields: price, pot size, availability.
Polite, idempotent (skip already-snapshotted), low-concurrency, WAF-safe.
"""
import json, re, time, html, urllib.parse, threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

SOURCE = "azur"
SNAPSHOT_DATE = "2026-06-25"
SITEMAP = "https://azurflowers.co.il/product-sitemap.xml"
RAW_DIR = Path(f"data/raw/{SOURCE}/{SNAPSHOT_DATE}")
RAW_DIR.mkdir(parents=True, exist_ok=True)
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
REQUEST_SLEEP = 0.12

# keep only live-plant categories (exclude pots, tools, bouquets, fertilizer, gifts, …)
PLANT_CATEGORIES = {
    "צמחים-לבית-ולמשרד", "צמחים-למרפסת-ולגינה", "סחלבים",
    "עצים", "סוקולנטים-וקקטוסים", "ירקות",
}
TAG_RE = re.compile(r"<[^>]+>")
TITLE_RE = re.compile(r'class="product_title[^"]*"[^>]*>(.*?)</h1>', re.S)
# the PRODUCT price lives in the summary's <p class="price">…</p>; anchor there so we
# don't grab the empty header mini-cart (₪0.00) or related-product prices.
PRICE_BLOCK_RE = re.compile(r'<p class="price[^"]*">(.*?)</p>', re.S)
BDI_RE = re.compile(r'<bdi>(.*?)</bdi>', re.S)
POT_RE = re.compile(r'קוטר\s*\d+\s*ס(?:"|״)?מ|\d+\s*ליטר')

_lock = threading.Lock()


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub("", s))).strip()


def product_urls() -> list[tuple]:
    """(url, category, slug) for plant products only, from the sitemap."""
    sm = requests.get(SITEMAP, headers={"User-Agent": UA}, timeout=30).text
    out = []
    for raw in re.findall(r"<loc>(.*?)</loc>", sm):
        u = urllib.parse.unquote(raw)
        parts = [p for p in u.split("/") if p]
        if "חנות" not in parts:
            continue
        i = parts.index("חנות")
        if len(parts) < i + 3:                     # need category + product slug
            continue
        cat, slug = parts[i + 1], parts[-1]
        if cat in PLANT_CATEGORIES:
            out.append((raw, cat, slug))
    return out


def parse_product(t: str) -> dict:
    name = TITLE_RE.search(t)
    price = None
    block = PRICE_BLOCK_RE.search(t)               # first <p class="price"> = product price
    if block:
        bdi = BDI_RE.search(block.group(1))         # first amount = base / low end of range
        if bdi:
            digits = re.sub(r"[^\d.]", "", _clean(bdi.group(1)))
            price = float(digits) if digits else None
    pot_sizes = sorted({_clean(x) for x in POT_RE.findall(t)})
    avail = "out_of_stock" if 'class="stock out-of-stock"' in t else "in_stock"
    return {"name_he": _clean(name.group(1)) if name else None,
            "price": price, "currency": "ILS",
            "pot_sizes": pot_sizes, "availability": avail}


def scrape_one(url: str, category: str, slug: str) -> str:
    p = RAW_DIR / f"{slug}.json"
    if p.exists():
        return "cached"
    r = requests.get(url, headers={"User-Agent": UA}, timeout=25)
    if r.status_code == 404:
        return "absent"
    r.raise_for_status()
    r.encoding = "utf-8"
    fields = parse_product(r.text)
    if not fields["name_he"]:
        return "no_name"
    rec = {"source": SOURCE, "source_id": slug, "url": urllib.parse.unquote(url),
           "category": category, "scraped_at": SNAPSHOT_DATE, **fields}
    p.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    time.sleep(REQUEST_SLEEP)
    return "ok"


def run(workers: int = 5) -> dict:
    items = product_urls()
    print(f"sitemap: {len(items)} plant products to snapshot")
    stats = {"ok": 0, "cached": 0, "absent": 0, "no_name": 0, "error": 0}
    done = 0

    def handle(it):
        nonlocal done
        url, cat, slug = it
        try:
            st = scrape_one(url, cat, slug)
        except Exception as e:
            st = "error"
            print(f"  ! {slug}: {type(e).__name__} {e}")
        with _lock:
            stats[st] += 1
            done += 1
            if done % 100 == 0:
                print(f"  ...{done}/{len(items)}  {stats}")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(handle, items))
    print(f"\nStage 0 (azur) done: {stats['ok']+stats['cached']} snapshotted  {stats}")
    print(f"  raw dir: {RAW_DIR}")
    return stats


if __name__ == "__main__":
    run()
