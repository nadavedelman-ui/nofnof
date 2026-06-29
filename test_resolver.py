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
