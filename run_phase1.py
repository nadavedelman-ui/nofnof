"""Phase 1 orchestrator — native-flora spine, end to end (Stage 1 → 2 → 3).

Assumes Stage 0 (scrape) has populated data/raw/wildflowers/<date>/.
Runs: EAV load + crosswalk → resolve → reconcile → canonical.parquet, then prints the gate.
"""
from pipeline import stage1_stage, stage3_canonical


def main():
    print("=== Stage 1: stage raw → EAV + crosswalk ===")
    n_records = stage1_stage.run()

    print("\n=== Stage 2+3: resolve → reconcile → parquet ===")
    res = stage3_canonical.run()

    print("\n=== PHASE 1 GATE ===")
    total = res["taxa_in"]
    resolved_pct = res["resolved"] / total if total else 0
    print(f"Source taxa staged          : {total}")
    print(f"Resolved to GBIF taxon key  : {res['resolved']} ({resolved_pct:.0%})")
    print(f"Canonical rows (deduped)    : {res['canonical_rows']}  "
          f"-> web/public/canonical.parquet")
    print(f"Unresolved -> review queue  : {res['review']} ({res['review']/total:.0%})  "
          f"-> review/unresolved.csv")
    print(f"Provenance rows populated   : {res['provenance_rows']}")
    print(f"GBIF cache hits/misses      : {res['cache']['hits']}/{res['cache']['misses']}")


if __name__ == "__main__":
    main()
