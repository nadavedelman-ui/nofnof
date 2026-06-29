"""Phase 2 orchestrator — enrichment flags (invasive_status) on top of the Phase 1 spine.

Stage 1 (EAV + crosswalk) → load invasive sets (GRIIS-Israel + curated IL invasive list)
→ Stage 3 reconcile WITH invasive_status → canonical.parquet. Then prints the Phase 2 gate.
"""
from pipeline import stage1_stage, stage3_canonical, invasive


def main():
    print("=== Stage 1: stage raw → EAV + crosswalk ===")
    stage1_stage.run()

    print("\n=== Phase 2: load invasive-status sources ===")
    inv = invasive.load_invasive_sets()

    print("\n=== Stage 3: resolve → reconcile (+invasive_status) → parquet ===")
    res = stage3_canonical.run(invasive=inv)

    print("\n=== PHASE 2 GATE ===")
    total = res["canonical_rows"]
    b = res["invasive"]
    covered = b["listed"] + b["potential"] + b["not_listed"]
    print(f"Canonical taxa                 : {total}")
    print(f"invasive_status coverage       : {covered}/{total} "
          f"({'100%' if covered == total else f'{covered/total:.0%}'})  (gate: 100%)")
    print(f"  listed (curated IL invasive) : {b['listed']}")
    print(f"  potential (GRIIS alien)      : {b['potential']}")
    print(f"  not_listed (native)          : {b['not_listed']}")
    print(f"Provenance rows                : {res['provenance_rows']}")


if __name__ == "__main__":
    main()
