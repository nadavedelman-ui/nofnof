"""Phase 3 orchestrator — seller availability joined on top of the spine.

Assumes the Phase 2 canonical.parquet exists. Runs the commercial join (no identity
rebuild) and prints the Phase 3 gate.
"""
from pipeline import commercial


def main():
    print("=== Phase 3: commercial join (azur → spine) ===")
    r = commercial.run()

    total = r["seller_products"]
    print("\n=== PHASE 3 GATE ===")
    print(f"Seller products (azur)        : {total}")
    print(f"Matched to existing taxa      : {r['matched']} ({r['matched']/total:.0%})")
    print(f"Unmatched -> review (no orphan): {r['unmatched']} -> review/seller_unmatched.csv")
    print(f"Canonical taxa with seller    : {r['taxa_with_seller']}")
    print(f"No orphans (rows before==after): {r['canon_rows_before']} == {r['canon_rows_after']} "
          f"-> {'PASS' if r['canon_rows_before']==r['canon_rows_after'] else 'FAIL'}")
    print(f"Multi-listing taxa (conflicts): {r['multi_listing_taxa']} (preserved in provenance)")
    print(f"Provenance rows added         : {r['provenance_added']}")


if __name__ == "__main__":
    main()
