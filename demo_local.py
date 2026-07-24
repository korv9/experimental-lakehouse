"""Local, dependency-free demo: messy raw JSON -> structured -> quality -> gold.

Runs the whole conceptual flow in pure Python (no Spark, no Databricks) so you
can SEE the cleaning happen:

    python demo_local.py

It reuses ``lakehouse_platform.transforms.cleaning.clean_record`` — the exact logic the
PySpark transform applies as a UDF — so this demo and the cluster agree. The
Spark version (bronze->silver->gold on Delta) lives in
``products/messy_records/transformations.py`` and is exercised by
``tests/integration/test_messy_pipeline.py``.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from lakehouse_platform.transforms.cleaning import clean_record

RAW_PATH = Path(__file__).parent / "datasets" / "messy_demo" / "raw_records.json"


def _rule(line: str = "-") -> None:
    print(line * 78)


def main() -> None:
    raw_records = json.loads(RAW_PATH.read_text())
    print(f"Loaded {len(raw_records)} raw records from {RAW_PATH.name}\n")

    # --- BRONZE -> SILVER: clean every record ---
    cleaned = [clean_record(r) for r in raw_records]

    # show a few before/after transformations so the cleaning is visible
    _rule("=")
    print("SAMPLE TRANSFORMATIONS (raw -> cleaned)")
    _rule("=")
    samples = [
        ("title", "REC-001 title", raw_records[0]["title"], cleaned[0]["title"]),
        ("creators", "row2 creator obj", raw_records[1]["creator"], cleaned[1]["creators"]),
        ("year", "row3 'c. 1200'", raw_records[2]["year"], cleaned[2]["year"]),
        ("year", "row6 roman", raw_records[5]["year"], cleaned[5]["year"]),
        ("price", "rec-010 kr", raw_records[10]["price"], cleaned[10]["price"]),
        ("updated_at", "row4 epoch", raw_records[3]["updated"], cleaned[3]["updated_at"]),
        ("category", "row3 'NON-FICTION '", raw_records[2]["category"], cleaned[2]["category"]),
        ("email", "row2 invalid", raw_records[1]["email"], cleaned[1]["email"]),
    ]
    for field, label, before, after in samples:
        print(f"  {field:<11} {label:<20} {before!r:<28} -> {after!r}")

    # --- QUALITY: error rules = quarantine (mirrors config/quality/messy_records_checks.yaml) ---
    def passes(rec: dict) -> bool:
        return rec["record_id"] is not None and rec["title"] is not None

    good = [r for r in cleaned if passes(r)]
    quarantined = [r for r in cleaned if not passes(r)]

    # --- DEDUP: latest wins (here: last occurrence per record_id) ---
    by_id: dict[str, dict] = {}
    for r in good:
        by_id[r["record_id"]] = r
    silver = list(by_id.values())

    _rule("=")
    print("QUALITY + DEDUP")
    _rule("=")
    print(f"  cleaned:     {len(cleaned)}")
    print(f"  quarantined: {len(quarantined)}  (null record_id or title)")
    print(f"  deduped:     {len(good) - len(silver)}  duplicate id(s) collapsed")
    print(f"  -> silver.records rows: {len(silver)}")

    # --- GOLD: a small product (counts + avg rating by category) ---
    counts = Counter(r["category"] for r in silver)
    ratings: dict[str, list[float]] = {}
    for r in silver:
        if r["rating"] is not None:
            ratings.setdefault(r["category"], []).append(r["rating"])

    _rule("=")
    print("GOLD: works by category")
    _rule("=")
    print(f"  {'category':<14}{'count':>6}   avg_rating")
    for cat, n in counts.most_common():
        avg = ratings.get(cat)
        avg_s = f"{sum(avg) / len(avg):.2f}" if avg else "-"
        print(f"  {cat!s:<14}{n:>6}   {avg_s}")

    _rule("=")
    print("Done. Same clean_record() runs on Spark as a UDF (see messy_records.py).")


if __name__ == "__main__":
    main()
