"""Dependency-free reference pipeline over the checked-in example response."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

from lakehouse_platform.observability.progress import progress
from products.example_works.experiments.category_metrics import aggregate_category_metrics

DEFAULT_SOURCE = (
    Path(__file__).resolve().parents[3]
    / "datasets"
    / "example_source"
    / "sample_response.json"
)


def _key(value: str) -> int:
    """Stable positive surrogate key for local reference data."""
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:15], 16)


def run_local_pipeline(source: str | Path = DEFAULT_SOURCE) -> dict[str, list[dict]]:
    source_path = Path(source)
    progress("LOCAL", "Reading example response", path=source_path)
    response = json.loads(source_path.read_text(encoding="utf-8"))
    bronze = response["results"]
    progress("BRONZE", "Raw records landed", rows=len(bronze))

    by_id = {}
    for record in bronze:
        by_id[record["id"]] = {
            "work_id": record["id"],
            "title": record["title"].strip(),
            "author_id": record["author"]["id"],
            "author_name": record["author"]["name"].strip(),
            "category": record["category"].strip().lower(),
            "year": int(record["year"]),
            "language": record["language"].lower(),
            "tags": record.get("tags", []),
            "updated_at": datetime.fromisoformat(
                record["updated_at"].replace("Z", "+00:00")
            ),
        }
    silver = list(by_id.values())
    progress("SILVER", "Records typed and deduplicated", rows=len(silver))

    dim_work = [
        {
            "work_key": _key(row["work_id"]),
            "work_id": row["work_id"],
            "title": row["title"],
            "language": row["language"],
            "year": row["year"],
        }
        for row in silver
    ]
    dim_author = [
        {
            "author_key": _key(author_id),
            "author_id": author_id,
            "author_name": next(
                row["author_name"] for row in silver if row["author_id"] == author_id
            ),
        }
        for author_id in sorted({row["author_id"] for row in silver})
    ]
    dim_category = [
        {
            "category_key": _key(category),
            "category_name": category,
        }
        for category in sorted({row["category"] for row in silver})
    ]
    dates = sorted({row["updated_at"].date() for row in silver})
    dim_date = [
        {
            "date_key": int(date.strftime("%Y%m%d")),
            "full_date": date.isoformat(),
            "calendar_year": date.year,
            "calendar_quarter": (date.month - 1) // 3 + 1,
            "calendar_month": date.month,
            "day_of_month": date.day,
        }
        for date in dates
    ]
    fact_work = [
        {
            "work_key": _key(row["work_id"]),
            "author_key": _key(row["author_id"]),
            "category_key": _key(row["category"]),
            "date_key": int(row["updated_at"].strftime("%Y%m%d")),
            "work_count": 1,
            "tag_count": len(row["tags"]),
        }
        for row in silver
    ]
    progress(
        "GOLD",
        "Kimball model built",
        facts=len(fact_work),
        dimensions=len(dim_work) + len(dim_author) + len(dim_category) + len(dim_date),
    )

    category_metrics = aggregate_category_metrics(fact_work, dim_category)
    return {
        "bronze": bronze,
        "silver": silver,
        "dim_work": dim_work,
        "dim_author": dim_author,
        "dim_category": dim_category,
        "dim_date": dim_date,
        "fact_work": fact_work,
        "category_metrics": category_metrics,
    }


def main() -> None:
    result = run_local_pipeline()
    print("\nEXPERIMENT: category metrics")
    for row in result["category_metrics"]:
        print(
            f"  {row['category']:<12} works={row['work_count']} "
            f"tags={row['tag_count']}"
        )


if __name__ == "__main__":
    main()
