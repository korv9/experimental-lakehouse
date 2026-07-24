from products.example_works.local.reference_pipeline import run_local_pipeline


def test_example_data_builds_valid_kimball_star():
    result = run_local_pipeline()

    assert len(result["bronze"]) == 2
    assert len(result["silver"]) == 2
    assert len(result["fact_work"]) == 2
    assert len(result["dim_work"]) == 2
    assert len(result["dim_author"]) == 2
    assert len(result["dim_category"]) == 2
    assert len(result["dim_date"]) == 2

    work_keys = {row["work_key"] for row in result["dim_work"]}
    author_keys = {row["author_key"] for row in result["dim_author"]}
    category_keys = {row["category_key"] for row in result["dim_category"]}
    date_keys = {row["date_key"] for row in result["dim_date"]}

    for fact in result["fact_work"]:
        assert fact["work_key"] in work_keys
        assert fact["author_key"] in author_keys
        assert fact["category_key"] in category_keys
        assert fact["date_key"] in date_keys


def test_experiment_aggregates_facts_by_dimension():
    metrics = run_local_pipeline()["category_metrics"]
    assert metrics == [
        {"category": "fiction", "work_count": 1, "tag_count": 2},
        {"category": "nonfiction", "work_count": 1, "tag_count": 1},
    ]
