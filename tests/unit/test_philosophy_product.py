from pathlib import Path

import yaml


def test_philosophy_product_manifest_uses_unity_catalog_names():
    manifest = yaml.safe_load(
        Path("products/philosophy_litterature/product.yaml").read_text(encoding="utf-8")
    )
    unity_catalog = manifest["unity_catalog"]

    assert manifest["product"]["status"] == "discovery"
    assert manifest["scope"]["start_year"] == 1700
    assert unity_catalog["source_volume"] == "${catalog}.landing.source_files"
    for table in unity_catalog["control_tables"] + unity_catalog["planned_tables"]:
        assert table.startswith("${catalog}.")
        assert len(table.replace("${catalog}.", "").split(".")) == 2


def test_development_environment_uses_fully_qualified_volume_paths():
    environment = yaml.safe_load(
        Path("config/environments/dev.yaml").read_text(encoding="utf-8")
    )

    assert environment["landing_volume"] == (
        "/Volumes/dev_lakehouse/landing/source_files"
    )
    assert environment["checkpoint_volume"] == (
        "/Volumes/dev_lakehouse/platform/checkpoints"
    )


def test_gutendex_source_has_rate_limit_resume_and_uc_bronze_target():
    source = yaml.safe_load(
        Path("config/sources/philosophy_gutendex.yaml").read_text(encoding="utf-8")
    )

    assert source["rate_limit"]["requests_per_second"] == 1
    assert source["incremental"]["strategy"] == "replay_idempotently"
    assert source["selection"]["type"] == "corpus_report"
    assert source["selection"]["batch_size"] == 25
    assert source["selection"]["accepted_statuses"] == [
        "matched",
        "matched_without_plain_text",
    ]
    assert source["destination"]["bronze_table"] == (
        "bronze.philosophy_litterature_work_raw"
    )
    assert source["destination"]["contract"].endswith("contract:TableDefinition")


def test_candidate_corpus_is_product_owned_and_unique():
    corpus = yaml.safe_load(
        Path("products/philosophy_litterature/corpus.yaml").read_text(encoding="utf-8")
    )
    works = corpus["works"]

    assert corpus["corpus"]["status"] == "candidate"
    assert len(works) == 82
    assert len({work["id"] for work in works}) == len(works)
