# Runbook: getting the drug synergy datasets

Three files to download, one API that needs nothing. Land the files in a Unity
Catalog Volume, then point the notebook paths at them.

## 1. DrugComb screening results

- **Portal:** <https://drugcomb.org/download/>
- **Direct (v1.4, May 2024):** <https://zenodo.org/records/11102665> —
  `summary_table_v1.4.csv`, about **193 MB**
- Covers ~438,000 combination experiments across 93 cancer cell lines.

> **Verify the header before running.** The transformation's source schema was
> derived from the original project's `BRAclean.py`, which used `Drug1`,
> `Drug2`, `Cell line`, `ZIP`. The v1.4 summary table is documented with
> different names — expect something closer to `drug_row`, `drug_col`,
> `cell_line_name`, `synergy_zip`, plus `block_id`, `study_name` and CSS
> columns. Open the header first:
>
> ```bash
> head -1 summary_table_v1.4.csv
> ```
>
> Then edit `SOURCE_SCHEMA` and the `_parse` projection in
> `products/drug_synergy/tables/silver/drug_combination/transform.py`, and the
> `id_fields` in `products/drug_synergy/pipelines/land_bronze.yaml`. Nothing
> else changes — that is the point of keeping parsing in one place.

## 2. DepMap cell-line metadata and expression

- **Portal:** <https://depmap.org/portal/download/all/>
- **Direct (24Q2):** <https://plus.figshare.com/articles/dataset/DepMap_24Q2_Public/25880521>

Two files:

| File | Contents | Rough size |
| --- | --- | --- |
| `Model.csv` | cell lines, `OncotreeLineage` (the cancer type) | ~1 MB |
| `OmicsExpressionProteinCodingGenesTPMLogp1.csv` | log2(TPM+1) per cell line and gene | ~500 MB |

> The expression filename in `config/sources/depmap_expression.yaml` was taken
> from the original project's `Omic_ny.py`; the 24Q2 release calls it
> `OmicsExpressionProteinCodingGenesTPMLogp1.csv`. Check the release you
> download and update the config comment — the pipeline reads whatever path the
> notebook passes, so only the documentation is affected.

Set `schema_version` / `depmap_release` to the release you downloaded (`24Q2`).
Silver keeps the newest release per cell line, so a later download supersedes
rather than duplicates.

## 3. PubChem — nothing to download

Structures are fetched live by `notebooks/products/drug_synergy/ingest_pubchem.py`,
which resolves only the drugs that actually appear in the screens and that
Bronze does not already cover.

## Landing the files

Upload to a Volume, then point the notebook at it:

```python
# notebooks/products/drug_synergy/land_bronze.py
DRUGCOMB_FILE = "/Volumes/dev_lakehouse/landing/source_files/summary_table_v1.4.csv"
DEPMAP_MODEL_FILE = "/Volumes/dev_lakehouse/landing/source_files/Model.csv"
DEPMAP_EXPRESSION_FILE = (
    "/Volumes/dev_lakehouse/landing/source_files/"
    "OmicsExpressionProteinCodingGenesTPMLogp1.csv"
)
DEPMAP_RELEASE = "24Q2"
```

Databricks CLI:

```bash
databricks fs cp summary_table_v1.4.csv \
  dbfs:/Volumes/dev_lakehouse/landing/source_files/summary_table_v1.4.csv
```

## Licences

DrugComb and DepMap are both freely available for research use, DepMap under
CC BY 4.0. Check each portal's terms before redistributing anything derived
from them — that matters if a Gold export ends up on a public site.

## Suggested first run

Run against the checked-in fixtures before the real files:

1. `land_bronze` → 10 DrugComb rows, 6 cell lines, 6 expression rows
2. `bronze_to_silver` → 7 rows in `silver.drug_combination`, 1 quarantined
3. `silver_to_gold` → the star, with `dim_drug` empty of structures
4. install `rdkit`, run `ingest_pubchem`, rerun Silver and Gold

That proves the graph before a 700 MB download is involved, and step 4 is the
first time the REST client meets a real API.
