# Drug Synergy

Predicting whether two cancer drugs work better together than apart, from public
screening data. Ported from the standalone
[DrugComb-Synergy-Prediction](https://github.com/korv9/DrugComb-Synergy-Prediction)
project, where the same steps were loose scripts writing CSVs.

## Four sources

| Source | What it gives | How it arrives |
| --- | --- | --- |
| **DrugComb** | drug pair + cell line + synergy scores (ZIP, Bliss, Loewe, HSA) | downloaded CSV |
| **PubChem** | compound id and SMILES structure per drug name | REST, live |
| **DepMap Model** | cell lines and their **cancer type** (OncotreeLineage) | downloaded CSV |
| **DepMap expression** | RNA expression per cell line and gene (**omics**) | downloaded CSV |

Fingerprints are a fifth dataset, but a *derived* one: Morgan/ECFP4 vectors
computed from PubChem SMILES, so they are reproducible from Silver without
calling anything.

## Flow

```text
DrugComb CSV ─┐
DepMap Model ─┼─> bronze.*_raw ─> silver.drug_combination ─┐
DepMap expr  ─┘                   silver.cell_line ────────┤
                                  silver.cell_expression   │
PubChem REST ────> bronze ──────> silver.drug ─> silver.drug_fingerprint
                                                           │
                                                           v
                              gold.dim_drug, dim_cell_line, dim_cancer_type
                                        gold.fact_drug_synergy
```

`fact_drug_synergy` has one row per canonical drug pair and cell line, holding
only foreign keys and additive measures. That is what lets the same synergy
numbers be sliced by drug, by cell line or by cancer type without rebuilding it.

## What the platform changed

The original scripts worked; these are the differences that came from moving in.

| Original | Here |
| --- | --- |
| `SLEEP_BETWEEN = 0.12` | `rate_limit.requests_per_second` in source config |
| no retries — PubChem 503s lost data silently | exponential backoff honouring `Retry-After` |
| `drug_name2cid_cache.json` on one laptop | `bronze.pubchem_compound_raw` — queryable, shared, replayable |
| `groupby(...).mean()` into a new CSV | Delta MERGE on the declared grain, idempotent |
| dropped bad rows | quarantined and countable |
| column names implicit in pandas | contracts validated before every write |
| fingerprints in a 2048-column CSV | sparse active-bit arrays in Delta |

## Running

```text
notebooks/products/drug_synergy/land_bronze.py       files  -> Bronze
notebooks/products/drug_synergy/bronze_to_silver.py  Bronze -> Silver
notebooks/products/drug_synergy/ingest_pubchem.py    PubChem -> Bronze (needs Silver drugs)
notebooks/products/drug_synergy/bronze_to_silver.py  rerun, now with structures
notebooks/products/drug_synergy/silver_to_gold.py    Silver -> star schema
```

PubChem comes after the first Silver run because it only looks up drugs that
actually appear in the screens, and only those Bronze does not already cover.

**Cluster library:** `rdkit` is required for fingerprints. Everything else runs
on the platform's own dependencies.

## Status and caveats

- The repository ships **small fixtures, not the real downloads** — see
  `datasets/drug_synergy/README.md`. Point the notebook paths at a Volume for
  real runs.
- Column names for DrugComb and DepMap were read from the original project's
  `BRAclean.py` and `Omic_ny.py`. **Verify them against your actual downloads**
  before the first production run; a rename is a one-line change in the
  corresponding transform's source schema.
- Not yet ported: the autoencoder cell embeddings (`cell_rna_autoenc.csv`) and
  the classifier. Those are modelling, not pipeline — they belong in a sandbox
  notebook reading Gold, with MLflow for tracking.
