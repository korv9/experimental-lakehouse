# Drug synergy sample datasets

**These are small, hand-written fixtures — not the real downloads.** They exist so
the pipeline, tests and local reference run work without a multi-gigabyte
download. Column names were read from the original project's scripts
(`BRAclean.py`, `Omic_ny.py`); **verify them against your actual files before the
first production run.**

## Real sources

| Fixture | Real file | Where from |
| --- | --- | --- |
| `drugcombs_scored_sample.csv` | `drugcombs_scored.csv` | <https://drugcomb.org/download/> |
| `depmap_model_sample.csv` | `Model.csv` | <https://depmap.org/portal/download/> |
| `depmap_expression_sample.csv` | `OmicsExpressionTPMLogp1HumanProteinCodingGenes.csv` | DepMap, same release |
| — (no fixture) | PubChem CID + SMILES | PubChem PUG REST, ingested live |

Drug structures are **not** a file source: `config/sources/pubchem_compound.yaml`
resolves names to CIDs and SMILES through the platform's REST client, and
fingerprints are derived from those SMILES in Silver.

## What the synergy fixture deliberately contains

The cleaning step has to survive real-world mess, so the sample reproduces every
case `BRAclean.py` handled:

| Row | Mess |
| --- | --- |
| `paclitaxel ` / `a549` | casing, trailing space, dashless cell-line alias |
| `Carboplatin,Paclitaxel` vs `Paclitaxel,Carboplatin` | same pair in reverse order — canonicalised, then averaged |
| `Tamoxifen,Tamoxifen` | self-pair, dropped |
| `Vinblastine,` | missing `drug2`, quarantined |
| `Sorafenib` ZIP `N/A` | non-numeric score, coerced to null |
| `HCT-116` / `HCT116` | same cell line written two ways |

Expected outcome from this fixture: 10 raw rows → the self-pair is dropped (9) →
the three Paclitaxel+Carboplatin measurements average into one row → **7 rows in
`silver.drug_combination`**.

The `Vinblastine` row with no second drug is **not** dropped by the
transformation. It reaches the quality gate and is quarantined there, so a
malformed source row stays visible in `quarantine.drug_synergy_combination`
instead of disappearing between two steps.
