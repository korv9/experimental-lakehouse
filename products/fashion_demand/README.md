# Fashion Demand — demand forecasting on the lakehouse

**Business question:** how many units of a given article will sell, per sales
channel, fourteen days from now?

That is the question buying and allocation actually needs answered. Fourteen
days because that is roughly the lead time between a replenishment decision and
stock on a shelf — a forecast for tomorrow is accurate and useless.

This is the product that extends the platform into machine learning. The
architectural reasoning lives in
[`docs/architecture/ml_layer.md`](../../docs/architecture/ml_layer.md); this
README is what you need to run it.

## Status

| Layer | State |
|---|---|
| Bronze → Silver → Gold → feature, all four ACONs | **built** |
| Contracts, quality rules, notebooks, job chain | **built** |
| Baselines, metrics, splits, model registry | **built and tested in CI** |
| Model training, tuning, SHAP | **not built** |

**Nothing here has been executed against Spark.** pyspark could not be installed
in the environment these files were written in, so the transformations are held
in place by tests that check the agreements between files — contracts against
transforms, merge keys against grain, each stage's inputs against the previous
stage's outputs — not by a run. Expect to fix things on the first real run, and
treat the column names below as claims to verify rather than facts.

## The data

**H&M Personalized Fashion Recommendations** (Kaggle, 2022) — H&M's own public
transaction data. Using it for an H&M application is worth a sentence in the
cover letter on its own.

| File | Shape |
|---|---|
| `transactions_train.csv` | ~31.8M rows, 5 columns: transaction date, customer, article, price, sales channel |
| `articles.csv` | ~105k articles, ~25 columns: product type, colour, department, garment group |
| `customers.csv` | ~1.37M customers: age, club status, postal area |

Download requires a Kaggle account and accepting the competition rules; there is
no anonymous URL. `kaggle competitions download -c h-and-m-personalized-fashion-recommendations`
once the CLI is authenticated. Skip the images — they are the bulk of the
archive and this product does not use them.

**Verify the column names against the header row before writing Bronze.** The
figures above come from the competition description, not from a file this repo
has read.

**If the Kaggle login is a hassle:** Rossmann Store Sales is a drop-in
alternative — daily sales per store with promotion and holiday flags. The
feature layer is written against a generic (entity, date, value) panel, so
swapping the source changes Bronze through Gold and leaves `feature/` and `ml/`
untouched. That is the design working.

## The transformation chain

```
transactions_train.csv                       31.8M transaction lines
        │  land_bronze
        ▼
bronze.hm_transactions_raw                   raw payload + ingestion metadata
        │  bronze_to_silver: type, validate, quarantine
        ▼
silver.transactions                          one clean row per transaction line
        │  silver_to_gold: aggregate to a day, densify zero-demand days
        ▼
gold.fact_daily_demand                       article × channel × day, units_sold
        │  gold_to_features  ← the ML layer starts here
        ▼
feature.demand_features                      one supervised row, target 14d ahead
        │  train
        ▼
platform.ml_runs + platform.ml_predictions   metrics, and forecasts that can be
                                             joined back to actuals
```

The step that carries the most modelling judgement is `silver_to_gold`, and it
is the one line above that is easy to skim past: **days with no sales must
become rows with `units_sold = 0`.** A transactions table only contains days
something sold. Aggregate it naively and the model never sees a zero, learns
that demand is always positive, and systematically overstocks. Densifying the
panel against the article's on-sale window is not a detail, it is the difference
between a model and a plausible-looking one.

## Layout

```text
products/fashion_demand/
├── pipelines/                       one ACON per medallion hop
│   ├── land_bronze.yaml
│   ├── bronze_to_silver.yaml
│   ├── silver_to_gold.yaml
│   └── gold_to_features.yaml        the ML layer's entry point
├── tables/                          one folder per physical table
│   ├── bronze/
│   │   ├── hm_transactions_raw/     contract only — Bronze types nothing
│   │   └── hm_articles_raw/
│   ├── silver/
│   │   ├── transactions/            contract, transform, quality rules
│   │   └── articles/
│   ├── gold/
│   │   ├── fact_daily_demand/       the densified panel
│   │   └── dim_article/
│   └── feature/
│       └── demand_features/         the supervised training table
└── ml/
    ├── baselines.py                 seasonal naive, last observed, moving average
    ├── dataset.py                   split, leakage guard, hand-off to pandas
    ├── train.py                     (todo) LightGBM against the baselines
    └── explain.py                   (todo) SHAP over the trained model
```

`customers.csv` is not landed. The demand grain is article x channel x day, so
customer attributes have nowhere to attach — they matter for the recommendation
variant of this dataset, not for forecasting. `dim_date` is not built either:
the calendar features live in the feature table, and a date dimension only earns
its place once BI queries need it.

## Running it

```bash
databricks bundle run fashion_demand_pipeline -t dev
```

Or notebook by notebook, in order — `land_bronze`, `bronze_to_silver`,
`silver_to_gold`, `gold_to_features`. The landing notebook expects the CSVs
already in the landing Volume; it does not fetch them, because the Kaggle export
needs an authenticated account and there is no URL a job could pull from.

**On a laptop, set `min_total_units` first.** The full panel is ~105k articles
across two channels and two years of days. Densification multiplies rows by the
length of each series, so the fact can reach tens of millions of rows before the
feature table is even built. The option is in `silver_to_gold.yaml`; a few
hundred restricts it to articles with real volume, which is also the segment a
forecast is worth having for.

## Running the model work

Order matters, and the first step is the one people skip:

1. **Baselines first.** `ml/baselines.py` gives you seasonal naive, last
   observed and a 28-day moving average, scored on the same folds the model will
   use. If the model cannot beat seasonal naive, the honest result is that it
   added nothing — and knowing that on day one is worth more than a tuned model
   on day three.
2. **One holdout, then folds.** `dataset.build_splits(features, folds=1)` to
   iterate quickly, `folds=4` before believing a number. A single holdout
   measures one month's weather.
3. **Record every run.** `lakehouse_platform.ml.registry.record_model_run`
   writes the data window, parameters and metrics to `platform.ml_runs`. A
   comparison you cannot re-query is a comparison you will redo.
4. **Score back into the lakehouse.** `write_predictions`, then
   `backfill_actuals` on a schedule. That pair is what makes the forecast
   monitorable instead of a one-off notebook output.

## Metrics

Lead with **WMAPE**. Retail demand is full of zero-sales days and MAPE divides
by the actual, so a single zero makes it infinite; WMAPE — total absolute error
over total volume — is defined regardless and weights a high-volume article's
error above a long-tail one's.

Report **bias** next to it. Under-forecasting loses a sale, over-forecasting
funds a markdown. Two models with identical MAE are not equally good if one is
systematically low, and only bias makes that visible.

Segment before concluding. A single number over 105k articles hides that the
model is excellent on the top decile and no better than a moving average on the
long tail — which is exactly the finding worth putting in the README.
