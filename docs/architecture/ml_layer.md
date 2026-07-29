# Where the ML layer goes

Short answer: **beside Gold, not above it.** Two new Unity Catalog schemas,
`feature` and `ml`, sitting at the same level as `gold` rather than stacked on
top of it.

```
landing → bronze → silver → gold ─┬─→ (BI, dashboards, ad hoc SQL)
                                  │
                                  ├─→ feature ──→ model training ──→ ml
                                  │                                   │
                                  └───────────── ml.predictions ──────┘
                                                  joined back for serving
```

## Why not a fourth medallion layer

"Platinum" tiers get proposed a lot and they do not survive contact with the
work, because the medallion layers are defined by *how refined the data is* —
raw, cleaned, conformed — and features are not more refined than Gold. They are
Gold **reshaped for one specific model**. A 28-day rolling mean of units sold is
not a more business-conformed fact; it is an input to one algorithm with one
horizon, and it is worthless to the finance dashboard that reads the same fact.

The consequences of getting this wrong are concrete:

- If features live in Gold, every model's private feature ends up in the layer
  the whole company reads, and Gold becomes an unnavigable mix of business facts
  and algorithm plumbing.
- Gold is modelled for *aggregation* (a star schema, facts joined to
  dimensions). Features are modelled for *one row per training example*. Those
  are different grains, and forcing them into one layer means one of them is
  wrong.
- Models come and go. A schema that can be dropped and rebuilt without touching
  the reporting layer is worth having.

So: `feature` is a consumer of Gold, in the same way BI is a consumer of Gold.
And `ml` closes the loop by writing predictions back into the lakehouse, where
they can be joined to Gold like any other table.

## The two new schemas

### `feature.*` — model inputs

One table per (model family, grain, horizon). Each is built by a normal ACON
pipeline: read Gold, transform, quality gate, contract, write. Nothing about
feature engineering justifies a second orchestration path.

The rule that defines the layer: **every column must be knowable at the forecast
origin.** That is not a comment, it is enforced in three places —
`lakehouse_platform.ml.features` only builds windows that end the day before,
the contract pins the exact column list, and the quality gate rejects rows whose
label is missing or impossible.

### `ml.*` and `platform.ml_*` — model outputs

- `platform.ml_runs` — one row per training run: data window, parameters,
  metrics, and the `run_id` of the ACON pipeline that built its features.
- `platform.ml_predictions` — one row per scored entity-date, tagged with the
  model run. `actual` starts null and is backfilled once the truth arrives.
- `ml.*` — any serving-shaped table a consumer needs: the current forecast per
  article, a recommendation list, a customer segment assignment.

Model runs live in `platform` beside `pipeline_runs` and
`data_quality_results` because they are the same kind of thing: operational
metadata that someone will query in six months. Serving outputs live in `ml`
because they are data products that consumers read.

The `actual` backfill is the part that turns this from a portfolio piece into
something operationally real. Once predictions and outcomes sit in one table,
"is the live model still any good this week" is a `GROUP BY`, not a retraining
run — and model drift becomes visible before a stakeholder reports it.

## What is genuinely new code, and what is not

| Concern | Handled by |
|---|---|
| Reading Gold, writing features | existing ACON engine — unchanged |
| Contract enforcement on a feature table | existing `BaseSchema` — unchanged |
| Quality gate on training rows | existing quality engine — unchanged |
| Run logging, quarantine, lineage | existing control tables — unchanged |
| Point-in-time-correct lags and windows | `lakehouse_platform.ml.features` |
| Time splits with a leakage embargo | `lakehouse_platform.ml.splits` |
| Forecast metrics that survive zeros | `lakehouse_platform.ml.metrics` |
| Model runs and predictions as tables | `lakehouse_platform.ml.registry` |

Four small modules. That ratio is the argument for the design: a platform that
already governs tables does not need a second, ML-shaped platform bolted to it,
it needs the handful of primitives that are actually specific to modelling.

## The three mistakes this layout is built to prevent

**1. A random train/test split on a time series.** Cutting on date fixes half of
it. The half that survives is the horizon: forecasting 14 days ahead while
training right up to the test window means the last 14 training labels describe
days the model is supposed to predict blind. `splits.time_split` defaults the
embargo to the horizon, and `assert_embargo` fails the run if someone narrows
it.

**2. A rolling window that includes today.** Every window in `ml.features` ends
at `-1`. A model trained on a 7-day mean that contains the target scores
beautifully and forecasts nothing. Related and less obvious: windows range over
*days*, not rows, because a retail panel has gaps and `rowsBetween(-6, -1)`
silently reaches further back for a sparse article than a dense one.

**3. A feature derived from the outcome.** In the fashion demand product, unit
price is observed from transactions, so on the day being forecast it is a
function of the target. The transform lags it and drops the raw column, so it
cannot be reintroduced by adding a name to a feature list later.

## Local development

The pivot to running locally does not need a second implementation. PySpark in
`local[*]` mode runs the same ACON pipelines, the same transforms and the same
contracts on a laptop — a few million rows is comfortable, and the training step
is single-node pandas either way. A DuckDB or pandas rewrite of the transforms
would be a second codebase that drifts from the first; the whole point of
`transform.py` being a pure DataFrame function is that the runtime is
interchangeable.

Use DuckDB for ad-hoc querying of the Delta/Parquet output if it is convenient.
Do not use it to reimplement the pipeline.
