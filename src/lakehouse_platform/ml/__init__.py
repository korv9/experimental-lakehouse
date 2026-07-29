"""The ML layer: features derived from Gold, models scored back into the lakehouse.

The layer is deliberately thin. Feature engineering is a normal ACON pipeline —
it reads Gold, transforms, passes a quality gate and writes a contracted table —
so nothing here re-implements orchestration. What lives in this package is only
the part that is specific to machine learning and that a notebook would
otherwise get subtly wrong:

``features``  point-in-time-correct lag and rolling-window helpers
``splits``    time-ordered train/test splits with a leakage embargo
``metrics``   forecast accuracy measures that survive zero-demand days
``registry``  control tables so a model run is as queryable as a pipeline run

``splits`` and ``metrics`` are pure Python and run in CI without Spark.
"""
