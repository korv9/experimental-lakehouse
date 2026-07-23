"""Gold schema for ``gold.analytics_works_by_category`` (a data product).

Gold tables are named ``gold.<consumer>_<product>`` and every one has a defined
consumer. Columns: ``category`` (string), ``year`` (int), ``work_count`` (bigint).

Consumer: the dashboard's "works over time" tile.
"""
TABLE = "gold.analytics_works_by_category"
