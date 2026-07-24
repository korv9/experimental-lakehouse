from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress


def build(source: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_DATE", "Building dimension", grain="one row per source update date")
    date = F.to_date("updated_at")
    return (
        source.where(F.col("updated_at").isNotNull())
        .select(
            F.date_format(date, "yyyyMMdd").cast("int").alias("date_key"),
            date.alias("full_date"),
            F.year(date).alias("calendar_year"),
            F.quarter(date).alias("calendar_quarter"),
            F.month(date).alias("calendar_month"),
            F.dayofmonth(date).alias("day_of_month"),
        )
        .dropDuplicates(["date_key"])
    )
