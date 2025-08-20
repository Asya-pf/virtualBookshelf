from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _add_ingest_time(df: DataFrame) -> DataFrame:
    return df.withColumn("ingest_ts", F.current_timestamp())


def clean_web_logs(df: DataFrame) -> DataFrame:
    cleaned = (
        df.dropna(subset=["timestamp", "ip", "url"])  # minimal required fields
        .withColumn("status", F.coalesce(F.col("status"), F.lit(0)))
        .withColumn("method", F.upper(F.coalesce(F.col("method"), F.lit("GET"))))
    )
    cleaned = _add_ingest_time(cleaned)
    # deduplicate by session and timestamp; keep latest by ingest
    w = Window.partitionBy("session_id", "timestamp").orderBy(F.col("ingest_ts").desc())
    return cleaned.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")


def clean_sales(df: DataFrame) -> DataFrame:
    cleaned = (
        df.dropna(subset=["timestamp", "transaction_id", "user_id", "product_id"])  # required
        .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
        .withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)))
        .withColumn("amount", F.col("price") * F.col("quantity"))
    )
    cleaned = _add_ingest_time(cleaned)
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingest_ts").desc())
    return cleaned.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")


def clean_social(df: DataFrame) -> DataFrame:
    cleaned = (
        df.dropna(subset=["timestamp", "post_id", "user_id", "text"])  # required
        .withColumn("text", F.trim(F.col("text")))
        .withColumn("lang", F.lower(F.coalesce(F.col("lang"), F.lit("en"))))
    )
    cleaned = _add_ingest_time(cleaned)
    w = Window.partitionBy("post_id").orderBy(F.col("ingest_ts").desc())
    return cleaned.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")


def with_watermark(df: DataFrame, ts_col: str = "timestamp", delay: str = "10 minutes") -> DataFrame:
    return df.withWatermark(ts_col, delay)

