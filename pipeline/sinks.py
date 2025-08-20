from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame


def write_parquet_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    partition_by: Optional[list[str]] = None,
    trigger: str = "1 minute",
    query_name: Optional[str] = None,
):
    writer = (
        df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if query_name:
        writer = writer.queryName(query_name)
    return writer.start()


def write_to_dlq(df: DataFrame, dlq_path: str, checkpoint_path: str, query_name: str = "dlq"):
    return (
        df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", dlq_path)
        .option("checkpointLocation", checkpoint_path)
        .queryName(query_name)
        .start()
    )

