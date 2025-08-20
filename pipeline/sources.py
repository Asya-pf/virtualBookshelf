from __future__ import annotations

from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json

from .schemas import web_log_schema, sales_schema, social_schema


def _base_kafka_reader(spark: SparkSession, kafka_bootstrap_servers: str, options: Optional[Dict[str, str]] = None):
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
    )
    if options:
        for k, v in options.items():
            reader = reader.option(k, v)
    return reader


def _read_and_parse(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str,
    schema,
) -> Tuple[DataFrame, DataFrame]:
    raw = _base_kafka_reader(spark, kafka_bootstrap_servers).option("subscribe", topic).load()
    json_df = raw.selectExpr("CAST(value AS STRING) AS json_str")
    parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"), col("json_str"))
    valid = parsed_df.where(col("data").isNotNull()).select("data.*")
    invalid = parsed_df.where(col("data").isNull()).select(col("json_str").alias("raw_payload"))
    return valid, invalid


def read_web_logs(
    spark: SparkSession, kafka_bootstrap_servers: str, topic: str = "web_logs"
) -> Tuple[DataFrame, DataFrame]:
    return _read_and_parse(spark, kafka_bootstrap_servers, topic, web_log_schema)


def read_sales(
    spark: SparkSession, kafka_bootstrap_servers: str, topic: str = "sales_transactions"
) -> Tuple[DataFrame, DataFrame]:
    return _read_and_parse(spark, kafka_bootstrap_servers, topic, sales_schema)


def read_social(
    spark: SparkSession, kafka_bootstrap_servers: str, topic: str = "social_feeds"
) -> Tuple[DataFrame, DataFrame]:
    return _read_and_parse(spark, kafka_bootstrap_servers, topic, social_schema)

