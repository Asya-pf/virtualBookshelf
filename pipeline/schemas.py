from __future__ import annotations

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    IntegerType,
)


web_log_schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("ip", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("session_id", StringType(), True),
    ]
)


sales_schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("currency", StringType(), True),
    ]
)


social_schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("post_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("source", StringType(), True),
        StructField("lang", StringType(), True),
    ]
)

