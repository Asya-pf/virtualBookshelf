import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from pipeline.transformations import clean_sales, clean_web_logs, clean_social


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_clean_sales_basic(spark: SparkSession):
    rows = [
        ("2024-01-01T00:00:00Z", "t1", "u1", "p1", 10.0, 2, "USD"),
        ("2024-01-01T00:00:00Z", "t1", "u1", "p1", 10.0, 2, "USD"),
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("currency", StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema)
    out = clean_sales(df)
    assert out.count() == 1
    assert "amount" in out.columns


def test_clean_web_logs_basic(spark: SparkSession):
    rows = [
        ("2024-01-01T00:00:00Z", "1.1.1.1", None, "/", 200, "ua", "s1"),
        ("2024-01-01T00:00:00Z", "1.1.1.1", None, "/", 200, "ua", "s1"),
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("session_id", StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema)
    out = clean_web_logs(df)
    assert out.count() == 1
    # method defaulted to GET and uppercased
    method_val = out.select(F.first("method")).collect()[0][0]
    assert method_val == "GET"


def test_clean_social_basic(spark: SparkSession):
    rows = [
        ("2024-01-01T00:00:00Z", "p1", "u1", " hello ", "twitter", None),
        ("2024-01-01T00:00:00Z", "p1", "u1", " hello ", "twitter", None),
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("source", StringType(), True),
        StructField("lang", StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema)
    out = clean_social(df)
    assert out.count() == 1
    lang_val = out.select(F.first("lang")).collect()[0][0]
    assert lang_val == "en"
