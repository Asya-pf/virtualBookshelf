from __future__ import annotations

import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from .config import load_config
from .logging_utils import get_logger
from .sources import read_web_logs, read_sales, read_social
from .transformations import clean_web_logs, clean_sales, clean_social, with_watermark
from .sinks import write_parquet_stream, write_to_dlq
from .ml_inference import apply_spark_ml_predictions, apply_sagemaker_predictions
from .metrics import CloudWatchMetricsReporter


logger = get_logger(__name__)


def _build_spark_session(app_name: str = "spark-streaming-pipeline") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _attach_ml_if_configured(df: DataFrame, cfg) -> DataFrame:
    if cfg.spark_ml_model_path:
        logger.info("Applying Spark ML predictions", extra={"model_path": cfg.spark_ml_model_path})
        df = apply_spark_ml_predictions(df, cfg.spark_ml_model_path)
    elif cfg.sagemaker_endpoint_name:
        logger.info("Applying SageMaker predictions", extra={"endpoint": cfg.sagemaker_endpoint_name})
        # Example: assume column 'payload_json' exists; adapt as needed
        df = apply_sagemaker_predictions(df, cfg.sagemaker_endpoint_name, input_col="payload_json")
    else:
        logger.info("No ML inference configured")
    return df


def main(argv: List[str]) -> int:
    cfg = load_config()
    logger.info("Starting pipeline", extra={"config": cfg.__dict__})
    spark = _build_spark_session()

    # Sources
    web_logs, web_invalid = read_web_logs(spark, cfg.kafka_bootstrap_servers)
    sales, sales_invalid = read_sales(spark, cfg.kafka_bootstrap_servers)
    social, social_invalid = read_social(spark, cfg.kafka_bootstrap_servers)

    # Transformations and watermarks
    web_clean = with_watermark(clean_web_logs(web_logs), "timestamp", "10 minutes")
    sales_clean = with_watermark(clean_sales(sales), "timestamp", "10 minutes")
    social_clean = with_watermark(clean_social(social), "timestamp", "10 minutes")

    # Enrich with a unified source label and simple join keys for demonstration
    web_ready = web_clean.withColumn("source", F.lit("web"))
    sales_ready = sales_clean.withColumn("source", F.lit("sales"))
    social_ready = social_clean.withColumn("source", F.lit("social"))

    # Example: Union to a single analytics stream with a minimal common projection
    def base_projection(df: DataFrame) -> DataFrame:
        return (
            df.select(
                F.col("timestamp"),
                F.col("ingest_ts"),
                F.col("source"),
                F.to_json(F.struct([c for c in df.columns])).alias("payload_json"),
            )
        )

    unified = base_projection(web_ready).unionByName(base_projection(sales_ready)).unionByName(base_projection(social_ready))

    # ML inference (optional)
    unified_with_pred = _attach_ml_if_configured(unified, cfg)

    # Sink
    query = write_parquet_stream(
        unified_with_pred,
        output_path=cfg.s3_output_path,
        checkpoint_path=cfg.s3_checkpoint_path,
        partition_by=["source"],
        query_name="unified_analytics_stream",
    )

    logger.info("Streaming query started", extra={"id": query.id, "name": query.name})

    # DLQ writers for invalid records
    dlq_queries = [
        write_to_dlq(web_invalid, f"{cfg.s3_dlq_path}/web", f"{cfg.s3_checkpoint_path}/dlq/web", "dlq_web"),
        write_to_dlq(sales_invalid, f"{cfg.s3_dlq_path}/sales", f"{cfg.s3_checkpoint_path}/dlq/sales", "dlq_sales"),
        write_to_dlq(social_invalid, f"{cfg.s3_dlq_path}/social", f"{cfg.s3_checkpoint_path}/dlq/social", "dlq_social"),
    ]

    # Optional CloudWatch metrics
    if cfg.enable_cloudwatch_metrics:
        reporter = CloudWatchMetricsReporter(region=cfg.aws_region)
        reporter.start(query, app_name="spark-streaming-pipeline", interval_seconds=60)

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))