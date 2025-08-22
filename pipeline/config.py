from __future__ import annotations

import os
from dataclasses import dataclass


def _get_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in {"1", "true", "t", "yes", "y"}


@dataclass(frozen=True)
class AppConfig:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_ssl_enabled: bool = _get_bool("KAFKA_SSL_ENABLED", False)

    s3_output_path: str = os.getenv("S3_OUTPUT_PATH", "s3a://change-me/processed/")
    s3_checkpoint_path: str = os.getenv("S3_CHECKPOINT_PATH", "s3a://change-me/checkpoints/")
    s3_dlq_path: str = os.getenv("S3_DLQ_PATH", "s3a://change-me/dlq/")

    aws_region: str = os.getenv("AWS_REGION", "us-east-1")

    enable_cloudwatch_logs: bool = _get_bool("ENABLE_CLOUDWATCH_LOGS", False)
    enable_cloudwatch_metrics: bool = _get_bool("ENABLE_CLOUDWATCH_METRICS", False)

    spark_ml_model_path: str | None = os.getenv("SPARK_ML_MODEL_PATH")
    sagemaker_endpoint_name: str | None = os.getenv("SAGEMAKER_ENDPOINT_NAME")


def load_config() -> AppConfig:
    return AppConfig()

