#!/usr/bin/env bash
set -euo pipefail

export PYSPARK_PYTHON=python3
export PYTHONUNBUFFERED=1

spark-submit \
  --master local[2] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.parquet.mergeSchema=false \
  pipeline/main.py "$@"
