## Spark Streaming Data Pipeline (Python)

Production-ready template for a scalable data processing pipeline using Apache Spark (Structured Streaming), AWS S3 for storage, CloudWatch logging/metrics, and optional real-time ML inference via Spark ML or SageMaker. Designed for CI/CD and containerized deployments.

### Features
- Multiple sources: Kafka topics for `web_logs`, `sales_transactions`, `social_feeds` (extensible)
- Cleaning, transformation, deduplication, and watermarking
- Real-time predictions via either Spark ML `PipelineModel` or SageMaker endpoint
- Writes processed data to Amazon S3 as Parquet with partitioning
- Error handling, DLQ, and CloudWatch logs/metrics
- Dockerized; CI with code quality checks and unit tests

### Requirements
- Python 3.10+
- Java 11 (for Spark)
- AWS credentials with S3 and CloudWatch/SageMaker permissions (e.g., via IAM role or env vars)

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS` (e.g., `localhost:9092`)
- `KAFKA_SSL_ENABLED` (default `false`)
- `S3_OUTPUT_PATH` (e.g., `s3a://my-bucket/processed/`)
- `S3_CHECKPOINT_PATH` (e.g., `s3a://my-bucket/checkpoints/streaming/`)
- `S3_DLQ_PATH` (e.g., `s3a://my-bucket/dlq/`)
- `AWS_REGION` (default `us-east-1`)
- `ENABLE_CLOUDWATCH_LOGS` (`true`/`false`)
- `ENABLE_CLOUDWATCH_METRICS` (`true`/`false`)
- `SPARK_ML_MODEL_PATH` (e.g., `s3a://my-bucket/models/pipeline_model`) Optional
- `SAGEMAKER_ENDPOINT_NAME` Optional (if using SageMaker for inference)

### Install (local dev)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Ensure Java 11 is installed. On Debian/Ubuntu:
```bash
sudo apt-get update && sudo apt-get install -y openjdk-11-jre-headless
```

### Run locally with Kafka
Provide Kafka topics `web_logs`, `sales_transactions`, `social_feeds`.
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export S3_OUTPUT_PATH=s3a://my-bucket/processed/
export S3_CHECKPOINT_PATH=s3a://my-bucket/checkpoints/streaming/
export S3_DLQ_PATH=s3a://my-bucket/dlq/
export AWS_REGION=us-east-1

./scripts/run_local.sh
```

This uses `spark.jars.packages` to pull `hadoop-aws` and AWS SDK automatically.

### Docker
```bash
docker build -t spark-pipeline:latest .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e S3_OUTPUT_PATH=s3a://my-bucket/processed/ \
  -e S3_CHECKPOINT_PATH=s3a://my-bucket/checkpoints/streaming/ \
  -e S3_DLQ_PATH=s3a://my-bucket/dlq/ \
  -e AWS_REGION=us-east-1 \
  spark-pipeline:latest
```

When running on EMR/EKS with CloudWatch/Fluent Bit, stdout logs are captured automatically. You can also enable direct CloudWatch logging with `ENABLE_CLOUDWATCH_LOGS=true`.

### CI/CD
GitHub Actions workflow `.github/workflows/ci.yml` runs formatting, linting, and unit tests. Adapt to your runner/image.

### Notes on ML Inference
- Prefer Spark ML `PipelineModel` for in-cluster inference (`SPARK_ML_MODEL_PATH`).
- If using SageMaker (`SAGEMAKER_ENDPOINT_NAME`), the template includes a batch invoker sketch. You may need to adapt the request/response payloads to your model contract.

### Development Tips
- Keep transformations pure and testable (see `pipeline/transformations.py`).
- Keep schemas centralized (see `pipeline/schemas.py`).
- Avoid heavy work at import time to keep unit tests fast.

# virtualBookshelf