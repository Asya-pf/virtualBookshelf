from __future__ import annotations

from typing import Optional

import boto3
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def apply_spark_ml_predictions(df: DataFrame, model_path: str, features_col: str = "features", prediction_col: str = "prediction") -> DataFrame:
    model = PipelineModel.load(model_path)
    pred_df = model.transform(df)
    # Ensure we keep only relevant columns plus prediction
    if prediction_col not in pred_df.columns:
        # Commonly regression/classification produces column named 'prediction'
        pass
    return pred_df


def _invoke_sagemaker_batch_udf(endpoint_name: str):
    session = boto3.session.Session()
    client = session.client("sagemaker-runtime")

    def _predict(payload: str) -> Optional[str]:
        try:
            resp = client.invoke_endpoint(
                EndpointName=endpoint_name,
                ContentType="application/json",
                Body=payload.encode("utf-8"),
            )
            result = resp["Body"].read().decode("utf-8")
            return result
        except Exception:
            return None

    return F.udf(_predict, returnType=StringType())


def apply_sagemaker_predictions(df: DataFrame, endpoint_name: str, input_col: str, output_col: str = "prediction_raw") -> DataFrame:
    predict_udf = _invoke_sagemaker_batch_udf(endpoint_name)
    return df.withColumn(output_col, predict_udf(F.col(input_col)))

