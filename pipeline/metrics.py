from __future__ import annotations

import json
import threading
import time
from typing import Callable, Optional

import boto3
from pyspark.sql.streaming import StreamingQuery


class CloudWatchMetricsReporter:
    def __init__(self, namespace: str = "SparkPipeline", region: Optional[str] = None):
        session = boto3.session.Session(region_name=region)
        self._client = session.client("cloudwatch")
        self._namespace = namespace
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self, query: StreamingQuery, app_name: str, interval_seconds: int = 60) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _run():
            while not self._stop_event.is_set():
                try:
                    progress = query.lastProgress
                    if progress:
                        self._publish(progress, app_name)
                except Exception:
                    pass
                self._stop_event.wait(interval_seconds)

        self._thread = threading.Thread(target=_run, name="cw-metrics-reporter", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _publish(self, progress: dict, app_name: str) -> None:
        metrics = []
        dims = [{"Name": "Application", "Value": app_name}]
        try:
            input_rps = float(progress.get("inputRowsPerSecond", 0.0))
            proc_rps = float(progress.get("processedRowsPerSecond", 0.0))
            batch_dur = float(progress.get("durationMs", {}).get("addBatch", 0.0))
            metrics.extend(
                [
                    {"MetricName": "InputRowsPerSecond", "Dimensions": dims, "Value": input_rps, "Unit": "Count/Second"},
                    {"MetricName": "ProcessedRowsPerSecond", "Dimensions": dims, "Value": proc_rps, "Unit": "Count/Second"},
                    {"MetricName": "BatchDurationMs", "Dimensions": dims, "Value": batch_dur, "Unit": "Milliseconds"},
                ]
            )
        except Exception:
            pass

        if metrics:
            self._client.put_metric_data(Namespace=self._namespace, MetricData=metrics)

