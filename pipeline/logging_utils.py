from __future__ import annotations

import logging
import os
import sys
from logging import Logger

from pythonjsonlogger import jsonlogger


def _create_json_formatter() -> logging.Formatter:
    return jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        json_ensure_ascii=False,
    )


def _configure_stream_handler(level: int) -> logging.Handler:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(_create_json_formatter())
    return handler


def get_logger(name: str) -> Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(_configure_stream_handler(level))
        logger.propagate = False
    return logger

