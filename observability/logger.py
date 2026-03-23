"""
Structured pipeline logger — emits JSON logs for observability.
In production, ship these to Azure Monitor / Application Insights.
"""
import json
import logging
import sys
from datetime import datetime


class PipelineLogger:
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.logger = logging.getLogger(f"etl.{dag_id}")
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)

    def _emit(self, level: str, message: str):
        record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "dag_id": self.dag_id,
            "message": message,
        }
        self.logger.info(json.dumps(record))

    def info(self, msg: str):
        self._emit("INFO", msg)

    def error(self, msg: str):
        self._emit("ERROR", msg)

    def warning(self, msg: str):
        self._emit("WARNING", msg)
