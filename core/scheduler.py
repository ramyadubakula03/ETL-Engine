"""
Pipeline Scheduler — runs DAGs on a schedule (like Azure Data Factory triggers).
"""
import time
import threading
from typing import Callable, Dict, Optional
from datetime import datetime


class ScheduledPipeline:
    def __init__(self, name: str, dag_factory: Callable, interval_seconds: int = 60):
        """
        Args:
            name: Pipeline name
            dag_factory: A callable that returns a DAG instance (fresh each run)
            interval_seconds: How often to run the pipeline
        """
        self.name = name
        self.dag_factory = dag_factory
        self.interval_seconds = interval_seconds
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.run_history: list = []
        self.total_runs = 0
        self.successful_runs = 0

    def _run_loop(self):
        while self._running:
            print(f"\n🕐 [{datetime.utcnow().isoformat()}] Scheduler triggering '{self.name}'")
            dag = self.dag_factory()
            summary = dag.run()
            self.total_runs += 1
            if summary["status"] == "SUCCESS":
                self.successful_runs += 1
            self.run_history.append(summary)
            if len(self.run_history) > 100:
                self.run_history.pop(0)
            print(f"⏭️  Next run in {self.interval_seconds}s\n")
            time.sleep(self.interval_seconds)

    def start(self):
        """Start the scheduler in a background thread."""
        if self._running:
            print(f"Scheduler '{self.name}' is already running.")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"✅ Scheduler '{self.name}' started (interval: {self.interval_seconds}s)")

    def stop(self):
        """Stop the scheduler."""
        self._running = False
        print(f"🛑 Scheduler '{self.name}' stopped.")

    def stats(self) -> Dict:
        success_rate = (
            round(self.successful_runs / self.total_runs * 100, 1)
            if self.total_runs > 0 else 0
        )
        return {
            "pipeline": self.name,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.total_runs - self.successful_runs,
            "success_rate_pct": success_rate,
        }
