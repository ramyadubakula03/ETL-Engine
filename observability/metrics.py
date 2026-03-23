"""
In-memory metrics collector.
In production: push to Azure Monitor, Prometheus, or OpenTelemetry.
"""
import time
from collections import defaultdict
from typing import Dict, List


class MetricsCollector:
    def __init__(self):
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, float] = {}

    def increment(self, metric: str, value: int = 1):
        self._counters[metric] += value

    def gauge(self, metric: str, value: float):
        self._gauges[metric] = value

    def record(self, metric: str, value: float):
        self._histograms[metric].append(value)

    def start_timer(self, metric: str):
        self._timers[metric] = time.time()

    def stop_timer(self, metric: str) -> float:
        if metric not in self._timers:
            return 0.0
        elapsed = (time.time() - self._timers.pop(metric)) * 1000
        self.record(metric, elapsed)
        return elapsed

    def summary(self) -> Dict:
        hist_summary = {}
        for k, values in self._histograms.items():
            if values:
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                hist_summary[k] = {
                    "count": n,
                    "avg_ms": round(sum(sorted_vals) / n, 2),
                    "min_ms": round(sorted_vals[0], 2),
                    "max_ms": round(sorted_vals[-1], 2),
                    "p95_ms": round(sorted_vals[int(n * 0.95)], 2),
                    "p99_ms": round(sorted_vals[int(n * 0.99)], 2),
                }
        return {
            "counters": dict(self._counters),
            "gauges": self._gauges,
            "histograms": hist_summary,
        }


# Global singleton
metrics = MetricsCollector()
