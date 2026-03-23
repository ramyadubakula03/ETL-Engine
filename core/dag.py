"""
DAG (Directed Acyclic Graph) Engine for ETL Pipeline Orchestration
"""
import time
import uuid
import threading
from collections import defaultdict, deque
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any
from core.task import Task, TaskStatus
from observability.logger import PipelineLogger


class DAGValidationError(Exception):
    pass


class DAG:
    """
    Represents a Directed Acyclic Graph of ETL tasks.
    Supports dependency resolution, parallel execution, and retry logic.
    """

    def __init__(self, dag_id: str, description: str = ""):
        self.dag_id = dag_id
        self.description = description
        self.tasks: Dict[str, Task] = {}
        self.dependencies: Dict[str, List[str]] = defaultdict(list)  # task_id -> [upstream_task_ids]
        self.created_at = datetime.utcnow().isoformat()
        self.logger = PipelineLogger(dag_id)
        self.run_id: Optional[str] = None
        self.status = "IDLE"
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.execution_log: List[Dict] = []

    def add_task(self, task: Task):
        """Register a task in the DAG."""
        if task.task_id in self.tasks:
            raise DAGValidationError(f"Task '{task.task_id}' already exists in DAG '{self.dag_id}'")
        self.tasks[task.task_id] = task
        self.logger.info(f"Task '{task.task_id}' added to DAG")

    def set_dependency(self, task_id: str, depends_on: str):
        """
        Set task_id to run only after depends_on completes.
        depends_on -> task_id
        """
        if task_id not in self.tasks:
            raise DAGValidationError(f"Task '{task_id}' not found in DAG")
        if depends_on not in self.tasks:
            raise DAGValidationError(f"Upstream task '{depends_on}' not found in DAG")
        self.dependencies[task_id].append(depends_on)

    def _validate_no_cycles(self):
        """Topological sort to detect cycles using Kahn's algorithm."""
        in_degree = {t: 0 for t in self.tasks}
        for task_id, upstreams in self.dependencies.items():
            in_degree[task_id] += len(upstreams)

        queue = deque([t for t, deg in in_degree.items() if deg == 0])
        visited = 0

        adj = defaultdict(list)
        for task_id, upstreams in self.dependencies.items():
            for u in upstreams:
                adj[u].append(task_id)

        while queue:
            node = queue.popleft()
            visited += 1
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited != len(self.tasks):
            raise DAGValidationError("Cycle detected in DAG! ETL pipelines must be acyclic.")

    def _get_execution_order(self) -> List[List[str]]:
        """
        Returns tasks grouped by execution wave (tasks in the same wave run in parallel).
        """
        in_degree = {t: 0 for t in self.tasks}
        adj = defaultdict(list)

        for task_id, upstreams in self.dependencies.items():
            in_degree[task_id] += len(upstreams)
            for u in upstreams:
                adj[u].append(task_id)

        waves = []
        queue = deque([t for t, deg in in_degree.items() if deg == 0])

        while queue:
            wave = list(queue)
            waves.append(wave)
            queue.clear()
            for task_id in wave:
                for neighbor in adj[task_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        return waves

    def run(self, context: Optional[Dict[str, Any]] = None) -> Dict:
        """
        Execute the DAG. Tasks in the same wave run in parallel threads.
        Returns execution summary.
        """
        self._validate_no_cycles()
        self.run_id = str(uuid.uuid4())[:8]
        self.status = "RUNNING"
        self.start_time = time.time()
        context = context or {}

        self.logger.info(f"🚀 DAG '{self.dag_id}' started | Run ID: {self.run_id}")
        self.execution_log.append({
            "event": "DAG_START",
            "run_id": self.run_id,
            "timestamp": datetime.utcnow().isoformat()
        })

        waves = self._get_execution_order()
        failed_tasks = []

        for wave_idx, wave in enumerate(waves):
            self.logger.info(f"⚡ Wave {wave_idx + 1}: Running tasks {wave} in parallel")
            threads = []
            results = {}

            def run_task(task_id, ctx):
                task = self.tasks[task_id]
                result = task.execute(ctx)
                results[task_id] = result
                self.execution_log.append({
                    "event": "TASK_COMPLETE",
                    "task_id": task_id,
                    "status": task.status.value,
                    "duration_ms": task.duration_ms,
                    "timestamp": datetime.utcnow().isoformat()
                })

            for task_id in wave:
                t = threading.Thread(target=run_task, args=(task_id, context))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Check for failures
            for task_id in wave:
                if self.tasks[task_id].status == TaskStatus.FAILED:
                    failed_tasks.append(task_id)

            if failed_tasks:
                self.logger.error(f"❌ Tasks failed: {failed_tasks}. Halting pipeline.")
                break

        self.end_time = time.time()
        duration = round((self.end_time - self.start_time) * 1000, 2)
        self.status = "FAILED" if failed_tasks else "SUCCESS"

        summary = {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "status": self.status,
            "duration_ms": duration,
            "total_tasks": len(self.tasks),
            "succeeded": sum(1 for t in self.tasks.values() if t.status == TaskStatus.SUCCESS),
            "failed": len(failed_tasks),
            "skipped": sum(1 for t in self.tasks.values() if t.status == TaskStatus.SKIPPED),
        }

        self.logger.info(f"{'✅' if self.status == 'SUCCESS' else '❌'} DAG '{self.dag_id}' {self.status} in {duration}ms")
        self.execution_log.append({"event": "DAG_END", **summary, "timestamp": datetime.utcnow().isoformat()})
        return summary
