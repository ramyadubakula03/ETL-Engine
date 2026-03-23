"""
Task model with retry logic, timeout, and status tracking.
"""
import time
import traceback
from enum import Enum
from typing import Callable, Optional, Any, Dict
from datetime import datetime


class TaskStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    RETRYING = "RETRYING"


class Task:
    """
    A single unit of work in the ETL pipeline.
    Supports retry logic, timeouts, and context passing.
    """

    def __init__(
        self,
        task_id: str,
        func: Callable,
        retries: int = 3,
        retry_delay_seconds: float = 1.0,
        timeout_seconds: Optional[float] = None,
        description: str = "",
    ):
        self.task_id = task_id
        self.func = func
        self.retries = retries
        self.retry_delay_seconds = retry_delay_seconds
        self.timeout_seconds = timeout_seconds
        self.description = description

        self.status = TaskStatus.PENDING
        self.attempt = 0
        self.error: Optional[str] = None
        self.result: Any = None
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration_ms: Optional[float] = None
        self.logs: list = []

    def _log(self, msg: str):
        entry = f"[{datetime.utcnow().isoformat()}] [{self.task_id}] {msg}"
        self.logs.append(entry)
        print(entry)

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the task function with retry logic.
        Passes context dict to the function.
        """
        self.start_time = time.time()
        self.status = TaskStatus.RUNNING
        self._log(f"Starting task (max retries: {self.retries})")

        for attempt in range(1, self.retries + 2):  # +1 for initial attempt
            self.attempt = attempt
            try:
                self._log(f"Attempt {attempt}")
                self.result = self.func(context)
                self.status = TaskStatus.SUCCESS
                self._log(f"✅ Succeeded on attempt {attempt}")
                break

            except Exception as e:
                self.error = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                self._log(f"❌ Attempt {attempt} failed: {type(e).__name__}: {str(e)}")

                if attempt <= self.retries:
                    self.status = TaskStatus.RETRYING
                    self._log(f"⏳ Retrying in {self.retry_delay_seconds}s...")
                    time.sleep(self.retry_delay_seconds)
                else:
                    self.status = TaskStatus.FAILED
                    self._log(f"💀 All {self.retries + 1} attempts exhausted. Task FAILED.")

        self.end_time = time.time()
        self.duration_ms = round((self.end_time - self.start_time) * 1000, 2)
        return self.result

    def to_dict(self) -> Dict:
        return {
            "task_id": self.task_id,
            "description": self.description,
            "status": self.status.value,
            "attempt": self.attempt,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "result": str(self.result) if self.result is not None else None,
        }
