# 🏭 Mini ETL Orchestration Engine

A production-grade ETL pipeline orchestrator built in Python, inspired by **Azure Data Factory** and **Apache Airflow**.

## Features

| Feature | Description |
|---|---|
| **DAG Execution** | Define task dependencies as a Directed Acyclic Graph |
| **Parallel Execution** | Tasks without dependencies run concurrently (wave-based) |
| **Retry Logic** | Configurable retries with delay on transient failures |
| **Cycle Detection** | Kahn's algorithm prevents invalid pipeline definitions |
| **Observability** | Structured JSON logs + execution metrics per run |
| **Scheduler** | Background thread scheduler for recurring pipeline runs |
| **ETL Templates** | Pre-built Extract / Transform / Load task functions |

## Project Structure

```
etl_engine/
├── core/
│   ├── dag.py          # DAG engine (dependency resolution, wave execution)
│   ├── task.py         # Task model with retry logic & status tracking
│   └── scheduler.py    # Cron-like pipeline scheduler
├── tasks/
│   └── etl_tasks.py    # Pre-built Extract / Transform / Load functions
├── observability/
│   ├── logger.py       # Structured JSON logger
│   └── metrics.py      # In-memory metrics (p95/p99 latency tracking)
└── main.py             # Demo runner (3 live demos)
```

## Quick Start

```bash
python main.py
```

## Usage Example

```python
from core.dag import DAG
from core.task import Task

dag = DAG(dag_id="my_pipeline")

extract = Task("extract", my_extract_fn, retries=3)
transform = Task("transform", my_transform_fn, retries=2)
load = Task("load", my_load_fn, retries=3)

dag.add_task(extract)
dag.add_task(transform)
dag.add_task(load)

dag.set_dependency("transform", depends_on="extract")
dag.set_dependency("load", depends_on="transform")

summary = dag.run(context={"source": "s3://bucket/data.csv"})
```

## Demos

1. **Employee Analytics Pipeline** — CSV → Clean → Normalize → Aggregate → Load (parallel loads)
2. **Retry Demo** — Flaky task that fails 2x then recovers, showing retry backoff
3. **Cycle Detection** — Validates that circular dependencies are caught at runtime

## Resume Bullets

- Built a DAG-based ETL orchestration engine in Python supporting parallel task execution, configurable retry logic, and structured JSON observability logs
- Implemented Kahn's algorithm for cycle detection in pipeline dependency graphs, preventing invalid DAG definitions at runtime
- Designed wave-based parallel executor using Python threading, reducing pipeline latency by running independent tasks concurrently
- Architected modular Extract/Transform/Load task library with context-passing pattern, enabling reusable pipeline components across multiple DAGs

## Tech Stack

- Python 3.8+, threading, collections
- No external dependencies required
- Production path: Azure Data Factory, Apache Airflow, Azure Monitor
