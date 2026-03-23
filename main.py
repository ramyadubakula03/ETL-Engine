import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from core.dag import DAG
from core.task import Task
from tasks.etl_tasks import (
    extract_from_csv,
    extract_from_api,
    transform_clean_nulls,
    transform_normalize_salary,
    transform_aggregate_by_department,
    load_to_data_warehouse,
    load_to_json_sink,
    load_to_audit_log,
    flaky_transform,
)

DIVIDER = "=" * 65


def demo_employee_pipeline():
   
    print(f"\n{DIVIDER}")
    print("  DEMO 1: Employee Analytics ETL Pipeline")
    print(f"{DIVIDER}")

    dag = DAG(
        dag_id="employee_analytics_v1",
        description="Process employee CSV → normalize → aggregate → load"
    )

    # Define tasks
    t_extract  = Task("extract_csv",       extract_from_csv,                  retries=2, description="Read employee CSV")
    t_clean    = Task("clean_nulls",       transform_clean_nulls,             retries=1, description="Remove null records")
    t_norm     = Task("normalize_salary",  transform_normalize_salary,        retries=1, description="Min-max normalize salary")
    t_agg      = Task("aggregate_dept",    transform_aggregate_by_department, retries=1, description="Aggregate by department")
    t_load_dw  = Task("load_warehouse",    load_to_data_warehouse,            retries=3, description="Write to data warehouse")
    t_load_json= Task("load_json",         load_to_json_sink,                 retries=2, description="Write JSON output")
    t_audit    = Task("audit_log",         load_to_audit_log,                 retries=1, description="Write audit entry")

    # Register tasks
    for t in [t_extract, t_clean, t_norm, t_agg, t_load_dw, t_load_json, t_audit]:
        dag.add_task(t)

    # Define execution order (DAG edges)
    dag.set_dependency("clean_nulls",       depends_on="extract_csv")
    dag.set_dependency("normalize_salary",  depends_on="clean_nulls")
    dag.set_dependency("aggregate_dept",    depends_on="normalize_salary")
    dag.set_dependency("load_warehouse",    depends_on="aggregate_dept")
    dag.set_dependency("load_json",         depends_on="aggregate_dept")   # runs PARALLEL to load_warehouse
    dag.set_dependency("audit_log",         depends_on="load_warehouse")

    context = {
        "source": "employees.csv",
        "target_table": "analytics.fact_employees",
        "output_path": "/tmp/employee_output.json",
    }

    summary = dag.run(context=context)
    _print_summary(summary)
    return summary


def demo_retry_pipeline():
   
    print(f"\n{DIVIDER}")
    print("  DEMO 2: Retry Logic Pipeline (Transient Failure Simulation)")
    print(f"{DIVIDER}")

    dag = DAG(dag_id="retry_demo_pipeline", description="Demo retry on transient errors")

    t_extract = Task("extract_api",    extract_from_api, retries=2, description="Fetch from API")
    t_flaky   = Task("flaky_step",     flaky_transform,  retries=3, retry_delay_seconds=0.5,
                     description="Flaky transform (fails 2x, then recovers)")
    t_load    = Task("final_load",     load_to_json_sink, retries=1, description="Write output")

    for t in [t_extract, t_flaky, t_load]:
        dag.add_task(t)

    dag.set_dependency("flaky_step",  depends_on="extract_api")
    dag.set_dependency("final_load",  depends_on="flaky_step")

    summary = dag.run(context={"api_endpoint": "https://events.example.com/v1", "output_path": "/tmp/retry_output.json"})
    _print_summary(summary)
    return summary


def demo_cycle_detection():
   
    print(f"\n{DIVIDER}")
    print("  DEMO 3: Cycle Detection Guard")
    print(f"{DIVIDER}")
    from core.dag import DAGValidationError

    dag = DAG(dag_id="cyclic_dag_test")
    t1 = Task("task_A", lambda ctx: "A", retries=0)
    t2 = Task("task_B", lambda ctx: "B", retries=0)
    t3 = Task("task_C", lambda ctx: "C", retries=0)

    dag.add_task(t1); dag.add_task(t2); dag.add_task(t3)
    dag.set_dependency("task_B", depends_on="task_A")
    dag.set_dependency("task_C", depends_on="task_B")
    dag.set_dependency("task_A", depends_on="task_C")   # introduces cycle

    try:
        dag.run()
        print("❌ ERROR: Should have raised DAGValidationError!")
    except DAGValidationError as e:
        print(f"✅ Cycle correctly detected: {e}")


def _print_summary(summary: dict):
    print(f"\n{'─'*45}")
    print(f"  📊 EXECUTION SUMMARY")
    print(f"{'─'*45}")
    for k, v in summary.items():
        print(f"  {k:<20}: {v}")
    print(f"{'─'*45}\n")


if __name__ == "__main__":
    print("\n🏭 ETL ORCHESTRATION ENGINE — LIVE DEMO")
    print("   Inspired by Azure Data Factory & Apache Airflow\n")

    demo_employee_pipeline()
    demo_retry_pipeline()
    demo_cycle_detection()

    print(f"\n{DIVIDER}")
    print("  ✅ All demos complete. Check /tmp/ for output files.")
    print(f"{DIVIDER}\n")
