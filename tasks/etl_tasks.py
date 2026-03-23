
import time
import random
import json
import csv
import io
from typing import Dict, Any, List


# ─── EXTRACT TASKS ────────────────────────────────────────────────────────────

def extract_from_csv(context: Dict[str, Any]) -> List[Dict]:
    """Simulate reading records from a CSV source."""
    print(f"  [EXTRACT] Reading from CSV: {context.get('source', 'data.csv')}")
    time.sleep(0.3)  # simulate I/O
    sample_csv = """id,name,age,salary,department
1,Alice,30,90000,Engineering
2,Bob,25,75000,Marketing
3,Charlie,35,120000,Engineering
4,Diana,28,85000,Sales
5,Eve,32,95000,Engineering
6,Frank,45,150000,Management"""
    reader = csv.DictReader(io.StringIO(sample_csv))
    records = list(reader)
    context["raw_records"] = records
    print(f"  [EXTRACT] ✅ Extracted {len(records)} records")
    return records


def extract_from_api(context: Dict[str, Any]) -> List[Dict]:
    """Simulate fetching data from a REST API."""
    endpoint = context.get("api_endpoint", "https://api.example.com/users")
    print(f"  [EXTRACT] Calling API: {endpoint}")
    time.sleep(0.5)  # simulate network latency
    # Simulated API response
    records = [
        {"user_id": i, "event": random.choice(["login", "purchase", "view"]),
         "amount": round(random.uniform(10, 500), 2), "ts": f"2024-01-{i:02d}"}
        for i in range(1, 11)
    ]
    context["raw_records"] = records
    print(f"  [EXTRACT] ✅ Fetched {len(records)} events from API")
    return records


def extract_from_database(context: Dict[str, Any]) -> List[Dict]:
    """Simulate a SQL query result from a database."""
    query = context.get("query", "SELECT * FROM orders WHERE status = 'pending'")
    print(f"  [EXTRACT] Executing query: {query}")
    time.sleep(0.4)
    records = [
        {"order_id": f"ORD-{100 + i}", "customer": f"Customer_{i}",
         "amount": round(random.uniform(50, 2000), 2), "status": "pending"}
        for i in range(1, 8)
    ]
    context["raw_records"] = records
    print(f"  [EXTRACT] ✅ Query returned {len(records)} rows")
    return records


# ─── TRANSFORM TASKS ──────────────────────────────────────────────────────────

def transform_clean_nulls(context: Dict[str, Any]) -> List[Dict]:
    """Remove records with missing critical fields."""
    records = context.get("raw_records", [])
    print(f"  [TRANSFORM] Cleaning nulls from {len(records)} records")
    time.sleep(0.2)
    cleaned = [r for r in records if all(v not in (None, "", "null") for v in r.values())]
    removed = len(records) - len(cleaned)
    context["cleaned_records"] = cleaned
    print(f"  [TRANSFORM] ✅ Removed {removed} null rows. {len(cleaned)} records remaining")
    return cleaned


def transform_normalize_salary(context: Dict[str, Any]) -> List[Dict]:
    """Normalize salary to a 0-1 range (min-max scaling)."""
    records = context.get("cleaned_records", context.get("raw_records", []))
    print(f"  [TRANSFORM] Normalizing salary values")
    time.sleep(0.15)
    salaries = [float(r.get("salary", 0)) for r in records if r.get("salary")]
    if not salaries:
        return records
    min_sal, max_sal = min(salaries), max(salaries)
    for r in records:
        if r.get("salary"):
            r["salary_normalized"] = round(
                (float(r["salary"]) - min_sal) / (max_sal - min_sal), 4
            )
    context["transformed_records"] = records
    print(f"  [TRANSFORM] ✅ Salary normalized. Range: {min_sal} - {max_sal}")
    return records


def transform_aggregate_by_department(context: Dict[str, Any]) -> Dict:
    """Aggregate salary stats per department."""
    records = context.get("transformed_records", context.get("raw_records", []))
    print(f"  [TRANSFORM] Aggregating by department")
    time.sleep(0.2)
    dept_stats = {}
    for r in records:
        dept = r.get("department", "Unknown")
        if dept not in dept_stats:
            dept_stats[dept] = {"count": 0, "total_salary": 0}
        dept_stats[dept]["count"] += 1
        dept_stats[dept]["total_salary"] += float(r.get("salary", 0))
    for dept in dept_stats:
        dept_stats[dept]["avg_salary"] = round(
            dept_stats[dept]["total_salary"] / dept_stats[dept]["count"], 2
        )
    context["aggregated"] = dept_stats
    print(f"  [TRANSFORM] ✅ Aggregated {len(dept_stats)} departments")
    return dept_stats


# ─── LOAD TASKS ───────────────────────────────────────────────────────────────

def load_to_data_warehouse(context: Dict[str, Any]) -> bool:
    """Simulate loading transformed data to a data warehouse (e.g., Azure Synapse)."""
    records = context.get("transformed_records", context.get("cleaned_records", []))
    target = context.get("target_table", "analytics.fact_employees")
    print(f"  [LOAD] Writing {len(records)} records to {target}")
    time.sleep(0.5)  # simulate write latency
    print(f"  [LOAD] ✅ Successfully loaded {len(records)} rows to {target}")
    context["load_status"] = "success"
    return True


def load_to_json_sink(context: Dict[str, Any]) -> str:
    """Write final output to a JSON file sink."""
    data = context.get("aggregated", context.get("transformed_records", []))
    output_path = context.get("output_path", "C:/Users/ramya/Desktop/projects/etl-engine/output.json")
    print(f"  [LOAD] Writing output to {output_path}")
    time.sleep(0.1)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  [LOAD] ✅ JSON written to {output_path}")
    context["output_path"] = output_path
    return output_path


def load_to_audit_log(context: Dict[str, Any]) -> bool:
    """Write a pipeline audit record (compliance / observability)."""
    print(f"  [AUDIT] Writing audit log entry")
    time.sleep(0.1)
    audit = {
        "pipeline": context.get("dag_id", "unknown"),
        "records_processed": len(context.get("transformed_records", [])),
        "load_status": context.get("load_status", "unknown"),
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open("C:/Users/ramya/Desktop/projects/etl-engine/audit_log.json", "w") as f:
        json.dump(audit, f, indent=2)
    print(f"  [AUDIT] ✅ Audit entry written")
    return True


# ─── FLAKY TASK (for retry demo) ──────────────────────────────────────────────

_attempt_counter = {}

def flaky_transform(context: Dict[str, Any]) -> str:
    """Simulates a task that fails the first 2 attempts, then succeeds (retry demo)."""
    key = "flaky_transform"
    _attempt_counter[key] = _attempt_counter.get(key, 0) + 1
    print(f"  [FLAKY] This is attempt #{_attempt_counter[key]}")
    if _attempt_counter[key] < 3:
        raise ConnectionError(f"Simulated transient failure on attempt {_attempt_counter[key]}")
    print(f"  [FLAKY] ✅ Succeeded on attempt {_attempt_counter[key]}")
    return "recovered"
