import sys, os, json, threading
sys.path.insert(0, os.path.dirname(__file__))

from flask import Flask, jsonify, render_template_string
from core.dag import DAG
from core.task import Task
from tasks.etl_tasks import (
    extract_from_csv, extract_from_api,
    transform_clean_nulls, transform_normalize_salary,
    transform_aggregate_by_department,
    load_to_data_warehouse, load_to_json_sink, load_to_audit_log,
    flaky_transform
)

app = Flask(__name__)

pipeline_results = []
pipeline_lock = threading.Lock()


def build_employee_dag():
    dag = DAG("employee_analytics", "CSV → Clean → Normalize → Aggregate → Load")
    tasks = [
        Task("extract_csv",      extract_from_csv,                  retries=2),
        Task("clean_nulls",      transform_clean_nulls,             retries=1),
        Task("normalize_salary", transform_normalize_salary,        retries=1),
        Task("aggregate_dept",   transform_aggregate_by_department, retries=1),
        Task("load_warehouse",   load_to_data_warehouse,            retries=3),
        Task("load_json",        load_to_json_sink,                 retries=2),
        Task("audit_log",        load_to_audit_log,                 retries=1),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.set_dependency("clean_nulls",       "extract_csv")
    dag.set_dependency("normalize_salary",  "clean_nulls")
    dag.set_dependency("aggregate_dept",    "normalize_salary")
    dag.set_dependency("load_warehouse",    "aggregate_dept")
    dag.set_dependency("load_json",         "aggregate_dept")
    dag.set_dependency("audit_log",         "load_warehouse")
    return dag


def build_retry_dag():
    dag = DAG("retry_demo", "API Extract → Flaky Transform → Load")
    tasks = [
        Task("extract_api", extract_from_api,  retries=2),
        Task("flaky_step",  flaky_transform,   retries=3, retry_delay_seconds=0.3),
        Task("final_load",  load_to_json_sink, retries=2),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.set_dependency("flaky_step",  "extract_api")
    dag.set_dependency("final_load",  "flaky_step")
    return dag


HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>ETL Orchestration Engine</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@400;700;800&display=swap" rel="stylesheet"/>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0e1a;--surface:#111827;--surface2:#1a2235;
  --border:#1e2d45;--accent:#00d4ff;--accent2:#7c3aed;
  --success:#10b981;--fail:#ef4444;--warn:#f59e0b;
  --text:#e2e8f0;--muted:#64748b;
  --font-mono:'JetBrains Mono',monospace;
  --font-display:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--font-mono);min-height:100vh;}

/* grid bg */
body::before{
  content:'';position:fixed;inset:0;
  background-image:linear-gradient(var(--border) 1px,transparent 1px),
    linear-gradient(90deg,var(--border) 1px,transparent 1px);
  background-size:40px 40px;opacity:.3;pointer-events:none;z-index:0;
}

.wrap{max-width:1100px;margin:0 auto;padding:2rem;position:relative;z-index:1;}

header{margin-bottom:2.5rem;}
.logo{font-family:var(--font-display);font-size:2rem;font-weight:800;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:-1px;}
.tagline{color:var(--muted);font-size:.75rem;margin-top:.25rem;letter-spacing:.1em;text-transform:uppercase;}

/* pipeline cards */
.pipelines{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:2rem;}
.pipeline-card{
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:1.5rem;cursor:pointer;transition:border-color .2s,transform .15s;position:relative;overflow:hidden;
}
.pipeline-card::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--accent),var(--accent2));opacity:0;transition:opacity .2s;
}
.pipeline-card:hover{border-color:var(--accent);transform:translateY(-2px);}
.pipeline-card:hover::before{opacity:1;}
.pipeline-card.running{border-color:var(--warn);animation:pulse-border 1s ease-in-out infinite;}
@keyframes pulse-border{0%,100%{border-color:var(--warn)}50%{border-color:#fbbf24}}

.card-title{font-family:var(--font-display);font-size:1rem;font-weight:700;margin-bottom:.35rem;}
.card-desc{color:var(--muted);font-size:.72rem;line-height:1.5;margin-bottom:1rem;}
.run-btn{
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:6px;color:#fff;font-family:var(--font-mono);
  font-size:.75rem;font-weight:600;padding:.5rem 1rem;cursor:pointer;
  transition:opacity .15s;letter-spacing:.05em;
}
.run-btn:hover{opacity:.85;}
.run-btn:disabled{opacity:.4;cursor:not-allowed;}
.status-badge{
  display:inline-block;font-size:.65rem;padding:.2rem .5rem;border-radius:4px;
  letter-spacing:.08em;font-weight:600;text-transform:uppercase;margin-top:.5rem;
}
.badge-idle{background:#1e2d45;color:var(--muted);}
.badge-running{background:#451a03;color:var(--warn);}
.badge-success{background:#052e16;color:var(--success);}
.badge-failed{background:#450a0a;color:var(--fail);}

/* results area */
.results-header{
  font-family:var(--font-display);font-size:1rem;font-weight:700;
  margin-bottom:1rem;display:flex;align-items:center;gap:.5rem;
}
.dot{width:8px;height:8px;border-radius:50%;background:var(--accent);}

.run-card{
  background:var(--surface);border:1px solid var(--border);border-radius:10px;
  margin-bottom:1rem;overflow:hidden;
}
.run-header{
  display:flex;align-items:center;justify-content:space-between;
  padding:1rem 1.25rem;border-bottom:1px solid var(--border);
  cursor:pointer;
}
.run-id{font-size:.8rem;color:var(--accent);font-weight:600;}
.run-meta{font-size:.72rem;color:var(--muted);}
.run-status{font-size:.72rem;font-weight:600;}
.run-status.SUCCESS{color:var(--success);}
.run-status.FAILED{color:var(--fail);}

.run-body{padding:1.25rem;}
.metrics-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:.75rem;margin-bottom:1rem;}
.metric{background:var(--surface2);border-radius:8px;padding:.75rem;text-align:center;}
.metric-val{font-family:var(--font-display);font-size:1.4rem;font-weight:800;line-height:1;}
.metric-label{font-size:.65rem;color:var(--muted);margin-top:.3rem;text-transform:uppercase;letter-spacing:.08em;}
.val-success{color:var(--success);}
.val-fail{color:var(--fail);}
.val-accent{color:var(--accent);}

.tasks-title{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:.5rem;}
.task-row{
  display:flex;align-items:center;gap:.75rem;
  padding:.5rem .75rem;border-radius:6px;background:var(--surface2);margin-bottom:.3rem;
  font-size:.75rem;
}
.task-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;}
.dot-SUCCESS{background:var(--success);}
.dot-FAILED{background:var(--fail);}
.dot-PENDING{background:var(--muted);}
.task-name{flex:1;font-weight:600;}
.task-dur{color:var(--muted);font-size:.68rem;}
.task-attempts{color:var(--warn);font-size:.68rem;}

.empty-state{
  text-align:center;padding:3rem;color:var(--muted);
  border:1px dashed var(--border);border-radius:10px;font-size:.8rem;line-height:1.8;
}

.spinner{
  display:inline-block;width:12px;height:12px;border:2px solid var(--border);
  border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite;
}
@keyframes spin{to{transform:rotate(360deg)}}

.chevron{transition:transform .2s;font-size:.7rem;color:var(--muted);}
.chevron.open{transform:rotate(180deg);}
.run-body{display:none;}
.run-body.open{display:block;}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo">ETL Orchestration Engine</div>
    <div class="tagline">DAG-based pipeline runner · Parallel execution · Retry logic · Live observability</div>
  </header>

  <div class="pipelines">
    <div class="pipeline-card" id="card-employee">
      <div class="card-title">Employee Analytics Pipeline</div>
      <div class="card-desc">CSV → Clean nulls → Normalize salary → Aggregate by dept → Load warehouse + JSON + Audit</div>
      <button class="run-btn" onclick="runPipeline('employee')">▶ Run Pipeline</button>
      <br/><span class="status-badge badge-idle" id="badge-employee">IDLE</span>
    </div>
    <div class="pipeline-card" id="card-retry">
      <div class="card-title">Retry Demo Pipeline</div>
      <div class="card-desc">API Extract → Flaky transform (fails 2x, recovers on 3rd) → Load — shows retry backoff in action</div>
      <button class="run-btn" onclick="runPipeline('retry')">▶ Run Pipeline</button>
      <br/><span class="status-badge badge-idle" id="badge-retry">IDLE</span>
    </div>
  </div>

  <div class="results-header"><div class="dot"></div> Pipeline Runs</div>
  <div id="results">
    <div class="empty-state">No runs yet.<br/>Click "Run Pipeline" above to execute a DAG and see live results here.</div>
  </div>
</div>

<script>
let runs = [];

async function runPipeline(type) {
  const btn = document.querySelector(`#card-${type} .run-btn`);
  const badge = document.getElementById(`badge-${type}`);
  const card = document.getElementById(`card-${type}`);

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Running...';
  badge.className = 'status-badge badge-running';
  badge.textContent = 'RUNNING';
  card.classList.add('running');

  try {
    const res = await fetch(`/api/run/${type}`, {method:'POST'});
    const data = await res.json();

    runs.unshift(data);
    renderResults();

    const s = data.summary.status;
    badge.className = `status-badge badge-${s === 'SUCCESS' ? 'success' : 'failed'}`;
    badge.textContent = s;
  } catch(e) {
    badge.className = 'status-badge badge-failed';
    badge.textContent = 'ERROR';
  }

  card.classList.remove('running');
  btn.disabled = false;
  btn.innerHTML = '▶ Run Pipeline';
}

function renderResults() {
  const el = document.getElementById('results');
  if (!runs.length) {
    el.innerHTML = '<div class="empty-state">No runs yet.</div>';
    return;
  }
  el.innerHTML = runs.map((r, i) => {
    const s = r.summary;
    const ts = new Date(r.timestamp).toLocaleTimeString();
    const tasks = r.tasks || [];
    return `
    <div class="run-card">
      <div class="run-header" onclick="toggleRun(${i})">
        <div>
          <div class="run-id">${s.dag_id} · run ${s.run_id}</div>
          <div class="run-meta">${ts} · ${s.duration_ms}ms</div>
        </div>
        <div style="display:flex;align-items:center;gap:.75rem">
          <span class="run-status ${s.status}">${s.status}</span>
          <span class="chevron" id="chev-${i}">▼</span>
        </div>
      </div>
      <div class="run-body" id="body-${i}">
        <div class="metrics-grid">
          <div class="metric"><div class="metric-val val-accent">${s.total_tasks}</div><div class="metric-label">Total tasks</div></div>
          <div class="metric"><div class="metric-val val-success">${s.succeeded}</div><div class="metric-label">Succeeded</div></div>
          <div class="metric"><div class="metric-val val-fail">${s.failed}</div><div class="metric-label">Failed</div></div>
          <div class="metric"><div class="metric-val val-accent">${s.duration_ms}ms</div><div class="metric-label">Duration</div></div>
        </div>
        <div class="tasks-title">Task breakdown</div>
        ${tasks.map(t => `
          <div class="task-row">
            <div class="task-dot dot-${t.status}"></div>
            <div class="task-name">${t.task_id}</div>
            <div class="task-dur">${t.duration_ms ?? '—'}ms</div>
            <div class="task-attempts">attempt ${t.attempt}</div>
            <div class="run-status ${t.status}" style="font-size:.65rem">${t.status}</div>
          </div>`).join('')}
      </div>
    </div>`;
  }).join('');
}

function toggleRun(i) {
  const body = document.getElementById(`body-${i}`);
  const chev = document.getElementById(`chev-${i}`);
  body.classList.toggle('open');
  chev.classList.toggle('open');
}
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/run/<pipeline_type>", methods=["POST"])
def run_pipeline(pipeline_type):
    from datetime import datetime
    if pipeline_type == "employee":
        dag = build_employee_dag()
    elif pipeline_type == "retry":
        from tasks.etl_tasks import _attempt_counter
        _attempt_counter.clear()
        dag = build_retry_dag()
    else:
        return jsonify({"error": "unknown pipeline"}), 400

    context = {"source": "employees.csv", 
           "target_table": "analytics.fact_employees",
           "output_path": "C:/Users/ramya/Desktop/projects/etl-engine/output.json",
           "api_endpoint": "https://api.example.com"}
    summary = dag.run(context=context)
    tasks = [t.to_dict() for t in dag.tasks.values()]

    return jsonify({
        "summary": summary,
        "tasks": tasks,
        "timestamp": datetime.utcnow().isoformat()
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n🚀 ETL Dashboard running at http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)