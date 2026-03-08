# Task: Cost Regression Detection

---

## Metadata

```yaml
id: p4-07-cost-regression
status: todo
phase: 4
priority: high
agent: ~
blocked_by: [p4-06-historical-baselines]
created_by: planner
```

---

## Context

### Goal

Detect when a job's cost increases beyond a configurable threshold between deployments or over time. Flag when cost increases >2× week-over-week (alert threshold) or >20% growth (investigate threshold). This addresses the industry's biggest blind spot: jobs that never fail but silently become expensive.

### Files to read

```
# Required
src/dburnrate/tables/billing.py
src/dburnrate/tables/attribution.py
src/dburnrate/cli/main.py

# Reference
files/09-REDESIGN.md           # §"Cost regression detection"
files/04-FEATURE-ROADMAP.md    # F7 (Cost Regression Detection)
DESIGN.md                      # §"Workload drift"
```

### Background

Cost regression SQL (built-in Databricks pattern):
```sql
SELECT job_id,
  SUM(CASE WHEN usage_date >= CURRENT_DATE() - 7 THEN list_cost END) as last_7d,
  SUM(CASE WHEN usage_date < CURRENT_DATE() - 7 THEN list_cost END) as prev_7d,
  ((last_7d - prev_7d) / prev_7d * 100) AS growth_pct
FROM job_cost_view
GROUP BY job_id
HAVING growth_pct > 20
```

Industry benchmarks: well-managed workloads target 12-20% variance. "Workload drift" is pervasive — jobs that never fail but progressively consume more DBU as data volume grows and autoscaling kicks in more frequently.

---

## Acceptance Criteria

- [ ] Query cost data week-over-week
- [ ] Calculate growth percentage
- [ ] Alert at 2× threshold (configurable)
- [ ] Investigate at 20% growth (configurable)
- [ ] Detect drift signals: rising DBU, increasing autoscaling, growing duration
- [ ] CLI: `dburnrate regression --days 30` shows cost regressions
- [ ] CLI: `dburnrate regression --job-id 12345 --days 90` shows job history
- [ ] All public functions have type hints and docstrings

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Requires Databricks connection
uv run dburnrate regression --days 30 --workspace-url $DATABRICKS_HOST --token $DATABRICKS_TOKEN
```

### Expected output

- Table: job_id, job_name, last_7d_cost, prev_7d_cost, growth_pct, severity
- Severity: ALERT (>100% growth), WARNING (>20% growth), OK

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
