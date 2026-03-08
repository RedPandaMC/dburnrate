# Task: Historical Cost Baselines by Job Fingerprint

---

## Metadata

```yaml
id: p4-06-historical-baselines
status: todo
phase: 4
priority: high
agent: ~
blocked_by: [p4a-01-critical-bug-fixes, p4-03-fingerprint-lookup]
created_by: planner
```

---

## Context

### Goal

Query `system.billing.usage` joined with `system.lakeflow.job_run_timeline` to build per-job cost distributions — mean, P50, P90, P95. Use the new December 2025 duration columns (`execution_duration_seconds`, `setup_duration_seconds`, `queue_duration_seconds`, `cleanup_duration_seconds`) for granular cost decomposition.

### Files to read

```
# Required
src/dburnrate/tables/billing.py
src/dburnrate/tables/attribution.py
src/dburnrate/core/pricing.py

# Reference
files/09-REDESIGN.md           # §"Historical cost baselines by job fingerprint"
files/02-ARCHITECTURE-GAPS.md  # Attribution SQL patterns
DESIGN.md                      # §"Lakeflow Job Cost Attribution"
```

### Background

Per-job cost attribution SQL:
```sql
SELECT t1.usage_metadata.job_id, t1.usage_metadata.job_run_id,
       SUM(t1.usage_quantity * lp.pricing.default) as list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices lp
  ON t1.sku_name = lp.sku_name
  AND t1.usage_start_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR t1.usage_end_time < lp.price_end_time)
WHERE t1.billing_origin_product = 'JOBS'
GROUP BY ALL
```

Key insight from R7 (files/05-RESEARCH-BACKLOG.md): `system.query.history` covers SQL Warehouses + serverless only. All-purpose clusters and classic Jobs are NOT captured. Use `system.billing.usage` instead.

January 2026 improvement: timeline tables now use clock-hour-aligned slicing, enabling precise temporal joins without date manipulation.

---

## Acceptance Criteria

- [ ] Query `system.billing.usage` for job DBU consumption
- [ ] Join with `system.billing.list_prices` for dollar costs
- [ ] Join with `system.lakeflow.job_run_timeline` for duration breakdown
- [ ] Compute per-job stats: mean, P50, P90, P95
- [ ] Support DLT pipelines via `dlt_pipeline_id`
- [ ] CLI: `dburnrate baselines --job-id 12345` shows cost distribution
- [ ] CLI: `dburnrate baselines --days 30` shows top N jobs by cost
- [ ] All public functions have type hints and docstrings

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Requires Databricks connection
uv run dburnrate baselines --days 30 --workspace-url $DATABRICKS_HOST --token $DATABRICKS_TOKEN
```

### Expected output

- Table: job_id, job_name, runs, mean_cost, p50_cost, p90_cost, p95_cost
- Duration breakdown: execution, setup, queue, cleanup

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
