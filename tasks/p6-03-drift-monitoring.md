# Task: Workload Drift Monitoring

---

## Metadata

```yaml
id: p4-09-drift-monitoring
status: todo
phase: 4
priority: medium
agent: ~
blocked_by: [p4-06-historical-baselines]
created_by: planner
```

---

## Context

### Goal

Track workload drift signals — rising DBU consumption, increasing autoscaling frequency, growing execution duration — for recurring jobs. Address the industry's biggest blind spot: jobs that never fail but silently become expensive.

### Files to read

```
# Required
src/dburnrate/tables/billing.py
src/dburnrate/tables/compute.py
src/dburnrate/tables/attribution.py
src/dburnrate/cli/main.py

# Reference
files/09-REDESIGN.md           # §"Drift monitoring"
files/04-FEATURE-ROADMAP.md    # F7 (Cost Regression Detection)
DESIGN.md                      # §"Workload drift"
```

### Background

**Drift signals to track**:
1. **DBU consumption trend**: Week-over-week, month-over-month growth
2. **Autoscaling frequency**: How often max_workers is reached
3. **Execution duration trend**: Growing runtime indicates data growth or plan regression
4. **Data volume growth**: Compare `read_bytes` from `system.query.history`
5. **Plan regression**: Join strategy changes (SortMergeJoin → BroadcastHashJoin indicates skew)

The new December 2025 columns in `system.lakeflow.job_run_timeline` enable decomposition:
- `execution_duration_seconds` — actual compute time
- `setup_duration_seconds` — cluster startup
- `queue_duration_seconds` — waiting for resources
- `cleanup_duration_seconds` — teardown

**Industry target**: Well-managed workloads target 12-20% variance. Drift detection should flag >20% month-over-month changes.

---

## Acceptance Criteria

- [ ] Track DBU consumption trends over time
- [ ] Detect execution duration growth
- [ ] Identify autoscaling frequency increases
- [ ] Compare data volume growth (read_bytes)
- [ ] Detect plan regression via EXPLAIN comparison
- [ ] CLI: `dburnrate drift --days 30` shows all drifting jobs
- [ ] CLI: `dburnrate drift --job-id 12345` shows detailed drift analysis
- [ ] Output includes trend charts (ASCII or JSON for external visualization)
- [ ] All public functions have type hints and docstrings

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Requires Databricks connection
uv run dburnrate drift --days 30 --workspace-url $DATABRICKS_HOST --token $DATABRICKS_TOKEN
```

### Expected output

- Table: job_id, job_name, dbu_trend, duration_trend, autoscaling_trend, severity
- Severity: STABLE, DEGRADING, CRITICAL
- Detailed view: week-by-week breakdown

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
