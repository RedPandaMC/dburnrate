# Task: Budget Integration and Enforcement

---

## Metadata

```yaml
id: p4-08-budget-integration
status: todo
phase: 4
priority: medium
agent: ~
blocked_by: [p4a-01-critical-bug-fixes]
created_by: planner
```

---

## Context

### Goal

Integrate with Databricks Budgets API to check projected costs against team/project budgets before job submission. Surface budget utilization percentage alongside cost projections. Enable CI/CD cost gates.

### Files to read

```
# Required
src/dburnrate/tables/connection.py
src/dburnrate/core/config.py
src/dburnrate/cli/main.py

# Reference
files/09-REDESIGN.md           # §"Budget integration"
files/04-FEATURE-ROADMAP.md    # F6 (Hidden Cost Audit)
DESIGN.md                      # §"Budget policies and cost controls"
```

### Background

**Budgets API**: `/api/2.0/accounts/{account_id}/budgets` supports CRUD for monthly spending thresholds filtered by workspace and custom tags. **Important**: budgets do NOT stop usage — they trigger email notifications with up to 24-hour delay.

**Serverless budget policies**: Account-level objects that tag serverless workloads with custom key:value pairs, propagating to `system.billing.usage.custom_tags`. Tags enable cost attribution by team/project/environment.

**Budget check flow**:
1. Parse job definition or get historical baseline
2. Project estimated cost
3. Query Budgets API for applicable budgets
4. Calculate utilization: (projected_cost / budget_limit) × 100
5. Warn if >80%, block if >100% (configurable)

---

## Acceptance Criteria

- [ ] Query Budgets API for workspace budgets
- [ ] Match budgets by custom tags (team, project, environment)
- [ ] Calculate projected cost vs budget limit
- [ ] Surface budget utilization percentage
- [ ] Warn threshold: 80% (configurable)
- [ ] Block threshold: 100% (configurable)
- [ ] CLI: `dburnrate budgets --workspace-url $HOST --account-id $ACC` lists budgets
- [ ] CLI: `dburnrate check-budget ./databricks.yml` projects and checks against budgets
- [ ] All public functions have type hints and docstrings

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Test budget check (requires connection)
uv run dburnrate check-budget ./tests/fixtures/databricks.yml \
  --workspace-url $DATABRICKS_HOST \
  --account-id $DATABRICKS_ACCOUNT_ID \
  --token $DATABRICKS_TOKEN
```

### Expected output

- Table: budget_name, limit, current_spend, projected_cost, utilization_pct, status
- Status: OK, WARNING, EXCEEDED

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
