# Task: Static Cost Projection from Job Definitions

---

## Metadata

```yaml
id: p4-05-job-definition-cost
status: todo
phase: 4
priority: critical
agent: ~
blocked_by: [p4a-01-critical-bug-fixes]
created_by: planner
```

---

## Context

### Goal

Implement static cost projection from Databricks Asset Bundles (DABs) YAML or job JSON. Parse cluster configuration (instance type, worker count, autoscaling, compute type, Photon, spot) and compute cost range — minimum to maximum — without any execution history. This enables CI/CD pre-deployment gates.

### Files to read

```
# Required
src/dburnrate/cli/main.py
src/dburnrate/core/pricing.py
src/dburnrate/core/models.py

# Reference
files/09-REDESIGN.md            # §"Static cost projection from job definitions"
files/04-FEATURE-ROADMAP.md     # F1 (Total Cost of Ownership)
DESIGN.md                       # §"Qubika Cost Multiplier Model"
```

### Background

Based on the Qubika Cost Multiplier Model:
- **Total Cost = Workload Design × Compute Strategy × Feature Overhead**
- Compute type creates 3-4× cost spread (Jobs $0.15-0.30, All-Purpose $0.40-0.65, Serverless $0.70-0.95)
- Instance type adds 2-6× variance
- Photon adds 2.5-2.9× DBU rate (cost-effective only above ~500M rows)
- Autoscaling reduces 40-60% vs fixed-size

Static analysis checklist from job JSON/YAML:
- `node_type_id` → DBU rate lookup
- `num_workers` / `autoscale.max_workers` → linear cost multiplier
- Job compute vs All-Purpose → 3-4× rate multiplier
- Spot vs On-Demand → 50-90% infrastructure savings
- Photon enabled → 2.5-2.9× DBU rate
- `autotermination_minutes` → idle cost exposure

---

## Acceptance Criteria

- [ ] Parse `databricks.yml` (DABs) for job/pipeline definitions
- [ ] Parse job JSON from Jobs API (`/api/2.1/jobs/get`)
- [ ] Extract cluster config: instance type, worker count, autoscaling, compute type, Photon, spot
- [ ] Compute cost range: minimum (min_workers × spot rate) to maximum (max_workers × on-demand × Photon)
- [ ] CLI: `dburnrate estimate-job ./databricks.yml` outputs cost projection
- [ ] CLI: `dburnrate estimate-job --job-id 12345` fetches from API and projects
- [ ] Support `--cloud` flag (azure/aws/gcp)
- [ ] All public functions have type hints and docstrings

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Test with sample DABs YAML
uv run dburnrate estimate-job ./tests/fixtures/databricks.yml
```

### Expected output

- Cost projection: minimum, maximum, and breakdown by cost component
- Works offline (no API required for YAML parsing)

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
# ^ change to done/blocked when finished
```

### Blocked reason

[If blocked, explain exactly what is missing.]
