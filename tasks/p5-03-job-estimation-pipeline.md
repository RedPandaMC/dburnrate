# Task: Job Estimation Pipeline

---

## Metadata

```yaml
id: p5-03-job-estimation-pipeline
status: todo
phase: 5
priority: high
agent: ~
blocked_by: [p5-01-job-definition-cost, p5-02-historical-baselines]
created_by: planner
```

---

## Context

### Goal

Orchestrate the fast static configuration projection (Task p5-01) with the richer historical `lakeflow` projection (Task p5-02) into a unified job-level estimate. The tool should operate at two levels simultaneously: a fast, static cost estimate from job configuration and a richer historical projection using system tables. Both feed into the same pre-orchestration decision.

### Files to read

```
# Required
src/dburnrate/estimators/job_pipeline.py  # NEW
src/dburnrate/estimators/static_job.py    # Created in p5-01
src/dburnrate/tables/attribution.py       # Created in p5-02
```

---

## Acceptance Criteria

- [ ] Create `src/dburnrate/estimators/job_pipeline.py`
- [ ] Implement `JobEstimationPipeline.estimate_job(job_definition)` which:
    - [ ] Computes static cost bounds using `static_job.py`
    - [ ] Extracts `job_id` (if available) and queries historical baselines via `attribution.py`
    - [ ] Blends the estimates, preferring historical if confidence is high, and bounds check against static limits
- [ ] Fall back gracefully if offline (return only static)
- [ ] Output the projection (mean, P50, P90, static bounds)
- [ ] Add unit tests

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
```
