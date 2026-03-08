# 07 — Agent Workflow Improvements

> Planner/executor validation gaps, task specification problems, DESIGN.md conflicts.

---

## 7.1 No Validation Loop

AGENTS.md defines planner (creates tasks) and executor (implements them). Missing: a **validator** role that:

- Runs estimates against a known benchmark dataset
- Compares predicted vs actual costs
- Flags when mathematical constants change without empirical justification
- Checks that new code integrates with the rest of the system

**Result:** Phase 3 built the hybrid estimator with completely unvalidated constants ($0.20/DBU, 0.5 DBU/GB scan), and the planner marked it "complete" based on test-pass status alone.

### Fix: Add Validation Step to Every Estimation Task

```yaml
# In task YAML
validation:
  - Run `dburnrate estimate` on 5 benchmark queries from tests/benchmarks/
  - Compare output to expected range (documented in expected_costs.json)
  - Phase 4 tolerance: all within 10× of expected
  - Phase 5 tolerance: all within 3× of expected
  - Phase 6 tolerance: all within 2× of expected
```

---

## 7.2 Task Files Don't Specify Mathematical Constraints

Phase 3 tasks say "implement hybrid estimator" but don't specify:
- What the constants should be
- How they should be derived (citation or first-principles)
- Expected output for test inputs
- Acceptable error bounds

An executor agent given `p3-03-hybrid-estimator.md` fabricated constants because the task didn't constrain them.

### Fix: Every Estimation Task Must Include

1. The formula to implement (with units)
2. How constants were derived (citation or calculation)
3. Expected output for ≥3 test inputs
4. Acceptable error bounds (e.g., "within 10× of actual")

---

## 7.3 DESIGN.md Has Duplicate Phases

| Conflict | Phase A | Phase B |
|---|---|---|
| Production Hardening | Phase 5 | Phase 9 |
| Multi-Cloud | Phase 4 (p4-04) | Phase 7 |
| CLI Enhancements | Phase 4 (p4-01) | Phase 10 |

Executor agents may reference the wrong phase. Consolidate: Phases 7-10 merge into Phase 5 as sub-tasks. Post-Phase-6 becomes a clear "v2" milestone.

---

## 7.4 No Benchmarking Asset

The single highest-leverage missing asset for the agent workflow. Without reference queries with known costs, there's no way to tell if changes improve or degrade accuracy.

### Create: `tests/benchmarks/`

```
tests/benchmarks/
├── README.md                # How to add new benchmarks
├── queries/                 # Reference SQL queries (10-20)
├── explain_outputs/         # Known EXPLAIN COST outputs
├── delta_metadata/          # Known DESCRIBE DETAIL outputs
├── expected_costs.json      # Known actual costs from billing
└── conftest.py              # Benchmark fixtures + parametrized tests
```

Every task that touches estimation logic must run benchmarks before marking complete.

---

## 7.5 Task Completion Should Require Integration Check

Current: executor runs `pytest -m unit` + `ruff check` → marks complete.

Proposed addition: executor must also verify that the new code is **reachable from the CLI**. Phase 3 built `HybridEstimator` with 100% test coverage but zero CLI integration — the user-facing surface didn't change at all.

```yaml
# Additional verification step
integration_check:
  - Run `dburnrate estimate "SELECT 1"` and verify output includes new signal
  - Run `dburnrate estimate "SELECT 1" --explain` and verify new tier appears
```

---

## 7.6 Self-Referential Estimation Should Be Dropped

Phase 11 (`dburnrate.estimate_self()`) is a demo feature with no workflow value. A developer who imports dburnrate already has the code in a file they can pass to `dburnrate estimate`. The `inspect`-based approach adds complexity for marginal benefit.

**Recommendation:** Remove Phase 11 from DESIGN.md. Reallocate to calibration work.
