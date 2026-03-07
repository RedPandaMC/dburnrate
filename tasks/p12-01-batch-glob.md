# Task: Implement batch file analysis with glob patterns

---

## Metadata

```yaml
id: p12-01-batch-glob-analysis
status: todo
phase: 12
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Implement the `dburnrate.estimate_batch()` API and CLI command that allows analyzing multiple files at once using glob patterns (e.g., `queries/*.sql`), directory recursion, or explicit file lists. This is essential for CI/CD integration where teams need to estimate costs for entire codebases.

### Files to read (executor reads ONLY these)

```
# Required
src/dburnrate/cli/main.py
src/dburnrate/core/models.py
src/dburnrate/parsers/sql.py
src/dburnrate/estimators/static.py

# Reference
DESIGN.md (Phase 12 section)
docs/cli-workflows.md
AGENTS.md
```

### Background

From DESIGN.md Phase 12:
- Support glob patterns: `"queries/*.sql"`, `"notebooks/**/*.ipynb"`
- Support explicit file lists: `['query1.sql', 'query2.py']`
- Support directory recursion
- Output formats: summary table, individual results, CSV, JSON
- Parallel processing for large batches
- Progress bar with `rich`
- Aggregate statistics (total, average, min/max)
- Sort and filter by cost/confidence

CLI examples:
```bash
dburnrate estimate-batch "queries/*.sql"
dburnrate estimate-batch ./queries/ --format csv --output costs.csv
dburnrate estimate-batch queries/ --parallel 8 --sort-by cost
```

---

## Acceptance Criteria

- [ ] Implement `estimate_batch()` function supporting glob patterns
- [ ] Implement CLI command `dburnrate estimate-batch`
- [ ] Support directory recursion (default on)
- [ ] Support explicit file list input
- [ ] CSV export option
- [ ] JSON export option
- [ ] Summary-only mode
- [ ] Parallel processing support (--jobs/-j flag)
- [ ] Progress bar for large batches
- [ ] Sort by cost (ascending/descending)
- [ ] Filter by confidence level
- [ ] Aggregate statistics (total, average, min, max, count)
- [ ] Skip unsupported file types gracefully
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes with no failures
- [ ] `uv run ruff check src/ tests/` produces zero errors

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v -k "batch"
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/
```

### Expected output

- Batch estimation tests should pass
- Test coverage should include:
  - Glob pattern matching
  - Directory recursion
  - Export formats (CSV, JSON)
  - Parallel processing
  - Filtering and sorting
- All lint checks pass

---

## Handoff

### Result

[Executor fills this in when done]

```
status: todo
```

### Blocked reason

[If blocked, explain what is missing]
