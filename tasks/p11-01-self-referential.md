# Task: Implement self-referential cost estimation API

---

## Metadata

```yaml
id: p11-01-self-referential-estimation
status: todo
phase: 11
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Implement the `dburnrate.estimate_self()` API that allows users to add a line at the bottom of any Python file or notebook to estimate the cost of running that file. This feature reads the current file using `__file__` or `inspect`, parses all code above the import statement, and returns a CostEstimate. This is a unique and powerful feature that educates users about the cost of their code as they write it.

### Files to read (executor reads ONLY these)

```
# Required
src/dburnrate/__init__.py
src/dburnrate/core/models.py
src/dburnrate/parsers/pyspark.py
src/dburnrate/estimators/static.py

# Reference
DESIGN.md (Phase 11 section)
AGENTS.md
```

### Background

From DESIGN.md Phase 11:
- Users should be able to add `import dburnrate; dburnrate.estimate_self()` at the bottom of any file
- The function should detect if running in a notebook vs script
- Must exclude the import statement itself from analysis
- Should cache results to avoid re-parsing
- CLI command `dburnrate estimate-self` should also be available

The API should work like this:
```python
# At the bottom of any Python file/notebook
import dburnrate
estimate = dburnrate.estimate_self()
print(f"This file would cost ${estimate.cost_usd:.4f} to run")
```

---

## Acceptance Criteria

- [ ] Implement `estimate_self()` function in `src/dburnrate/__init__.py` or new module
- [ ] Function detects current file path using `__file__` or `inspect.stack()`
- [ ] Function reads and parses all code above the call site
- [ ] Excludes the `estimate_self()` import/call from analysis
- [ ] Handles both regular Python scripts and Jupyter notebooks
- [ ] Returns a proper `CostEstimate` object
- [ ] Implements caching to avoid re-parsing
- [ ] CLI command `dburnrate estimate-self` implemented
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes with no failures
- [ ] `uv run ruff check src/ tests/` produces zero errors

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v -k "self"
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/
```

### Expected output

- Tests for `estimate_self()` should pass
- Test coverage should include:
  - Regular Python file estimation
  - Notebook cell estimation (mocked)
  - Caching behavior
  - CLI command
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
