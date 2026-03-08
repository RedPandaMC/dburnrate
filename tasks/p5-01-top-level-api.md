# Task: Top-Level Programmatic API

---

## Metadata

```yaml
id: p5-01-top-level-api
status: todo
phase: 5
priority: critical
agent: ~
blocked_by: [p4a-03-table-registry-runtime-backend]
created_by: planner
```

---

## Context

### Goal

Make `dburnrate` a first-class programmatic Python library, not just a CLI tool. Data engineers should be able to `import dburnrate` inside their Databricks notebooks to run cost-aware linting, get interactive advice, and estimate costs.

### Files to read

```
# Required
src/dburnrate/__init__.py
src/dburnrate/estimators/pipeline.py
src/dburnrate/parsers/antipatterns.py
```

### Background

Currently `__init__.py` is virtually empty. We need to expose the core functions of the package so developers can use them seamlessly without wrestling with internal class instantiation.

---

## Acceptance Criteria

- [ ] Expose `dburnrate.lint(sql_or_file_path: str)` -> returns list of AntiPattern objects
- [ ] Expose `dburnrate.estimate(sql_or_file_path: str, cluster=None, registry=None)` -> returns CostEstimate
- [ ] Expose `dburnrate.advise(run_id: str)` -> returns AdvisoryReport (stub this out for the next task)
- [ ] Ensure `__all__` is properly defined in `__init__.py`
- [ ] Add unit tests verifying these top-level imports and functions work

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Create a small test script `test_api.py` that runs `import dburnrate; print(dburnrate.lint("SELECT * FROM a,b"))` and verify it works without throwing import errors.

---

## Handoff

### Result

```yaml
status: todo
```
