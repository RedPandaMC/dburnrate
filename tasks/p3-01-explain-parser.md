# Task: Implement EXPLAIN COST output parser

---

## Metadata

```yaml
id: p3-01-explain-parser
status: todo
phase: 3
priority: high
agent: ~        # Claude/Sonnet — regex + AST parsing, structured output
blocked_by: [p3-00-research-explain-cost]
created_by: planner
```

---

## Context

### Goal

Implement `src/dburnrate/parsers/explain.py` that parses the text output of Databricks `EXPLAIN COST` and `EXPLAIN EXTENDED` into structured data. The parser extracts `sizeInBytes`, `rowCount`, join types, shuffle counts, and plan depth. Returns an `ExplainPlan` model that the hybrid estimator (p3-03) will consume.

### Files to read

```
# Required
docs/explain-cost-schema.md       (from p3-00 — read this first)
src/dburnrate/core/models.py
src/dburnrate/core/exceptions.py
src/dburnrate/parsers/sql.py      (for style reference)

# Reference
tests/unit/parsers/test_sql.py    (for test style reference)
```

### Background

See `docs/explain-cost-schema.md` for full parsing spec. Key implementation notes:

- Input: raw string output from `EXPLAIN COST` or `EXPLAIN EXTENDED`
- Use `re` module for pattern matching — no external deps
- `Statistics(sizeInBytes=X, rowCount=Y)` — Y may be missing if stats not analyzed
- Size units: `B`, `KiB`, `MiB`, `GiB`, `TiB` — normalize all to bytes (int)
- Confidence: `high` if all operators have rowCount, `medium` if partial, `low` if none
- Return `ExplainPlan` Pydantic model (define in `models.py`)

`ExplainPlan` should contain:
- `total_size_bytes: int` — max sizeInBytes across all operators
- `estimated_rows: int | None` — root operator rowCount
- `join_types: list[str]` — e.g. `["BroadcastHashJoin", "SortMergeJoin"]`
- `shuffle_count: int` — number of Exchange/Sort operators
- `plan_depth: int` — number of lines with `+-` prefix
- `stats_complete: bool` — True if all operators have rowCount
- `raw_plan: str` — original text

---

## Acceptance Criteria

- [ ] `src/dburnrate/parsers/explain.py` exists
- [ ] `ExplainPlan` Pydantic model added to `src/dburnrate/core/models.py`
- [ ] `parse_explain(text: str) -> ExplainPlan` implemented
- [ ] `parse_explain` raises `ParseError` on empty/malformed input
- [ ] Size unit normalization: B/KiB/MiB/GiB/TiB → bytes correctly
- [ ] Join type detection covers: BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin
- [ ] `stats_complete` correctly False when any operator lacks rowCount
- [ ] Unit tests in `tests/unit/parsers/test_explain.py` — at least 8 test cases
- [ ] Tests use fixture strings (no network calls)
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/parsers/test_explain.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

### Expected output

```
All explain tests pass
N passed in ...  (N >= 122)
All checks passed.
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
