# Task: Implement Level 1 AST-Based Lint Rules

---

## Metadata

```yaml
id: s2-04-ast-lint-rules
status: todo
phase: 2
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Implement comprehensive AST-based lint rules for PySpark, SQL, and Spark Declarative Pipelines (SDP) based on the research document at `tasks/research/lint_research.md`. These rules catch the most frequent performance killers like UDFs, collect() calls, and inefficient data reading patterns—without requiring a live Spark session.

### Files to read

```
# Required
src/burnt/parsers/antipatterns.py    # Current implementation
src/burnt/parsers/pyspark.py         # Python AST parsing
src/burnt/parsers/sql.py             # SQL parsing
tests/unit/parsers/test_antipatterns.py

# Reference
tasks/research/lint_research.md      # Research document
DESIGN.md                            # Architecture context
```

### Background

The research document identifies three levels of lint rules. This task focuses on **Level 1: Syntax and Structure Rules (AST-Based)** which can be verified by parsing Python or SQL code into an Abstract Syntax Tree without requiring a live Spark session.

**Rules to implement:**

| Rule | Severity | Description |
|------|----------|-------------|
| `select_star` | ERROR | Flag SELECT * in SQL or .select("*") in PySpark |
| `python_udf` | ERROR | Warn on use of udf() - check for equivalent native functions |
| `pandas_udf` | WARNING | Use of pandas_udf instead of native functions preferred |
| `collect_without_limit` | ERROR | collect() without limit() can OOM the driver |
| `toPandas` | ERROR | toPandas() brings all data to driver |
| `count_without_filter` | WARNING | count() triggers full job - use estimated row count |
| `withColumn_in_loop` | WARNING | .withColumn() in loop creates deep plan - use withColumns() |
| `repartition_one` | WARNING | repartition(1) causes single partition bottleneck |
| `jdbc_incomplete_partition` | ERROR | JDBC read missing partitionColumn/numPartitions/lowerBound/upperBound |
| `sdp_prohibited_ops` | ERROR | collect(), count(), toPandas() in @dp.table/@dp.materialized_view |
| `cross_join` | WARNING | CROSS JOIN without explicit ON - check for cartesian product |

---

## Acceptance Criteria

- [ ] All 11 lint rules implemented with proper AST-based detection
- [ ] Rules work for both SQL and PySpark code
- [ ] No false positives on valid code patterns
- [ ] All existing tests pass
- [ ] New unit tests added for each rule
- [ ] Lint passes on the implementation itself

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Run `burnt lint` on sample PySpark/SQL files and confirm rules are detected

---

## Handoff

### Result

```yaml
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
