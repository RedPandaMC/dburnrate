# 06 — Testing Strategy

> Current gaps, accuracy benchmarks, property-based testing, and integration infrastructure.

---

## 6.1 Current State: Wide But Shallow

263 tests verify that functions return correct types, handle edge cases, and don't crash. **Zero tests verify that estimates are accurate** or even in the right order of magnitude.

---

## 6.2 Missing Test Categories

### Accuracy Sanity Checks
```python
def test_simple_select_cheaper_than_five_way_join():
    simple = estimator.estimate("SELECT id FROM t")
    complex = estimator.estimate("""
        SELECT a.id FROM t1 a
        JOIN t2 b ON a.id = b.id
        JOIN t3 c ON b.id = c.id
        JOIN t4 d ON c.id = d.id
        JOIN t5 e ON d.id = e.id
        GROUP BY a.id
    """)
    assert complex.estimated_dbu > simple.estimated_dbu

def test_monotonicity_adding_join_increases_cost():
    base = estimator.estimate("SELECT * FROM orders")
    joined = estimator.estimate("SELECT * FROM orders JOIN customers ON orders.cid = customers.id")
    assert joined.estimated_dbu >= base.estimated_dbu
```

### Calibration Smoke Tests
```python
def test_known_explain_output_produces_reasonable_estimate():
    """Given real EXPLAIN output, estimate should be within 10× of actual DBU."""
    explain_text = FIXTURES["tpcds_q3_explain"]  # Real EXPLAIN output
    actual_dbu = FIXTURES["tpcds_q3_actual_dbu"]  # Known from billing
    
    plan = parse_explain_cost(explain_text)
    estimate = hybrid._explain_dbu(plan, STANDARD_CLUSTER)
    
    assert actual_dbu / 10 <= estimate <= actual_dbu * 10
```

### Regression Tests
```python
@pytest.mark.parametrize("query,expected_range", BENCHMARK_QUERIES)
def test_benchmark_query_in_range(query, expected_range):
    result = estimator.estimate(query)
    low, high = expected_range
    assert low <= result.estimated_dbu <= high, (
        f"Estimate {result.estimated_dbu} outside [{low}, {high}]"
    )
```

---

## 6.3 Benchmark Dataset (Required)

```
tests/benchmarks/
├── README.md                # How to add new benchmarks
├── queries/                 # Reference SQL queries
│   ├── tpcds_q3.sql
│   ├── tpcds_q19.sql
│   └── simple_groupby.sql
├── explain_outputs/         # Known EXPLAIN COST outputs
│   ├── tpcds_q3.txt
│   └── simple_groupby.txt
├── expected_costs.json      # Known actual costs from billing
└── conftest.py              # Benchmark fixtures
```

**Accuracy targets by phase:**
- Phase 4: All estimates within **10×** of actual
- Phase 5: Within **3×** of actual
- Phase 6 (ML): Within **2×** of actual

---

## 6.4 Property-Based Testing (Hypothesis)

Hypothesis is already in dev dependencies but unused. Ideal candidates:

```python
from hypothesis import given, strategies as st

@given(sql=st.from_regex(r"SELECT \w+ FROM \w+( WHERE \w+ = \d+)?", fullmatch=True))
def test_any_valid_select_parses_without_error(sql):
    """Any structurally valid SELECT should not crash the parser."""
    try:
        analyze_query(sql)
    except ParseError:
        pass  # Invalid SQL is fine; crashes are not

@given(sql=st.text(min_size=1))
def test_normalize_idempotent(sql):
    """Normalizing any SQL twice produces the same result."""
    once = normalize_sql(sql)
    twice = normalize_sql(once)
    assert once == twice

@given(sql=st.text(min_size=10))
def test_whitespace_doesnt_change_fingerprint(sql):
    """Adding whitespace to SQL should not change its fingerprint."""
    compact = normalize_sql(sql)
    spaced = normalize_sql(f"  {sql}  ")
    assert compact == spaced

@given(size_bytes=st.integers(min_value=0, max_value=10**15))
def test_larger_scan_produces_higher_estimate(size_bytes):
    """An EXPLAIN plan with higher sizeInBytes should produce higher estimate."""
    small_plan = ExplainPlan(total_size_bytes=size_bytes, ...)
    large_plan = ExplainPlan(total_size_bytes=size_bytes * 10, ...)
    assert hybrid._explain_dbu(large_plan, cluster) >= hybrid._explain_dbu(small_plan, cluster)
```

---

## 6.5 Integration Test Infrastructure

```python
# tests/integration/conftest.py
import pytest
from dburnrate.core.config import Settings
from dburnrate.tables.connection import DatabricksClient

@pytest.fixture
def databricks_client():
    """Skip if no Databricks connection configured."""
    settings = Settings()
    if not settings.workspace_url:
        pytest.skip("No Databricks connection (set DBURNRATE_WORKSPACE_URL)")
    return DatabricksClient(settings)

@pytest.fixture
def warehouse_id():
    """Skip if no warehouse ID configured."""
    wid = os.environ.get("DBURNRATE_WAREHOUSE_ID")
    if not wid:
        pytest.skip("No warehouse ID (set DBURNRATE_WAREHOUSE_ID)")
    return wid
```

Integration tests should cover:
- `EXPLAIN COST` submission and parsing round-trip
- `system.query.history` fingerprint matching
- `DESCRIBE DETAIL` on known tables
- Billing attribution join accuracy
- End-to-end estimate vs. actual comparison

---

## 6.6 Anti-Pattern Detector Needs AST-Based Tests

Current `antipatterns.py` string matching matches inside comments and literals. Test for false positives:

```python
def test_cross_join_in_comment_not_flagged():
    sql = "SELECT * FROM t -- CROSS JOIN is bad"
    patterns = detect_antipatterns(sql, "sql")
    assert not any(p.name == "cross_join" for p in patterns)

def test_cross_join_in_string_literal_not_flagged():
    sql = "SELECT 'CROSS JOIN' as label FROM t"
    patterns = detect_antipatterns(sql, "sql")
    assert not any(p.name == "cross_join" for p in patterns)
```
