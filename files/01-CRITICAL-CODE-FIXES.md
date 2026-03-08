# 01 — Critical Code Fixes

> Mathematical errors, fabricated constants, and code quality issues that must be fixed before any estimate can be trusted.

---

## 1.1 Static Estimator: Quadratic Formula (estimators/static.py)

**Current formula:**
```python
cluster_factor = cluster.num_workers * cluster.dbu_per_hour
time_estimate = complexity / 100
estimated_dbu = complexity * cluster_factor * time_estimate
# → complexity² × num_workers × dbu_per_hour / 100
```

**Impact:** A `SELECT ... GROUP BY` (complexity 8) on 2-worker DS3_v2 estimates 0.96 DBU. Actual: ~0.001 DBU. **960× overestimate.**

The quadratic relationship has no empirical or theoretical basis. Doubling operations doesn't quadruple cost.

**Fix:** Replace with linear throughput-based model:
```python
estimated_seconds = (scan_bytes / throughput_bytes_per_sec) + (shuffle_count * shuffle_overhead_sec)
estimated_dbu = (estimated_seconds / 3600) * cluster_dbu_per_hour
```

Even as a heuristic fallback: `complexity × cluster_factor × CONSTANT` (linear), not `complexity² / 100`.

---

## 1.2 Hybrid Estimator: Phantom Price (estimators/hybrid.py)

```python
_NOMINAL_USD_PER_DBU: float = 0.20
```

No Azure Databricks SKU costs $0.20/DBU. Real rates: Jobs $0.30, ALL_PURPOSE $0.55, SQL_SERVERLESS $0.70. Systematic 1.5–3.5× underpricing.

**Fix:** Remove hardcoded constant. Use `get_dbu_rate(sku)` from `core/pricing.py`. The hybrid estimator should return DBU estimates; the pricing layer converts to dollars.

---

## 1.3 EXPLAIN DBU Calculation: Ungrounded Constants (estimators/hybrid.py)

```python
_SCAN_DBU_PER_GB: float = 0.5
_SHUFFLE_DBU_EACH: float = 0.2
_JOIN_DBU_WEIGHTS = {"BroadcastHashJoin": 0.1, "SortMergeJoin": 0.5, ...}
```

**`_SCAN_DBU_PER_GB = 0.5` is ~7,900× too high.** A 4-worker DS3_v2 scans Parquet at ~3.2 GB/s throughput. 1 GB takes ~0.3 seconds = 0.000063 DBU. The constant should be derived from empirical throughput benchmarks (see 05-RESEARCH-BACKLOG.md, R1).

---

## 1.4 Historical Estimation Ignores Data Volume Scaling (estimators/hybrid.py)

```python
p50_ms = median(durations)
dbu = (p50_ms / 3_600_000) * cluster.dbu_per_hour
```

If the table grew 10× since historical runs, this estimate is 10× too low. DESIGN.md specifies "adjust by input data size ratio" but the code doesn't implement it.

**Fix:**
```python
scale_factor = current_table_size / median_historical_read_bytes
adjusted_ms = p50_ms * scale_factor
```

---

## 1.5 SQL Injection in System Table Queries

**Files:** `tables/billing.py`, `tables/queries.py`, `tables/compute.py`

```python
sql = f"""SELECT ... FROM system.compute.clusters WHERE cluster_id = '{cluster_id}'"""
```

Every system table query uses f-string interpolation. While targeting Databricks' own tables, a malicious `cluster_id` or `warehouse_id` could inject SQL. Use parameterized queries or sanitize inputs.

---

## 1.6 SKU Inference Is Fragile (estimators/static.py)

```python
def _infer_sku(self, cluster: ClusterConfig) -> str:
    if "Standard_D" in cluster.instance_type:
        return "ALL_PURPOSE"
    return "JOBS_COMPUTE"
```

Misclassifies SQL Warehouses, serverless compute, DLT pipelines, any AWS/GCP instance types. SKU should be an explicit parameter.

---

## 1.7 Anti-Pattern Detector Uses String Matching (parsers/antipatterns.py)

```python
if "CROSS JOIN" in sql.upper():
```

Matches inside string literals, comments, and CTEs. Should use sqlglot AST traversal (already implemented in `sql.py`'s `detect_operations`).

---

## 1.8 protocols.py Has Shadowed Classes

`CostEstimate` and `ParseResult` placeholder classes in `protocols.py` shadow the real `CostEstimate` in `models.py`. The `Estimator` protocol references the wrong one. Will cause type confusion.

---

## 1.9 The forecast/prophet.py Is a Stub

The file exists but is essentially empty. Either remove it or mark it clearly as a placeholder. Its presence implies functionality that doesn't exist.

---

## 1.10 No Graceful Degradation in CLI

If sqlglot isn't installed, `dburnrate estimate "SELECT ..."` crashes with ImportError instead of falling back or suggesting `uv sync --extra sql`.
