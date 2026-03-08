# 02 — Architecture Gaps

> Missing orchestration, no Databricks Runtime support, no billing attribution, incomplete pricing model.

---

## 2.1 No Estimation Pipeline Orchestrator

The `HybridEstimator`, `DatabricksClient`, fingerprinting, Delta metadata, and EXPLAIN parsing all exist but **nothing connects them**. The CLI bypasses everything except the static estimator.

**Required module:** `src/dburnrate/estimators/pipeline.py`

```python
class EstimationPipeline:
    """Orchestrates: table resolution → Delta metadata → EXPLAIN COST → fingerprint → blend."""
    
    def __init__(self, backend: RuntimeBackend | None = None, registry: TableRegistry = TableRegistry()):
        self._backend = backend or auto_backend()
        self._registry = registry
        self._hybrid = HybridEstimator()
    
    def estimate(self, query: str, cluster: ClusterConfig) -> CostEstimate:
        # 1. Parse tables from query (sqlglot)
        # 2. Fetch delta metadata per table (if connected)
        # 3. Submit EXPLAIN COST (if connected)
        # 4. Fingerprint + history lookup (if connected)
        # 5. Blend via HybridEstimator
        # 6. Fall back gracefully at each tier
        ...
```

---

## 2.2 No Databricks Runtime Support (Critical for 70% In-Cluster Use)

The package communicates with Databricks **exclusively over REST APIs from outside the cluster**. When running inside a Databricks notebook, this creates an absurd round-trip: notebook → HTTP POST to /api/2.0/sql/statements → Databricks routes to SQL Warehouse → response back — when `spark.sql()` is already available.

### Current State
- Zero awareness of `SparkSession.getActiveSession()`
- Zero detection of `DATABRICKS_RUNTIME_VERSION` env var
- Every code path assumes external REST access
- `EXPLAIN COST` requires a warehouse ID even from inside a cluster
- No structured plan access via JVM bridge (`df._jdf.queryExecution()`)
- No auto-detection of cluster config from `SparkConf`

### Required: Dual-Mode RuntimeBackend

```python
# src/dburnrate/runtime/__init__.py
class RuntimeBackend(Protocol):
    def execute_sql(self, sql: str) -> list[dict[str, Any]]: ...
    def explain_cost(self, query: str) -> str: ...
    def describe_detail(self, table_name: str) -> dict[str, Any]: ...
    @property
    def is_connected(self) -> bool: ...

# src/dburnrate/runtime/spark_backend.py  
class SparkBackend:
    """Direct SparkSession access — inside Databricks Runtime."""
    def __init__(self):
        from pyspark.sql import SparkSession
        self._spark = SparkSession.getActiveSession()
    
    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        return [row.asDict() for row in self._spark.sql(sql).collect()]
    
    def explain_cost(self, query: str) -> str:
        return self._spark.sql(f"EXPLAIN COST {query}").collect()[0][0]

# src/dburnrate/runtime/rest_backend.py
class RestBackend:
    """REST API access — from external tools."""
    def __init__(self, client: DatabricksClient, warehouse_id: str): ...

# src/dburnrate/runtime/detect.py
def auto_backend() -> RuntimeBackend | None:
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkBackend()
    settings = Settings()
    if settings.workspace_url and settings.token:
        return RestBackend(DatabricksClient(settings), settings.warehouse_id)
    return None  # Offline mode
```

### In-Cluster Capabilities REST Cannot Do

| Capability | REST Path | SparkSession Path |
|---|---|---|
| EXPLAIN COST | Statement API → warehouse → poll → parse text | `spark.sql("EXPLAIN COST ...")` directly |
| Structured plan stats | Text regex parsing | `df._jdf.queryExecution().optimizedPlan().stats()` |
| Cluster config | Manual `--cluster`/`--workers` flags | `spark.sparkContext.getConf()` auto-detection |
| System table queries | REST → Statement API → warehouse | `spark.table("system.query.history")` |
| Authentication | PAT token in env var | Already authenticated |
| Latency | 500ms–2s per REST call | <50ms per spark.sql() |
| Post-execution calibration | Impossible | Execute → capture metrics → compare |

---

## 2.3 Missing `tables/attribution.py`

DESIGN.md references this module. It doesn't exist. It's required for:

- Calibrating estimates (predicted vs actual)
- "Last time this cost $X" signals
- Training data for Phase 6 ML model

**Core SQL pattern** (from Databricks official docs):
```sql
SELECT
    t1.usage_metadata.job_id,
    t1.usage_metadata.job_run_id,
    SUM(t1.usage_quantity * list_prices.pricing.default) as list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices list_prices
    ON t1.cloud = list_prices.cloud
    AND t1.sku_name = list_prices.sku_name
    AND t1.usage_start_time >= list_prices.price_start_time
    AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE t1.billing_origin_product = 'JOBS'
GROUP BY ALL
```

For SQL Warehouse per-query attribution (approximate):
```sql
-- Duration-proportional attribution
SELECT q.statement_id,
       (q.total_task_duration_ms / hourly_total.total_ms) * u.usage_quantity AS attributed_dbu
FROM system.query.history q
JOIN system.billing.usage u 
    ON q.compute.warehouse_id = u.usage_metadata.warehouse_id
    AND q.start_time BETWEEN u.usage_start_time AND u.usage_end_time
JOIN (...) hourly_total ON ...
```

---

## 2.4 Pricing Model Is Azure-Only and Misses VM Costs

### Multi-Cloud Gap
`get_dbu_rate(sku_name)` has no cloud parameter. The function signature must change now:
```python
def get_dbu_rate(sku_name: str, cloud: str = "AZURE", tier: str = "PREMIUM") -> Decimal:
```

### Missing Dual-Bill (VM + DBU)
For classic compute, VM cost is 40-60% of total. Current estimates show only the DBU half.

**Total cost formula:**
- Classic: `total = (dbu × dbu_rate) + (vm_hours × vm_hourly_rate × (1 + num_workers))`
- Serverless: `total = dbu × serverless_dbu_rate` (VM bundled)

VM prices are available via Azure Retail Prices API (zero auth required):
```
https://prices.azure.com/api/retail/prices?$filter=serviceName eq 'Virtual Machines' and armRegionName eq 'eastus' and armSkuName eq 'Standard_DS4_v2'
```

---

## 2.5 DESIGN.md Has Duplicate/Conflicting Phases

Phases 5 and 9 are both "Production Hardening" with overlapping scope. Phase 7 is "Multi-Cloud Support" but Phase 4 already includes AWS/GCP pricing. Phases 7-10 should be consolidated into Phase 5.

---

## 2.6 Missing Top-Level API for Notebook Use

No `dburnrate.estimate()` function exists. The only entry point is the CLI. For notebook use:

```python
# This should work but doesn't
import dburnrate
estimate = dburnrate.estimate("SELECT ...")
```

Need a simple top-level function that auto-detects runtime context.
